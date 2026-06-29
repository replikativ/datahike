(ns datahike.test.experimental.graph-cost-oracle
  "Cost-model ORACLE harness for the query planner's handling of graph-algorithm
   functions.  Each scenario wraps `transitive-closure` in an atom counter via a
   top-level var that carries the SAME :datahike/output-cardinality and
   :datahike/cost metadata as the real fn (the metadata must live on the VAR, as
   the planner resolves it through `(meta (resolve fn-sym))`).  Each scenario
   COUNTS the ACTUAL number of algorithm invocations the planner's chosen order
   produces.  The query result cache is always disabled.

   `run-all` prints a table; `cost-oracle-optimal-placement` asserts the optimal
   invocation counts the planner+executor now achieve:

     FILTER  5   — a selective filter shrinks the fn input 200 -> 5
     EXPAND  5   — an expanding join (5 -> 200) must NOT pull the fn late
     JOINFN  5   — a row-reducing join (100 -> 5) is probe-sunk ahead of the fn
                   at execution time (no static cost model can see this skew)
     MUTUAL  cheap=20 exp=3 — cheaper/more-selective fn ordered before the dear one

   History: the static planner achieved FILTER/EXPAND/MUTUAL but left JOINFN at
   100 (the row-reducing-vs-expanding-join ambiguity is invisible to per-
   attribute stats); execute.cljc probe-and-sink closed it to 5."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.db :as db]
            [datahike.query :as q]
            [datahike.lru :as lru]
            [datahike.experimental.graph :as graph]
            [datahike.query.lower :as lower]
            [datahike.query.logical :as logical]))

(defn clear-plan-cache!
  "The query PLAN cache is keyed by query-shape + schema-hash, NOT by data —
   so without clearing it the planner reuses the plan it computed for the FIRST
   db of a given shape. The oracle sweeps must re-plan per db, so we reset the
   (private) plan cache before each measured query."
  []
  (vreset! @#'q/plan-cache (lru/lru 100)))

(def ^:private tc-meta (meta #'graph/transitive-closure))
(def ^:private tc-oc (:datahike/output-cardinality tc-meta))
(def ^:private tc-cost (:datahike/cost tc-meta))

;; Wrapper vars carry the REAL fn's metadata so the planner costs them
;; identically to the algorithm. Invocations are counted by the ENGINE (no atom,
;; no extra fn arg): `:count-fns? true` on the query makes bind-by-fn accumulate
;; {fn-sym → invocations} into the threaded context, surfaced as :fn-counts result
;; metadata. `count-of` reads it back by the wrapper's fully-qualified symbol.
(defn ^{:datahike/output-cardinality tc-oc :datahike/cost tc-cost} filter-tcv
  [g db s] (graph/transitive-closure g db s))
(defn ^{:datahike/output-cardinality tc-oc :datahike/cost tc-cost} expand-tcv
  [g db s] (graph/transitive-closure g db s))
(defn ^{:datahike/output-cardinality tc-oc :datahike/cost tc-cost} joinfn-tcv
  [g db s] (graph/transitive-closure g db s))
(defn ^{:datahike/output-cardinality tc-oc :datahike/cost tc-cost} sink-tcv
  [g db s] (graph/transitive-closure g db s))
;; MUTUAL: two wrappers, SAME input, DIFFERENT per-call cost.
(defn ^{:datahike/output-cardinality tc-oc :datahike/cost 1} mutual-cheap
  [g db s] (graph/transitive-closure g db s))
(defn ^{:datahike/output-cardinality tc-oc :datahike/cost 1000} mutual-exp
  [g db s] (graph/transitive-closure g db s))

(def ^:private oracle-ns "datahike.test.experimental.graph-cost-oracle")
(defn- count-of
  "Engine-collected invocation count for wrapper `nm` from a result's :fn-counts
   metadata (see :count-fns?)."
  [res nm]
  (get (:fn-counts (meta res)) (symbol oracle-ns nm) 0))

(defn- plan-of [dd where]
  (mapv (juxt :op :fn-sym #(second (:clause %)))
        (:ops (lower/lower (logical/build-logical-plan dd where #{} nil) dd nil))))

;; ---------------------------------------------------------------------------
;; FILTER: a selective ref-join shrinks the fn input 200 -> 5.
;;   :flagged has a high STANDALONE card (n-extra unrelated datoms) but, joined to
;;   the 200 cited ?x, only 5 survive.  Optimal invocations = 5, and must be
;;   INVARIANT to n-extra (sweeping it must not change the chosen order).
;; ---------------------------------------------------------------------------

(defn filter-case
  ([] (filter-case 3000))
  ([n-extra]
   (let [schema {:cites   {:db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
                 :flagged {:db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
                 :follows {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}}
         NP 200
         dd (let [node-txs (vec (for [i (range NP)]
                                  (cond-> {:db/id (+ 1000 i)}
                                    (< i (dec NP)) (assoc :follows (+ 1000 i 1)))))
                  doc-txs  (vec (for [i (range NP)] {:db/id (+ 5000 i) :cites (+ 1000 i)}))
                  flag-core (vec (for [i (range 5)] {:db/id (+ 1000 i) :flagged 99999}))
                  flag-extra (vec (for [i (range n-extra)] {:db/id (+ 700000 i) :flagged 99999}))]
              (-> (db/empty-db schema)
                  (d/db-with node-txs) (d/db-with doc-txs)
                  (d/db-with flag-core) (d/db-with flag-extra)))
         where '[[(datahike.experimental.graph-spec/attr-graph :follows) ?g]
                 [?p :cites ?x] [?x :flagged ?ff]
                 [(datahike.test.experimental.graph-cost-oracle/filter-tcv ?g $ ?x) [?n ...]]]]
     (clear-plan-cache!)
     (let [res (binding [q/*query-result-cache?* false]
                 (d/q {:query {:find '[?p ?n] :where where} :args [dd] :count-fns? true}))]
       {:invocations (count-of res "filter-tcv") :rows (count res) :plan (plan-of dd where)}))))

;; ---------------------------------------------------------------------------
;; EXPAND: the fn input ?x is bound by :seed (5).  A separate :tagged join EXPANDS
;;   5 -> 200.  Optimal = run the fn BEFORE the expanding join (5 invocations);
;;   over-deferral past the join would be 200.
;; ---------------------------------------------------------------------------

(defn expand-case []
  (let [schema {:tagged  {:db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
                :seed    {:db/cardinality :db.cardinality/one}
                :follows {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}}
        dd (let [nodes (vec (for [i (range 200)]
                              (cond-> {:db/id (+ 1000 i)}
                                (< i 199) (assoc :follows (+ 1000 i 1)))))
                 seeds (vec (for [i (range 5)] {:db/id (+ 1000 i) :seed true}))
                 docs (vec (for [i (range 5) j (range 40)]
                             {:db/id (+ 50000 (* i 100) j) :tagged (+ 1000 i)}))]
             (-> (db/empty-db schema)
                 (d/db-with nodes) (d/db-with seeds) (d/db-with docs)))
        where '[[(datahike.experimental.graph-spec/attr-graph :follows) ?g]
                [?x :seed true]
                [(datahike.test.experimental.graph-cost-oracle/expand-tcv ?g $ ?x) [?n ...]]
                [?doc :tagged ?x]]]
    (clear-plan-cache!)
    (let [res (binding [q/*query-result-cache?* false]
                (d/q {:query {:find '[?doc ?n] :where where} :args [dd] :count-fns? true}))]
      {:invocations (count-of res "expand-tcv") :rows (count res) :plan (plan-of dd where)})))

;; ---------------------------------------------------------------------------
;; JOINFN: a join with a HIGH standalone card (n-decoy :owns datoms point to a
;;   non-node) but which, joined to [?x :type], is ROW-REDUCING (100 -> 5).  The
;;   greedy with cost=1 runs the fn first (100 invocations); optimal runs the
;;   join first to feed the fn 5 inputs.
;; ---------------------------------------------------------------------------

(defn joinfn-case
  ([] (joinfn-case 3000))
  ([n-decoy]
   (let [schema {:type    {:db/cardinality :db.cardinality/one}
                 :owns    {:db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
                 :follows {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}}
         dd (let [nodes (vec (for [i (range 100)]
                               (cond-> {:db/id (+ 1000 i) :type "node"}
                                 (< i 99) (assoc :follows (+ 1000 i 1)))))
                  real-owns (vec (for [i (range 5)] {:db/id (+ 20000 i) :owns (+ 1000 i)}))
                  decoys (vec (for [i (range n-decoy)] {:db/id (+ 700000 i) :owns 999999}))]
              (-> (db/empty-db schema)
                  (d/db-with nodes) (d/db-with real-owns) (d/db-with decoys)))
         where '[[(datahike.experimental.graph-spec/attr-graph :follows) ?g]
                 [?x :type "node"] [?o :owns ?x]
                 [(datahike.test.experimental.graph-cost-oracle/joinfn-tcv ?g $ ?x) [?n ...]]]]
     (clear-plan-cache!)
     (let [res (binding [q/*query-result-cache?* false]
                 (d/q {:query {:find '[?o ?n] :where where} :args [dd] :count-fns? true}))]
       {:invocations (count-of res "joinfn-tcv") :rows (count res) :plan (plan-of dd where)}))))

;; ---------------------------------------------------------------------------
;; SINK: like JOINFN, but the graph is TINY so the fn's per-call complexity is
;;   small — small enough that its op-cost is BELOW the decoy-inflated static
;;   scan-card of the reducing :owns join, so the STATIC planner places the fn
;;   BEFORE the reducer. Optimal still = 5, reached only because the execution
;;   SINK probes :owns, sees it reduces, and pulls it ahead of the fn. This is
;;   the case static-deferral + hoist do NOT cover, so it guards the sink.
;; ---------------------------------------------------------------------------

(defn sink-case
  ([] (sink-case 3000))
  ([n-decoy]
   (let [schema {:type        {:db/cardinality :db.cardinality/one}
                 :owns        {:db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
                 :tinyfollows {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}}
         dd (let [nodes (vec (for [i (range 100)]
                               (cond-> {:db/id (+ 1000 i) :type "node"}
                                 (< i 3) (assoc :tinyfollows (+ 1000 i 1)))))
                  real-owns (vec (for [i (range 5)] {:db/id (+ 20000 i) :owns (+ 1000 i)}))
                  decoys (vec (for [i (range n-decoy)] {:db/id (+ 700000 i) :owns 999999}))]
              (-> (db/empty-db schema)
                  (d/db-with nodes) (d/db-with real-owns) (d/db-with decoys)))
         where '[[(datahike.experimental.graph-spec/attr-graph :tinyfollows) ?g]
                 [?x :type "node"] [?o :owns ?x]
                 [(datahike.test.experimental.graph-cost-oracle/sink-tcv ?g $ ?x) [?n ...]]]]
     (clear-plan-cache!)
     (let [res (binding [q/*query-result-cache?* false]
                 (d/q {:query {:find '[?o ?n] :where where} :args [dd] :count-fns? true}))]
       {:invocations (count-of res "sink-tcv") :rows (count res) :plan (plan-of dd where)}))))

;; ---------------------------------------------------------------------------
;; MUTUAL: two cost-annotated wrappers on the SAME input ?x with DIFFERENT
;;   per-call costs.  Each fn output feeds a selective filter (aliveA = 3 of 20,
;;   aliveB = 15 of 20).  The cheaper-per-call fn (mutual-cheap) is ALSO the more
;;   selective one; OPTIMAL = run it first so the dear fn runs on 3 survivors:
;;     optimal  cheap=20 exp=3      bad  exp=20 cheap=15
;;   (General mutual-filter-through-fn-output is the Step 4/5 case.)
;; ---------------------------------------------------------------------------

(defn mutual-case []
  (let [schema {:seed    {:db/cardinality :db.cardinality/one}
                :aliveA  {:db/cardinality :db.cardinality/one}
                :aliveB  {:db/cardinality :db.cardinality/one}
                :follows {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}}
        dd (let [chains (vec (for [i (range 20)]
                               {:db/id (+ 1000 (* i 3)) :seed true :follows (+ 1001 (* i 3))}))
                 mids   (vec (for [i (range 20)] {:db/id (+ 1001 (* i 3)) :follows (+ 1002 (* i 3))}))
                 tails  (vec (for [i (range 20)] {:db/id (+ 1002 (* i 3))}))
                 aliveA (vec (for [i (range 3)]  [:db/add (+ 1002 (* i 3)) :aliveA true]))
                 aliveB (vec (for [i (range 15)] [:db/add (+ 1002 (* i 3)) :aliveB true]))]
             (-> (db/empty-db schema)
                 (d/db-with chains) (d/db-with mids) (d/db-with tails)
                 (d/db-with aliveA) (d/db-with aliveB)))
        where '[[(datahike.experimental.graph-spec/attr-graph :follows) ?g]
                [?x :seed true]
                [(datahike.test.experimental.graph-cost-oracle/mutual-cheap ?g $ ?x) [?a ...]]
                [?a :aliveA true]
                [(datahike.test.experimental.graph-cost-oracle/mutual-exp ?g $ ?x) [?b ...]]
                [?b :aliveB true]]]
    (clear-plan-cache!)
    (let [res (binding [q/*query-result-cache?* false]
                (d/q {:query {:find '[?x] :where where} :args [dd] :count-fns? true}))]
      {:cheap (count-of res "mutual-cheap") :exp (count-of res "mutual-exp") :plan (plan-of dd where)})))

(defn run-all []
  (let [f (filter-case) e (expand-case) j (joinfn-case) s (sink-case) m (mutual-case)]
    (println "SINK    invocations:" (:invocations s) " rows:" (:rows s) " (optimal 5)")
    (println "        plan:" (:plan s))
    (println "FILTER  invocations:" (:invocations f) " rows:" (:rows f) " (optimal 5)")
    (println "        plan:" (:plan f))
    (println "EXPAND  invocations:" (:invocations e) " rows:" (:rows e) " (optimal 5)")
    (println "        plan:" (:plan e))
    (println "JOINFN  invocations:" (:invocations j) " rows:" (:rows j) " (optimal 5)")
    (println "        plan:" (:plan j))
    (println "MUTUAL  cheap:" (:cheap m) " exp:" (:exp m) " (optimal cheap=20 exp=3)")
    (println "        plan:" (:plan m))
    (println "FILTER  n-extra sweep (invocations should stay 5):"
             (mapv #(:invocations (filter-case %)) [0 100 1000 3000 8000]))
    {:filter f :expand e :joinfn j :sink s :mutual m}))

(deftest cost-oracle-optimal-placement
  (testing "FILTER — selective filter shrinks the fn input to 5"
    (is (= 5 (:invocations (filter-case)))))
  (testing "EXPAND — fn runs before the expanding join (not deferred to 200)"
    (is (= 5 (:invocations (expand-case)))))
  (testing "JOINFN — row-reducing join is probe-sunk ahead of the fn (was 100)"
    (is (= 5 (:invocations (joinfn-case)))))
  (testing "SINK — tiny-graph fn placed before an inflated reducer; sink corrects it"
    (is (= 5 (:invocations (sink-case)))))
  (testing "MUTUAL — cheaper/more-selective fn ordered before the dear one"
    (let [m (mutual-case)]
      (is (= 20 (:cheap m)))
      (is (= 3 (:exp m)))))
  (testing "FILTER invocations invariant to unrelated decoy count"
    (is (= [5 5 5 5 5]
           (mapv #(:invocations (filter-case %)) [0 100 1000 3000 8000])))))
