(ns datahike.test.experimental.graph-cost-oracle
  "Cost-model ORACLE harness for the query planner's handling of graph-algorithm
   functions.  Each scenario wraps `transitive-closure` in an atom counter via a
   top-level var that carries the SAME :datahike/output-cardinality and
   :datahike/cost metadata as the real fn (the metadata must live on the VAR, as
   the planner resolves it through `(meta (resolve fn-sym))`).  Each scenario
   COUNTS the ACTUAL number of algorithm invocations the planner's chosen order
   produces.  The query result cache is always disabled.

   These are not asserted in the guard suite (the observed numbers move as the
   planner evolves across the DP steps); run `(run-all)` to print a table.
   Optimal invocation counts (what the planner should achieve):

     FILTER  optimal 5   — a selective filter shrinks the fn input 200 -> 5
     EXPAND  optimal 5   — an expanding join (5 -> 200) must NOT pull the fn late
     JOINFN  optimal 5   — a high-card but row-reducing join shrinks input 100 -> 5
     MUTUAL  cheap=20 exp=3 — cheaper/more-selective fn ordered before the dear one

   Baseline (pre-Step-1, :datahike/cost 1 compromise) observed:
     FILTER 5, EXPAND 5, JOINFN 100 (BAD), MUTUAL cheap=20 exp=3."
  (:require [datahike.api :as d]
            [datahike.db :as db]
            [datahike.query :as q]
            [datahike.experimental.graph :as graph]
            [datahike.query.lower :as lower]
            [datahike.query.logical :as logical]))

(def ^:private tc-meta (meta #'graph/transitive-closure))
(def ^:private tc-oc (:datahike/output-cardinality tc-meta))
(def ^:private tc-cost (:datahike/cost tc-meta))

;; Counter atoms (reset per run).
(def filter-cnt (atom 0))
(def expand-cnt (atom 0))
(def joinfn-cnt (atom 0))
(def mutual-cheap-cnt (atom 0))
(def mutual-exp-cnt (atom 0))

;; Wrapper vars carry the REAL fn's metadata so the planner costs them identically.
(defn ^{:datahike/output-cardinality tc-oc :datahike/cost tc-cost} filter-tcv
  [g db s] (swap! filter-cnt inc) (graph/transitive-closure g db s))
(defn ^{:datahike/output-cardinality tc-oc :datahike/cost tc-cost} expand-tcv
  [g db s] (swap! expand-cnt inc) (graph/transitive-closure g db s))
(defn ^{:datahike/output-cardinality tc-oc :datahike/cost tc-cost} joinfn-tcv
  [g db s] (swap! joinfn-cnt inc) (graph/transitive-closure g db s))
;; MUTUAL: two wrappers, SAME input, DIFFERENT per-call cost.
(defn ^{:datahike/output-cardinality tc-oc :datahike/cost 1} mutual-cheap
  [g db s] (swap! mutual-cheap-cnt inc) (graph/transitive-closure g db s))
(defn ^{:datahike/output-cardinality tc-oc :datahike/cost 1000} mutual-exp
  [g db s] (swap! mutual-exp-cnt inc) (graph/transitive-closure g db s))

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
     (reset! filter-cnt 0)
     (let [res (binding [q/*query-result-cache?* false]
                 (d/q {:query {:find '[?p ?n] :where where} :args [dd]}))]
       {:invocations @filter-cnt :rows (count res) :plan (plan-of dd where)}))))

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
    (reset! expand-cnt 0)
    (let [res (binding [q/*query-result-cache?* false]
                (d/q {:query {:find '[?doc ?n] :where where} :args [dd]}))]
      {:invocations @expand-cnt :rows (count res) :plan (plan-of dd where)})))

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
     (reset! joinfn-cnt 0)
     (let [res (binding [q/*query-result-cache?* false]
                 (d/q {:query {:find '[?o ?n] :where where} :args [dd]}))]
       {:invocations @joinfn-cnt :rows (count res) :plan (plan-of dd where)}))))

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
    (reset! mutual-cheap-cnt 0) (reset! mutual-exp-cnt 0)
    (binding [q/*query-result-cache?* false]
      (d/q {:query {:find '[?x] :where where} :args [dd]}))
    {:cheap @mutual-cheap-cnt :exp @mutual-exp-cnt :plan (plan-of dd where)}))

(defn run-all []
  (let [f (filter-case) e (expand-case) j (joinfn-case) m (mutual-case)]
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
    {:filter f :expand e :joinfn j :mutual m}))
