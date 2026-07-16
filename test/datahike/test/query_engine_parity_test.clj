(ns datahike.test.query-engine-parity-test
  "Pins engine-parity contracts between the planner and the base (relational)
   engine that have historically drifted apart silently.

   Contract 1 — unresolvable clauses RAISE on both engines.
   The base engine's clause loop (tools/resolve-clauses) raises \"Cannot
   resolve any more clauses\" when no clause can make progress. The planner
   executes its plan in a single linear pass: bind-by-fn's nil (retry-later)
   signal has no retry queue to land in, so it must raise the same error
   (execute/bind-by-fn-strict). History of drifting here: #814 added an
   `(or next-ctx ctx)` guard that silently DROPPED the clause; #815 removed
   it as \"structurally unreachable\" — a premise the ordering fallbacks
   (order-plan-ops greedy/:force branches, which emit unrunnable ops by
   design) never satisfied — after which the nil WIPED the whole context.
   Both states produced silent wrong results instead of an error. This test
   makes the invariant executable so the next refactor can't remove it on a
   prose argument.

   Contract 2 — replan must not leak nested-query-literal vars.
   plan/replan seeds re-ordering with the executed prefix's bound vars; using
   the recursive :vars walk would mark a subquery literal's ?p as bound in
   the outer scope (the same leak fixed in order-plan-ops/op-available-vars),
   letting a consumer of the real ?p be ordered before its producer."
  (:require
   [clojure.test :refer [deftest is testing]]
   [datahike.api :as d]
   [datahike.query :as q]
   [datahike.query.plan :as plan]))

(defn- fresh-conn []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write}]
    (d/create-database cfg)
    (d/connect cfg)))

(defn- planner [query & args]
  (binding [q/*disable-planner* false] (apply d/q query args)))

(defn- base [query & args]
  (binding [q/*disable-planner* true] (apply d/q query args)))

(defn- raised-cannot-resolve? [f]
  (try (f) false
       (catch Exception e
         (boolean (re-find #"Cannot resolve any more clauses"
                           (str (.getMessage e)))))))

(deftest unresolvable-clause-raises-on-both-engines
  (testing "a fn clause whose input var can never be bound raises identically"
    (let [conn (fresh-conn)
          _    (d/transact conn [{:db/ident :v :db/valueType :db.type/long
                                  :db/cardinality :db.cardinality/one}])
          db   (d/db conn)
          query '[:find ?n . :in $ :where [(count ?never) ?n]]]
      (is (raised-cannot-resolve? #(base query db))
          "base engine raises")
      (is (raised-cannot-resolve? #(planner query db))
          "planner raises the same error (was: silent nil via ctx wipe)"))))

(deftest unresolvable-chained-clause-raises-on-both-engines
  (testing "an unresolvable clause downstream of resolvable ones also raises"
    (let [conn (fresh-conn)
          _    (d/transact conn [{:db/ident :person/name :db/valueType :db.type/string
                                  :db/cardinality :db.cardinality/one}])
          _    (d/transact conn [{:person/name "alice"}])
          db   (d/db conn)
          query '[:find ?n . :in $ :where
                  [?e :person/name ?p]
                  [(count ?nowhere) ?n]]]
      (is (raised-cannot-resolve? #(base query db)))
      (is (raised-cannot-resolve? #(planner query db))))))

(deftest get-else-quoted-default-parity
  (testing "a (quote x) get-else default unwraps to its constant on BOTH engines"
    ;; The base path unwraps quote args in -call-fn; the planner's fused
    ;; optional-merge paths plant :default-value into tuples VERBATIM, so the
    ;; unwrap must happen at LOptionalScan construction (logical.cljc) — found
    ;; by adversarial review: base returned (:a :b) while the planner returned
    ;; the literal (quote (:a :b)).
    (let [conn (fresh-conn)
          _    (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                                  :db/cardinality :db.cardinality/one}
                                 {:db/ident :nick :db/valueType :db.type/string
                                  :db/cardinality :db.cardinality/one}])
          _    (d/transact conn [{:db/id 100 :name "alice" :nick "al"}
                                 {:db/id 101 :name "bob"}])
          db   (d/db conn)
          query '[:find ?e ?v :in $ :where
                  [?e :name ?n]
                  [(get-else $ ?e :nick (quote (:a :b))) ?v]]]
      (is (= #{[100 "al"] [101 '(:a :b)]} (base query db)))
      (is (= (base query db) (planner query db))
          "planner must plant the unwrapped constant, not the (quote …) literal"))))

(deftest replan-seed-does-not-leak-literal-vars
  (testing "replan orders a consumer of the real ?p after ?p's producer, even
            when an executed nested-q op mentions ?p inside its query literal"
    (let [conn (fresh-conn)
          _    (d/transact conn [{:db/ident :g/account :db/valueType :db.type/ref
                                  :db/cardinality :db.cardinality/one}
                                 {:db/ident :person/name :db/valueType :db.type/string
                                  :db/cardinality :db.cardinality/one}])
          _    (d/transact conn [{:person/name "alice"}])
          db   (d/db conn)
          query '{:find [?u ?n]
                  :in [$]
                  :where [[(datahike.api/q [:find ?p :in $x :where [$x ?p :g/account _]] $) ?v]
                          [(count ?v) ?n]
                          [?e :person/name ?p]
                          [(clojure.string/upper-case ?p) ?u]]}
          plan0 (#'q/get-or-create-plan db (:where query) #{} nil {})
          ;; Simulate a mid-execution replan right after the nested-q op
          ;; (index 0 in plan order: it is the only unconditionally-runnable op).
          replanned (plan/replan plan0 0 1 db)
          ops (:ops replanned)
          idx-of (fn [pred] (first (keep-indexed (fn [i op] (when (pred op) i)) ops)))
          scan-idx (idx-of (fn [op] (#{:pattern-scan :entity-group} (:op op))))
          upper-idx (idx-of (fn [op] (and (= :function (:op op))
                                          (= 'clojure.string/upper-case (:fn-sym op)))))]
      (is (some? scan-idx) "plan retains the :person/name scan")
      (is (some? upper-idx) "plan retains the upper-case fn op")
      (is (< scan-idx upper-idx)
          "consumer of the real ?p is ordered after ?p's producer (leaked
           literal ?p in the replan seed would allow the reverse)")
      ;; And end-to-end: the full query stays correct on both engines.
      (is (= (base query db) (planner query db) #{["ALICE" 0]})))))
