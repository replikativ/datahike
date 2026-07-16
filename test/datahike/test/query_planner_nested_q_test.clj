(ns datahike.test.query-planner-nested-q-test
  "Regression test for a query-planner correctness bug.

   A scalar-find (`:find ?m .`) query that (1) counts the result of a nested
   `(q …)` subquery and (2) feeds that count into a downstream binding clause
   `[(= 0 ?n) ?matches]` returns `nil` under the planner where the base
   (relational) engine returns the correct value.

   Root cause (observed): the planner mis-orders the directly-chained
   function-expression clauses `[(count ?v) ?n]` → `[(= 0 ?n) ?matches]` when
   `?v` is produced by a nested subquery — it plans/evaluates the `=` clause
   before `?n` is bound by `count`, so `?matches` never binds and the scalar
   find yields an empty result (`nil`). By contrast, `count` over a plain `:in`
   collection (rather than a nested-q result) is unaffected — see
   `count-over-in-collection-unaffected`.

   It is data-independent — it reproduces on a trivial db (the nested subquery
   even returns empty). Surfaced by kontor's datalog integrity invariants,
   which reduce a nested-q violator-set to a boolean via `count`/`=`.

   Assertion form (as in query-planner-temporal-test): compare the planner
   path against the base engine; they MUST agree."
  (:require
   [clojure.test :refer [deftest is testing]]
   [datahike.api :as d]
   [datahike.query :as q]))

(defn- fresh-db
  "A minimal in-memory db with two ref attributes — enough for the query to
   parse. No data is needed to trigger the bug."
  []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write}]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn [{:db/ident :g/account   :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
                        {:db/ident :g/commodity :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}])
      (d/db conn))))

(defn- planner [query & args]
  (binding [q/*disable-planner* false] (apply d/q query args)))

(defn- base [query & args]
  (binding [q/*disable-planner* true] (apply d/q query args)))

(deftest count-of-nested-q-feeding-equals-binding
  (testing "count over a nested-(q) result feeding [(= 0 ?n) ?m] in a scalar find"
    (let [db    (fresh-db)
          ;; nested subquery is empty (no :g/account datoms) → count 0
          ;; → (= 0 0) → true. The base engine binds ?m = true.
          query '[:find ?m .
                  :in $a
                  :where
                  [(datahike.api/q [:find ?p :in $x :where [$x ?p :g/account _]] $a) ?v]
                  [(count ?v) ?n]
                  [(= 0 ?n) ?m]]]
      (is (= true (base query db))
          "base engine binds ?m = true")
      (is (= (base query db) (planner query db))
          "planner must equal base engine (BUG: planner returns nil)"))))

(deftest count-of-cross-source-nested-q
  (testing "cross-source nested-q (the shape kontor's commodity-match invariant uses)"
    (let [db    (fresh-db)
          query '[:find ?m .
                  :in $after $empty
                  :where
                  [(datahike.api/q [:find ?p
                                    :in $x $y
                                    :where
                                    [$y ?p :g/account ?acc]
                                    [$x ?acc :g/commodity ?c]]
                                   $after $empty) ?v]
                  [(count ?v) ?n]
                  [(= 0 ?n) ?m]]]
      (is (= true (base query db db)))
      (is (= (base query db db) (planner query db db))
          "planner must equal base engine (BUG: planner returns nil)"))))

(deftest count-over-in-collection-unaffected
  (testing "count over a plain :in collection (not a nested-q) is NOT affected"
    (let [db    (fresh-db)
          query '[:find ?m .
                  :in $a ?coll
                  :where
                  [(count ?coll) ?n]
                  [(= 0 ?n) ?m]]]
      (is (= true (base query db [])))
      (is (= (base query db []) (planner query db []))
          "count over an :in collection plans correctly on both engines"))))
