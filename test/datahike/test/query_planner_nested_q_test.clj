(ns datahike.test.query-planner-nested-q-test
  "Regression test for a query-planner correctness bug.

   A scalar-find (`:find ?m .`) query that (1) counts the result of a nested
   `(q …)` subquery and (2) feeds that count into a downstream binding clause
   `[(= 0 ?n) ?matches]` returns `nil` under the planner where the base
   (relational) engine returns the correct value.

   Root cause: the planner extracted \"input vars\" of a function clause by
   walking its args RECURSIVELY (plan/args-free-vars), descending into the
   nested query literal `[:find ?p :in $x :where …]`. The ?vars inside that
   literal are lexically scoped to the subquery, but the walk surfaced them
   as outer-scope inputs, so the op was marked as requiring vars no producer
   can ever bind. Ordering then emitted the whole function chain in arbitrary
   (here: reverse) order, and execution silently dropped the unrunnable
   clauses — the scalar find yields `nil`. The runtime contract (-call-fn /
   bind-by-fn / post-apply-fns) resolves only TOP-LEVEL symbols; nested seq
   forms are walked for ordering (#814), but non-seq collections are data
   literals and must be opaque. By contrast, `count` over a plain `:in`
   collection (rather than a nested-q result) never had phantom inputs — see
   `count-over-in-collection-unaffected`.

   The dual leak (also fixed, see `literal-var-must-not-shadow-outer-var`):
   the ordering loop accumulated a function op's whole recursive :vars set
   into bound-vars, so a query-literal ?p marked the OUTER ?p as bound —
   a later consumer of the real ?p could then be ordered before ?p's actual
   producer and silently dropped, yielding wrong (nil-padded) rows.

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

(deftest literal-var-must-not-shadow-outer-var
  (testing "a ?var inside a nested-q literal must not mark the outer ?var bound"
    (let [cfg  {:store {:backend :memory :id (random-uuid)}
                :schema-flexibility :write}
          _    (d/create-database cfg)
          conn (d/connect cfg)
          _    (d/transact conn [{:db/ident :g/account :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
                                 {:db/ident :person/name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])
          _    (d/transact conn [{:person/name "alice"} {:person/name "bob"}])
          db   (d/db conn)
          ;; The nested-q literal binds its own ?p; the OUTER ?p is produced
          ;; by the [?e :person/name ?p] pattern. If the literal's ?p leaks
          ;; into bound-vars, upper-case is ordered before the pattern and
          ;; silently dropped → #{[nil nil]} instead of the real rows.
          query '[:find ?u ?n
                  :in $
                  :where
                  [(datahike.api/q [:find ?p :in $x :where [$x ?p :g/account _]] $) ?v]
                  [(count ?v) ?n]
                  [?e :person/name ?p]
                  [(clojure.string/upper-case ?p) ?u]]]
      (is (= #{["ALICE" 0] ["BOB" 0]} (base query db)))
      (is (= (base query db) (planner query db))
          "planner must equal base engine (BUG: planner returned #{[nil nil]})"))))

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
