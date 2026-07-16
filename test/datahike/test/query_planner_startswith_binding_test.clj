(ns datahike.test.query-planner-startswith-binding-test
  "Regression test for a query-planner variable-binding bug.

   A query that (1) binds a string var from a pattern `[?p :code ?c]`,
   (2) applies a Java-interop string predicate `[(.startsWith ^String ?c …)]`,
   and (3) is followed by a JOIN clause `[?b :parent ?p]` that re-binds `?p`,
   throws `java.lang.Long.startsWith(java.lang.String) not found` under the
   planner — i.e. the planner binds `?c` to a Long (an entity id) rather than
   the string value of `:code`. The base (relational) engine binds `?c`
   correctly.

   The trigger is the JOIN that comes *after* the predicate (see the two
   `-ok` controls): with no trailing join, or with the join placed *before*
   the `[?p :code ?c]` pattern (so `?p` is bound first), both engines agree.
   It appears the planner reorders the trailing join ahead of the code
   pattern and, in doing so, mis-binds the predicate's input var.

   Data-minimal — three datoms suffice. Surfaced by kontor's per-country tax
   provider tests, which match statute-parameter codes with a datalog
   `.startsWith` predicate joined to a bracket table.

   Assertion form (as in query-planner-temporal-test): compare the planner
   path against the base engine; they MUST agree."
  (:require
   [clojure.test :refer [deftest is testing]]
   [datahike.api :as d]
   [datahike.query :as q]))

(defn- fresh-db []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write}]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn [{:db/ident :code   :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
                        {:db/ident :parent :db/valueType :db.type/ref    :db/cardinality :db.cardinality/one}])
      ;; eid 3 :code "X.a", eid 4 :code "Y.b", eid 5 :parent -> 3
      (d/transact conn [{:db/id -1 :code "X.a"} {:code "Y.b"} {:parent -1}])
      (d/db conn))))

(defn- planner [query & args]
  (binding [q/*disable-planner* false] (apply d/q query args)))

(defn- base [query & args]
  (binding [q/*disable-planner* true] (apply d/q query args)))

(deftest startswith-var-mis-bound-with-trailing-join
  (testing "string predicate var mis-bound to a Long when a join follows the predicate"
    (let [db    (fresh-db)
          query '[:find ?b
                  :where
                  [?p :code ?c]
                  [(.startsWith ^String ?c "X")]
                  [?b :parent ?p]]]
      (is (= #{[5]} (base query db))
          "base engine: ?c bound to the string, returns the parent entity")
      (is (= (base query db) (planner query db))
          "planner must equal base engine (BUG: planner binds ?c to a Long → Long.startsWith)"))))

(deftest startswith-no-trailing-join-ok
  (testing "control: without a trailing join, both engines agree"
    (let [db    (fresh-db)
          query '[:find ?p
                  :where
                  [?p :code ?c]
                  [(.startsWith ^String ?c "X")]]]
      (is (= #{[3]} (base query db)))
      (is (= (base query db) (planner query db))))))

(deftest startswith-join-before-predicate-ok
  (testing "control: join placed before the code pattern (so ?p is bound first), both agree"
    (let [db    (fresh-db)
          query '[:find ?b
                  :where
                  [?b :parent ?p]
                  [?p :code ?c]
                  [(.startsWith ^String ?c "X")]]]
      (is (= #{[5]} (base query db)))
      (is (= (base query db) (planner query db))))))
