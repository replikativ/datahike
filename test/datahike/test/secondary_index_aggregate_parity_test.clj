(ns datahike.test.secondary-index-aggregate-parity-test
  "The secondary-index aggregate path must answer what the ordinary engines
   answer, or decline the query.

   That path pushes an aggregate straight into a columnar secondary index. To
   do so it FLATTENS the plan (`plan-sub-ops`) and reads each sub-op's
   attribute and ground value — i.e. it reads every sub-op as a POSITIVE
   equality constraint on a column. Anything that negates, defaults, filters or
   re-sources a sub-op is invisible to it, and the answer changes silently:

     * a folded negation carries `:anti?`, and became `[:= col v]` — so the
       aggregate ran over exactly the rows the negation EXCLUDES;
     * a range predicate folded into the scan lives in `:pushdown-preds`,
       which is neither a standalone `:predicate` op nor `:attached-preds`,
       so the filter was dropped;
     * `:not-join` / `:or-join` / rule ops are not column constraints at all
       and were passed through as though they were.

   None of this had test coverage: no database in the suite declared a
   secondary index, so the whole path was exercised zero times. That is why
   these survived, and why the tests live here rather than beside a fix.

   Each case asserts an ABSOLUTE expected value as well as engine parity, so a
   future state where all three paths agree on a wrong answer still fails."
  (:require
   [clojure.test :refer [deftest is testing]]
   [datahike.api :as d]
   [datahike.query :as q]
   ;; loading the integration namespace is what registers the :stratum
   ;; secondary-index type; without it `create-database` rejects the schema
   [datahike.index.secondary.stratum]))

(defn- salaries-db []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write}]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn [{:db/ident :emp/name :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one :db/index true}
                        {:db/ident :emp/salary :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/one :db/index true}
                        {:db/ident :emp/dept :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one}
                        {:db/ident :idx/employees
                         :db.secondary/type :stratum
                         :db.secondary/attrs [:emp/salary :emp/dept]
                         :db.secondary/config {}
                         :db.secondary/status :ready}])
      (d/transact conn [{:emp/name "Ivan"  :emp/salary 50000 :emp/dept "eng"}
                        {:emp/name "Ivan"  :emp/salary 80000 :emp/dept "sales"}
                        {:emp/name "Petr"  :emp/salary 60000 :emp/dept "eng"}
                        {:emp/name "Ivan"  :emp/salary 70000 :emp/dept "eng"}
                        {:emp/name "Petr"  :emp/salary 90000 :emp/dept "sales"}])
      (d/db conn))))

(defn- planner [query db]
  (binding [q/*disable-planner* false q/*query-result-cache?* false]
    (set (d/q query db))))

(defn- base [query db]
  (binding [q/*disable-planner* true q/*query-result-cache?* false]
    (set (d/q query db))))

(deftest secondary-index-aggregate-honours-negation
  (testing "a folded negation must exclude, not select"
    (let [db (salaries-db)
          ;; eng = {50000 60000 70000}, sales = {80000 90000}
          not-clause '[:find (min ?s) (max ?s) :where
                       [?e :emp/salary ?s] (not [?e :emp/dept "eng"])]
          not-join   '[:find (min ?s) :where
                       [?e :emp/salary ?s] (not-join [?e] [?e :emp/dept "eng"])]
          or-join    '[:find (min ?s) :where
                       [?e :emp/salary ?s] (or-join [?e] [?e :emp/dept "sales"])]]
      (is (= #{[80000 90000]} (base not-clause db)))
      (is (= #{[80000 90000]} (planner not-clause db))
          "BUG: aggregated over the EXCLUDED set, answering [50000 70000]")
      (is (= #{[80000]} (base not-join db)))
      (is (= #{[80000]} (planner not-join db))
          "not-join must not be dropped")
      (is (= #{[80000]} (base or-join db)))
      (is (= #{[80000]} (planner or-join db))
          "or-join must not be dropped"))))

(deftest secondary-index-aggregate-honours-pushed-down-predicate
  (testing "a predicate folded into the scan still filters"
    (let [db (salaries-db)
          ;; :emp/salary is :db/index true, so [(> ?s 60000)] is folded into
          ;; the scan's :pushdown-preds rather than left as a :predicate op
          q '[:find (min ?s) (max ?s) :where
              [?e :emp/salary ?s] [(> ?s 60000)]]]
      (is (= #{[70000 90000]} (base q db)))
      (is (= #{[70000 90000]} (planner q db))
          "BUG: the pushed-down filter was dropped, answering [50000 90000]"))))

(deftest secondary-index-aggregate-honours-get-else-default
  (testing "a get-else default is not lost"
    (let [db (salaries-db)
          q '[:find (min ?b) :where
              [?e :emp/salary ?s] [(get-else $ ?e :emp/bonus 42) ?b]]]
      (is (= #{[42]} (base q db)))
      (is (= #{[42]} (planner q db))))))

(deftest secondary-index-aggregate-plain-case-still-works
  (testing "the shapes the path DOES implement keep working"
    ;; The gate must decline unsupported meaning without disabling the path:
    ;; a plain aggregate, and one with an ordinary equality constraint, are
    ;; exactly what it exists for.
    (let [db (salaries-db)
          plain '[:find (min ?s) (max ?s) :where [?e :emp/salary ?s]]
          grouped '[:find ?d (min ?s) :where
                    [?e :emp/salary ?s] [?e :emp/dept ?d]]]
      (is (= #{[50000 90000]} (base plain db)))
      (is (= (base plain db) (planner plain db)))
      (is (= #{["eng" 50000] ["sales" 80000]} (base grouped db)))
      (is (= (base grouped db) (planner grouped db))))))
