(ns datahike.test.temporal-index
  (:require
    #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer [is are deftest testing use-fixtures]])
    [datahike.api :as d]
    [datahike.db :as dd]))

(def schema-tx [{:db/ident       :name
                 :db/valueType   :db.type/string
                 :db/unique      :db.unique/identity
                 :db/index       true
                 :db/cardinality :db.cardinality/one}
                {:db/ident       :age
                 :db/valueType   :db.type/long
                 :db/cardinality :db.cardinality/one}
                {:name "Alice"
                 :age  25}
                {:name "Bob"
                 :age  35}])

(defn create-test-db [uri]
  (d/create-database uri :initial-tx schema-tx))

(deftest test-base-history
  (let [uri "datahike:mem://test-base-history"
        _ (d/delete-database uri)
        _ (create-test-db uri)
        conn (d/connect uri)]

    (testing "Initial data"
      (is (= #{["Alice" 25] ["Bob" 35]}
             (d/q '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]] @conn))))

    (testing "historical values"
      (d/transact conn [{:db/id [:name "Alice"] :age 30}])
      (are [x y]
          (= x y)
        #{[30]}
        (d/q '[:find ?a :in $ ?e :where [?e :age ?a]] @conn [:name "Alice"])
        #{[30] [25]}
        (d/q '[:find ?a :in $ ?e :where [?e :age ?a]] (d/history @conn) [:name "Alice"])))
    (testing "historical values after with retraction"
      (d/transact conn [[:db/retractEntity [:name "Alice"]]])
      (are [x y]
          (= x y)
        #{}
        (d/q '[:find ?a :in $ ?e :where [?e :age ?a]] @conn [:name "Alice"])
        #{[30] [25]}
        (d/q '[:find ?a :in $ ?e :where [?e :age ?a]] (d/history @conn) [:name "Alice"])))
    (testing "find retracted values"
      (is (= #{["Alice" 25] ["Alice" 30]}
             (d/q '[:find ?n ?a  :where [?e :db/retracted ?r] [?r :age ?a] [?r :name ?n]] (d/history @conn)))))))

(deftest test-historical-queries
  (let [uri "datahike:mem://test-historical-queries"
        _ (d/delete-database uri)
        _ (create-test-db uri)
        conn (d/connect uri)]

    (testing "get all values before specific time"
      (let [_ (d/transact conn [{:db/id [:name "Alice"] :age 30}])
            _ (Thread/sleep 100)
            date (java.util.Date.)
            _ (Thread/sleep 100)
            _ (d/transact conn [{:db/id [:name "Alice"] :age 35}])
            history-db (d/history @conn)
            current-db @conn
            current-query '[:find ?a :in $ ?e :where [?e :age ?a]]
            query '[:find ?a
                    :in $ ?e ?fd
                    :where
                    [?e :age ?a ?tx]
                    [?tx :db/txInstant ?t]
                    [(before? ?t ?fd)]]
            query-with-< '[:find ?a
                           :in $ ?e ?fd
                           :where
                           [?e :age ?a ?tx]
                           [?tx :db/txInstant ?t]
                           [(< ?t ?fd)]]]
        (is (= #{[35]}
               (d/q current-query current-db [:name "Alice"])))
        (is (= #{[25] [30]}
               (d/q query history-db [:name "Alice"] date)))
        (is (= #{[25] [30]}
               (d/q query-with-< history-db [:name "Alice"] date)))))))

(deftest test-as-of-db
  (let [uri "datahike:mem://test-historical-queries"
        _ (d/delete-database uri)
        _ (create-test-db uri)
        conn (d/connect uri)
        first-date (java.util.Date.)
        query '[:find ?a :in $ ?e :where [?e :age ?a ?tx]]]
    (testing "get values at specific time"
      (is (= #{[25]}
               (d/q query (d/as-of @conn first-date) [:name "Alice"]))))
    (testing "use unix epoch time as long"
      (let [epoch-date (.getTime first-date)]
        (is (= #{[25]}
                 (d/q query (d/as-of @conn epoch-date) [:name "Alice"])))))))

(deftest test-since-db
  (let [uri "datahike:mem://test-historical-queries"
        _ (d/delete-database uri)
        _ (create-test-db uri)
        conn (d/connect uri)
        first-date (java.util.Date.)
        query '[:find ?a :where [?e :age ?a]]]
    (testing "empty after first insertion"
      (is (= #{}
            (d/q query (d/since @conn first-date)))))
    (testing "added new value"
      (let [new-age 30
            _ (d/transact conn [{:db/id [:name "Alice"] :age new-age}])]
        (is (= #{[new-age]}
               (d/q query (d/since @conn first-date))))))))
