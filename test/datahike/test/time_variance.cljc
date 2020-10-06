(ns datahike.test.time-variance
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer [is are deftest testing use-fixtures]])
   [datahike.constants :as const]
   [datahike.api :as d])
  (:import [java.util Date]))

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

(defn now []
  (Date.))

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
             (d/q '[:find ?n ?a :where [?r :age ?a _ false] [?r :name ?n _ false]] (d/history @conn)))))
    (testing "find source transaction of retracted values"
      (is (= #{[25 true] [25 false] [30 true] [30 false]}
             (d/q '[:find ?a ?op
                    :in $ ?e
                    :where
                    [?e :age ?a ?t ?op]
                    [?t :db/txInstant ?d]]
                  (d/history @conn)
                  [:name "Alice"]))))))

(deftest test-historical-queries
  (let [uri "datahike:mem://test-historical-queries"
        _ (d/delete-database uri)
        _ (create-test-db uri)
        conn (d/connect uri)]

    (testing "get all values before specific time"
      (let [_ (d/transact conn [{:db/id [:name "Alice"] :age 30}])
            _ (Thread/sleep 100)
            date (now)
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
        first-date (now)
        query '[:find ?a :in $ ?e :where [?e :age ?a ?tx]]]
    (testing "get values at specific time"
      (is (= #{[25]}
             (d/q query (d/as-of @conn first-date) [:name "Alice"]))))
    (testing "use transaction ID"
      (let [tx-id 536870914]
        (is (= #{[25]}
               (d/q query (d/as-of @conn tx-id) [:name "Alice"])))))))

(deftest test-since-db
  (let [uri "datahike:mem://test-historical-queries"
        _ (d/delete-database uri)
        _ (create-test-db uri)
        conn (d/connect uri)
        first-date (now)
        query '[:find ?a :where [?e :age ?a]]]
    (testing "empty after first insertion"
      (is (= #{}
             (d/q query (d/since @conn first-date)))))
    (testing "added new value"
      (let [new-age 30
            _ (d/transact conn [{:db/id [:name "Alice"] :age new-age}])]
        (is (= #{[new-age]}
               (d/q query (d/since @conn first-date))))))))

(deftest test-no-history
  (let [uri "datahike:mem://test-no-history"
        _ (d/delete-database uri)
        _ (d/create-database uri :initial-tx [{:db/ident :name
                                               :db/cardinality :db.cardinality/one
                                               :db/valueType :db.type/string
                                               :db/unique :db.unique/identity}
                                              {:db/ident :age
                                               :db/cardinality :db.cardinality/one
                                               :db/valueType :db.type/long
                                               :db/noHistory true}
                                              {:name "Alice" :age 25}
                                              {:name "Bob" :age 35}])
        conn (d/connect uri)
        query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
        first-date (now)]
    (testing "all names and ages are present in history"
      (is (= #{["Alice" 25] ["Bob" 35]}
             (d/q query (d/history @conn)))))
    (d/transact conn [[:db/retractEntity [:name "Alice"]]])
    (testing "no-history attributes are not present in history"
      (is (= #{["Bob" 35]}
             (d/q query (d/history @conn)))))
    (testing "all other attributes are present in history"
      (is (= #{["Alice"] ["Bob"]}
             (d/q '[:find ?n :where [?e :name ?n]] (d/history @conn)))))))

(deftest upsert-history
  (let [cfg {:store {:backend :mem
                     :id "test-upsert-history"}
             :keep-history? true
             :schema-flexibility :read
             :initial-tx schema-tx}
        _ (do (d/delete-database cfg)
              (d/create-database cfg))
        conn (d/connect cfg)
        query '[:find ?a ?t ?op
                :where
                [?e :name "Alice"]
                [?e :age ?a ?t ?op]]]
    (testing "upsert entity"
      (d/transact conn [[:db/add [:name "Alice"] :age 30]])
      (is (= #{[30 (+ const/tx0 2) true]}
             (d/q query @conn)))
      (is (= #{[25 (+ const/tx0 1) true]
               [25 (+ const/tx0 2) false]
               [30 (+ const/tx0 2) true]}
             (d/q query (d/history @conn)))))
    (testing "second upsert"
      (d/transact conn [[:db/add [:name "Alice"] :age 35]])
      (is (= #{[35 (+ const/tx0 3) true]}
             (d/q query @conn)))
      (is (= #{[25 (+ const/tx0 1) true]
               [25 (+ const/tx0 2) false]
               [30 (+ const/tx0 2) true]
               [30 (+ const/tx0 3) false]
               [35 (+ const/tx0 3) true]}
             (d/q query (d/history @conn)))))
    (testing "re-insert previous value"
      (d/transact conn [[:db/add [:name "Alice"] :age 25]])
      (is (= #{[25 (+ const/tx0 4) true]}
             (d/q query @conn)))
      (is (= #{[25 (+ const/tx0 1) true]
               [25 (+ const/tx0 2) false]
               [30 (+ const/tx0 2) true]
               [30 (+ const/tx0 3) false]
               [35 (+ const/tx0 3) true]
               [35 (+ const/tx0 4) false]
               [25 (+ const/tx0 4) true]}
             (d/q query (d/history @conn)))))
    (testing "retract upserted values"
      (d/transact conn [[:db/retract [:name "Alice"] :age 25]])
      (is (= #{}
             (d/q query @conn)))
      (is (= #{[25 (+ const/tx0 1) true]
               [25 (+ const/tx0 2) false]
               [30 (+ const/tx0 2) true]
               [30 (+ const/tx0 3) false]
               [35 (+ const/tx0 3) true]
               [35 (+ const/tx0 4) false]
               [25 (+ const/tx0 4) true]
               [25 (+ const/tx0 5) false]}
             (d/q query (d/history @conn)))))))
