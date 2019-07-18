(ns datahike.test.temporal-index
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing use-fixtures]])
   [datahike.api :as d]
   [datahike.test.core :as tdc]))


(deftest test-historical-queries
  (let [uri "datahike:mem://test-historical-queries"
        _ (d/delete-database uri)
        schema-tx [{:db/ident       :name
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
                    :age  35}]
        config {:uri uri :initial-tx schema-tx}
        _ (d/create-database config)
        conn (d/connect uri)]

    (testing "Initial data"
      (let [db (d/db conn)
            max-tx (:max-tx db)]
        (are [x y]
          (= x y)

          #{["Alice" 25] ["Bob" 35]}
          (d/q '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]] db)

          #{["Alice" 25 max-tx] ["Bob" 35 max-tx]}
          (d/q '[:find ?n ?a ?tx :where [?e :name ?n ?tx] [?e :age ?a]] db)))

      (testing "transact new :age value to 'Alice'"
        (let [_ (d/transact! conn [{:db/id [:name "Alice"] :age 30}])
              db (d/db conn)]
          (are [x y]
            (= x y)
            #{[30]}
            (d/q '[:find ?a :in $ ?e :where [?e :age ?a]] db [:name "Alice"])
            #{[30] [25]}
            (d/q '[:find ?a :in $ ?e :where [?e :age ?a ?tx]] db [:name "Alice"]))))

      (testing "get all values before current transaction"
        (let [db (d/db conn)
              max-tx (:max-tx db)]
          (is (=
                #{[25]}
                (d/q '[:find ?a :in $ ?e ?t :where [?e :age ?a ?tx] [(< ?tx ?t)]] db [:name "Alice"] max-tx))))))))
