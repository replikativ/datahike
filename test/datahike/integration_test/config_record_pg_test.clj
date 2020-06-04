(ns datahike.integration-test.config-record-pg-test
  (:require
   [clojure.test :refer :all]
   [datahike.api :as d]
   [datahike-postgres.core]))

(def config {:store {:backend :pg :host "localhost" :port 5432 :user "alice" :password "foo" :dbname "config-test"}})

(defn config-record-pg-fixture [f]
  (d/delete-database config)
  (d/create-database config)
  (def conn (d/connect config))
  ;; the first transaction will be the schema we are using
  (d/transact conn [{:db/ident :name
                     :db/valueType :db.type/string
                     :db/cardinality :db.cardinality/one}
                    {:db/ident :age
                     :db/valueType :db.type/long
                     :db/cardinality :db.cardinality/one}])

  ;; lets add some data and wait for the transaction
  (d/transact conn [{:name  "Alice", :age   20}
                    {:name  "Bob", :age   30}
                    {:name  "Charlie", :age   40}
                    {:age 15}])

  (f))

(use-fixtures :once config-record-pg-fixture)

(deftest ^:integration config-record-pg-test
  ;; search the data
  (is (= #{[3 "Alice" 20] [4 "Bob" 30] [5 "Charlie" 40]}
         (d/q '[:find ?e ?n ?a
                :where
                [?e :name ?n]
                [?e :age ?a]]
              @conn)))

  ;; add new entity data using a hash map
  (d/transact conn {:tx-data [{:db/id 3 :age 25}]})

  ;; if you want to work with queries like in
  ;; https://grishaev.me/en/datomic-query/,
  ;; you may use a hashmap
  (is (= #{[5 "Charlie" 40] [4 "Bob" 30] [3 "Alice" 25]}
         (d/q {:query '{:find [?e ?n ?a]
                        :where [[?e :name ?n]
                                [?e :age ?a]]}
               :args [@conn]})))

  ;; query the history of the data
  (is (= #{[20] [25]}
         (d/q '[:find ?a
                :where
                [?e :name "Alice"]
                [?e :age ?a]]
              (d/history @conn))))

  ;; you might need to release the connection, e.g. for leveldb
  (is (= nil (d/release conn)))

  ;; database should exist
  (is (= true (d/database-exists? config)))

  ;; clean up the database if it is not needed any more
  (is (let [deleted? (d/delete-database config)]
        (= [0] deleted?)))

  ;; database should exist
  (is (= false (d/database-exists? config))))
