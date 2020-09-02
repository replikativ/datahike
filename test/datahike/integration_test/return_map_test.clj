(ns datahike.integration-test.return-map-test
  (:require
   [clojure.test :refer :all]
   [datahike.api :as d]))

(defn return-map-test-fixture [f]
  (d/delete-database)
  (d/create-database)
  (def conn (d/connect))
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
                    {:name  "Charlie", :age   40}])

  (f))

(use-fixtures :once return-map-test-fixture)

(deftest return-keys-test
  ;; search the data
  (is (= [{:name "Bob" :age 30}
          {:name "Charlie" :age 40}
          {:name "Alice" :age 20}]
         (d/q '[:find ?n ?a
                :keys name age
                :where
                [?e :name ?n]
                [?e :age ?a]]
              @conn))))

(deftest return-syms-test
  ;; search the data
  (is (= [{'name "Bob" 'age 30}
          {'name "Charlie" 'age 40}
          {'name "Alice" 'age 20}]
         (d/q '[:find ?n ?a
                :syms name age
                :where
                [?e :name ?n]
                [?e :age ?a]]
              @conn))))

(deftest return-strs-test
  ;; search the data
  (is (= [{"name" "Bob" "age" 30}
          {"name" "Charlie" "age" 40}
          {"name" "Alice" "age" 20}]
         (d/q '[:find ?n ?a
                :strs name age
                :where
                [?e :name ?n]
                [?e :age ?a]]
              @conn))))
