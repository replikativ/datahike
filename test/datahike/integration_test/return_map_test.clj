(ns datahike.integration-test.return-map-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]))

(def test-db
  (do (d/delete-database)
  (d/create-database)
  (let [conn (d/connect)]
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

    @conn)))


(deftest return-keys-test
  ;; search the data
  (is (= [{:entity 4 :name "Bob" :age 30}
          {:entity 5 :name "Charlie" :age 40}
          {:entity 3 :name "Alice" :age 20}]
         (d/q '[:find ?e ?n ?a
                :keys entity name age
                :where
                [?e :name ?n]
                [?e :age ?a]]
              test-db))))

(deftest return-syms-test
  ;; search the data
  (is (= [{'entity 4 'name "Bob" 'age 30}
          {'entity 5 'name "Charlie" 'age 40}
          {'entity 3 'name "Alice" 'age 20}]
         (d/q '[:find ?e ?n ?a
                :syms entity name age
                :where
                [?e :name ?n]
                [?e :age ?a]]
              test-db))))

(deftest return-strs-test
  ;; search the data
  (is (= [{"entity" 4 "name" "Bob" "age" 30}
          {"entity" 5 "name" "Charlie" "age" 40}
          {"entity" 3 "name" "Alice" "age" 20}]
         (d/q '[:find ?e ?n ?a
                :strs entity name age
                :where
                [?e :name ?n]
                [?e :age ?a]]
              test-db))))
