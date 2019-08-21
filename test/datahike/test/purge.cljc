(ns datahike.test.purge
  (:require
    #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer [is are deftest testing use-fixtures]])
    [datahike.api :as d]))

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
  (d/delete-database uri)
  (d/create-database uri :initial-tx schema-tx)
  (d/connect uri))

(defn find-age [db name]
  (d/q '[:find ?a . :in $ ?n :where [?e :name ?n] [?e :age ?a]] db name))

(deftest test-purge
  (let [conn (create-test-db "datahike:mem://test-purge")]
    (testing "retract datom, data is removed from current db and found in history"
      (let [name "Alice"]
        (d/transact conn [[:db/retract [:name "Alice"] :age 25]])
        (are [x y] (= x y)
          true (nil? (find-age @conn name))
          25 (find-age (d/history @conn) name))))
    (testing "purge datom from current index"
      (let [name "Bob"]
        (d/transact conn [[:db/purge [:name "Bob"] :age 35]])
        (are [x y] (= x y)
          true (nil? (find-age @conn name))
          true (nil? (find-age (d/history @conn) name)))))
    (testing "purge retracted datom"
      (let [name "Alice"]
        (d/transact conn [[:db/purge [:name name] :age 25]])
        (are [x y] (= x y)
                   true (nil? (find-age @conn name))
                   true (nil? (find-age (d/history @conn) name))))
      )))
