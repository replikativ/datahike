(ns datahike.test.entity-spec
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer [is are deftest testing use-fixtures]])
   [datahike.test.core]
   [datahike.api :as d]))

(def schema [{:db/ident :name
              :db/valueType :db.type/string
              :db/unique :db.unique/identity
              :db/cardinality :db.cardinality/one}
             {:db/ident :age
              :db/valueType :db.type/long
              :db/cardinality :db.cardinality/one}
             {:db/ident :person-validate
              :db.entity/attrs [:name :age]}])

(def cfg-template {:store {:backend :mem
                           :id "entity-spec"}
                   :keep-history? false
                   :initial-tx schema})

(defn setup-db [cfg]
  (d/delete-database cfg)
  (d/create-database cfg)
  (d/connect cfg))

(deftest test-attribute-assertion
  (let [valid-person {:name "Antonia"
                      :age 55}
        invalid-person {:name "Arthur"}
        empty-person {}]
    (testing "with write schema flexibility"
      (let [cfg (-> cfg-template
                    (assoc :schema-flexibility :write)
                    (assoc-in [:store :id] "attribute-assertion-write"))
            conn (setup-db cfg)]
        (testing "assert valid person"
          (d/transact conn [(merge valid-person {:db/ensure :person-validate})])
          (is (= valid-person (d/pull @conn '[:name :age] [:name (:name valid-person)]))))
        (testing "assert invalid person"
          (is (thrown-msg?
               "Entity 5 missing attributes #{:age} of by :person-validate"
               (d/transact conn [(merge invalid-person {:db/ensure :person-validate})]))))
        (testing "assert empty entity"
          (is (thrown-msg?
               "Entity 5 missing attributes #{:age :name} of by :person-validate"
               (d/transact conn [(merge empty-person {:db/ensure :person-validate})]))))))
    (testing "with read schema flexibility"
      (let [cfg (-> cfg-template
                    (assoc :schema-flexibility :read)
                    (assoc-in [:store :id] "attribute-assertion-read"))
            conn (setup-db cfg)]
        (testing "assert valid person"
          (d/transact conn [(merge valid-person {:db/ensure :person-validate})])
          (is (= valid-person (d/pull @conn '[:name :age] [:name (:name valid-person)]))))
        (testing "assert invalid person"
          (is (thrown-msg?
               "Entity 5 missing attributes #{:age} of by :person-validate"
               (d/transact conn [(merge invalid-person {:db/ensure :person-validate})]))))
        (testing "assert empty entity"
          (is (thrown-msg?
               "Entity 5 missing attributes #{:age :name} of by :person-validate"
               (d/transact conn [(merge empty-person {:db/ensure :person-validate})]))))))))
