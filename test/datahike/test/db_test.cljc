(ns datahike.test.db-test
  (:require
   [clojure.data]
   #?(:cljs [cljs.test    :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer        [is deftest testing]])
   [datahike.api :as d]
   [datahike.constants :as const]
   [datahike.test.core-test]
   [datahike.test.cljs-utils]
   [datahike.db :as db #?@(:cljs [:refer-macros [defrecord-updatable]]
                           :clj  [:refer [defrecord-updatable]])]))

;; verify that defrecord-updatable works with compiler/core macro configuration
;; define dummy class which redefines hash, could produce either
;; compiler or runtime error
;;
(defrecord-updatable HashBeef [x]
  #?@(:cljs [IHash                (-hash  [hb] 0xBEEF)]
      :clj  [clojure.lang.IHashEq (hasheq [hb] 0xBEEF)]))

(deftest test-defrecord-updatable
  (is (= 0xBEEF (-> (map->HashBeef {:x :ignored}) hash))))

(deftest test-fn-hash-changes
  (let [db (d/db-with (db/empty-db)
                      [{:db/id 1 :name "Konrad"}])
        r1 (d/db-with db [[:db.fn/retractEntity 1]])
        r2 (d/db-with db [[:db.fn/retractEntity 1]])]
    (is (= (hash r1) (hash r2)))))

(deftest test-equiv-db-hash
  (let [db (d/db-with (db/empty-db)
                      [{:db/id 1 :name "Konrad"}])
        r1 (d/db-with db [[:db.fn/retractEntity 1]])]
    (is (= (hash (db/empty-db)) (hash r1)))))

(deftest empty-db-with-schema
  (testing "Test old write schema"
    (is (thrown-msg?
         "Incomplete schema attributes, expected at least :db/valueType, :db/cardinality"
         (db/empty-db {:name {:db/cardinality :db.cardinality/many}} {:schema-flexibility :write})))
    (is (= (merge const/non-ref-implicit-schema
                  {:name {:db/cardinality :db.cardinality/one :db/valueType :db.type/string}})
           (:schema (db/empty-db {:name {:db/cardinality :db.cardinality/one
                                         :db/valueType :db.type/string}}
                                 {:schema-flexibility :write}))))

    (is (= (merge const/non-ref-implicit-schema
                  {:name {:db/ident :name :db/cardinality :db.cardinality/one :db/valueType :db.type/string}})
           (:schema (db/empty-db [{:db/ident :name
                                   :db/cardinality :db.cardinality/one
                                   :db/valueType :db.type/string}]
                                 {:schema-flexibility :write}))))))
