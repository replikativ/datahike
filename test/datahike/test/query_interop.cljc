(ns datahike.test.query-interop
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [datahike.core :as d]))

(def test-db
  (d/db-with
   (d/empty-db)
   [[:db/add 1 :name "Vlad"]
    [:db/add 2 :name "Ivan"]
    [:db/add 3 :name "Sergey"]]))

(deftest test-filter
  (are [q expected] (= (d/q q test-db) expected)
    '[:find ?v
      :where [_ :name ?v]
      [(.startsWith ?v "Ser")]]
    #{["Sergey"]}

    '[:find ?v
      :where [_ :name ?v]
      [(.contains ?v "a")]]
    #{["Vlad"] ["Ivan"]}

    '[:find ?v
      :where [_ :name ?v]
      [(.matches ?v ".+rg.+")]]
    #{["Sergey"]}))

(deftest test-bind
  (are [q expected] (= (d/q q test-db) expected)
    '[:find ?V
      :where
      [?e :name ?v]
      [(.toLowerCase ?v) ?V]]
    #{["vlad"] ["ivan"] ["sergey"]}))

(deftest test-method-not-found
  (is (thrown? Exception (d/q '[:find ?v
                                :where
                                [?e :name ?v]
                                [(.thisMethodDoesNotExist ?v 1)]]
                              test-db))))