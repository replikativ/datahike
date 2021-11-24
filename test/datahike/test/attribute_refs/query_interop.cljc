(ns datahike.test.query-interop
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [deftest are]]
      :clj [clojure.test :as t :refer [deftest are]])
   [datahike.test.attribute-refs.util :refer [ref-db ref-e0
                                              wrap-direct-datoms]]
   [datahike.api :as d]))

(def test-db
  (d/db-with ref-db
             (wrap-direct-datoms ref-db ref-e0 :db/add [[1 :name "Vlad"]
                                                        [2 :name "Ivan"]
                                                        [3 :name "Sergey"]])))

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