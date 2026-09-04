(ns datahike.test.query-interop-test
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest]]
      :clj  [clojure.test :as t :refer        [is are deftest]])
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.query.resolve :as qr]))

#?(:cljs (def Exception js/Error))

(def test-db
  (d/db-with
   (db/empty-db)
   [[:db/add 1 :name "Vlad"]
    [:db/add 2 :name "Ivan"]
    [:db/add 3 :name "Sergey"]]))

(deftest test-interop-needs-the-permissive-resolver
  (binding [qr/*symbol-resolver* qr/safe-symbol-resolver]
    ;; a query the permissive tests do not run: results are cached per query
    (is (thrown-with-msg? Exception #"Unknown predicate"
                          (d/q '[:find ?v
                                 :where [_ :name ?v]
                                 [(.endsWith ?v "ey")]]
                               test-db))
        "under the safe resolver, as the server runs, a method is not called by reflection")))

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
