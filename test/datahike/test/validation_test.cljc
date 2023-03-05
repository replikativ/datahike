(ns datahike.test.validation-test
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest]]
      :clj  [clojure.test :as t :refer        [is are deftest]])
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.test.core-test]))

#?(:cljs
   (def Throwable js/Error))

(deftest test-with-validation
  (let [db (db/empty-db {:profile {:db/valueType :db.type/ref}})]
    (are [tx] (thrown-with-msg? Throwable #"Expected number, string or lookup ref for :db/id" (d/db-with db tx))
      [{:db/id #"" :name "Ivan"}])

    (are [tx] (thrown-with-msg? Throwable #"Bad entity attribute" (d/db-with db tx))
      [[:db/add -1 nil "Ivan"]]
      [[:db/add -1 17 "Ivan"]]
      [{:db/id -1 17 "Ivan"}])

    (are [tx] (thrown-with-msg? Throwable #"Cannot store nil as a value" (d/db-with db tx))
      [[:db/add -1 :name nil]]
      [{:db/id -1 :name nil}])

    (are [tx] (thrown-with-msg? Throwable #"Expected number or lookup ref for entity id" (d/db-with db tx))
      [[:db/add nil :name "Ivan"]]
      [[:db/add {} :name "Ivan"]]
      [[:db/add -1 :profile #"regexp"]]
      [{:db/id -1 :profile #"regexp"}])

    (is (thrown-with-msg? Throwable #"Unknown operation" (d/db-with db [["aaa" :name "Ivan"]])))
    (is (thrown-with-msg? Throwable #"Tempids are allowed in :db/add only" (d/db-with db [[:db/retract -1 :name "Ivan"]])))
    (is (thrown-with-msg? Throwable #"Bad transaction data" (d/db-with db {:profile "aaa"})))))

(deftest ^:no-spec test-with-validation-caught-by-spec
  (let [db (db/empty-db {:profile {:db/valueType :db.type/ref}})]
    (is (thrown-with-msg? Throwable #"Bad entity type at" (d/db-with db [:db/add "aaa" :name "Ivan"])))))

(deftest test-unique
  (let [db (d/db-with (db/empty-db {:name {:db/unique :db.unique/value}})
                      [[:db/add 1 :name "Ivan"]
                       [:db/add 2 :name "Petr"]])]
    (are [tx] (thrown-with-msg? Throwable #"unique constraint" (d/db-with db tx))
      [[:db/add 3 :name "Ivan"]]
      [{:db/add 3 :name "Petr"}])
    (d/db-with db [[:db/add 3 :name "Igor"]])
    (d/db-with db [[:db/add 3 :nick "Ivan"]])))
