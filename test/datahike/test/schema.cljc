(ns datahike.test.schema
  (:require
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing]])
    [datahike.api :as d]
    [datahike.test.core :as tdc]))

(def test-uri "datahike:mem://schema-test")

(defn schema-test-fixture [f]
  (d/create-database test-uri)
  (f)
  (d/delete-database test-uri))

(deftest empty-db
  (let [conn (d/connect test-uri)
        db (d/db conn)]
    (is (= nil (:schema db)))))
