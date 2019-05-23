(ns datahike.test.schema
  (:require [clojure.test :as t :refer [is are deftest testing]]
            [datahike.core :as c]
            [datahike.schema :as s]))

(defn now []
  (.getTime (java.util.Date.)))

(deftest test-schema-tx
  (let [db (c/empty-db)
        schema (get-in db [:schema :db.part/db])]
    (testing "db schema values"
      (testing ":db/ident"
        (are [valid? v] (= valid? (s/schema-val-valid? [1 :db/ident v (now)] schema))
          true :name
          true :user/name
          false "name"
          false "user/name"
          false {}))
      (testing ":db/valueType"
        (are [valid? v] (= valid? (s/schema-val-valid? [1 :db/valueType v (now)] schema))
          true :db.type/string
          true :db.type/long
          false "string"
          false String
          false nil))
      (testing ":db/cardinality"
        (are [valid? v] (= valid? (s/schema-val-valid? [1 :db/cardinality v (now)] schema))
          true :db.cardinality/many
          true :db.cardinality/one
          false "many"
          false :many
          false nil))
      (testing ":db/unique"
        (are [valid? v] (= valid? (s/schema-val-valid? [1 :db/unique v (now)] schema))
          true :db.unique/value
          true :db.unique/identity
          false true
          false "value"
          false nil))
      (testing ":db/index"
        (are [valid? v] (= valid? (s/schema-val-valid? [1 :db/index v (now)] schema))
          true true
          true false
          false :true
          false "false"
          false nil
          false 0))
      (testing ":db.install/_attribute"
        (are [valid? v] (= valid? (s/schema-val-valid? [1 :db.install/_attribute v (now)] schema))
          true :db.part/db
          true :db.part/user
          true :db.part/tx
          false "db"
          false :db
          false nil)))))
