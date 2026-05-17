(ns datahike.test.json-test
  "Unit tests for datahike.json — primarily the schema-aware tx-data
  coercion used by libdatahike's transact entry point (and any other
  caller that feeds JSON-parsed values into a strict-schema database).

  Without this coercion, JSON-parsed integers arrive as java.lang.Integer
  and fail schema validation for :db.type/long attributes. Regression
  tested end-to-end by pydatahike's test_transact_with_explicit_schema."
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest]]
      :clj [clojure.test :as t :refer [is deftest]])
   [datahike.api :as d]
   [datahike.json :as djson]))

(defn- with-schema-db [f]
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write}]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn [{:db/ident :age
                         :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/one}])
      (try
        (f @conn)
        (finally
          (d/release conn)
          (d/delete-database cfg))))))

(deftest xf-data-for-tx-coerces-integer-to-long
  (with-schema-db
    (fn [db]
      ;; Simulate JSON-parsed tx data: small numbers arrive as Integer.
      (let [tx-data [{:age (int 35)}]
            xformed (djson/xf-data-for-tx tx-data db)
            coerced-val (-> xformed first :age)]
        (is (instance? java.lang.Long coerced-val)
            (str ":age should be coerced from Integer to Long, got "
                 (class coerced-val)))
        (is (= 35 coerced-val))))))

(deftest xf-data-for-tx-leaves-non-schema-attrs-alone
  (with-schema-db
    (fn [db]
      ;; Attribute not in schema is passed through as-is (no coercion).
      (let [tx-data [{:unknown-attr (int 99)}]
            xformed (djson/xf-data-for-tx tx-data db)]
        (is (= [{:unknown-attr (int 99)}] (vec xformed)))))))
