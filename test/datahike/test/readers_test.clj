(ns datahike.test.readers-test
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.readers :as readers]
            [datahike.test.utils :as du]
            [datahike.transit :as transit]
            [datahike.writing :as dw]))

(def ^:private schema
  [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])

(defn- names [db]
  (set (d/q '[:find [?n ...] :where [_ :name ?n]] db)))

(defn- decode-counting-rebuilds
  "Decode `db` the way a server decodes a request argument. Returns
   [decoded-db number-of-stored->db-read-only-calls]."
  [db]
  (let [calls    (atom 0)
        original dw/stored->db-read-only]
    (with-redefs [dw/stored->db-read-only (fn [& args] (swap! calls inc) (apply original args))]
      [(readers/db-from-reader (transit/db->map db)) @calls])))

(deftest test-db-from-reader-head
  (let [conn (du/setup-db {:schema-flexibility :write :keep-history? true})]
    (try
      (d/transact conn schema)
      (d/transact conn [{:name "Alice"}])
      (testing "the head commit is not rebuilt from the store"
        (let [[decoded rebuilds] (decode-counting-rebuilds @conn)]
          (is (= 0 rebuilds))
          (is (= #{"Alice"} (names decoded)))))
      (testing "an older commit is rebuilt from the store and keeps its data"
        (let [old @conn]
          (d/transact conn [{:name "Bob"}])
          (let [[decoded rebuilds] (decode-counting-rebuilds old)]
            (is (= 1 rebuilds))
            (is (= #{"Alice"} (names decoded)))
            (is (= #{"Alice" "Bob"} (names @conn))))))
      (finally
        (d/release conn)))))
