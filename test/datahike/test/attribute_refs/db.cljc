(ns datahike.test.attribute-refs.db
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj [clojure.test :as t :refer [is deftest testing]])
   [datahike.api :as d]))

(def ref-cfg
  {:store {:backend :mem :id "attr-refs-test.db"}
   :keep-history? true
   :attribute-refs? true
   :schema-flexibility :write
   :name "attr-refs-test"})

(def name-schema [{:db/ident :name
                   :db/cardinality :db.cardinality/one
                   :db/valueType :db.type/string}])

(defn setup-db [cfg]
  (d/delete-database cfg)
  (d/create-database cfg)
  (d/connect cfg))

(deftest test-partial-db
  (let [conn (setup-db ref-cfg)
        _tx0 (d/transact conn name-schema)
        tx1 (-> (d/transact conn [{:name "Peter"}]) :db-after :max-tx)
        tx2 (-> (d/transact conn [{:name "Maria"}]) :db-after :max-tx)]
    (testing "AsOfDB"
      (is (= #{["Peter"] ["Maria"]}
             (d/q '[:find ?v :where [?e :name ?v]]
                  (d/as-of @conn tx2))))
      (is (= #{["Peter"]}
             (d/q '[:find ?v :where [?e :name ?v]]
                  (d/as-of @conn tx1)))))
    (testing "SinceDB"
      (is (= #{["Maria"] ["Peter"]}
             (d/q '[:find ?v :where [?e :name ?v]]
                  (d/since @conn tx1))))
      (is (= #{["Maria"]}
             (d/q '[:find  ?v :where [?e :name ?v]]
                  (d/since @conn tx2)))))))
