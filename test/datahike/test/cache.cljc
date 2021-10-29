(ns datahike.test.cache
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer [is are deftest testing use-fixtures]])
   [datahike.api :as d]))

(defn setup-db [cfg]
  (d/delete-database cfg)
  (d/create-database cfg)
  (d/connect cfg))

(deftest test-history-cache-miss
  (let [cfg {:store              {:backend :mem
                                  :id      "cache-test"}
             :name               "cache-test"
             :keep-history?      true
             :schema-flexibility :write
             :cache-size         2
             :attribute-refs?    true}
        conn (setup-db cfg)
        schema [{:db/ident       :name
                 :db/cardinality :db.cardinality/one
                 :db/index       true
                 :db/unique      :db.unique/identity
                 :db/valueType   :db.type/string}]]
    (d/transact conn {:tx-data schema})

    (d/transact conn {:tx-data [{:name "Alice"}
                                {:name "Bob"}]})

    (d/transact conn [[:db/retractEntity [:name "Alice"]]])
    (is (= #{["Bob" true] ["Alice" false] ["Alice" true]}
           (d/q {:query '[:find ?n ?op
                          :where
                          [?e :name ?n _ ?op]]
                 :args  [(d/history @conn)]})))))

