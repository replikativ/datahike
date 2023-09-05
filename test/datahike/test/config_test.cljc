(ns datahike.test.config-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
      :clj [clojure.test :as t :refer [is are deftest testing]])
   [datahike.config :as c]
   [datahike.db :as db]
   [datahike.api :as d]
   [datahike.index :as di]
   [datahike.index.hitchhiker-tree :as dih]))

#?(:cljs (def Throwable js/Error))

(deftest int-from-env-test
  (is (= 1000
         (c/int-from-env :foo 1000))))

(deftest bool-from-env-test
  (is (c/bool-from-env :foo true)))

(deftest uri-test
  (let [mem-uri "datahike:mem://config-test"
        file-uri "datahike:file:///tmp/config-test"]

    (are [x y] (= x (c/uri->config y))
      {:backend :mem :host "config-test" :uri mem-uri}
      mem-uri

      {:backend :file :path "/tmp/config-test" :uri file-uri}
      file-uri)))

(deftest deprecated-test
  (let [mem-cfg {:backend :mem
                 :host "deprecated-test"}
        file-cfg {:backend :file
                  :path "/deprecated/test"}
        default-new-cfg {:attribute-refs? false
                         :keep-history? true
                         :initial-tx nil
                         :index :datahike.index/hitchhiker-tree
                         :index-config {:index-b-factor       dih/default-index-b-factor
                                        :index-log-size       dih/default-index-log-size
                                        :index-data-node-size dih/default-index-data-node-size}
                         :schema-flexibility :write
                         :crypto-hash? false
                         :branch :db
                         :writer c/self-writer
                         :search-cache-size c/*default-search-cache-size*
                         :store-cache-size c/*default-store-cache-size*}]
    (is (= (merge default-new-cfg
                  {:store {:backend :mem :id "deprecated-test"}})
           (c/from-deprecated mem-cfg)))
    (is (= (merge default-new-cfg
                  {:store {:backend :file
                           :path "/deprecated/test"}})
           (c/from-deprecated file-cfg)))))

(deftest load-config-test
  (testing "configuration defaults"
    (let [config (c/load-config)]
      (is (= (merge {:store {:backend :mem}
                     :attribute-refs? c/*default-attribute-refs?*
                     :keep-history? c/*default-keep-history?*
                     :schema-flexibility c/*default-schema-flexibility*
                     :index c/*default-index*
                     :crypto-hash? c/*default-crypto-hash?*
                     :branch c/*default-db-branch*
                     :writer c/self-writer
                     :search-cache-size c/*default-search-cache-size*
                     :store-cache-size c/*default-store-cache-size*}
                    (when (seq (di/default-index-config c/*default-index*))
                      {:index-config (di/default-index-config c/*default-index*)}))
             (update config :store dissoc :id :scope))))))

(deftest core-config-test
  (testing "Schema on write in core empty database"
    (is (thrown-with-msg? Throwable
                          #"Bad entity attribute :name at \{:db/id 1, :name \"Ivan\"\}, not defined in current schema"
                          (d/db-with (db/empty-db nil {:schema-flexibility :write})
                                     [{:db/id 1 :name "Ivan" :aka ["IV" "Terrible"]}
                                      {:db/id 2 :name "Petr" :age 37 :huh? false}])))
    (is (thrown-with-msg? Throwable
                          #"Incomplete schema attributes, expected at least :db/valueType, :db/cardinality"
                          (db/empty-db {:name {:db/cardinality :db.cardinality/one}} {:schema-flexibility :write})))
    (is (= #{["Alice"]}
           (let [db (-> (db/empty-db {:name {:db/cardinality :db.cardinality/one :db/valueType :db.type/string}} {:schema-flexibility :write})
                        (d/db-with  [{:name "Alice"}]))]
             (d/q '[:find ?n :where [_ :name ?n]] db))))
    (is (= #{["Alice"]}
           (let [db (-> (db/empty-db [{:db/ident :name :db/cardinality :db.cardinality/one :db/valueType :db.type/string}]
                                     {:schema-flexibility :write})
                        (d/db-with  [{:name "Alice"}]))]
             (d/q '[:find ?n :where [_ :name ?n]] db))))))
