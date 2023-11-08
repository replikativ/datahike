(ns datahike.test.config-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
      :clj [clojure.test :as t :refer [is are deftest testing]])
   [datahike.config :as c]
   [datahike.db :as db]
   [datahike.api :as d]
   [datahike.index :as di]
   [datahike.index.hitchhiker-tree :as dih]
   [datahike.connector :as conn]))

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

(deftest store-identity-config-test
  (testing "different configs with equal identities"
    (let [mem-start  {:store {:backend :mem
                              :id "store-identity-test"}}
          mem-other  {:store {:backend :mem
                              :id "another-store-identity-test"}}
          file-start {:store {:backend :file
                              :path "/tmp/store-identity-test"}}
          file-index {:index :datahike.index/hitchhiker-tree
                      :store {:backend :file
                              :path "/tmp/store-identity-test"}}
          mem-named  {:name "has-name"
                      :store {:backend :mem
                              :id "store-identity-test"}}
          mem-same   {:store {:backend :mem
                              :id "store-identity-test"
                              :irrelevant-property true}}]
      (is (thrown-with-msg? Throwable
                            #"Configuration does not match stored configuration."
                            (conn/ensure-stored-config-consistency mem-start mem-other)))
      (is (not= mem-start mem-other))
      (is (thrown-with-msg? Throwable
                            #"Configuration does not match stored configuration."
                            (conn/ensure-stored-config-consistency file-start file-index)))
      (is (not= file-start file-index))
      (is (thrown-with-msg? Throwable
                            #"Configuration does not match stored configuration."
                            (conn/ensure-stored-config-consistency mem-start file-start)))
      (is (not= mem-start file-start))
      (is (nil? (conn/ensure-stored-config-consistency mem-start mem-named)))
      (is (not= mem-start mem-named))
      (is (nil? (conn/ensure-stored-config-consistency mem-start mem-same)))
      (is (not= mem-start mem-same))
      (is (nil? (conn/ensure-stored-config-consistency mem-named mem-same)))
      (is (not= mem-named mem-same)))))

(deftest store-identity-connection-test
  (testing "different connections with equal identities"
    (let [mem-start  {:store {:backend :mem
                              :id "store-connection-test"}}
          mem-other  {:store {:backend :mem
                              :id "another-store-connection-test"}}
          file-start {:store {:backend :file
                              :path "/tmp/store-connection-test"}}
          file-index {:index :datahike.index/hitchhiker-tree
                      :store {:backend :file
                              :path "/tmp/store-connection-test"}}
          mem-named  {:name "has-name"
                      :store {:backend :mem
                              :id "store-connection-test"}}
          mem-same   {:store {:backend :mem
                              :id "store-connection-test"
                              :irrelevant-property true}}
          _ (doall (map d/delete-database [mem-start mem-other file-start file-index mem-named mem-same]))]
      (d/create-database mem-start)
      (is (= (d/connect mem-start) (d/connect mem-named) (d/connect mem-same)))
      (d/create-database file-start)
      (d/connect file-start)
      (is (thrown-with-msg? Throwable
                            #"Configuration does not match existing connections."
                            (d/connect file-index))))))