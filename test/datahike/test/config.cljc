(ns datahike.test.config
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer [is are deftest testing use-fixtures]])
   [datahike.config :as c]
   [datahike.constants :as const]
   [datahike.test.core]
   [datahike.core :as d]))

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
                         :index-config {:index-b-factor       const/default-index-b-factor
                                        :index-log-size       const/default-index-log-size
                                        :index-data-node-size const/default-index-data-node-size}
                         :schema-flexibility :write
                         :cache-size 100000}]
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
      (is (= {:store {:backend :mem
                      :id "default"}
              :attribute-refs? false
              :keep-history? true
              :schema-flexibility :write
              :index :datahike.index/hitchhiker-tree
              :index-config       {:index-b-factor       const/default-index-b-factor
                                   :index-log-size       const/default-index-log-size
                                   :index-data-node-size const/default-index-data-node-size}
              :cache-size 100000}
             (-> config (dissoc :name)))))))

(deftest core-config-test
  (testing "Schema on write in core empty database"
    (is (thrown-msg?
         "Bad entity attribute :name at {:db/id 1, :name \"Ivan\"}, not defined in current schema"
         (d/db-with (d/empty-db nil {:schema-flexibility :write})
                    [{:db/id 1 :name "Ivan" :aka ["IV" "Terrible"]}
                     {:db/id 2 :name "Petr" :age 37 :huh? false}])))
    (is (thrown-msg?
         "Incomplete schema attributes, expected at least :db/valueType, :db/cardinality"
         (d/empty-db {:name {:db/cardinality :db.cardinality/one}} {:schema-flexibility :write})))
    (is (= #{["Alice"]}
           (let [db (-> (d/empty-db {:name {:db/cardinality :db.cardinality/one :db/valueType :db.type/string}} {:schema-flexibility :write})
                        (d/db-with  [{:name "Alice"}]))]
             (d/q '[:find ?n :where [_ :name ?n]] db))))
    (is (= #{["Alice"]}
           (let [db (-> (d/empty-db [{:db/ident :name :db/cardinality :db.cardinality/one :db/valueType :db.type/string}]
                                    {:schema-flexibility :write})
                        (d/db-with  [{:name "Alice"}]))]
             (d/q '[:find ?n :where [_ :name ?n]] db))))))
