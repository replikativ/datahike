(ns datahike.test.config
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer [is are deftest testing use-fixtures]])
   [datahike.config :refer :all]
   [datahike.test.core]
   [datahike.core :as d]))

(deftest int-from-env-test
  (is (= 1000
         (int-from-env :foo 1000))))

(deftest bool-from-env-test
  (is (bool-from-env :foo true)))

(deftest uri-test
  (let [mem-uri "datahike:mem://config-test"
        file-uri "datahike:file:///tmp/config-test"
        level-uri "datahike:level:///tmp/config-test"
        pg-uri "datahike:pg://alice:foo@localhost:5432/config-test"]

    (are [x y] (= x (uri->config y))
      {:backend :mem :host "config-test" :uri mem-uri}
      mem-uri

      {:backend :file :path "/tmp/config-test" :uri file-uri}
      file-uri

      {:backend :level :path "/tmp/config-test" :uri level-uri}
      level-uri

      {:backend :pg
       :host "localhost" :port 5432 :username "alice" :password "foo" :path "/config-test"
       :uri pg-uri}
      pg-uri)))

(deftest deprecated-test
  (let [mem-cfg {:backend :mem
                 :host "deprecated-test"}
        file-cfg {:backend :file
                  :path "/deprecated/test"}
        default-new-cfg {:keep-history? true
                         :initial-tx nil
                         :index :datahike.index/hitchhiker-tree
                         :schema-flexibility :write}]
    (is (= (merge default-new-cfg
                  {:store {:backend :mem :id "deprecated-test"}})
           (from-deprecated mem-cfg)))
    (is (= (merge default-new-cfg
                  {:store {:backend :file
                           :path "/deprecated/test"}})
           (from-deprecated file-cfg)))))

(deftest load-config-test
  (testing "configuration defaults"
    (let [{:keys [store name] :as config} (load-config)]
      (is (= {:store {:backend :mem
                      :id "default"}
              :keep-history? true
              :schema-flexibility :write
              :index :datahike.index/hitchhiker-tree}
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

