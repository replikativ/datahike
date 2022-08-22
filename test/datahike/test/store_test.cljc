(ns datahike.test.store-test
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [datahike.api :as d])
  (:import [java.lang System]))

(defn test-store [cfg]
  (let [_ (d/delete-database cfg)]
    (is (not (d/database-exists? cfg)))
    (let [_ (d/create-database (merge cfg {:schema-flexibility :read}))
          conn (d/connect cfg)]
      (d/transact conn [{:db/id 1, :name  "Ivan", :age   15}
                        {:db/id 2, :name  "Petr", :age   37}
                        {:db/id 3, :name  "Ivan", :age   37}
                        {:db/id 4, :age 15}])
      (is (= (d/q '[:find ?e :where [?e :name]] @conn)
             #{[3] [2] [1]}))

      (d/release conn)
      (is (d/database-exists? cfg)))))

(deftest test-db-file-store
  (test-store {:store {:backend :file :path (case (System/getProperty "os.name")
                                              "Windows 10" (str (System/getProperty "java.io.tmpdir") "api-fs")
                                              "/tmp/api-fs")}}))

(deftest test-db-mem-store
  (test-store {:store {:backend :mem :id "test-mem"}}))

(deftest test-index
  (let [config {:store {:backend :mem
                        :id "test-index"}
                :schema-flexibility :read
                :keep-history? false}]
    (d/delete-database config)
    (d/create-database config)
    (let [conn (d/connect config)]
      (testing "root node type"
        (d/transact conn [{:db/id 1, :name "Alice"}])
        (is (= (if (= :datahike.index/persistent-set (-> @conn :config :index))
                 me.tonsky.persistent_sorted_set.PersistentSortedSet
                 hitchhiker.tree.DataNode)
               (-> @conn :eavt type))))
      (testing "upsert"
        (d/transact conn [{:db/id 1, :name "Paula"}])
        (is (= "Paula" (:name (d/entity @conn 1))))))))

(deftest test-binary-support
  (let [config {:store {:backend :mem
                        :id "test-binary-support"}
                :schema-flexibility :read
                :keep-history? false}]
    (d/delete-database config)
    (d/create-database config)
    (let [conn (d/connect config)]
      (d/transact conn [{:db/id 1, :name "Jiayi", :payload (byte-array [0 2 3])}
                        {:db/id 2, :name "Peter", :payload (byte-array [1 2 3])}])
      (is (= "Jiayi"
             (d/q '[:find ?n .
                    :in $ ?arr
                    :where
                    [?e :payload ?arr]
                    [?e :name ?n]]
                  @conn
                  (byte-array [0 2 3])))))))
