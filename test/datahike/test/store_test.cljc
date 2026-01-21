(ns datahike.test.store-test
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer        [is deftest testing]])
   [datahike.api :as d])
  (:import [java.lang System]))

(defn test-store [cfg]
  (let [_ (d/delete-database cfg)]
    (is (not (d/database-exists? cfg)))
    (let [cfg (merge cfg {:schema-flexibility :read})
          _ (d/create-database cfg)
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
  (test-store {:store {:backend :file
                       :path (case (System/getProperty "os.name")
                               "Windows 10" (str (System/getProperty "java.io.tmpdir") "api-fs")
                               "/tmp/api-fs")
                       :id #uuid "f11e0000-0000-0000-0000-00000000000f"}}))

(deftest test-db-mem-store
  (test-store {:store {:backend :memory :id #uuid "e0000000-0000-0000-0000-00000000000e"}}))

(deftest test-index
  (let [config {:store {:backend :memory
                        :id #uuid "f0000000-0000-0000-0000-00000000000f"}
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
        (is (= "Paula" (:name (d/entity @conn 1)))))
      (d/release conn))))

(deftest test-binary-support
  (let [config {:store {:backend :memory
                        :id #uuid "00100000-0000-0000-0000-000000000010"}
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
                  (byte-array [0 2 3]))))
      (d/release conn))))

(deftest test-database-exists-with-invalid-store
  (testing "Store with missing :id"
    (is (thrown-with-msg? #?(:clj Exception :cljs js/Error)
                          #"\s+:id\s+"
                          (d/database-exists?
                           {:store {:backend :memory}})))))
