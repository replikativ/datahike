(ns datahike.test.store
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

(deftest test-persistent-set-index
  (let [config {:store {:backend :mem
                        :id "test-persistent-set"}
                :schema-flexibility :read
                :keep-history? false
                :index :datahike.index/persistent-set}]
    (d/delete-database config)
    (d/create-database config)
    (let [conn (d/connect config)]
      (d/transact conn [{:db/id 1, :name "Alice"}])
      (is (= me.tonsky.persistent_sorted_set.PersistentSortedSet
             (-> @conn :eavt type))))))

(deftest test-hitchhiker-tree-index
  (let [config {:store {:backend :mem
                        :id "test-hitchhiker-tree"}
                :schema-flexibility :read
                :keep-history? false
                :index :datahike.index/hitchhiker-tree}]
    (d/delete-database config)
    (d/create-database config)
    (let [conn (d/connect config)]
      (d/transact conn [{:db/id 1, :name "Alice"}])
      (is (= hitchhiker.tree.DataNode
             (-> @conn :eavt type))))))
