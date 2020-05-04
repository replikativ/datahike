(ns datahike.test.store
  (:require
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing]])
    [datahike.api :as d]
    [datahike.config :as c]))

(defn test-store [uri]
  (let [_ (d/delete-database uri)]
    (is (not (d/database-exists? uri)))
    (let [db (d/create-database uri :schema-on-read true)
          conn (d/connect uri)]
      (d/transact conn [{ :db/id 1, :name  "Ivan", :age   15 }
                        { :db/id 2, :name  "Petr", :age   37 }
                        { :db/id 3, :name  "Ivan", :age   37 }
                        { :db/id 4, :age 15 }])
      (is (= (d/q '[:find ?e :where [?e :name]] @conn)
             #{[3] [2] [1]}))

      (d/release conn)
      (is (d/database-exists? uri)))))


(deftest test-db-file-store
  (test-store "datahike:file:///tmp/api-fs"))

(deftest test-db-mem-store
  (test-store "datahike:mem:///test-mem"))


(deftest test-persistent-set-index
  (c/reload-config {:store {:backend :mem :host "/test-persistent-set"}
                    :schema-on-read true
                    :temporal-index false
                    :index :datahike.index/persistent-set})
  (d/delete-database)
  (d/create-database)
  (let [conn (d/connect)]
    (d/transact conn [{:db/id 1, :name "Alice"}])
    (is (= me.tonsky.persistent_sorted_set.PersistentSortedSet
           (-> @conn :eavt type)))))

(deftest test-hitchhiker-tree-index
  (c/reload-config {:store {:backend :mem :host "/test-hitchhiker-tree"}
                    :schema-on-read true
                    :temporal-index false
                    :index :datahike.index/hitchhiker-tree})
  (d/delete-database)
  (d/create-database)
  (let [conn (d/connect)]
    (d/transact conn [{:db/id 1, :name "Alice"}])
    (is (= hitchhiker.tree.DataNode
           (-> @conn :eavt type)))))
