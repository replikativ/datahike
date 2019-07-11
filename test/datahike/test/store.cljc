(ns datahike.test.store
  (:require
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing]])
    [datahike.api :as d]))

(defn test-store [uri]
  (let [_ (d/delete-database uri)
        db (d/create-database {:uri uri
                               :schema-on-read true})
        conn (d/connect uri)]
    @(d/transact conn [{ :db/id 1, :name  "Ivan", :age   15 }
                       { :db/id 2, :name  "Petr", :age   37 }
                       { :db/id 3, :name  "Ivan", :age   37 }
                       { :db/id 4, :age 15 }])
    (is (= (d/q '[:find ?e :where [?e :name]] @conn)
           #{[3] [2] [1]}))

    (d/release conn)))

(deftest test-db-file-store
  (test-store "datahike:file:///tmp/api-fs"))

(deftest test-db-level-store
  (test-store "datahike:level:///tmp/api-leveldb"))

(deftest test-db-mem-store
  (test-store "datahike:mem:///test-mem"))
