(ns datahike.test.insert
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is deftest]]
      :clj  [clojure.test :as t :refer        [is deftest]])
   [datahike.api :as d]
   [datahike.datom :as datom]))

#?(:cljs
   (def Throwable js/Error))


(defn duplicate-test [config]
  (let [expected (datom/datom 502 :block/children 501 536870915)
        _      (d/create-database config)
        conn   (d/connect config)
        schema [{:db/ident       :block/string
                 :db/valueType   :db.type/string
                 :db/cardinality :db.cardinality/one}
                {:db/ident       :block/children
                 :db/valueType   :db.type/ref
                 :db/index       true
                 :db/cardinality :db.cardinality/many}]]
    (d/transact conn schema)
    (d/transact conn [{:db/id 501 :block/string "one"}])
    (d/transact conn [{:db/id 502 :block/children 501}])
    (d/transact conn [{:db/id 502 :block/children 501}])

    (let [eavt (d/datoms @conn :eavt 502 :block/children)
          aevt (d/datoms @conn :aevt :block/children 502)
          avet (d/datoms @conn :avet :block/children 501)]
      (is (= 1 (count eavt)))
      (is (= expected (first eavt)))

      (is (= 1 (count aevt)))
      (is (= expected (first aevt)))

      (is (= 1 (count avet)))
      (is (= expected (first avet)))

      (d/release conn)
      (d/delete-database config))))


(deftest mem-set
  (let [config {:store {:backend :mem :id "performance-set"}
                :schema-flexibility :write
                :keep-history? true
                :index :datahike.index/persistent-set}]
    (duplicate-test config)))

(deftest mem-hht
  (let [config {:store {:backend :mem :id "performance-hht"}
                :schema-flexibility :write
                :keep-history? true
                :index :datahike.index/hitchhiker-tree}]
    (duplicate-test config)))

(deftest file
  (let [config {:store {:backend :file :path "/tmp/performance-hht"}
                :schema-flexibility :write
                :keep-history? true
                :index :datahike.index/hitchhiker-tree}]
    (duplicate-test config)))
