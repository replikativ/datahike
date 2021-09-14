(ns datahike.test.insert
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is deftest]]
      :clj  [clojure.test :as t :refer        [is deftest]])
   [datahike.api :as d]
   [datahike.datom :as datom]))

#?(:cljs
   (def Throwable js/Error))

(defn duplicate-test [config]
  (let [_      (d/create-database config)
        conn   (d/connect config)
        schema [{:db/ident       :block/string
                 :db/valueType   :db.type/string
                 :db/cardinality :db.cardinality/one}
                {:db/ident       :block/children
                 :db/valueType   :db.type/ref
                 :db/index       true
                 :db/cardinality :db.cardinality/many}]
        _     (d/transact conn schema)
        _     (d/transact conn [{:db/id 501 :block/string "one"}])
        _     (d/transact conn [{:db/id 502 :block/children 501}])
        expected (d/datoms @conn :eavt 502 :block/children)
        tx-1   (.-tx (first expected))
        _ (d/transact conn [{:db/id 502 :block/children 501}])
        tx-2   (.-tx (first (d/datoms @conn :eavt 502 :block/children)))
        aevt (d/datoms @conn :aevt :block/children 502)
        avet (d/datoms @conn :avet :block/children 501)]

    (is (= tx-1 tx-2))
    (is (= 1 (count expected)))

    (is (= 1 (count aevt)))
    (is (datom/cmp-datoms-eavt expected (first aevt)))

    ;; (is (= 1 (count avet)))
    ;; (is (= expected (first avet)))

    (d/release conn)
    (d/delete-database config)))

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

(deftest insert-read-handlers
  (let [config {:store {:backend :file :path "/tmp/insert-read-handlers-9"}
                :schema-flexibility :write
                :keep-history? false
                :index :datahike.index/hitchhiker-tree}
        schema [{:db/ident       :block/string
                 :db/valueType   :db.type/string
                 :db/cardinality :db.cardinality/one}
                {:db/ident       :block/children
                 :db/valueType   :db.type/ref
                 :db/index       true
                 :db/cardinality :db.cardinality/many}]
        _      (d/create-database config)
        conn   (d/connect config)]
    (d/transact conn schema)
    (d/transact conn (vec (for [i (range 1000)]
                            {:db/id (inc i) :block/children (inc i)})))
    (d/release conn)

    (let [conn (d/connect config)]
      ;; Would fail if insert read handlers are not present
      (is (d/datoms @conn :eavt)))

    (d/delete-database config)))
