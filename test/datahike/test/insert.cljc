(ns datahike.test.insert
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer        [is deftest testing]])
   [datahike.api :as d]
   [datahike.datom :as datom]))

#?(:cljs
   (def Throwable js/Error))

;; Test that the second insertion of the same datom does not replace the initial one.
;; That is similar to Datomic's behaviour.
;; Note that the 'mem-set' backend does not have this semantics though.

(defn duplicate-test [config test-tx-id?]
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
        _     (d/transact conn [{:db/id 502 :block/children 501}]) ;; First transacton
        datom-1 (first (d/datoms @conn :eavt 502 :block/children))
        _     (d/transact conn [{:db/id 502 :block/children 501}]) ;; Second transaction
        datom-2 (first (d/datoms @conn :eavt 502 :block/children))
        aevt  (d/datoms @conn :aevt :block/children 502)
        avet  (d/datoms @conn :avet :block/children 501)]
    (when test-tx-id?
      (is (= (.-tx datom-1) (.-tx datom-2))))
    (is (= datom-1 datom-2))

    (is (= 1 (count aevt)))
    (is (= datom-1 (first aevt)))

    (is (= 1 (count avet)))
    (is (= datom-1 (first avet)))

    (d/release conn)
    (d/delete-database config)))

(deftest mem-set
  (let [config {:store {:backend :mem :id "performance-set"}
                :schema-flexibility :write
                :keep-history? true
                :index :datahike.index/persistent-set}
        test-tx-id? false]
    (duplicate-test config test-tx-id?)))

(deftest mem-hht
  (let [config {:store {:backend :mem :id "performance-hht"}
                :schema-flexibility :write
                :keep-history? true
                :index :datahike.index/hitchhiker-tree}
        test-tx-id? true]
    (duplicate-test config test-tx-id?)))

(deftest file
  (let [config {:store {:backend :file :path "/tmp/performance-hht"}
                :schema-flexibility :write
                :keep-history? true
                :index :datahike.index/hitchhiker-tree}
        test-tx-id? true]
    (duplicate-test config test-tx-id?)))

(defn insert-history-test [cfg]
  (let [schema [{:db/ident       :name
                 :db/cardinality :db.cardinality/one
                 :db/index       true
                 :db/unique      :db.unique/identity
                 :db/valueType   :db.type/string}
                {:db/ident       :email
                 :db/cardinality :db.cardinality/many
                 :db/valueType   :db.type/string}]
        _ (d/delete-database cfg)
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (testing "inserting a new datom creates an entry in history"
      (d/transact conn {:tx-data schema})
      (d/transact conn {:tx-data [{:name "Alice"
                                   :email  "al@al.com"}]})
      (is (= 1 (count (d/datoms (d/history @conn) :eavt [:name "Alice"] :email)))))
    (testing "inserting the exact same datom does not change history"
      (d/transact conn {:tx-data [{:db/id [:name "Alice"]
                                   :email "al@al.com"}]})
      (is (= 1 (count (d/datoms (d/history @conn) :eavt [:name "Alice"] :email)))))
    (testing "changing the datom value just add the new datom to history"
      (d/transact conn {:tx-data [{:db/id [:name "Alice"]
                                   :email "al@eco.com"}]})
      (is (= 2 (count (d/datoms (d/history @conn) :eavt [:name "Alice"] :email)))))))

(deftest insert-history-mem-hht
  (let [config {:store {:backend :mem :id "temp-hist-hht"}
                :schema-flexibility :write
                :keep-history? true
                :index :datahike.index/hitchhiker-tree}]
    (insert-history-test config)))

(deftest insert-history-file
  (let [config {:store {:backend :file :path "/tmp/temp-hist-hht"}
                :schema-flexibility :write
                :keep-history? true
                :index :datahike.index/hitchhiker-tree}]
    (insert-history-test config)))

(deftest insert-history-file-with-attr-refs
  (let [config {:store {:backend :file :path "/tmp/temp-hist-attr-refs"}
                :schema-flexibility :write
                :keep-history? true
                :attribute-refs? true
                :index :datahike.index/hitchhiker-tree}]
    (insert-history-test config)))

(deftest insert-history-mem-set
  (let [config {:store {:backend :mem :id "temp-hist-set"}
                :schema-flexibility :write
                :keep-history? true
                :index :datahike.index/persistent-set}]
    (insert-history-test config)))

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
