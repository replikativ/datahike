(ns datahike.test.upsert-impl-test
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer        [is deftest testing]])
   [hitchhiker.tree.utils.async :as ha]
   [hitchhiker.tree :as tree]
   [hitchhiker.tree.messaging :as msg]
   [datahike.index.hitchhiker-tree.upsert :as htu]
   [datahike.test.utils :refer [setup-db]]
   [datahike.constants :as const]
   [datahike.db :as db]
   [datahike.api :as d]))

#?(:cljs
   (def Throwable js/Error))

(defn upsert-helper
  [t k]
  (ha/<?? (msg/enqueue t [(htu/new-UpsertOp k 0 [0 1])])))

(deftest hh-tree-upsert
  (let [new-tree (tree/b-tree (tree/->Config 3 3 2))
        projected-vec [4 :name "Marcel" 1]
        tree (reduce upsert-helper (ha/<?? new-tree)
                     (into (sorted-set)
                           #{[1 :at "home" 1]
                             [1 :age 44 1]
                             [1 :name "Petr" 1]
                             [2 :age 25 1]
                             [2 :name "Ivan" 1]
                             [3 :age 11 1]
                             projected-vec}))]
    (testing "Against an entry in the projection area,"
      (testing "we are in a projection"
        (is (= (:key (first (:op-buf tree))) projected-vec)))
      (testing "basic lookup works"
        (is (= [[1 :age 44 1] nil] (first (msg/lookup-fwd-iter tree [1 :age 44 1])))))
      (testing "a totally new entry is persisted"
        (let [new [5 :name "Jo" 3]
              tree-after (upsert-helper tree new)]
          ;; Is new in?
          (is (= [new nil] (first (msg/lookup-fwd-iter tree-after new))))))
      (testing "new = old"
        (let [new [4 :name "Marcel" 2]
              tree-after (upsert-helper tree new)]
          (is (= [new nil] (first (msg/lookup-fwd-iter tree-after new))))))
      (testing "old v <> new v,"
        (testing "not keeping history"
          (let [new [4 :name "New-Name" 2]
                tree-after (upsert-helper tree new)]
            ;; Is new in?
            (is (= [new nil] (first (msg/lookup-fwd-iter tree-after new))))
            ;; Is old deleted?
            (is (= nil (msg/lookup tree-after projected-vec)))))))

    (testing "Against an entry located at leaf level,"
      (testing "basic lookup works"
        (is (= [[1 :age 44 1] nil] (first (msg/lookup-fwd-iter tree [1 :age 44 1])))))
      (testing "re-inserting works"
        (let [new [1 :age 44 1]
              tree-after (upsert-helper tree new)]
          (is (= [new nil] (first (msg/lookup-fwd-iter tree-after new))))))
      (testing "old v <> new v"
        (testing "not keeping history"
          (let [new [1 :age 40 5]
                tree-after (upsert-helper tree new)]
            ;; Is new in?
            (is (= [new nil] (first (msg/lookup-fwd-iter tree-after new))))
            ;; Is old deleted?
            (is (= nil (msg/lookup tree-after [1 :age 44 1]))))))))

  (testing "when it overflows"
    ;; I.e. testing the projection of a deferred-op sitting on an index-node.
    (let [new-tree (tree/b-tree (tree/->Config 3 3 2))
          tree (reduce upsert-helper (ha/<?? new-tree)
                       (into (sorted-set)
                             #{[1 :age 44 1]
                               [1 :name "Petr" 1]
                               [2 :age 25 1]
                               [2 :name "Ivan" 1]
                               [3 :age 11 1]
                               [4 :age 12 1]
                               [4 :age 20 1]
                               [4 :name "Paulo" 1]
                               [4 :age 40 1]}))] ;; triggers the overflow

      (is (= nil (msg/lookup tree [4 :age 12 1])))
      (is (= nil (msg/lookup tree [4 :age 20 1])))
      (is (= [[1 :age 44 1] nil] (first (msg/lookup-fwd-iter tree [1 :age 44 1])))))))

(defn connect []
  (let [cfg  {:schema-flexibility :read
              :initial-tx         []}
        _    (d/delete-database cfg)
        _    (d/create-database cfg)]
    (d/connect cfg)))

(deftest datahike-upsert
  (testing "IndexNode"
    (let [txs  (vec (for [i (range 1000)]
                      {:name (str "Peter" i)
                       :age  i}))]
      (is (d/transact (connect) txs))))

  (testing "simple upsert and history"
    (let [txs  [[:db/add 199 :name "Peter"]
                [:db/add 199 :name "Ptr"]]
          conn (connect)]
      (is (d/transact conn txs))
      (is (not (d/datoms @conn :eavt 199 :name "Peter"))) ;; no history
      (is (d/datoms (d/history @conn) :eavt 199 :name "Peter"))))

  (testing "transacting the same datoms twice should work with :avet"
    (let [dvec #(vector (:e %) (:a %) (:v %))
          txs [[:db/add 1 :age 44]
               [:db/add 2 :age 25]
               [:db/add 3 :age 11]]
          db (-> (db/empty-db {:age {:db/index true}})
                 (d/db-with txs)
                 ;; transacting again
                 (d/db-with txs))]
      (is (= [[3 :age 11]
              [2 :age 25]
              [1 :age 44]]
             (map dvec (d/datoms db :avet)))))))

(deftest temporal-upsert
  (let [schema [{:db/ident       :name
                 :db/valueType   :db.type/string
                 :db/unique      :db.unique/identity
                 :db/index       true
                 :db/cardinality :db.cardinality/one}
                {:db/ident       :age
                 :db/valueType   :db.type/long
                 :db/cardinality :db.cardinality/one}]
        initial-tx (conj schema
                         {:name "Alice"
                          :age  25}
                         {:name "Bob"
                          :age  35})
        cfg {:store {:backend :mem
                     :id "test-upsert-history"}
             :schema-flexibility :read
             :initial-tx initial-tx}
        conn (setup-db cfg)
        query '[:find ?a ?t ?op
                :where
                [?e :name "Alice"]
                [?e :age ?a ?t ?op]]]
    (testing "when multiple transactions"
      (d/transact conn [[:db/add [:name "Alice"] :age 20]
                        [:db/add [:name "Bob"] :age 15]])
      (d/transact conn [[:db/add [:name "Alice"] :age 10]])
      (d/transact conn [[:db/add [:name "Alice"] :age 1]])
      (is (= #{[25 (+ const/tx0 1) true]
               [25 (+ const/tx0 2) false]
               [20 (+ const/tx0 2) true]
               [20 (+ const/tx0 3) false]
               [10 (+ const/tx0 3) true]
               [10 (+ const/tx0 4) false]
               [1 (+ const/tx0 4) true]}
             (d/q query (d/history @conn)))))
    (testing "when only one transaction"
      (let [conn (setup-db cfg)]
        (d/transact conn [[:db/add [:name "Alice"] :age 20]
                          [:db/add [:name "Bob"] :age 15]
                          [:db/add [:name "Alice"] :age 10]
                          [:db/add [:name "Alice"] :age 1]])
        (is (= #{[25 (+ const/tx0 1) true]
                 [25 (+ const/tx0 2) false]
                 [20 (+ const/tx0 2) true]
                 [20 (+ const/tx0 2) false]
                 [10 (+ const/tx0 2) true]
                 [10 (+ const/tx0 2) false]
                 [1 (+ const/tx0 2) true]}
               (d/q query (d/history @conn))))))))

(deftest upsert-read-handlers
  (let [config {:store {:backend :file :path "/tmp/upsert-read-handlers"}
                :schema-flexibility :write
                :keep-history? false}
        schema [{:db/ident       :block/string
                 :db/valueType   :db.type/string
                 :db/cardinality :db.cardinality/one}
                {:db/ident       :block/children
                 :db/valueType   :db.type/ref
                 :db/index       true
                 :db/cardinality :db.cardinality/one}]
        _      (d/create-database config)
        conn   (d/connect config)]
    (d/transact conn schema)
    (d/transact conn (vec (for [i (range 1000)]
                            {:db/id (inc i) :block/children (inc i)})))
    (d/release conn)

    (let [conn (d/connect config)]
      ;; Would fail if upsert read handlers are not present
      (is (d/datoms @conn :eavt)))

    (d/delete-database config)))
