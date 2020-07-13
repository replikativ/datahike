(ns datahike.test.upsert-implem-test
  (:require
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing]])
    [hitchhiker.tree.utils.async :as ha]
    [hitchhiker.tree :as tree]
    [hitchhiker.tree.messaging :as msg]
    [datahike.index.hitchhiker-tree :as ht]))

#?(:cljs
   (def Throwable js/Error))


(defn upsert-helper
  [t k]
  (ha/<?? (msg/upsert t (ht/new-UpsertOp k k :eavt nil))))


(deftest upsert
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
        (is (= [1 :age 44 1] (msg/lookup tree [1 :age 44 1]))))
      (testing "a totally new entry is persisted"
        (let [new [5 :name "Jo" 3]
              tree-after (upsert-helper tree new)]
          ;; Is new in?
          (is (= new (msg/lookup tree-after new)))))
      (testing "new = old"
        (let [new [4 :name "Marcel" 2]
              tree-after (upsert-helper tree new)]
          (is (= new (msg/lookup tree-after new)))))
      (testing "old v <> new v,"
        (testing "not keeping history"
          (let [new [4 :name "New-Name" 2]
                tree-after (upsert-helper tree new)]
            ;; Is new in?
            (is (= new (msg/lookup tree-after new)))
            ;; Is old deleted?
            (is (= nil (msg/lookup tree-after projected-vec)))))))

    (testing "Against an entry located at leaf level,"
      (testing "basic lookup works"
        (is (= [1 :age 44 1] (msg/lookup tree [1 :age 44 1]))))
      (testing "re-inserting works"
        (let [new [1 :age 44 1]
              tree-after (upsert-helper tree new)]
          (is (= new (msg/lookup tree-after new)))))
      (testing "old v <> new v"
        (testing "not keeping history"
          (let [new [1 :age 40 5]
                tree-after (upsert-helper tree new)]
            ;; Is new in?
            (is (= new (msg/lookup tree-after new)))
            ;; Is old deleted?
            (is (= nil (msg/lookup tree-after [1 :age 44 1])))))
        )))

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
                     [4 :age 40 1] ;; triggers the overflow
                     }))]
      (is (= nil           (msg/lookup tree [4 :age 12 1])))
      (is (= nil           (msg/lookup tree [4 :age 20 1])))
      (is (= [4 :age 40 1] (msg/lookup tree [4 :age 40 1]))))))
