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

(comment

  (defn upsert-helper
    [t k]
    (ha/<?? (msg/upsert t (ht/new-UpsertOp k k))))


  (def tree (tree/b-tree (tree/->Config 3 3 2)))

  (:op-buf
   (reduce upsert-helper (ha/<?? tree) (into (sorted-set) #{1 2 3 4 5 6 7}))
   )

  (msg/lookup
   (reduce upsert-helper (ha/<?? tree) (into (sorted-set) #{1 2 3 4 5 6 7}))
   7)

  ;;;;;;
  (reduce upsert-helper (ha/<?? tree) (into (sorted-set)
                                        #{[1 :age 44]
                                          [1 :name "Petr"]
                                          [2 :age 25]
                                          [2 :name "Ivan"]
                                          [3 :age 11]
                                          [3 :name "Sergey"]
                                          [4 :name "Marcel"]}))
  )

(defn upsert-helper
  [t k]
  ;; TODO: why does it not work when we set the value to insert to nil
  (ha/<?? (msg/upsert t (ht/new-UpsertOp k k))))

(deftest in-projections
  (let [new-tree (tree/b-tree (tree/->Config 3 3 2))
        projected-vec [4 :name "Marcel" 1]
        tree (reduce upsert-helper (ha/<?? new-tree)
               (into (sorted-set)
                 #{[1 :age 44 1]
                   [1 :name "Petr" 1]
                   [2 :age 25 1]
                   [2 :name "Ivan" 1]
                   [3 :age 11 1]
                   [3 :name "Sergey" 1]
                   projected-vec}))]
    (testing "we are in a projection"
      (is (= (:key (first (:op-buf tree))) projected-vec)))
    (testing "basic lookup works"
      (is (= [1 :age 44 1]
            (msg/lookup tree [1 :age 44 1]))))
    (testing "a totally new entry is persisted"
      (let [new [5 :name "Jo" 3]
              tree-after (upsert-helper tree new)]
          ;; Is new in?
          (is (= new (msg/lookup tree-after new)))))
    (testing "old v <> new v"
      (testing "not keeping history"
        (let [new [4 :name "New-Name" 2]
              tree-after (upsert-helper tree new)]
          ;; Is new in?
          (is (= new (msg/lookup tree-after new)))
          ;; Is old deleted?
          (is (= nil (msg/lookup tree-after projected-vec))))))))
