(ns datahike.test.query-join-resource-test
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.query.relation :as rel]
            [datahike.query.cancel :as cancel]
            [datahike.query.resource :as resource]
            [org.replikativ.persistent-sorted-set.arrays :as da]))

(defn- relation [attrs tuples]
  (rel/->Relation (zipmap attrs (range)) (mapv to-array tuples)))

(defn- rows [relation]
  (let [indices (mapv (:attrs relation) (sort-by str (keys (:attrs relation))))]
    (mapv (fn [tuple] (mapv #(get tuple %) indices)) (:tuples relation))))

(defn- failure [f]
  (try (f) nil
       (catch #?(:clj clojure.lang.ExceptionInfo :cljs cljs.core/ExceptionInfo) e
         (ex-data e))))

(deftest rejects-before-allocating-a-forbidden-join-tuple
  (doseq [[left right] [[[[1] [2] [3]] [[4] [5] [6]]]
                        [[[1] [2]] [[3] [4] [5] [6]]]]]
    (let [allocated (atom 0)
          join-tuples rel/join-tuples
          error (binding [resource/*max-intermediate-slots* 12]
                  (with-redefs [rel/join-tuples
                                (fn [& args]
                                  (swap! allocated inc)
                                  (apply join-tuples args))]
                    (failure #(rel/hash-join (relation '[?a] left)
                                             (relation '[?b] right)))))]
      (is (= :query/intermediate-limit (:error error)))
      (is (= :join-output (:operator error)))
      (is (= 12 (:retained error)))
      (is (= 3 (:requested error)))
      (is (= 4 @allocated) "the rejected fifth tuple is never allocated"))))

(deftest exact-threshold-and-wide-tuples
  (let [left (relation '[?a ?b] [[1 2] [3 4]])
        right (relation '[?c ?d] [[5 6] [7 8]])]
    (binding [resource/*max-intermediate-slots* 20]
      (is (= #{[1 2 5 6] [3 4 5 6] [1 2 7 8] [3 4 7 8]}
             (set (rows (rel/hash-join left right))))))
    (binding [resource/*max-intermediate-slots* 19]
      (let [error (failure #(rel/hash-join left right))]
        (is (= :join-output (:operator error)))
        (is (= 15 (:retained error)))
        (is (= 5 (:requested error)))))))

(deftest zero-column-join-still-charges-retained-rows
  (let [left (relation [] [[] [] []])
        right (relation [] [[] [] []])]
    (binding [resource/*max-intermediate-slots* 9]
      (is (= 9 (count (:tuples (rel/hash-join left right))))))
    (binding [resource/*max-intermediate-slots* 8]
      (let [error (failure #(rel/hash-join left right))]
        (is (= :join-output (:operator error)))
        (is (= 8 (:retained error)))
        (is (= 1 (:requested error)))))))

(deftest join-index-counts-all-buckets-and-all-duplicate-references
  (testing "new buckets share one allowance, rather than resetting the counter"
    (binding [resource/*max-intermediate-slots* 8]
      (let [error (failure #(rel/hash-attrs first [[1] [2] [3]] 1))]
        (is (= :join-index (:operator error)))
        (is (= 8 (:retained error)))
        (is (= 4 (:requested error))))))
  (testing "duplicate keys still retain a reference for every tuple"
    (binding [resource/*max-intermediate-slots* 5]
      (is (= {1 '([1] [1])} (rel/hash-attrs first [[1] [1]] 1)))
      (let [error (failure #(rel/hash-attrs first [[1] [1] [1]] 1))]
        (is (= :join-index (:operator error)))
        (is (= 5 (:retained error)))
        (is (= 1 (:requested error)))))))

(deftest join-index-rejects-before-inserting-an-extra-entry
  (let [inserted (atom 0)
        insert! assoc!]
    (binding [resource/*max-intermediate-slots* 8]
      (with-redefs [#?(:clj clojure.core/assoc! :cljs cljs.core/assoc!)
                    ;; Core functions compile to fixed-arity properties on
                    ;; ClojureScript; keep those properties on the sentinel.
                    (fn
                      ([m k v]
                       (swap! inserted inc)
                       (insert! m k v))
                      ([m k v & kvs]
                       (swap! inserted + (inc (quot (count kvs) 2)))
                       (apply insert! m k v kvs)))]
        (let [error (failure #(rel/hash-attrs first [[1] [2] [3]] 1))]
          (is (= :join-index (:operator error)))
          (is (= 2 @inserted) "the third insertion never happens"))))))

(deftest composite-key-scratch-rejected-before-its-constructor
  (let [constructed (atom 0)]
    (binding [resource/*max-intermediate-slots* 2]
      (let [error (failure #(rel/hash-attrs
                             (fn [tuple] (swap! constructed inc) (vec tuple))
                             [[1 2 3]] 3))]
        (is (= :join-key (:operator error)))
        (is (zero? @constructed))))
    (testing "an empty index never constructs a key"
      (binding [resource/*max-intermediate-slots* 1]
        (is (= {} (rel/hash-attrs identity [] 100)))))))

(deftest large-matching-bucket-is-bounded-before-product-allocation
  (let [left (relation '[?k ?a] [[1 :a] [1 :b] [1 :c]])
        right (relation '[?k ?b] [[1 :x] [1 :y] [1 :z]])
        allocated (atom 0)
        join-tuples rel/join-tuples]
    (binding [resource/*max-intermediate-slots* 16]
      (with-redefs [rel/join-tuples
                    (fn [& args]
                      (swap! allocated inc)
                      (apply join-tuples args))]
        (let [error (failure #(rel/hash-join left right))]
          (is (= :join-output (:operator error)))
          (is (= 16 (:retained error)))
          (is (= 4 @allocated)))))))

(deftest allowance-is-local-to-each-materialization
  (let [left (relation '[?a] [[1] [2]])
        right (relation '[?b] [[3]])]
    (binding [resource/*max-intermediate-slots* 6]
      (dotimes [_ 100]
        (is (= #{[1 3] [2 3]} (set (rows (rel/hash-join left right)))))))))

(deftest capped-join-preserves-nulls-duplicates-and-empty-results
  (doseq [[left right]
          [[(relation '[?k ?a] [[nil :a] [1 :b] [1 :b]])
            (relation '[?k ?b] [[nil :x] [1 :y] [2 :z]])]
           [(relation '[?a] [[1] [1]]) (relation '[?b] [[2] [2]])]
           [(relation '[?k ?a] []) (relation '[?k ?b] [[1 :x]])]
           [(relation '[?k ?a] [[1 :a]]) (relation '[?k ?b] [[2 :x]])]]]
    (let [expected (rows (rel/hash-join left right))]
      (binding [resource/*max-intermediate-slots* 1000]
        (is (= expected (rows (rel/hash-join left right))))))))

(deftest indexing-remains-cancellable-without-a-resource-limit
  (let [visited (atom 0)]
    (binding [cancel/*cancel* (atom true)]
      (let [error (failure #(rel/hash-attrs
                             (fn [tuple] (swap! visited inc) (first tuple))
                             (map vector (range 2000)) 1))]
        (is (:datahike/canceled error))
        (is (< @visited 2000))))))

(deftest union-reserves-before-copying-initial-and-appended-tuples
  (let [one (relation '[?a ?b] [[1 2]])
        two (relation '[?a ?b] [[3 4] [5 6]])
        three (relation '[?a ?b] [[1 2] [3 4] [5 6]])
        empty (relation '[?a ?b] [])]
    (binding [resource/*max-intermediate-slots* 9]
      (is (= [[1 2] [3 4] [5 6]] (rows (rel/sum-rel one two)))))
    (doseq [[left right] [[one two] [three empty]]]
      (let [inserted (atom 0)
            insert! conj!]
        (binding [resource/*max-intermediate-slots* 8]
          (with-redefs [#?(:clj clojure.core/conj! :cljs cljs.core/conj!)
                        ;; Match core's multi-arity calling convention on CLJS.
                        (fn
                          ([output] (insert! output))
                          ([output tuple]
                           (swap! inserted inc)
                           (insert! output tuple))
                          ([output tuple & tuples]
                           (swap! inserted + (inc (count tuples)))
                           (apply insert! output tuple tuples)))]
            (let [error (failure #(rel/sum-rel left right))]
              (is (= :union-output (:operator error)))
              (is (= 6 (:retained error)))
              (is (= 3 (:requested error)))
              (is (= 2 @inserted) "the third tuple reference is never copied"))))))))

(deftest union-reserves-before-reindexing-tuples
  (let [left (relation '[?a ?b] [[1 2]])
        right (relation '[?b ?a] [[4 3] [6 5]])
        allocated (atom 0)
        make-array da/make-array]
    (binding [resource/*max-intermediate-slots* 9]
      (is (= [[1 2] [3 4] [5 6]] (rows (rel/sum-rel left right)))))
    (binding [resource/*max-intermediate-slots* 8]
      (with-redefs [da/make-array (fn [size]
                                    (swap! allocated inc)
                                    (make-array size))]
        (let [error (failure #(rel/sum-rel left right))]
          (is (= :union-output (:operator error)))
          (is (= 6 (:retained error)))
          (is (= 3 (:requested error)))
          (is (= 1 @allocated) "the second reindexed tuple is never allocated"))))))

(deftest union-counts-physical-width-and-preserves-bags
  (let [left (relation '[?a ?b] [[1 2 99]])
        right (relation '[?b ?a] [[4 3 88]])]
    (binding [resource/*max-intermediate-slots* 7]
      (is (= [[1 2] [3 4]] (rows (rel/sum-rel left right)))))
    (binding [resource/*max-intermediate-slots* 6]
      (let [error (failure #(rel/sum-rel left right))]
        (is (= 4 (:retained error)) "all three cells of the shared left tuple count")
        (is (= 3 (:requested error)) "the reindexed right tuple has only two cells"))))
  (binding [resource/*max-intermediate-slots* 6]
    (is (= [[1] [1] [1]]
           (rows (rel/sum-rel (relation '[?a] [[1] [1]])
                              (relation '[?a] [[1]])))))))

(deftest union-capped-and-uncapped-semantics-agree
  (doseq [[left right] [[(relation '[?a ?b] [[nil 1] [nil 1]])
                         (relation '[?b ?a] [[2 :x]])]
                        [(rel/->Relation '{?a "a" ?b "b"} [{"a" 1 "b" 2}])
                         (rel/->Relation '{?a "a" ?b "b"} [{"a" 3 "b" 4}])]
                        [(rel/->Relation '{?a "a" ?b "b"} [{"a" 1 "b" 2}])
                         (relation '[?b ?a] [[4 3]])]
                        [(relation '[?a] []) (relation '[?a] [[1]])]]]
    (let [expected (rows (rel/sum-rel left right))]
      (binding [resource/*max-intermediate-slots* 1000]
        (is (= expected (rows (rel/sum-rel left right))))))))

(deftest empty-join-does-not-allocate-probe-keys
  (let [left (relation '[?a ?b ?c] [])
        right (relation '[?a ?b ?c] [[1 2 3]])
        keys-built (atom 0)
        make-key rel/tuple-key-fn]
    (binding [resource/*max-intermediate-slots* 1]
      (with-redefs [rel/tuple-key-fn (fn [getters]
                                       (let [key-fn (make-key getters)]
                                         (fn [tuple]
                                           (swap! keys-built inc)
                                           (key-fn tuple))))]
        (is (empty? (:tuples (rel/hash-join left right))))
        (is (empty? (:tuples (rel/hash-join right left))))
        (is (zero? @keys-built))))))
