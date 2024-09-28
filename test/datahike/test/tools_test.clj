(ns datahike.test.tools-test
  (:require [datahike.tools :as dt]
            [clojure.test :refer :all]))

(deftest test-with-destructured-vector
  (is (= [11 19]
         (dt/with-destructured-vector [10 20]
           a (inc a)
           b (dec b))))
  (is (= [11]
         (dt/with-destructured-vector [10]
           a (inc a)
           b (+ a b))))
  (is (= [300 40]
         (dt/with-destructured-vector [10 30]
           a (* a b)
           b (+ a b))))
  (is (= [100 400]
         (dt/with-destructured-vector [10 20]
           a (* a a)
           b (* b b)
           c (throw (ex-info "This element should not be evaluated" {}))))))

(defn add-resolver [context [result-var a b]]
  (when (and (contains? context a)
             (contains? context b))
    (assoc context result-var (+ (context a)
                                 (context b)))))

(deftest resolve-clauses-test
  (is (= {:x 9 :y 10 :z 19 :w 28}
         (dt/resolve-clauses add-resolver
                             {:x 9 :y 10}
                             [[:w :z :x]
                              [:z :x :y]])))
  (is (= {:x 9 :y 10 :z 19 :w 28}
         (dt/resolve-clauses add-resolver
                             {:x 9 :y 10}
                             [[:z :x :y]
                              [:w :z :x]])))
  (is (thrown? Exception
               (dt/resolve-clauses add-resolver
                                   {:x 9 :y 10}
                                   [[:w :z :x]]))))

(deftest group-by-step-test
  (is (= {true [1000 1002 1004 1006 1008]
          false [1001 1003 1005 1007 1009]}
         (transduce (map #(+ 1000 %))
                    (dt/group-by-step even?)
                    (range 10)))))

(deftest test-match-vector
  (is (= 0 (dt/match-vector [nil nil]
                            [_ _] 0
                            [_ 1] 1
                            [1 *] 2)))
  (is (= 1 (dt/match-vector [nil 9]
                            [_ _] 0
                            [_ 1] 1
                            [1 *] 2)))
  (is (= 2 (dt/match-vector [10 nil]
                            [_ _] 0
                            [_ 1] 1
                            [1 *] 2)))
  (is (= 2 (dt/match-vector [10 :asdf]
                            [_ _] 0
                            [_ 1] 1
                            [1 *] 2)))
  (is (= 3 (dt/match-vector [10 :asdf]
                            [_ _] 0
                            [_ 1] 1
                            [1 _] 2
                            [1 1] 3)))
  (is (= 2 (dt/match-vector [10 nil]
                            [_ _] 0
                            [_ 1] 1
                            [1 _] 2
                            [1 1] 3))))

(defmacro wrap-range-tree [input-symbol]
  (dt/range-subset-tree 3 input-symbol (fn [x y] [:inds x :mask y])))

(deftest range-subset-tree-test
  (is (= (dt/range-subset-tree 1 'x (fn [inds _] [:inds inds]))
         '(if
           (clojure.core/empty? x)
            [:inds []]
            (if
             (clojure.core/= 0 (clojure.core/first x))
              (clojure.core/let [x (clojure.core/rest x)] [:inds [0]])
              [:inds []]))))
  (is (= [:inds [1 2] :mask [nil 0 1]]
         (wrap-range-tree [1 2])))
  (is (= [:inds [1] :mask [nil 0 nil]]
         (wrap-range-tree [1])))
  (is (= [:inds [0 2] :mask [0 nil 1]]
         (wrap-range-tree [0 2]))))

(deftest merge-distinct-sorted-seqs-test
  (testing "Custom comparator"
    (let [m {:one 1
             :two 2
             :three 3
             :four 4
             :five 5
             :six 6
             :seven 7
             :eight 8
             :nine 9
             :ten 10}
          cmp (fn [a b] (compare (m a) (m b)))]
      (is (= [] (dt/merge-distinct-sorted-seqs cmp [] [])))
      (is (= [:one] (dt/merge-distinct-sorted-seqs cmp [:one] [])))
      (is (= [:one] (dt/merge-distinct-sorted-seqs cmp [:one] [:one])))
      (is (= [:one] (dt/merge-distinct-sorted-seqs cmp [] [:one])))
      (is (= [:one :two] (dt/merge-distinct-sorted-seqs cmp [:two] [:one])))
      (is (= [:one :two :three :four :five :nine :ten]
             (dt/merge-distinct-sorted-seqs cmp
                                            [:one :two :three :nine :ten]
                                            [:two :three :four :five])))
      (is (dt/distinct-sorted-seq? cmp []))
      (is (dt/distinct-sorted-seq? cmp [:one]))
      (is (dt/distinct-sorted-seq? cmp [:one :two]))
      (is (dt/distinct-sorted-seq? cmp [:one :two :three]))
      (is (not (dt/distinct-sorted-seq? cmp [:one :two :three :three])))
      (is (not (dt/distinct-sorted-seq? cmp [:one :one :two :three])))
      (is (not (dt/distinct-sorted-seq? cmp [:one :two :two :three])))
      (is (not (dt/distinct-sorted-seq? cmp [:one :two :three :five :four])))))
  (testing "Infinite length sequences"
    (let [evens (iterate #(+ 2 %) 0)
          odds (iterate #(+ 2 %) 1)
          result (dt/merge-distinct-sorted-seqs compare odds evens)]
      (is (= (range 1000) (take 1000 result))))))
