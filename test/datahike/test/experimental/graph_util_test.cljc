(ns datahike.test.experimental.graph-util-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing]])
   [datahike.experimental.graph-util :as gu]))

(deftest priority-queue-ordering
  (testing "pops in priority order"
    (let [q (-> (gu/pq) (gu/pq-push 3 :c) (gu/pq-push 1 :a) (gu/pq-push 2 :b))]
      (is (= [1 :a] (gu/pq-peek q)))
      (let [q2 (gu/pq-pop q)]
        (is (= [2 :b] (gu/pq-peek q2)))
        (is (= [3 :c] (gu/pq-peek (gu/pq-pop q2))))))))

(deftest priority-queue-equal-priorities-not-dropped
  (testing "equal-priority distinct items all survive (the dedup bug)"
    (let [items [:a :b :c :d :e]
          q (reduce (fn [q i] (gu/pq-push q 7 i)) (gu/pq) items)
          drained (loop [q q acc []]
                    (if (gu/pq-empty? q)
                      acc
                      (recur (gu/pq-pop q) (conj acc (second (gu/pq-peek q))))))]
      (is (= (count items) (count drained)) "no entries silently merged")
      (is (= (set items) (set drained)) "all items recovered"))))

(deftest priority-queue-empty
  (is (gu/pq-empty? (gu/pq)))
  (is (nil? (gu/pq-peek (gu/pq))))
  (is (gu/pq-empty? (gu/pq-pop (gu/pq))) "pop on empty is a no-op"))

(deftest fifo-queue
  (testing "conj/peek/pop give FIFO order"
    (let [q (into gu/empty-queue [:a :b :c])]
      (is (= :a (peek q)))
      (is (= :b (peek (pop q))))
      (is (= :c (peek (pop (pop q))))))))

(deftest prng-deterministic
  (testing "same seed -> same sequence"
    (let [draw (fn [seed n]
                 (loop [s (gu/rng seed) acc [] k n]
                   (if (zero? k)
                     acc
                     (let [[s' u] (gu/rng-next s)]
                       (recur s' (conj acc u) (dec k))))))]
      (is (= (draw 42 20) (draw 42 20)))
      (is (not= (draw 42 20) (draw 43 20)))))
  (testing "units in [0,1)"
    (let [us (loop [s (gu/rng 7) acc [] k 100]
               (if (zero? k) acc
                   (let [[s' u] (gu/rng-next s)] (recur s' (conj acc u) (dec k)))))]
      (is (every? #(and (<= 0.0 %) (< % 1.0)) us))))
  (testing "rng-int stays in range and rng-nth picks from coll"
    (let [coll [:a :b :c :d]]
      (loop [s (gu/rng 1) k 200]
        (when (pos? k)
          (let [[s' i] (gu/rng-int s 4)
                [s'' e] (gu/rng-nth s coll)]
            (is (and (<= 0 i) (< i 4)))
            (is (contains? (set coll) e))
            (recur s'' (dec k))))))))

(deftest prng-roughly-uniform
  (testing "rng-int distribution is not wildly skewed"
    (let [n 6
          counts (loop [s (gu/rng 99) c (vec (repeat n 0)) k 6000]
                   (if (zero? k) c
                       (let [[s' i] (gu/rng-int s n)]
                         (recur s' (update c i inc) (dec k)))))]
      (is (every? #(< 800 % 1200) counts)
          (str "expected ~1000 each, got " counts)))))

(deftest math-helpers
  (is (= 5 (gu/abs -5)))
  (is (= 5 (gu/abs 5)))
  (is (= 0 (gu/abs 0)))
  (is (< 1e308 gu/infinity))
  (is (= gu/infinity (+ gu/infinity 1))))
