(ns datahike.test.array-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing]])
   [clojure.core :refer [byte-array]]
   [datahike.array :refer [compare-arrays a= wrap-comparable]]))

(deftest test-array-ordering
  (testing "Array value indexing support."

    ;; empty arrays
    (is (zero? (compare-arrays (byte-array []) (byte-array []))))

    ;; ordering is by dimensionality first
    (is (neg? (compare-arrays (byte-array []) (byte-array [5 2 3 5]))))
    (is (pos? (compare-arrays (byte-array [5 2 3 5]) (byte-array []))))
    (is (neg? (compare-arrays (byte-array [5 2 3]) (byte-array [5 2 3 5]))))
    (is (pos? (compare-arrays (byte-array [5 2 3 5]) (byte-array [5 2 3]))))

    ;; for equal length arrays we do an element-wise comparison
    (is (zero? (compare-arrays (byte-array [5 2 3 5]) (byte-array [5 2 3 5]))))
    (is (neg?  (compare-arrays (byte-array [5 2 2 5]) (byte-array [5 2 3 1]))))
    (is (pos?  (compare-arrays (byte-array [6 2 2 5]) (byte-array [5 2 3 5]))))))

(defn a2= [a b]
  (= (wrap-comparable a)
     (wrap-comparable b)))

(deftest test-extended-equality
  (testing "Testing extended equality with support for arrays."
    (doseq [cmp [a= a2=]]
      ;; some Clojure semantics safety checks
      (is (cmp 0 0))
      (is (cmp "foo" "foo"))
      (is (not (cmp "foo" "bar")))
      (is (cmp [{:a 5} 4 "bar"] [{:a 5} 4 "bar"]))
      (is (cmp (byte-array [5 2 3])
               (byte-array [5 2 3])))
      (is (not (cmp (byte-array [5 2 3])
                    (byte-array [5 2 4])))))))
