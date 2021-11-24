(ns datahike.test.array
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer [is are deftest testing use-fixtures]])
   [datahike.array :refer [compare-arrays a=]]))

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

(deftest test-extended-equality
  (testing "Testing extended equality with support for arrays."
    ;; some Clojure semantics safety checks
    (is (a= 0 0))
    (is (a= "foo" "foo"))
    (is (not (a= "foo" "bar")))
    (is (a= [{:a 5} 4 "bar"] [{:a 5} 4 "bar"]))))
