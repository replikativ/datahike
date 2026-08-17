(ns datahike.test.array-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing]])
   [clojure.core :refer [byte-array]]
   [datahike.array :refer [compare-arrays a= wrap-comparable
                           float-array? double-array? value-array?]]))

#?(:cljs (def float-array #(js/Float32Array. (clj->js %))))
#?(:cljs (def double-array #(js/Float64Array. (clj->js %))))

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
                    (byte-array [5 2 4]))))
      ;; float/double arrays get the same value semantics
      (is (cmp (float-array [1.5 2.5 3.5]) (float-array [1.5 2.5 3.5])))
      (is (not (cmp (float-array [1.5 2.5 3.5]) (float-array [1.5 2.5 9.0]))))
      (is (cmp (double-array [1.5 2.5]) (double-array [1.5 2.5])))
      (is (not (cmp (double-array [1.5 2.5]) (double-array [9.0 2.5]))))
      ;; a float[] and a double[] with the same numbers are NOT equal
      (is (not (cmp (float-array [1.5 2.5]) (double-array [1.5 2.5]))))

      ;; The congruence has to hold over the WHOLE value domain, and these are
      ;; the values where it did not: `wrap-comparable` represented bytes as a
      ;; String built with `(char signed-byte)`, which THREW from 0x80 up, and
      ;; float/double arrays as plain vectors, whose `=` disagrees with `a=` in
      ;; both directions on NaN and on signed zero. Asserting `a=` and `a2=`
      ;; together is the point: either one alone would have passed throughout.
      (is (cmp (byte-array [-1]) (byte-array [-1])))
      (is (cmp (byte-array [-1 -128 0 127]) (byte-array [-1 -128 0 127])))
      (is (not (cmp (byte-array [-1]) (byte-array [-2]))))
      #?(:clj
         (do
           ;; NaN: `Arrays/compare` and `Arrays/equals` both call two NaNs
           ;; equal, while Clojure's `=` on the boxed values does not
           (is (cmp (float-array [##NaN]) (float-array [##NaN])))
           (is (cmp (double-array [##NaN]) (double-array [##NaN])))
           (is (cmp (float-array [##NaN 1.5]) (float-array [##NaN 1.5])))
           ;; signed zero: ordered, so NOT equal — the reverse disagreement
           (is (not (cmp (float-array [-0.0]) (float-array [0.0]))))
           (is (not (cmp (double-array [-0.0]) (double-array [0.0])))))))))

(deftest test-primitive-array-predicates
  (testing "value-array type predicates"
    (is (float-array? (float-array [1.0])))
    (is (double-array? (double-array [1.0])))
    (is (not (float-array? (double-array [1.0]))))
    (is (not (double-array? (float-array [1.0]))))
    (is (value-array? (float-array [1.0])))
    (is (value-array? (double-array [1.0])))
    (is (value-array? (byte-array [1])))
    (is (not (value-array? [1.0 2.0])))
    (is (not (value-array? "foo")))))

(deftest test-float-double-array-ordering
  (testing "float[] / double[] compare element-wise, length-first"
    (is (zero? (compare-arrays (float-array [1.5 2.5]) (float-array [1.5 2.5]))))
    (is (neg?  (compare-arrays (float-array [1.5 2.0]) (float-array [1.5 2.5]))))
    (is (pos?  (compare-arrays (float-array [1.5 2.5]) (float-array [1.5 2.0]))))
    (is (neg?  (compare-arrays (float-array [1.5]) (float-array [1.5 2.5]))))
    (is (zero? (compare-arrays (double-array [1.5 2.5]) (double-array [1.5 2.5]))))
    (is (neg?  (compare-arrays (double-array [1.0 2.5]) (double-array [1.5 2.5]))))
    ;; distinct array kinds stay totally ordered (no throw)
    (is (not (zero? (compare-arrays (float-array [1.5]) (double-array [1.5])))))))
