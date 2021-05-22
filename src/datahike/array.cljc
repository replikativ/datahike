(ns datahike.array
  (:require [hitchhiker.tree.node :as n])
  (:import [java.util Arrays]))

(defn compare-arrays
  "Compare two arrays a and b element-wise in ascending order. If one array is a
  prefix of another then it comes first."
  [a b]
  (if (not (and (bytes? a) (bytes? b)))
    (try
      (compare a b)
      (catch ClassCastException _
        (- (n/-order-on-edn-types a)
           (n/-order-on-edn-types b))))
    (Arrays/compare a b)))

(defn a=
  "Extension of Clojure's equality to things we also want to treat like values,
  e.g. certain array types."
  [a b]
  (or (= a b)
      (and (bytes? a)
           (bytes? b)
           (zero? (compare-arrays a b)))))
