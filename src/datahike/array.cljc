(ns ^:no-doc datahike.array
  #?(:clj (:require [hitchhiker.tree.node :as n]))
  #?(:cljs (:require [goog.array]))
  #?(:clj (:import [java.util Arrays])))

#?(:clj
   (defn java8? []
     (try
       (= 8 (Integer/parseInt (subs (System/getProperty "java.specification.version") 2)))
       (catch Exception _
         false))))

#?(:clj
  ;; meta data doesn't get expanded in macros :/
   (defn array-compare [a b] (Arrays/compare ^bytes a ^bytes b)))

#?(:clj
   (defmacro raw-array-compare [a b]
     (if (java8?)
       ;; slow fallback for Java 8, but has same semantics
       `(let [bl# (alength ~b)
              al# (alength ~a)]
          (loop [i# 1]
            (cond (and (> i# bl#) (> i# al#))
                  0

                  (> i# bl#)
                  1 ;; b is a prefix of a

                  (> i# al#)
                  -1 ;; a is a prefix of b

                  :else
                  (let [ec# (compare (aget ~a (dec i#)) (aget ~b (dec i#)))]
                    (if (not (zero? ec#))
                      ec#
                      (recur (inc i#)))))))
       `(array-compare ~a ~b))))

#?(:cljs
   (defn bytes? [x]
     (and (instance? js/ArrayBuffer (.-buffer x))
          (number? (.-byteLength x)))))

(defn compare-arrays
  "Compare two arrays a and b element-wise in ascending order. If one array is a
prefix of another then it comes first."
  [a b]
  #?(:cljs (goog.array/compare3 a b)
     :clj
     (if (not (and (bytes? a) (bytes? b)))
       (try
         (compare a b)
         (catch ClassCastException _
           (- (n/-order-on-edn-types a)
              (n/-order-on-edn-types b))))
       (raw-array-compare a b))))

(defn string-from-bytes
  "Represents a byte array as a string. Two byte arrays are said to be equal iff their corresponding values after applying this function are equal. That way, we rely on the equality and hash code implementations of the String class to compare byte arrays."
  [x]
  #?(:cljs (.decode (js/TextDecoder. "utf8") x)
     :clj
     (let [n (alength x)
           dst (char-array n)]
       (dotimes [i n]
         (aset dst i (char (aget x i))))
       (String. dst))))

(defrecord WrappedBytes [string-repr])

(defn wrap-comparable
  "This functions is such that `(a= x y)` is equivalent to `(= (wrap-comparable x) (wrap-comparable y))`. This lets us also use these semantics in hash-sets or as keys in maps."
  [x]
  (if (bytes? x)
    (WrappedBytes. #?(:clj (string-from-bytes x)
                      :cljs x))
    x))

(defn a=
  "Extension of Clojure's equality to things we also want to treat like values,
  e.g. certain array types."
  [a b]
  (or (= a b)
      #?(:clj (and (bytes? a)
                   (bytes? b)
                   (zero? (compare-arrays a b)))
         :cljs (or (identical? a b)
                   (zero? (compare-arrays a b))))))
