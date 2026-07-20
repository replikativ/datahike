(ns ^:no-doc datahike.array
  (:refer-clojure :exclude [bytes?])
  #?(:cljs (:require [goog.array]))
  #?(:clj (:import [java.util Arrays])))

#?(:clj
   nil
   #_(defonce ^:private hh-node-ns
       (try
         (require 'hitchhiker.tree.node)
         (find-ns 'hitchhiker.tree.node)
         (catch Exception _ nil))))

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

#?(:clj
   (defn bytes? [x]
     (clojure.core/bytes? x))
   :cljs
   (defn bytes? [x]
     ;; A byte array is a 1-byte-per-element typed array (Uint8Array/Int8Array).
     ;; The BYTES_PER_ELEMENT guard is essential: without it every typed array —
     ;; incl. Float32Array/Float64Array — has an ArrayBuffer buffer + numeric
     ;; byteLength and would be misread as bytes, so a float array would be
     ;; capped/compared/wrapped as bytes.
     (and (instance? js/ArrayBuffer (.-buffer x))
          (number? (.-byteLength x))
          (= 1 (.-BYTES_PER_ELEMENT x)))))

#?(:clj (def ^:private byte-array-class (Class/forName "[B")))
#?(:clj (def ^:private float-array-class (Class/forName "[F")))
#?(:clj (def ^:private double-array-class (Class/forName "[D")))

(defn float-array?
  "Is `x` a primitive float array (`:db.type/float-array`)? JVM `float[]` /
  JS `Float32Array`."
  [x]
  #?(:clj (instance? float-array-class x)
     :cljs (instance? js/Float32Array x)))

(defn double-array?
  "Is `x` a primitive double array (`:db.type/double-array`)? JVM `double[]` /
  JS `Float64Array`."
  [x]
  #?(:clj (instance? double-array-class x)
     :cljs (instance? js/Float64Array x)))

(defn value-array?
  "Any primitive array datahike treats as a scalar value — byte[], float[] or
  double[]. These need element-wise comparison/equality because the JVM gives
  them identity semantics. On the JVM this is one getClass + a few identical?
  pointer compares (these array classes are final), keeping `compare-value` — a
  query hot path — cheap for the common non-array case."
  [x]
  #?(:clj (when (some? x)
            (let [c (class x)]
              (or (identical? c byte-array-class)
                  (identical? c float-array-class)
                  (identical? c double-array-class))))
     :cljs (or (bytes? x) (float-array? x) (double-array? x))))

(defn byte-count
  "Number of bytes in a byte-array value (`:db.type/bytes`)."
  [x]
  #?(:clj  (alength ^bytes x)
     :cljs (.-byteLength x)))

(defn value-array-length
  "Element count of a byte/float/double array value — the unit the per-type
  `:max-*-length` value-size caps bound (bytes for byte[], floats for float[],
  doubles for double[]), matching how `:max-string-length` counts chars."
  [x]
  #?(:clj  (cond
             (bytes? x)        (alength ^bytes x)
             (float-array? x)  (alength ^floats x)
             (double-array? x) (alength ^doubles x))
     :cljs (.-length x)))

(defn compare-arrays
  "Compare two arrays a and b element-wise in ascending order. If one array is a
prefix of another then it comes first. Same-typed byte/float/double arrays are
compared element-wise; a mismatched pair falls back to a stable class ordering."
  [a b]
  #?(:cljs (goog.array/compare3 a b)
     :clj
     (cond
       (and (bytes? a) (bytes? b)) (raw-array-compare a b)
       ;; Arrays/compare has float[]/double[] overloads (Java 9+); NaN sorts as
       ;; Float/Double.compare orders it, and -0.0 < 0.0, so ordering is total.
       (and (float-array? a) (float-array? b)) (java.util.Arrays/compare ^floats a ^floats b)
       (and (double-array? a) (double-array? b)) (java.util.Arrays/compare ^doubles a ^doubles b)
       :else
       (try
         (compare a b)
         (catch ClassCastException _
           ;; different value-array kinds (or an array vs. a non-array): order
           ;; by class name so the index stays totally ordered.
           (compare (str (class a)) (str (class b))))))))

(defn string-from-bytes
  "Represents a byte array as a string. Two byte arrays are said to be equal iff their corresponding values after applying this function are equal. That way, we rely on the equality and hash code implementations of the String class to compare byte arrays."
  [x]
  #?(:cljs (.decode (js/TextDecoder. "utf8") x)
     :clj
     (let [^bytes x x
           n (alength x)
           dst (char-array n)]
       (dotimes [i n]
         (aset dst i (char (aget x i))))
       (String. dst))))

(defrecord WrappedBytes [string-repr])

;; float[]/double[] have identity equality/hashCode. `(vec …)` gives a
;; value-equal representation usable as a key, but Clojure's `=` treats
;; `(float 1.5)` and `(double 1.5)` as equal, so a vec of Floats would collide
;; with a vec of Doubles — while `compare-arrays` keeps float[] and double[]
;; distinct (by class). The `kind` tag restores that distinction so
;; `wrap-comparable` and `a=` agree (the property test-extended-equality checks).
(defrecord WrappedArray [kind vals])

(defn wrap-comparable
  "This functions is such that `(a= x y)` is equivalent to `(= (wrap-comparable x) (wrap-comparable y))`. This lets us also use these semantics in hash-sets or as keys in maps."
  [x]
  (cond
    (bytes? x) (WrappedBytes. #?(:clj (string-from-bytes x)
                                 :cljs x))
    (float-array? x) (WrappedArray. :float (vec x))
    (double-array? x) (WrappedArray. :double (vec x))
    :else x))

(defn a=
  "Extension of Clojure's equality to things we also want to treat like values,
  e.g. certain array types."
  [a b]
  (or (= a b)
      #?(:clj (and (value-array? a)
                   (value-array? b)
                   (zero? (compare-arrays a b)))
         :cljs (or (identical? a b)
                   (zero? (compare-arrays a b))))))
