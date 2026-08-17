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
     ;;
     ;; `some?` first: `(.-buffer nil)` is a TypeError, not nil, so every caller
     ;; that may see a nil value — `compare-value` reaches this before it checks
     ;; anything else — would throw rather than answer.
     (and (some? x)
          (instance? js/ArrayBuffer (.-buffer x))
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
         ;; `(char -1)` throws — a byte is SIGNED, and every byte from 0x80 up
         ;; is negative. Mask to the unsigned value so the representation
         ;; exists for the whole domain.
         (aset dst i (char (bit-and (aget x i) 0xff))))
       (String. dst))))

#?(:clj
   (deftype ArrayKey [kind arr ^int hsh]
     Object
     (hashCode [_] hsh)
     (equals [this o]
       (or (identical? this o)
           (and (instance? ArrayKey o)
                (let [^ArrayKey o o]
                  (and (identical? kind (.-kind o))
                       (case kind
                         :byte   (java.util.Arrays/equals ^bytes (.-arr this) ^bytes (.-arr o))
                         :float  (java.util.Arrays/equals ^floats (.-arr this) ^floats (.-arr o))
                         :double (java.util.Arrays/equals ^doubles (.-arr this) ^doubles (.-arr o))))))))
     (toString [_] (str "#datahike/array-key[" (name kind) "]"))))

;; The `kind` tag keeps float[] and double[] distinct, which `compare-arrays`
;; does by class: without it a vec of Floats and a vec of Doubles holding the
;; same numbers would be `=`.
(defrecord WrappedArray [kind vals])

#?(:cljs
   (defn- canonical-element
     "Normalises an element so that vector equality reproduces what
      `compare-arrays` says on this platform: `goog.array/compare3` finds
      neither `<` nor `>` between two NaNs and calls them equal, and likewise
      does not separate -0.0 from 0.0."
     [x]
     (cond
       (js/Number.isNaN x) ::nan
       (and (zero? x) (neg? (/ 1 x))) 0
       :else x)))

(defn wrap-comparable
  "A key `k` such that `(a= x y)` iff `(= (wrap-comparable x) (wrap-comparable y))`,
   so array VALUES can be used in hash sets and as map keys.

   This has to hold over the WHOLE value domain, and it did not. Byte arrays
   were represented as a String built with `(char signed-byte)`, which THREW
   for every byte from 0x80 up; and float/double arrays as `(vec …)`, whose
   equality disagrees with `a=` in both directions — Clojure's `=` says two
   NaNs differ (`a=`, via `Arrays/compare`, says they are equal) and that -0.0
   equals 0.0 (`a=` says they differ). A join keyed on any of those either
   crashed or silently put equal values in different buckets.

   On the JVM the key holds the array itself and defers to `Arrays/equals`,
   which is bit-based for float/double and therefore agrees with
   `Arrays/compare` on exactly these cases — and allocates nothing per key
   beyond the wrapper. ClojureScript has no such primitive, so it keeps a
   canonicalised vector, matching what `compare3` reports there.

   Identity for every non-array value."
  [x]
  (cond
    (bytes? x) #?(:clj (ArrayKey. :byte x (java.util.Arrays/hashCode ^bytes x))
                  :cljs (WrappedArray. :byte (mapv canonical-element (array-seq x))))
    (float-array? x) #?(:clj (ArrayKey. :float x (java.util.Arrays/hashCode ^floats x))
                        :cljs (WrappedArray. :float (mapv canonical-element (array-seq x))))
    (double-array? x) #?(:clj (ArrayKey. :double x (java.util.Arrays/hashCode ^doubles x))
                         :cljs (WrappedArray. :double (mapv canonical-element (array-seq x))))
    :else x))

(defn a=
  "Extension of Clojure's equality to things we also want to treat like values,
  e.g. certain array types."
  [a b]
  (or (= a b)
      #?(:clj (and (value-array? a)
                   (value-array? b)
                   (zero? (compare-arrays a b)))
         ;; The `value-array?` guard is not symmetry-for-its-own-sake: without
         ;; it this branch handed ANY two values to `compare-arrays`, and
         ;; `goog.array/compare3` on two non-arrays reads `.length` (undefined
         ;; on both), skips its loop and returns 0 — so `a=` answered TRUE for
         ;; every pair. Measured on Node: `(a= {:a 1} {:b 2})`, `(a= 5 7)`,
         ;; `(a= #{1} #{2})` were all true. Anything downstream that asks "are
         ;; these the same value" — `compare-value`'s equality arm, and
         ;; `db.search`'s value matching — was told yes about everything.
         :cljs (and (value-array? a)
                    (value-array? b)
                    (zero? (compare-arrays a b))))))
