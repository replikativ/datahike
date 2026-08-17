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

#?(:cljs
   (defn- array-kind-rank
     "Ranks a value-array kind to match the JVM's class-name ordering of
      `[B` < `[D` < `[F` — byte, then double, then float. Non-arrays rank last;
      `compare-arrays` only sees them through `a=`, which guards with
      `value-array?`."
     [x]
     (cond (bytes? x) 0
           (double-array? x) 1
           (float-array? x) 2
           :else 3)))

(defn compare-arrays
  "Compare two arrays a and b element-wise in ascending order. If one array is a
prefix of another then it comes first. Same-typed byte/float/double arrays are
compared element-wise; a mismatched pair falls back to a stable class ordering."
  [a b]
  #?(:cljs (let [ka (array-kind-rank a)
                 kb (array-kind-rank b)]
             ;; `goog.array/compare3` is NOT usable here. It orders with plain
             ;; `<`/`>`, which are false in BOTH directions for NaN — so it
             ;; reports 0 and calls a NaN equal to every number, not just to
             ;; another NaN. It also ignores the typed-array KIND, where the
             ;; JVM orders a mismatched pair by class name — byte < double <
             ;; float (`[B` < `[D` < `[F`).
             ;;
             ;; ORDER CHANGE, and which stored indexes it can touch: for values
             ;; the old comparator could actually ORDER — one kind per
             ;; attribute, no NaN, no signed zero — it did the same lexicographic
             ;; `<`/`>` walk this does, so their order is unchanged and a stored
             ;; tree stays valid. It differs only where the old comparator
             ;; returned 0 for values that are NOT equal (a NaN against any
             ;; number, -0.0 against 0.0, one kind against another). A tree
             ;; holding those was already built with a comparator that is not a
             ;; total order — two distinct values compared equal, so placement
             ;; depended on insertion order and a seek could already miss.
             ;; NOTE what is NOT reconciled here: a byte array is UNSIGNED on
             ;; ClojureScript (typed arrays) and SIGNED on the JVM, so 0xff
             ;; sorts last there and first here. That predates this and is not
             ;; changed, because changing it WOULD reorder existing stored
             ;; indexes holding high bytes.
             (if (== ka kb)
               ;; Elements keep `compare3`'s plain `<`/`>` ORDER, deliberately.
               ;; Giving NaN and signed zero their JVM order here would be more
               ;; correct in the abstract and is NOT worth it: `cmp-datoms-avet`
               ;; breaks a value tie on the entity id, so the old comparator IS
               ;; a total order, and a stored tree built under it can hold
               ;; `[NaN]`-before-`[1.0]` — an order the JVM semantics reverse.
               ;; Changing this would silently invalidate those trees (seeks
               ;; navigating away from stored datoms) with no migration, for
               ;; databases that have existed since 0.8.1745. The NaN defect
               ;; that matters — a NaN array reading as EQUAL to every numeric
               ;; array — is fixed in `a=` instead, which decides equality and
               ;; touches nothing on disk.
               (goog.array/compare3 a b)
               (if (< ka kb) -1 1)))
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
  ;; NOT `TextDecoder`: it maps every invalid UTF-8 byte to the SAME
  ;; replacement character, so 0x80 and 0x81 produced equal strings for arrays
  ;; `a=` calls different — the opposite of what this function promises. One
  ;; character per byte, as on the JVM.
  #?(:cljs (let [n (.-length x)
                 ;; `(.-buffer x)` would read the whole underlying ArrayBuffer,
                 ;; ignoring a view's byteOffset/length, and `.apply` on the
                 ;; result blows the stack past ~64k arguments. Walk the VIEW,
                 ;; in chunks.
                 sb (js/Array.)]
             (loop [i 0]
               (when (< i n)
                 (let [end (min n (+ i 8192))
                       chunk (js/Array.)]
                   (loop [j i]
                     (when (< j end)
                       (.push chunk (bit-and (aget x j) 0xff))
                       (recur (inc j))))
                   (.push sb (.apply js/String.fromCharCode nil chunk))
                   (recur end))))
             (.join sb ""))
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
      `a=` says: two NaNs are equal, a NaN differs from every number, and -0.0
      differs from 0.0 — the same value semantics as the JVM. (The stored
      ORDER still says otherwise for both; see `compare-arrays`.)"
     [x]
     (cond
       ;; NaN is equal to itself and to nothing else, and -0.0 is distinct from
       ;; 0.0 — both as `compare-arrays` now reports them, and as the JVM does.
       (js/Number.isNaN x) ::nan
       ;; Sentinels, because `a=` separates both from everything else: a NaN
       ;; equals only a NaN, and -0.0 is not 0.0 (as on the JVM).
       (and (zero? x) (neg? (/ 1 x))) ::negative-zero
       :else x)))

#?(:clj
   (defn array-key?
     "Is `x` a wrapped array KEY rather than a value? A key hashes by content
      and is therefore what hash joins store — but it is NOT the stored value,
      is not `Comparable`, and must never be handed to an index seek."
     [x]
     (instance? ArrayKey x)))

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
    (bytes? x) #?(:clj (let [^bytes c (java.util.Arrays/copyOf ^bytes x (alength ^bytes x))]
                          (ArrayKey. :byte c (java.util.Arrays/hashCode c)))
                  :cljs (WrappedArray. :byte (mapv canonical-element (array-seq x))))
    (float-array? x) #?(:clj (let [^floats c (java.util.Arrays/copyOf ^floats x (alength ^floats x))]
                               (ArrayKey. :float c (java.util.Arrays/hashCode c)))
                        :cljs (WrappedArray. :float (mapv canonical-element (array-seq x))))
    (double-array? x) #?(:clj (let [^doubles c (java.util.Arrays/copyOf ^doubles x (alength ^doubles x))]
                                (ArrayKey. :double c (java.util.Arrays/hashCode c)))
                         :cljs (WrappedArray. :double (mapv canonical-element (array-seq x))))
    :else x))

(defn- same-array-kind?
  "Are `a` and `b` the same primitive-array kind?"
  [a b]
  (or (and (bytes? a) (bytes? b))
      (and (float-array? a) (float-array? b))
      (and (double-array? a) (double-array? b))))

#?(:cljs
   (defn- cljs-arrays=
     "Element-wise equality for two same-kind value arrays on ClojureScript.

      NOT `(zero? (compare-arrays …))`: element ORDER there is `compare3`'s,
      which finds neither `<` nor `>` between a NaN and a number and therefore
      reports them EQUAL — so every numeric array equalled every
      NaN-containing array of the same shape. Deciding equality separately
      fixes that without touching the order any stored index was built with.
      `-0.0` stays equal to `0.0`, matching that order; the JVM separates them,
      and that platform difference is deliberately left open."
     [a b]
     (and (same-array-kind? a b)
          (== (value-array-length a) (value-array-length b))
          (let [n (value-array-length a)]
            (loop [i 0]
              (if (== i n)
                true
                (let [x (aget a i)
                      y (aget b i)]
                  (if (or (and (js/Number.isNaN x) (js/Number.isNaN y))
                          ;; `==` alone calls -0.0 and 0.0 equal. The JVM's
                          ;; `Arrays/equals` is bit-based and separates them, and
                          ;; the principle applied to NaN applies here too: fix
                          ;; EQUALITY, which touches nothing on disk, and leave
                          ;; the stored ORDER alone. Without this the platforms
                          ;; answered differently for the same data — a
                          ;; `:db.unique/identity` upsert of -0.0 then 0.0 made
                          ;; two entities on the JVM and one here.
                          (and (== x y)
                               (= (neg? (/ 1 x)) (neg? (/ 1 y)))))
                    (recur (inc i))
                    false))))))))

(defn a=
  "Extension of Clojure's equality to things we also want to treat like values,
  e.g. certain array types.

  A value array is decided by an ARRAY comparison, never by `=` first:
  ClojureScript's `=` compares typed arrays structurally and ignores the kind,
  so `(= (js/Int8Array. #js [1]) (js/Float32Array. #js [1]))` is TRUE there,
  and an `(or (= a b) …)` short-circuited on it before any kind check could
  run.

  The `value-array?` guard on BOTH sides is not symmetry for its own sake:
  without it this handed ANY two values to `compare-arrays`, and
  `goog.array/compare3` on two non-arrays reads `.length` (undefined on both),
  skips its loop and returns 0 — so `a=` answered TRUE for every pair.
  Measured on Node: `(a= {:a 1} {:b 2})`, `(a= 5 7)`, `(a= #{1} #{2})` were all
  true. Anything downstream that asks \"are these the same value\" —
  `compare-value`'s equality arm, and `db.search`'s value matching — was told
  yes about everything.

  `boolean`, because `value-array?` is a `when` on the JVM and a nil operand
  made this answer nil. Every current caller uses it in boolean position; the
  next one to write `(= false (a= …))` would not."
  [a b]
  (if (or (value-array? a) (value-array? b))
    (boolean (and (value-array? a)
                  (value-array? b)
                  #?(:clj (zero? (compare-arrays a b))
                     :cljs (cljs-arrays= a b))))
    (= a b)))
