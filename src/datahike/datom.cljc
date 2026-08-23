(ns ^:no-doc datahike.datom
  (:require  [clojure.walk]
             [clojure.data]
             [datahike.array :as da]
             [datahike.constants :refer [tx0]]
             [datahike.tools :refer [combine-hashes]]
             #?(:cljs [goog.array :as garray]))
  #?(:cljs (:require-macros [datahike.datom :refer [combine-cmp]])))

(declare hash-datom equiv-datom seq-datom nth-datom assoc-datom val-at-datom)

(defprotocol IDatom
  (datom-tx [this])
  (datom-added [this]))

(deftype Datom #?(:clj  [^long e a v ^long tx ^:unsynchronized-mutable ^int _hash]
                  :cljs [^number e a v ^number tx ^:mutable ^number _hash])
  IDatom
  (datom-tx [d] (if (pos? tx) tx (- tx)))
  (datom-added [d] (pos? tx))

  #?@(:cljs
      [IHash
       (-hash [d] (if (zero? _hash)
                    (set! _hash (hash-datom d))
                    _hash))
       IEquiv
       (-equiv [d o] (and (instance? Datom o) (equiv-datom d o)))

       ISeqable
       (-seq [d] (seq-datom d))

       IFn
       (-invoke [d k] (val-at-datom d k nil))
       (-invoke [d k v] (val-at-datom d k v))

       ILookup
       (-lookup [d k] (val-at-datom d k nil))
       (-lookup [d k nf] (val-at-datom d k nf))

       IIndexed
       (-nth [this i] (nth-datom this i))
       (-nth [this i not-found] (nth-datom this i not-found))

       IAssociative
       (-assoc [d k v] (assoc-datom d k v))

       IPrintWithWriter
       (-pr-writer [d writer opts]
                   (pr-sequential-writer writer pr-writer
                                         "#datahike/Datom [" " " "]"
                                         opts [(.-e d) (.-a d) (.-v d) (datom-tx d) (datom-added d)]))]
      :clj
      [Object
       (hashCode [d]
                 (if (zero? _hash)
                   (let [h (int (hash-datom d))]
                     (set! _hash h)
                     h)
                   _hash))
       (toString [d] (pr-str d))

       clojure.lang.IHashEq
       (hasheq [d] (.hashCode d))

       clojure.lang.Seqable
       (seq [d] (seq-datom d))

       clojure.lang.IPersistentCollection
       (equiv [d o] (and (instance? Datom o) (equiv-datom d o)))
       (empty [d] (throw (UnsupportedOperationException. "empty is not supported on Datom")))
       (count [d] 5)
       (cons [d [k v]] (assoc-datom d k v))

       clojure.lang.Indexed
       (nth [this i] (nth-datom this i))
       (nth [this i not-found] (nth-datom this i not-found))

       clojure.lang.IFn
       (invoke [d k] (val-at-datom d k nil))
       (invoke [d k v] (val-at-datom d k v))

       clojure.lang.ILookup
       (valAt [d k] (val-at-datom d k nil))
       (valAt [d k nf] (val-at-datom d k nf))

       clojure.lang.Associative
       (entryAt [d k] (some->> (val-at-datom d k nil) (clojure.lang.MapEntry. k)))
       (containsKey [e k] (#{:e :a :v :tx :added} k))
       (assoc [d k v] (assoc-datom d k v))]))

#?(:cljs (goog/exportSymbol "datahike.datom.Datom" Datom))

(defn ^Datom datom
  ([e a v] (Datom. e a v tx0 0))
  ([e a v tx] (Datom. e a v tx 0))
  ([e a v tx added] (Datom. e a v (if added tx (- tx)) 0)))

(defn datom? [x] (instance? Datom x))

(defn- value-hash
  "`hash` of a datom's VALUE, by content for the types datahike treats as
   values.

   A byte/float/double array hashes by IDENTITY, and that leaks out of memory:
   `:hash` on a database is a SUM of datom hashes, so a database holding an
   array had a `:hash` that no recomputation could reproduce — measured, two
   loads of the same stored database recompute to different numbers. The
   `:build-indexes?` import path recomputes exactly this way, which is why a
   rebuilt array database could not be `:hash`-equal to its source.

   `wrap-comparable` is identity for every non-array value, so scalars hash
   exactly as before — no existing array-free database changes by one bit."
  [v]
  (if (da/value-array? v) (hash (da/wrap-comparable v)) (hash v)))

(defn- hash-datom [^Datom d]
  (-> (hash (.-e d))
      (combine-hashes (hash (.-a d)))
      (combine-hashes (value-hash (.-v d)))))

(defn- equiv-datom [^Datom d ^Datom o]
  (and (== (.-e d) (.-e o))
       (= (.-a d) (.-a o))
       ;; `a=`, not `=`: the INDEX already calls two equal-content arrays the
       ;; same value (`compare-value` returns 0), so datom equality has to
       ;; agree or `distinct`, a set of datoms, and `:hash` disagree with the
       ;; tree they came out of.
       (da/a= (.-v d) (.-v o))))

(defn- seq-datom [^Datom d]
  (list (.-e d) (.-a d) (.-v d) (datom-tx d) (datom-added d)))

;; keep it fast by duplicating for both keyword and string cases
;; instead of using sets or some other matching func
(defn- val-at-datom [^Datom d k not-found]
  (case k
    :e (.-e d) "e" (.-e d)
    :a (.-a d) "a" (.-a d)
    :v (.-v d) "v" (.-v d)
    :tx (datom-tx d)
    "tx" (datom-tx d)
    :added (datom-added d)
    "added" (datom-added d)
    not-found))

(defn- nth-datom
  ([^Datom d ^long i]
   (case i
     0 (.-e d)
     1 (.-a d)
     2 (.-v d)
     3 (datom-tx d)
     4 (datom-added d)
     #?(:clj  (throw (IndexOutOfBoundsException.))
        :cljs (throw (js/Error. (str "Datom/-nth: Index out of bounds: " i))))))
  ([^Datom d ^long i not-found]
   (case i
     0 (.-e d)
     1 (.-a d)
     2 (.-v d)
     3 (datom-tx d)
     4 (datom-added d)
     not-found)))

(defn- ^Datom assoc-datom [^Datom d k v]
  (case k
    :e (datom v (.-a d) (.-v d) (datom-tx d) (datom-added d))
    :a (datom (.-e d) v (.-v d) (datom-tx d) (datom-added d))
    :v (datom (.-e d) (.-a d) v (datom-tx d) (datom-added d))
    :tx (datom (.-e d) (.-a d) (.-v d) v (datom-added d))
    :added (datom (.-e d) (.-a d) (.-v d) (datom-tx d) v)
    (throw (#?(:clj IllegalArgumentException. :cljs js/Error.)
            (str "invalid key for #datahike/Datom: " k)))))

;; printing and reading
;; #datomic/DB {:schema <map>, :datoms <vector of [e a v tx]>}

(defn ^Datom datom-from-reader [vec]
  (apply datom vec))

#?(:clj
   (defmethod print-method Datom [^Datom d, ^java.io.Writer w]
     (.write w (str "#datahike/Datom "))
     (binding [*out* w]
       (pr [(.-e d) (.-a d) (.-v d) (datom-tx d) (datom-added d)]))))

;; ----------------------------------------------------------------------------
;; datom cmp macros/funcs
;;

#?(:clj
   (defmacro combine-cmp [& comps]
     (loop [comps (reverse comps)
            res (num 0)]
       (if (not-empty comps)
         (recur
          (next comps)
          `(let [c# ~(first comps)]
             (if (== 0 c#)
               ~res
               c#)))
         res))))

(defn cmp [o1 o2]
  (if (nil? o1) 0
      (if (nil? o2) 0
          (compare o1 o2))))

(defn long-cmp [^long a ^long b]
  #?(:clj  (Long/compare a b)
     :cljs (- a b)))

(defn boolean-cmp [a b]
  #?(:clj  (Boolean/compare ^Boolean a ^Boolean b)
     :cljs (- a b)))

(comment
;; Slower cmp-* fns allows for datom fields to be nil.
;; Such datoms come from slice method where they are used as boundary markers.

  (defn cmp-datoms-eavt [^Datom d1, ^Datom d2]
    (combine-cmp
     (#?(:clj Long/compare :cljs -) (.-e d1) (.-e d2))
     (cmp (.-a d1) (.-a d2))
     (cmp (.-v d1) (.-v d2))
     (#?(:clj Long/compare :cljs -) (datom-tx d1) (datom-tx d2))))

  (defn cmp-datoms-aevt [^Datom d1, ^Datom d2]
    (combine-cmp
     (cmp (.-a d1) (.-a d2))
     (#?(:clj Long/compare :cljs -) (.-e d1) (.-e d2))
     (cmp (.-v d1) (.-v d2))
     (#?(:clj Long/compare :cljs -) (datom-tx d1) (datom-tx d2))))

  (defn cmp-datoms-avet [^Datom d1, ^Datom d2]
    (combine-cmp
     (cmp (.-a d1) (.-a d2))
     (cmp (.-v d1) (.-v d2))
     (#?(:clj Long/compare :cljs -) (.-e d1) (.-e d2))
     (#?(:clj Long/compare :cljs -) (datom-tx d1) (datom-tx d2))))

  (defn cmp-temporal-datoms-eavt [^Datom d1, ^Datom d2]
    (combine-cmp
     (#?(:clj Long/compare :cljs -) (.-e d1) (.-e d2))
     (cmp (.-a d1) (.-a d2))
     (cmp (.-v d1) (.-v d2))
     (#?(:clj Long/compare :cljs -) (datom-tx d1) (datom-tx d2))
     (#?(:clj Boolean/compare :cljs -) (datom-added d1) (datom-added d2))))

  (defn cmp-temporal-datoms-aevt [^Datom d1, ^Datom d2]
    (combine-cmp
     (cmp (.-a d1) (.-a d2))
     (#?(:clj Long/compare :cljs -) (.-e d1) (.-e d2))
     (cmp (.-v d1) (.-v d2))
     (#?(:clj Long/compare :cljs -) (datom-tx d1) (datom-tx d2))
     (#?(:clj Boolean/compare :cljs -) (datom-added d1) (datom-added d2))))

  (defn cmp-temporal-datoms-avet [^Datom d1, ^Datom d2]
    (combine-cmp
     (cmp (.-a d1) (.-a d2))
     (cmp (.-v d1) (.-v d2))
     (#?(:clj Long/compare :cljs -) (.-e d1) (.-e d2))
     (#?(:clj Long/compare :cljs -) (datom-tx d1) (datom-tx d2))
     (#?(:clj Boolean/compare :cljs -) (datom-added d1) (datom-added d2)))))

;; fast versions without nil checks

(defn cmp-attr-quick [a1 a2]
  ;; either both are keywords or both are strings
  #?(:cljs
     (if (keyword? a1)
       (-compare a1 a2)
       (garray/defaultCompare a1 a2))
     :clj
     (.compareTo ^Comparable a1 a2)))

(defn- class-name
  "A STRING naming the type, on both platforms.

   The cljs branch used to return `(type x)` — a constructor object — and the
   only caller compared two of them with `compare`, which throws on cljs for
   anything that is not a number, string, array or boolean. So the fallback that
   exists to keep the order total was itself an exception there."
  [x]
  #?(:clj (when (some? x) (.getName (class x)))
     :cljs (when (some? x) (str (type x)))))

(defn- class-identical? [a b]
  #?(:clj (identical? (class a) (class b))
     :cljs (identical? (type a) (type b))))

(defn- class-order
  "Order two values of DIFFERENT types by type name.

   A tie-break, not a judgement: it exists so that an attribute holding both a
   long and a string still has a total order. It is only correct BECAUSE the
   types differ — for two values of the same type it returns 0, which a sorted
   set reads as \"the same datom\". That is what `safe-compare` did for every
   incomparable pair, and it is why a card-many attribute holding four maps
   stored one."
  [a b]
  (compare (class-name a) (class-name b)))

(defn- hash-order
  "Order two values of the same type that have no order of their own.

   DataScript settles this with `(int-compare (ihash x) (ihash y))` and has for
   years; this is the same idea with the collision made loud. The hash never
   decides EQUALITY — `a=` has already answered that — so this only ranks values
   already known to be unequal, and value semantics are preserved: equal values
   compare 0 through `a=`, never through a hash.

   What a hash cannot do is separate two unequal values that happen to share one.
   32 bits means a collision is near-certain somewhere in a few tens of thousands
   of values, and returning 0 there would silently drop one — the exact bug this
   whole change exists to remove. So a collision throws instead. It is
   astronomically rare per pair, and when it happens the caller learns rather
   than loses data."
  [a b]
  (let [ha (hash a) hb (hash b)]
    (cond
      (< ha hb) -1
      (> ha hb) 1
      :else
      (throw (ex-info (str "Cannot order two distinct values of type "
                           (class-name a) ": they are not comparable and their "
                           "hashes collide. Give this attribute a comparable "
                           ":db/valueType.")
                      {:error :datahike/incomparable-values
                       :type (class-name a)
                       :hash ha})))))

(defn- collision-refusal?
  "Is this our own `hash-order` refusal rather than a failed comparison?

   It has to survive every catch on the way out. `compare-sequential` recurses,
   so the refusal is raised on an INNER pair while the catch tests the OUTER
   one — and when the two containers have different classes (`[a]` against
   `(list b)`), `class-order` answered and the refusal was swallowed. That turns
   the one guarantee `hash-order` exists to provide, never silently merging two
   distinct values, back into a silent merge via `cmp-nil`."
  [e]
  (= :datahike/incomparable-values (:error (ex-data e))))

(defn- comparable?
  "Does this value carry its own order — i.e. will `compare` answer for it?

   The cljs arm is NOT just `satisfies? IComparable`, and getting that wrong is
   expensive. `cljs.core/compare` handles numbers BEFORE the protocol check and
   strings, booleans and arrays after it, so none of those satisfy `IComparable`
   — a JS number is a primitive, not a type that can extend a protocol. Asking
   only the protocol therefore answered FALSE for every number and string, which
   sent them to `hash-order` and ordered the indices by HASH. The Node suite
   showed it as updates that appeared not to take effect (an `as-of` read
   returning the pre-update value): 111 failures.

   So this mirrors `cljs.core/compare`'s own capability set, which is also what
   DataScript's `value-compare` does with its explicit
   `(or (number? x) (string? x) (array? x) (true? x) (false? x))` arm.

   The JVM arm has the mirror-image trap, and DataScript has an explicit arm for
   that too: NOT EVERY `Number` IS `Comparable`. `clojure.lang.BigInt` is the one
   that matters, because `:db.type/bigint` is a declared, supported value type —
   and `Util.compare` checks `instanceof Number` BEFORE `Comparable`, so asking
   only about `Comparable` is a narrower gate than the `compare` it guards.
   Missing it sent every bigint to `hash-order`:

     (sort compare-value (map bigint (range 20)))
       => [3N 18N 2N 4N 14N 10N 8N 7N 0N …]

   i.e. every `:db.type/bigint` index ordered by hash, `seek-datoms` returning
   the wrong range, `(compare-value 5N 0)` = -7 against `(compare-value 0 5N)`
   = -1, and a transaction of two bigints whose hashes collide — 6 pairs in
   300000 random values — refused outright by `hash-order`."
  [x]
  #?(:clj (or (instance? Number x)
              (instance? Comparable x))
     :cljs (or (number? x)
               (string? x)
               (true? x)
               (false? x)
               (array? x)
               (satisfies? IComparable x))))

(declare compare-value)

(defn- compare-sequential
  "Order two sequential values element-wise, recursing through `compare-value`.

   Deliberately the same shape as `clojure.lang.APersistentVector.compareTo`:
   SHORTER FIRST, then element by element. Matching it is the point — a plain
   vector of Comparables must sort exactly where it always did, because any
   change here reorders live indices.

   NIL ELEMENTS are handled here rather than left to `compare-value`, and that
   is not defensive coding: a COMPOSITE tuple is nil-padded when its components
   are missing, so a nil inside a vector is ordinary rather than exotic.
   `compare` used to absorb them (nil sorts before everything); routing them
   through `compare-value` instead reached `class-order`, and `(class nil)` is
   nil, so `tuples-test` died with a NullPointerException out of `class-name`.
   Nil-first preserves what `compare` did, so the order does not move."
  [a b]
  (let [ca (count a) cb (count b)]
    (cond
      (< ca cb) -1
      (> ca cb) 1
      :else (loop [xs (seq a) ys (seq b)]
              (if (nil? xs)
                0
                (let [x (first xs)
                      y (first ys)
                      c (cond
                          (and (nil? x) (nil? y)) 0
                          (nil? x) -1
                          (nil? y) 1
                          :else (compare-value x y))]
                  (if (zero? c)
                    (recur (next xs) (next ys))
                    c)))))))

(defn compare-value
  "Compare two values with cross-platform UUID compatibility.
   CLJS UUID comparison is adjusted to match CLJ's signed comparison,
   ensuring consistent ordering when indices are built on CLJ and queried on CLJS.
   Byte/float/double arrays are compared element-wise via
   datahike.array/compare-arrays, since primitive arrays do not implement
   Comparable.

   ## Arrays INSIDE a value, not only as one

   A `:db.type/tuple` whose `:db/tupleTypes` names one of the three array types
   arrives as a VECTOR holding an array, and the array check above only looks at
   the top level. `compare` then reached the array through
   `APersistentVector.compareTo` and threw `ClassCastException: class [B cannot
   be cast to class java.lang.Comparable` — so on released datahike that schema
   was not merely mis-ordered, it was unusable. Measured, per operation:

     first write, explicit retract          worked
     card-many, several values              8 transacted, 1 STORED (silent)
     card-one update                        threw, from `transact-add`
     `:db/index true`                       threw, from `cmp-datoms-avet-quick`
     `:db.unique/identity`                  threw, from `transact-add`

   Two faces, one cause. The throwing sites call this directly; the silent one
   goes through `safe-compare`, whose fallback compares CLASS NAMES — equal for
   two vectors, so it answered \"incomparable\" with \"equal\", and a sorted set
   reads equal as a duplicate. Repairing this function fixes all four, which is
   why the fix is here and not at the call sites.

   Only values that CONTAIN an array change order; anything already Comparable
   takes the same path it always did, so existing indices do not move."
  [v1 v2]
  #?(:clj (try
            (cond
              (and (da/value-array? v1) (da/value-array? v2))
              (da/compare-arrays v1 v2)

            ;; Checked AFTER the array case and before `compare`, so a scalar —
            ;; every value in an ordinary database — reaches `compare` having
            ;; paid one extra predicate.
              (and (sequential? v1) (sequential? v2))
              (compare-sequential v1 v2)

            ;; Carries its own order: the ordinary case, and the only one an
            ;; ordinary database ever reaches. `compare` can still throw here
            ;; when the two are Comparable but of DIFFERENT types (a long
            ;; against a string, under `:schema-flexibility :read`) — the catch
            ;; below settles that by type name.
              (and (comparable? v1) (comparable? v2)) (compare v1 v2)

              ;; No order of its own. Checked BEFORE the hash so that equality is
              ;; decided by `a=` and never by a hash — which is what keeps value
              ;; semantics intact.
              (da/a= v1 v2) 0
              (not (class-identical? v1 v2)) (class-order v1 v2)
              :else (hash-order v1 v2))
            ;; Two Comparables of DIFFERENT types — a long against a string in
            ;; one attribute, which `:schema-flexibility :read` permits.
            ;; `(compare 1 "x")` throws, and only `cmp-nil` used to catch it, so
            ;; the same pair ordered fine through a slice and crashed through
            ;; `cmp-datoms-avet-quick`. Settled here so every caller agrees.
            ;; A hash collision from `hash-order` is an ex-info, not a
            ;; ClassCastException, so it passes through untouched.
            (catch ClassCastException e
              (if (class-identical? v1 v2)
                (throw e)
                (class-order v1 v2))))
     :cljs
     (try
       (cond
         (and (uuid? v1) (uuid? v2))
       ;; Match Java's signed UUID comparison where MSB is treated as signed
       ;; In signed comparison: 0x8... is negative, so 0x8... < 0x0...
         (let [s1 (.-uuid ^cljs.core/UUID v1)
               s2 (.-uuid ^cljs.core/UUID v2)
               c1 (.charCodeAt s1 0)
               c2 (.charCodeAt s2 0)
             ;; charCode 56 = "8", chars 8-F are "negative" in signed
               neg1 (>= c1 56)
               neg2 (>= c2 56)]
           (cond
             (and neg1 (not neg2)) -1  ;; v1 "negative", v2 "positive" → v1 < v2
             (and neg2 (not neg1)) 1   ;; v2 "negative", v1 "positive" → v1 > v2
             :else (compare s1 s2)))   ;; same sign → string compare works

         (and (da/value-array? v1) (da/value-array? v2))
         (da/compare-arrays v1 v2)

         (and (sequential? v1) (sequential? v2))
         (compare-sequential v1 v2)

         (and (comparable? v1) (comparable? v2)) (compare v1 v2)

         (da/a= v1 v2) 0
         (not (class-identical? v1 v2)) (class-order v1 v2)
         :else (hash-order v1 v2))
       (catch :default e
         ;; cljs has no ClassCastException, so this catch is broad and must
         ;; filter explicitly. The class check alone is not enough: the refusal
         ;; may have been raised on an INNER pair by `compare-sequential` while
         ;; the classes of the OUTER pair differ, and then it would be answered
         ;; away by name.
         (if (or (collision-refusal? e) (class-identical? v1 v2))
           (throw e)
           (class-order v1 v2))))))

(defn- safe-compare
  "`compare-value`, with the last resort for two values of DIFFERENT types.

   This used to catch everything and answer with `class-order` unconditionally —
   which is right when the types differ and catastrophic when they do not, since
   two values of one type get 0 and a sorted set reads 0 as \"already present\".
   That is how a card-many attribute holding four maps stored one, and why the
   same root cause showed up as a crash in the `*-quick` comparators (no catch)
   and as silent deduplication here.

   `compare-value` now settles same-type values itself, so the only thing that
   should still reach this catch is a pair of mutually-incomparable types — and
   if the types are in fact identical, we have no answer and must not invent
   one, so it rethrows."
  [a b]
  (try
    (compare-value a b)
    (catch #?(:clj Exception :cljs js/Error) e
      (if (or (collision-refusal? e) (class-identical? a b))
        (throw e)
        (class-order a b)))))

(defn cmp-nil [o1 o2]
  (if (nil? o1) nil
      (if (nil? o2) nil
          (safe-compare o1 o2))))

(defn type-hint-datom [x]
  (vary-meta x assoc :tag `Datom))

(defn cmp-val [val]
  (case val
    :e (fn [^Datom d1 ^Datom d2] (long-cmp (.-e d1) (.-e d2)))
    :a (fn [^Datom d1 ^Datom d2] (cmp-nil (.-a d1) (.-a d2)))
    :v (fn [^Datom d1 ^Datom d2] (cmp-nil (.-v d1) (.-v d2)))
    :tx (fn [^Datom d1 ^Datom d2] (long-cmp (datom-tx d1) (datom-tx d2)))
    :added (fn [^Datom d1 ^Datom d2] (long-cmp (datom-added d1) (datom-added d2)))))

(defn cmp-val-expr [val d1 d2]
  (case val
    :e `(long-cmp (.-e ~d1) (.-e ~d2))
    :a `(cmp-nil (.-a ~d1) (.-a ~d2))
    :v `(cmp-nil (.-v ~d1) (.-v ~d2))
    :tx `(long-cmp (datom-tx ~d1) (datom-tx ~d2))
    :added `(boolean-cmp (datom-added ~d1) (datom-added ~d2))))

(defn cmp-datoms-eavt-quick [^Datom d1, ^Datom d2]
  (combine-cmp
   (long-cmp (.-e d1) (.-e d2))
   (cmp-attr-quick (.-a d1) (.-a d2))
   (compare-value (.-v d1) (.-v d2))
   (long-cmp (datom-tx d1) (datom-tx d2))))

(defn cmp-datoms-aevt-quick [^Datom d1, ^Datom d2]
  (combine-cmp
   (cmp-attr-quick (.-a d1) (.-a d2))
   (long-cmp (.-e d1) (.-e d2))
   (compare-value (.-v d1) (.-v d2))
   (long-cmp (datom-tx d1) (datom-tx d2))))

(defn cmp-datoms-avet-quick [^Datom d1, ^Datom d2]
  (combine-cmp
   (cmp-attr-quick (.-a d1) (.-a d2))
   (compare-value (.-v d1) (.-v d2))
   (long-cmp (.-e d1) (.-e d2))
   (long-cmp (datom-tx d1) (datom-tx d2))))

(defn cmp-temporal-datoms-eavt-quick [^Datom d1, ^Datom d2]
  (combine-cmp
   (long-cmp (.-e d1) (.-e d2))
   (cmp-attr-quick (.-a d1) (.-a d2))
   (compare-value (.-v d1) (.-v d2))
   (long-cmp (datom-tx d1) (datom-tx d2))
   (boolean-cmp (datom-added d1) (datom-added d2))))

(defn cmp-temporal-datoms-aevt-quick [^Datom d1, ^Datom d2]
  (combine-cmp
   (cmp-attr-quick (.-a d1) (.-a d2))
   (long-cmp (.-e d1) (.-e d2))
   (compare-value (.-v d1) (.-v d2))
   (long-cmp (datom-tx d1) (datom-tx d2))
   (boolean-cmp (datom-added d1) (datom-added d2))))

(defn cmp-temporal-datoms-avet-quick [^Datom d1, ^Datom d2]
  (combine-cmp
   (cmp-attr-quick (.-a d1) (.-a d2))
   (compare-value (.-v d1) (.-v d2))
   (long-cmp (.-e d1) (.-e d2))
   (long-cmp (datom-tx d1) (datom-tx d2))
   (boolean-cmp (datom-added d1) (datom-added d2))))

;; EA-only comparator for merge lookups (compare e,a only, ignoring v and tx)
;; Used by compiled query engine for seekGE merge lookups where we only need
;; to find the first datom with matching (entity, attribute).
(defn cmp-datoms-ea [^Datom d1, ^Datom d2]
  (combine-cmp
   (long-cmp (.-e d1) (.-e d2))
   (cmp-attr-quick (.-a d1) (.-a d2))))

;; Prefix comparators for existence checks (compare e,a,v only, ignoring tx)
;; Used by insert to check if ANY datom with same e,a,v exists
(defn cmp-datoms-eavt-prefix [^Datom d1, ^Datom d2]
  (combine-cmp
   (long-cmp (.-e d1) (.-e d2))
   (cmp-attr-quick (.-a d1) (.-a d2))
   (compare-value (.-v d1) (.-v d2))))

(defn cmp-datoms-aevt-prefix [^Datom d1, ^Datom d2]
  (combine-cmp
   (cmp-attr-quick (.-a d1) (.-a d2))
   (long-cmp (.-e d1) (.-e d2))
   (compare-value (.-v d1) (.-v d2))))

(defn cmp-datoms-avet-prefix [^Datom d1, ^Datom d2]
  (combine-cmp
   (cmp-attr-quick (.-a d1) (.-a d2))
   (compare-value (.-v d1) (.-v d2))
   (long-cmp (.-e d1) (.-e d2))))

(defn diff-sorted [a b cmp]
  (loop [only-a []
         only-b []
         both []
         a a
         b b]
    (cond
      (empty? a) [(not-empty only-a) (not-empty (into only-b b)) (not-empty both)]
      (empty? b) [(not-empty (into only-a a)) (not-empty only-b) (not-empty both)]
      :else
      (let [first-a (first a)
            first-b (first b)
            diff (cmp first-a first-b)]
        (cond
          (== diff 0) (recur only-a only-b (conj both first-a) (next a) (next b))
          (< diff 0) (recur (conj only-a first-a) only-b both (next a) b)
          (> diff 0) (recur only-a (conj only-b first-b) both a (next b)))))))

(defn coll->datoms
  "Converts a collection with elements of form [e a v t] into a collection of Datoms."
  [coll]
  (map
   (fn [[e a v t]]
     (datom e a v t))
   coll))

(defn index-type->cmp-quick
  ([index-type] (index-type->cmp-quick index-type true))
  ([index-type current?] (if current?
                           (case index-type
                             :aevt cmp-datoms-aevt-quick
                             :avet cmp-datoms-avet-quick
                             cmp-datoms-eavt-quick)
                           (case index-type
                             :aevt cmp-temporal-datoms-aevt-quick
                             :avet cmp-temporal-datoms-avet-quick
                             cmp-temporal-datoms-eavt-quick))))

;; Comparators for replace operations - only compare key parts, not values
(defn cmp-datoms-eavt-replace
  "Compare datoms by (e,a) only, ignoring v and tx.
   Used for replace operations where value changes."
  [^Datom d1, ^Datom d2]
  (combine-cmp
   (long-cmp (.-e d1) (.-e d2))
   (cmp-attr-quick (.-a d1) (.-a d2))))

(defn cmp-datoms-av-only
  "Compare datoms by (a,v) only, ignoring e and tx.
   Used for point lookup in AVET index when entity is unbound."
  [^Datom d1, ^Datom d2]
  (combine-cmp
   (cmp-attr-quick (.-a d1) (.-a d2))
   (compare-value (.-v d1) (.-v d2))))

(defn cmp-datoms-aevt-replace
  "Compare datoms by (a,e) only, ignoring v and tx.
   Used for replace operations where value changes."
  [^Datom d1, ^Datom d2]
  (combine-cmp
   (cmp-attr-quick (.-a d1) (.-a d2))
   (long-cmp (.-e d1) (.-e d2))))

(defn cmp-datoms-avet-replace
  "Compare datoms by (a,e) only, ignoring v and tx.
   Used for replace operations in AVET index where value changes."
  [^Datom d1, ^Datom d2]
  (combine-cmp
   (cmp-attr-quick (.-a d1) (.-a d2))
   (long-cmp (.-e d1) (.-e d2))))

(defn index-type->cmp-replace
  "Get comparator for replace operations.
   Only compares the logical key parts, allowing value/tx to differ."
  [index-type]
  (case index-type
    :aevt cmp-datoms-aevt-replace
    :avet cmp-datoms-avet-replace
    cmp-datoms-eavt-replace))

(defn index-type->cmp-prefix
  "Get prefix comparator for (e,a,v) matching, ignoring tx.
   Used for existence checks in insert operations."
  [index-type]
  (case index-type
    :aevt cmp-datoms-aevt-prefix
    :avet cmp-datoms-avet-prefix
    cmp-datoms-eavt-prefix))
