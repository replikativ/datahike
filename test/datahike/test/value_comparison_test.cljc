(ns datahike.test.value-comparison-test
  "`:db.type/tuple` whose `:db/tupleTypes` names an array type.

   `compare-value` special-cased byte/float/double arrays only where the VALUE
   is one. A tuple holding one is a vector, so `compare` reached the array
   through `APersistentVector.compareTo` and threw. That made a declared,
   supported schema unusable rather than merely mis-ordered — and in the one
   place it did not throw, it lost data instead.

   Two faces, one cause: the `*-quick` and `*-prefix` comparator families call
   `compare-value` directly, so they threw; `cmp-val`'s `:v` case goes through
   `safe-compare`, whose fallback compares CLASS NAMES — equal for two vectors,
   so it answered \"incomparable\" with \"equal\" and a sorted set read that as a
   duplicate."
  (:require #?(:clj [clojure.test :refer [deftest testing is]]
               :cljs [cljs.test :refer-macros [deftest testing is]])
            #?(:clj [datahike.api :as d])
            [datahike.datom :as dd]
            #?(:clj [datahike.test.utils :as utils])))

(defn- ba [& xs]
  #?(:clj (byte-array (map unchecked-byte xs))
     :cljs (js/Uint8Array.from (clj->js (map #(bit-and % 0xff) xs)))))

(deftest scalar-nan-has-a-total-numeric-order
  (doseq [nan #?(:clj [Double/NaN Float/NaN
                       (Double/longBitsToDouble 9221120237041090561)]
                 :cljs [js/NaN])
          finite [##-Inf -1 0 -0.0 0.0 1 1.5 ##Inf]]
    (is (pos? (dd/compare-value nan finite)))
    (is (neg? (dd/compare-value finite nan)))
    (is (zero? (dd/compare-value nan nan)))
    (is (pos? (dd/compare-value [nan] [finite]))))
  (is (zero? (dd/compare-value -0.0 0.0)))
  #?(:clj (is (zero? (dd/compare-value Float/NaN Double/NaN)))))

#?(:clj
   (defn- conn-with [attr]
     (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
                :keep-history? false :schema-flexibility :write}]
       (d/create-database cfg)
       (let [c (d/connect cfg)]
         (d/transact c [attr])
         c))))

#?(:clj
   (deftest scalar-nan-writes-and-index-lookups
     (doseq [[type finite nan] [[:db.type/double 0.0 Double/NaN]
                                [:db.type/float (float 0) Float/NaN]]]
       (let [c (conn-with {:db/ident :n/value :db/valueType type
                           :db/cardinality :db.cardinality/one :db/index true})]
         (try
           (d/transact c [{:db/id 100 :n/value finite}])
           (let [report (d/transact c [[:db/add 100 :n/value nan]])]
             (is (= 2 (count (filter #(= :n/value (:a %)) (:tx-data report)))))
             (is (Double/isNaN (double (:n/value (d/entity @c 100))))))
           (d/transact c [{:db/id 101 :n/value finite}])
           (is (= [101] (mapv :e (d/datoms @c :avet :n/value finite))))
           (is (= [100] (mapv :e (d/datoms @c :avet :n/value nan))))
           (is (= [101 100] (mapv :e (d/datoms @c :avet :n/value))))
           (let [report (d/transact c [[:db/add 100 :n/value finite]])]
             (is (= 2 (count (filter #(= :n/value (:a %)) (:tx-data report)))))
             (is (zero? (:n/value (d/entity @c 100)))))
           (finally (d/release c)))))))

#?(:clj
   (deftest scalar-nan-index-order-survives-reconnect
     (let [root (java.nio.file.Files/createTempDirectory
                 "datahike-nan-index-"
                 (make-array java.nio.file.attribute.FileAttribute 0))
           cfg {:store {:backend :file :id (java.util.UUID/randomUUID)
                        :path (str (.resolve root "store"))}
                :keep-history? true :schema-flexibility :write}]
       (try
         (d/create-database cfg)
         (let [c (d/connect cfg)]
           (try
             (d/transact c [{:db/ident :n/value :db/valueType :db.type/double
                             :db/cardinality :db.cardinality/many :db/index true}])
             ;; Put NaN on the smaller entity: ordering by entity before value
             ;; would give the wrong AVET order after reopening.
             (d/transact c [[:db/add 100 :n/value Double/NaN]
                            [:db/add 101 :n/value 0.0]
                            [:db/add 101 :n/value Double/NaN]])
             (is (= 2 (count (d/datoms @c :eavt 101 :n/value))))
             (finally (d/release c))))
         (let [c (d/connect cfg)]
           (try
             (is (= [101 100 101] (mapv :e (d/datoms @c :avet :n/value))))
             (is (= [100 101] (mapv :e (d/datoms @c :avet :n/value Double/NaN))))
             (is (= [101] (mapv :e (d/datoms @c :avet :n/value 0.0))))
             (finally (d/release c))))
         (finally
           (d/delete-database cfg)
           (java.nio.file.Files/deleteIfExists root))))))

#?(:clj
   (deftest a-tuple-holding-an-array-is-usable
     (testing "each of these was measured on the released implementation; three
            threw ClassCastException and one silently stored one datom of eight."

       (testing "card-many keeps every value — this is the silent one"
         (let [c (conn-with {:db/ident :m/sig :db/valueType :db.type/tuple
                             :db/tupleTypes [:db.type/bytes :db.type/string]
                             :db/cardinality :db.cardinality/many})]
           (d/transact c [{:db/id 100 :m/sig (vec (for [i (range 8)]
                                                    [(ba i (inc i)) (str "v" i)]))}])
           (is (= 8 (count (d/datoms @c {:index :eavt :components [100 :m/sig]})))
               "8 transacted, 8 stored; it used to store 1")
           (d/release c)))

       (testing "card-one updates rather than throwing"
         (let [c (conn-with {:db/ident :o/sig :db/valueType :db.type/tuple
                             :db/tupleTypes [:db.type/bytes :db.type/string]
                             :db/cardinality :db.cardinality/one})]
           (d/transact c [{:db/id 100 :o/sig [(ba 1 1) "first"]}])
           (d/transact c [{:db/id 100 :o/sig [(ba 9 9) "second"]}])
           (let [ds (vec (d/datoms @c {:index :eavt :components [100 :o/sig]}))]
             (is (= 1 (count ds)) "the update replaced, it did not accumulate")
             (is (= "second" (second (nth (first ds) 2)))
                 "and the surviving value is the new one"))
           (d/release c)))

       (testing "an indexed attribute builds an AVET entry and is findable BY VALUE
              — this one failed inside `cmp-datoms-avet-quick`, i.e. in the
              index itself rather than in the transaction"
         (let [c (conn-with {:db/ident :i/sig :db/valueType :db.type/tuple
                             :db/tupleTypes [:db.type/bytes :db.type/string]
                             :db/cardinality :db.cardinality/one
                             :db/index true})]
           (d/transact c (vec (for [i (range 5)]
                                {:db/id (+ 100 i) :i/sig [(ba i) (str "v" i)]})))
           (is (= 5 (count (d/datoms @c {:index :avet :components [:i/sig]}))))
           (is (= 1 (count (d/q '[:find ?e :in $ ?v :where [?e :i/sig ?v]]
                                @c [(ba 3) "v3"])))
               "looked up by a byte array with the same CONTENT, not the same object")
           (d/release c)))

       (testing "unique identity keeps two distinct entities distinct — the failure
              mode to fear here is not the exception but two entities merging"
         (let [c (conn-with {:db/ident :u/sig :db/valueType :db.type/tuple
                             :db/tupleTypes [:db.type/bytes :db.type/string]
                             :db/cardinality :db.cardinality/one
                             :db/unique :db.unique/identity})]
           (d/transact c [{:u/sig [(ba 1) "a"] :db/doc "one"}
                          {:u/sig [(ba 2) "b"] :db/doc "two"}])
           (is (= 2 (count (d/q '[:find ?e :where [?e :u/sig _]] @c))))
           (is (= ["one" "two"] (sort (map first (d/q '[:find ?doc :where [?e :db/doc ?doc]] @c)))))
           (d/release c)))

       (testing "retracting one tuple value still removes exactly that datom"
         (let [c (conn-with {:db/ident :r/sig :db/valueType :db.type/tuple
                             :db/tupleTypes [:db.type/bytes :db.type/string]
                             :db/cardinality :db.cardinality/many})]
           (d/transact c [{:db/id 100 :r/sig [[(ba 1) "a"] [(ba 2) "b"]]}])
           (d/transact c [[:db/retract 100 :r/sig [(ba 1) "a"]]])
           (is (= 1 (count (d/datoms @c {:index :eavt :components [100 :r/sig]}))))
           (d/release c))))))

(deftest the-order-of-values-that-already-worked-does-not-move
  (testing "this is the migration hazard, not the fix. `compare-value` orders
            every stored index, so any pair whose answer CHANGES reorders a
            durable tree. `compare-sequential` therefore mirrors
            `APersistentVector.compareTo` exactly — shorter first, then
            element-wise — and only recurses where `compare` would have thrown."
    (is (neg? (dd/compare-value [1 2] [1 3])))
    (is (pos? (dd/compare-value [1 3] [1 2])))
    (is (zero? (dd/compare-value [1 2] [1 2])))
    (is (neg? (dd/compare-value [1] [1 2])) "shorter sorts first")
    (is (pos? (dd/compare-value [1 2] [1])))
    (is (neg? (dd/compare-value [[1] 2] [[1] 3])) "and nested vectors recurse")
    (is (zero? (dd/compare-value [] [])))
    (is (neg? (dd/compare-value [] [1])))

    (testing "scalars are untouched — they never reach the new branch"
      (is (neg? (dd/compare-value 1 2)))
      (is (neg? (dd/compare-value "a" "b")))
      (is (neg? (dd/compare-value :a :b)))
      (is (zero? (dd/compare-value 1 1))))

    (testing "and a top-level array still compares by content"
      (is (zero? (dd/compare-value (ba 1 2 3) (ba 1 2 3))))
      (is (neg? (dd/compare-value (ba 1) (ba 2)))))))

(deftest a-tuple-holding-an-array-orders-by-content
  (testing "the property the index needs: two tuples with equal bytes are ONE
            value, two with different bytes are ordered and never equal. The old
            behaviour had both wrong at once — equal content compared unequal
            (different objects), and different content compared EQUAL (class
            names)."
    (is (zero? (dd/compare-value [(ba 1 2) "a"] [(ba 1 2) "a"]))
        "equal content, distinct objects")
    (is (not (zero? (dd/compare-value [(ba 1 2) "a"] [(ba 9 9) "a"])))
        "different content must not collapse")
    (is (= (- (dd/compare-value [(ba 1) "a"] [(ba 2) "a"]))
           (dd/compare-value [(ba 2) "a"] [(ba 1) "a"]))
        "antisymmetric, which a sorted set relies on")

    (testing "transitivity, over a shuffled corpus — a comparator that violates
              it corrupts a sorted set silently rather than throwing"
      (let [vs (vec (for [i (range 25)] [(ba (mod i 7) i) (str "v" (mod i 3))]))
            sorted (sort dd/compare-value vs)]
        (is (every? (fn [[a b]] (not (pos? (dd/compare-value a b))))
                    (partition 2 1 sorted))
            "the sorted sequence is non-decreasing under the comparator")))))

;; ---------------------------------------------------------------------------
;; Values with no order of their own.
;;
;; DataScript settles these with `(int-compare (ihash x) (ihash y))` and has for
;; years — `value-compare` in `datascript.db`. datahike inherited the shape of
;; that function but not its last two arms, so where DataScript ranks two
;; incomparable values, datahike compared CLASS NAMES: equal for two values of
;; one type, hence 0, hence one datom where the user wrote four.

#?(:clj
   (defn- many-conn
     "Schema-on-read, with `:obj` DECLARED cardinality-many.

   The declaration matters and is easy to get wrong: without it the attribute is
   cardinality-ONE, so four values legitimately become one datom and a test that
   omits it measures nothing. A first version of this measurement did exactly
   that and reported data loss for `:db.type/long`."
     []
     (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
                :keep-history? false :schema-flexibility :read}]
       (d/create-database cfg)
       (let [c (d/connect cfg)]
         (d/transact c [{:db/ident :obj :db/cardinality :db.cardinality/many}])
         c))))

#?(:clj
   (defn- stored [vals]
     (let [c (many-conn)]
       (try
         (d/transact c [{:db/id 100 :obj vals}])
         (count (d/datoms @c {:index :eavt :components [100 :obj]}))
         (finally (d/release c))))))

#?(:clj
   (deftest values-without-an-order-are-ranked-not-merged
     (testing "each of these stored ONE datom before: the comparator had no answer
            and said 0, which a sorted set reads as a duplicate."
       (is (= 4 (stored [{:a 1} {:b 2} {:c 3} {:d 4}])) "maps")
       (is (= 4 (stored [#{1} #{2} #{3} #{4}])) "sets")
       (is (= 4 (stored (vec (repeatedly 4 #(Object.))))) "opaque java objects")
       (is (= 4 (stored (vec (map atom (range 4))))) "atoms — an in-memory cache of
                                                   mutable cells is a real use
                                                   for schema-on-read + :memory")

       (testing "and mixed types in one attribute, which schema-on-read permits.
              `(compare 1 \"x\")` throws, and only `cmp-nil` caught it — so the
              same pair ordered fine through a slice and crashed through
              `cmp-datoms-avet-quick`."
         (is (= 4 (stored [1 "x" :k {:a 1}]))))

       (testing "controls: types that always worked must be unaffected"
         (is (= 4 (stored [1 2 3 4])))
         (is (= 4 (stored ["a" "b" "c" "d"])))))))

(deftest equality-is-decided-by-equality-never-by-a-hash
  (testing "the property that keeps value semantics intact. `a=` answers before
            the hash is consulted, so the hash only RANKS values already known
            to be unequal — it never decides that two things are the same."
    #?(:clj (is (= 2 (stored [{:a 1} {:b 2} {:a 1} {:b 2}]))
                "four values, two distinct: equal maps still deduplicate"))
    (is (zero? (dd/compare-value {:a 1} {:a 1})) "distinct but equal maps")
    (is (zero? (dd/compare-value #{1 2} #{2 1})) "sets ignore insertion order")
    (is (not (zero? (dd/compare-value {:a 1} {:b 2}))))

    ;; A bare `Object` has no cljs equivalent — `js/Object` is a constructor
    ;; with value-ish semantics rather than the opaque identity this asserts.
    #?(:clj
       (testing "an opaque object is equal to itself and ordered against another"
         (let [a (Object.) b (Object.)]
           (is (zero? (dd/compare-value a a)))
           (is (not (zero? (dd/compare-value a b)))))))

    (testing "different types are ordered by type name — a tie-break that is
              only valid BECAUSE the types differ"
      (is (not (zero? (dd/compare-value {:a 1} 5))))
      (is (= (- (dd/compare-value {:a 1} 5)) (dd/compare-value 5 {:a 1}))))))

#?(:clj
   (deftest a-hash-collision-refuses-rather-than-dropping-a-value
     (testing "32 bits collide somewhere in a few tens of thousands of values, and
            returning 0 there would silently drop one — the bug this whole
            change removes. DataScript accepts that residual; we make it loud.

            Forced with two objects whose `hash` is equal by construction but
            which are not `=`."
       (let [fixed (fn [] (reify Object (hashCode [_] 42)))
             a (fixed) b (fixed)]
         (is (= 42 (hash a) (hash b)) "precondition: the hashes really do collide")
         (is (not (= a b)) "and the values really are distinct")
         (is (zero? (dd/compare-value a a)) "self is still equal")
         (is (= :datahike/incomparable-values
                (try (dd/compare-value a b) ::no-throw
                     (catch clojure.lang.ExceptionInfo e (:error (ex-data e)))))
             "distinct values with colliding hashes are refused, not merged")))))

(deftest a-nil-inside-a-tuple-sorts-first
  (testing "not a defensive edge case: a COMPOSITE tuple is nil-padded when its
            components are missing, so nil inside a vector is ordinary. `compare`
            used to absorb it — nil sorts before everything — and routing
            elements through `compare-value` instead reached `class-order`,
            where `(class nil)` is nil and `class-name` threw a
            NullPointerException. `tuples-test` caught it; these pin it.

            Nil-first is what `compare` already did, so the order does not move."
    (is (neg? (dd/compare-value [nil "b"] ["a" "b"])) "nil before a value")
    (is (pos? (dd/compare-value ["a" "b"] [nil "b"])))
    (is (zero? (dd/compare-value [nil "b"] [nil "b"])) "nil equals nil")
    (is (zero? (dd/compare-value [nil nil] [nil nil])))
    (is (neg? (dd/compare-value [nil nil] [nil "b"])))
    (is (= (- (dd/compare-value [nil 1] [2 1])) (dd/compare-value [2 1] [nil 1]))
        "antisymmetric across the nil boundary")

    (testing "and it agrees with what plain `compare` answered before"
      (is (= (compare [nil "b"] ["a" "b"]) (dd/compare-value [nil "b"] ["a" "b"])))
      (is (= (compare ["a" nil] ["a" "b"]) (dd/compare-value ["a" nil] ["a" "b"]))))))
