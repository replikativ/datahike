(ns datahike.test.nested-array-value-test
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
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.datom :as dd]
            [datahike.test.utils :as utils]))

(defn- ba [& xs] (byte-array (map unchecked-byte xs)))

(defn- conn-with [attr]
  (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
             :keep-history? false :schema-flexibility :write}]
    (d/create-database cfg)
    (let [c (d/connect cfg)]
      (d/transact c [attr])
      c)))

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
        (d/release c)))))

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
