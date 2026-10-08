(ns datahike.test.nan-value-test
  "A NaN is EQUAL TO ITSELF and GREATER THAN every other number, in the index
   and in every query path alike.

   Three different equalities used to answer this question three different
   ways. The index compared with `clojure.core/compare`, which ranks numbers
   by `lt` -- false in both directions for a NaN -- and therefore reported a
   NaN EQUAL TO EVERY NUMBER, so a sorted set read each further value as a
   duplicate and dropped it. The query paths compared with `clojure.core/=`,
   which says a NaN differs from ITSELF, so a grouping or a distinct projection
   produced one row per occurrence. Arrays had already been brought in line
   (`array-test`); these are the same assertions for the scalar."
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing]])
   [datahike.array :as da]
   [datahike.datom :refer [compare-value]]
   [datahike.query :as dq]
   #?(:clj [datahike.api :as d])))

;; `clojure.test` compares with `=`, and `=` is precisely the equality under
;; repair here: `(= #{[##NaN]} #{[##NaN]})` is FALSE, so an assertion written
;; the obvious way fails on a correct result. Every comparison of a
;; NaN- or array-bearing result therefore goes through the canonical key --
;; the same `value-key` the engine now uses -- which is also the only honest
;; way to state "these are the same results".
(defn- canon
  "A collection of tuples as a set of canonical keys."
  [tuples]
  (into #{} (map (fn [t] (mapv da/value-key t))) tuples))

;; A test written with the `##NaN` LITERAL proves nothing about grouping, and
;; this one did until it was measured. The compiler lifts constants, so two
;; occurrences of `##NaN` in one form are the SAME boxed object
;; -- `(identical? ##NaN ##NaN)` is true -- and `clojure.core/=` opens with a
;; reference check, so the broken equality never got asked. The memory backend
;; stores the object it was handed, which kept the mask in place; every backend
;; that deserialises hands back a fresh `Double` and shows the bug. Measured on
;; the parent commit: with the literal a grouped query answered
;; `[[##NaN 2] [1.0 1]]`, correct by accident, and with distinct objects
;; `[[##NaN 1] [##NaN 1] [1.0 1]]`.
(defn- fresh-nan
  "A NaN that is its own object, as a deserialising backend returns."
  []
  #?(:clj (Double/parseDouble "NaN") :cljs (js/parseFloat "NaN")))

(deftest test-nan-ordering
  (testing "equal to itself"
    (is (zero? (compare-value ##NaN ##NaN)))
    (is (zero? (compare-value (float ##NaN) (float ##NaN))))
    (is (zero? (compare-value (float ##NaN) ##NaN))))

  (testing "greater than every other number, in both directions"
    (doseq [n [0.0 -0.0 1.0 -1.0 ##Inf ##-Inf 7 -7]]
      (is (pos? (compare-value ##NaN n)) (str "NaN vs " n))
      (is (neg? (compare-value n ##NaN)) (str n " vs NaN"))))

  (testing "a total order: sorting no longer leaves the collection untouched"
    (is (= [0.5 1.0 2.0 :datahike.array/nan :datahike.array/nan]
           (mapv da/value-key (sort compare-value [1.0 ##NaN 0.5 2.0 ##NaN])))))

  (testing "signed zero still compares EQUAL -- only NaN moved, so an index
            free of NaN keeps the order it was written with"
    (is (zero? (compare-value -0.0 0.0)))
    (is (zero? (compare-value 0.0 -0.0))))

  (testing "a NaN against a non-number keeps the by-type tie-break"
    (is (not (zero? (compare-value ##NaN "x"))))
    (is (= (- (compare-value ##NaN "x")) (compare-value "x" ##NaN)))))

(deftest test-nan-value-key
  (testing "`value-key` canonicalises a scalar NaN, because a Clojure
            container decides membership with `=`"
    (is (= (da/value-key ##NaN) (da/value-key ##NaN)))
    (is (= (da/value-key (float ##NaN)) (da/value-key ##NaN)))
    (is (not= (da/value-key ##NaN) (da/value-key 1.0))))

  (testing "and leaves everything else alone, identically"
    (doseq [v [1.0 -0.0 0 7 "s" :k 'sym ##Inf]]
      (is (identical? (da/value-key v) v) (str "value-key changed " (pr-str v))))))

#?(:clj
   (deftest test-nan-in-storage-and-queries
     (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
                :schema-flexibility :write
                :keep-history? false}]
       (d/create-database cfg)
       (let [conn (d/connect cfg)]
         (try
           (d/transact conn [{:db/ident :nan/many
                              :db/valueType :db.type/double
                              :db/cardinality :db.cardinality/many}
                             {:db/ident :nan/d
                              :db/valueType :db.type/double
                              :db/cardinality :db.cardinality/one}
                             {:db/ident :nan/b
                              :db/valueType :db.type/bytes
                              :db/cardinality :db.cardinality/one}])

           (testing "a NaN no longer swallows the other values of a
                     cardinality/many attribute -- it compared EQUAL to each
                     of them, so the sorted set kept only the first"
             (d/transact conn [{:db/id 10 :nan/many [(fresh-nan) 1.0 2.0 (fresh-nan)]}])
             (is (= (canon #{[1.0] [2.0] [##NaN]})
                    (canon (d/q '{:find [?v] :where [[10 :nan/many ?v]]}
                                (d/db conn))))))

           (testing "and it can be retracted on its own"
             (d/transact conn [[:db/retract 10 :nan/many (fresh-nan)]])
             (is (= #{[1.0] [2.0]}
                    (d/q '{:find [?v] :where [[10 :nan/many ?v]]} (d/db conn)))))

           (d/transact conn [{:db/id 20 :nan/d (fresh-nan) :nan/b (byte-array [1 2])}
                             {:db/id 21 :nan/d (fresh-nan) :nan/b (byte-array [1 2])}
                             {:db/id 22 :nan/d 1.0         :nan/b (byte-array [3])}])
           (let [db (d/db conn)]
             (testing "and the rows themselves are untouched"
               (is (= 3 (count (d/q '{:find [?e ?v] :where [[?e :nan/d ?v]]} db)))))

             (testing "a grouped query groups by VALUE. The group counts are
                       asserted directly: comparing the whole result goes through
                       `canon`, which collapses exactly the duplicate this is
                       looking for -- so that assertion passed on the parent commit
                       too, where the query answered [[##NaN 1] [##NaN 1]]"
               (is (= [1 2]
                      (sort (map second (d/q '{:find [?v (count ?e)]
                                               :where [[?e :nan/d ?v]]} db)))))
               (is (= (canon #{[##NaN 2] [1.0 1]})
                      (canon (d/q '{:find [?v (count ?e)]
                                    :where [[?e :nan/d ?v]]} db))))
               (is (= [1 2]
                      (sort (map second (d/q '{:find [?v (count ?e)]
                                               :where [[?e :nan/b ?v]]} db))))))

             (testing "`min`/`max` over a stored NaN, through a real query"
               ;; Oracle: SELECT min(v), max(v) FROM (VALUES (1.0),('NaN'),('NaN'))
               ;;   =>  1 | NaN.  The parent commit answered NaN | NaN.
               (is (= [1.0 :datahike.array/nan]
                      (let [[mn mx] (first (d/q '{:find [(min ?v) (max ?v)]
                                                  :where [[?e :nan/d ?v]]} db))]
                        [mn (da/value-key mx)]))))

             (testing "a join over a NaN still matches. This one was NOT broken --
                       the hash joins were taught the key rule when the array rule
                       landed, and they answer 5 pairs on the parent commit too, with
                       distinct objects. A guard, not evidence of a fix"
               (is (= 5 (count (d/q '{:find [?e1 ?e2]
                                      :where [[?e1 :nan/d ?v]
                                              [?e2 :nan/d ?v]]} db)))))

             (testing "no dedup key leaks into a result"
               (is (every? #(instance? (Class/forName "[B") (first %))
                           (d/q '{:find [?v] :where [[?e :nan/b ?v]]} db)))))
           (finally
             (d/release conn)
             (d/delete-database cfg)))))))

(deftest test-aggregates-use-value-semantics
  (let [agg dq/built-in-aggregates
        mx (agg 'max) mn (agg 'min)
        cd (agg 'count-distinct) ds (agg 'distinct)]
    (testing "`min`/`max` compared with `clojure.core/compare`, which is not a
              total order over the value domain -- so a NaN compared equal to
              every number and never won"
      ;; Oracle: SELECT max(v), min(v) FROM (VALUES (1.0),('NaN'),(2.0))
      ;;   =>  NaN | 1
      (is (= :datahike.array/nan (da/value-key (mx [1.0 ##NaN 2.0]))))
      (is (= 1.0 (mn [1.0 ##NaN 2.0]))))

    #?(:clj
       (testing "and an array is not Comparable at all, so min/max over
                 `:db.type/bytes` values threw"
         (is (= [3] (vec (mx [(byte-array [1]) (byte-array [3]) (byte-array [2])]))))
         (is (= [1] (vec (mn [(byte-array [1]) (byte-array [3]) (byte-array [2])]))))))

    (testing "`distinct` and `count-distinct` decided with `=`, which compares
              an array by identity and a NaN as unequal to itself"
      ;; Oracle: count(DISTINCT v) over two 'NaN'::float8 and 1.0  =>  2
      (is (= 2 (cd [##NaN ##NaN 1.0])))
      (is (= 2 (count (ds [##NaN ##NaN 1.0]))))
      #?(:clj
         ;; Oracle: count(DISTINCT v) over two '\x0102'::bytea and '\x03'  =>  2
         (is (= 2 (cd [(byte-array [1 2]) (byte-array [1 2]) (byte-array [3])])))))

    (testing "and an ordinary collection is untouched"
      (is (= 3 (cd [1 1 2 3])))
      (is (= #{1 2 3} (ds [1 1 2 3])))
      (is (= 3 (mx [1 2 3])))
      (is (= 1 (mn [1 2 3]))))))

#?(:clj
   (deftest test-order-by-uses-the-value-comparator
     (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
                :schema-flexibility :write
                :keep-history? false}]
       (d/create-database cfg)
       (let [conn (d/connect cfg)]
         (try
           (d/transact conn [{:db/ident :o/b :db/valueType :db.type/bytes
                              :db/cardinality :db.cardinality/one}
                             {:db/ident :o/d :db/valueType :db.type/double
                              :db/cardinality :db.cardinality/one}])
           (d/transact conn [{:db/id 1 :o/b (byte-array [3]) :o/d 2.0}
                             {:db/id 2 :o/b (byte-array [1]) :o/d ##NaN}
                             {:db/id 3 :o/b (byte-array [2]) :o/d 1.0}])
           (let [db (d/db conn)
                 by (fn [attr] (d/q {:query {:find '[?v] :where [['?e attr '?v]]}
                                     :args [db] :order-by [0 :asc]}))]
             (testing "`:order-by` compared with `clojure.core/compare`, so an
                       array value -- not Comparable at all -- threw
                       `class [B cannot be cast to class java.lang.Comparable`"
               (is (= [[1] [2] [3]] (mapv #(vec (first %)) (by :o/b)))))

             (testing "and a NaN compared equal to every number, which did not
                       mis-order the rows so much as leave them UNSORTED:
                       [2.0 ##NaN 1.0], the order they went in"
               (is (= [1.0 2.0 :datahike.array/nan]
                      (mapv #(da/value-key (first %)) (by :o/d))))))
           (finally
             (d/release conn)
             (d/delete-database cfg)))))))
