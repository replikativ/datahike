(ns datahike.test.query-aggregates-test
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer        [is deftest testing]])
   [clojure.core.async :refer [<!]]
   [datahike.api :as d]
   [datahike.query :as dq]
   [datahike.test.async #?(:clj :refer :cljs :refer-macros) [deftest-async]]))

(defn- connect!
  "Create + connect to `cfg`. Returns a channel yielding the connection so the
   body works on both JVM (sync) and cljs (async writer)."
  [cfg]
  (clojure.core.async/go
    #?(:clj  (do (d/create-database cfg) (d/connect cfg))
       :cljs (do (<! (d/create-database cfg)) (<! (d/connect cfg {:sync? false}))))))

(defn sort-reverse [xs]
  (reverse (sort xs)))

;; ---------------------------------------------------------------------------
;; The aggregate contract, stated once.
;;
;; This table exists because there was no single place asserting it: `variance`
;; and `stddev` had drifted to the SAMPLE estimator on the columnar path (÷n−1)
;; while the reference used the population one (÷n) — 82.667 against 62 on the
;; same data, and ##NaN against 0.0 for a one-element group. Every path that
;; claims an aggregate must answer this table, so anything reimplementing one
;; (the columnar delegate in `secondary-integration-test`) reuses it.
;;
;; The two decisions it pins:
;;   * variance/stddev are POPULATION statistics. A query's answer set IS the
;;     population — it is not a sample drawn from a larger one — so ÷n is the
;;     right denominator, and it keeps them total: a singleton group is 0.0,
;;     never NaN. Matches Datomic (measured on peer 1.0.7387).
;;   * The central-tendency aggregates are REAL-VALUED: avg and median return
;;     doubles even when the division is exact, and median does so for an odd
;;     count too, so the result TYPE never depends on how many rows happened to
;;     match. `sum`/`min`/`max` preserve the column's own type. Datomic truncates
;;     an even median to the inputs' type (2 for [1 2 3 4]); we deliberately do
;;     not — a median is real-valued in general, truncating it would be
;;     inconsistent with avg (which Datomic itself returns as a double), and a
;;     columnar delegate computing in doubles could not answer it.
(def aggregate-contract
  [{:agg 'avg      :in [10 15 20 35 75] :expect 31.0  :real? true
    :note "real-valued even when it divides exactly"}
   {:agg 'avg      :in [1 2]            :expect 1.5   :real? true}
   {:agg 'variance :in [10 15 20 35 75] :expect 554.0 :real? true
    :note "population: mean 31, sum sq dev 2770, ÷5"}
   {:agg 'variance :in [10 20 30 12]    :expect 62.0  :real? true}
   {:agg 'variance :in [7]              :expect 0.0   :real? true
    :note "total on a singleton — the sample estimator gives NaN here"}
   {:agg 'stddev   :in [10 15 20 35 75] :expect 23.53720459187964 :real? true}
   {:agg 'stddev   :in [7]              :expect 0.0   :real? true}
   {:agg 'median   :in [10 15 20 35 75] :expect 20.0  :real? true
    :note "odd count is real-valued too — the type must not depend on the row count"}
   {:agg 'median   :in [1 2 3 4]        :expect 2.5   :real? true}
   {:agg 'median   :in [10 20 30 12]    :expect 16.0  :real? true
    :note "even count -> real, not Datomic's truncated 16"}
   {:agg 'sum      :in [10 15 20 35 75] :expect 155   :real? false
    :note "exact: stays integral, no float rounding"}
   {:agg 'min      :in [10 15 20 35 75] :expect 10    :real? false}
   {:agg 'max      :in [10 15 20 35 75] :expect 75    :real? false}
   {:agg 'count    :in [10 15 20 35 75] :expect 5     :real? false}])

(defn- close-enough?
  [expected actual]
  (and (number? actual) (< (abs (- (double expected) (double actual))) 1e-9)))

(deftest test-aggregate-contract
  (doseq [{:keys [agg in expect real? note]} aggregate-contract
          ;; both engines: the compiled planner and the reference relational one
          disable-planner? [false true]]
    (let [label (str "(" agg " " (pr-str in) ")"
                     (when note (str " — " note))
                     (if disable-planner? " [reference]" " [planner]"))
          actual (binding [dq/*disable-planner* disable-planner?]
                   (ffirst (d/q {:find [(list agg '?x)] :in '[[?x ...]]} in)))]
      (is (close-enough? expect actual) label)
      ;; the TYPE is half the contract — a truncating median or an integral avg
      ;; passes a tolerance check while still being the wrong answer. cljs has
      ;; one numeric type, so this can only be asserted on the JVM.
      #?(:clj
         (if real?
           (is (double? actual) (str label " must be real-valued"))
           (is (not (double? actual)) (str label " must preserve the input type")))))))

(deftest-async test-aggregates
  (let [monsters [["Cerberus" 3]
                  ["Medusa" 1]
                  ["Cyclops" 1]
                  ["Chimera" 1]]]
    (testing "with"
      (is (= (d/q '[:find ?heads
                    :with ?monster
                    :in   [[?monster ?heads]]]
                  [["Medusa" 1]
                   ["Cyclops" 1]
                   ["Chimera" 1]])
             [[1] [1] [1]])))

    (testing "Wrong grouping without :with"
      (is (= (d/q '[:find (sum ?heads)
                    :in   [[?monster ?heads]]]
                  monsters)
             [[4]])))

    (testing "Multiple aggregates, correct grouping with :with"
      (is (= (d/q '[:find (sum ?heads) (min ?heads) (max ?heads) (count ?heads) (count-distinct ?heads)
                    :with ?monster
                    :in   [[?monster ?heads]]]
                  monsters)
             [[6 1 3 4 2]])))

    (testing "Min and max are using comparator instead of default compare"
      ;; Wrong: using js '<' operator
      ;; (apply min [:a/b :a-/b :a/c]) => :a-/b
      ;; (apply max [:a/b :a-/b :a/c]) => :a/c
      ;; Correct: use IComparable interface
      ;; (sort compare [:a/b :a-/b :a/c]) => (:a/b :a/c :a-/b)
      (is (= (d/q '[:find (min ?x) (max ?x)
                    :in [?x ...]]
                  [:a-/b :a/b])
             [[:a/b :a-/b]]))

      (is (= (d/q '[:find (min 2 ?x) (max 2 ?x)
                    :in [?x ...]]
                  [:a/b :a-/b :a/c])
             [[[:a/b :a/c] [:a/c :a-/b]]])))

    (testing "Grouping and parameter passing"
      (is (= (set (d/q '[:find ?color (max ?amount ?x) (min ?amount ?x)
                         :in   [[?color ?x]] ?amount]
                       [[:red 1]  [:red 2] [:red 3] [:red 4] [:red 5]
                        [:blue 7] [:blue 8]]
                       3))
             #{[:red  [3 4 5] [1 2 3]]
               [:blue [7 8]   [7 8]]})))

    (testing "avg aggregate"
      ;; real-valued even when it divides exactly: 155/5 = 31.0, not 31
      (is (= (ffirst (d/q '[:find (avg ?x)
                            :in [?x ...]]
                          [10 15 20 35 75]))
             31.0)))

    (testing "median aggregate"
      (is (= (ffirst (d/q '[:find (median ?x)
                            :in [?x ...]]
                          [10 15 20 35 75]))
             20.0)))

    (testing "variance aggregate"
      (is (= (ffirst (d/q '[:find (variance ?x)
                            :in [?x ...]]
                          [10 15 20 35 75]))
             554.0)))

    (testing "stddev aggregate"
      (is (= (ffirst (d/q '[:find (stddev ?x)
                            :in [?x ...]]
                          [10 15 20 35 75]))
             23.53720459187964)))

    (testing "Custom aggregates"
      (let [data   [[:red 1]  [:red 2] [:red 3] [:red 4] [:red 5]
                    [:blue 7] [:blue 8]]
            result #{[:red [5 4 3 2 1]] [:blue [8 7]]}]

        (is (= (set (d/q '[:find ?color (aggregate ?agg ?x)
                           :in   [[?color ?x]] ?agg]
                         data
                         sort-reverse))
               result))

        #?(:clj
           (is (= (set (d/q '[:find ?color (datahike.test.query-aggregates-test/sort-reverse ?x)
                              :in   [[?color ?x]]]
                            data))
                  result))))))

  (testing "Aggregate with predicate filter"
    ;; Regression: the columnar aggregate path (via stratum) skipped
    ;; attached-preds, so predicates in WHERE were ignored for COUNT/SUM.
    (let [cfg {:store {:backend :memory :id (random-uuid)}
               :schema-flexibility :write
               :keep-history? true}
          conn (<! (connect! cfg))]
      (<! (d/transact! conn [{:db/ident :num/v :db/valueType :db.type/long
                              :db/cardinality :db.cardinality/one}]))
      (<! (d/transact! conn [{:num/v 1} {:num/v 2} {:num/v 3} {:num/v 4} {:num/v 5}]))
      (let [db (d/db conn)]
        (is (= [[3]] (d/q '{:find [(count ?e)]
                            :where [[?e :num/v ?v] [(> ?v 2)]]} db))
            "COUNT with > predicate")
        (is (= [[12]] (d/q '{:find [(sum ?v)]
                             :where [[?e :num/v ?v] [(> ?v 2)]]} db))
            "SUM with > predicate")
        (is (= [[2]] (d/q '{:find [(count ?e)]
                            :where [[?e :num/v ?v] [(< ?v 3)]]} db))
            "COUNT with < predicate"))
      (d/release conn))))
