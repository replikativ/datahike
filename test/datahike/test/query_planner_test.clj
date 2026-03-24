(ns datahike.test.query-planner-test
  "Dual-execution correctness tests: compiled engine vs legacy engine.
   For every test case, runs through both engines and asserts identical results."
  (:require
   [clojure.test :refer [is are deftest testing]]
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.query :as q]))

;; ---------------------------------------------------------------------------
;; Test infrastructure

(defn assert-engines-agree
  "Run query through both compiled and legacy engines, assert identical results."
  ([db query]
   (assert-engines-agree db query []))
  ([db query extra-args]
   (let [legacy  (binding [q/*force-legacy* true]
                   (apply d/q query db extra-args))
         compiled (binding [q/*force-legacy* false]
                    (apply d/q query db extra-args))]
     (is (= (set (seq legacy)) (set (seq compiled)))
         (str "Engines disagree on: " (pr-str query))))))

(defn assert-engines-agree-with-rules
  "Run query with rules through both engines.
   Query must contain :in $ % for rules to be passed."
  [db query rules]
  (let [legacy  (binding [q/*force-legacy* true]
                  (d/q query db rules))
        compiled (binding [q/*force-legacy* false]
                   (d/q query db rules))]
    (is (= (set (seq legacy)) (set (seq compiled)))
        (str "Engines disagree on: " (pr-str query)))))

;; ---------------------------------------------------------------------------
;; Test data

(def test-db
  (delay
    (d/db-with (db/empty-db {:name      {:db/index true}
                             :last-name {:db/index true}
                             :sex       {:db/index true}
                             :age       {:db/index true}
                             :salary    {:db/index true}
                             :follows   {:db/valueType   :db.type/ref
                                         :db/cardinality :db.cardinality/many}})
               [{:db/id 1  :name "Ivan"   :last-name "Ivanov"   :sex :male   :age 15 :salary 10000}
                {:db/id 2  :name "Petr"   :last-name "Petrov"   :sex :male   :age 37 :salary 20000}
                {:db/id 3  :name "Ivan"   :last-name "Sidorov"  :sex :male   :age 37 :salary 30000}
                {:db/id 4  :name "Oleg"   :last-name "Kovalev"  :sex :male   :age 15 :salary 40000}
                {:db/id 5  :name "Ivan"   :last-name "Kuznetsov" :sex :female :age 20 :salary 50000}
                {:db/id 6  :name "Sergei" :last-name "Voronoi"  :sex :male   :age 20 :salary 60000}
                {:db/id 7  :name "Dmitry" :last-name "Ivanov"   :sex :male   :age 44 :salary 70000}
                {:db/id 8  :name "Ivan"   :last-name "Petrov"   :sex :male   :age 10 :salary 80000}
                ;; Follows graph for rule tests
                {:db/id 1 :follows 2}
                {:db/id 2 :follows 3}
                {:db/id 3 :follows 4}
                {:db/id 5 :follows 6}])))

;; ---------------------------------------------------------------------------
;; Phase 1: Entity joins (Q1-Q4 equivalents)

(deftest test-entity-joins
  (testing "Q1: Simple lookup"
    (assert-engines-agree @test-db
                          '[:find ?e :where [?e :name "Ivan"]]))

  (testing "Q2: Two-clause join"
    (assert-engines-agree @test-db
                          '[:find ?e ?a :where [?e :name "Ivan"] [?e :age ?a]]))

  (testing "Q3: Three clauses"
    (assert-engines-agree @test-db
                          '[:find ?e ?a :where [?e :name "Ivan"] [?e :age ?a] [?e :sex :male]]))

  (testing "Q4: Four clauses"
    (assert-engines-agree @test-db
                          '[:find ?e ?l ?a :where
                            [?e :name "Ivan"] [?e :last-name ?l] [?e :age ?a] [?e :sex :male]]))

  (testing "Q2-switch: Reversed clause order"
    (assert-engines-agree @test-db
                          '[:find ?e ?a :where [?e :age ?a] [?e :name "Ivan"]])))

;; ---------------------------------------------------------------------------
;; Phase 2: Predicates

(deftest test-predicates
  (testing "qpred1: salary > 50000"
    (assert-engines-agree @test-db
                          '[:find ?e ?s :where [?e :salary ?s] [(> ?s 50000)]]))

  (testing "qpred2: salary > ?min with :in binding"
    (assert-engines-agree @test-db
                          '[:find ?e ?s :in $ ?min_s :where [?e :salary ?s] [(> ?s ?min_s)]]
                          [50000]))

  (testing "Range predicate: age between 15 and 37"
    (assert-engines-agree @test-db
                          '[:find ?e ?a :where [?e :age ?a] [(>= ?a 15)] [(<= ?a 37)]]))

  (testing "Equality predicate"
    (assert-engines-agree @test-db
                          '[:find ?e ?a :where [?e :age ?a] [(= ?a 37)]])))

;; ---------------------------------------------------------------------------
;; Phase 2: Value joins (Q5)

(deftest test-value-joins
  (testing "Q5: Value join on age"
    (assert-engines-agree @test-db
                          '[:find ?e1 ?l ?a :where
                            [?e :name "Ivan"] [?e :age ?a]
                            [?e1 :age ?a] [?e1 :last-name ?l]])))

;; ---------------------------------------------------------------------------
;; Phase 3: OR support

(deftest test-or
  (testing "Simple OR"
    (assert-engines-agree @test-db
                          '[:find ?e :where
                            (or [?e :name "Oleg"]
                                [?e :name "Sergei"])]))

  (testing "OR with join"
    (assert-engines-agree @test-db
                          '[:find ?e :where
                            [?e :age ?a]
                            (or [?e :name "Ivan"]
                                [?e :name "Oleg"])]))

  (testing "OR with AND branches"
    (assert-engines-agree @test-db
                          '[:find ?e :where
                            [?e :age ?a]
                            (or (and [?e :name "Ivan"]
                                     [?e :sex :male])
                                (and [?e :name "Oleg"]))])))

;; ---------------------------------------------------------------------------
;; Phase 3: NOT support

(deftest test-not
  (testing "Simple NOT"
    (assert-engines-agree @test-db
                          '[:find ?e :where
                            [?e :name ?n]
                            (not [?e :name "Ivan"])]))

  (testing "NOT with multiple clauses"
    (assert-engines-agree @test-db
                          '[:find ?e ?a :where
                            [?e :age ?a]
                            (not [?e :sex :female])])))

;; ---------------------------------------------------------------------------
;; Phase 3: OR-JOIN and NOT-JOIN

(deftest test-or-join
  (testing "OR-JOIN"
    (assert-engines-agree @test-db
                          '[:find ?e :where
                            [?e :age ?a]
                            (or-join [?e]
                                     [?e :name "Ivan"]
                                     [?e :name "Oleg"])])))

(deftest test-not-join
  (testing "NOT-JOIN"
    (assert-engines-agree @test-db
                          '[:find ?e :where
                            [?e :age ?a]
                            (not-join [?e]
                                      [?e :name "Ivan"])])))

;; ---------------------------------------------------------------------------
;; Phase 4: Non-recursive rules

(deftest test-non-recursive-rules
  (testing "Simple rule"
    (assert-engines-agree-with-rules @test-db
                                     '[:find ?e1 ?e2 :in $ % :where (follows ?e1 ?e2)]
                                     '[[(follows ?e1 ?e2) [?e1 :follows ?e2]]]))

  (testing "Multi-branch rule"
    (assert-engines-agree-with-rules @test-db
                                     '[:find ?e :in $ % :where (ivan-or-oleg ?e)]
                                     '[[(ivan-or-oleg ?e) [?e :name "Ivan"]]
                                       [(ivan-or-oleg ?e) [?e :name "Oleg"]]])))

;; ---------------------------------------------------------------------------
;; Phase 4: Recursive rules

(deftest test-recursive-rules
  (testing "Recursive rule (transitive closure)"
    (assert-engines-agree-with-rules @test-db
                                     '[:find ?e1 ?e2 :in $ % :where (follows ?e1 ?e2)]
                                     '[[(follows ?e1 ?e2) [?e1 :follows ?e2]]
                                       [(follows ?e1 ?e2) [?e1 :follows ?t] (follows ?t ?e2)]]))

  (testing "Recursive rule with additional pattern"
    (assert-engines-agree-with-rules @test-db
                                     '[:find ?e1 ?n :in $ % :where (follows ?e1 ?e2) [?e2 :name ?n]]
                                     '[[(follows ?e1 ?e2) [?e1 :follows ?e2]]
                                       [(follows ?e1 ?e2) [?e1 :follows ?t] (follows ?t ?e2)]])))

;; ---------------------------------------------------------------------------
;; Edge cases

(deftest test-edge-cases
  (testing "Empty result"
    (assert-engines-agree @test-db
                          '[:find ?e :where [?e :name "Nonexistent"]]))

  (testing "Single result"
    (assert-engines-agree @test-db
                          '[:find ?e :where [?e :name "Dmitry"]]))

  (testing "All match"
    (assert-engines-agree @test-db
                          '[:find ?e :where [?e :name ?n]]))

  (testing "Multiple find vars"
    (assert-engines-agree @test-db
                          '[:find ?e ?n ?a :where [?e :name ?n] [?e :age ?a]])))

;; ---------------------------------------------------------------------------
;; ORDER BY

(defn assert-engines-agree-ordered
  "Run query with :order-by through both engines, assert identical ordered results."
  [db query-map]
  (let [legacy  (binding [q/*force-legacy* true]
                  (d/q query-map))
        compiled (binding [q/*force-legacy* false]
                   (d/q query-map))]
    (is (= legacy compiled)
        (str "Engines disagree on ordered: " (pr-str (:query query-map))))))

(deftest test-order-by
  (testing "ORDER BY single variable ascending"
    (assert-engines-agree-ordered @test-db
                                  {:query '[:find ?e ?a :where [?e :age ?a]]
                                   :args [@test-db]
                                   :order-by '[?a :asc]}))

  (testing "ORDER BY single variable descending"
    (assert-engines-agree-ordered @test-db
                                  {:query '[:find ?e ?a :where [?e :age ?a]]
                                   :args [@test-db]
                                   :order-by '[?a :desc]}))

  (testing "ORDER BY with LIMIT"
    (assert-engines-agree-ordered @test-db
                                  {:query '[:find ?e ?a :where [?e :age ?a]]
                                   :args [@test-db]
                                   :order-by '[?a :asc]
                                   :limit 3}))

  (testing "ORDER BY with OFFSET and LIMIT"
    (assert-engines-agree-ordered @test-db
                                  {:query '[:find ?e ?a :where [?e :age ?a]]
                                   :args [@test-db]
                                   :order-by '[?a :desc]
                                   :offset 2
                                   :limit 3}))

  (testing "ORDER BY by name"
    (assert-engines-agree-ordered @test-db
                                  {:query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
                                   :args [@test-db]
                                   :order-by '[?n :asc ?a :asc]}))

  (testing "ORDER BY multi-key"
    (assert-engines-agree-ordered @test-db
                                  {:query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
                                   :args [@test-db]
                                   :order-by '[?a :asc ?n :desc]}))

  (testing "ORDER BY column index"
    (assert-engines-agree-ordered @test-db
                                  {:query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
                                   :args [@test-db]
                                   :order-by [1 :desc 0 :asc]}))

  (testing "ORDER BY returns vector"
    (let [result (d/q {:query '[:find ?e ?a :where [?e :age ?a]]
                       :args [@test-db]
                       :order-by '[?a :asc]})]
      (is (vector? result) "ORDER BY should return a vector")))

  (testing "No ORDER BY returns set"
    (let [result (d/q '[:find ?e ?a :where [?e :age ?a]] @test-db)]
      (is (set? result) "Without ORDER BY should return a set"))))

;; ---------------------------------------------------------------------------
;; Aggregates — compiled engine must route through relation path correctly

(deftest test-aggregates
  (testing "count aggregate"
    (is (= 4 (d/q '[:find (count ?e) . :where [?e :name "Ivan"]] @test-db))))

  (testing "min/max aggregate"
    (is (= 10 (d/q '[:find (min ?a) . :where [?e :name "Ivan"] [?e :age ?a]] @test-db)))
    (is (= 37 (d/q '[:find (max ?a) . :where [?e :name "Ivan"] [?e :age ?a]] @test-db))))

  (testing "sum aggregate"
    (is (= 82 (d/q '[:find (sum ?a) . :where [?e :name "Ivan"] [?e :age ?a]] @test-db))))

  (testing "avg aggregate"
    (let [avg (d/q '[:find (avg ?a) . :where [?e :name "Ivan"] [?e :age ?a]] @test-db)]
      (is (< 20 avg 21))))

  (testing "group-by with count"
    (let [result (d/q '[:find ?n (count ?e) :where [?e :name ?n]] @test-db)]
      (is (= #{"Ivan" "Petr" "Oleg" "Sergei" "Dmitry"} (set (map first result))))
      (is (= 4 (second (first (filter #(= "Ivan" (first %)) result)))))))

  (testing "engines agree on aggregates"
    (let [query '[:find ?n (count ?e) :where [?e :name ?n]]
          legacy   (binding [q/*force-legacy* true]  (d/q query @test-db))
          compiled (binding [q/*force-legacy* false] (d/q query @test-db))]
      (is (= (set legacy) (set compiled))))))

;; ---------------------------------------------------------------------------
;; AVET secondary index — exact value lookups and range pushdown

(deftest test-avet-index
  (testing "exact value lookup on indexed attr"
    (assert-engines-agree @test-db
                          '[:find ?e :where [?e :age 37]]))

  (testing "exact value lookup on indexed attr (salary)"
    (assert-engines-agree @test-db
                          '[:find ?e :where [?e :salary 50000]]))

  (testing "range predicate on indexed attr uses AVET"
    (assert-engines-agree @test-db
                          '[:find ?e ?s :where [?e :salary ?s] [(> ?s 50000)]]))

  (testing "double range on indexed attr"
    (assert-engines-agree @test-db
                          '[:find ?e ?s :where [?e :salary ?s] [(> ?s 20000)] [(< ?s 60000)]]))

  (testing "exact value + join on indexed attr"
    (assert-engines-agree @test-db
                          '[:find ?e ?n :where [?e :age 37] [?e :name ?n]]))

  (testing "indexed attr in multi-clause"
    (assert-engines-agree @test-db
                          '[:find ?e ?a ?s :where
                            [?e :name "Ivan"] [?e :age ?a] [?e :salary ?s]]))

  (testing "AVET with NOT"
    (assert-engines-agree @test-db
                          '[:find ?e ?s :where
                            [?e :salary ?s] [(> ?s 30000)]
                            (not [?e :name "Ivan"])])))

;; ---------------------------------------------------------------------------
;; Find specifications — scalar, collection, tuple

(deftest test-find-specs
  (testing "FindScalar"
    (let [legacy   (binding [q/*force-legacy* true]  (d/q '[:find ?a . :where [7 :age ?a]] @test-db))
          compiled (binding [q/*force-legacy* false] (d/q '[:find ?a . :where [7 :age ?a]] @test-db))]
      (is (= legacy compiled))))

  (testing "FindColl"
    (let [legacy   (binding [q/*force-legacy* true]  (d/q '[:find [?e ...] :where [?e :name "Ivan"]] @test-db))
          compiled (binding [q/*force-legacy* false] (d/q '[:find [?e ...] :where [?e :name "Ivan"]] @test-db))]
      (is (= (set legacy) (set compiled)))))

  (testing "FindTuple"
    (let [legacy   (binding [q/*force-legacy* true]  (d/q '[:find [?e ?a] :where [?e :name "Dmitry"] [?e :age ?a]] @test-db))
          compiled (binding [q/*force-legacy* false] (d/q '[:find [?e ?a] :where [?e :name "Dmitry"] [?e :age ?a]] @test-db))]
      (is (= legacy compiled))))

  (testing "FindRel (default)"
    (assert-engines-agree @test-db
                          '[:find ?e ?a :where [?e :name "Ivan"] [?e :age ?a]])))
