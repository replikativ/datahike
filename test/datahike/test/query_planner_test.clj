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
   (let [legacy  (binding [q/*disable-planner* true]
                   (apply d/q query db extra-args))
         compiled (binding [q/*disable-planner* false]
                    (apply d/q query db extra-args))]
     (is (= (set (seq legacy)) (set (seq compiled)))
         (str "Engines disagree on: " (pr-str query))))))

(defn assert-engines-agree-with-rules
  "Run query with rules through both engines.
   Query must contain :in $ % for rules to be passed."
  [db query rules]
  (let [legacy  (binding [q/*disable-planner* true]
                  (d/q query db rules))
        compiled (binding [q/*disable-planner* false]
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
                          '[:find ?e ?a :where [?e :age ?a] [(= ?a 37)]]))

  (testing "not= predicate on AVET-indexed attribute — version 0 excluded"
    ;; Regression: planner was consuming not= as a pushdown but silently dropping it,
    ;; causing the predicate to be ignored and all values returned.
    (let [db (d/db-with (db/empty-db {:version/id {:db/unique :db.unique/identity}})
                        [{:version/id 0} {:version/id 1} {:version/id 2}])]
      (assert-engines-agree db '[:find ?v :where [?e :version/id ?v] [(not= ?v 0)]])))

  (testing "== predicate on AVET-indexed attribute — numeric equality"
    ;; Regression: planner was consuming == as a pushdown but silently dropping it.
    (let [db (d/db-with (db/empty-db {:version/id {:db/unique :db.unique/identity}})
                        [{:version/id 0} {:version/id 1} {:version/id 2}])]
      (assert-engines-agree db '[:find ?v :where [?e :version/id ?v] [(== ?v 1)]]))))

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
  (let [legacy  (binding [q/*disable-planner* true]
                  (d/q query-map))
        compiled (binding [q/*disable-planner* false]
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
          legacy   (binding [q/*disable-planner* true]  (d/q query @test-db))
          compiled (binding [q/*disable-planner* false] (d/q query @test-db))]
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
    (let [legacy   (binding [q/*disable-planner* true]  (d/q '[:find ?a . :where [7 :age ?a]] @test-db))
          compiled (binding [q/*disable-planner* false] (d/q '[:find ?a . :where [7 :age ?a]] @test-db))]
      (is (= legacy compiled))))

  (testing "FindColl"
    (let [legacy   (binding [q/*disable-planner* true]  (d/q '[:find [?e ...] :where [?e :name "Ivan"]] @test-db))
          compiled (binding [q/*disable-planner* false] (d/q '[:find [?e ...] :where [?e :name "Ivan"]] @test-db))]
      (is (= (set legacy) (set compiled)))))

  (testing "FindTuple"
    (let [legacy   (binding [q/*disable-planner* true]  (d/q '[:find [?e ?a] :where [?e :name "Dmitry"] [?e :age ?a]] @test-db))
          compiled (binding [q/*disable-planner* false] (d/q '[:find [?e ?a] :where [?e :name "Dmitry"] [?e :age ?a]] @test-db))]
      (is (= legacy compiled))))

  (testing "FindRel (default)"
    (assert-engines-agree @test-db
                          '[:find ?e ?a :where [?e :name "Ivan"] [?e :age ?a]])))

;; ---------------------------------------------------------------------------
;; Temporal query tests (HistoricalDB, AsOfDB, SinceDB)

(def temporal-db
  (delay
    (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
               :keep-history? true :schema-flexibility :write}
          _ (d/create-database cfg)
          conn (d/connect cfg)]
      (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                        {:db/ident :age :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/one}
                        {:db/ident :likes :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/many}])
      (d/transact conn [{:name "Alice" :age 25 :likes ["cats" "pizza"]}
                        {:name "Bob" :age 30}])
      (d/transact conn [{:name "Alice" :age 26}])       ;; upsert age
      (d/transact conn [{:name "Alice" :age 27}])       ;; another upsert
      (d/transact conn [[:db/retract [:name "Alice"] :likes "cats"]]) ;; retract card-many
      conn)))

(deftest test-temporal-queries
  (let [conn (force temporal-db)
        hdb (d/history @conn)
        tx2 (+ 536870912 2)]

    (testing "history: join with upserted card-one"
      (assert-engines-agree hdb
                            '[:find ?a ?tx ?added :where [?e :age ?a ?tx ?added] [?e :name "Alice"]]))

    (testing "history: bound tx from :in filters correctly"
      (assert-engines-agree hdb
                            '[:find ?e ?a ?v :in $ ?t :where [?e ?a ?v ?t true]]
                            [tx2]))

    (testing "history: ground added=false (only retractions)"
      (assert-engines-agree hdb
                            '[:find ?e ?a ?v ?tx :where [?e ?a ?v ?tx false]]))

    (testing "history: NOT anti-merge checks value, not just entity+attr"
      (assert-engines-agree hdb
                            '[:find ?n ?tx ?added :where [?e :name ?n ?tx ?added] (not [?e :age 30])]))

    (testing "history: card-many attr join"
      (assert-engines-agree hdb
                            '[:find ?l ?tx ?added :where [?e :likes ?l ?tx ?added] [?e :name "Alice"]]))

    (testing "history: multi-merge with per-merge cursors (attribute order differs from EA)"
      ;; merge0=:name (later in EA), merge1=:age (earlier) — cursor ordering bug
      (assert-engines-agree hdb
                            '[:find ?a :in $ ?n ?l :where [?e :name ?n] [?e :age ?a] [?e :likes ?l]]
                            ["Alice" "pizza"]))

    (testing "history: predicates on temporal values"
      (assert-engines-agree hdb
                            '[:find ?n ?a ?tx ?added :where [?e :name ?n] [?e :age ?a ?tx ?added] [(> ?a 26)]]))

    (testing "history: OR clause"
      (assert-engines-agree hdb
                            '[:find ?n :where [?e :name ?n] (or [?e :age 25] [?e :age 30])]))

    (testing "as-of: card-one merge returns temporal value, not current"
      (assert-engines-agree (d/as-of @conn tx2)
                            '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]))

    (testing "as-of: card-many join"
      (assert-engines-agree (d/as-of @conn tx2)
                            '[:find ?n ?l :where [?e :name ?n] [?e :likes ?l]]))

    (testing "as-of: NOT"
      (assert-engines-agree (d/as-of @conn tx2)
                            '[:find ?n :where [?e :name ?n] (not [?e :age 30])]))

    (testing "since: join"
      (assert-engines-agree (d/since @conn tx2)
                            '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]))))

(def temporal-friend-db
  "DB with history, friendships, and retractions — for shared-variable merge tests."
  (delay
    (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
               :keep-history? true :schema-flexibility :write}
          _ (d/create-database cfg)
          conn (d/connect cfg)]
      (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                        {:db/ident :id :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                        {:db/ident :age :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/one}
                        {:db/ident :friend :db/valueType :db.type/ref
                         :db/cardinality :db.cardinality/many}])
      ;; tx1: initial data
      (d/transact conn [{:db/id -1 :name "Ivan" :id 1 :age 25 :friend [-2]}
                        {:db/id -2 :name "Petr" :id 2 :age 30 :friend [-3]}
                        {:db/id -3 :name "Oleg" :id 3 :age 35}])
      ;; tx2: update ages (different tx than names)
      (d/transact conn [{:db/id [:name "Ivan"] :age 26}
                        {:db/id [:name "Petr"] :age 31}])
      ;; tx3: retract a friendship
      (d/transact conn [[:db/retract [:name "Ivan"] :friend [:name "Petr"]]])
      conn)))

(deftest test-shared-variable-merges
  (let [conn (force temporal-friend-db)
        db (d/db conn)
        hdb (d/history db)]

    (testing "tx-join on current db: [?e :name ?n ?tx] [?e :age ?a ?tx]"
      ;; Names set at tx1, ages updated at tx2. Only entities whose name and
      ;; age share the same tx should appear. Without shared-var checks the
      ;; merge ignores ?tx and returns spurious matches.
      (assert-engines-agree db
                            '[:find ?n ?a ?tx :where [?e :name ?n ?tx] [?e :age ?a ?tx]]))

    (testing "tx-join on history db"
      (assert-engines-agree hdb
                            '[:find ?n ?a :where [?e :name ?n ?tx] [?e :age ?a ?tx]]))

    (testing "history NOT with shared ?f and ?tx (anti-merge + added?)"
      ;; NOT [?e :friend ?f ?tx false]: exclude friendships retracted at
      ;; the SAME tx as the assertion. The retraction is at tx3, assertions
      ;; at tx1, so nothing should be excluded.
      (assert-engines-agree hdb
                            '[:find ?e ?f ?tx :where [?e :friend ?f ?tx true]
                              (not [?e :friend ?f ?tx false])]))

    (testing "history NOT with shared ?f, wildcard tx"
      ;; NOT [?e :friend ?f _ false]: for each (e,f) pair, check if there
      ;; is ANY retraction of that specific friend value.
      (assert-engines-agree hdb
                            '[:find ?e ?f ?tx :where [?e :friend ?f ?tx true]
                              (not [?e :friend ?f _ false])]))

    (testing "history multi-merge cross-product (cursor cache must not collapse)"
      ;; When the same entity has multiple values for card-one attrs across
      ;; transactions in history, ALL combinations must appear in the result.
      ;; A ForwardCursor cache that stores only one datom per entity would
      ;; miss combinations.
      (assert-engines-agree hdb
                            '[:find ?n ?a ?s :where [?e :name ?n] [?e :age ?a] [?e :salary ?s]]))))

(deftest test-variable-attribute-join
  (testing "variable-attr scan + merge: all entity datoms pass merge filter"
    ;; [?e ?a ?v] produces multiple datoms per entity (all attributes).
    ;; The sorted-scan single cursor can't re-seek the same entity.
    ;; Disabled sorted-scan when scan attribute is variable.
    (assert-engines-agree @test-db
                          '[:find ?e ?a ?v :where [?e ?a ?v] [?e :name "Ivan"]])))

(deftest test-multi-source-queries
  (let [schema {:name {:db/unique :db.unique/identity}
                :id   {:db/unique :db.unique/identity}
                :friend {:db/valueType :db.type/ref}}
        db1 (d/db-with (db/empty-db schema)
                       [{:db/id 1 :name "Ivan" :id 1 :friend 2}
                        {:db/id 2 :name "Petr" :id 2 :friend 3}
                        {:db/id 3 :name "Oleg" :id 3}])
        db2 (d/db-with (db/empty-db schema)
                       [{:db/id 3 :name "Ivan" :id 3}
                        {:db/id 1 :name "Petr" :id 1}
                        {:db/id 2 :name "Oleg" :id 2}])
        assert-multi
        (fn [query & extra-args]
          (let [legacy  (binding [q/*disable-planner* true]
                          (apply d/q query extra-args))
                planner (binding [q/*disable-planner* false]
                          (apply d/q query extra-args))]
            (is (= (set (seq legacy)) (set (seq planner)))
                (str "Engines disagree on: " (pr-str query)))))]

    (testing "simple multi-source join"
      (assert-multi '[:find ?e ?a1 ?a2
                      :in $1 $2
                      :where [$1 ?e :name ?a1] [$2 ?e :name ?a2]]
                    db1 db2))

    (testing "collection-bound lookup refs across sources"
      (assert-multi '[:find ?e ?e1 ?e2
                      :in $1 $2 [?e ...]
                      :where [$1 ?e :id ?e1] [$2 ?e :id ?e2]]
                    db1 db2 [[:name "Ivan"] [:name "Petr"] [:name "Oleg"]]))

    (testing "scalar lookup ref with multi-source"
      (assert-multi '[:find ?v
                      :in $1 ?e
                      :where [$1 ?e :friend ?v]]
                    db1 [:name "Ivan"]))

    (testing "multi-source with NOT"
      (assert-multi '[:find ?n
                      :in $1 $2
                      :where [$1 ?e :name ?n]
                      (not [$2 ?e :name "Ivan"])]
                    db1 db2))

    (testing "multi-source with predicate"
      (assert-multi '[:find ?e ?a1 ?a2
                      :in $1 $2
                      :where [$1 ?e :name ?n] [$1 ?e :id ?a1]
                      [$2 ?e :id ?a2] [(> ?a2 1)]]
                    db1 db2))

    (testing "multi-source with OR"
      (assert-multi '[:find ?n
                      :in $1 $2
                      :where [$1 ?e :name ?n]
                      (or [$2 ?e :id 3] [$2 ?e :id 1])]
                    db1 db2))

    (testing "multi-source with disjoint dbs (empty result)"
      (let [db3 (d/db-with (db/empty-db schema)
                           [{:db/id 100 :name "Alice" :id 100}])]
        (assert-multi '[:find ?n1 ?n2
                        :in $1 $2
                        :where [$1 ?e :name ?n1] [$2 ?e :name ?n2]]
                      db1 db3)))

    (testing "collection-bound lookup refs + NOT"
      ;; The planner correctly resolves lookup refs in NOT sub-queries.
      ;; Legacy has a known bug here (doesn't resolve lookup refs in NOT context),
      ;; so we assert the correct result directly.
      (let [result (binding [q/*disable-planner* false]
                     (d/q '[:find ?e ?a
                            :in $1 [?e ...]
                            :where [$1 ?e :id ?a]
                            (not [$1 ?e :name "Oleg"])]
                          db1 [[:name "Ivan"] [:name "Petr"] [:name "Oleg"]]))]
        (is (= #{[[:name "Ivan"] 1] [[:name "Petr"] 2]} result))))))

;; ---------------------------------------------------------------------------
;; Path 4 regression: shared value variable with NOT (per-cursor-merge bug)

(deftest test-per-cursor-merge-shared-variable
  (testing "NOT clause + shared value variable: execute-per-cursor-merge must enforce ?x equality"
    ;; [?e :val-a ?x] [?e :val-b ?x] (not [?e :flag :no])
    ;; The NOT clause routes the group to execute-per-cursor-merge (Path 4).
    ;; Bug: Path 4 dropped merge-check-scan-v, so the ?x equality constraint
    ;; between :val-a and :val-b was silently ignored, causing entity 5
    ;; (val-a="foo", val-b="bar") to spuriously appear in the result.
    (let [db (d/db-with (db/empty-db {:val-a {} :val-b {} :flag {}})
                        [{:db/id 4 :val-a "foo" :val-b "foo" :flag :yes}   ;; val-a == val-b, flag != :no  => matches
                         {:db/id 5 :val-a "foo" :val-b "bar" :flag :yes}   ;; val-a != val-b               => must NOT match
                         {:db/id 6 :val-a "baz" :val-b "baz" :flag :no}])  ;; val-a == val-b but flag == :no => excluded by NOT
          query '[:find ?e :where [?e :val-a ?x] [?e :val-b ?x] (not [?e :flag :no])]]
      (assert-engines-agree db query)
      (is (= #{[4]}
             (binding [q/*disable-planner* false] (d/q query db)))
          "Only entity 4 should match: val-a == val-b AND flag != :no")))

  (testing "shared value variable without NOT still correct (sorted-merge path)"
    ;; Without NOT the group routes to execute-sorted-merge (Path 3), which
    ;; was already correct. Confirm it still is after the fix.
    (let [db (d/db-with (db/empty-db {:val-a {} :val-b {} :flag {}})
                        [{:db/id 4 :val-a "foo" :val-b "foo" :flag :yes}
                         {:db/id 5 :val-a "foo" :val-b "bar" :flag :yes}
                         {:db/id 6 :val-a "baz" :val-b "baz" :flag :no}])
          query '[:find ?e :where [?e :val-a ?x] [?e :val-b ?x]]]
      (assert-engines-agree db query)
      (is (= #{[4] [6]}
             (binding [q/*disable-planner* false] (d/q query db)))
          "Entities 4 and 6 match: both have val-a == val-b"))))

;; ---------------------------------------------------------------------------
;; get-else with :attribute-refs? on temporal DB (legacy lookup path)

(deftest test-get-else-attribute-refs-on-as-of
  ;; Regression for two bugs in the planner's handling of get-else on temporal
  ;; DBs (AsOfDB / SinceDB / HistoricalDB):
  ;;
  ;; 1. KEYWORD RESOLUTION. When :attribute-refs? is enabled, every other
  ;;    data-pattern clause has its keyword attribute resolved to the
  ;;    attribute eid by resolve-pattern-lookup-refs. The function form
  ;;    [(get-else $ ?e :attr default) ?v] is classified as :function, so
  ;;    its inner :attr keyword was never visited by that pass — the
  ;;    synthetic LOptionalScan clause carried a keyword while surrounding
  ;;    merge-clauses carried eids. Fused-path resolves keywords at runtime
  ;;    via build-merge-attrs/resolve-attr, so the bug only manifested on
  ;;    the legacy lookup-batch-search path. Fixed by resolving the attr
  ;;    when constructing LOptionalScan in logical.cljc.
  ;;
  ;; 2. INNER-JOIN INSTEAD OF LEFT-OUTER. The legacy entity-group path
  ;;    treats every merge as an inner-join via lookup-batch-search,
  ;;    including LOptionalScan-derived merges. That drops upstream tuples
  ;;    whose entity lacks the optional attribute, instead of emitting them
  ;;    with the default value. Fixed in execute.cljc by routing optional
  ;;    merges through bind-by-fn (which evaluates -get-else per row),
  ;;    matching the legacy engine's behavior for the same query.
  ;;
  ;; Reported by Jonas Östlund (jobtech-taxonomy-api), where show-versions
  ;; queries against (d/get-db cfg :next) returned empty result sets.
  (let [cfg {:store {:backend :memory
                     :id (java.util.UUID/randomUUID)}
             :attribute-refs? true
             :keep-history? true
             :schema-flexibility :write}
        _ (d/delete-database cfg)
        _ (d/create-database cfg)
        conn (d/connect cfg)
        _ (d/transact conn
                      [{:db/ident :concept/id :db/valueType :db.type/string
                        :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                       {:db/ident :concept/type :db/valueType :db.type/string
                        :db/cardinality :db.cardinality/one}
                       {:db/ident :concept/legacy-id :db/valueType :db.type/string
                        :db/cardinality :db.cardinality/one}])
        ;; "A" has the optional attribute, "B" does not — second bug exposed.
        _ (d/transact conn [{:concept/id "A" :concept/type "T"
                             :concept/legacy-id "1384"}
                            {:concept/id "B" :concept/type "T"}])
        ;; Drive both engines through an as-of DB so the :entity-group op
        ;; falls into the legacy lookup path.
        db (d/as-of @conn (java.util.Date.))
        query '[:find ?id ?legacy
                :in $ [?type ...]
                :where [?c :concept/id ?id]
                [?c :concept/type ?type]
                [(get-else $ ?c :concept/legacy-id "missing") ?legacy]]]
    (assert-engines-agree db query [["T"]])
    (is (= #{["A" "1384"] ["B" "missing"]}
           (binding [q/*disable-planner* false] (d/q query db ["T"])))
        "Both concepts returned: A with its legacy-id, B with the default.")
    (d/release conn)))

;; ---------------------------------------------------------------------------
;; estimate-pattern-with-bindings — bound-aware cardinality estimation
;;
;; Without bound-var-cards, every pattern with `[?e a ?v]` (a ground, e/v free)
;; on the same attribute returns the same estimate (= attribute-count). This
;; collapses scan-vs-merge selection in entity groups when patterns differ only
;; in which free var is bound from upstream — see the eures regression where
;; the planner picks the worst pattern as scan. estimate-pattern-with-bindings
;; uses bound-var cardinalities to differentiate.

(deftest test-estimate-pattern-with-bindings
  (let [;; A schema with one indexed cardinality-one ref attr
        db (d/db-with (db/empty-db {:friend {:db/valueType   :db.type/ref
                                             :db/cardinality :db.cardinality/one
                                             :db/index       true}})
                      [{:db/id 1}
                       {:db/id 2 :friend 1}
                       {:db/id 3 :friend 1}
                       {:db/id 4 :friend 2}
                       {:db/id 5 :friend 3}])
        ;; Look up the resolved attribute for use in pattern-info
        analyze (requiring-resolve 'datahike.query.analyze/pattern-schema-info)
        estimate-with-bindings (requiring-resolve 'datahike.query.estimate/estimate-pattern-with-bindings)
        estimate (requiring-resolve 'datahike.query.estimate/estimate-pattern)
        ;; pattern: [?e :friend ?f] — both free
        pi-free      {:e '?e :a :friend :v '?f :pattern '[?e :friend ?f]}
        si           (analyze db pi-free)
        ;; with no bindings, returns attr-total-count
        base         (estimate db pi-free si)
        ;; with ?e bound (3 entities have :friend), card-one → ≤ 3
        with-e-bound (estimate-with-bindings db pi-free si {'?e 3})
        ;; with ?f bound (small number of distinct friend targets), value-side fan-out
        with-v-bound (estimate-with-bindings db pi-free si {'?f 1})
        ;; with both bound: point lookup
        with-both    (estimate-with-bindings db pi-free si {'?e 3 '?f 1})]
    (testing "base estimate (no bindings) returns attribute total"
      (is (>= base 4)
          (str "base should be ≥ 4 datoms with :friend, got " base)))
    (testing "entity-bound is ≤ entity cardinality (card-one)"
      (is (<= with-e-bound 3)
          (str "with ?e bound to 3 entities, expected ≤ 3, got " with-e-bound)))
    (testing "value-bound for indexed attr is per-value fan-out"
      (is (< with-v-bound base)
          (str "with ?v bound, expected < base " base ", got " with-v-bound)))
    (testing "both bound is a point lookup"
      (is (<= with-both 3)
          (str "both-bound should be ≤ min(3,1,base)≤3, got " with-both)))
    (testing "empty bindings = base"
      (is (= base (estimate-with-bindings db pi-free si {}))))))

;; ---------------------------------------------------------------------------
;; Regression: rule+OR driven by a collection (:in [?x ...]) binding.
;;
;; The `edge`-style rule below is a pure producer (every OR branch binds all
;; head vars), so before the join-ordering fix the planner scheduled the OR
;; FIRST — generating every edge and hash-joining the bound ids afterward —
;; which on a large store (jobtech-taxonomy: 150k relations) turned a
;; millisecond lookup into a multi-second hang. The fix makes the planner
;; order the selective `:node/id` binder (bound via the `[?ida ...]` input)
;; AHEAD of the producer OR, and routes :in-binding queries through the
;; SIP-capable execution path so the bound values drive the scan.
;;
;; Guards two invariants that are cheap and deterministic to assert:
;;   (a) both engines agree (correctness), and
;;   (b) the bound-var binder scan is ordered BEFORE the OR in the plan.

(def ^:private or-rule-db
  (delay
    (let [base (db/empty-db {:node/id   {:db/unique :db.unique/identity
                                         :db/index true}
                             :edge/from {:db/valueType :db.type/ref}
                             :edge/to   {:db/valueType :db.type/ref}})
          n 1000
          nodes (mapv (fn [i] {:db/id (inc i) :node/id i}) (range n))
          edges (mapv (fn [i] {:edge/from (inc i)
                               :edge/to   (inc (mod (inc i) n))})
                      (range n))]
      (d/db-with base (into nodes edges)))))

(def ^:private edge-or-rules
  '[[(linked ?a ?b ?r)
     (or (and [?r :edge/from ?a] [?r :edge/to ?b])
         (and [?r :edge/to ?a]   [?r :edge/from ?b]))]])

(def ^:private edge-or-query
  '{:find  [?ida ?idb]
    :in    [$ % [?ida ...]]
    :where [[?a :node/id ?ida]
            (linked ?a ?b ?r)
            [?b :node/id ?idb]]})

(defn- plan-line-index
  "Index of the first plan line matching `re`, or nil."
  [plan-str re]
  (first (keep-indexed (fn [i l] (when (re-find re l) i))
                       (clojure.string/split-lines plan-str))))

(deftest test-rule-or-collection-binding-ordering
  (let [db   @or-rule-db
        ids  (vec (range 50))]
    (testing "engines agree on a rule+OR driven by a collection binding"
      (let [legacy   (binding [q/*disable-planner* true]  (d/q edge-or-query db edge-or-rules ids))
            compiled (binding [q/*disable-planner* false] (d/q edge-or-query db edge-or-rules ids))]
        (is (= (set legacy) (set compiled)))
        (is (seq compiled) "query should return rows")))
    (testing "the selective :node/id binder is ordered before the producer OR"
      (let [plan     (binding [q/*disable-planner* false]
                       (d/explain {:query edge-or-query :args [db edge-or-rules ids]}))
            scan-idx (plan-line-index plan #"SCAN.*node/id|node/id.*\?ida")
            or-idx   (plan-line-index plan #"(?i)^\s*OR\b")]
        (is (some? scan-idx) (str "expected a :node/id binder scan in plan:\n" plan))
        (is (some? or-idx)   (str "expected an OR in plan:\n" plan))
        (when (and scan-idx or-idx)
          (is (< scan-idx or-idx)
              (str "binder scan must precede the producer OR; got scan@" scan-idx
                   " or@" or-idx "\n" plan)))))))

(deftest test-collection-binding-join-agreement
  ;; SIP path: an :in collection binding feeding a 2-pattern join (no rule).
  ;; Guards that routing :in-binding queries through the SIP-capable engine
  ;; preserves results across a range of batch sizes.
  (let [db @or-rule-db]
    (doseq [n [1 10 100 1000]]
      (let [ids   (vec (range n))
            qy    '{:find [?ida ?to] :in [$ [?ida ...]]
                    :where [[?a :node/id ?ida]
                            [?r :edge/from ?a]
                            [?r :edge/to ?b]
                            [?b :node/id ?to]]}]
        (assert-engines-agree db qy [ids])))))

(deftest test-recursive-rule-ground-root-ordering
  ;; A recursive rule with a GROUND root is a selective generator and must be
  ;; ordered BEFORE a broad attribute scan that would otherwise bind its output
  ;; var first (forcing the rule to run as a late semi-join filter behind a full
  ;; attribute scan — the jobtech `indirectly-replaced-by` over-an-as-of-DB
  ;; full-scan, ~1.2s → ~10ms once the rule leads). The scalar `:in ?start` is
  ;; const-substituted into the call, so `op-required-vars` must recognise the
  ;; literal call-arg and mark the rule a producer (runnable with nothing bound).
  (let [n     300 ;; so the broad :node/id scan (n rows) costs > the rule's flat producer cost (100)
        db    (d/db-with
               (db/empty-db {:node/id   {:db/unique :db.unique/identity :db/index true}
                             :edge/from {:db/valueType :db.type/ref}
                             :edge/to   {:db/valueType :db.type/ref}})
               ;; star: node 0 (entity 1) → every other node (ids 1..n-1)
               (into (mapv (fn [i] {:db/id (inc i) :node/id i}) (range n))
                     (mapv (fn [i] {:edge/from 1 :edge/to (+ i 2)}) (range (dec n)))))
        rules '[[(reaches ?a ?b) [?r :edge/from ?a] [?r :edge/to ?b]]
                [(reaches ?a ?b) [?r :edge/from ?a] [?r :edge/to ?c] (reaches ?c ?b)]]
        query '{:find [?bid] :in [$ % ?start] :where [(reaches ?start ?b) [?b :node/id ?bid]]}]
    (testing "engines agree; node 0 reaches all other nodes"
      (let [legacy   (binding [q/*disable-planner* true]  (d/q query db rules 1))
            compiled (binding [q/*disable-planner* false] (d/q query db rules 1))]
        (is (= (set legacy) (set compiled)))
        (is (= (dec n) (count compiled)))))
    (testing "the ground-rooted recursive rule leads, the broad :node/id scan trails"
      (let [plan     (binding [q/*disable-planner* false]
                       (d/explain {:query query :args [db rules 1]}))
            rule-idx (plan-line-index plan #"RECURSIVE-RULE")
            scan-idx (plan-line-index plan #"SCAN.*\?bid")]
        (is (some? rule-idx) (str "expected a RECURSIVE-RULE op in plan:\n" plan))
        (is (some? scan-idx) (str "expected the broad :node/id ?bid scan in plan:\n" plan))
        (when (and rule-idx scan-idx)
          (is (< rule-idx scan-idx)
              (str "ground-rooted recursive rule must precede the broad scan; got rule@"
                   rule-idx " scan@" scan-idx "\n" plan)))))))

;; ---------------------------------------------------------------------------
;; Regression: variable-attribute cross-source join with a function-produced
;; value and ≥2 linked entity-groups on the driving source.
;;
;; A `[$b ?p ?attr ?val]` pattern (attribute is a logic var, e.g. driven by a
;; reference's stored attribute) whose value ?val is produced by a FUNCTION,
;; reached through two linked $a groups (?doc -> :doc/author -> ?r), was
;; mis-planned: the value-producing function was ordered AFTER the
;; variable-attribute scan, so the scan ran unconstrained and the non-selective
;; ?attr-only join produced a Cartesian product (planner), while the legacy
;; engine crashed ("No matching clause: 5"). Correct answer: Doc1->Peter,
;; Doc2->Anna.

(def var-attr-docs-db
  (delay
    (d/db-with (db/empty-db {:doc/title  {:db/index true}
                             :doc/author {:db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
                             :link/attr  {:db/index true}
                             :link/enc   {:db/index true}})
               [{:db/id 1 :doc/title "Doc1" :doc/author 10}
                {:db/id 2 :doc/title "Doc2" :doc/author 20}
                {:db/id 10 :link/attr :person/email :link/enc "xxxpeter@x"}
                {:db/id 20 :link/attr :person/email :link/enc "xxxanna@x"}])))

(def var-attr-people-db
  (delay
    (d/db-with (db/empty-db {:person/email {:db/unique :db.unique/identity}
                             :person/name  {:db/index true}})
               [{:person/email "peter@x" :person/name "Peter"}
                {:person/email "anna@x"  :person/name "Anna"}])))

(deftest test-variable-attribute-multisource-function-value
  (let [dba @var-attr-docs-db
        dbb @var-attr-people-db
        query '[:find ?title ?pname :in $a $b :where
                [$a ?doc :doc/title ?title]
                [$a ?doc :doc/author ?r]
                [$a ?r :link/attr ?attr]
                [$a ?r :link/enc ?enc]
                [(subs ?enc 3) ?val]
                [$b ?p ?attr ?val]
                [$b ?p :person/name ?pname]]
        expected #{["Doc1" "Peter"] ["Doc2" "Anna"]}]
    (testing "compiled planner correlates the variable attribute (no Cartesian product)"
      (is (= expected (binding [q/*disable-planner* false] (d/q query dba dbb)))))
    (testing "legacy engine handles the variable-attribute cross-source pattern"
      (is (= expected (binding [q/*disable-planner* true] (d/q query dba dbb)))))))

;; When the target relation is large enough to cross the AVET-seek cost
;; threshold, the variable-attribute scan `[$b ?p ?attr ?val]` (attr + value
;; bound upstream) must point-seek AVET per (attr, value) pair rather than
;; full-scanning — and must still return EXACTLY the matching entities, not the
;; unrelated bulk. This test forces C's seek path (thousands of non-matching
;; people) and asserts precise results.
(def var-attr-many-people-db
  (delay
    (d/db-with (db/empty-db {:person/email {:db/unique :db.unique/identity}
                             :person/name  {:db/index true}})
               (into [{:person/email "peter@x" :person/name "Peter"}
                      {:person/email "anna@x"  :person/name "Anna"}]
                     (map (fn [i] {:person/email (str "u" i "@x") :person/name (str "U" i)}))
                     (range 5000)))))

(deftest test-variable-attribute-multisource-avet-seek
  (let [dba @var-attr-docs-db
        dbb @var-attr-many-people-db
        query '[:find ?title ?pname :in $a $b :where
                [$a ?doc :doc/title ?title] [$a ?doc :doc/author ?r]
                [$a ?r :link/attr ?attr] [$a ?r :link/enc ?enc]
                [(subs ?enc 3) ?val]
                [$b ?p ?attr ?val] [$b ?p :person/name ?pname]]
        expected #{["Doc1" "Peter"] ["Doc2" "Anna"]}]
    (testing "AVET-seek path returns exactly the matching entities (large target)"
      (is (= expected (binding [q/*disable-planner* false] (d/q query dba dbb))))
      (is (= expected (binding [q/*disable-planner* true] (d/q query dba dbb)))))))
