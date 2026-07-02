(ns datahike.test.query-ir-test
  "Tests for the query planner (logical IR → lowering → execution).
   Validates that:
   1. Logical IR builds correct structure from where-clauses
   2. Lowered plans produce correct structure
   3. End-to-end planner results match legacy engine
   4. Pipeline annotations assign correct fused-paths"
  (:require
   [clojure.test :refer [deftest testing is are]]
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.query :as q]
   [datahike.query.analyze :as analyze]
   [datahike.query.ir :as ir]
   [datahike.query.logical :as logical]
   [datahike.query.lower :as lower]
   [datahike.query.plan :as plan]))

;; ---------------------------------------------------------------------------
;; Helpers

(defn- ir-plan [db clauses]
  (let [logical (logical/build-logical-plan db clauses #{} nil)]
    (lower/lower logical db nil)))

(defn- query-both
  "Run a query through both planner and legacy paths, return [planner legacy]."
  [query db]
  [(binding [q/*disable-planner* false q/*query-result-cache?* false]
     (d/q query db))
   (binding [q/*disable-planner* true q/*query-result-cache?* false]
     (d/q query db))])

(defmacro assert-ir-match
  "Assert that the planner produces the same query result as the legacy engine."
  [query db]
  `(let [[planner# legacy#] (query-both ~query ~db)]
     (is (= planner# legacy#)
         (str "Planner mismatch for " (pr-str ~query)))))

;; ---------------------------------------------------------------------------
;; Logical IR structure tests

(deftest test-logical-ir-single-pattern
  (let [db (db/empty-db {:name {}})
        plan (logical/build-logical-plan db '[[?e :name ?n]] #{} nil)
        nodes (.-nodes plan)]
    (is (= 1 (count nodes)))
    (is (instance? datahike.query.ir.LScan (first nodes)))
    (is (= '?e (.-e ^datahike.query.ir.LScan (first nodes))))
    (is (= :name (.-a ^datahike.query.ir.LScan (first nodes))))))

(deftest test-logical-ir-entity-join
  (let [db (db/empty-db {:name {} :age {}})
        plan (logical/build-logical-plan db '[[?e :name ?n] [?e :age ?a]] #{} nil)
        nodes (.-nodes plan)]
    (is (= 1 (count nodes)))
    (is (instance? datahike.query.ir.LEntityJoin (first nodes)))
    (let [^datahike.query.ir.LEntityJoin ej (first nodes)]
      (is (= '?e (.-entity_var ej)))
      (is (= 2 (count (.-scans ej))))
      (is (= 0 (count (.-anti_scans ej)))))))

(deftest test-logical-ir-anti-merge-folding
  (testing "Simple NOT folds into entity group as anti-scan"
    (let [db (db/empty-db {:name {} :age {} :flag {}})
          plan (logical/build-logical-plan
                db '[[?e :name ?n] [?e :age ?a] (not [?e :flag :deleted])] #{} nil)
          nodes (.-nodes plan)]
      (is (= 1 (count nodes)))
      (is (instance? datahike.query.ir.LEntityJoin (first nodes)))
      (let [^datahike.query.ir.LEntityJoin ej (first nodes)]
        (is (= 2 (count (.-scans ej))))
        (is (= 1 (count (.-anti_scans ej)))))))

  (testing "NOT-JOIN does NOT fold into entity group"
    (let [db (db/empty-db {:name {} :flag {}})
          plan (logical/build-logical-plan
                db '[[?e :name ?n] (not-join [?e] [?e :flag :deleted])] #{} nil)
          nodes (.-nodes plan)]
      ;; Should have separate scan and anti-join nodes
      (is (> (count nodes) 1))
      (is (some #(instance? datahike.query.ir.LAntiJoin %) nodes)))))

(deftest test-logical-ir-predicate
  (let [db (db/empty-db {:age {}})
        plan (logical/build-logical-plan
              db '[[?e :age ?a] [(> ?a 18)]] #{} nil)
        nodes (.-nodes plan)]
    (is (= 2 (count nodes)))
    (is (some #(instance? datahike.query.ir.LScan %) nodes))
    (is (some #(instance? datahike.query.ir.LFilter %) nodes))))

(deftest test-logical-ir-function
  (let [db (db/empty-db {:name {}})
        plan (logical/build-logical-plan
              db '[[?e :name ?n] [(str ?n " Jr.") ?full]] #{} nil)
        nodes (.-nodes plan)]
    (is (= 2 (count nodes)))
    (is (some #(instance? datahike.query.ir.LBind %) nodes))))

(deftest test-logical-ir-or
  (let [db (db/empty-db {:type {} :flag {}})
        plan (logical/build-logical-plan
              db '[(or [?e :type :admin] [?e :flag :special])] #{} nil)
        nodes (.-nodes plan)]
    (is (some #(instance? datahike.query.ir.LUnion %) nodes))))

(deftest test-logical-ir-multi-group
  (let [db (db/empty-db {:name {} :friend {}})
        plan (logical/build-logical-plan
              db '[[?e :name ?n] [?e2 :friend ?n]] #{} nil)
        nodes (.-nodes plan)]
    (is (= 2 (count nodes)))
    (is (every? #(instance? datahike.query.ir.LScan %) nodes))))

;; ---------------------------------------------------------------------------
;; Lowering shape tests
;;
;; These tests previously cross-checked the IR pipeline against the legacy
;; physical-only `plan/create-plan` to guard the migration. Now that the
;; legacy path is gone (PR removing `create-plan`) and every sub-plan
;; routes through the IR pipeline, the comparison would be a tautology.
;; End-to-end correctness is covered by `test-end-to-end-queries` (below)
;; and by the broader suite (`query-rules-test`, `jobtech-patterns-test`,
;; the planner-vs-legacy diff harness in `assert-engines-agree`).

(deftest test-lowered-entity-group-shape
  (testing "Lowered entity-group has the structural fields callers expect"
    (let [db (db/empty-db {:name {} :age {} :email {}})
          clauses '[[?e :name ?n] [?e :age ?a] [?e :email ?em]]
          eg (first (:ops (ir-plan db clauses)))]
      (is (= :entity-group (:op eg)))
      (is (= '?e (:entity-var eg)))
      (is (some? (:scan-op eg)))
      (is (vector? (:merge-ops eg)))
      (is (some? (:pipeline eg))))))

;; ---------------------------------------------------------------------------
;; End-to-end query correctness tests

(def ^:private test-db
  (d/db-with (db/empty-db {:name {} :age {} :email {} :friend {} :flag {} :salary {}})
             [{:db/id 1 :name "Alice" :age 30 :email "a@test.com" :salary 80000}
              {:db/id 2 :name "Bob" :age 15 :email "b@test.com" :friend "Alice"}
              {:db/id 3 :name "Carol" :age 25 :email "c@test.com" :flag :deleted}
              {:db/id 4 :name "Dave" :age 40 :friend "Alice" :salary 90000}]))

(deftest test-ir-pipeline-q1-single
  (assert-ir-match '[:find ?e :where [?e :name "Alice"]] test-db))

(deftest test-ir-pipeline-q2-entity-join
  (assert-ir-match '[:find ?e ?n :where [?e :name ?n] [?e :age ?a]] test-db))

(deftest test-ir-pipeline-q3-3way
  (assert-ir-match '[:find ?e ?n ?em :where [?e :name ?n] [?e :age ?a] [?e :email ?em]] test-db))

(deftest test-ir-pipeline-q4-4way
  (assert-ir-match
   '[:find ?e ?n ?em ?s :where [?e :name ?n] [?e :age ?a] [?e :email ?em] [?e :salary ?s]]
   test-db))

(deftest test-ir-pipeline-value-join
  (assert-ir-match '[:find ?e :where [?e :friend ?n] [?e2 :name ?n]] test-db))

(deftest test-ir-pipeline-predicate
  (assert-ir-match '[:find ?e :where [?e :age ?a] [(> ?a 18)]] test-db))

(deftest test-ir-pipeline-anti-merge
  (assert-ir-match
   '[:find ?e ?n :where [?e :name ?n] [?e :age ?a] (not [?e :flag :deleted]) [(> ?a 18)]]
   test-db))

(deftest test-ir-pipeline-or-clause
  (assert-ir-match '[:find ?e :where [?e :name ?n] (or [?e :age 30] [?e :age 25])] test-db))

(deftest test-ir-pipeline-not-join
  (assert-ir-match
   '[:find ?e :where [?e :name ?n] (not-join [?e] [?e :flag :deleted])]
   test-db))

(deftest test-ir-pipeline-function-binding
  (assert-ir-match
   '[:find ?e ?full :where [?e :name ?n] [(str ?n " Jr.") ?full]]
   test-db))

(deftest test-ir-pipeline-with-in-bindings
  (testing "IR plan builds cleanly for a `:in ?name` binding shape"
    (is (seq (:ops (ir-plan (db/empty-db {:name {}}) '[[?e :name ?name]]))))))

;; ---------------------------------------------------------------------------
;; Pipeline annotation tests

(defn- get-pipeline
  "Get the PPipeline from the first entity-group/pattern-scan op in an IR plan."
  [db clauses]
  (let [logical (logical/build-logical-plan db clauses #{} nil)
        plan (lower/lower logical db nil)
        op (first (filter #(#{:entity-group :pattern-scan} (:op %)) (:ops plan)))]
    (:pipeline op)))

(deftest test-pipeline-fused-path-scan-only
  (testing "Single pattern → :scan-only"
    (let [pl (get-pipeline (db/empty-db {:name {}}) '[[?e :name "Alice"]])]
      (is (= :scan-only (:fused-path pl)))
      (is (false? (:use-cursors? pl))))))

(deftest test-pipeline-fused-path-sorted-merge
  (testing "Entity join, all card-one, no anti → :sorted-merge"
    (let [pl (get-pipeline (db/empty-db {:name {} :age {}}) '[[?e :name ?n] [?e :age ?a]])]
      (is (= :sorted-merge (:fused-path pl)))
      (is (true? (:use-cursors? pl))))))

(deftest test-pipeline-fused-path-per-cursor-merge
  (testing "Entity join with anti-merge → :per-cursor-merge"
    (let [pl (get-pipeline (db/empty-db {:name {} :age {} :flag {}})
                           '[[?e :name ?n] [?e :age ?a] (not [?e :flag :deleted])])]
      (is (= :per-cursor-merge (:fused-path pl)))
      (is (true? (:use-cursors? pl))))))

(deftest test-pipeline-fused-path-card-many
  (testing "Entity join with card-many attribute → :card-many-merge"
    (let [pl (get-pipeline (db/empty-db {:name {} :tags {:db/cardinality :db.cardinality/many}})
                           '[[?e :name ?n] [?e :tags ?t]])]
      (is (= :card-many-merge (:fused-path pl))))))

(deftest test-pipeline-step-sequence
  (testing "scan-only pipeline has IndexScan, GroundFilter, ProbeFilter, EmitTuple"
    (let [pl (get-pipeline (db/empty-db {:name {}}) '[[?e :name ?n]])
          step-types (mapv type (:steps pl))]
      (is (= [datahike.query.ir.PIndexScan
              datahike.query.ir.PGroundFilter
              datahike.query.ir.PProbeFilter
              datahike.query.ir.PEmitTuple]
             step-types))))

  (testing "sorted-merge pipeline has SortedMerge step"
    (let [pl (get-pipeline (db/empty-db {:name {} :age {}})
                           '[[?e :name ?n] [?e :age ?a]])
          step-types (mapv type (:steps pl))]
      (is (= [datahike.query.ir.PIndexScan
              datahike.query.ir.PGroundFilter
              datahike.query.ir.PProbeFilter
              datahike.query.ir.PSortedMerge
              datahike.query.ir.PEmitTuple]
             step-types))))

  (testing "per-cursor-merge pipeline has PerCursorMerge step"
    (let [pl (get-pipeline (db/empty-db {:name {} :age {} :flag {}})
                           '[[?e :name ?n] [?e :age ?a] (not [?e :flag :deleted])])
          step-types (mapv type (:steps pl))]
      (is (some #(= datahike.query.ir.PPerCursorMerge %) step-types)))))

(deftest test-pipeline-structurally-fusable
  (testing "Plan with only entity-groups is structurally fusable"
    (let [logical (logical/build-logical-plan
                   (db/empty-db {:name {} :age {}})
                   '[[?e :name ?n] [?e :age ?a]] #{} nil)
          plan (lower/lower logical (db/empty-db {:name {} :age {}}) nil)]
      (is (true? (:structurally-fusable? plan)))))

  (testing "Single-group plan with predicate ops is structurally fusable"
    (let [logical (logical/build-logical-plan
                   (db/empty-db {:age {}})
                   '[[?e :age ?a] [(> ?a 18)]] #{} nil)
          plan (lower/lower logical (db/empty-db {:age {}}) nil)]
      ;; Single-group with predicate is fusable (post-filter pipeline)
      (is (true? (:structurally-fusable? plan))))))

(deftest test-pipeline-always-present
  (testing "All plans now have :pipeline annotation"
    (let [db (d/db-with (db/empty-db {:name {} :age {}})
                        [{:db/id 1 :name "Alice" :age 30}])
          plan (ir-plan db '[[?e :name ?n] [?e :age ?a]])]
      (is (every? #(some? (:pipeline %))
                  (filter #(#{:entity-group :pattern-scan} (:op %)) (:ops plan)))))))

(deftest test-merge-lost-preds-restored
  (testing "Predicates on merge-op attributes are not silently dropped"
    ;; Regression: predicates consumed as pushdowns on patterns that become
    ;; merge-ops were silently lost, returning too many results.
    (let [db (d/db-with (db/empty-db {:salary {} :age {}})
                        (for [i (range 100)]
                          {:salary (+ 40000 (* i 200))
                           :age (+ 18 (mod i 30))}))
          planner-result (binding [q/*disable-planner* false q/*query-result-cache?* false]
                           (d/q '[:find ?e ?s ?a
                                  :where [?e :salary ?s] [?e :age ?a]
                                  [(> ?s 50000)] [(< ?a 24)]]
                                db))
          legacy-result (binding [q/*disable-planner* true q/*query-result-cache?* false]
                          (d/q '[:find ?e ?s ?a
                                 :where [?e :salary ?s] [?e :age ?a]
                                 [(> ?s 50000)] [(< ?a 24)]]
                               db))]
      (is (= planner-result legacy-result)
          "Both predicates must be applied — merge-op pushdowns must be restored")
      (is (< (count planner-result) 100)
          "Result should be filtered by both predicates"))))

;; ---------------------------------------------------------------------------
;; op-required-vars contract — single source of truth for per-op-type binding
;; requirements consumed by op-cost during cost-based ordering. Tests assert
;; the documented contract per op type so future refactors don't drift.

(deftest test-op-required-vars
  (testing ":entity-group / non-optional :pattern-scan are producers"
    (is (= [#{} :none] (plan/op-required-vars
                        {:op :pattern-scan :clause '[?e :name ?n] :vars #{'?e '?n}})))
    (is (= [#{} :none] (plan/op-required-vars
                        {:op :entity-group :clause '[?e :name ?n] :vars #{'?e '?n}}))))

  (testing ":pattern-scan with :optional? requires its e-var bound"
    ;; LOptionalScan synthesised from get-else; e is the first clause element.
    (is (= [#{'?e} :all]
           (plan/op-required-vars
            {:op :pattern-scan :optional? true
             :clause '[?e :concept/deprecated ?d]
             :default-value false
             :vars #{'?e '?d}})))
    ;; If e is ground (a number), no var to require.
    (is (= [#{} :none]
           (plan/op-required-vars
            {:op :pattern-scan :optional? true
             :clause '[42 :concept/deprecated ?d]
             :default-value false
             :vars #{'?d}}))))

  (testing ":predicate requires every referenced var"
    (is (= [#{'?x '?y} :all]
           (plan/op-required-vars
            {:op :predicate :fn-sym '< :args ['?x '?y] :vars #{'?x '?y}}))))

  (testing ":function requires every input-arg var (excludes output binding)"
    (is (= [#{'?x} :all]
           (plan/op-required-vars
            {:op :function :fn-sym 'inc :args ['?x] :binding '?y :vars #{'?x '?y}}))))

  (testing ":external-engine honours :input-vars-spec"
    ;; :all-bound (or absent) → strict: every input-arg must be bound.
    ;; args-free-vars filters to free-vars (?…), excluding source symbols
    ;; like $ — source symbols are always available, never need binding.
    (let [op {:op :external-engine
              :args ['$ '?col1 '?col2]
              :binding '[[?out]]
              :vars #{'?col1 '?col2 '?out}
              :input-vars-spec :all-bound}
          [vars policy] (plan/op-required-vars op)]
      (is (= :all policy))
      (is (= #{'?col1 '?col2} vars)))
    ;; Default (no spec declared) is :all — safer than the legacy `some`
    ;; check that used to live in op-cost.
    (let [op {:op :external-engine
              :args ['$ '?col1]
              :binding '[?out]
              :vars #{'?col1 '?out}}
          [_ policy] (plan/op-required-vars op)]
      (is (= :all policy)))
    ;; :any-bound declared → :any.
    (let [op {:op :external-engine
              :args ['$ '?col1 '?col2]
              :binding '[?out]
              :vars #{'?col1 '?col2 '?out}
              :input-vars-spec :any-bound}
          [_ policy] (plan/op-required-vars op)]
      (is (= :any policy))))

  (testing ":rule-lookup is a producer (accumulator-driven, no pre-bound deps)"
    (is (= [#{} :none] (plan/op-required-vars
                        {:op :rule-lookup :rule-name 'some-rule
                         :call-args ['?a '?b] :mode :main :vars #{'?a '?b}}))))

  (testing ":not / :not-join require the full set of join-vars / referenced vars"
    (is (= [#{'?e '?n} :all]
           (plan/op-required-vars {:op :not :vars #{'?e '?n}})))
    (is (= [#{'?e} :all]
           (plan/op-required-vars
            {:op :not-join :vars #{'?e '?n} :join-vars #{'?e}}))))

  (testing ":or / :or-join require all join-vars NOT covered by every branch"
    ;; No :branches → no branch covers any var → all join-vars required.
    (is (= [#{'?e '?v} :all]
           (plan/op-required-vars {:op :or :vars #{'?e '?v}})))
    (is (= [#{'?e} :all]
           (plan/op-required-vars
            {:op :or-join :vars #{'?e '?v} :join-vars #{'?e}})))))

(deftest test-op-cost-ready-vs-blocked
  (testing "op-cost returns max-cost for unsatisfied required vars,
            finite cost when satisfied"
    ;; predicate — costs 0 when ready (cheapest filter)
    (let [op {:op :predicate :args ['?x '?y] :vars #{'?x '?y}}]
      (is (= 0 (plan/op-cost op #{'?x '?y}))
          "predicate is the cheapest op type when its inputs are bound")
      (is (= Long/MAX_VALUE (plan/op-cost op #{'?x}))
          "missing one input var → blocked"))
    ;; function — costs 1 when ready
    (let [op {:op :function :args ['?x] :binding '?y :vars #{'?x '?y}}]
      (is (= 1 (plan/op-cost op #{'?x}))
          "function costs 1 when its inputs are bound")
      (is (= Long/MAX_VALUE (plan/op-cost op #{}))
          "no input bound → blocked"))
    ;; or-join — :all policy on join-vars NOT covered by every branch.
    ;; With no :branches, no var is covered → all join-vars required.
    (let [op {:op :or-join :vars #{'?e '?v} :join-vars #{'?e '?v}
              :estimated-card 50}]
      (is (= Long/MAX_VALUE (plan/op-cost op #{}))
          "no join-var bound → blocked")
      (is (= Long/MAX_VALUE (plan/op-cost op #{'?e}))
          "one join-var bound but not all → still blocked under :all policy")
      (is (< (plan/op-cost op #{'?e '?v}) Long/MAX_VALUE)
          "all join-vars bound → ready"))))

;; ---------------------------------------------------------------------------
;; source-card: unified produce-side cardinality seeding (patterns / binds / :in)

(deftest source-cards-input-arm
  (testing "plan/source-cards :input — collection/relation bind many rows, tuple/scalar one"
    (is (= '{?x 100} (plan/source-cards {:kind :input :shape :collection :vars '[?x]}))
        "collection :in seeds the unknown-size default, not 1")
    (is (= '{?a 100 ?b 100} (plan/source-cards {:kind :input :shape :relation :vars '[?a ?b]}))
        "relation :in seeds every column")
    (is (nil? (plan/source-cards {:kind :input :shape :tuple :vars '[?a ?b]}))
        "tuple :in is a single row → no override (card-1 placeholder stands)")
    (is (nil? (plan/source-cards {:kind :input :shape :scalar :vars '[?x]}))
        "scalar :in is a single value → no override")))

(def ^:private in-card-test-db
  ;; :name spans 500 entities — a join on it is broad unless the other side bounds it.
  (-> (db/empty-db {:name {:db/unique :db.unique/identity}})
      (d/db-with (mapv (fn [i] {:db/id (+ i 100) :name (str i)}) (range 500)))))

(defn- scan-card-with-in
  "scan-card the planner assigns to the :name pattern when ?v is an :in var,
   with the given in-cards seed (nil = today's card-1 placeholder)."
  [in-cards]
  (let [logical (logical/build-logical-plan in-card-test-db '[[?e :name ?v]] '#{?v} nil)
        plan (lower/lower logical in-card-test-db nil in-cards)]
    (some (fn [op] (when (and (= :pattern-scan (:op op)) (= :name (second (:clause op))))
                     (:scan-card op)))
          (:ops plan))))

(deftest in-collection-cardinality-flows-into-scan
  (testing "a collection :in var seeds a multi-row source, not a point lookup"
    (let [placeholder (scan-card-with-in nil)
          seeded (scan-card-with-in '{?v 100})]
      (is (= 1 placeholder)
          "without the seed, the :in var is treated as a single value (card 1)")
      (is (= 100 seeded)
          "with the source-card :input seed, the join is sized as a 100-row source")
      (is (< placeholder seeded)
          "the seed corrects the under-estimate that mis-orders collection joins"))))

(deftest in-card-seed-distinguishes-tuple-from-relation
  (testing "tuple [?a ?b] and relation [[?a ?b]] differ — must not collide in the plan cache"
    (let [tuple-seed (#'q/in-card-seed
                      (:qin (q/memoized-parse-query '[:find ?e :in $ [?a ?b] :where [?e :name ?a]])))
          rel-seed (#'q/in-card-seed
                    (:qin (q/memoized-parse-query '[:find ?e :in $ [[?a ?b]] :where [?e :name ?a]])))]
      (is (= '{} tuple-seed) "tuple binds one row → empty seed")
      (is (= '{?a 100 ?b 100} rel-seed) "relation binds many rows → non-empty seed")
      (is (not= tuple-seed rel-seed)
          "distinct seeds keep the two queries on distinct plan-cache keys"))))

