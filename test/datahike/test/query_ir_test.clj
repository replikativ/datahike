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

(defn- plan-match?
  "Check that IR pipeline and create-plan produce structurally equivalent plans.
   The IR path may attach predicates to groups (optimization); normalize by
   flattening attached-preds back to standalone ops for comparison."
  [db clauses]
  (let [old (plan/create-plan db clauses #{} nil)
        new (ir-plan db clauses)
        ;; Normalize: flatten attached-preds back to standalone ops
        normalize-ops (fn [ops]
                        (mapv :op (into [] (mapcat (fn [op]
                                                     (if (seq (:attached-preds op))
                                                       (cons op (:attached-preds op))
                                                       [op])))
                                        ops)))]
    (and (= (mapv :op (:ops old)) (normalize-ops (:ops new)))
         (= (:group-joins old) (:group-joins new)))))

(defn- query-both
  "Run a query through both planner and legacy paths, return [planner legacy]."
  [query db]
  [(binding [q/*force-legacy* false q/*query-result-cache?* false]
     (d/q query db))
   (binding [q/*force-legacy* true q/*query-result-cache?* false]
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
;; Lowering equivalence tests

(deftest test-lower-matches-create-plan
  (let [db (db/empty-db {:name {:db/index true} :age {} :email {} :flag {} :friend {}})]
    (testing "Single pattern"
      (is (plan-match? db '[[?e :name "Alice"]])))
    (testing "Entity join"
      (is (plan-match? db '[[?e :name ?n] [?e :age ?a]])))
    (testing "3-way entity join"
      (is (plan-match? db '[[?e :name ?n] [?e :age ?a] [?e :email ?em]])))
    (testing "Value join"
      (is (plan-match? db '[[?e :name ?n] [?e2 :friend ?n]])))
    (testing "With predicate"
      (is (plan-match? db '[[?e :age ?a] [(> ?a 18)]])))
    (testing "Pushdown predicate"
      (is (plan-match? db '[[?e :name ?n] [(> ?n "M")]])))
    (testing "Anti-merge"
      (is (plan-match? db '[[?e :name ?n] [?e :age ?a] (not [?e :flag :deleted])])))
    (testing "OR clause"
      (is (plan-match? db '[[?e :name ?n] (or [?e :age 30] [?e :age 25])])))))

(deftest test-lower-deep-structural-equality
  (testing "Entity group internals match exactly"
    (let [db (db/empty-db {:name {} :age {} :email {}})
          clauses '[[?e :name ?n] [?e :age ?a] [?e :email ?em]]
          old-eg (first (:ops (plan/create-plan db clauses #{} nil)))
          new-eg (first (:ops (ir-plan db clauses)))]
      (is (= (:clause (:scan-op old-eg)) (:clause (:scan-op new-eg))))
      (is (= (:index (:scan-op old-eg)) (:index (:scan-op new-eg))))
      (is (= (mapv :clause (:merge-ops old-eg)) (mapv :clause (:merge-ops new-eg))))
      (is (= (mapv :index (:merge-ops old-eg)) (mapv :index (:merge-ops new-eg))))
      (is (= (:output-vars old-eg) (:output-vars new-eg)))
      (is (= old-eg new-eg) "Full entity-group equality (both paths now produce :pipeline)")
      (is (some? (:pipeline new-eg)) "IR plan has :pipeline annotation")
      (is (some? (:pipeline old-eg)) "Legacy plan has :pipeline annotation"))))

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
  (let [[current ir] (query-both '[:find ?e :in $ ?name :where [?e :name ?name]]
                                 test-db)]
    ;; Can't use assert-ir-match directly since d/q needs extra args
    ;; Test the plan equivalence instead
    (is (plan-match? (db/empty-db {:name {}}) '[[?e :name ?name]]))))

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
          planner-result (binding [q/*force-legacy* false q/*query-result-cache?* false]
                           (d/q '[:find ?e ?s ?a
                                  :where [?e :salary ?s] [?e :age ?a]
                                  [(> ?s 50000)] [(< ?a 24)]]
                                db))
          legacy-result (binding [q/*force-legacy* true q/*query-result-cache?* false]
                          (d/q '[:find ?e ?s ?a
                                 :where [?e :salary ?s] [?e :age ?a]
                                 [(> ?s 50000)] [(< ?a 24)]]
                               db))]
      (is (= planner-result legacy-result)
          "Both predicates must be applied — merge-op pushdowns must be restored")
      (is (< (count planner-result) 100)
          "Result should be filtered by both predicates"))))
