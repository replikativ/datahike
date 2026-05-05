(ns datahike.test.query-planner-temporal-test
  "Regression tests for planner bugs surfaced by the jobtech-taxonomy-api
   test suite. Each test is a minimal repro that compares the planner
   path against the legacy engine on a HistoricalDB in :attribute-refs?
   mode — the combination that exposed each bug in production. The tests
   should pass on both engines; the assertion form is `(= legacy planner)`
   so a future regression in either path is caught."
  (:require
   [clojure.test :refer [deftest is testing]]
   [datahike.api :as d]
   [datahike.query :as q])
  (:import [java.util UUID]))

(defn- fresh-cfg
  "In-memory DB with history enabled and attr-refs on — the mode where
   datoms are stored as `(e attr-eid v tx)` rather than `(e :keyword v tx)`,
   so any keyword-attribute pattern that reaches the executor without
   keyword→eid resolution silently slices an empty range. `:writer
   {:backend :self}` makes `transact` return its TxReport synchronously
   so we don't have to deref a Future."
  []
  {:store {:backend :memory :id (UUID/randomUUID)}
   :writer {:backend :self}
   :schema-flexibility :write
   :keep-history? true
   :attribute-refs? true})

(defn- run-both
  "Execute `q-form` once with the planner enabled and once forced to legacy.
   Return both result sets so tests can assert equality, capture both into
   the failure message, and check the planner result independently."
  [q-form & inputs]
  {:legacy  (binding [q/*force-legacy* true]
              (apply d/q q-form inputs))
   :planner (binding [q/*force-legacy* false]
              (apply d/q q-form inputs))})

;; ---------------------------------------------------------------------------
;; Bug 1 — rule body with nested or-join + :attribute-refs? mode
;;
;; A rule body that consists of a single compound form (or-join / or / and)
;; was not having its INNER data patterns' attribute keywords rewritten to
;; eids. The single-level resolve-clause inside rename-branch-vars only
;; matched top-level data patterns:
;;   (and (vector? clause) (not (sequential? (first clause))) ...)
;; — so for a body like `(or-join [...] (and [?e :attr ?v] ...))` the
;; clause itself is a list, vector? returns false, the rewrite is skipped,
;; every nested pattern keeps its keyword, and lookup-batch-search slices
;; AEVT for the keyword (no match in attr-refs storage), returning 0 rows.
;;
;; Surface symptom in jobtech: 3 changelog GraphQL tests on HistoricalDB
;; returned 0 events instead of 2-5. Fixed in
;; src/datahike/query/plan.cljc rename-branch-vars by replacing the
;; one-level resolve-clause with resolve-recursive that descends into
;; or / or-join / and / not / not-join compound forms.

(deftest test-rule-body-or-join-attr-refs
  (testing "rule body containing (or-join ...) with data patterns resolves
            attribute keywords for both engines on HistoricalDB"
    (let [cfg (fresh-cfg)]
      (try
        (d/create-database cfg)
        (let [conn (d/connect cfg)]
          (d/transact conn
                       [{:db/ident :version/id
                         :db/cardinality :db.cardinality/one
                         :db/valueType :db.type/long
                         :db/unique :db.unique/identity
                         :db/index true}
                        {:db/ident :version/tx
                         :db/cardinality :db.cardinality/one
                         :db/valueType :db.type/ref}])
          ;; Two version markers, each linked to its own creation tx
          (d/transact conn [{:version/id 1 :version/tx "datomic.tx"}])
          (d/transact conn [{:version/id 2 :version/tx "datomic.tx"}])

          (let [hdb     (d/history (d/db conn))
                ;; Rule body is a single (or-join …) compound. Without
                ;; recursive attr-resolution the planner returns ∅.
                ;; Legacy can only execute this rule when the input
                ;; is concretely bound BEFORE the rule call (otherwise
                ;; legacy sum-rel chokes on heterogeneous branch attrs);
                ;; the production callers all bind via
                ;; `[(identity ?in-input) ?in]` first, so we mirror that
                ;; here to keep the test exercising the bug we fixed
                ;; rather than an orthogonal legacy edge case.
                rules   '[[(->inst ?in ?ret)
                           (or-join [?in ?ret]
                             (and [(int? ?in)]
                                  [?v :version/id ?in]
                                  [?v :version/tx ?tx]
                                  [?tx :db/txInstant ?ret])
                             (and [(inst? ?in)]
                                  [(identity ?in) ?ret]))]]
                q-form  '[:find ?ret
                          :in $ % ?in-input
                          :where
                          [(identity ?in-input) ?in]
                          (->inst ?in ?ret)]
                {:keys [legacy planner]} (run-both q-form hdb rules 1)]
            (is (= 1 (count legacy))
                "legacy resolves the rule and returns one tx-instant")
            (is (= legacy planner)
                "planner must match legacy under attribute-refs+history")
            (is (instance? java.util.Date (ffirst planner))
                "planner returns a real instant, not nil/empty")))
        (finally (d/delete-database cfg))))))

;; ---------------------------------------------------------------------------
;; Bug 2 — pushed-down predicate dropped in entity-group temporal fallback
;;
;; The planner pushes range predicates like `[(<= ?from ?inst)]` onto the
;; relevant scan-op's :pushdown-preds and consumes the original clause
;; (so it never reappears as a standalone :predicate op). On regular DBs
;; the fused PSS scan path applies the predicate via slice bounds and a
;; strict-filter; on HistoricalDB / AsOfDB / SinceDB the fallback
;; delegates to legacy/lookup-batch-search, which has no concept of
;; :pushdown-preds — the predicate is silently dropped and rows that
;; would have been filtered out leak through.
;;
;; Surface symptom in jobtech: 3 daynotes-test cases returned more rows
;; than legacy because their `[(<= ?from-version ?inst)]` predicate was
;; pushed onto the [?tx :db/txInstant ?inst] scan but never evaluated.
;; Fixed in src/datahike/query/execute.cljc :entity-group temporal
;; branch by re-applying scan-op + merge-op :pushdown-preds via
;; legacy/filter-by-pred after the scan/merge/optional/anti phases.

(deftest test-temporal-entity-group-pushdown-pred
  (testing "range predicate pushed onto entity-group scan-op is applied
            on HistoricalDB"
    (let [cfg (fresh-cfg)]
      (try
        (d/create-database cfg)
        (let [conn (d/connect cfg)]
          (d/transact conn
                       [{:db/ident :event/marker
                         :db/cardinality :db.cardinality/one
                         :db/valueType :db.type/string
                         :db/index true}
                        {:db/ident :event/category
                         :db/cardinality :db.cardinality/one
                         :db/valueType :db.type/string}])
          ;; Three transactions with distinct txInstants. A predicate
          ;; bounded between tx2 and tx3 must keep ONLY the middle event.
          (d/transact conn [{:event/marker "e1" :event/category "alpha"}])
          (Thread/sleep 5)
          (d/transact conn [{:event/marker "e2" :event/category "beta"}])
          (Thread/sleep 5)
          (d/transact conn [{:event/marker "e3" :event/category "alpha"}])

          (let [hdb (d/history (d/db conn))
                ;; Three event-bearing txs in chronological order. We
                ;; pick a window that excludes the FIRST event but
                ;; includes the other two — the predicate must shrink
                ;; the result set from 3 → 2.
                event-tx-instants (sort
                                   (mapv first
                                         (d/q '[:find ?inst
                                                :where
                                                [_ :event/marker ?m ?tx true]
                                                [?tx :db/txInstant ?inst]]
                                              hdb)))
                e1-inst (nth event-tx-instants 0)
                e3-inst (nth event-tx-instants 2)
                ;; Range: > e1, ≤ e3 — should keep e2 and e3.
                from-inst (java.util.Date/from
                           (.plus (.toInstant ^java.util.Date e1-inst)
                                  1 java.time.temporal.ChronoUnit/MILLIS))
                to-inst   e3-inst
                q-form '[:find ?m
                         :in $ ?from-inst ?to-inst
                         :where
                         [?e :event/marker ?m ?tx true]
                         [?tx :db/txInstant ?inst]
                         [(<= ?from-inst ?inst)]
                         [(<= ?inst ?to-inst)]]
                {:keys [legacy planner]} (run-both q-form hdb from-inst to-inst)]
            (is (= 2 (count legacy))
                "legacy filters out e1, keeps e2 and e3")
            (is (= legacy planner)
                "planner must match legacy — pushed-down predicate
                  must be re-applied in the temporal fallback path")
            (is (= #{["e2"] ["e3"]} (set planner))
                "planner result must drop e1 (whose ?inst < ?from-inst)")))
        (finally (d/delete-database cfg))))))
