(ns datahike.test.cross-tx-vt-validation-test
  "Tests for the cross-tx vf<vt validation guard at the transactor.

   The standard `vf < vt` check on `:tx-meta` (transaction.cljc:1027)
   only validates the CURRENT tx's own valid-time meta. A retroactive
   `[:db/add prior-tx-eid :db.valid/{from,to} ...]` write produces a
   closure formed against the prior tx's existing meta — and without
   this cross-tx guard would silently corrupt the prior tx's queryable
   window when the resulting (vf, vt) pair has vf >= vt.

   These tests pin the guard's behaviour against the 8 scenarios from
   the design discussion: retroactive vt, retroactive vf, both-halves-
   in-one-tx, cardinality-one-double-write, :db.fn/cas, retract-alone,
   retract-plus-add, and the no-op fast path (no vt-meta writes at all)."
  (:require [clojure.test :as t :refer [is deftest testing]]
            [datahike.api :as d]))

(defn- fresh-conn []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write
             :keep-history? true}]
    (d/create-database cfg)
    (d/connect cfg)))

(defn- transact-ex-data
  "datahike's async writer wraps the raw ExceptionInfo in a
   java.util.concurrent.ExecutionException, which then propagates as
   the outer ExceptionInfo's cause. Walk the chain until we find the
   non-empty ex-data."
  [^Throwable e]
  (loop [t e]
    (cond
      (nil? t) nil
      (seq (ex-data t)) (ex-data t)
      :else (recur (.getCause t)))))

(defn- install-schema! [conn]
  (d/transact conn
              [{:db/ident :p/k
                :db/valueType :db.type/string
                :db/unique :db.unique/identity
                :db/cardinality :db.cardinality/one}
               {:db/ident :p/n
                :db/valueType :db.type/long
                :db/cardinality :db.cardinality/one}]))

(defn- post-with-vt!
  "Post one entity at the given vf (and optional vt). Returns the
   tx-eid of the resulting tx for later retroactive testing."
  [conn k vf & [vt]]
  (let [r (d/transact conn {:tx-data [{:p/k k :p/n 1}]
                            :tx-meta (cond-> {:db.valid/from vf}
                                       vt (assoc :db.valid/to vt))})]
    (->> (:tx-data r)
         (filter #(= :db/txInstant (.-a %)))
         first
         .-tx)))

;; ============================================================================
;; Scenario 1: retroactive :db.valid/to write — good + bad paths
;; ============================================================================

(deftest retroactive-vt-to-good-path
  (testing "Writing :db.valid/to > existing vf is accepted (the
            standard 'close a window' supersession move)."
    (let [conn (fresh-conn)
          _ (install-schema! conn)
          txA (post-with-vt! conn "x" #inst "2026-01-31")]
      (is (some? (d/transact conn [{:db/id txA
                                    :db.valid/to #inst "2026-02-15"}]))
          "valid closure accepted"))))

(deftest retroactive-vt-to-bad-path
  (testing "Writing :db.valid/to <= existing vf is rejected with
            :transact/invalid-valid-times-cross-tx."
    (let [conn (fresh-conn)
          _ (install-schema! conn)
          txA (post-with-vt! conn "x" #inst "2026-01-31")]
      (try
        (d/transact conn [{:db/id txA
                           :db.valid/to #inst "2026-01-15"}])
        (is false "expected throw")
        (catch Exception e
          (let [data (transact-ex-data e)]
            (is (= :transact/invalid-valid-times-cross-tx (:error data)))
            (is (= txA (:tx-eid data)))
            (is (= #inst "2026-01-31" (:db.valid/from data)))
            (is (= #inst "2026-01-15" (:db.valid/to data)))))))))

(deftest retroactive-vt-to-equal-vf-rejected
  (testing "vt == vf (zero-width window) is also rejected — matches the
            existing same-tx guard semantic (strict vf < vt)."
    (let [conn (fresh-conn)
          _ (install-schema! conn)
          txA (post-with-vt! conn "x" #inst "2026-01-31")]
      (is (thrown-with-msg? Exception #"Invalid cross-tx valid-time window"
                            (d/transact conn [{:db/id txA
                                               :db.valid/to #inst "2026-01-31"}]))))))

;; ============================================================================
;; Scenario 2: retroactive :db.valid/from write — symmetric
;; ============================================================================

(deftest retroactive-vt-from-good-path
  (testing "Writing :db.valid/from < existing vt is accepted (shifting
            the window's lower bound)."
    (let [conn (fresh-conn)
          _ (install-schema! conn)
          txA (post-with-vt! conn "x" #inst "2026-01-31" #inst "2026-02-15")]
      (is (some? (d/transact conn [{:db/id txA
                                    :db.valid/from #inst "2026-01-01"}]))))))

(deftest retroactive-vt-from-bad-path
  (testing "Writing :db.valid/from >= existing vt is rejected."
    (let [conn (fresh-conn)
          _ (install-schema! conn)
          txA (post-with-vt! conn "x" #inst "2026-01-31" #inst "2026-02-15")]
      (is (thrown-with-msg? Exception #"Invalid cross-tx valid-time window"
                            (d/transact conn [{:db/id txA
                                               :db.valid/from #inst "2026-03-01"}]))))))

;; ============================================================================
;; Scenario 3: both halves written in the same tx — combined-state check
;; ============================================================================

(deftest both-halves-in-one-tx-good
  (testing "Same tx writes BOTH vf and vt on a prior tx-entity. Validation
            sees the FINAL combined state — vf < vt → accepted."
    (let [conn (fresh-conn)
          _ (install-schema! conn)
          txA (post-with-vt! conn "x" #inst "2026-01-31")]
      ;; tx-A had vf=jan-31, no vt. Re-stamp the window entirely:
      (is (some? (d/transact conn [{:db/id txA
                                    :db.valid/from #inst "2026-01-01"
                                    :db.valid/to #inst "2026-02-01"}]))))))

(deftest both-halves-in-one-tx-bad
  (testing "Same tx writes BOTH vf and vt producing vf >= vt → rejected."
    (let [conn (fresh-conn)
          _ (install-schema! conn)
          txA (post-with-vt! conn "x" #inst "2026-01-31")]
      (is (thrown-with-msg? Exception #"Invalid cross-tx valid-time window"
                            (d/transact conn [{:db/id txA
                                               :db.valid/from #inst "2026-03-01"
                                               :db.valid/to #inst "2026-02-01"}]))))))

;; ============================================================================
;; Scenario 4: cardinality-one double-write within one tx — last wins
;; ============================================================================

(deftest cardinality-one-double-write-final-wins
  (testing "Same tx writes :db.valid/to twice; cardinality-one means the
            second write wins. The deferred check sees the final state."
    (let [conn (fresh-conn)
          _ (install-schema! conn)
          txA (post-with-vt! conn "x" #inst "2026-01-31")]
      ;; First write (vt=feb-01) is valid. Second write (vt=jan-15) is invalid.
      ;; Final state: vf=jan-31, vt=jan-15 → reject.
      (is (thrown-with-msg? Exception #"Invalid cross-tx valid-time window"
                            (d/transact conn [{:db/id txA
                                               :db.valid/to #inst "2026-02-01"}
                                              {:db/id txA
                                               :db.valid/to #inst "2026-01-15"}]))))))

;; ============================================================================
;; Scenario 5: :db.fn/cas (lowered to retract+add) — caught by add side
;; ============================================================================

(deftest cas-bad-vt-rejected
  (testing ":db.fn/cas on :db.valid/to with a bad new value is rejected —
            the lowered :db/add fires the cross-tx tracker."
    (let [conn (fresh-conn)
          _ (install-schema! conn)
          txA (post-with-vt! conn "x" #inst "2026-01-31" #inst "2026-02-15")]
      (is (thrown-with-msg? Exception #"Invalid cross-tx valid-time window"
                            (d/transact conn [[:db/cas txA :db.valid/to
                                               #inst "2026-02-15"
                                               #inst "2026-01-15"]]))))))

;; ============================================================================
;; Scenario 6: retract alone — never invalidates (can only broaden window)
;; ============================================================================

(deftest retract-alone-no-validation-needed
  (testing "Retracting :db.valid/to alone broadens window to [vf, ∞);
            cannot invalidate. The guard is skipped on retract-only paths."
    (let [conn (fresh-conn)
          _ (install-schema! conn)
          txA (post-with-vt! conn "x" #inst "2026-01-31" #inst "2026-02-15")]
      (is (some? (d/transact conn [[:db/retract txA :db.valid/to
                                    #inst "2026-02-15"]])))
      ;; Confirm the retract took effect: only vf remains on tx-A.
      (let [datoms (vec (d/datoms (d/db conn) :eavt txA))
            has-vt? (some #(= :db.valid/to (.-a %)) datoms)]
        (is (not has-vt?))))))

;; ============================================================================
;; Scenario 7: retract+add same tx — add-side triggers the guard
;; ============================================================================

(deftest retract-plus-add-same-tx-rejected
  (testing "Retract :db.valid/to, then add a bad new value, in the same
            tx. The add fires the tracker; deferred check sees the
            invalid final state and rejects."
    (let [conn (fresh-conn)
          _ (install-schema! conn)
          txA (post-with-vt! conn "x" #inst "2026-01-31" #inst "2026-02-15")]
      (is (thrown-with-msg? Exception #"Invalid cross-tx valid-time window"
                            (d/transact conn [[:db/retract txA :db.valid/to
                                               #inst "2026-02-15"]
                                              [:db/add txA :db.valid/to
                                               #inst "2026-01-15"]]))))))

;; ============================================================================
;; Scenario 8: the no-op fast path — no vt-meta writes at all
;; ============================================================================

(deftest no-vt-meta-writes-no-bookkeeping-leak
  (testing "A tx that writes no :db.valid/from / :db.valid/to attrs
            triggers no bookkeeping and the returned TxReport carries
            only the canonical 5 keys (no ::pending-vt-validation leak)."
    (let [conn (fresh-conn)
          _ (install-schema! conn)
          r (d/transact conn [{:p/k "y" :p/n 42}])]
      (is (= #{:db-before :db-after :tx-data :tempids :tx-meta}
             (set (keys r)))
          "TxReport carries only the canonical keys"))))

(deftest current-tx-vt-meta-not-tracked
  (testing "Writes to the CURRENT tx's :db.valid/from / :db.valid/to
            (i.e., the standard :tx-meta path via {:db/id 'datomic.tx'})
            do NOT fire the cross-tx guard — they're handled by the
            existing pre-loop :tx-meta validator. No bookkeeping leak."
    (let [conn (fresh-conn)
          _ (install-schema! conn)
          r (d/transact conn {:tx-data [{:p/k "z" :p/n 7}]
                              :tx-meta {:db.valid/from #inst "2026-01-01"
                                        :db.valid/to #inst "2026-02-01"}})]
      (is (= #{:db-before :db-after :tx-data :tempids :tx-meta}
             (set (keys r)))))))

;; ============================================================================
;; Scenario 9: validation runs on db-after — sees the closure correctly
;; even when interleaved with non-vt writes on the same tx-entity
;; ============================================================================

(deftest multiple-prior-txs-each-validated
  (testing "When one commit touches N different prior tx-entities, each
            is validated independently. Mixing a good close with a bad
            close in the same tx is rejected (first violation wins)."
    (let [conn (fresh-conn)
          _ (install-schema! conn)
          txA (post-with-vt! conn "x" #inst "2026-01-31")
          txB (post-with-vt! conn "y" #inst "2026-01-31")]
      ;; Close txA validly, close txB invalidly — should reject.
      (is (thrown-with-msg? Exception #"Invalid cross-tx valid-time window"
                            (d/transact conn [{:db/id txA
                                               :db.valid/to #inst "2026-02-15"}
                                              {:db/id txB
                                               :db.valid/to #inst "2026-01-15"}]))))))

(deftest multiple-prior-txs-all-good
  (testing "Multiple good closes in one commit all succeed."
    (let [conn (fresh-conn)
          _ (install-schema! conn)
          txA (post-with-vt! conn "x" #inst "2026-01-31")
          txB (post-with-vt! conn "y" #inst "2026-01-31")]
      (is (some? (d/transact conn [{:db/id txA
                                    :db.valid/to #inst "2026-02-15"}
                                   {:db/id txB
                                    :db.valid/to #inst "2026-03-01"}]))))))
