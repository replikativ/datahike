(ns datahike.test.audit-verify-test
  "End-to-end test for `datahike.audit/verify-chain`.

   Sets up a real :file-backend db with `:crypto-hash? true`, transacts
   a few commits, then exercises:

     - clean walk: verify-chain reports :status :ok
     - tamper detection: rewriting a non-head stored commit (e.g. its
       :max-tx) makes that commit's cid mismatch on recompute
     - advisory mode: when :crypto-hash? is off, walk completes but
       per-commit status is :advisory and overall status is :advisory
     - explicit head-cid: caller can pass a specific cid to start from"
  #?(:cljs (:require [cljs.test :as t :refer-macros [is deftest testing]]
                     [datahike.api :as d]
                     [datahike.audit :as audit]
                     [datahike.index.audit :as idx-audit]
                     [konserve.core :as k])
     :clj  (:require [datahike.index.hitchhiker-tree]
                     [clojure.test :as t :refer [is deftest testing]]
                     [datahike.api :as d]
                     [datahike.audit :as audit]
                     [datahike.index.audit :as idx-audit]
                     [konserve.core :as k])))

#?(:clj
   (defn- temp-store-path []
     (str (System/getProperty "java.io.tmpdir") "/datahike-audit-verify-"
          (java.util.UUID/randomUUID))))

(defn- file-cfg
  "Persistent file-store config — required, since :crypto-hash? is a
   no-op on the memory backend."
  [crypto-hash?]
  {:store              {:backend :file
                        :path    #?(:clj (temp-store-path) :cljs nil)
                        :id      #?(:clj (java.util.UUID/randomUUID)
                                    :cljs (random-uuid))}
   :crypto-hash?       crypto-hash?
   :keep-history?      true
   :schema-flexibility :write
   :index              :datahike.index/persistent-set})

(defn- bootstrap [crypto?]
  (let [cfg (file-cfg crypto?)]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one}])
      (d/transact conn [{:name "alice"}])
      (d/transact conn [{:name "bob"}])
      [conn cfg])))

(defn- cleanup [conn cfg]
  (d/release conn)
  (d/delete-database cfg))

;; ============================================================================

(deftest clean-chain-verifies-ok
  (let [[conn cfg] (bootstrap true)]
    (try
      (let [report (audit/verify-chain (d/db conn))]
        (is (= :ok (:status report)))
        (is (audit/ok? report))
        (is (zero? (count (:mismatches report))))
        (is (zero? (count (:missing report))))
        (is (every? #{:ok} (map :status (:commits report))))
        (is (>= (count (:commits report)) 4)
            "genesis + schema + alice + bob = 4 commits at least"))
      (finally (cleanup conn cfg)))))

(deftest tamper-on-non-head-commit-is-detected
  (let [[conn cfg] (bootstrap true)]
    (try
      (let [db (d/db conn)
            head (get-in db [:meta :datahike/commit-id])
            head-stored (k/get (:store db) head nil {:sync? true})
            ;; Pick the immediate parent and rewrite its :max-tx field
            target-cid (first (-> head-stored :meta :datahike/parents))
            target-stored (k/get (:store db) target-cid nil {:sync? true})
            tampered (assoc target-stored :max-tx 999999)
            _ (k/assoc (:store db) target-cid tampered {:sync? true})
            report (audit/verify-chain db)]
        (is (= :mismatch (:status report)))
        (is (false? (audit/ok? report)))
        (is (= 1 (count (:mismatches report))))
        (is (= target-cid (-> report :mismatches first :cid)))
        (is (not= target-cid (-> report :mismatches first :recomputed))
            "recomputed cid differs from stored cid (the tamper signal)"))
      (finally (cleanup conn cfg)))))

(deftest non-crypto-hash-mode-is-advisory
  (let [[conn cfg] (bootstrap false)]
    (try
      (let [report (audit/verify-chain (d/db conn))]
        (is (= :advisory (:status report))
            "without :crypto-hash?, the chain can be walked but not verified")
        (is (every? #{:advisory} (map :status (:commits report))))
        (is (every? #{:crypto-hash-disabled} (map :reason (:commits report)))))
      (finally (cleanup conn cfg)))))

(deftest verify-from-explicit-head-cid
  (let [[conn cfg] (bootstrap true)]
    (try
      (let [db (d/db conn)
            head (get-in db [:meta :datahike/commit-id])
            ;; Use a known earlier cid (the immediate parent) as the head
            prev-cid (first (-> db :meta :datahike/parents))
            report (audit/verify-chain db prev-cid)]
        (is (= :ok (:status report)))
        (is (= prev-cid (:head report)))
        (is (every? (fn [e] (not= head (:cid e))) (:commits report))
            "walking from prev-cid never visits head"))
      (finally (cleanup conn cfg)))))

(deftest non-db-input-throws
  (testing "verify-chain refuses anything that isn't a db value"
    (let [[conn cfg] (bootstrap true)]
      (try
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #"expected a db value"
                              (audit/verify-chain conn)))
        (finally (cleanup conn cfg))))))

;; ============================================================================
;; Secondary-index integration — only run when stratum is on the classpath.
;; Stratum is the cleanest test target: its dataset commit-id is content-
;; addressed and is bridged into the standardized :merkle-root field of
;; the secondary key-map, so audit picks it up automatically.
;; ============================================================================

#?(:clj
   (def ^:private stratum-available?
     (try (require '[datahike.index.secondary.stratum]) true
          (catch Throwable _ false))))

#?(:clj
   (when stratum-available?
     (defn- bootstrap-stratum []
       (let [cfg (file-cfg true)]
         (d/create-database cfg)
         (let [conn (d/connect cfg)]
           (d/transact conn [{:db/ident :p/name :db/valueType :db.type/string
                              :db/cardinality :db.cardinality/one}
                             {:db/ident :p/age :db/valueType :db.type/long
                              :db/cardinality :db.cardinality/one}])
           (d/transact conn [{:db/ident :idx/strat
                              :db.secondary/type :stratum
                              :db.secondary/attrs [:p/name :p/age]}])
           (d/transact conn [{:p/name "Alice" :p/age 30}
                             {:p/name "Bob" :p/age 25}])
           [conn cfg])))))

#?(:clj
   (when stratum-available?
     (deftest stratum-secondary-contributes-merkle-root
       (testing "stratum's dataset commit-id surfaces under
                 :merkle-roots :secondary; audit walks clean"
         (let [[conn cfg] (bootstrap-stratum)]
           (try
             (let [db (d/db conn)
                   head (get-in db [:meta :datahike/commit-id])
                   stored (k/get (:store db) head nil {:sync? true})
                   strat-root (get-in stored [:merkle-roots :secondary :idx/strat])
                   strat-key (get-in stored [:secondary-index-keys :idx/strat])
                   report (audit/verify-chain db)]
               (is (some? strat-root)
                   "stratum should provide a merkle-root via IAuditable")
               (is (uuid? strat-root))
               (is (= (:dataset-commit-id strat-key) strat-root)
                   "merkle-root equals the dataset commit-id")
               (is (= :ok (:status report))
                   "every commit verifies cleanly with audit-grade secondary"))
             (finally (cleanup conn cfg))))))

     (deftest stratum-deep-verify-ok
       (testing ":deep? true succeeds when primary storage is intact;
                 stratum's secondary deep-verify is flagged unsupported
                 (no verify-from-cold in stratum yet)"
         (let [[conn cfg] (bootstrap-stratum)]
           (try
             (let [r (audit/verify-chain (d/db conn) nil {:deep? true})]
               (is (= :ok (:status r)) "overall :ok — no mismatches")
               (is (= :ok (-> r :deep :status)))
               (is (empty? (-> r :deep :diffs)) "no diffs")
               (is (some #(= :idx/strat (:index %)) (-> r :deep :unsupported))
                   "stratum surfaces unsupported via :audit/recompute-unsupported"))
             (finally (cleanup conn cfg))))))

     (deftest stratum-secondary-root-tampering-detected
       (testing "rewriting the stored :merkle-roots :secondary entry
                 makes the recomputed cid diverge (layer-1 catches it)"
         (let [[conn cfg] (bootstrap-stratum)]
           (try
             (let [db (d/db conn)
                   head (get-in db [:meta :datahike/commit-id])
                   head-stored (k/get (:store db) head nil {:sync? true})
                   tampered (assoc-in head-stored
                                      [:merkle-roots :secondary :idx/strat]
                                      #?(:clj  (java.util.UUID/randomUUID)
                                         :cljs (random-uuid)))
                   _ (k/assoc (:store db) head tampered {:sync? true})
                   ;; verify-chain starts from db's in-memory head cid; the
                   ;; recompute now reads the tampered stored commit and
                   ;; will produce a different cid.
                   report (audit/verify-chain db)]
               (is (= :mismatch (:status report)))
               (is (= 1 (count (:mismatches report))))
               (is (= head (-> report :mismatches first :cid))))
             (finally (cleanup conn cfg))))))

     (deftest stratum-tamper-detected
       (testing "rewriting a non-head commit makes the chain mismatch
                 even when the commit referenced a secondary index"
         (let [[conn cfg] (bootstrap-stratum)]
           (try
             (let [db (d/db conn)
                   head (get-in db [:meta :datahike/commit-id])
                   head-stored (k/get (:store db) head nil {:sync? true})
                   target-cid (first (-> head-stored :meta :datahike/parents))
                   target-stored (k/get (:store db) target-cid nil {:sync? true})
                   tampered (assoc target-stored :max-tx 999999)
                   _ (k/assoc (:store db) target-cid tampered {:sync? true})
                   report (audit/verify-chain db)]
               (is (= :mismatch (:status report)))
               (is (= 1 (count (:mismatches report))))
               (is (= target-cid (-> report :mismatches first :cid))))
             (finally (cleanup conn cfg))))))))

;; ============================================================================
;; Scriptum without crypto-hash → advisory.
;; (Scriptum WITH crypto-hash is blocked by a pre-existing upstream bug:
;;  maybe-create-secondary-index instantiates as soon as :type+:attrs are
;;  present, before :db.secondary/config arrives, so the writer is created
;;  with default config — :crypto-hash? never reaches the bridge.)
;; ============================================================================

;; ============================================================================
;; Deep-verify mismatch path via a fake IAuditable index.
;; This exercises `audit/verify-chain :deep? true`'s handling of a
;; library-side `:audit/merkle-mismatch` signal without depending on a
;; specific backend's working crypto-hash path through datahike.
;; ============================================================================

(deftest deep-verify-mismatch-from-fake-index
  (testing ":deep? reports :mismatch when an IAuditable impl signals
            :audit/merkle-mismatch from -recompute-merkle-root"
    (let [[conn cfg] (bootstrap true)]
      (try
        (let [db (d/db conn)
              fake-stored-root #?(:clj (java.util.UUID/randomUUID) :cljs (random-uuid))
              ;; Splice a fake secondary index into the live db + stored
              ;; commit so the auditor walks it. The fake's
              ;; -recompute-merkle-root throws :audit/merkle-mismatch.
              fake-index (reify
                           idx-audit/IAuditable
                           (-merkle-root [_] fake-stored-root)
                           (-recompute-merkle-root [_]
                             (throw (ex-info "fake: bytes tampered"
                                             {:type :audit/merkle-mismatch
                                              :details :fake}))))
              db-with-fake (assoc db :secondary-indices {:idx/fake fake-index})
              head (get-in db [:meta :datahike/commit-id])
              head-stored (k/get (:store db) head nil {:sync? true})
              stored-with-fake (assoc-in head-stored [:merkle-roots :secondary :idx/fake]
                                         fake-stored-root)
              _ (k/assoc (:store db) head stored-with-fake {:sync? true})
              r (audit/verify-chain db-with-fake nil {:deep? true})]
          (is (= :mismatch (:status r)))
          (is (= :mismatch (-> r :deep :status)))
          (is (some #(= :idx/fake (:index %)) (-> r :deep :diffs))
              "the fake index is flagged in :diffs"))
        (finally (cleanup conn cfg))))))

#?(:clj
   (def ^:private scriptum-available?
     (try (require '[datahike.index.secondary.scriptum]) true
          (catch Throwable _ false))))

#?(:clj
   (when scriptum-available?
     (deftest scriptum-without-crypto-hash-is-advisory
       (testing "scriptum that didn't opt into crypto-hash leaves :merkle-root
                 absent → audit downgrades affected commits to advisory"
         (let [cfg (file-cfg true)]
           (d/create-database cfg)
           (let [conn (d/connect cfg)]
             (try
               (d/transact conn [{:db/ident :p/name :db/valueType :db.type/string
                                  :db/cardinality :db.cardinality/one}])
               (d/transact conn [{:db/ident :idx/sc :db.secondary/type :scriptum
                                  :db.secondary/attrs [:p/name]}])
               (d/transact conn [{:p/name "Alice"}])
               (let [db (d/db conn)
                     stored-head (k/get (:store db)
                                        (get-in db [:meta :datahike/commit-id])
                                        nil {:sync? true})
                     sc-root (get-in stored-head [:merkle-roots :secondary :idx/sc])
                     report (audit/verify-chain db)]
                 (is (nil? sc-root)
                     "scriptum without crypto-hash should fail to produce a merkle-root")
                 (is (= :advisory (:status report)))
                 (is (some #{:secondary-not-audit-grade}
                           (map :reason (:commits report)))
                     "at least one commit should be flagged with the secondary reason"))
               (finally (cleanup conn cfg)))))))))

#?(:clj
   (when scriptum-available?
     (deftest scriptum-with-crypto-hash-is-audit-grade
       (testing "scriptum opted into :crypto-hash? exposes :merkle-root
                 via -sec-flush, and audit walks :ok end-to-end"
         (let [cfg (file-cfg true)
               scriptum-path (str (System/getProperty "java.io.tmpdir")
                                  "/scriptum-audit-"
                                  (java.util.UUID/randomUUID))]
           (d/create-database cfg)
           (let [conn (d/connect cfg)]
             (try
               (d/transact conn [{:db/ident :p/name :db/valueType :db.type/string
                                  :db/cardinality :db.cardinality/one}])
               (d/transact conn [{:db/ident :idx/sc :db.secondary/type :scriptum
                                  :db.secondary/attrs [:p/name]
                                  :db.secondary/config {:path scriptum-path
                                                        :crypto-hash? true}}])
               (d/transact conn [{:p/name "Alice"}])
               (let [db (d/db conn)
                     stored-head (k/get (:store db)
                                        (get-in db [:meta :datahike/commit-id])
                                        nil {:sync? true})
                     sc-root (get-in stored-head [:merkle-roots :secondary :idx/sc])
                     report (audit/verify-chain db)]
                 (is (some? sc-root)
                     "scriptum's :content-hash should reach merkle-roots")
                 (is (uuid? sc-root))
                 (is (= :ok (:status report))
                     "every commit verifies cleanly with audit-grade scriptum"))
               (finally (cleanup conn cfg)))))))))
