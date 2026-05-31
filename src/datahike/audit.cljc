(ns datahike.audit
  "Audit-chain verification for `:crypto-hash? true` databases.

   `verify-chain` walks the commit DAG from a head cid backwards via
   `:datahike/parents`, recomputes each cid from the stored merkle
   roots, and reports any mismatch. Tampering with a stored commit
   flips its recomputed cid (layer-1 detection).

   With `:deep? true` the head commit is additionally cross-checked:
   each live index in the snapshot is asked to `-recompute-merkle-root`
   from its actual storage, and the result is compared with the root
   that was hashed into the commit-id (layer-2: catches bytes-level
   tampering on a secondary's external storage that doesn't change the
   datahike commit itself)."
  (:require [datahike.db.utils :as dbu]
            [datahike.index.audit :as idx-audit]
            [datahike.writing :as dw]
            [konserve.core :as k]
            [superv.async #?(:clj :refer :cljs :refer-macros) [go-try- <?-]]
            [konserve.utils :refer [#?(:clj async+sync) *default-sync-translation*]
             #?@(:cljs [:refer-macros [async+sync]])]
            ;; cljs: superv.async/go-try- expands to clojure.core.async/go, so the `go`
            ;; MACRO must be required here or it falls back to the JVM macro and fails to
            ;; compile (vary-meta on keyword in go-impl). Mirrors datahike.versioning.
            #?(:cljs [clojure.core.async :refer [<!]]))
  #?(:cljs (:require-macros [clojure.core.async :refer [go]])))

(defn- audit-grade-stored?
  "Mirror of writing/audit-grade? but for already-stored commits we're
   verifying. Looks at the stored merkle-roots map; legacy commits
   that lack it (or have nil primary roots) get advisory."
  [stored]
  (let [config (:config stored)
        roots (:merkle-roots stored)]
    (and (:crypto-hash? config)
         (not= :memory (get-in config [:store :backend]))
         (every? some? (vals (select-keys roots [:eavt-key :aevt-key :avet-key]))))))

(defn- classify [stored cid recomputed]
  (let [config (:config stored)
        roots (:merkle-roots stored)
        sec-roots (:secondary roots)]
    (cond
      (not (:crypto-hash? config))
      {:status :advisory :reason :crypto-hash-disabled}
      (= :memory (get-in config [:store :backend]))
      {:status :advisory :reason :memory-backend}
      (not (audit-grade-stored? stored))
      {:status :advisory :reason :primary-not-audit-grade}
      (and (seq sec-roots) (not (every? some? (vals sec-roots))))
      {:status :advisory :reason :secondary-not-audit-grade}
      (= cid recomputed) {:status :ok}
      :else              {:status :mismatch})))

(defn- step
  "Load `cid` from `store`, recompute, return a verification entry."
  [store cid sync?]
  (async+sync sync? *default-sync-translation*
              (go-try-
               (when-let [stored (<?- (k/get store cid nil {:sync? sync?}))]
                 (let [recomputed (dw/create-commit-id stored stored)
                       stored-cid (get-in stored [:meta :datahike/commit-id])
                       parents    (get-in stored [:meta :datahike/parents] #{})
                       {:keys [status reason]} (classify stored stored-cid recomputed)]
                   (cond-> {:cid stored-cid :recomputed recomputed
                            :parents parents :status status}
                     reason (assoc :reason reason)))))))

;; ============================================================================
;; Deep verification — re-derive merkle roots from live storage for the
;; head snapshot and compare with the cached roots.

(defn- deep-verify-head
  "Walk every live index in `db` and ask each to recompute its merkle
   root. Compare against the stored roots. Returns
   `{:status :ok|:mismatch, :diffs […], :unsupported […]}`.

   Diffs are genuine mismatches (the impl recomputed something and it
   didn't match). Unsupported entries flag indexes whose impl can't
   deep-verify on the current snapshot — they're surfaced but don't
   degrade the overall :ok status.

   The protocol returns a result map (`{:status :ok|:mismatch|:unsupported …}`)
   so this function only dispatches on :status."
  [db stored-head]
  (let [stored-roots (:merkle-roots stored-head)
        check (fn [idx-key live]
                (let [stored (get stored-roots idx-key)
                      result (idx-audit/-recompute-merkle-root live)]
                  (case (:status result)
                    :ok          (when (not= stored (:root result))
                                   {:kind :diff :index idx-key
                                    :stored stored :recomputed (:root result)})
                    :unsupported {:kind :unsupported :index idx-key
                                  :reason (:reason result)}
                    :mismatch    {:kind :diff :index idx-key
                                  :stored stored :errors (:errors result)
                                  :recomputed (:root result)})))
        primary (keep identity
                      [(check :eavt-key (:eavt db))
                       (check :aevt-key (:aevt db))
                       (check :avet-key (:avet db))
                       (when (:keep-history? (:config db))
                         (check :temporal-eavt-key (:temporal-eavt db)))
                       (when (:keep-history? (:config db))
                         (check :temporal-aevt-key (:temporal-aevt db)))
                       (when (:keep-history? (:config db))
                         (check :temporal-avet-key (:temporal-avet db)))])
        sec-stored (:secondary stored-roots)
        secondary (when (seq sec-stored)
                    (keep (fn [[ident live]]
                            (check ident live))
                          (:secondary-indices db)))
        all (concat primary secondary)
        diffs (filterv (comp #{:diff} :kind) all)
        unsupp (filterv (comp #{:unsupported} :kind) all)]
    (cond-> {:status (if (seq diffs) :mismatch :ok)
             :diffs diffs}
      (seq unsupp) (assoc :unsupported unsupp))))

(defn verify-chain
  "Walk the commit DAG anchored at db snapshot `db` and return
   `{:head, :status, :commits, :mismatches, :missing}`.

   `db` must be a db value (from `d/db`). Defaults the head to the
   snapshot's own `:datahike/commit-id`; pass an explicit `head-cid` to
   verify from a different point.

   Options:
     :deep?   — when true, additionally re-derive merkle roots from
                live storage for the head snapshot's indexes and confirm
                they match the cached roots. Catches bytes-level tampering
                that doesn't show up at the commit-pointer level. Adds a
                `:deep` entry to the report with `{:status, :diffs}`.
     :limit   — max commits to walk (default unlimited).
     :sync?   — block (default true).

   `:status` is `:ok | :mismatch | :advisory | :incomplete`. Each entry
   in `:commits` is `{:cid :recomputed :parents :status [:reason]}`."
  ([db] (verify-chain db nil {}))
  ([db head-cid] (verify-chain db head-cid {}))
  ([db head-cid {:keys [limit sync? deep?]
                 :or {limit #?(:clj Long/MAX_VALUE :cljs (.-MAX_SAFE_INTEGER js/Number))
                      sync? true
                      deep? false}}]
   (when-not (dbu/db? db)
     (throw (ex-info "verify-chain: expected a db value (try (d/db conn))"
                     {:type :audit/expected-db :got (type db)})))
   (async+sync sync? *default-sync-translation*
               (go-try-
                (let [store   (:store db)
                      head    (or head-cid (get-in db [:meta :datahike/commit-id]))
                      _       (when-not head
                                (throw (ex-info "verify-chain: no head cid on db"
                                                {:type :audit/no-head})))
                      commits (volatile! [])
                      missing (volatile! [])
                      visited (volatile! #{})]
                  (loop [frontier [head] n 0]
                    (when (and (seq frontier) (< n limit))
                      (let [cid (first frontier) rest-f (rest frontier)]
                        (if (contains? @visited cid)
                          (recur rest-f n)
                          (do (vswap! visited conj cid)
                              (if-let [entry (<?- (step store cid sync?))]
                                (do (vswap! commits conj entry)
                                    (recur (into (vec rest-f)
                                                 (remove @visited (:parents entry)))
                                           (inc n)))
                                (do (vswap! missing conj cid)
                                    (recur rest-f n))))))))
                  (let [es @commits
                        mism (filterv (comp #{:mismatch} :status) es)
                        miss @missing
                        adv  (some (comp #{:advisory} :status) es)
                        deep (when deep?
                               (let [head-stored (<?- (k/get store head nil {:sync? sync?}))]
                                 (when head-stored
                                   (deep-verify-head db head-stored))))
                        base-status (cond (seq mism) :mismatch
                                          (seq miss) :incomplete
                                          adv        :advisory
                                          :else      :ok)
                        deep-mismatch? (= :mismatch (:status deep))]
                    (cond-> {:head head
                             :status (if deep-mismatch? :mismatch base-status)
                             :commits es :mismatches mism :missing miss}
                      deep (assoc :deep deep))))))))

(defn ok? [report] (= :ok (:status report)))
