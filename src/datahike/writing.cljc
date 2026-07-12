(ns datahike.writing
  "Manage all state changes and access to state of durable store."
  (:require [datahike.connections :refer [delete-connection! *connections*]]
            [datahike.db :as db]
            [datahike.db.transaction :as dbtx]
            [datahike.db.utils :as dbu]
            [datahike.db.interface :as dbi]
            [datahike.index :as di]
            [datahike.index.audit :as audit]
            [datahike.index.secondary :as sec]
            [datahike.store :as ds]
            [datahike.tools :as dt]
            [datahike.core :as core]
            [datahike.query :as dq]
            [datahike.config :as dc]
            [datahike.schema-cache :as sc]
            [datahike.online-gc :as online-gc]
            [konserve.core :as k]
            [konserve.store :as ks]
            [replikativ.logging :as log]
            [hasch.core :refer [uuid squuid]]
            [hasch.platform]
            [clojure.core.async :as async :refer [go put!]]
            [superv.async #?(:clj :refer :cljs :refer-macros) [go-try- <?-]]
            [konserve.utils :refer [#?(:clj async+sync) multi-key-capable? *default-sync-translation*]
             #?@(:cljs [:refer-macros [async+sync]])]))

;; mapping to storage

(defn stored-db? [obj]
  ;; TODO use proper schema to match?
  (let [keys-to-check [:eavt-key :aevt-key :avet-key :config
                       :max-tx :max-eid :op-count :hash :meta]]
    (= (count (select-keys obj keys-to-check))
       (count keys-to-check))))

(defn get-and-clear-pending-kvs!
  "Retrieves and clears pending key-value pairs from the store's pending-writes atom.
  Assumes :pending-writes in store's storage holds an atom of a collection of [key value] pairs."
  [store]
  (let [pending-writes-atom (-> store :storage :pending-writes) ; Assumes :storage key holds the CachedStorage
        kvs-to-write (atom [])]
    (when pending-writes-atom
      ;; Atomically get current KVs and reset the pending-writes atom.
      (swap! pending-writes-atom (fn [old-kvs] (reset! kvs-to-write old-kvs) [])))
    @kvs-to-write))

(defn db->stored
  "Maps memory db to storage layout. Index flushes will add [k v] pairs to pending-writes."
  [db flush?]
  (when-not (dbu/db? db)
    (log/raise "Argument is not a database."
               {:type     :argument-is-not-a-db
                :argument db}))
  (let [{:keys [eavt aevt avet temporal-eavt temporal-aevt temporal-avet
                schema rschema system-entities ident-ref-map ref-ident-map config
                max-tx max-eid op-count hash meta store]} db
        schema-meta {:schema schema
                     :rschema rschema
                     :system-entities system-entities
                     :ident-ref-map ident-ref-map
                     :ref-ident-map ref-ident-map}
        schema-meta-key (uuid schema-meta)
        backend                                           (di/konserve-backend (:index config) store)
        not-in-memory?                                    (not= :memory (-> config :store :backend))
        flush! (and flush? not-in-memory?)
        ;; Prepare schema meta KV pair for writing, but don't write it here.
        schema-meta-kv-to-write (when-not (sc/write-cache-has? (:store config) schema-meta-key)
                                  (sc/add-to-write-cache (:store config) schema-meta-key)
                                  [schema-meta-key schema-meta])]
    (when-not (sc/cache-has? schema-meta-key)
      (sc/cache-miss schema-meta-key schema-meta))
    (let [;; Flush primary indices, capturing the post-flush instances so
          ;; we can both serialize their storage keys and ask each for a
          ;; merkle-root via the IAuditable protocol.
          eavt'          (cond-> eavt flush! (di/-flush backend))
          aevt'          (cond-> aevt flush! (di/-flush backend))
          avet'          (cond-> avet flush! (di/-flush backend))
          temporal-eavt' (when (:keep-history? config)
                           (cond-> temporal-eavt flush! (di/-flush backend)))
          temporal-aevt' (when (:keep-history? config)
                           (cond-> temporal-aevt flush! (di/-flush backend)))
          temporal-avet' (when (:keep-history? config)
                           (cond-> temporal-avet flush! (di/-flush backend)))
          ;; Secondary indices manage their own storage (Lucene files,
          ;; konserve, mmap) so they must always be flushed regardless of
          ;; the primary store backend.
          secondary-index-keys
          #?(:clj
             (when (and flush? (seq (:secondary-indices db)))
               (reduce-kv
                (fn [acc idx-ident idx]
                  (if (satisfies? sec/IVersionedSecondaryIndex idx)
                    (assoc acc idx-ident (sec/-sec-flush idx store (:branch config)))
                    acc))
                {} (:secondary-indices db)))
             :cljs nil)
          ;; Audit roots: per-index content-addressed UUIDs that feed
          ;; into the commit-id via merkle-leaves.
          ;;
          ;; Primary indexes implement IAuditable: their flushed instance
          ;; carries the merkle root (e.g. PSS `_address` is post-flush).
          ;;
          ;; Secondary indexes can produce their merkle root in two
          ;; ways: (a) extend IAuditable when their live instance has
          ;; post-flush state visible to the bridge — scriptum, whose
          ;; underlying Java writer is mutable so `(.getLastContentHash
          ;; bw)` reflects the latest commit on the same handle; (b)
          ;; surface `:merkle-root` in their -sec-flush return map when
          ;; sync produces a new immutable value the bridge field
          ;; doesn't capture — stratum and proximum, whose record-typed
          ;; live values stay pinned to the pre-sync state. The reader
          ;; below tries (a) first, then (b).
          safe-root      (fn [x]
                           (when x
                             (try (audit/-merkle-root x)
                                  (catch #?(:clj Throwable :cljs js/Error) _ nil))))
          sec-roots      (when (seq (:secondary-indices db))
                           (reduce-kv
                            (fn [acc idx-ident idx]
                              (assoc acc idx-ident
                                     (or (safe-root idx)
                                         (:merkle-root (get secondary-index-keys idx-ident)))))
                            {} (:secondary-indices db)))
          merkle-roots
          (cond-> {:eavt-key (safe-root eavt')
                   :aevt-key (safe-root aevt')
                   :avet-key (safe-root avet')}
            (:keep-history? config)
            (assoc :temporal-eavt-key (safe-root temporal-eavt')
                   :temporal-aevt-key (safe-root temporal-aevt')
                   :temporal-avet-key (safe-root temporal-avet'))
            sec-roots
            (assoc :secondary sec-roots))
          ;; Detach index roots before they enter the store: a stored value
          ;; must never carry a live storage handle. Serializing backends
          ;; strip it anyway (the canonical write handler); this makes the
          ;; same invariant hold for identity-preserving stores (tiered
          ;; memory frontend), which would otherwise cache the live root
          ;; with this connection's storage inside. stored->db rebinds.
          detach (fn [idx] (di/with-storage (:index config) idx nil))
          ;; Root fusion (EXPERIMENTAL, opt-in): inline each flushed index's
          ;; root NODE into the db-record; `commit!` then skips writing those
          ;; roots as separate objects (fused-root-addresses). Nodes carry no
          ;; storage handle, so no detach is needed. Works under crypto-hash?:
          ;; the root's address is still its content hash and the audit walk
          ;; verifies the inlined root (walk-pss-node!) before recursing into
          ;; its separately-stored children. PSS-only (the protocol methods
          ;; are implemented for the persistent-set index).
          fuse? (and flush!
                     (:fuse-index-roots? config)
                     (= (:index config) :datahike.index/persistent-set))
          fused-roots (when fuse?
                        (cond-> {:eavt-root (di/-root-node eavt')
                                 :aevt-root (di/-root-node aevt')
                                 :avet-root (di/-root-node avet')}
                          (:keep-history? config)
                          (assoc :temporal-eavt-root (di/-root-node temporal-eavt')
                                 :temporal-aevt-root (di/-root-node temporal-aevt')
                                 :temporal-avet-root (di/-root-node temporal-avet'))))]
      [schema-meta-kv-to-write
       (merge
        {:schema-meta-key  schema-meta-key
         :config          config
         :meta            meta
         :hash            hash
         :max-tx          max-tx
         :max-eid         max-eid
         :op-count        op-count
         :merkle-roots    merkle-roots
         :eavt-key        (detach eavt')
         :aevt-key        (detach aevt')
         :avet-key        (detach avet')}
        (when (:keep-history? config)
          {:temporal-eavt-key (detach temporal-eavt')
           :temporal-aevt-key (detach temporal-aevt')
           :temporal-avet-key (detach temporal-avet')})
        (when secondary-index-keys
          {:secondary-index-keys secondary-index-keys})
        fused-roots)])))

(defn- restore-secondary-indices
  "Restore secondary index instances from stored key-maps.
   For versioned indices (IVersionedSecondaryIndex), restores from durable storage.
   For non-versioned or missing keys, creates empty instances that need backfill."
  [schema ident-ref-map secondary-index-keys store]
  #?(:clj
     (reduce-kv
      (fn [acc ident entry]
        (if (and (map? entry) (:db.secondary/type entry))
          (let [idx-type (:db.secondary/type entry)
                idx-attrs (set (:db.secondary/attrs entry))
                key-map (get secondary-index-keys ident)
                idx-config (cond-> (merge (:db.secondary/config entry)
                                          {:attrs idx-attrs})
                             (seq ident-ref-map)
                             (assoc :ident-ref-map ident-ref-map)
                             ;; When a key-map carries a branch, route the
                             ;; skeleton into that branch too — otherwise
                             ;; the factory defaults to "main" and a non-
                             ;; main connection re-opens the main writer,
                             ;; contending for its per-branch lock.
                             (:branch key-map)
                             (assoc :branch (:branch key-map)))]
            (try
              (let [skeleton (sec/create-index idx-type idx-config nil)]
                (if (and key-map (satisfies? sec/IVersionedSecondaryIndex skeleton))
                  ;; Restore from durable storage. The skeleton existed
                  ;; only to satisfy the protocol check; close its native
                  ;; resources (e.g. Lucene's per-branch write lock)
                  ;; before `-sec-restore` opens its own writer at the
                  ;; same path/branch — otherwise the two contend.
                  (do (when (instance? java.io.Closeable skeleton)
                        (try (.close ^java.io.Closeable skeleton)
                             (catch Exception _)))
                      (assoc acc ident (sec/-sec-restore skeleton store key-map)))
                  ;; No stored keys — empty index, needs backfill
                  (assoc acc ident skeleton)))
              (catch Exception e
                (log/warn :datahike/secondary-index-restore-failed {:ident ident :error (.getMessage e)})
                acc)))
          acc))
      {} schema)
     :cljs {}))

(defn stored->db
  "Constructs in-memory db instance from stored map value."
  [stored-db store]
  (let [{:keys [eavt-key aevt-key avet-key
                temporal-eavt-key temporal-aevt-key temporal-avet-key
                eavt-root aevt-root avet-root
                temporal-eavt-root temporal-aevt-root temporal-avet-root
                secondary-index-keys
                schema rschema system-entities ref-ident-map ident-ref-map
                config max-tx max-eid op-count hash meta schema-meta-key]
         :or   {op-count 0}} stored-db
        schema-meta (or (sc/cache-lookup schema-meta-key)
                        ;; not in store in case we load an old db where the schema meta data was inline
                        (when-let [schema-meta (k/get store schema-meta-key nil {:sync? true})]
                          (sc/cache-miss schema-meta-key schema-meta)
                          schema-meta))
        effective-schema (or (:schema schema-meta) schema)
        effective-ident-ref-map (or (:ident-ref-map schema-meta) ident-ref-map)
        sec-indices (restore-secondary-indices effective-schema effective-ident-ref-map
                                               secondary-index-keys store)
        empty       (db/empty-db nil config store)
        ;; Bind each index to THIS connection's storage (as a copy). Stored
        ;; values are storage-detached (db->stored) and deserializing
        ;; backends bind on read anyway, but identity-preserving stores
        ;; (tiered memory frontend) return the stored object as-is — so
        ;; binding must happen here, at materialization, for every backend.
        ;;
        ;; Root fusion: seed an inlined root into the COPY, never into the
        ;; stored record's index — the record may be shared through the
        ;; store's cache by every reader of this key, and shared objects are
        ;; read-only (the cross-version projection lesson, persistent-sorted-set
        ;; #19). The with-storage copy is owned and unpublished, so the
        ;; seed mutation is single-threaded by construction; root() then
        ;; returns the fused root with no storage round-trip (deeper
        ;; children stay lazy). Presence-based, so fused and legacy records
        ;; both restore — no reader config needed.
        attach (fn [idx root]
                 (cond-> (di/with-storage (:index config) idx (:storage store))
                   root (di/-seed-root! root)))]
    (merge
     (assoc empty
            :max-tx max-tx
            :max-eid max-eid
            :config config
            :meta meta
            :schema schema
            :hash hash
            :op-count op-count
            :eavt (attach eavt-key eavt-root)
            :aevt (attach aevt-key aevt-root)
            :avet (attach avet-key avet-root)
            :temporal-eavt (attach temporal-eavt-key temporal-eavt-root)
            :temporal-aevt (attach temporal-aevt-key temporal-aevt-root)
            :temporal-avet (attach temporal-avet-key temporal-avet-root)
            :rschema rschema
            :system-entities system-entities
            :ident-ref-map ident-ref-map
            :ref-ident-map ref-ident-map
            :store store)
     (when (seq sec-indices)
       {:secondary-indices sec-indices})
     schema-meta)))

(defn branch-heads-as-commits
  "Resolve keyword parents (branch names) to their head commit-ids.

   `known-heads` is an optional {branch-keyword commit-id} map of heads the
   caller already holds in memory: under datahike's single-writer invariant
   the writer's current db carries its own branch's head cid in
   [:meta :datahike/commit-id], so re-reading the branch record from storage
   (3 sequential requests on S3-class backends) is redundant for it. A nil or
   missing entry falls back to the storage read — first load, foreign
   branches (merge parents), or writers without an in-memory head. This is a
   read elision, not a fence: head-flip semantics on concurrent writer misuse
   are unchanged (last-writer-wins, exactly as with the read)."
  ([store parents] (branch-heads-as-commits store parents {}))
  ([store parents known-heads]
   (set (doall (for [p parents]
                 (do
                   (when (nil? p)
                     (log/raise "Parent cannot be nil." {:type :parent-cannot-be-nil
                                                         :parent p}))
                   (if-not (keyword? p) p
                           (or (get known-heads p)
                               (let [{{:keys [datahike/commit-id]} :meta :as old-db}
                                     (k/get store p nil {:sync? true})]
                                 (when-not old-db
                                   (log/raise "Parent does not exist in store."
                                              {:type   :parent-does-not-exist-in-store
                                               :parent p}))
                                 commit-id)))))))))

(defn- audit-grade?
  "Audit-grade cids require :crypto-hash? on a persistent backend,
   plus a `:merkle-roots` map computed during `db->stored` whose
   primary entries (eavt-key/aevt-key/avet-key) are non-nil — i.e.
   the primary index impl extends `IAuditable`."
  [config stored-db]
  (and (:crypto-hash? config)
       (not= :memory (get-in config [:store :backend]))
       (some? stored-db)
       (every? some?
               (vals (select-keys (:merkle-roots stored-db)
                                  [:eavt-key :aevt-key :avet-key])))))

(defn create-commit-id
  "Compute the commit-id for `db`.

   In audit-grade mode, returns a content-addressed UUID-5 over the
   stored `:merkle-roots` map + schema-meta-key + max-tx + max-eid +
   meta. Otherwise falls back to `[hash max-tx max-eid meta]`, wrapped
   in `squuid` when `:crypto-hash?` is off."
  ([db] (create-commit-id db nil))
  ([db stored-db]
   (let [{:keys [hash max-tx max-eid meta config]} db
         content (if (audit-grade? config stored-db)
                   [(:merkle-roots stored-db)
                    (:schema-meta-key stored-db)
                    max-tx max-eid
                    (dissoc meta :datahike/commit-id)]
                   [hash max-tx max-eid meta])
         content-uuid (uuid content)]
     (if (:crypto-hash? config)
       content-uuid
       (squuid content-uuid)))))

(defn- fused-root-addresses
  "When root fusion is enabled, the addresses of the index root nodes that
  `db->stored` inlined into the record. These must be excluded from the
  pending-writes drain so they are not also written as separate objects.
  Each index's root address == its post-flush `_address` — exactly the value
  `db->stored` captured in `:merkle-roots`, and exactly its pending-writes
  key. Exact-by-address: deeper dirty nodes stay.

  NOT under :crypto-hash?: content-derived addresses dedup across trees, so a
  root's address can also be referenced as an interior CHILD of another
  index's tree (e.g. a temporal leaf identical to the current index's whole
  single-leaf root); excluding the object would dangle that reference. squuid
  addresses are minted uniquely per stored node, so exclusion is exact there.
  Under crypto the roots stay separate objects (fusion still saves the
  per-index cold-open GET via the inlined copy, just not the PUT)."
  [config db-to-store]
  (when (and (:fuse-index-roots? config)
             (not (:crypto-hash? config))
             (= (:index config) :datahike.index/persistent-set))
    (->> (select-keys (:merkle-roots db-to-store)
                      [:eavt-key :aevt-key :avet-key
                       :temporal-eavt-key :temporal-aevt-key :temporal-avet-key])
         vals
         (remove nil?)
         set)))

(defn write-pending-kvs!
  "Writes a collection of key-value pairs to the store.
  Handles synchronous and asynchronous writes.
  Assumes it's called within a go-try- block if sync? is false."
  [store kvs sync?]
  ;; pending-kvs are content-addressed index nodes (write-once) → mark immutable so a sync
  ;; peer can skip re-storing/re-publishing a node it already holds (anti-entropy/echo).
  (if sync?
    (doseq [[k v] kvs]
      (k/assoc store k v {:immutable? true} {:sync? true}))
    (let [pending-ops (mapv (fn [[k v]] (k/assoc store k v {:immutable? true} {:sync? false})) kvs)]
      (go-try- (doseq [op pending-ops] (<?- op))))))

(defn commit!
  ([db parents]
   (commit! db parents true))
  ([db parents sync?]
   (commit! db parents sync? nil))
  ([db parents sync? known-head-cid]
   (async+sync sync? *default-sync-translation*
               (go-try-
                ;; Contain fatal ERRORS (AssertionError, OOM, ...): go-try- catches
                ;; Exception only, so an Error would escape the go state machine,
                ;; kill the dispatch thread, and leave the returned channel silent —
                ;; the writer's commit loop parks on it FOREVER and every queued
                ;; transact hangs with no diagnostic. Convert to ex-info at the go
                ;; boundary so the error flows through the channel to the writer's
                ;; Throwable handler: callbacks get the error and the writer shuts
                ;; down loudly. Commit ordering is unaffected (the HEAD flip never
                ;; happened when we land here).
                (try
                  (let [{:keys [store config]} db
                        ;; Head-cid cache: for an ORDINARY commit (no explicit
                        ;; parents) the writer's own head cid is already in
                        ;; memory — stamped by the previous commit!, or by
                        ;; stored->db at connect — so skip the per-commit
                        ;; branch-head storage read (3 sequential requests on
                        ;; S3 backends). Explicit-parents commits (merge!,
                        ;; branch machinery) keep the read: their db may
                        ;; descend from ANOTHER branch's lineage, so its meta
                        ;; cid is not necessarily this branch's head.
                        ;; known-head-cid is threaded by the WRITER's commit
                        ;; loop (its previous commit's cid) — nil on the first
                        ;; commit after connect, which falls back to the read.
                        ;; The db's own meta cid is NOT usable here: the
                        ;; transaction loop chains applied dbs whose meta
                        ;; predates recent commits (the old storage read was,
                        ;; in effect, the cross-loop synchronization point).
                        known-heads   (if (and (nil? parents) known-head-cid)
                                        {(get config :branch) known-head-cid}
                                        {})
                        parents       (or parents #{(get config :branch)})
                        parents       (branch-heads-as-commits store parents known-heads)
                      ;; Stamp parents BEFORE flushing so they're in the
                      ;; stored form the cid will be derived from.
                        db            (assoc-in db [:meta :datahike/parents] parents)
                      ;; Flush first → cid sees post-flush storage
                      ;; addresses (true merkle leaves under crypto-hash?).
                        [schema-meta-kv-to-write db-to-store-pre]
                        (db->stored db true)
                        cid           (create-commit-id db db-to-store-pre)
                        db            (assoc-in db [:meta :datahike/commit-id] cid)
                        db-to-store   (assoc-in db-to-store-pre
                                                [:meta :datahike/commit-id] cid)
                      ;; Root fusion: roots are inlined in db-to-store, so drop
                      ;; them from the separate-object writes.
                        fused-addrs   (fused-root-addresses config db-to-store)
                        pending-kvs   (cond->> (get-and-clear-pending-kvs! store)
                                        (seq fused-addrs)
                                        (remove (fn [[k _]] (contains? fused-addrs k))))
                        ;; Commit graph (opt-out): the immutable cid record is
                        ;; the provenance chain (audit, ancestry, ?commit=
                        ;; refs). With :commit-graph? false only the branch
                        ;; head is written — the cid is still computed and
                        ;; stamped in :meta, so identity, sync dedup and the
                        ;; writer's head-cid threading are unaffected.
                        commit-graph? (get config :commit-graph? true)]

                    (if (multi-key-capable? store)
                      (let [[meta-key meta-val] schema-meta-kv-to-write
                            branch-key (:branch config)
                            ;; ORDERED batch. konserve applies a [k v] seq in sequence order,
                            ;; so state the SAME causal discipline the non-atomic path below
                            ;; spells out ("make sure all pointed to values are written before
                            ;; the commit log and branch"): every value the new head references
                            ;; first, the MUTABLE branch head LAST.
                            ;;
                            ;; Handing konserve a MAP would throw that order away — and the
                            ;; order is real: pending-kvs comes out of the index flush with
                            ;; children before the parents that address them. A torn batch then
                            ;; leaves unreachable orphans (collectable), never a head pointing
                            ;; at values that were never written. That is what makes the batch
                            ;; safe WITHOUT atomic multi-key writes, which S3 and filesystems
                            ;; cannot give us anyway.
                            ;;
                            ;; It also means a sync subscriber relaying this batch applies it
                            ;; in the order we committed it, instead of guessing an order back
                            ;; from the shape of the keys.
                            writes (cond-> (vec pending-kvs)
                                     schema-meta-kv-to-write (conj [meta-key meta-val])
                                     commit-graph?           (conj [cid db-to-store])
                                     true                    (conj [branch-key db-to-store]))
                            ;; nodes + schema-meta (uuid) + commit (cid) are content-addressed →
                            ;; immutable; the branch-head pointer stays mutable (unmarked).
                            metas  (into {}
                                         (comp (map first)
                                               (remove #(= % branch-key))
                                               (map (fn [k] [k {:immutable? true}])))
                                         writes)]
                        (<?- (k/multi-assoc store writes metas {:sync? sync?})))
                    ;; Then write schema-meta, commit-log, branch
                      (let [[meta-key meta-val] schema-meta-kv-to-write
                            schema-meta-written (when schema-meta-kv-to-write
                                                ;; schema-meta-key = (uuid schema-meta) → content-addressed → immutable
                                                  (k/assoc store meta-key meta-val {:immutable? true} {:sync? sync?}))

                          ;; Make sure all pointed to values are written before the commit log and branch
                            _ (when schema-meta-kv-to-write (<?- schema-meta-written))
                            _ (<?- (write-pending-kvs! store pending-kvs sync?))

                          ;; the commit is content-addressed by cid → immutable; the branch head is mutable
                            commit-log-written (when commit-graph?
                                                 (k/assoc store cid db-to-store {:immutable? true} {:sync? sync?}))
                            branch-written     (k/assoc store (:branch config) db-to-store {:sync? sync?})]
                        (when-not sync?
                          (when commit-log-written (<?- commit-log-written))
                          (<?- branch-written))))

                  ;; Online GC: delete freed addresses after writes are committed
                    (when (get-in config [:online-gc :enabled?])
                      (<?- (online-gc/online-gc! store (assoc (:online-gc config) :sync? false))))

                    db)
                  (catch #?(:clj Error :cljs :default) e
                    #?(:clj  (throw (ex-info "Fatal error during commit."
                                             {:type :fatal-commit-error}
                                             e))
                       :cljs (throw e))))))))

(defn complete-db-update [old tx-report]
  (let [{:keys [writer]} old
        {:keys [db-after tx-data]
         {:keys [db/txInstant]} :tx-meta} tx-report
        new-meta  (assoc (:meta db-after) :datahike/updated-at txInstant)
        db        (assoc db-after :meta new-meta :writer writer)
        ;; Propagate query result cache from old DB to new DB
        ;; Extract modified attributes from tx-data for selective invalidation
        rim (:ref-ident-map db)
        modified-attrs (into #{}
                             (comp (map :a)
                                   (filter some?)
                                   (map (fn [a] (if (and rim (number? a)) (get rim a a) a))))
                             tx-data)
        _ (dq/propagate-query-cache old db modified-attrs)
        tx-report (assoc tx-report :db-after db)]
    tx-report))

(defprotocol PDatabaseManager
  (-create-database [config opts])
  (-delete-database [config])
  (-database-exists? [config]))

(defn -database-exists?* [config]
  (let [p (dt/throwable-promise)]
    (go
      (put! p (try
                (let [config (dc/load-config config)
                      store-config (:store config)
                      ;; First check if store exists (avoids exception when store not in registry)
                      store-exists? (<?- (ks/store-exists? store-config {:sync? false}))]
                  (if store-exists?
                    ;; Store exists, now check if it contains a database
                    (let [raw-store (<?- (ks/connect-store store-config {:sync? false}))
                          store (ds/add-cache-and-handlers raw-store config)
                          stored-db (<?- (k/get store :db nil {:sync? false}))]
                      ;; Release store and await completion
                      (<?- (ks/release-store store-config store {:sync? false}))
                      (some? stored-db))
                    ;; Store doesn't exist, so database doesn't exist
                    false))
                (catch #?(:clj Exception :cljs js/Error) e
                  e))))
    p))

(defn -create-database* [config deprecated-config]
  (go-try-
   (let [opts {:sync? false}
         {:keys [keep-history?] :as config} (dc/load-config config deprecated-config)
         store-config (:store config)
         store (ds/add-cache-and-handlers (<?- (ks/create-store store-config opts)) config)
         stored-db (<?- (k/get store :db nil opts))
         _ (when stored-db
             (log/raise "Database already exists."
                        {:type :db-already-exists :config store-config}))
         {:keys [eavt aevt avet temporal-eavt temporal-aevt temporal-avet
                 schema rschema system-entities ref-ident-map ident-ref-map
                 config max-tx max-eid op-count hash meta] :as db}
         (db/empty-db nil config store)
         backend (di/konserve-backend (:index config) store)
         schema-meta {:schema schema
                      :rschema rschema
                      :system-entities system-entities
                      :ident-ref-map ident-ref-map
                      :ref-ident-map ref-ident-map}
         schema-meta-key (uuid schema-meta)
         ;; Flush first → cid sees post-flush storage addresses.
         eavt'          (di/-flush eavt backend)
         aevt'          (di/-flush aevt backend)
         avet'          (di/-flush avet backend)
         temporal-eavt' (when keep-history? (di/-flush temporal-eavt backend))
         temporal-aevt' (when keep-history? (di/-flush temporal-aevt backend))
         temporal-avet' (when keep-history? (di/-flush temporal-avet backend))
         safe-root      (fn [x]
                          (when x
                            (try (audit/-merkle-root x)
                                 (catch #?(:clj Throwable :cljs :default) _ nil))))
         merkle-roots   (cond-> {:eavt-key (safe-root eavt')
                                 :aevt-key (safe-root aevt')
                                 :avet-key (safe-root avet')}
                          keep-history?
                          (assoc :temporal-eavt-key (safe-root temporal-eavt')
                                 :temporal-aevt-key (safe-root temporal-aevt')
                                 :temporal-avet-key (safe-root temporal-avet')))
         ;; Detach roots before they enter the store (see db->stored).
         detach (fn [idx] (di/with-storage (:index config) idx nil))
         pre-cid-stored
         (merge {:max-tx          max-tx
                 :max-eid         max-eid
                 :op-count        op-count
                 :hash            hash
                 :merkle-roots    merkle-roots
                 :schema-meta-key schema-meta-key
                 :config          (update config :initial-tx (comp not empty?))
                 :meta            meta
                 :eavt-key        (detach eavt')
                 :aevt-key        (detach aevt')
                 :avet-key        (detach avet')}
                (when keep-history?
                  {:temporal-eavt-key (detach temporal-eavt')
                   :temporal-aevt-key (detach temporal-aevt')
                   :temporal-avet-key (detach temporal-avet')}))
         cid (create-commit-id db pre-cid-stored)
         meta (assoc meta :datahike/commit-id cid)
         db-to-store (assoc pre-cid-stored :meta meta)]
     ;;we just created the first data base in this store, so the write cache is empty
     ;; schema-meta-key = (uuid schema-meta) → content-addressed, immutable
     (<?- (k/assoc store schema-meta-key schema-meta {:immutable? true} opts))
     (sc/add-to-write-cache (:store config) schema-meta-key)
     (when-not (sc/cache-has? schema-meta-key)
       (sc/cache-miss schema-meta-key schema-meta))

     ;; Process pending KVs from index flushes synchronously
     (let [pending-kvs (get-and-clear-pending-kvs! store)]
       (<?- (write-pending-kvs! store pending-kvs false)))

     (<?- (k/assoc store :branches #{:db} opts))           ; mutable: branch set
     (<?- (k/assoc store cid db-to-store {:immutable? true} opts)) ; content-addressed commit
     (<?- (k/assoc store :db db-to-store opts))             ; mutable: branch head
     (ks/release-store store-config store)
     config)))

(defn -delete-database* [config]
  (go-try-
   (let [config (dc/load-config config {})
         config-store-id (ds/store-identity (:store config))
         active-conns (filter (fn [[store-id _branch]]
                                (= store-id config-store-id))
                              (keys @*connections*))]
     (sc/clear-write-cache (:store config))
     (doseq [conn active-conns]
       (log/warn :datahike/delete-unreleased-connections {:connection conn})
       (delete-connection! conn))
     (ks/delete-store (:store config)))))

(extend-protocol PDatabaseManager
  #?(:clj String :cljs string)
  (-create-database #?(:clj [uri & opts] :cljs [uri opts])
    (-create-database (dc/uri->config uri) opts))

  (-delete-database [uri]
    (-delete-database (dc/uri->config uri)))

  (-database-exists? [uri]
    (-database-exists? (dc/uri->config uri)))

  #?(:clj clojure.lang.IPersistentMap :cljs PersistentArrayMap)
  (-database-exists? [config]
    (-database-exists?* config))
  (-create-database [config opts]
    (-create-database* config opts))
  (-delete-database [config]
    (-delete-database* config))

  #?(:cljs PersistentHashMap)
  #?(:cljs
     (-database-exists? [config]
                        (-database-exists?* config)))
  #?(:cljs (-create-database [config opts] (-create-database* config opts)))
  #?(:cljs (-delete-database [config] (-delete-database* config))))

;; public API

(defn create-database
  ([]
   (-create-database {} nil))
  ([config & opts]
   (-create-database config opts)))

(defn delete-database
  ([]
   (-delete-database {}))
  ;;deprecated
  ([config]
   ;; TODO log deprecation notice with #54
   (-delete-database config)))

(defn database-exists?
  ([]
   (-database-exists? {}))
  ([config]
   ;; TODO log deprecation notice with #54
   (-database-exists? config)))

#?(:clj
   (defn build-secondary-index!
     "Backfill a secondary index by scanning AEVT for all covered attributes.
      Returns a channel (async op) so the writer continues processing other
      transactions during backfill. When complete, dispatches a lightweight
      install-secondary-index! op to atomically swap in the result."
     [old idx-ident]
     (log/trace :datahike/build-secondary-index {:idx-ident idx-ident})
     ;; Return a channel — writer runs this in background (lines 89-93 of writer.cljc)
     (let [db old
           idx (get-in db [:secondary-indices idx-ident])
           _ (when-not idx
               (log/raise "Secondary index not found" {:idx-ident idx-ident}))
           attrs (sec/-indexed-attrs idx)
           building-since-tx (get-in db [:schema idx-ident :db.secondary/building-since-tx])
           use-transient? (satisfies? sec/ITransientSecondaryIndex idx)
           t-idx (if use-transient? (sec/-as-transient idx) idx)]
       ;; Background go block — doesn't block the writer
       (go-try-
        (let [populated-idx
              (reduce
               (fn [current-idx attr]
                 (let [datoms (dbi/datoms db :aevt [attr])
                       n (atom 0)]
                   (log/debug :datahike/backfilling {:attr attr})
                   (let [result (reduce
                                 (fn [idx d]
                                   (if (and building-since-tx
                                            (> (.-tx ^datahike.datom.Datom d) building-since-tx))
                                     idx
                                     (do (swap! n inc)
                                         ;; Reconstruct each datom's tx-meta from the
                                         ;; historical EAVT seek on its tx-id. Without
                                         ;; this, vt-aware adapters miss the writing-tx
                                         ;; `:db.valid/from` and degrade to `txInstant`.
                                         (let [tx-id (.-tx ^datahike.datom.Datom d)
                                               tx-report {:datom d :added? true
                                                          :tx-meta (dbtx/meta-for-tx-id db tx-id)}]
                                           (if use-transient?
                                             (do (sec/-transact! idx tx-report) idx)
                                             (sec/-transact idx tx-report))))))
                                 current-idx datoms)]
                     (log/debug :datahike/backfilled {:attr attr :count @n})
                     result)))
               t-idx attrs)
              final-idx (if use-transient?
                          (sec/-persistent! populated-idx)
                          populated-idx)]
          (log/trace :datahike/secondary-index-built {:idx-ident idx-ident})
          ;; Return the populated index — the writer dispatch callback
          ;; receives this, but we need to install it via a separate writer op.
          ;; Store it in an atom for install-secondary-index! to pick up.
          {:idx-ident idx-ident :index final-idx})))))

#?(:clj
   (defn install-secondary-index!
     "Lightweight synchronous writer op that installs a backfilled index.
      Called after build-secondary-index! completes in the background."
     [old {:keys [idx-ident index]}]
     (let [db-after (-> old
                        (assoc-in [:secondary-indices idx-ident] index)
                        (assoc-in [:schema idx-ident :db.secondary/status] :ready)
                        (update-in [:schema idx-ident] dissoc :db.secondary/building-since-tx))]
       (complete-db-update old {:db-before old
                                :db-after db-after
                                :tx-data []
                                :tx-meta {}}))))

(defn merge-writer!
  "Writer operation for merge. Applies tx-data and records merge parents
   on the db meta so the commit loop creates a multi-parent merge commit."
  [old {:keys [parents tx-data tx-meta]}]
  (log/trace :datahike/merge {:parent-count (count parents) :tx-count (count tx-data)})
  (let [tx-report (complete-db-update old (core/with old tx-data tx-meta))
        ;; Add merge parents to db meta — commit loop picks these up
        branch (get-in old [:config :branch])
        all-parents (conj (set parents) branch)]
    (update tx-report :db-after
            assoc-in [:meta :datahike/merge-parents] all-parents)))

(defn transact! [old {:keys [tx-data tx-meta]}]
  (log/debug :datahike/transact {:tx-count (count tx-data)})
  (log/trace :datahike/transact-detail {:tx-data tx-data :tx-meta tx-meta})
  (complete-db-update old (core/with old tx-data tx-meta)))

(defn load-entities [old entities]
  (log/debug :datahike/load-entities {:entity-count (count entities)})
  (complete-db-update old (core/load-entities-with old entities nil)))
