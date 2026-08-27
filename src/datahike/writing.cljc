(ns datahike.writing
  "Manage all state changes and access to state of durable store."
  (:require [datahike.connections :refer [invalidate-store-connections!]]
            [datahike.db :as db]
            [datahike.gc-guard :as guard]
            [datahike.gc-roots :as roots]
            [datahike.db.transaction :as dbtx]
            [datahike.db.utils :as dbu]
            [datahike.db.interface :as dbi]
            [datahike.index :as di]
            [datahike.index.persistent-set :as dip]
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
             (when flush?
               (let [flushed (reduce-kv
                              (fn [acc idx-ident idx]
                                ;; A :building index is not a durable snapshot.
                                ;; Publishing its key-map lets a crash preserve an
                                ;; arbitrary prefix of the backfill, which cannot
                                ;; safely be replayed for non-idempotent indices.
                                (if (and (satisfies? sec/IVersionedSecondaryIndex idx)
                                         (not= :building
                                               (get-in db [:schema idx-ident
                                                           :db.secondary/status])))
                                  (assoc acc idx-ident (sec/-sec-flush idx store (:branch config)))
                                  acc))
                              {} (:secondary-indices db))
                     ;; Carry forward the stored pointer of every index the
                     ;; SCHEMA still declares but that has no live instance
                     ;; right now. A restore that failed once (it is caught and
                     ;; the ident dropped, see restore-secondary-indices) must
                     ;; not durably DELETE the index: writing a head without its
                     ;; key would make the next reconnect build an empty
                     ;; skeleton marked :ready, which nothing ever backfills
                     ;; (only :building is). Removal is explicit — retract the
                     ;; index's schema entry and its key goes with it; "no live
                     ;; instance" is never read as "delete".
                     carried (into {}
                                   (filter (fn [[ident _]]
                                             (and (get-in db [:schema ident :db.secondary/type])
                                                  (not= :building
                                                        (get-in db [:schema ident
                                                                    :db.secondary/status])))))
                                   (:secondary-index-keys db))]
                 (not-empty (merge carried flushed))))
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
                           (not-empty
                            (reduce-kv
                             (fn [acc idx-ident idx]
                               (if (= :building
                                      (get-in db [:schema idx-ident :db.secondary/status]))
                                 acc
                                 (assoc acc idx-ident
                                        (or (safe-root idx)
                                            (:merkle-root
                                             (get secondary-index-keys idx-ident))))))
                             {} (:secondary-indices db))))
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

(def ^:dynamic *on-secondary-restore-failure*
  "What to do when a secondary index that EXISTS in storage cannot be restored.

   `:fail` (default) aborts materialization — see `restore-secondary-indices` for
   why silently continuing loses the index for good. `:drop` comes up without it,
   which is the pre-existing behaviour and is what a deployment wants when it can
   rebuild the index and would rather be degraded than down.

   Not a config key: it is a property of the PROCESS doing the restoring, not of
   the database, and the stored config is the wrong place to record it."
  :fail)

(defn- restore-secondary-indices
  "Restore secondary index instances from stored key-maps.
   For versioned indices (IVersionedSecondaryIndex), restores from durable storage.
   For non-versioned or missing keys, creates empty instances that need backfill.

   A failure to restore an index THAT HAS A STORED KEY-MAP aborts the whole
   materialization. Dropping it instead — which is what this did — produces a db
   that is indistinguishable from one whose index has simply never been built:
   `finalize-secondary-indices` then creates a fresh empty instance on the next
   transaction, the commit overwrites the stored key-map with that empty one, and
   the index is durably gone. Nothing detects it afterwards, because an empty
   index and a lost index look the same. Failing at connect is recoverable; that
   is not.

   An index with NO stored key-map is a different case and still yields an empty
   skeleton: there is nothing to lose, and backfill is the normal path.

   Binding `*on-secondary-restore-failure*` to `:drop` restores the old behaviour
   for a deployment that would rather come up degraded than not at all — e.g. one
   whose index backend is not multi-process safe (scriptum's Lucene directory)
   and that accepts rebuilding. It logs at ERROR, not WARN."
  [schema ident-ref-map secondary-index-keys store]
  #?(:clj
     (reduce-kv
      (fn [acc ident entry]
        (if (and (map? entry) (:db.secondary/type entry))
          (let [idx-type (:db.secondary/type entry)
                idx-attrs (set (:db.secondary/attrs entry))
                ;; A key-map written while the schema says :building is, at
                ;; best, a partial snapshot from an older Datahike. Ignore it
                ;; and rebuild from the primary index instead.
                key-map (when-not (= :building (:db.secondary/status entry))
                          (get secondary-index-keys ident))
                idx-config (cond-> (merge (:db.secondary/config entry)
                                          {:attrs idx-attrs
                                           ::sec/index-ident ident})
                             (seq ident-ref-map)
                             (assoc :ident-ref-map ident-ref-map)
                             (= :building (:db.secondary/status entry))
                             (assoc ::sec/build-attempt (random-uuid))
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
                (if (and key-map (not= :drop *on-secondary-restore-failure*))
                  (do
                    ;; Nobody will ever hold the indices restored before this
                    ;; one: the accumulator dies with the raise. Close them, or
                    ;; each failed attempt strands another native writer — and
                    ;; for a lock-holding backend (Lucene) the leaked lock is
                    ;; what makes the NEXT attempt fail too, turning a transient
                    ;; failure into a permanent one.
                    (doseq [[_ idx] acc]
                      (when (instance? java.io.Closeable idx)
                        (try (.close ^java.io.Closeable idx)
                             (catch Exception close-e
                               (log/warn :datahike/secondary-index-close-failed
                                         {:error (.getMessage close-e)})))))
                    (log/raise "Could not restore a secondary index that exists in storage. Continuing would overwrite it with an empty one on the next commit. Bind datahike.writing/*on-secondary-restore-failure* to :drop to come up without it anyway (the stored index is then lost on the next write)."
                               {:type  :secondary-index-restore-failed
                                :ident ident
                                :index-type idx-type
                                :error e}))
                  (do (log/error :datahike/secondary-index-restore-failed
                                 {:ident ident :has-stored-index? (some? key-map)
                                  :error (.getMessage e)})
                      acc)))))
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
        ;; A partial key-map from an older release must not survive merely
        ;; because stored->db retained it for the carry-forward path.
        secondary-index-keys
        (not-empty
         (into {}
               (remove (fn [[ident _]]
                         (= :building
                            (get-in effective-schema [ident :db.secondary/status]))))
               secondary-index-keys))
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
     ;; Kept on the db so the next db->stored can carry forward the pointer of
     ;; an index whose restore failed (see there). Only when there is one: an
     ;; explicit nil would make every db built from storage unequal to the
     ;; same db built in memory.
     (when (seq secondary-index-keys)
       {:secondary-index-keys secondary-index-keys})
     (when (seq sec-indices)
       {:secondary-indices sec-indices})
     schema-meta)))

(defn stored->db-read-only
  "`stored->db` for a db that can never be written back — a historical commit, a
   branch loaded as a value, a reader-materialized db.

   The reason [[*on-secondary-restore-failure*]] defaults to `:fail` is that a
   dropped index would be overwritten with an empty one by the NEXT COMMIT on
   that db. A db nothing can commit cannot lose an index that way, so failing
   here buys no safety and costs availability: it turns a history walk into a
   hard error whenever an index backend is momentarily unrestorable — which for
   scriptum, whose Lucene directory holds a per-branch write lock, is simply
   \"a writer is running\".

   Use this ONLY where the db is a value handed to the caller AND no writer
   reads an index off it. `deref-conn` does hand back a value — it never
   `reset!`s the connection — but it must still keep `:fail`, because
   `versioning/branch!` takes `(:secondary-indices @conn)` as its LIVE index set
   and writes the resulting key-map as the new branch record's
   `:secondary-index-keys`. An index dropped from that deref is simply absent
   from the reduce, so the new branch is created without its pointer — the same
   durable loss, one level removed."
  [stored-db store]
  (binding [*on-secondary-restore-failure* :drop]
    (stored->db stored-db store)))

(defn reload-head
  "`old` itself when `stored` is the very commit `old` already is, otherwise a
   fresh in-memory db built from `stored`.

   A record with NO cid is never treated as unmoved, even against an `old` that
   also has none: neither side knowing its identity is not evidence that the two
   match, and reading it as a match would make the writer blind to every other
   process's commits — precisely the failure shared writer ownership exists to
   prevent. Unreachable for databases this version creates (`create-database`
   stamps a cid); the guard is for foreign or legacy records.

   Identity is the commit-id: the same cid means the same stored record, hence
   the same primary index roots AND the same secondary-index key-maps, so there
   is nothing to rebuild and nothing to re-open — which is what makes a re-read
   cheap enough to do per transaction. A moved head rebuilds everything,
   secondary indices included; index creation and removal ride on the cid too.

   The runtime config and the `:writer` are carried over from `old`: neither
   lives in storage (`:writer` is the connection's own transactor), and dropping
   it would leave the connection without a transactor."
  ([old stored store] (reload-head old stored store nil nil))
  ([old stored store head-revision] (reload-head old stored store head-revision nil))
  ([old stored store head-revision materialized]
   (let [stored-cid (get-in stored [:meta :datahike/commit-id])
         ;; The konserve revision the head blob was AT when we read it, carried on
         ;; the db so a later commit can fence its head write against it. Distinct
         ;; from the commit-id: the cid identifies datahike's state, the revision
         ;; identifies the STORAGE write, and only the latter is what konserve can
         ;; compare-and-set on. Kept as an internal top-level field so it travels
         ;; with the db through the writer loop, but never enters db->stored,
         ;; public database metadata, or the content-derived commit id.
         stamp (fn [db] (cond-> db head-revision
                                (assoc ::head-revision head-revision)))]
     (if (and stored-cid (= stored-cid (get-in old [:meta :datahike/commit-id])))
       ;; Unmoved head: the db is unchanged, but the revision may not be — the blob
       ;; can have been rewritten with identical content. Take the fresh one.
       (stamp old)
       (stamp (assoc (or materialized
                         (stored->db (assoc stored :config (:config old)) store))
                     :writer (:writer old)))))))

(def ^:private primary-index-keys
  [:eavt :aevt :avet :temporal-eavt :temporal-aevt :temporal-avet])

(defn- hydrate-moved-head
  "Materialize a moved persistent-set head without enumerating the store.

   Schema metadata is fetched explicitly because `stored->db` is synchronous.
   Each primary index is then walked against the connection's old root. Shared
   subtrees are pruned by persistent-sorted-set; reads of the remaining frontier
   use Konserve's awaited read-through so a nested S3 -> IndexedDB -> memory
   store is complete from the deepest tier outward before this returns."
  [old stored store sync?]
  (async+sync sync? *default-sync-translation*
              (go-try-
               (when-let [schema-key (:schema-meta-key stored)]
                 (when-not (sc/cache-has? schema-key)
                   (when-let [schema-meta
                              (<?- (k/get store schema-key nil
                                          {:sync? sync?
                                           :await-read-through? true}))]
                     (sc/cache-miss schema-key schema-meta))))
               (let [new-db (stored->db (assoc stored :config (:config old)) store)
                     walk-opts {:sync? sync? :await-read-through? true}]
                 #?(:clj
                    (doseq [index-key primary-index-keys]
                      (let [old-index (get old index-key)
                            new-index (get new-db index-key)]
                        (when (and old-index new-index)
                          (dip/walk-index-delta old-index new-index walk-opts))))
                    :cljs
                    (doseq [index-key primary-index-keys]
                      (let [old-index (get old index-key)
                            new-index (get new-db index-key)]
                        (when (and old-index new-index)
                          (<?- (dip/walk-index-delta old-index new-index walk-opts))))))
                 new-db))))

(defn reload-branch-head
  "Re-read `old`'s branch head from storage and rebuild an in-memory db from it
   when it moved (see [[reload-head]]).

   This is the synchronization point of a SHARED self writer (`:writer
   {:backend :self :writer-ownership :shared}`), which is the default. Opt-in
   exclusive ownership assumes this JVM is the only writer and keeps the branch head
   in memory, so a second process
   holding a writer for the same database would transact on top of its own
   stale snapshot and overwrite the other's commits. Re-reading before every
   transaction makes each one apply to whatever is actually stored.

   An unchanged head costs ONE konserve read (one S3 GET). A moved
   persistent-set head additionally walks and hydrates only the changed Merkle
   frontier, pruning structurally shared subtrees and never listing the store.
   A legacy index falls back to the full tier synchronization it required
   before this incremental path existed.

   SECONDARY INDICES are re-read with the rest of the head whenever it moved.
   Stratum and proximum are konserve-backed copy-on-write values, so that is
   exactly right for them: another process's commit is picked up. Scriptum is
   the exception — it keeps its own Lucene directory with a per-branch write
   lock and is NOT multi-process safe. That is transitional (its blobs are
   konserve-backed, but its manifest is still written unconditionally, so two
   writers on one branch orphan the loser's segments), not a property of
   secondary indices."
  ([old] (reload-branch-head old true))
  ([old sync?]
   (async+sync sync? *default-sync-translation*
               (go-try-
                (let [store    (:store old)
                      branch   (:branch (:config old))
                      fenced?  (some? (k/conditional-write-domain store))
                      ;; ONE read for both when the store can fence. Reading the
                      ;; revision separately would be racy. On a tiered store the
                      ;; revision-bearing read deliberately goes to the backend,
                      ;; because that is where the conditional write is decided.
                      raw      (if fenced?
                                 (<?- (k/get store branch nil {:sync? sync?
                                                               :with-revision? true}))
                                 [(<?- (k/get store branch nil {:sync? sync?})) nil])
                      [stored revision] raw]
                  (when-not stored
                    (log/raise "Branch head vanished from the store; the database may have been deleted."
                               {:type   :branch-head-does-not-exist-in-store
                                :branch branch}))
                  ;; The engine below this boundary is synchronous. Hydrate only
                  ;; the immutable PSS frontier introduced by a foreign head,
                  ;; then publish the new db. The legacy hitchhiker index cannot
                  ;; expose that structural delta and retains the full tier sync.
                  (let [moved? (not= (get-in stored [:meta :datahike/commit-id])
                                     (get-in old [:meta :datahike/commit-id]))
                        materialized
                        (when moved?
                          (if (= :datahike.index/persistent-set
                                 (get-in old [:config :index]))
                            (<?- (hydrate-moved-head old stored store sync?))
                            (do
                              (<?- (ds/refresh-tiered-frontend store {:sync? sync?}))
                              (stored->db (assoc stored :config (:config old)) store))))]
                    (reload-head old stored store revision materialized)))))))

(defn branch-heads-as-commits
  "Resolve keyword parents (branch names) to their head commit-ids.

   `known-heads` is an optional {branch-keyword commit-id} map of heads the
   caller already holds in memory: under datahike's single-writer invariant
   the writer's current db carries its own branch's head cid in
   [:meta :datahike/commit-id], so re-reading the branch record from storage
   (one konserve read per parent branch, i.e. one S3 GET for the single parent
   of an ordinary commit) is redundant for it. A nil or
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

(defn- as-awaitable
  "Hand `x` back in the shape the streaming index builder's `await` expects.

   Two async worlds meet at this seam and they are not the same one. Everything
   in datahike's write path is core.async: `write-pending-kvs!` returns a channel
   under `:sync? false`. persistent-sorted-set's ClojureScript builder is
   partial-cps, and its `await` wants a continuation — handed a channel it fails
   with `fexpr.call is not a function` on the very first flush, because a channel
   is not callable.

   On the JVM the builder is synchronous and never awaits, so the value passes
   through untouched; only ClojureScript needs the adapter. konserve delivers
   errors as values, so an error on the channel becomes a rejection rather than a
   result that looks like success."
  [x]
  #?(:clj x
     :cljs (fn [resolve reject]
             (if (satisfies? cljs.core.async.impl.protocols/ReadPort x)
               (async/take! x (fn [v] (if (instance? js/Error v) (reject v) (resolve v))))
               (resolve x)))))

(def ^:const default-index-flush-threshold
  "Nodes allowed to accumulate in `pending-writes` before a streaming index build
   drains them. ~1000 nodes at the default branching factor is on the order of
   tens of MB — generous enough that no ordinary build pays for the check, small
   enough that the bound is a bound."
  1000)

(defn bulk-flush-fn
  "A `:flush-fn` for `di/init-index-sorted`: drain `pending-writes` once it grows
   past `threshold`, else do nothing.

   ## The caller MUST hold the GC guard

   This writes index nodes that nothing in the store references yet — the branch
   head still names the previous snapshot. `datahike.gc-guard` spells out what
   that means: a mark running in that window classifies them as garbage and a
   sweep deletes them, after which the commit publishes a root pointing at
   deleted addresses. The guard must therefore be held across the WHOLE
   sequence, from the first flush to the commit that makes the root reachable —
   `(guard/writing! store-id)` before the build, `(guard/done! …)` after the
   commit.

   It lives here rather than in `datahike.index.persistent-set` for exactly that
   reason. The index layer cannot hold the guard for the right span, because the
   span extends past the build into a commit it knows nothing about; and
   durability policy — when nodes become durable, in what order, under whose
   guard — already lives in this namespace. An earlier version drained from
   inside the index layer, unguarded, which is the blind spot `gc_guard`
   exists to close.

   Writes go through `write-pending-kvs!`, so the flush and the commit share one
   implementation of the write discipline rather than the index layer keeping a
   weaker copy.

   nil when there is nothing to drain or flushing is switched off, which lets the
   builder skip the hook entirely."
  ([store] (bulk-flush-fn store true))
  ([store sync?]
   (let [pending   (-> store :storage :pending-writes)
         threshold (:datahike/index-flush-threshold store default-index-flush-threshold)]
     (when (and pending (pos? threshold))
       (fn []
         (if (>= (count @pending) threshold)
           (let [[kvs _] (swap-vals! pending (constantly []))]
             (cond-> (write-pending-kvs! store kvs sync?)
               (not sync?) as-awaitable))
           ;; Nothing to do — but the builder AWAITS this, so the async arm still
           ;; has to hand back something awaitable.
           (when-not sync? (as-awaitable nil))))))))

(defn ^:private fencing-required?
  "Did the caller demand fencing for this connection?

   Truthiness, NOT `some?`, so that it answers the same question
   `check-fencing!` asks at connect: there `:require-fencing false` skips the
   check (a `when`), so reading it as a demand HERE would admit a connection at
   connect and then fail its every commit — the two gates disagreeing about what
   `false` means. `false` is what nil becomes in a config that cannot spell nil
   (JSON, env vars), and both gates now read it as \"not required\"."
  [db]
  (boolean (get-in db [:config :writer :require-fencing])))

(defn commit!
  ([db parents]
   (commit! db parents true))
  ([db parents sync?]
   (commit! db parents sync? nil))
  ([db parents sync? known-head-cid] (commit! db parents sync? known-head-cid nil))
  ([db parents sync? known-head-cid head-revision]
   ;; Set by whichever write path runs; read after, to stamp the db we return.
   (when (and (nil? head-revision) (fencing-required? db))
     ;; A nil revision means "write unconditionally", which is right for a store
     ;; that cannot fence — and wrong, silently, for a connection that DEMANDED
     ;; fencing. Refuse instead: one unfenced commit is exactly the lost update
     ;; :require-fencing was set to prevent.
     (log/raise (str "This commit has no head revision to fence against, but the connection requires fencing. "
                     "On an upgraded database this means the branch head predates revisions: one ordinary "
                     "transact on a connection WITHOUT :require-fencing writes the head once unconditionally "
                     "and it is fenceable from then on.")
                {:type :datahike/fencing-unavailable
                 :branch (:branch (:config db))}))
   (let [new-head-revision (atom nil)]
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
                ;;
                ;; GC GUARD: everything from here until the head flips is written
                ;; UNREFERENCED — the head still names the previous snapshot — so a
                ;; concurrent collector would call it garbage and sweep it. Opened
                ;; BEFORE db->stored because a secondary index's -sec-flush (stratum)
                ;; writes konserve keys from inside it. Closed in the finally: an
                ;; aborted commit leaves orphans, which are genuinely collectable.
                  (let [gc-store-id (:id (:store (:config db)))
                        gc-token    (guard/writing! gc-store-id)]
                    (try
                      (let [{:keys [store config]} db
                        ;; Head-cid cache: for an ORDINARY commit (no explicit
                        ;; parents) the writer's own head cid is already in
                        ;; memory — stamped by the previous commit!, or by
                        ;; stored->db at connect — so skip the per-commit
                        ;; branch-head storage read (ONE konserve read = one
                        ;; S3 GET: konserve-s3 >= 0.1.33 is PReadMissSafe and
                        ;; serves header, metadata and value out of that
                        ;; single response body, with no HEAD probe in front
                        ;; of it). Explicit-parents commits (merge!,
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
                            ;; FENCED: the head leaves the batch. `multi-assoc`
                            ;; refuses `:expected-revision`, and rightly so —
                            ;; verifying every key and then writing every key is
                            ;; not one atomic step when locks are per blob. The
                            ;; ordering that matters is unaffected: values as a
                            ;; batch, THEN the mutable pointer, which is the same
                            ;; barrier the unfenced path spells out below.
                                writes (cond-> (vec pending-kvs)
                                         schema-meta-kv-to-write (conj [meta-key meta-val])
                                         commit-graph?           (conj [cid db-to-store])
                                         (not head-revision)     (conj [branch-key db-to-store]))
                            ;; nodes + schema-meta (uuid) + commit (cid) are content-addressed →
                            ;; immutable; the branch-head pointer stays mutable (unmarked).
                                metas  (into {}
                                             (comp (map first)
                                                   (remove #(= % branch-key))
                                                   (map (fn [k] [k {:immutable? true}])))
                                             writes)]
                            ;; With root fusion and no commit graph, a small fenced
                            ;; commit can have nothing to write before the head: the
                            ;; roots are embedded in `db-to-store`, there is no cid
                            ;; record, and the head deliberately left this batch.
                            ;; Some transactional backends (DynamoDB in particular)
                            ;; reject an empty transaction, so do not issue one.
                            (when (seq writes)
                              (<?- (k/multi-assoc store writes metas {:sync? sync?})))
                            (when head-revision
                              ;; `:with-revision? true` and the capture below are
                              ;; NOT optional bookkeeping. The commit loop threads
                              ;; the revision this write CREATES into the next
                              ;; commit group's fence; without them the returned db
                              ;; kept the pre-commit stamp, and on every multi-key
                              ;; store each chained group fenced against a revision
                              ;; this very writer had already moved — measured as
                              ;; 24 manufactured head conflicts in 300 transactions
                              ;; from a SOLE writer, each one a retry with backoff,
                              ;; and each one a caller-visible error under
                              ;; :head-conflict-retries 0.
                              (let [r (<?- (k/assoc store branch-key db-to-store
                                                    {:sync? sync?
                                                     :expected-revision head-revision
                                                     :with-revision? true}))]
                                (reset! new-head-revision (second r)))))
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
                            ;; AWAIT the commit record before ISSUING the head write.
                            ;; Under :sync? false both k/assoc calls return ops that are
                            ;; ALREADY RUNNING, so binding them side by side lets the
                            ;; mutable head land before the immutable record it names —
                            ;; a crash in between truncates branch-history. (Under
                            ;; :sync? true k/assoc blocks, so the order already holds.)
                                _                  (when (and commit-log-written (not sync?))
                                                     (<?- commit-log-written))
                            ;; THE FENCE. `head-revision` is the konserve revision
                            ;; the head was at when this transaction was applied to
                            ;; it; the write is rejected if anything has written
                            ;; there since. nil means unfenced — a store that cannot
                            ;; compare, or a caller that did not read a revision —
                            ;; and behaves exactly as before.
                                branch-written     (k/assoc store (:branch config) db-to-store
                                                            (cond-> {:sync? sync?}
                                                              head-revision
                                                              (assoc :expected-revision head-revision
                                                                     :with-revision? true)))]
                            (reset! new-head-revision
                                    (let [r (if sync? branch-written (<?- branch-written))]
                                      (when head-revision (second r))))))

                  ;; Online GC: delete freed addresses after writes are committed
                        (when (get-in config [:online-gc :enabled?])
                          (<?- (online-gc/online-gc! store (assoc (:online-gc config) :sync? false))))

                      ;; Keep what we just wrote on the db we hand back, so the
                      ;; NEXT db->stored can carry a pointer forward for an
                      ;; index whose live instance went missing meanwhile.
                      ;; The head revision this commit created rides along for the
                      ;; same reason: the next commit of the same batch fences
                      ;; against it, and asking storage for it would be a read we
                      ;; just earned the right not to make.
                        (cond-> db
                          (seq (:secondary-index-keys db-to-store))
                          (assoc :secondary-index-keys (:secondary-index-keys db-to-store))
                          @new-head-revision
                          (assoc ::head-revision @new-head-revision)))
                      (catch #?(:clj Error :cljs :default) e
                        #?(:clj  (throw (ex-info "Fatal error during commit."
                                                 {:type :fatal-commit-error}
                                                 e))
                           :cljs (throw e)))
                      (finally
                        (guard/done! gc-store-id gc-token)))))))))

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
         ;; Value-size caps are OPT-IN. Resolve the `:value-caps :default` preset
         ;; and drop the selector at create only; an unconfigured database is left
         ;; unbounded. Warn once so the choice is conscious rather than silent.
         loaded-config (dc/load-config config deprecated-config)
         _ (when-not (dc/value-caps-configured? loaded-config)
             (log/warn :datahike/value-caps-unset
                       (str "No value-size caps set — large :db.type/string / :db.type/bytes / "
                            ":db.type/float-array / :db.type/double-array values can bloat the "
                            "index and hit backend limits. Pass :value-caps :default for the "
                            "default caps (or set :max-string-length etc. explicitly) to bound "
                            "them, or :max-string-length 0 to stay unbounded and silence this.")))
         {:keys [keep-history?] :as config} (dc/apply-default-value-caps loaded-config)
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
         db-to-store (assoc pre-cid-stored :meta meta)
         ;; GC guard: same values-then-pointer sequence as commit!. A brand-new
         ;; store cannot be collected (no :branches key yet), but a store being
         ;; RE-created after delete-database can still have a background collector
         ;; running against it from a previous connection.
         gc-token (guard/writing! (:id store-config))]
     (try
       ;;we just created the first data base in this store, so the write cache is empty
       ;; schema-meta-key = (uuid schema-meta) → content-addressed, immutable
       (<?- (k/assoc store schema-meta-key schema-meta {:immutable? true} opts))
       (sc/add-to-write-cache (:store config) schema-meta-key)
       (when-not (sc/cache-has? schema-meta-key)
         (sc/cache-miss schema-meta-key schema-meta))

       ;; Process pending KVs from index flushes synchronously
       (let [pending-kvs (get-and-clear-pending-kvs! store)]
         (<?- (write-pending-kvs! store pending-kvs false)))

       (<?- (k/assoc store cid db-to-store {:immutable? true} opts)) ; content-addressed commit
       ;; Claim the initial mutable head. The existence check above is useful for
       ;; its message but cannot serialize two creators in different processes;
       ;; on a conditional store, exactly one absent-key CAS wins. Everything the
       ;; loser wrote before this point is immutable and collectable.
       (try
         (<?- (k/assoc store :db db-to-store
                       (cond-> opts
                         (k/conditional-write-domain store)
                         (assoc :expected-revision k/absent))))
         (catch #?(:clj Throwable :cljs :default) e
           (if (= :konserve/revision-mismatch (:type (ex-data e)))
             (log/raise "Database already exists."
                        {:type :db-already-exists :config store-config})
             (throw e))))
       ;; :branches names :db, so it is a POINTER and must be written LAST — a
       ;; collector that reads it before the head exists marks nothing for :db.
       (<?- (k/assoc store :branches #{:db} opts))           ; mutable: branch set
       (ks/release-store store-config store)
       config
       (finally
         (guard/done! (:id store-config) gc-token))))))

(defn -delete-database* [config]
  (go-try-
   (let [config (dc/load-config config {})
         config-store-id (ds/store-identity (:store config))]
     (sc/clear-write-cache (:store config))
     (invalidate-store-connections! config-store-id)
     ;; AWAIT the deletion.
     ;;
     ;; konserve.store/delete-store defaults to {:sync? false}, and the async backends
     ;; (:s3, …) then hand back a CHANNEL. Without awaiting it, this go-try- yields that
     ;; channel as its value, so `delete-database` resolves — handing a raw core.async
     ;; channel to the caller — while the store is still being deleted. `(d/delete-database
     ;; cfg)` followed by `(d/database-exists? cfg)` then still sees the database, and
     ;; delete-then-recreate races its own deletion.
     ;;
     ;; Requires konserve >= 0.9.357: -delete-store used to ignore :sync? on :memory
     ;; and :file (returning a plain value where the contract promises a channel), and
     ;; :tiered dropped its backend's channel entirely — so a tiered delete over S3
     ;; removed nothing. konserve#152 makes all backends honour the contract, which is
     ;; what lets us simply await here.
     (<?- (ks/delete-store (:store config))))))

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
   (defn- close-secondary-index! [idx]
     (when (instance? java.io.Closeable idx)
       (try (.close ^java.io.Closeable idx)
            (catch Exception e
              (log/warn :datahike/secondary-index-close-failed
                        {:error (.getMessage e)}))))))

#?(:clj
   (defn build-secondary-index!
     "Backfill a secondary index by scanning AEVT for all covered attributes.
      Returns a channel so the writer can continue serving transactions. While
      it runs, those transactions journal changes for this index; the serialized
      install operation replays that delta before publishing the result."
     [old idx-ident]
     (log/trace :datahike/build-secondary-index {:idx-ident idx-ident})
     (let [db old
           writer-config (get-in db [:config :writer])
           _ (when-not (and (= :self (get writer-config :backend :self))
                            (= :exclusive
                               (get writer-config :writer-ownership :shared)))
               (log/raise
                "Asynchronous secondary-index backfill requires local exclusive writer ownership."
                {:type :secondary-index-backfill-unsupported-writer
                 :idx-ident idx-ident
                 :writer writer-config}))
           idx (get-in db [:secondary-indices idx-ident])
           _ (when-not idx
               (log/raise "Secondary index not found" {:idx-ident idx-ident}))
           attrs (sec/-indexed-attrs idx)
           building-since-tx (get-in db [:schema idx-ident :db.secondary/building-since-tx])
           _ (when-not building-since-tx
               (log/raise "A building secondary index has no snapshot boundary"
                          {:type :secondary-index-missing-build-boundary
                           :idx-ident idx-ident}))
           use-transient? (satisfies? sec/ITransientSecondaryIndex idx)
           t-idx (if use-transient? (sec/-as-transient idx) idx)
           ;; The scan reads the DURABLE head, not `old`. The db a writer op
           ;; receives can run ahead of the commit loop — unflushed roots, a
           ;; lagging :meta — and nothing unflushed can be pinned. Any committed
           ;; snapshot at or after the boundary is a correct scan source: the
           ;; scan skips datoms newer than the boundary and the journal carries
           ;; them, and a datom retracted after the boundary is replayed as a
           ;; retraction of nothing. The schema commit was awaited before this
           ;; op was dispatched, so the head is at least that far.
           store (:store db)
           branch (get-in db [:config :branch] :db)
           head-record (k/get store branch nil {:sync? true})
           _ (when-not (and head-record (>= (:max-tx head-record) building-since-tx))
               (log/raise "The branch head is behind the backfill boundary; the schema commit has not landed."
                          {:type :secondary-index-head-behind-boundary
                           :idx-ident idx-ident
                           :head-max-tx (:max-tx head-record)
                           :building-since-tx building-since-tx}))
           ;; No secondary indices on the scan snapshot: it is read for its
           ;; primary AEVT only, and restoring adapters (a Lucene lock, say)
           ;; is work and hazard for nothing.
           snapshot (stored->db-read-only (dissoc head-record :secondary-index-keys) store)
           gc-store-id (:id (:store (:config db)))
           ;; A versioned adapter may write private nodes while it builds. They
           ;; remain unreachable until install's commit publishes its key-map,
           ;; so protect the whole scan -> ready-commit window from GC in this
           ;; process. (A durable checkpoint for them is the follow-up.)
           gc-token (guard/writing! gc-store-id)
           ;; The snapshot being scanned is pinned with a DURABLE root, so a
           ;; collector in any process keeps it until the ready commit lands.
           ;; The lease is renewed in the background; if it is ever lost the
           ;; build is discarded at install rather than published over swept
           ;; nodes.
           lost (atom nil)]
       (go-try-
        (let [root-id (try (<?- (roots/root! db {:kind :pin
                                                 :record head-record
                                                 :note (str "secondary-index backfill " idx-ident)
                                                 :owner {:idx-ident idx-ident}}))
                           (catch Throwable e
                             (close-secondary-index! idx)
                             (guard/done! gc-store-id gc-token)
                             (throw e)))
              stop-renewal! (roots/start-renewal! db root-id {:on-lost #(reset! lost %)})
              release-root! (fn []
                              (stop-renewal!)
                              (try (roots/release! db root-id {:sync? true})
                                   (catch Throwable e
                                     (log/warn :datahike/secondary-index-root-release-failed
                                               {:idx-ident idx-ident
                                                :message (.getMessage ^Throwable e)
                                                :error e}))))]
          (try
            (let [populated-idx
                  (reduce
                   (fn [current-idx attr]
                     (let [datoms (dbi/datoms snapshot :aevt [attr])
                           n (atom 0)]
                       (log/debug :datahike/backfilling {:attr attr})
                       (let [result (reduce
                                     (fn [idx d]
                                       (if (> (.-tx ^datahike.datom.Datom d) building-since-tx)
                                         idx
                                         (do (swap! n inc)
                                             (let [tx-id (.-tx ^datahike.datom.Datom d)
                                                   tx-report {:datom d :added? true
                                                              :tx-meta (dbtx/meta-for-tx-id snapshot tx-id)}]
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
              {:idx-ident idx-ident
               :index final-idx
               :building-since-tx building-since-tx
               ::gc-store-id gc-store-id
               ::gc-token gc-token
               ::root-id root-id
               ::root-db db
               ::root-lost lost
               ::release-root! release-root!})
            (catch Throwable e
              (close-secondary-index! idx)
              (guard/done! gc-store-id gc-token)
              (release-root!)
              (throw e))))))))

#?(:clj
   (defn finish-secondary-index-build!
     "Release the GC guard and the durable root carried by a completed
      background build. Call only after install's writer callback, which means
      its ready commit is durable. Idempotent."
     [build-result]
     (when-let [token (::gc-token build-result)]
       (guard/done! (::gc-store-id build-result) token))
     (when-let [release! (::release-root! build-result)]
       (release!))))

#?(:clj
   (defn- assert-build-root-live!
     "The publish-time check: the root that pinned this build's snapshot is
      still there and was renewed recently. A build whose lease was lost —
      reaped, or eaten by an older collector — may have scanned nodes that
      have since been swept, and must not be published."
     [{:keys [idx-ident] ::keys [root-id root-db root-lost]}]
     (when-let [e (some-> root-lost deref)]
       (throw (ex-info "Discarding a secondary-index build whose GC root was lost during the scan."
                       {:type :gc/root-lost :idx-ident idx-ident :root-id root-id}
                       e)))
     (when root-id
       (roots/assert-live! root-db root-id (quot roots/DEFAULT_TTL_MS 2) {:sync? true}))))

#?(:clj
   (defn- drop-build-deltas [db idx-ident]
     (let [remaining (dissoc (:secondary-index-build-deltas db) idx-ident)]
       (if (empty? remaining)
         (dissoc db :secondary-index-build-deltas)
         (assoc db :secondary-index-build-deltas remaining)))))

#?(:clj
   (defn install-secondary-index!
     "Replay changes accumulated during an asynchronous backfill and publish
      the resulting index. This operation is serialized by the writer, which
      closes the handoff gap between the delta journal and normal live updates."
     [old {:keys [idx-ident index building-since-tx] :as build-result}]
     (try
       (let [status (get-in old [:schema idx-ident :db.secondary/status])
             current-boundary (get-in old [:schema idx-ident
                                           :db.secondary/building-since-tx])]
         (when-not (and (= :building status)
                        (= building-since-tx current-boundary))
           (log/raise "Discarding a stale secondary-index build"
                      {:type :secondary-index-stale-build
                       :idx-ident idx-ident
                       :expected-building-since-tx current-boundary
                       :actual-building-since-tx building-since-tx
                       :status status}))
         (assert-build-root-live! build-result)
         (let [deltas (get-in old [:secondary-index-build-deltas idx-ident] [])
               use-transient? (satisfies? sec/ITransientSecondaryIndex index)
               t-idx (if use-transient? (sec/-as-transient index) index)
               replayed (reduce (fn [idx tx-report]
                                  (if use-transient?
                                    (do (sec/-transact! idx tx-report) idx)
                                    (sec/-transact idx tx-report)))
                                t-idx deltas)
               final-idx (if use-transient? (sec/-persistent! replayed) replayed)
               db-after (-> old
                            (assoc-in [:secondary-indices idx-ident] final-idx)
                            (assoc-in [:schema idx-ident :db.secondary/status] :ready)
                            (update-in [:schema idx-ident] dissoc
                                       :db.secondary/building-since-tx)
                            (drop-build-deltas idx-ident))]
           (complete-db-update
            old {:db-before old
                 :db-after db-after
                 :tx-data []
                 :tx-meta {:db/txInstant (get-in old [:meta :datahike/updated-at])}
                 :secondary-index-build-guard
                 (select-keys build-result [::gc-store-id ::gc-token ::release-root!])})))
       (catch Throwable e
         (close-secondary-index! index)
         (finish-secondary-index-build! build-result)
         (throw e)))))

#?(:clj
   (defn reset-secondary-index-build-boundary!
     "Re-anchor a recovered :building index's snapshot boundary at the current
      head. The stored boundary was set by the schema transaction; every
      transaction after it was journaled only in the process that then
      stopped, so on reconnect those datoms exist solely in the primary index.
      Moving the boundary to this head makes the scan cover everything
      committed so far while the journal covers everything committed after
      this serialized operation. Any entries journaled before it belong to
      transactions the scan will see, so they are dropped."
     [old idx-ident]
     (let [status (get-in old [:schema idx-ident :db.secondary/status])]
       (when-not (= :building status)
         (log/raise "Only a building secondary index has a snapshot boundary to reset"
                    {:type :secondary-index-not-building
                     :idx-ident idx-ident
                     :status status}))
       (complete-db-update
        old {:db-before old
             :db-after (-> old
                           (assoc-in [:schema idx-ident
                                      :db.secondary/building-since-tx]
                                     (:max-tx old))
                           (drop-build-deltas idx-ident))
             :tx-data []
             :tx-meta {:db/txInstant (get-in old [:meta :datahike/updated-at])}}))))

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

#?(:clj
   (defn- validate-secondary-backfill-writer!
     "Reject a new asynchronous backfill before its schema report reaches the
      commit queue. This belongs at the writer boundary so pure `db-with` can
      still model a :building database without owning a writer."
     [old {:keys [db-after]}]
     (let [before (:schema old)
           new-building
           (into []
                 (keep (fn [[ident entry]]
                         (when (and (= :building (:db.secondary/status entry))
                                    (not= :building
                                          (get-in before
                                                  [ident :db.secondary/status])))
                           ident)))
                 (:schema db-after))
           writer-config (get-in old [:config :writer])
           local-exclusive? (and (= :self (get writer-config :backend :self))
                                 (= :exclusive
                                    (get writer-config :writer-ownership :shared)))]
       (when (and (seq new-building) (not local-exclusive?))
         ;; Factories already ran in core/with. Close any native private
         ;; generation before refusing its uncommitted schema report.
         (doseq [idx-ident new-building]
           (close-secondary-index! (get-in db-after [:secondary-indices idx-ident])))
         (log/raise
          "Asynchronous secondary-index backfill currently requires local exclusive writer ownership. Remote writers cannot transfer a live build generation, and shared writers cannot coordinate its in-memory delta journal across processes."
          {:type :secondary-index-backfill-unsupported-writer
           :idx-ident (first new-building)
           :idx-idents (set new-building)
           :writer writer-config})))))

(defn transact! [old {:keys [tx-data tx-meta]}]
  (log/debug :datahike/transact {:tx-count (count tx-data)})
  (log/trace :datahike/transact-detail {:tx-data tx-data :tx-meta tx-meta})
  (let [tx-report (core/with old tx-data tx-meta)]
    #?(:clj (validate-secondary-backfill-writer! old tx-report))
    (complete-db-update old tx-report)))

(defn load-entities
  [old entities]
  (log/debug :datahike/load-entities {:entity-count (count entities)})
  (complete-db-update old (core/load-entities-with old entities nil nil)))

(defn ^:no-doc load-entities-migrating
  "`load-entities` threading an import's id mapping. **Internal to
   `datahike.migrate`.**

   `migration` is the `{:eids … :tids …}` map `transact-entities-directly` takes
   in and hands back on the report: an import is many calls, and a ref in a late
   batch may name an entity first seen in an early one, so the mapping has to
   survive between them.

   A SEPARATE function rather than an arity on `load-entities`, because that one
   is public, `:stability :stable`, and declared in `datahike.api.specification`
   as taking exactly two arguments. Widening it would have made an
   import-internal id map part of the contract that generates the Java, pod, CLI
   and TypeScript bindings, where it means nothing — and this release already
   changes `load-entities`' BEHAVIOUR (transaction ids no longer vary with
   `:batch-size`). One change to a stable function is a documented bug fix; two
   is a habit."
  [old entities migration]
  (log/debug :datahike/load-entities-migrating {:entity-count (count entities)})
  (complete-db-update old (core/load-entities-with old entities nil migration)))

(defn publish-built-db!
  "Replace `old`'s indexes and derived fields wholesale with ones built OUTSIDE
   the writer, and hand the result to the ordinary commit path.

   `datahike.migrate/run-index-build` builds six index trees from a dump by
   sorting it, which takes as long as the import takes. Doing that inside the
   writer's transaction loop would block every other write on this connection for
   the duration and, worse, would put a multi-minute synchronous call inside a
   `go` block. So the build happens outside and this only substitutes the
   result — the writer's own commit loop then flushes, commits and publishes it,
   which is what keeps an index-build import's durability identical to a transaction's.

   `fields` carries only what a bulk build computes: the index trees, the
   schema-derived maps, `:max-eid`, `:max-tx`, `:hash`, `:op-count`. Everything
   else — store, config, writer, meta, system entities — is `old`'s, so this
   cannot smuggle in a database from somewhere else.

   `:tx-data` is empty and this is deliberately NOT wrapped in `with-tx-pred`: a
   transaction predicate judges datoms, and an index-build import presents none for it to
   judge. A store that relies on a tx-pred as a gate should not enable `:build-indexes?`.

   Refuses a non-empty `old`, which is the same precondition
   `migrate/check-target!` enforces one level up — restated here because this is
   the function that would silently discard the data."
  [old fields]
  (log/debug :datahike/publish-built-db {:max-eid (:max-eid fields) :max-tx (:max-tx fields)})
  (when-not (zero? (count (:eavt old)))
    (throw (ex-info "publish-built-db! would discard an existing database"
                    {:error :index-build/target-not-empty})))
  (complete-db-update
   old
   {:db-before old
    :db-after  (merge old fields)
    :tx-data   []
    :tempids   {}
    :tx-meta   {:db/txInstant (dt/get-date)}}))
