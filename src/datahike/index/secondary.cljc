(ns datahike.index.secondary
  "Pluggable secondary index protocol and registry.
   Secondary indices are declared through schema and maintained in-transaction.
   Anyone can register their own index type — the planner treats all uniformly."
  ;; protocol methods -as-transient / -persistent! shadow cljs.core's names
  #?(:cljs (:refer-clojure :exclude [-as-transient -persistent!]))
  (:require [replikativ.logging :as log]
            [datahike.bitemporal.predicate :as bp.pred]
            [datahike.db.interface :as dbi]
            [datahike.index.entity-set :as es]))

;; ---------------------------------------------------------------------------
;; Protocol

(defprotocol ISecondaryIndex
  (-search [this query-spec entity-filter]
    "Execute search, optionally filtered by an entity ID set.
     query-spec is index-type-specific (e.g., text query, vector, predicate).
     entity-filter is nil or a RoaringBitmap of entity IDs to intersect with.
     Returns an EntityBitSet (IDs only) or ColumnSlice (IDs + values).")

  (-estimate [this query-spec]
    "Estimate result cardinality for the given query-spec.
     Used by the planner for cost-based optimization.
     Returns a long (estimated number of matching entities).")

  (-can-order? [this attr direction]
    "Can this index produce results already sorted by attr in direction?
     direction is :asc or :desc.")

  (-slice-ordered [this query-spec entity-filter attr direction limit]
    "Like -search but guarantees results ordered by attr in direction.
     For sort pushdown. limit may be nil (unlimited) or a positive long.")

  (-indexed-attrs [this]
    "Returns the set of attributes covered by this index.")

  (-transact [this tx-report]
    "Update index with transaction data. Called in-transaction (synchronous).
     `tx-report` carries at minimum `:datom` and `:added?`. Since
     bitemporal-v1 it also carries `:tx-meta` — a map of the current
     transaction's meta-attrs (`:db/txInstant`, `:db.valid/from`,
     `:db.valid/to`) when present. Adapters that don't need vt simply
     ignore the key; adapters that DO want vt-pushdown read it here and
     persist alongside their content keys.
     Returns updated index instance (must be persistent/immutable)."))

(defprotocol ITransientSecondaryIndex
  "Optional protocol for secondary indices that support batch-mode transients.
   When supported, the transaction loop makes the index transient once at the
   start, calls -transact! per datom (mutable, no return value), then calls
   persistent! at the end. This avoids O(n) persistent rebuilds per datom."

  (-as-transient [this]
    "Return a transient (mutable) version of this index for batch updates.")

  (-transact! [this tx-report]
    "Mutably update the transient index. Does not return a new instance.")

  (-persistent! [this]
    "Freeze the transient index back to a persistent/immutable instance."))

(defprotocol IDbContextAware
  "Optional protocol for secondary indices that need database context
   (e.g., ident-ref-map for attribute-refs mode) injected before transient use.
   Called by db-transient before -as-transient."

  (-with-db-context [this context]
    "Update index with database context map (keys: :ident-ref-map).
     Returns updated index instance with context applied."))

(defprotocol IColumnarAggregate
  "Optional protocol for secondary indices that can execute columnar aggregates
   directly on their native storage, bypassing PSS scan + column extraction."

  (-columnar-aggregate [this query-spec] [this query-spec entity-filter]
    "Execute a columnar aggregate query directly on the index's native storage.
     query-spec: {:group [col-kw ...], :agg [[op col-kw] ...],
                  :where [[op col val] ...], :having [...], :order [...], :limit n}
     entity-filter: nil or a RoaringBitmap of entity IDs to restrict aggregation to.
                    When non-nil, only rows whose :eid is in the bitmap are included.
     Returns a seq of result maps, e.g. [{:dept \"eng\" :avg 7500.0 :count 100} ...]"))

(defprotocol IVersionedSecondaryIndex
  "Optional protocol for secondary indices with durable CoW storage.
   When implemented, index state is persisted in commits, restored on connect,
   and branched/forked alongside the primary indices. Indices that do NOT
   implement this protocol are transient and rebuilt from AEVT on connect."

  (-sec-flush [this store branch]
    "Persist current index state to durable storage.
     store: konserve store (index may use its own storage internally).
     branch: current branch keyword.
     Returns an opaque key-map that can be stored in the commit and used
     by -sec-restore. Must include :type keyword for dispatch.")

  (-sec-restore [this store key-map]
    "Restore index state from a previously flushed key-map.
     Called on a skeleton instance (from create-index with nil db).
     Returns a fully populated index instance.
     store: konserve store.
     key-map: the opaque map returned by -sec-flush.")

  (-sec-branch [this store from-branch new-branch]
    "Create a CoW branch of this index.
     Returns a new index instance on the new branch.
     For scriptum: forks Lucene segments. For stratum: forks dataset.
     For proximum: forks HNSW graph.")

  (-sec-mark [this]
    "Return the set of konserve keys referenced by this index instance.
     Used by GC to mark reachable storage. Indices using external storage
     (e.g., scriptum/Lucene filesystem) return #{}."))

(defprotocol IValidTimeAware
  "Optional protocol for secondary indices that natively understand the
   tx-meta valid-time axis (`:db.valid/from` / `:db.valid/to`).

   Indices that implement this can push the `valid-at` / `valid-between`
   filter into their own query plan — for example, stratum can range-
   prune on `_valid_from` / `_valid_to` columns at scan time; a
   scriptum implementation can search per-vt-period segments.

   Indices that DON'T implement this still produce correct results —
   `search-with-vt` / `slice-ordered-with-vt` apply a generic post-hoc
   filter that drops eids whose tx-vt-supersession-winner doesn't admit
   them at the query's valid-time. The post-hoc filter is correct but
   slower than a native `-search-at-vt`; vt-aware adapters are the
   fast path."
  (-search-at-vt [this query-spec entity-filter valid-at-window]
    "Like `-search`, but restrict to entities whose tx-meta valid-time
     window contains `valid-at-window`. `valid-at-window` is either:

       a `java.util.Date`        — point-in-vt membership semantics
                                   (equivalent to `valid-at` rule).
       `[from to]` — half-open  — interval semantics (equivalent to
                                   `valid-between` rule).

     Returns the same shape as `-search` — an EntityBitSet or
     ColumnSlice."))

(defprotocol IValidTimeStable
  "Optional opt-out protocol for secondary indices whose data is
   invariant under valid-time — e.g., schema-only indices, hashes,
   content-addressed indices. When `(-vt-stable? this)` returns true,
   `search-with-vt` / `slice-ordered-with-vt` bypass the post-hoc vt
   filter entirely (the data has no vt-shadowing to compute).

   Default for indices that don't implement this protocol: NOT
   vt-stable (the safe assumption — apply the filter)."
  (-vt-stable? [this]
    "True iff this index's data is invariant under valid-time."))

(defn vt-aware?
  "True iff `index` implements `IValidTimeAware`."
  [index]
  (satisfies? IValidTimeAware index))

(defn vt-stable?
  "True iff `index` opts out of vt-filtering via `IValidTimeStable`."
  [index]
  (and (satisfies? IValidTimeStable index)
       (boolean (-vt-stable? index))))

;; ---------------------------------------------------------------------------
;; Post-hoc vt filter for non-vt-aware secondaries
;;
;; A non-vt-aware secondary returns candidates that ignore valid-time;
;; we keep only those whose entity has at least one datom visible to
;; `d/valid-at`'s supersession-aware predicate. The check uses the same
;; pred `d/valid-at` builds (cached per query call), so the algorithmic
;; cost mirrors that of routing the same query through datalog's
;; FilteredDB.
;;
;; Shape dispatch:
;;   EntityBitSet                       → new EntityBitSet, survivors only
;;   vec of {:entity-id …}              → filterv on :entity-id (preserves
;;                                         per-result columns like :score
;;                                         and :distance from -slice-ordered)
;;   nil / empty                        → returned as-is
;;
;; Other shapes pass through untouched with a warning; ColumnSlice
;; support is a follow-up once a non-vt-aware ColumnSlice-returning
;; secondary actually exists.

(defn- entity-bitset? [x]
  #?(:clj  (instance? org.roaringbitmap.RoaringBitmap x)
     :cljs (set? x)))

(defn- post-filter-vt
  "Filter a secondary's result-set to entities whose `(e, a, v)` survives
   `d/valid-at`'s supersession-aware predicate at valid-time `at`."
  [db at result]
  (let [;; Build the same pred d/valid-at would install. `mk-vt-pred`
        ;; lives in the leaf ns `datahike.bitemporal.predicate` so that
        ;; both `api.impl` and `secondary` can require it statically —
        ;; no cycle, no `resolve` foot-gun.
        vt-pred  (bp.pred/mk-vt-pred at)
        keep?    (fn [eid]
                   ;; Enumerate the entity's datoms and ask the pred.
                   ;; Any survivor → entity is visible. Caches inside
                   ;; vt-pred amortize across eids.
                   (some (fn [d] (vt-pred db d))
                         (dbi/datoms db :eavt [eid])))]
    (cond
      (nil? result) result

      (entity-bitset? result)
      (es/entity-bitset-from-longs
       (filter keep? (es/entity-bitset-seq result)))

      (sequential? result)
      (filterv (fn [m] (keep? (or (:entity-id m) (get m :entity-id)))) result)

      :else
      (do
        (log/warn :datahike/post-filter-vt-unknown-shape
                  {:type (type result)})
        result))))

(defn search-with-vt
  "Dispatch a secondary-index search with valid-time routing.

   Routing when the db carries a `:datahike/valid-at` marker
   (set by `d/valid-at`):

     1. index satisfies `IValidTimeAware`
          → `-search-at-vt` (native fast path)
     2. index satisfies `IValidTimeStable` (returns true)
          → `-search` (no filtering needed — data is vt-invariant)
     3. otherwise
          → `-search` then `post-filter-vt` (correct, slower fallback)

   No marker → `-search` directly.

   The post-filter dispatches on the result shape (EntityBitSet or
   vec-of-maps-with-:entity-id), so non-vt-aware adapters get
   correctness for free; they only need to opt into `IValidTimeAware`
   if perf matters."
  [db index query-spec entity-filter]
  (if-let [at (:datahike/valid-at (meta db))]
    (cond
      (vt-aware? index)  (-search-at-vt index query-spec entity-filter at)
      (vt-stable? index) (-search index query-spec entity-filter)
      :else              (post-filter-vt db at
                                         (-search index query-spec entity-filter)))
    (-search index query-spec entity-filter)))

(defn slice-ordered-with-vt
  "Like `search-with-vt` but for `-slice-ordered`. Same routing rules.
   Used by `:retrieval`-mode plan nodes that want both eids and a
   per-result column (score/distance)."
  [db index query-spec entity-filter attr direction limit]
  (let [unfiltered (-slice-ordered index query-spec entity-filter attr direction limit)]
    (if-let [at (:datahike/valid-at (meta db))]
      (cond
        ;; Native fast path: only if the index also implements vt-aware
        ;; slice-ordered. Stratum doesn't ship that yet (a TODO), so for
        ;; now even vt-aware indices flow through post-filter for the
        ;; -slice-ordered axis. Add `-slice-ordered-at-vt` when needed.
        (vt-stable? index) unfiltered
        :else              (post-filter-vt db at unfiltered))
      unfiltered)))

;; ---------------------------------------------------------------------------
;; GC: static key-map marking (avoids loading full index just for GC)

(defmulti mark-from-key-map
  "Given a stored key-map from -sec-flush, return the set of konserve keys
   that are reachable (for GC mark phase). Dispatches on (:type key-map).
   Avoids instantiating the full index — works directly from the stored metadata."
  (fn [key-map store] (:type key-map)))

(defmethod mark-from-key-map :default [_ _] #{})

;; ---------------------------------------------------------------------------
;; Static branch-from-key-map (for branch! without loading full index)

(defmulti branch-from-key-map
  "Given a stored key-map, create a CoW branch in the index's native storage.
   Returns a new key-map for the branched index. Dispatches on (:type key-map).
   Used by versioning/branch! which operates at the store level without connections."
  (fn [key-map store from-branch new-branch] (:type key-map)))

(defmethod branch-from-key-map :default [key-map _ _ _] key-map)

;; ---------------------------------------------------------------------------
;; Registry: index-type keyword → factory function

(defonce ^:private index-types (atom {}))

(defn register-index-type!
  "Register a secondary index type. Factory-fn takes (config, db) and returns
   an ISecondaryIndex instance. Anyone can register their own index type.

   Example:
     (register-index-type! :my-geo-index
       (fn [config db] (->MyGeoIndex config)))"
  [type-keyword factory-fn]
  (swap! index-types assoc type-keyword factory-fn))

(defn registered-types
  "Returns the set of currently registered secondary index type keywords."
  []
  (set (keys @index-types)))

(defn create-index
  "Create a secondary index instance from a registered type.
   config is the index-specific configuration map.
   db is the current database (for initial population if needed).
   Auto-requires the integration namespace if the type is namespace-qualified."
  [type-keyword config db]
  #?(:clj
     (when-not (get @index-types type-keyword)
       ;; Try auto-requiring the namespace for qualified keywords
       (when-let [ns-sym (and (qualified-keyword? type-keyword)
                              (symbol (namespace type-keyword)))]
         (try (require ns-sym)
              (catch Exception e
                (log/warn :datahike/secondary-index-require-failed {:ns ns-sym :error (.getMessage e)})))))
     :cljs nil)
  (if-let [factory (get @index-types type-keyword)]
    (factory config db)
    (throw (ex-info (str "Unknown secondary index type: " type-keyword
                         ". Registered types: " (registered-types)
                         ". Did you require the integration namespace?")
                    {:type type-keyword
                     :registered (registered-types)}))))
