(ns datahike.index.secondary
  "Pluggable secondary index protocol and registry.
   Secondary indices are declared through schema and maintained in-transaction.
   Anyone can register their own index type — the planner treats all uniformly."
  ;; protocol methods -as-transient / -persistent! shadow cljs.core's names
  #?(:cljs (:refer-clojure :exclude [-as-transient -persistent!]))
  (:require [replikativ.logging :as log]
            [hasch.core :as hasch]
            [datahike.bitemporal.predicate :as bp.pred]
            [datahike.db.interface :as dbi]
            [datahike.index.entity-set :as es]))

;; ---------------------------------------------------------------------------
;; The primary/secondary contract for `:db.secondary/only`

(defn secondary-only-hash
  "Content hash (string) of `v` — the value the PRIMARY indexes hold for a
   `:db.secondary/only` attribute, while the real value goes only to the
   secondary index.

   Here rather than in `datahike.db.transaction` because it is the term of a
   contract between the two sides, and three parties need it: the transactor
   (to project a datom on the way in), the retract handler (to find the stored
   datom), and export (to check that what a secondary index handed back is the
   value this datom actually names). Spelled in one place so those three cannot
   disagree about what the primary holds. Deterministic, so equal values dedup
   and a retraction re-hashes to the datom it retracts."
  [v]
  (str (hasch/uuid v)))

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

(defprotocol ISecondaryCandidateScan
  "Optional, paged candidate API for secondary indexes.

   `ISecondaryIndex` deliberately remains unchanged: existing adapters keep
   working and query paths may adopt this richer API incrementally.  This
   protocol is for indexes which can expose enough information for Datahike to
   decide whether a primary-index recheck is required and whether exhausting
   the scan is guaranteed to find every match.

   The result of `-candidate-page` is validated by `candidate-page` and has:

     :candidates    vector of candidate maps
     :precision     :exact or :recheck
     :recall        :complete or :approximate
     :ordering      :exact, :approximate, or :none
     :exhausted?    boolean
     :continuation  opaque token when more pages remain, nil when exhausted

   A candidate map contains at least `:entity-id` and `:attribute`.  Those two
   fields identify the primary datoms that core must inspect for an exact
   recheck.  `:value-hash` may additionally identify one value of a
   cardinality-many or `:db.secondary/only` attribute.  Adapters may attach
   result columns such as `:score` or `:distance`.

   Precision and recall are independent.  For example, a text index may return
   a complete superset (`:recheck` + `:complete`), while ANN typically has
   approximate recall even if every returned distance is subsequently
   rechecked.  Ordering describes the advertised result order, relative to the
   order requested by `query-spec`; it does not describe match precision."
  (-candidate-page [this query-spec entity-filter page-request]
    "Return one candidate page. `page-request` contains a positive `:limit`
     and optionally the opaque `:continuation` returned by the previous page."))

(def ^:private candidate-precisions #{:exact :recheck})
(def ^:private candidate-recalls #{:complete :approximate})
(def ^:private candidate-orderings #{:exact :approximate :none})

(defn candidate-scannable?
  "Whether `index` implements the optional paged candidate contract."
  [index]
  (satisfies? ISecondaryCandidateScan index))

(defn candidate-recheck-required?
  "Whether candidates must be checked against canonical primary datoms."
  [page]
  (= :recheck (:precision page)))

(defn candidate-recall-complete?
  "Whether exhausting the scan is guaranteed to enumerate every match."
  [page]
  (= :complete (:recall page)))

(defn candidate-order-exact?
  "Whether the page's advertised ordering is exact."
  [page]
  (= :exact (:ordering page)))

(defn- invalid-candidate-page!
  [message data]
  (throw (ex-info message (assoc data :type :invalid-secondary-candidate-page))))

(defn validate-candidate-page
  "Validate and normalize a page returned by `ISecondaryCandidateScan`.

   Returns the page with `:candidates` realized as a vector.  Throws a typed
   `ExceptionInfo` for malformed adapter output, so a bad index cannot silently
   turn into incomplete query results."
  [page]
  (when-not (map? page)
    (invalid-candidate-page! "A secondary candidate page must be a map."
                             {:page page}))
  (let [{:keys [candidates precision recall ordering exhausted? continuation]}
        page]
    (when-not (sequential? candidates)
      (invalid-candidate-page! "Secondary candidate :candidates must be sequential."
                               {:page page}))
    (when-not (contains? candidate-precisions precision)
      (invalid-candidate-page! "Secondary candidate :precision must be :exact or :recheck."
                               {:page page :precision precision}))
    (when-not (contains? candidate-recalls recall)
      (invalid-candidate-page! "Secondary candidate :recall must be :complete or :approximate."
                               {:page page :recall recall}))
    (when-not (contains? candidate-orderings ordering)
      (invalid-candidate-page! "Secondary candidate :ordering must be :exact, :approximate, or :none."
                               {:page page :ordering ordering}))
    (when-not (boolean? exhausted?)
      (invalid-candidate-page! "Secondary candidate :exhausted? must be boolean."
                               {:page page :exhausted? exhausted?}))
    (when (and exhausted? (some? continuation))
      (invalid-candidate-page! "An exhausted candidate page cannot have a continuation."
                               {:page page}))
    (when (and (not exhausted?) (nil? continuation))
      (invalid-candidate-page! "A non-exhausted candidate page must have a continuation."
                               {:page page}))
    (doseq [candidate candidates]
      (when-not (and (map? candidate)
                     (integer? (:entity-id candidate))
                     (keyword? (:attribute candidate)))
        (invalid-candidate-page!
         "Each secondary candidate must identify an integer :entity-id and keyword :attribute."
         {:page page :candidate candidate})))
    (assoc page :candidates (vec candidates))))

(defn- candidate-identity
  [candidate]
  (cond-> [(:entity-id candidate) (:attribute candidate)]
    (contains? candidate :value-hash) (conj (:value-hash candidate))))

(defn- invalid-candidate-scan!
  [message data]
  (throw (ex-info message (assoc data :type :invalid-secondary-candidate-scan))))

(defn validate-candidate-scan
  "Validate pages collected from one candidate scan and return them normalized.

   This is also the reusable adapter conformance assertion: Scriptum, Proximum,
   and third-party indexes can collect a small fixture scan by feeding each
   page's `:continuation` into the next request, then pass the pages here.

   A scan has stable precision, recall, and ordering declarations; every page
   except the last has a distinct continuation; the last is exhausted; and a
   candidate identity does not repeat across pages.  Identity is
   `[entity-id attribute]`, extended by `value-hash` when supplied."
  [pages]
  (when-not (seq pages)
    (invalid-candidate-scan! "A secondary candidate scan must contain at least one page."
                             {:pages pages}))
  (let [pages (mapv validate-candidate-page pages)
        declaration (select-keys (first pages) [:precision :recall :ordering])
        declarations (mapv #(select-keys % [:precision :recall :ordering]) pages)
        preceding (pop pages)
        final-page (peek pages)
        continuations (mapv :continuation preceding)
        identities (mapv candidate-identity (mapcat :candidates pages))]
    (when-not (every? #(= declaration %) declarations)
      (invalid-candidate-scan!
       "Secondary candidate precision, recall, and ordering must remain stable across pages."
       {:pages pages :declarations declarations}))
    (when (some :exhausted? preceding)
      (invalid-candidate-scan! "Only the final secondary candidate page may be exhausted."
                               {:pages pages}))
    (when-not (:exhausted? final-page)
      (invalid-candidate-scan! "The final secondary candidate page must be exhausted."
                               {:pages pages}))
    (when-not (= (count continuations) (count (distinct continuations)))
      (invalid-candidate-scan! "Secondary candidate continuations must not repeat."
                               {:pages pages :continuations continuations}))
    (when-not (= (count identities) (count (distinct identities)))
      (invalid-candidate-scan! "Secondary candidate identities must not repeat within a scan."
                               {:pages pages :candidate-identities identities}))
    pages))

(declare require-query-eligible!)

(defn candidate-page
  "Read and validate one candidate page from `index`.

   The database and index ident are mandatory so every core caller goes through
   the same lifecycle-readiness gate as existing external-engine queries.
   Direct calls to the adapter protocol are intentionally low-level."
  [db idx-ident index query-spec entity-filter page-request]
  (require-query-eligible! db idx-ident)
  (when-not (candidate-scannable? index)
    (throw (ex-info (str "Secondary index " (pr-str idx-ident)
                         " does not support candidate scans.")
                    {:type :secondary-candidate-scan-unsupported
                     :index-ident idx-ident})))
  (when-not (and (map? page-request)
                 (integer? (:limit page-request))
                 (pos? (:limit page-request)))
    (throw (ex-info "A secondary candidate page request requires a positive integer :limit."
                    {:type :invalid-secondary-candidate-request
                     :index-ident idx-ident
                     :request page-request})))
  (let [page (validate-candidate-page
              (-candidate-page index query-spec entity-filter page-request))]
    (when (> (count (:candidates page)) (:limit page-request))
      (invalid-candidate-page!
       "A secondary candidate page cannot exceed the requested :limit."
       {:page page :request page-request}))
    page))

(defprotocol ISecondaryScannable
  "Optional: enumerate what this index holds, so a BACKUP can carry it.

   Exists for `:db.secondary/only` attributes and only for them. For such an
   attribute the primary EAVT/AEVT/AVET store a content hash — `project-primary`
   replaces the value on the way in — and the full value lives ONLY in the
   secondary index. `datahike.migrate/export-db` reads the primary indexes, so
   without this protocol a dump carries the hash and the value is simply absent
   from the backup: measured, a round trip produced `hasch(hasch(v))` in the
   primary and no way to recover `v` at all.

   An index that does not implement this cannot be backed up losslessly, and
   `export-db` REFUSES rather than writing a dump it knows is incomplete.

   A POINT LOOKUP, deliberately, not a scan. An earlier version returned every
   `[eid value]` pair at once and the caller held them in a map for the duration
   of the export — which for a full-text corpus is every document resident at
   once. Everything else in the export path is bounded by `:chunk-size` and
   `:sort-buffer`; a whole-index map beside them would have been the one
   unbounded term, and `:db.secondary/only` exists precisely for values too big
   to want in the primary index. One value at a time is the same bound the
   record stream already has.

   The cost is a random read per datom of such an attribute. That is the right
   trade here — these attributes are opt-in and rarely the bulk of a database —
   and a backend free to cache internally still can. It also happens to be the
   only shape proximum can serve: it fetches a vector BY external id and cannot
   list its ids."
  (-sec-value [this attr eid]
    "The full value this index holds for `[attr eid]`, or nil."))

(defprotocol ISecondaryHashAddressable
  "OPTIONAL, and the answer to a question `ISecondaryScannable` cannot ask.

   `-sec-value` is keyed on `[attr eid]`, and that key does not identify a
   VALUE. Two shapes break it, both measured:

     * `:db.cardinality/many` — one entity, several values, one key. Export
       asked once per datom and got the same arbitrary value each time: a dump
       held `\"alpha\"` twice and `\"beta\"` not at all.
     * `{:history? true}` over an overwritten attribute — the index holds only
       the CURRENT value, so every historical assertion was written with
       today's value. `\"version-one\"` appeared nowhere in the backup.

   An index that stores its values individually and can find one by its content
   hash implements this and both shapes resolve exactly: the hash is what the
   PRIMARY datom carries, so the caller always knows which value it means.

   Implementing this is a claim about STORAGE, not about lookup. It says values
   are kept one per datom rather than one per entity — which is why declaring it
   is also what lets a `:db.secondary/only` attribute be cardinality-many at
   all. Of the three indices shipped beside datahike, only scriptum's shape
   admits it (one Lucene document per datom); stratum is columnar with one cell
   per `[eid column]` and proximum is keyed by external id, so for those a
   second value overwrites the first at WRITE time, and no read protocol can
   recover it.

   Returning nil is a legitimate answer — the value is not (or is no longer)
   held — and export REFUSES rather than writing a dump it knows is wrong."
  (-sec-value-by-hash [this attr eid hash]
    "The value whose `secondary-only-hash` is `hash`, for `[attr eid]`, or nil."))

(defn hash-addressable?
  "Whether `idx` can resolve a value by its content hash. One predicate so the
   export path and the schema check agree about what an index can do."
  [idx]
  (satisfies? ISecondaryHashAddressable idx))

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

(defn query-eligible?
  "Whether the secondary index identified by `idx-ident` may answer queries.

   `nil` remains eligible for legacy databases and hand-assembled indexes that
   predate lifecycle status. A declared lifecycle state is fail-closed: only
   `:ready` may answer. Keeping this rule here gives aggregate, full-text,
   vector, and future query paths one correctness gate."
  [db idx-ident]
  (contains? #{nil :ready}
             (get-in db [:schema idx-ident :db.secondary/status])))

(defn require-query-eligible!
  "Raise a typed error when an explicitly requested secondary index is not
   queryable. Optimizations with an exact primary-index fallback should use
   `query-eligible?` and decline instead."
  [db idx-ident]
  (let [status (get-in db [:schema idx-ident :db.secondary/status])]
    (when-not (query-eligible? db idx-ident)
      (throw (ex-info (str "Secondary index " (pr-str idx-ident)
                           " cannot answer queries while its status is "
                           (pr-str status) ".")
                      {:type :secondary-index-unavailable
                       :index-ident idx-ident
                       :status status})))))

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

(defprotocol ISecondaryWarmable
  "EXPERIMENTAL. Optional protocol for secondary indices whose storage can be
   prefetched ahead of demand. A SEPARATE protocol rather than a method on
   `IVersionedSecondaryIndex`, deliberately: adding a method to a protocol
   breaks every existing implementer at call time, and warmth is orthogonal to
   versioning anyway — an index that does not implement this is simply skipped
   by `datahike.api/warm-db`'s `:secondary` pass (a transient index rebuilt
   from AEVT is already in memory and has nothing to warm).

   BUDGETS ARE IN THE INDEX'S OWN UNITS and deliberately do not translate:
   stratum counts tree nodes, scriptum counts Lucene segment files, proximum
   counts tree nodes over what its eager restore already loaded. One number
   spanning those would mean nothing. What IS shared is the report envelope —
   at least {:fetched :ms :budget-exhausted?} — so one caller can log one
   decay metric across every index family."

  (-sec-warm! [this opts]
    "Prefetch this index's storage. `opts` is index-family-specific but every
     family accepts `:budget` (a hard ceiling in its own units). Returns the
     warm-report envelope; synchronous."))

(defn sec-warm!
  "EXPERIMENTAL. `-sec-warm!` if `idx` implements it, a zero envelope marked
   `:unsupported?` otherwise — so `warm-db`'s `:secondary` pass reports every
   index it was asked about rather than silently skipping the ones that cannot
   answer."
  [idx opts]
  (if (satisfies? ISecondaryWarmable idx)
    (-sec-warm! idx opts)
    {:fetched 0 :ms 0.0 :budget-exhausted? false :unsupported? true}))

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
   Auto-requires the integration namespace if the type is namespace-qualified.

   A factory invoked for an asynchronous backfill also receives the ephemeral
   keys `::index-ident` and `::build-attempt`. An adapter with external mutable
   storage must use `::build-attempt` to create a private, empty generation;
   reopening a path left by an earlier crashed attempt can duplicate replayed
   datoms. These keys are runtime context and are not stored in schema."
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
