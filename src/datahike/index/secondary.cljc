(ns datahike.index.secondary
  "Pluggable secondary index protocol and registry.
   Secondary indices are declared through schema and maintained in-transaction.
   Anyone can register their own index type — the planner treats all uniformly."
  (:require [replikativ.logging :as log]))

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
     tx-report contains at minimum :datom and :added? keys.
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
