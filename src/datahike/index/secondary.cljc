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
