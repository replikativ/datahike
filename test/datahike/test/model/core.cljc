(ns datahike.test.model.core
  "Pure model of Datahike semantics for model-based testing.
   
   This namespace implements a pure Clojure model of how Datahike
   should behave, without any side effects. It can be used to:
   
   1. Generate valid sequences of transactions
   2. Track expected state after each transaction
   3. Compare expected state against actual Datahike state
   
   The model handles:
   - Cardinality semantics (one vs many)
   - Type constraints (string, long, ref)
   - Historical state tracking (for as-of queries)
   - Index content tracking (EAVT, AVET, AEVT)"
  (:require [datahike.test.model.rng :as rng]))

;; =============================================================================
;; Pattern Matching (for remove-by-pattern)
;; =============================================================================

(defn- matches
  "Helper that treats nil as wildcard."
  [x0 x]
  (or (nil? x0) (= x0 x)))

(defn predicate-from-pattern
  "Returns a predicate function for a pattern.
   Nil in any position matches anything."
  [[e0 a0 v0]]
  (fn [[e a v]]
    (and (matches e0 e)
         (matches a0 a)
         (matches v0 v))))

(defn remove-pattern
  "Filter a set of datoms by removing those matching pattern."
  [datom-set pattern]
  (into #{} (remove (predicate-from-pattern pattern)) datom-set))

;; =============================================================================
;; Model State
;; =============================================================================

(defrecord Model
           [;; Schema: {attr-ident {:db/valueType, :db/cardinality, :db/index, ...}}
            schema
            ;; Current datoms: #{[e a v]}
            current-datoms
            ;; Historical states: {tx #{[e a v]}}
            ;; Maps transaction ID to state at that transaction
            history
            ;; Transaction counter
            next-tx])

(defn create-model
  "Create initial model state with given schema."
  [schema]
  (->Model schema #{} {} 536870912))

;; =============================================================================
;; Core Semantics: Accumulate Datom
;; =============================================================================

(defn lookup-attribute
  "Look up attribute info from schema."
  [schema a]
  (or (get schema a)
      (throw (ex-info (str "Attribute not found: " a) {:attribute a}))))

(defn accumulate-datom
  "Apply a single datom to the model state.
   
   This is the core of the model - it implements Datahike's
   cardinality semantics in pure Clojure:
   
   - :db.cardinality/one: Replaces existing value for [e a]
   - :db.cardinality/many: Accumulates values, no duplicates
   
   Returns updated datom set."
  [schema datom-set [e a v added]]
  (if added
    (let [attr (lookup-attribute schema a)
          cardinality (or (:db/cardinality attr) :db.cardinality/one)]
      (case cardinality
        :db.cardinality/many
        (if (some (predicate-from-pattern [e a v]) datom-set)
          datom-set
          (conj datom-set [e a v]))
        :db.cardinality/one
        (-> datom-set
            (remove-pattern [e a nil])
            (conj [e a v]))))
    (remove-pattern datom-set [e a v])))

(defn apply-tx
  "Apply a transaction to the model, returning updated model.
   
   Transaction format: {:tx-data [[op e a v] ...]}
   where op is :db/add or :db/retract.
   
   Also records historical state for as-of queries."
  [model {:keys [tx-data] :as _tx}]
  (let [tx-id (:next-tx model)
        ;; For accumulate-datom, we need [e a v added] (4 elements)
        datoms-for-accumulate (mapv (fn [[op e a v]]
                                      [e a v (= op :db/add)])
                                    tx-data)
        new-current (reduce
                     (fn [acc datom]
                       (accumulate-datom (:schema model) acc datom))
                     (:current-datoms model)
                     datoms-for-accumulate)
        new-history (assoc (:history model) tx-id new-current)]
    (assoc model
           :current-datoms new-current
           :history new-history
           :next-tx (inc tx-id))))

(defn apply-txs
  "Apply multiple transactions sequentially."
  [model txs]
  (reduce apply-tx model txs))

;; =============================================================================
;; Querying the Model
;; =============================================================================

(defn get-current-datoms
  "Get current datoms as a set of [e a v] tuples."
  [model]
  (:current-datoms model))

(defn get-datoms-at-tx
  "Get datoms as of a specific transaction."
  [model tx-id]
  (get (:history model) tx-id))

(defn get-transaction-ids
  "Get all transaction IDs in order."
  [model]
  (sort (keys (:history model))))

;; =============================================================================
;; Index Computation
;; =============================================================================

(defn compute-eavt
  "Compute EAVT index from model (sorted by [e a v])."
  [model]
  (sort-by identity (get-current-datoms model)))

(defn compute-aevt
  "Compute AEVT index from model (sorted by [a e v])."
  [model]
  (sort-by (fn [[e a v]] [a e v]) (get-current-datoms model)))

(defn indexed-attrs
  "Get set of indexed attribute identifiers from schema."
  [schema]
  (set (keep (fn [[k v]] (when (:db/index v) k)) schema)))

(defn ref-attrs
  "Get set of ref-type attribute identifiers from schema."
  [schema]
  (set (keep (fn [[k v]] (when (= :db.type/ref (:db/valueType v)) k)) schema)))

(defn compute-avet
  "Compute AVET index from model.
   
   Only includes datoms where:
   - Attribute has :db/index true, OR
   - Attribute is :db.type/ref"
  [model]
  (let [schema (:schema model)
        indexed (indexed-attrs schema)
        refs (ref-attrs schema)
        indexed-or-ref? (fn [a]
                          (or (contains? indexed a)
                              (contains? refs a)))]
    (sort-by (fn [[e a v]] [a v e])
             (filter (fn [[_e a _v]] (indexed-or-ref? a))
                     (get-current-datoms model)))))

;; =============================================================================
;; Transaction Generation
;; =============================================================================

(defn generate-tx-op
  "Generate a single transaction operation.
   
   Returns [op e a v] where op is :db/add or :db/retract."
  [rng schema entity-range]
  (let [attr-idents (vec (keys schema))
        attr-id (nth attr-idents (rng/next-int rng (count attr-idents)))
        attr (get schema attr-id)
        value-type (:db/valueType attr)
        op (if (rng/next-boolean rng) :db/add :db/retract)
        e (nth entity-range (rng/next-int rng (count entity-range)))
        v (case value-type
            :db.type/string (str "v" (rng/next-int rng 100))
            :db.type/long (long (rng/next-int rng 100))
            :db.type/ref (nth entity-range (rng/next-int rng (count entity-range)))
            :db.type/boolean (rng/next-boolean rng)
            :db.type/keyword (keyword (str "k" (rng/next-int rng 20)))
            (rng/next-long rng))]
    [op e attr-id v]))

(defn generate-tx-batch
  "Generate a batch of transaction operations."
  [rng schema entity-range max-ops]
  (let [n (inc (rng/next-int rng max-ops))]
    (vec (repeatedly n #(generate-tx-op rng schema entity-range)))))
