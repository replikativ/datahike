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
;; Transaction Generation - Weighted Action Sampling
;; =============================================================================

(def action-weights
  "Weights for each action type in transaction generation.
   :add      - Add entirely new random datom
   :remove   - Retract an existing datom
   :tweak    - Take existing datom, generate new value for same entity/attribute
   :combine  - Take two existing datoms, mix their entity/value parts
   :transact - End current transaction (emit boundary)"
  {:add 4
   :remove 2
   :tweak 1
   :combine 1
   :transact 1})

(defn- generate-value
  "Generate a random value for the given attribute type."
  [rng value-type entity-range]
  (case value-type
    :db.type/string (str "v" (rng/next-int rng 100))
    :db.type/long (long (rng/next-int rng 100))
    :db.type/ref (rng/rand-nth-rng rng entity-range)
    :db.type/boolean (rng/next-boolean rng)
    :db.type/keyword (keyword (str "k" (rng/next-int rng 20)))
    (rng/next-long rng)))

(defn- generate-add
  "Generate a random :db/add operation."
  [rng schema entity-range]
  (let [attr-idents (vec (keys schema))
        attr-id (rng/rand-nth-rng rng attr-idents)
        attr (get schema attr-id)
        value-type (:db/valueType attr)
        e (rng/rand-nth-rng rng entity-range)
        v (generate-value rng value-type entity-range)]
    [:db/add e attr-id v]))

(defn- generate-remove
  "Generate a :db/retract operation from existing datoms."
  [rng current-datoms]
  (let [[e a v] (rng/rand-nth-rng rng (vec current-datoms))]
    [:db/retract e a v]))

(defn- generate-tweak
  "Generate a :db/add that modifies an existing datom with a new value.
   Exercises cardinality-one replacement and upsert semantics."
  [rng schema entity-range current-datoms]
  (let [[e a _old-v] (rng/rand-nth-rng rng (vec current-datoms))
        attr (get schema a)
        value-type (:db/valueType attr)
        new-v (generate-value rng value-type entity-range)]
    [:db/add e a new-v]))

(defn- generate-combine
  "Generate a :db/add by mixing entity or value from two datoms with same attribute.
   Creates interesting patterns while preserving type safety.
   Returns nil if no valid combination exists."
  [rng current-datoms]
  (when (seq current-datoms)
    (let [by-attr (group-by second current-datoms)
          valid-groups (filter #(<= 2 (count (second %))) by-attr)]
      (when (seq valid-groups)
        (let [[_ datoms-with-same-attr] (rng/rand-nth-rng rng (vec valid-groups))
              [e1 a _v1] (rng/rand-nth-rng rng (vec datoms-with-same-attr))
              [e2 _a v2] (rng/rand-nth-rng rng (vec datoms-with-same-attr))
              idx (rng/next-int rng 2)
              e (case idx 0 e1 1 e2)
              v (case idx 0 v2 1 v2)]
          [:db/add e a v])))))

(defn- sample-action
  "Sample an action type based on weights and current state.
   When no datoms exist, only :add or :transact are valid."
  [rng current-datoms]
  (if (seq current-datoms)
    (rng/weighted-sample-rng rng action-weights)
    (rng/weighted-sample-rng rng {:add 4 :transact 1})))

(defn- datom-equivalence
  "Returns the equivalence key for a datom.
   Two datoms are equivalent if they would conflict in the same transaction.
   For cardinality-one: same [e a]
   For cardinality-many: same [e a v]"
  [schema [e a v]]
  (let [attr (get schema a)
        cardinality (or (:db/cardinality attr) :db.cardinality/one)]
    (case cardinality
      :db.cardinality/many [e a v]
      :db.cardinality/one [e a])))

(defn- op-equivalence
  "Returns the equivalence key for an operation [op e a v].
   Ignores the op type - add and retract of same datom are equivalent."
  [schema [_op e a v]]
  (datom-equivalence schema [e a v]))

(defn generate-transaction
  "Generate a single transaction using weighted action sampling.
   
   Returns tx-ops: a vector of [op e a v] operations.
   
   The transaction ends when:
   - :transact action is sampled (probabilistic boundary)
   - max-ops limit is reached
   
   Uses an internal datom set to track state during generation, so operations
   can reference datoms from previous operations within the same transaction.
   Does NOT update model state - caller should apply the transaction.
   
   Avoids generating conflicting operations within the same transaction
   (e.g., add and retract of same datom, or two adds of same cardinality-one
   attribute with same entity but different values)."
  [rng schema entity-range model-state max-ops]
  (let [initial-datoms (get-current-datoms model-state)]
    (letfn [(accumulate [datoms [e a v added?]]
              (if added?
                (let [attr (lookup-attribute schema a)
                      cardinality (or (:db/cardinality attr) :db.cardinality/one)]
                  (case cardinality
                    :db.cardinality/many
                    (if (some (predicate-from-pattern [e a v]) datoms)
                      datoms
                      (conj datoms [e a v]))
                    :db.cardinality/one
                    (-> datoms
                        (remove-pattern [e a nil])
                        (conj [e a v]))))
                (remove-pattern datoms [e a v])))

            (apply-op-to-datoms [datoms [op e a v]]
              (accumulate datoms [e a v (= op :db/add)]))

            (equiv-key [op]
              (op-equivalence schema op))

            (step [ops current-datoms current-equiv n]
              (if (>= n max-ops)
                (vec ops)
                (let [action (sample-action rng current-datoms)]
                  (case action
                    :transact
                    (vec ops)

                    :add
                    (let [op (generate-add rng schema entity-range)
                          equiv (equiv-key op)]
                      (if (contains? current-equiv equiv)
                        ;; Skip conflicting op, try again with same n
                        (recur ops current-datoms current-equiv n)
                        ;; Accept op
                        (let [current-datoms' (apply-op-to-datoms current-datoms op)]
                          (recur (conj ops op) current-datoms' (conj current-equiv equiv) (inc n)))))

                    :remove
                    (let [op (generate-remove rng current-datoms)
                          equiv (equiv-key op)]
                      (if (contains? current-equiv equiv)
                        (recur ops current-datoms current-equiv n)
                        (let [current-datoms' (apply-op-to-datoms current-datoms op)]
                          (recur (conj ops op) current-datoms' (conj current-equiv equiv) (inc n)))))

                    :tweak
                    (let [op (generate-tweak rng schema entity-range current-datoms)
                          equiv (equiv-key op)]
                      (if (contains? current-equiv equiv)
                        (recur ops current-datoms current-equiv n)
                        (let [current-datoms' (apply-op-to-datoms current-datoms op)]
                          (recur (conj ops op) current-datoms' (conj current-equiv equiv) (inc n)))))

                    :combine
                    (if-let [op (generate-combine rng current-datoms)]
                      (let [equiv (equiv-key op)]
                        (if (contains? current-equiv equiv)
                          (recur ops current-datoms current-equiv n)
                          (let [current-datoms' (apply-op-to-datoms current-datoms op)]
                            (recur (conj ops op) current-datoms' (conj current-equiv equiv) (inc n)))))
                      ;; combine failed, skip this iteration
                      (recur ops current-datoms current-equiv (inc n)))))))]
      (step [] initial-datoms #{} 0))))

(defn generate-tx-batch
  "Generate a batch of transaction operations.
   
   Deprecated: Use generate-transaction for weighted action sampling.
   Kept for backward compatibility."
  ([rng schema entity-range max-ops]
   (generate-tx-batch rng schema entity-range max-ops nil))
  ([rng schema entity-range max-ops model-state]
   (let [n (inc (rng/next-int rng max-ops))]
     (vec (repeatedly n #(generate-add rng schema entity-range))))))
