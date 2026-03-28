(ns datahike.index.secondary.stratum
  "Stratum (columnar analytics) integration with Datahike.

   Two integration paths:

   1. **Secondary index** (ISecondaryIndex): Stratum maintains a columnar copy
      of configured attributes, updated in-transaction. The query planner can
      push aggregates directly to the native columnar storage, bypassing PSS.

      Schema declaration:
        {:idx/analytics {:db.secondary/type :stratum
                         :db.secondary/attrs [:person/dept :person/salary]
                         :db.secondary/config {}}}

   2. **External engine functions** (aggregate, window, columnar-sort): Called
      from WHERE clauses via :datahike/external-engine metadata. These receive
      the current relation as input and delegate to Stratum.

   The secondary index path is preferred when available — it avoids the
   PSS scan + column extraction overhead entirely."
  (:require
   [datahike.index.secondary :as sec]
   [datahike.index.entity-set :as es]
   [datahike.db.interface :as dbi]
   [stratum.api :as st]
   [stratum.dataset :as sd]
   [stratum.storage :as ss])
  (:import
   [datahike.datom Datom]))

;; ---------------------------------------------------------------------------
;; Relation conversion helpers

(defn relation->columns
  "Convert a seq of tuples + column names to Stratum column map.
   Uses Stratum's tuples->columns which auto-detects types."
  [tuples col-names]
  (when (seq tuples)
    (st/tuples->columns (vec tuples) col-names)))

(defn columns->relation
  "Convert Stratum result (vector of maps) to tuples with given column order."
  [result-maps col-names]
  (mapv (fn [m]
          (mapv #(get m %) col-names))
        result-maps))

;; ---------------------------------------------------------------------------
;; Aggregate function (external engine — WHERE clause)

(defn aggregate
  "Run a Stratum aggregate query on the current relation context.

   query-spec is a map with Stratum query keys:
     :group  - columns to group by (keywords matching bound var names)
     :agg    - aggregation specs, e.g. [[:avg :salary] [:count] [:sum :revenue]]
     :where  - optional post-filter predicates on input columns
     :having - optional post-aggregation filter
     :order  - optional sort spec [[:col :asc] ...]
     :limit  - optional result limit

   This function is NOT called directly during compiled query execution.
   The planner sees :datahike/external-engine metadata and generates a plan op.
   For legacy/fallback execution, it IS called with the input relation."
  {:datahike/external-engine
   {:binding-columns :dynamic         ;; determined by :group + :agg at plan time
    :accepts-entity-filter? false     ;; operates on values, not entity IDs
    :input-vars :all-bound            ;; needs all referenced columns bound
    :cost-model (fn [_db _idx-ident args _n-cols]
                  ;; Stratum aggregation is fast but needs materialization
                  (let [spec (first args)]
                    {:estimated-card (max 1 (count (:group spec)))
                     :cost-per-result 0.0001}))}}
  [query-spec input-tuples input-col-names]
  (let [columns (relation->columns input-tuples input-col-names)
        stratum-query (merge {:from columns} (select-keys query-spec [:group :agg :where :having :order :limit]))]
    (st/q stratum-query)))

;; ---------------------------------------------------------------------------
;; Window function

(defn window
  "Run a Stratum window function on the current relation context.

   query-spec:
     :window - window specs, e.g. [{:op :row-number :as :rn :partition [:dept]
                                     :order [[:salary :desc]]}]
     :order  - optional global sort
     :limit  - optional limit

   Returns tuples with all input columns plus window function results."
  {:datahike/external-engine
   {:binding-columns :dynamic
    :accepts-entity-filter? false
    :input-vars :all-bound
    :cost-model (fn [_db _idx-ident args _n-cols]
                  {:estimated-card 1000  ;; windows don't reduce cardinality
                   :cost-per-result 0.0001})}}
  [query-spec input-tuples input-col-names]
  (let [columns (relation->columns input-tuples input-col-names)
        stratum-query (merge {:from columns} (select-keys query-spec [:window :order :limit :select]))]
    (st/q stratum-query)))

;; ---------------------------------------------------------------------------
;; Sort function (pushdown ORDER BY to Stratum SIMD sort)

(defn columnar-sort
  "Sort the current relation using Stratum's SIMD-accelerated sort.
   Useful when ORDER BY on large result sets where Stratum's radix sort
   outperforms Clojure's TimSort.

   query-spec:
     :order - sort spec [[:col :asc] [:col :desc] ...]
     :limit - optional top-N (Stratum uses heap selection)"
  {:datahike/external-engine
   {:binding-columns :passthrough     ;; same columns in, same columns out
    :accepts-entity-filter? false
    :input-vars :all-bound
    :cost-model (fn [_db _idx-ident _args _n-cols]
                  {:estimated-card 1000
                   :cost-per-result 0.00001})}}
  [query-spec input-tuples input-col-names]
  (let [columns (relation->columns input-tuples input-col-names)
        stratum-query (merge {:from columns} (select-keys query-spec [:order :limit :offset]))]
    (st/q stratum-query)))

;; ---------------------------------------------------------------------------
;; Columnar aggregate adapter for execute-columnar-aggregate
;;
;; Called from the query engine when :find has aggregates and stratum is available.
;; Receives pre-extracted typed column arrays — no tuple materialization needed.

(def stratum-agg-ops
  "Map from datahike built-in aggregate symbols to stratum agg op keywords.
   min/max excluded: stratum returns doubles, but datahike must preserve
   input type (Long min → Long result, not Double)."
  {:avg :avg, :sum :sum, :count :count,
   :variance :variance, :stddev :stddev, :count-distinct :count-distinct,
   :median :median})

(defn stratum-compatible-aggs?
  "Check if all aggregate specs can be handled by stratum."
  [agg-specs]
  (every? (fn [spec]
            (contains? stratum-agg-ops (first spec)))
          agg-specs))

(defn columnar-aggregate
  "Run stratum group-by + aggregate on pre-extracted typed column arrays.
   column-map: {col-keyword typed-array} — e.g. {:dept String[], :salary long[]}
   group-keys: [:dept] — columns to group by
   agg-specs:  [[:avg :salary] [:count]] — aggregate operations

   Returns a seq of result tuples (vectors) matching the find-element order:
   [group-val1 ... agg-result1 agg-result2 ...]"
  [column-map group-keys agg-specs find-elements]
  (let [stratum-aggs (mapv (fn [spec]
                             (let [op (get stratum-agg-ops (first spec))]
                               (if (> (count spec) 1)
                                 [op (second spec)]
                                 [op])))
                           agg-specs)
        stratum-query (cond-> {:from column-map
                               :agg stratum-aggs}
                        (seq group-keys) (assoc :group group-keys))
        result-maps (st/q stratum-query)

        agg-result-keys (mapv (fn [spec stratum-spec]
                                (first stratum-spec))
                              agg-specs stratum-aggs)]
    (mapv (fn [result-map]
            (let [tuple (object-array (count find-elements))
                  gi (volatile! 0)
                  ai (volatile! 0)]
              (dotimes [fi (count find-elements)]
                (let [fe (nth find-elements fi)]
                  (if (instance? datalog.parser.type.Aggregate fe)
                    (let [spec (nth agg-specs @ai)
                          op-key (nth agg-result-keys @ai)
                          col-key (when (> (count spec) 1) (second spec))
                          v (or (get result-map op-key)
                                (when col-key
                                  (get result-map (keyword (str (name op-key) "_" (name col-key))))))]
                      (aset tuple fi v)
                      (vswap! ai unchecked-inc))
                    ;; Group-by variable
                    (let [col-key (nth group-keys @gi)]
                      (aset tuple fi (get result-map col-key))
                      (vswap! gi unchecked-inc)))))
              (clojure.lang.PersistentVector/adopt tuple)))
          result-maps)))

(defn columnar-aggregate-from-maps
  "Convert stratum result maps to tuples matching find-element order.
   Used when stratum secondary index answers aggregates directly."
  [result-maps group-keys stratum-aggs find-elements]
  (let [agg-result-keys (mapv first stratum-aggs)]
    (mapv (fn [result-map]
            (let [tuple (object-array (count find-elements))
                  gi (volatile! 0)
                  ai (volatile! 0)]
              (dotimes [fi (count find-elements)]
                (let [fe (nth find-elements fi)]
                  (if (instance? datalog.parser.type.Aggregate fe)
                    (let [op-key (nth agg-result-keys @ai)
                          stratum-spec (nth stratum-aggs @ai)
                          col-key (when (> (count stratum-spec) 1) (second stratum-spec))
                          v (or (get result-map op-key)
                                (when col-key
                                  (get result-map (keyword (str (name op-key) "_" (name col-key))))))]
                      (aset tuple fi v)
                      (vswap! ai unchecked-inc))
                    ;; Group-by variable
                    (let [col-key (nth group-keys @gi)]
                      (aset tuple fi (get result-map col-key))
                      (vswap! gi unchecked-inc)))))
              (clojure.lang.PersistentVector/adopt tuple)))
          result-maps)))

;; ---------------------------------------------------------------------------
;; ISecondaryIndex implementation
;;
;; Maintains a StratumDataset with :eid column + attribute value columns.
;; Updated in-transaction via ITransientSecondaryIndex (batch append!).

(defn attr-col-key
  "Convert a datahike attribute ident to a stratum column keyword.
   Keywords pass through; numbers get prefixed with 'a'."
  [a-ident]
  (if (keyword? a-ident)
    (keyword (name a-ident))
    (keyword (str "a" a-ident))))

(defn- value-type->col-type
  "Map datahike value types to stratum column types for initial dataset creation."
  [value-type]
  (case value-type
    (:db.type/long :db.type/ref) :long
    (:db.type/double :db.type/float) :double
    (:db.type/string :db.type/keyword) :string
    ;; Default to object/string for other types
    :string))

(defn- make-empty-column
  "Create an empty typed array for the given datahike value type."
  [value-type]
  (case value-type
    (:db.type/double :db.type/float) (double-array 0)
    (:db.type/string :db.type/keyword) (into-array String (make-array String 0))
    ;; Default: empty long array — persist-transient-stratum-index will determine
    ;; actual type from values via col-types pre-scan on first insert
    (long-array 0)))

(defn- build-initial-dataset
  "Build a StratumDataset from the DB's existing data for configured attributes.
   Scans AEVT per attribute to collect entity+value pairs, then joins by entity.
   When db is nil (during empty-db creation), returns an empty dataset."
  [db attrs config]
  (if (nil? db)
    ;; No DB yet — return empty dataset with typed columns
    (let [col-map (into {:eid (long-array 0)}
                        (map (fn [a] [(attr-col-key a) (long-array 0)]))
                        attrs)]
      (sd/ensure-indexed (st/make-dataset col-map)))
    (let [;; For each attr, collect {eid value} via AEVT datoms
          attr->eid-vals
          (into {}
                (map (fn [a]
                       (let [pairs (java.util.LinkedHashMap.)]
                         (doseq [^Datom d (dbi/datoms db :aevt [a])]
                           (.put pairs (long (.-e d)) (.-v d)))
                         [a pairs])))
                attrs)
          ;; Collect all entity IDs that appear in ANY attribute
          all-eids (java.util.TreeSet.)
          _ (doseq [[_a ^java.util.LinkedHashMap pairs] attr->eid-vals]
              (.addAll all-eids (.keySet pairs)))
          n (.size all-eids)]
      (if (zero? n)
      ;; Empty dataset — use long-array 0 for all columns since we don't know
      ;; actual value types yet. persist-transient-stratum-index will determine
      ;; real types from values via col-types pre-scan on first insert.
        (let [col-map (into {:eid (long-array 0)}
                            (map (fn [a] [(attr-col-key a) (long-array 0)]))
                            attrs)]
          (sd/ensure-indexed (st/make-dataset col-map)))
      ;; Build column arrays
        (let [entity-ids (long-array n)
              _ (let [i (volatile! 0)]
                  (doseq [^Long eid all-eids]
                    (aset entity-ids @i (long eid))
                    (vswap! i unchecked-inc)))
            ;; Build eid→row-index map for O(1) lookup
              eid->row (java.util.HashMap. (* n 2))
              _ (dotimes [i n] (.put eid->row (aget entity-ids i) (int i)))
              attr-arrays
              (into {}
                    (map (fn [a]
                           (let [col-key (attr-col-key a)
                                 schema (get (:schema db) a)
                                 vtype (or (:db/valueType schema) :db.type/string)
                                 ^java.util.LinkedHashMap pairs (get attr->eid-vals a)]
                             [col-key
                              (case vtype
                                (:db.type/long :db.type/ref)
                                (let [arr (long-array n)]
                                  (doseq [^java.util.Map$Entry e (.entrySet pairs)]
                                    (let [row (int (.get eid->row (.getKey e)))]
                                      (aset arr row (long (.getValue e)))))
                                  arr)

                                (:db.type/double :db.type/float)
                                (let [arr (double-array n)]
                                  (doseq [^java.util.Map$Entry e (.entrySet pairs)]
                                    (let [row (int (.get eid->row (.getKey e)))]
                                      (aset arr row (double (.getValue e)))))
                                  arr)

                              ;; String/keyword/other
                                (let [arr ^"[Ljava.lang.String;" (make-array String n)]
                                  (doseq [^java.util.Map$Entry e (.entrySet pairs)]
                                    (let [row (int (.get eid->row (.getKey e)))]
                                      (aset arr row (str (.getValue e)))))
                                  arr))])))
                    attrs)
              col-map (assoc attr-arrays :eid entity-ids)]
          (sd/ensure-indexed (st/make-dataset col-map)))))))

;; ---------------------------------------------------------------------------
;; Persistent stratum index (declared first — TransientStratumIndex references it)

(declare make-transient-stratum-index)
(declare persist-transient-stratum-index)

(deftype StratumIndex [dataset    ;; StratumDataset or nil
                       attrs      ;; set of datahike attribute idents being indexed
                       attr-refs  ;; set of numeric refs (for attr-refs mode) or nil
                       config]    ;; user config map

  sec/ISecondaryIndex
  (-search [_ query-spec entity-filter]
    ;; query-spec: {:where [[op col val] ...]}
    ;; Returns EntityBitSet of matching entity IDs
    (if (nil? dataset)
      (es/entity-bitset)
      (let [result-maps (st/q (cond-> {:from dataset :select [:eid]}
                                (:where query-spec) (assoc :where (:where query-spec))))
            bs (es/entity-bitset)]
        (doseq [m result-maps]
          (es/entity-bitset-add! bs (long (:eid m))))
        (if entity-filter
          (es/entity-bitset-and bs entity-filter)
          bs))))

  (-estimate [_ query-spec]
    (if (nil? dataset)
      0
      (st/row-count dataset)))

  (-can-order? [_ _attr direction]
    true)

  (-slice-ordered [_ query-spec entity-filter attr direction limit]
    (if (nil? dataset)
      []
      (let [col-key (attr-col-key attr)
            result-maps (st/q (cond-> {:from dataset
                                       :select [:eid col-key]
                                       :order [[col-key direction]]}
                                limit (assoc :limit limit)
                                (:where query-spec) (assoc :where (:where query-spec))))]
        (cond->> (mapv (fn [m] {:entity-id (long (:eid m)) :value (get m col-key)})
                       result-maps)
          entity-filter (filterv (fn [{:keys [entity-id]}]
                                   (es/entity-bitset-contains? entity-filter entity-id)))))))

  (-indexed-attrs [_] attrs)

  (-transact [this tx-report]
    (let [t (sec/-as-transient this)]
      (sec/-transact! t tx-report)
      (sec/-persistent! t)))

  sec/ITransientSecondaryIndex
  (-as-transient [this]
    (make-transient-stratum-index dataset attrs attr-refs config))

  (-transact! [_ _tx-report]
    (throw (IllegalStateException. "Cannot -transact! on persistent StratumIndex. Call (-as-transient) first to create a mutable batch version.")))

  (-persistent! [this] this)

  sec/IDbContextAware
  (-with-db-context [this context]
    (let [irm (:ident-ref-map context)
          new-attr-refs (when (seq irm)
                          (not-empty (set (keep irm attrs))))
          new-config (if (seq irm)
                       (assoc config :ident-ref-map irm)
                       config)]
      (if (and (= new-attr-refs attr-refs) (= new-config config))
        this
        (StratumIndex. dataset attrs new-attr-refs new-config))))

  sec/IVersionedSecondaryIndex
  (-sec-flush [_ store branch]
    ;; Persist dataset to konserve via stratum's sync!
    (if dataset
      (let [synced-ds (sd/sync! dataset store (name branch))]
        {:type :stratum
         :branch (name branch)
         :dataset-commit-id (get-in synced-ds [:commit-info :id])})
      {:type :stratum :branch (name branch) :dataset-commit-id nil}))

  (-sec-restore [_ store key-map]
    ;; Restore dataset from konserve
    (if-let [commit-id (:dataset-commit-id key-map)]
      (let [restored-ds (sd/load store commit-id)]
        (StratumIndex. restored-ds attrs attr-refs config))
      (StratumIndex. nil attrs attr-refs config)))

  (-sec-branch [_ store _from-branch new-branch]
    ;; Fork dataset (O(1) structural sharing) and sync to new branch
    (if dataset
      (let [forked-ds (sd/fork dataset)
            synced-ds (sd/sync! forked-ds store (name new-branch))]
        (StratumIndex. synced-ds attrs attr-refs config))
      (StratumIndex. nil attrs attr-refs config)))

  (-sec-mark [_]
    ;; Stratum shares datahike's store but -sec-mark on a live instance
    ;; doesn't have access to the store. GC uses mark-from-key-map instead,
    ;; which gets the key-map + store from the stored commit.
    #{})

  sec/IColumnarAggregate
  (-columnar-aggregate [this query-spec]
    (sec/-columnar-aggregate this query-spec nil))
  (-columnar-aggregate [_ query-spec entity-filter]
    (when dataset
      (if entity-filter
        ;; Build long[] mask: 1 for rows whose :eid is in the bitmap
        (let [eid-col ^longs (:data (sd/column dataset :eid))
              n (int (st/row-count dataset))
              mask (long-array n)]
          (dotimes [i n]
            (when (es/entity-bitset-contains? entity-filter (aget eid-col i))
              (aset mask i 1)))
          ;; Add mask as synthetic column, filter with [:= :__eid_mask 1]
          (let [mask-ds (sd/add-column dataset :__eid_mask mask)
                where (conj (vec (:where query-spec)) [:= :__eid_mask 1])]
            (st/q (assoc query-spec :from mask-ds :where where))))
        (st/q (assoc query-spec :from dataset))))))

;; ---------------------------------------------------------------------------
;; Transient stratum index — mutable batch mode

(deftype TransientStratumIndex [^:unsynchronized-mutable dataset
                                attrs          ;; set of keyword idents
                                attr-refs      ;; set of numeric refs (or nil when not attr-refs mode)
                                ref->col-key   ;; map of numeric ref → keyword col-key (or nil)
                                config
                                ^java.util.HashMap pending-adds
                                ^java.util.HashSet pending-retracts]
  sec/ITransientSecondaryIndex
  (-as-transient [this] this)

  (-transact! [this tx-report]
    (let [{:keys [^Datom datom added?]} tx-report
          eid (.-e datom)
          a-raw (.-a datom)
          ;; In attr-refs mode, a-raw is numeric — check both attrs and attr-refs
          a-match? (or (contains? attrs a-raw)
                       (and attr-refs (contains? attr-refs a-raw)))
          ;; Use ref->col-key to translate numeric refs to keyword col-keys
          col-key (if (and ref->col-key (number? a-raw))
                    (get ref->col-key a-raw)
                    (attr-col-key a-raw))]
      (when a-match?
        (if added?
          (let [entity-map (or (.get pending-adds eid) {})]
            (.put pending-adds eid (assoc entity-map col-key (.-v datom)))
            ;; Remove from pending-retracts: re-add after retract in same TX
            (.remove pending-retracts eid))
          (.add pending-retracts eid)))))

  (-persistent! [this]
    (persist-transient-stratum-index dataset attrs attr-refs config pending-adds pending-retracts)))

(defn- make-transient-stratum-index [dataset attrs attr-refs config]
  (let [;; Build ref→col-key map from ident-ref-map in config
        irm (:ident-ref-map config)
        ref->col-key (when (seq irm)
                       (into {} (keep (fn [attr]
                                        (when-let [ref (get irm attr)]
                                          [ref (attr-col-key attr)])))
                             attrs))]
    (TransientStratumIndex. dataset attrs attr-refs ref->col-key config
                            (java.util.HashMap.)
                            (java.util.HashSet.))))

(defn- persist-transient-stratum-index
  [dataset attrs attr-refs config ^java.util.HashMap pending-adds ^java.util.HashSet pending-retracts]
  (let [current-ds dataset
        has-retracts? (pos? (.size pending-retracts))
        has-adds? (pos? (.size pending-adds))
        current-cols (when current-ds (st/columns current-ds))
        current-n (if current-ds (st/row-count current-ds) 0)
        surviving-eids (when (and has-retracts? (pos? current-n))
                         (let [eid-col (:data (get current-cols :eid))
                               survivors (java.util.ArrayList.)]
                           (dotimes [i current-n]
                             (let [eid (aget ^longs eid-col i)]
                               (when-not (.contains pending-retracts eid)
                                 (.add survivors (int i)))))
                           survivors))
        new-entity-count (.size pending-adds)
        keep-n (if has-retracts?
                 (if surviving-eids (.size ^java.util.ArrayList surviving-eids) 0)
                 current-n)
        total-n (+ keep-n new-entity-count)]
    (if (and (zero? total-n) (not has-adds?) (not has-retracts?))
      ;; No changes — return as-is
      (StratumIndex. current-ds attrs attr-refs config)
      (if (zero? total-n)
        (StratumIndex. nil attrs attr-refs config)
        (let [col-keys (into [:eid] (map attr-col-key) attrs)
              ;; Pre-compute column types: scan pending values once to determine types for new columns
              col-types (when has-adds?
                          (reduce (fn [types [_eid em]]
                                    (reduce-kv (fn [t k v]
                                                 (if (or (contains? t k) (nil? v))
                                                   t
                                                   (assoc t k (cond (or (string? v) (keyword? v)) :string
                                                                    (double? v) :double
                                                                    :else :long))))
                                               types em))
                                  {} pending-adds))
              col-map
              (into {}
                    (map (fn [col-key]
                           (let [current-col-info (when current-cols (get current-cols col-key))
                                 current-col-data (when current-col-info (:data current-col-info))
                                 has-dict? (:dict current-col-info)
                                 ;; String col: raw String[], OR dict-encoded (data=long[] + dict=String[]),
                                 ;; OR new column with string values in pending
                                 is-string? (or (and current-col-data
                                                     (instance? (Class/forName "[Ljava.lang.String;") current-col-data))
                                                (some? has-dict?)
                                                (= :string (get col-types col-key)))
                                 is-double? (or (and current-col-data (not has-dict?)
                                                     (instance? (Class/forName "[D") current-col-data))
                                                (and (nil? current-col-data) (= :double (get col-types col-key))))
                                 is-long? (or (= col-key :eid)
                                              (and current-col-data (not has-dict?) (not is-double?)
                                                   (instance? (Class/forName "[J") current-col-data))
                                              (and (nil? current-col-data) (not is-string?) (not is-double?)))]
                             [col-key
                              (cond
                                ;; String column — always produce String[] for make-dataset to re-encode
                                is-string?
                                (let [str-arr ^"[Ljava.lang.String;" (make-array String total-n)]
                                  (when (and current-col-data (pos? keep-n))
                                    (if has-dict?
                                      ;; Dict-encoded: decode codes → strings
                                      (let [^"[Ljava.lang.String;" dict has-dict?]
                                        (if has-retracts?
                                          (dotimes [j keep-n]
                                            (let [code (aget ^longs current-col-data
                                                             (int (.get ^java.util.ArrayList surviving-eids j)))]
                                              (when-not (= code Long/MIN_VALUE)
                                                (aset str-arr j (aget dict (int code))))))
                                          (dotimes [i keep-n]
                                            (let [code (aget ^longs current-col-data i)]
                                              (when-not (= code Long/MIN_VALUE)
                                                (aset str-arr i (aget dict (int code))))))))
                                      ;; Raw String[]
                                      (if has-retracts?
                                        (dotimes [j keep-n]
                                          (aset str-arr j (aget ^"[Ljava.lang.String;" current-col-data
                                                                (int (.get ^java.util.ArrayList surviving-eids j)))))
                                        (System/arraycopy current-col-data 0 str-arr 0 keep-n))))
                                  (when has-adds?
                                    (let [idx (volatile! keep-n)]
                                      (doseq [[_eid entity-map] pending-adds]
                                        (let [v (get entity-map col-key)]
                                          (when v
                                            (aset str-arr @idx (str v))))
                                        (vswap! idx unchecked-inc))))
                                  str-arr)

                                ;; Long column
                                is-long?
                                (let [arr (long-array total-n)]
                                  (when (and current-col-data (pos? keep-n))
                                    (if has-retracts?
                                      (dotimes [j keep-n]
                                        (aset arr j (aget ^longs current-col-data
                                                          (int (.get ^java.util.ArrayList surviving-eids j)))))
                                      (System/arraycopy current-col-data 0 arr 0 keep-n)))
                                  (when has-adds?
                                    (let [idx (volatile! keep-n)]
                                      (doseq [[_eid entity-map] pending-adds]
                                        (let [v (if (= col-key :eid)
                                                  _eid
                                                  (get entity-map col-key))]
                                          (when v
                                            (aset arr @idx (long v))))
                                        (vswap! idx unchecked-inc))))
                                  arr)

                                ;; Double column
                                is-double?
                                (let [arr (double-array total-n)]
                                  (when (and current-col-data (pos? keep-n))
                                    (if has-retracts?
                                      (dotimes [j keep-n]
                                        (aset arr j (aget ^doubles current-col-data
                                                          (int (.get ^java.util.ArrayList surviving-eids j)))))
                                      (System/arraycopy current-col-data 0 arr 0 keep-n)))
                                  (when has-adds?
                                    (let [idx (volatile! keep-n)]
                                      (doseq [[_eid entity-map] pending-adds]
                                        (let [v (get entity-map col-key)]
                                          (when v
                                            (aset arr @idx (double v))))
                                        (vswap! idx unchecked-inc))))
                                  arr)

                                :else
                                (long-array total-n))])))
                    col-keys)]
          ;; ensure-indexed converts array-backed columns to index-backed (PSS)
          ;; columns, which is required before sync! can persist.
          (StratumIndex. (sd/ensure-indexed (st/make-dataset col-map)) attrs attr-refs config))))))

;; ---------------------------------------------------------------------------
;; Registration

(let [factory (fn [config db]
                (let [attrs (set (:attrs config))
                      ident-ref-map (:ident-ref-map config)
                      attr-refs (when ident-ref-map
                                  (not-empty (set (keep ident-ref-map attrs))))]
                  (StratumIndex. (build-initial-dataset db attrs config)
                                 attrs
                                 attr-refs
                                 config)))]
  (sec/register-index-type! :stratum factory)
  (sec/register-index-type! :datahike.index.secondary/stratum factory))

;; GC: stratum writes to datahike's konserve store, so datahike's GC must
;; preserve stratum's keys. Walk the dataset commit chain to collect all
;; reachable keys: dataset commits, index commits, PSS node addresses,
;; plus branch metadata keys.
(defmethod sec/mark-from-key-map :stratum [key-map store]
  (if-let [commit-id (:dataset-commit-id key-map)]
    (let [;; Walk parent chain from this commit to collect all reachable dataset commits
          reachable-ds-commits
          (loop [queue [commit-id]
                 visited #{}]
            (if (empty? queue)
              visited
              (let [[current & rest] queue]
                (if (or (nil? current) (visited current))
                  (recur (vec rest) visited)
                  (let [snapshot (ss/load-dataset-commit store current)
                        parents (when snapshot (seq (:parents snapshot)))]
                    (recur (into (vec rest) parents)
                           (conj visited current)))))))
          ;; Collect reachable index commits from dataset snapshots
          reachable-idx-commits (ss/collect-live-index-commits store reachable-ds-commits)
          ;; Collect reachable PSS node addresses from index snapshots
          reachable-pss-addrs (ss/collect-live-pss-addresses store reachable-idx-commits)]
      ;; Return the union of all reachable keys in datahike's store format
      (into #{}
            (concat
             ;; PSS node addresses (flat UUIDs)
             reachable-pss-addrs
             ;; Index commit keys
             (map (fn [id] [:indices :commits id]) reachable-idx-commits)
             ;; Dataset commit keys
             (map (fn [id] [:datasets :commits id]) reachable-ds-commits)
             ;; Branch metadata keys
             (when-let [branch (:branch key-map)]
               [[:datasets :heads branch]
                [:datasets :branches]]))))
    #{}))

;; Branch: fork dataset and sync to new branch
(defmethod sec/branch-from-key-map :stratum [key-map store _from-branch new-branch]
  (if-let [commit-id (:dataset-commit-id key-map)]
    (let [ds (sd/load store commit-id)
          forked (sd/fork ds)
          synced (sd/sync! forked store (name new-branch))]
      (assoc key-map
             :branch (name new-branch)
             :dataset-commit-id (get-in synced [:commit-info :id])))
    (assoc key-map :branch (name new-branch))))
