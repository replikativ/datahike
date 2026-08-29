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
   [clojure.core.async :as async]
   [datahike.index.audit :as audit]
   [datahike.index.secondary :as sec]
   [datahike.index.entity-set :as es]
   [datahike.db.interface :as dbi]
   [datalog.parser.type]
   [stratum.api :as st]
   [stratum.dataset :as sd]
   [stratum.warm :as stratum-warm]
   [stratum.index :as sidx]
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
   Since stratum 0.1.45, min/max on int64 columns return Long (type-preserving).

   A fast path may only claim an aggregate it provably computes to datahike's
   contract (see `built-in-aggregates`). Mapping by NAME alone is what let the
   columnar path answer 82.667 where the reference answers 62, and ##NaN for a
   one-element group: stratum's `:variance`/`:stddev` are the SAMPLE estimator
   (÷n−1), and passing `{:sample? false}` in the agg spec has no effect. Its
   `:variance-pop`/`:stddev-pop` ops are the population form datahike specifies,
   and they are total — a one-element group gives 0.0, not NaN. Same name,
   different statistic, so the mapping must be explicit rather than identity."
  {:avg :avg, :sum :sum, :count :count, :min :min, :max :max,
   :variance :variance-pop, :stddev :stddev-pop,
   :count-distinct :count-distinct, :median :median})

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
    ;; Stratum returns Doubles for numeric aggregates. For min/max/sum on Long
    ;; columns, coerce back to Long to preserve type semantics.
    ;; min/max/sum preserve the column's type; the central-tendency aggregates
    ;; (avg, median, variance, stddev) are real-valued and stay doubles.
    (let [long-coerce-agg? #{:min :max :sum}
          coerce-fns (mapv (fn [spec]
                             (let [agg-op (first spec)
                                   col-key (when (> (count spec) 1) (second spec))
                                   col-arr (when col-key (get column-map col-key))]
                               (if (and (long-coerce-agg? agg-op)
                                        col-arr
                                        (instance? (Class/forName "[J") col-arr))
                                 long
                                 identity)))
                           agg-specs)]
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
                                    (get result-map (keyword (str (name op-key) "_" (name col-key))))))
                            coerce (nth coerce-fns @ai)]
                        (aset tuple fi (if (number? v) (coerce v) v))
                        (vswap! ai unchecked-inc))
                      ;; Group-by variable
                      (let [col-key (nth group-keys @gi)]
                        (aset tuple fi (get result-map col-key))
                        (vswap! gi unchecked-inc)))))
                (clojure.lang.PersistentVector/adopt tuple)))
            result-maps))))

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

;; ---------------------------------------------------------------------------
;; Valid-time (SCD2) support — opt-in via `:valid-time true` in :db.secondary/config
;;
;; In vt-mode, each entity update appends a new row instead of mutating the
;; existing one. Old rows have their `:_valid_to` closed to the new tx's
;; `:db.valid/from`. Queries push `valid-at` filters down via -search-at-vt
;; → stratum WHERE predicates → zone-map pruning.

(defn- vt-mode? [config] (boolean (:valid-time config)))

(def ^:private vt-from-col :_valid_from)
(def ^:private vt-to-col   :_valid_to)
(def ^:private sys-from-col :_system_from)
(def ^:private sys-to-col   :_system_to)
(def ^:private vt-open-sentinel Long/MAX_VALUE)
;; A vt-from that is older than any real tx-meta; rows seeded from
;; pre-existing AEVT data in build-initial-dataset use it so they appear
;; valid for any `valid-at` query against historical data.
(def ^:private vt-from-floor Long/MIN_VALUE)

(defn- date->micros
  "Coerce a :db.valid/from or :db.valid/to value to long microseconds. Accepts
   java.util.Date (millis × 1000), already-long values (passthrough), or nil
   for which the caller supplies a default."
  ^long [v]
  (cond
    (nil? v) 0
    (instance? java.util.Date v) (* 1000 (.getTime ^java.util.Date v))
    (number? v) (long v)
    :else (throw (ex-info "Unsupported vt value type"
                          {:value v :type (type v)}))))

(defn- tx-meta->vf
  "Pick the row's `_valid_from` for this tx. Prefers `:db.valid/from`;
   falls back to `:db/txInstant` (so non-vt txes still get a meaningful
   vf in vt-aware indices)."
  ^long [tx-meta]
  (date->micros (or (:db.valid/from tx-meta)
                    (:db/txInstant tx-meta))))

(defn- tx-meta->vt
  "Pick the row's `_valid_to` for this tx. `:db.valid/to` if specified,
   otherwise the open-ended sentinel."
  ^long [tx-meta]
  (if-let [v (:db.valid/to tx-meta)]
    (date->micros v)
    vt-open-sentinel))

(defn- tx-meta->sf
  "Pick the row's `_system_from` for this tx — the instant the DB
   first knew about the row. Comes from `:db/txInstant` (the
   transactor stamps it on every tx) or the legacy
   `:datahike/created-at` fallback, or wall-clock now if neither
   is set."
  ^long [tx-meta]
  (date->micros (or (:db/txInstant tx-meta)
                    (:datahike/created-at tx-meta)
                    (java.util.Date.))))

(defn- ds-metadata
  "Build the `:metadata` map passed to `st/make-dataset` for this
   index. In vt-mode it carries `:bitemporal {:valid … :system …}`
   matching stratum's current API: stratum tags all four temporal
   columns with `:temporal-unit :micros` and round-trips the config
   through `sync!`/`load`. Both axes are always present in vt-mode
   so SCD2 surgery can advance the system-time axis when correcting
   a valid-time window — the audit guarantee that backdated
   corrections don't silently rewrite past `FOR SYSTEM_TIME` views."
  [config]
  (if (vt-mode? config)
    {:bitemporal {:valid  {:from-col vt-from-col  :to-col vt-to-col   :unit :micros}
                  :system {:from-col sys-from-col :to-col sys-to-col :unit :micros}}}
    {}))

(defn- with-vt-cols
  "If vt-mode, attach empty `_valid_from`/`_valid_to`/`_system_from`/
   `_system_to` columns (n long zeros) alongside the per-attribute
   columns. The caller fills them when building rows; this helper
   just makes sure the columns exist before `make-dataset` infers
   the schema."
  [col-map config n]
  (if (vt-mode? config)
    (assoc col-map
           vt-from-col  (long-array n)
           vt-to-col    (long-array n)
           sys-from-col (long-array n)
           sys-to-col   (long-array n))
    col-map))

(defn- make-vt-dataset
  "Wrap st/make-dataset with the vt-aware metadata when vt-mode is on."
  [col-map config]
  (st/make-dataset col-map {:metadata (ds-metadata config)}))

(defn- build-initial-dataset
  "Build a StratumDataset from the DB's existing data for configured attributes.
   Scans AEVT per attribute to collect entity+value pairs, then joins by entity.
   When db is nil (during empty-db creation), returns an empty dataset.

   Valid-time mode refuses a populated current AEVT snapshot. Inventing
   `[MIN, MAX)` windows makes the generation look complete while discarding
   real valid/system history; a future online builder must reconstruct rows
   from tx history under the normal GC fence."
  [db attrs config]
  (if (nil? db)
    ;; No DB yet — return empty dataset with typed columns
    (let [col-map (-> (into {:eid (long-array 0)}
                            (map (fn [a] [(attr-col-key a) (long-array 0)]))
                            attrs)
                      (with-vt-cols config 0))]
      (sd/ensure-indexed (make-vt-dataset col-map config)))
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
      (cond
        (and (vt-mode? config) (pos? n))
        (throw
         (ex-info
          "A valid-time Stratum index cannot be seeded from a current AEVT snapshot. Build it while empty or reconstruct it from transaction history."
          {:type :secondary/stratum-valid-time-backfill-required
           :row-count n
           :attrs attrs}))

        (zero? n)
      ;; Empty dataset — use long-array 0 for all columns since we don't know
      ;; actual value types yet. persist-transient-stratum-index will determine
      ;; real types from values via col-types pre-scan on first insert.
        (let [col-map (-> (into {:eid (long-array 0)}
                                (map (fn [a] [(attr-col-key a) (long-array 0)]))
                                attrs)
                          (with-vt-cols config 0))]
          (sd/ensure-indexed (make-vt-dataset col-map config)))

        :else
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
              ;; In vt-mode, seed every pre-existing row with the maximally-
              ;; permissive vt-window [MIN, MAX). Subsequent vt-aware txes
              ;; close these windows when the entity is updated.
              ;; The system-time axis on seeded rows uses [MIN, MAX) too —
              ;; we don't know when the DB first knew about pre-existing
              ;; data; treating it as known-since-time-began matches the
              ;; vt-axis convention.
              vt-from-arr  (when (vt-mode? config)
                             (let [arr (long-array n)]
                               (java.util.Arrays/fill arr vt-from-floor) arr))
              vt-to-arr    (when (vt-mode? config)
                             (let [arr (long-array n)]
                               (java.util.Arrays/fill arr vt-open-sentinel) arr))
              sys-from-arr (when (vt-mode? config)
                             (let [arr (long-array n)]
                               (java.util.Arrays/fill arr vt-from-floor) arr))
              sys-to-arr   (when (vt-mode? config)
                             (let [arr (long-array n)]
                               (java.util.Arrays/fill arr vt-open-sentinel) arr))
              col-map (cond-> (assoc attr-arrays :eid entity-ids)
                        (vt-mode? config) (assoc vt-from-col  vt-from-arr
                                                 vt-to-col    vt-to-arr
                                                 sys-from-col sys-from-arr
                                                 sys-to-col   sys-to-arr))]
          (sd/ensure-indexed (make-vt-dataset col-map config)))))))

;; ---------------------------------------------------------------------------
;; Persistent stratum index (declared first — TransientStratumIndex references it)

(declare make-transient-stratum-index)
(declare persist-transient-stratum-index)

(defn- unsupported-vt-view!
  [operation temporal-request reason]
  (throw (ex-info
          "This Stratum generation cannot preserve the requested temporal view."
          {:type :secondary/stratum-temporal-view-unsupported
           :operation operation
           :temporal-request temporal-request
           :reason reason})))

(defn- temporal-where
  [operation temporal-request]
  (when-not (= :current (get-in temporal-request [:system :mode]))
    (unsupported-vt-view! operation temporal-request :system-time-view))
  (let [{:keys [mode at from to]} (:valid temporal-request)]
    (case mode
      :at [[:= sys-to-col vt-open-sentinel]
           [:<= vt-from-col (date->micros at)]
           [:> vt-to-col (date->micros at)]]
      :between [[:= sys-to-col vt-open-sentinel]
                [:< vt-from-col (date->micros to)]
                [:> vt-to-col (date->micros from)]]
      :during [[:= sys-to-col vt-open-sentinel]
               [:>= vt-from-col (date->micros from)]
               [:<= vt-to-col (date->micros to)]]
      (unsupported-vt-view! operation temporal-request
                            :missing-valid-time-selector))))

(defn- require-ordinary-view!
  [config operation]
  (when (vt-mode? config)
    ;; System-open SCD rows include the current belief ABOUT historical valid
    ;; intervals. They are not necessarily the current Datahike value. Until
    ;; the representation carries an explicit primary-current marker, using
    ;; this generation for an unqualified query can return a retracted value.
    (unsupported-vt-view! operation nil :primary-current-row-not-represented)))

(defn- search-dataset
  [dataset query-spec entity-filter extra-where]
  (if (nil? dataset)
    (es/entity-bitset)
    (let [eid-pred (when entity-filter
                     (fn [^long eid]
                       (es/entity-bitset-contains? entity-filter eid)))
          where (cond-> (into (vec (:where query-spec)) extra-where)
                  eid-pred (conj [:fn :eid eid-pred]))
          result-maps (st/q (cond-> {:from dataset :select [:eid]}
                              (seq where) (assoc :where where)))
          bs (es/entity-bitset)]
      (doseq [m result-maps]
        (es/entity-bitset-add! bs (long (:eid m))))
      bs)))

(defn- slice-dataset
  [dataset query-spec entity-filter attr direction limit extra-where]
  (if (nil? dataset)
    []
    (let [col-key (attr-col-key attr)
          eid-pred (when entity-filter
                     (fn [^long eid]
                       (es/entity-bitset-contains? entity-filter eid)))
          where (cond-> (into (vec (:where query-spec)) extra-where)
                  eid-pred (conj [:fn :eid eid-pred]))
          result-maps (st/q (cond-> {:from dataset
                                     :select [:eid col-key]
                                     :order [[col-key direction] [:eid :asc]]}
                              limit (assoc :limit limit)
                              (seq where) (assoc :where where)))]
      (mapv (fn [m] {:entity-id (long (:eid m)) :value (get m col-key)})
            result-maps))))

(defn- candidate-page*
  [dataset attrs scan-id query-spec entity-filter page-request temporal-request]
  (let [{:keys [attribute direction where]} query-spec
        direction (or direction :asc)
        limit (long (:limit page-request))
        entity-ids (when entity-filter
                     (vec (es/entity-bitset-seq entity-filter)))
        extra-where (if temporal-request
                      (do
                        (when-not (= :at (get-in temporal-request [:valid :mode]))
                          (unsupported-vt-view! :candidate-page temporal-request
                                                :interval-candidate-identity))
                        (temporal-where :candidate-page temporal-request))
                      [])
        scan-identity
        {:version 2
         :generation (if-let [generation-id (and dataset
                                                 (not (sd/dirty? dataset))
                                                 (sd/generation-id dataset))]
                       [:dataset generation-id]
                       [:instance scan-id])
         :attribute attribute
         :direction direction
         :where (vec where)
         :temporal-request temporal-request
         ;; Exact rather than hashed: a collision must never change which
         ;; rows an OFFSET continuation denotes.
         :entity-ids entity-ids}
        continuation (:continuation page-request)
        _ (when (and continuation
                     (or (not (map? continuation))
                         (not= 2 (:version continuation))
                         (not= scan-identity (:scan-identity continuation))
                         (not (and (integer? (:offset continuation))
                                   (not (neg? (:offset continuation)))))))
            (throw
             (ex-info
              "Stratum candidate continuation does not belong to this generation/query."
              {:type :secondary/stratum-continuation-mismatch
               :expected scan-identity
               :continuation continuation})))
        offset (long (or (:offset continuation) 0))]
    (if (or (nil? dataset) (not (contains? attrs attribute)))
      {:candidates []
       :precision :exact
       :recall :complete
       :ordering :exact
       :exhausted? true
       :continuation nil
       :stop-reason :source-exhausted}
      (let [col-key (attr-col-key attribute)
            eid-pred (when entity-filter
                       (fn [^long eid]
                         (es/entity-bitset-contains? entity-filter eid)))
            where (cond-> (into (vec where) extra-where)
                    eid-pred (conj [:fn :eid eid-pred]))
            rows (st/q (cond-> {:from dataset
                                :select [:eid col-key]
                                :order [[col-key direction] [:eid :asc]]
                                :limit (inc limit)
                                :offset offset}
                         (seq where) (assoc :where where)))
            more? (> (count rows) limit)
            page-rows (take limit rows)
            candidates (mapv (fn [row]
                               {:entity-id (long (:eid row))
                                :attribute attribute
                                :value (get row col-key)})
                             page-rows)]
        {:candidates candidates
         :precision :exact
         :recall :complete
         :ordering :exact
         :exhausted? (not more?)
         :continuation (when more?
                         {:version 2
                          :scan-identity scan-identity
                          :offset (+ offset limit)})
         :stop-reason (when-not more? :source-exhausted)}))))

(defn- delivered [value]
  (let [ch (async/promise-chan)]
    (async/put! ch value)
    ch))

(defrecord StratumPreparation [prepared-index]
  sec/IPreparedSecondaryGeneration
  (-sec-generation-index [_] prepared-index)
  ;; Datahike's root is the only publication point. Stratum generations need
  ;; no post-head native ref movement; unreachable attempts are ordinary GC.
  (-sec-release [_ _outcome] (delivered true)))

(defn- stratum-key-map-failure-reason [key-map]
  (cond
    (not= :stratum (:type key-map)) :wrong-type
    ;; The first detached-generation bridge wrote a UUID without a versioned
    ;; ownership envelope. Its object may be immutable, but the stored map does
    ;; not establish the format or who must retain it.
    (not (contains? key-map :format-version))
    :legacy-stratum-generation-envelope
    (not= 1 (:format-version key-map)) :unsupported-format-version
    (not= :datahike (:storage-owner key-map)) :wrong-storage-owner
    (not (uuid? (:dataset-commit-id key-map))) :invalid-dataset-commit-id
    :else nil))

(defn- validate-stratum-generation-key-map [key-map]
  (when-let [reason (stratum-key-map-failure-reason key-map)]
    (throw (ex-info
            "Invalid Stratum generation key-map."
            {:type :secondary/invalid-stratum-generation
             :reason reason
             :key-map key-map
             :expected {:type :stratum
                        :format-version 1
                        :storage-owner :datahike
                        :dataset-commit-id :uuid}})))
  key-map)

(deftype StratumIndex [dataset    ;; StratumDataset or nil
                       attrs      ;; set of datahike attribute idents being indexed
                       attr-refs  ;; set of numeric refs (for attr-refs mode) or nil
                       config     ;; user config map
                       scan-id]   ;; exact identity for unsealed in-memory generations

  sec/ISecondaryIndex
  (-search [_ query-spec entity-filter]
    ;; query-spec: {:where [[op col val] ...]}
    ;; Returns EntityBitSet of matching entity IDs
    (require-ordinary-view! config :search)
    (search-dataset dataset query-spec entity-filter []))

  (-estimate [_ query-spec]
    (if (nil? dataset)
      0
      (st/row-count dataset)))

  (-can-order? [_ _attr direction]
    true)

  (-slice-ordered [_ query-spec entity-filter attr direction limit]
    (require-ordinary-view! config :slice-ordered)
    (slice-dataset dataset query-spec entity-filter attr direction limit []))

  sec/ISecondaryCandidateScan
  (-candidate-page [_ query-spec entity-filter page-request]
    (require-ordinary-view! config :candidate-page)
    (candidate-page* dataset attrs scan-id query-spec entity-filter
                     page-request nil))

  sec/IValidTimeCandidateScan
  (-candidate-page-at-vt [_ query-spec entity-filter page-request temporal-request]
    (when-not (vt-mode? config)
      (unsupported-vt-view! :candidate-page temporal-request
                            :valid-time-mode-disabled))
    (candidate-page* dataset attrs scan-id query-spec entity-filter
                     page-request temporal-request))

  (-indexed-attrs [_] attrs)

  sec/ISecondaryScannable
  (-sec-value [_ attr eid]
    ;; Stratum keeps the real column values, so a backup can read them back —
    ;; which matters only for `:db.secondary/only` attributes, whose value is
    ;; NOT in the primary indexes (those hold a content hash) and would
    ;; otherwise be absent from the dump entirely.
    ;;
    ;; A point query, not a column scan: the caller streams a dump and must stay
    ;; bounded. `:where` takes `[[op col val] ...]` — see `-slice-ordered`.
    (when dataset
      (let [col-key (attr-col-key attr)
            rows (st/q (cond-> {:from dataset
                                :select (cond-> [:eid col-key]
                                          (vt-mode? config)
                                          (into [sys-from-col vt-from-col]))
                                :where (cond-> [[:= :eid (long eid)]]
                                         (vt-mode? config)
                                         (conj [:= sys-to-col vt-open-sentinel]))}
                         (not (vt-mode? config)) (assoc :limit 1)))
            row (if (vt-mode? config)
                  ;; A correction can leave several current-system-belief rows
                  ;; for one eid: bounded corrected history plus the latest
                  ;; primary value. Pick the newest system version, then the
                  ;; latest valid start within that correction transaction.
                  ;; Requiring `_valid_to = MAX` is wrong for a current primary
                  ;; value that was asserted with a finite valid window.
                  (reduce (fn [best candidate]
                            (if (or (nil? best)
                                    (pos? (compare [(get candidate sys-from-col)
                                                    (get candidate vt-from-col)]
                                                   [(get best sys-from-col)
                                                    (get best vt-from-col)])))
                              candidate
                              best))
                          nil rows)
                  (first rows))]
        (get row col-key))))

  sec/ISecondaryBackfillPolicy
  (-backfill-policy [_]
    (if (vt-mode? config) :transaction-history :current-values))

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

  sec/IPureSecondaryMutation
  (-pure-secondary-mutation? [_] true)

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
        (StratumIndex. dataset attrs new-attr-refs new-config scan-id))))

  sec/ISecondaryWarmable
  (-sec-warm! [_ opts]
    ;; One shared budget across every column tree, in NODES — stratum's unit.
    ;; See stratum.warm for depth semantics: leaves there are FAT (a leaf
    ;; carries its chunks), so :with-leaves is materializing the column data.
    (if dataset
      (stratum-warm/warm! dataset opts)
      {:fetched 0 :ms 0.0 :budget-exhausted? false}))

  sec/IDurableSecondaryIndex
  (-sec-generation-key-map [_]
    {:type :stratum
     :format-version 1
     :storage-owner :datahike
     :dataset-commit-id (some-> dataset sd/generation-id)})

  (-sec-prepare [_ {:keys [store]}]
    (let [;; Reuse an already sealed clean generation. Besides avoiding one
          ;; metadata commit per unrelated Datahike transaction, this is what
          ;; makes Datahike branch creation a genuine O(1) root copy.
          sealed-ds (cond
                      (nil? dataset) nil
                      (and (sd/generation-id dataset)
                           (not (sd/dirty? dataset))) dataset
                      :else (sd/seal-generation! dataset store))
          prepared-index (StratumIndex. sealed-ds attrs attr-refs config scan-id)]
      (delivered (->StratumPreparation prepared-index))))

  (-sec-restore [_ store key-map]
    ;; A stored ready index always names an exact generation. Absence is not an
    ;; empty dataset: treating it that way loses the only authoritative value
    ;; for :db.secondary/only attributes.
    (let [{:keys [dataset-commit-id]}
          (validate-stratum-generation-key-map key-map)
          restored-ds (sd/open-generation store dataset-commit-id)]
      (StratumIndex. restored-ds attrs attr-refs config (random-uuid))))

  audit/IAuditable
  (-merkle-root [_]
    ;; A Stratum generation ID is immutable, but its default UUID is opaque,
    ;; not a content hash. Advertising it as a Merkle root would make an
    ;; audit-grade Datahike commit claim more than it can verify.
    nil)
  (-recompute-merkle-root [_]
    {:status :unsupported
     :reason (if dataset
               :stratum-generation-id-not-content-addressed
               :unsynced)})

  sec/IColumnarAggregate
  (-columnar-aggregate [this query-spec]
    (sec/-columnar-aggregate this query-spec nil))
  (-columnar-aggregate [_ query-spec entity-filter]
    (require-ordinary-view! config :columnar-aggregate)
    (when dataset
      (if entity-filter
        ;; Push entity-filter as a :fn predicate on the :eid column.
        ;; Stratum compiles this into the fused filter+aggregate loop — no mask allocation.
        (let [eid-pred (fn [^long eid] (es/entity-bitset-contains? entity-filter eid))
              where (conj (vec (:where query-spec)) [:fn :eid eid-pred])]
          (st/q (assoc query-spec :from dataset :where where)))
        (st/q (assoc query-spec :from dataset)))))

  sec/IValidTimeAware
  ;; Native vt-pushdown: extend the query's `:where` with predicates on
  ;; the two vt-window columns. Stratum's zone-map pruner skips chunks
  ;; whose [min,max] range can't intersect, and the predicate kernel
  ;; runs the row-level check on the survivors. Open-ended rows
  ;; (`_valid_to = Long/MAX_VALUE`) participate normally because the
  ;; predicate uses strict-greater.
  ;;
  ;; `-native-valid-time?` is deliberately per instance: an ordinary Stratum
  ;; generation uses the generic post-hoc fallback and must never route here.
  (-native-valid-time? [_]
    (boolean (vt-mode? config)))
  (-search-at-vt [_ query-spec entity-filter temporal-request]
    (if (and (vt-mode? config) dataset)
      (do
        (when-not (= :at (get-in temporal-request [:valid :mode]))
          (unsupported-vt-view! :search temporal-request
                                :transaction-event-interval-semantics))
        (search-dataset dataset query-spec entity-filter
                        (temporal-where :search temporal-request)))
      (if (vt-mode? config)
        (es/entity-bitset)
        (unsupported-vt-view! :search temporal-request
                              :valid-time-mode-disabled))))

  sec/IValidTimeOrdered
  (-slice-ordered-at-vt [_ query-spec entity-filter attr direction limit
                         temporal-request]
    (when-not (vt-mode? config)
      (unsupported-vt-view! :slice-ordered temporal-request
                            :valid-time-mode-disabled))
    (when-not (= :at (get-in temporal-request [:valid :mode]))
      (unsupported-vt-view! :slice-ordered temporal-request
                            :interval-candidate-identity))
    (slice-dataset dataset query-spec entity-filter attr direction limit
                   (temporal-where :slice-ordered temporal-request))))

;; ---------------------------------------------------------------------------
;; Transient stratum index — mutable batch mode

(deftype TransientStratumIndex [^:unsynchronized-mutable dataset
                                attrs          ;; set of keyword idents
                                attr-refs      ;; set of numeric refs (or nil when not attr-refs mode)
                                ref->col-key   ;; map of numeric ref → keyword col-key (or nil)
                                config
                                ^java.util.HashMap pending-adds
                                ;; eid -> #{col-key}: a retraction names an
                                ;; ATTRIBUTE of an entity, not the entity.
                                ^java.util.HashMap pending-retracts
                                ^"[Ljava.lang.Object;" tx-meta-ref] ;; 1-slot mutable cell
  sec/ITransientSecondaryIndex
  (-as-transient [this] this)

  (-transact! [this tx-report]
    (let [{:keys [^Datom datom added? tx-meta]} tx-report
          eid (.-e datom)
          a-raw (.-a datom)
          ;; In attr-refs mode, a-raw is numeric — check both attrs and attr-refs
          a-match? (or (contains? attrs a-raw)
                       (and attr-refs (contains? attr-refs a-raw)))
          ;; Use ref->col-key to translate numeric refs to keyword col-keys
          col-key (if (and ref->col-key (number? a-raw))
                    (get ref->col-key a-raw)
                    (attr-col-key a-raw))]
      ;; tx-meta is the same for every datom in a batch — capture once,
      ;; persist! reads it back.
      (when tx-meta (aset tx-meta-ref 0 tx-meta))
      (when a-match?
        (if added?
          (let [entity-map (or (.get pending-adds eid) {})]
            (.put pending-adds eid (assoc entity-map col-key (.-v datom)))
            ;; Re-assert after retract in the same tx cancels the retraction —
            ;; for THIS COLUMN. It used to cancel the entity's whole retraction,
            ;; which was the granularity bug: a card-one update is retract+add
            ;; in one tx, so the cancel kept the old row alive and the new
            ;; partial row was appended beside it.
            (when-let [cols (.get pending-retracts eid)]
              (let [cols' (disj cols col-key)]
                (if (seq cols')
                  (.put pending-retracts eid cols')
                  (.remove pending-retracts eid)))))
          ;; A retraction names one [entity, attribute] — never the entity.
          (.put pending-retracts eid (conj (or (.get pending-retracts eid) #{}) col-key))))))

  (-persistent! [this]
    (persist-transient-stratum-index dataset attrs attr-refs config
                                     pending-adds pending-retracts
                                     (aget tx-meta-ref 0)))

  sec/IDurableSecondaryTransient
  (-durable-persistent-result? [_] true))

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
                            (java.util.HashMap.)
                            (object-array 1))))

(declare persist-transient-stratum-index)

(defn- row->col-map
  "Pivot a row-vector of maps to a column map of arrays. Type per column
   is inferred from the first non-nil value seen — matches how stratum's
   encode-column auto-types raw arrays."
  [rows col-keys]
  (let [n (count rows)]
    (into {}
          (map (fn [k]
                 (let [;; Find first non-nil value to infer type
                       sample (some #(let [v (get % k)] (when (some? v) v)) rows)
                       t (cond
                           (= k :eid) :long
                           (or (= k vt-from-col) (= k vt-to-col)) :long
                           (nil? sample) :long
                           (or (string? sample) (keyword? sample)) :string
                           (double? sample) :double
                           :else :long)]
                   [k (case t
                        :long   (let [arr (long-array n)]
                                  (dotimes [i n]
                                    (when-let [v (get (nth rows i) k)]
                                      (aset arr i (long v))))
                                  arr)
                        :double (let [arr (double-array n)]
                                  (dotimes [i n]
                                    (when-let [v (get (nth rows i) k)]
                                      (aset arr i (double v))))
                                  arr)
                        :string (let [arr ^"[Ljava.lang.String;" (make-array String n)]
                                  (dotimes [i n]
                                    (when-let [v (get (nth rows i) k)]
                                      (aset arr i (str v))))
                                  arr))])))
          col-keys)))

(defn- stratum-tx-meta
  "datahike's tx-meta translated to the keys stratum's `upsert!`/`retract!`
   stamp axis columns from. Nil outside vt-mode, where there are no axes."
  [config tx-meta]
  (when (vt-mode? config)
    {:valid-from (tx-meta->vf tx-meta)
     :valid-to   (tx-meta->vt tx-meta)
     :system-from (tx-meta->sf tx-meta)}))

(defn- pending->specs
  "One `eid -> {col-key value}` map for the whole batch, which is what
   `upsert!` takes as `:rows`.

   A RETRACTED column becomes an explicit nil rather than an absent key: absent
   means \"leave this column alone\" (that is what makes partial updates work),
   and a retraction must say the opposite. `-transact!` has already removed a
   column from the entity's retract set if the same transaction re-asserts it,
   so the two cannot disagree here."
  [^java.util.HashMap pending-adds ^java.util.HashMap pending-retracts]
  (persistent!
   (reduce (fn [acc [eid cols]]
             (assoc! acc eid (merge (get acc eid) (zipmap cols (repeat nil)))))
           (reduce (fn [acc [eid m]] (assoc! acc eid m))
                   (transient {}) pending-adds)
           pending-retracts)))

(defn- create-dataset-from-specs
  "The first batch, where there is no dataset to upsert into. `upsert!` cannot
   create columns from nothing — it needs a table to name rows in — so the
   initial one is built here from the same specs.

   This is the ONLY row-building left in the adapter. Everything else about
   updating rows now belongs to stratum: what used to be here was a
   reimplementation of SCD2 (close the open row, carry unchanged columns
   forward, rebuild the column map) that `upsert!` already performs, and the
   four data-loss defects this change fixes all lived in that copy."
  [specs attrs config tx-meta]
  (let [vt?      (vt-mode? config)
        attr-cols (mapv attr-col-key attrs)
        col-keys (if vt?
                   (into [:eid vt-from-col vt-to-col sys-from-col sys-to-col] attr-cols)
                   (into [:eid] attr-cols))
        stamp    (when vt?
                   ;; `:db.valid/to` via `tx-meta->vt`, NOT the open sentinel.
                   ;; A first transaction may declare a bounded validity window,
                   ;; and hardcoding MAX here left it open — so the next write
                   ;; OVERLAPPED it and triggered SCD2 surgery where the two
                   ;; windows should simply have abutted. The system axis is
                   ;; open by construction: a row just written is current.
                   {vt-from-col (tx-meta->vf tx-meta)
                    vt-to-col   (tx-meta->vt tx-meta)
                    sys-from-col (tx-meta->sf tx-meta)
                    sys-to-col  vt-open-sentinel})
        rows     (mapv (fn [[eid cols]] (merge {:eid eid} stamp cols)) specs)
        col-map  (row->col-map rows col-keys)]
    (sd/ensure-indexed (if vt? (make-vt-dataset col-map config) (st/make-dataset col-map)))))

(defn- prune-valueless-rows
  "Remove entities left holding no value at all.

   A retraction clears CELLS, which is what keeps an entity's other attributes
   (the defect this replaces dropped the whole row). But an entity whose last
   attribute is retracted should leave the index rather than linger as a row of
   nils — so those keys are retracted, which physically removes the row on a
   plain table and closes the open row in vt-mode."
  [ds attrs config]
  (if (nil? ds)
    ds
    (let [attr-cols (mapv attr-col-key attrs)
          vt?       (vt-mode? config)
          live      (st/q (cond-> {:from ds :select (into [:eid] attr-cols)}
                            vt? (assoc :where [[:= vt-to-col vt-open-sentinel]])))
          empties   (into [] (comp (filter (fn [r] (every? #(nil? (get r %)) attr-cols)))
                                   (map :eid))
                          live)]
      (if (seq empties)
        (-> ds transient (sd/retract! {:by :eid :keys empties}) persistent!)
        ds))))

(defn- persist-transient-stratum-index
  "Apply one transaction's pending cells to the dataset.

   ONE path for both modes. `upsert!` branches on the dataset's own axis
   configuration — SCD2 close-and-reopen where there is a `:valid` axis,
   overwrite in place where there is not — and with a `:valid` axis it inherits
   unchanged columns from the previous open row, which is exactly what makes a
   partial spec correct. The adapter used to carry two persist functions and a
   private SCD2 implementation to say the same thing; it said it wrongly.

   `{:by :eid}` rather than a `:where` per entity: `:where` is evaluated per
   row, so N entities would mean N passes over the index. Keyed, the whole
   batch is one pass (stratum 0.3.78, replikativ/stratum#36)."
  [dataset attrs attr-refs config ^java.util.HashMap pending-adds
   ^java.util.HashMap pending-retracts tx-meta]
  (let [specs (pending->specs pending-adds pending-retracts)]
    (if (empty? specs)
      (StratumIndex. dataset attrs attr-refs config (random-uuid))
      (let [;; An EMPTY dataset is built too, not upserted into. `build-initial-dataset`
            ;; makes one at index declaration whose columns are typed before any
            ;; value has been seen, so appending a string into a column guessed
            ;; long is a ClassCastException out of `idx-append!`. Zero rows is
            ;; exactly the boundary at which types are still inferable from the
            ;; data, which is what `row->col-map` does.
            fresh? (or (nil? dataset) (zero? (long (st/row-count dataset))))
            ds (if fresh?
                 (create-dataset-from-specs specs attrs config tx-meta)
                 (-> dataset transient
                     (sd/upsert! {:by :eid :rows specs} (stratum-tx-meta config tx-meta))
                     persistent!))]
        (StratumIndex. (prune-valueless-rows ds attrs config) attrs attr-refs config
                       (random-uuid))))))

;; ---------------------------------------------------------------------------
;; Registration

(let [descriptor
      {:create
       (fn [config db]
         (let [attrs (set (:attrs config))
               ident-ref-map (:ident-ref-map config)
               attr-refs (when ident-ref-map
                           (not-empty (set (keep ident-ref-map attrs))))]
           (StratumIndex. (build-initial-dataset db attrs config)
                          attrs
                          attr-refs
                          config
                          (random-uuid))))
       :storage-owner :datahike
       :validate-generation validate-stratum-generation-key-map
       :mark-generation
       (fn [key-map store]
         (ss/generation-reachable-keys store (:dataset-commit-id key-map)))}]
  (sec/register-index-type! :stratum descriptor)
  (sec/register-index-type! :datahike.index.secondary/stratum descriptor))

;; Stratum writes into Datahike's konserve store. The owning Datahike commit
;; marks exactly the immutable generation it names; Stratum parent history and
;; mutable standalone branch refs are separate retention policies.
