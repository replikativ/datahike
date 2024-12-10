(ns datahike.db.transaction
  (:require
   [clojure.spec.alpha :as s]
   [clojure.string :as str]
   [datahike.index :as di]
   [datahike.datom :as dd :refer [datom datom-tx datom-added datom?]]
   #?(:cljs [datahike.db :refer [HistoricalDB]])
   [datahike.db.interface :as dbi]
   [datahike.db.search :as dbs]
   [datahike.db.utils :as dbu]
   [datahike.constants :refer [tx0]]
   [datahike.tools :refer [get-date raise]]
   [datahike.schema :as ds]
   [me.tonsky.persistent-sorted-set.arrays :as arrays])
  #?(:cljs (:require-macros [datahike.datom :refer [datom]]
                            [datahike.tools :refer [raise]]))
  #?(:clj (:import [clojure.lang ExceptionInfo]
                   [datahike.datom Datom]
                   [datahike.db HistoricalDB]
                   [java.util Date])))

(defn validate-datom [db ^Datom datom]
  (when (and (datom-added datom)
             (dbu/is-attr? db (.-a datom) :db/unique))
    (when-let [found (not-empty (dbi/datoms db :avet [(.-a datom) (.-v datom)]))]
      (raise "Cannot add " datom " because of unique constraint: " found
             {:error :transact/unique :attribute (.-a datom) :datom datom}))))

(defn- validate-val [v [_ _ a _ _ :as at] {:keys [config schema ref-ident-map] :as db}]
  (when (nil? v)
    (raise "Cannot store nil as a value at " at
           {:error :transact/syntax, :value v, :context at}))
  (let [{:keys [attribute-refs? schema-flexibility]} config
        a-ident (if (and attribute-refs? (number? a)) (dbi/-ident-for db a) a)
        v-ident (if (and attribute-refs?
                         (contains? (dbi/-system-entities db) a)
                         (not (nil? (ref-ident-map v))))
                  (ref-ident-map v)
                  v)]

    (when (= :write schema-flexibility)
      (let [schema-spec (if (or (ds/meta-attr? a-ident) (ds/schema-attr? a-ident))
                          ds/implicit-schema-spec
                          schema)]
        (when-not (ds/value-valid? a-ident v-ident schema)
          (raise "Bad entity value " v-ident " at " at ", value does not match schema definition. Must be conform to: "
                 (ds/describe-type (get-in schema-spec [a-ident :db/valueType]))
                 {:error :transact/schema :value v-ident :attribute a-ident :schema (get-in db [:schema a-ident])}))))))

(defn- current-tx [report]
  (inc (get-in report [:db-before :max-tx])))

(defn next-eid [db]
  (inc (:max-eid db)))

(defn- #?@(:clj [^Boolean tx-id?]
           :cljs [^boolean tx-id?])
  [e]
  (or (= e :db/current-tx)
      (= e ":db/current-tx")                                ;; for datahike.js interop
      (= e "datomic.tx")
      (= e "datahike.tx")))

(defn- #?@(:clj [^Boolean tempid?]
           :cljs [^boolean tempid?])
  [x]
  (or (and (number? x) (neg? x)) (string? x)))

(defn advance-max-eid [db eid]
  (cond-> db
    (and (> eid (:max-eid db))
         (< eid tx0))                                 ;; do not trigger advance if transaction id was referenced
    (assoc :max-eid eid)))

(defn- allocate-eid
  ([report eid]
   (update-in report [:db-after] advance-max-eid eid))
  ([report e eid]
   (cond-> report
     (tx-id? e)
     (assoc-in [:tempids e] eid)
     (tempid? e)
     (assoc-in [:tempids e] eid)
     true
     (update-in [:db-after] advance-max-eid eid))))

(defn update-schema [db ^Datom datom]
  (let [schema (dbi/-schema db)
        attribute-refs? (:attribute-refs? (dbi/-config db))
        e (.-e datom)
        a (.-a datom)
        v (.-v datom)
        a-ident (if attribute-refs? (dbi/-ident-for db a) a)
        v-ident (if (and attribute-refs? (contains? (dbi/-system-entities db) v))
                  (dbi/-ident-for db v)
                  v)]
    (when (and attribute-refs? (contains? (dbi/-system-entities db) e))
      (raise "System schema entity cannot be changed"
             {:error :transact/schema :entity-id e}))
    (if (= a-ident :db/ident)
      (if (schema v-ident)
        (raise (str "Schema with attribute " v-ident " already exists")
               {:error :transact/schema :attribute v-ident})
        (-> (assoc-in db [:schema v-ident] (merge (or (schema e) {}) (hash-map a-ident v-ident)))
            (assoc-in [:schema e] v-ident)
            (assoc-in [:ident-ref-map v-ident] e)
            (assoc-in [:ref-ident-map e] v-ident)))
      (if-let [schema-entry (schema e)]
        (if (schema schema-entry)
          (update-in db [:schema schema-entry a-ident] (fn [old]
                                                         (if (ds/entity-spec-attr? a-ident)
                                                           (if old
                                                             (conj old v-ident)
                                                             [v-ident])
                                                           v-ident)))
          (assoc-in db [:schema e a-ident] v-ident))
        (assoc-in db [:schema e] (hash-map a-ident v-ident))))))

(defn update-rschema [db]
  (assoc db :rschema (dbu/rschema (:schema db))))

(defn remove-schema [db ^Datom datom]
  (let [schema (dbi/-schema db)
        attribute-refs? (:attribute-refs? (dbi/-config db))
        e (.-e datom)
        a (.-a datom)
        v (.-v datom)
        a-ident (if attribute-refs? (dbi/-ident-for db a) a)
        v-ident (if (and attribute-refs? (contains? (dbi/-system-entities db) v))
                  (dbi/-ident-for db v)
                  v)]
    (when (and attribute-refs? (contains? (dbi/-system-entities db) e))
      (raise "System schema entity cannot be changed"
             {:error :retract/schema :entity-id e}))
    (if (= a-ident :db/ident)
      (if-not (schema v-ident)
        (let [err-msg (str "Schema with attribute " v-ident " does not exist")
              err-map {:error :retract/schema :attribute v-ident}]
          (throw (ex-info err-msg err-map)))
        (-> (assoc-in db [:schema e] (dissoc (schema v-ident) a-ident))
            (update-in [:schema] #(dissoc % v-ident))
            (update-in [:ident-ref-map] #(dissoc % v-ident))
            (update-in [:ref-ident-map] #(dissoc % e))))
      (if-let [schema-entry (schema e)]
        (if (schema schema-entry)
          (update-in db [:schema schema-entry] #(dissoc % a-ident))
          (update-in db [:schema e] #(dissoc % a-ident v-ident)))
        (let [err-msg (str "Schema with entity id " e " does not exist")
              err-map {:error :retract/schema :entity-id e :attribute a :value e}]
          (throw (ex-info err-msg err-map)))))))

;; In context of `with-datom` we can use faster comparators which
;; do not check for nil (~10-15% performance gain in `transact`)

(defn- with-datom [db ^Datom datom]
  (validate-datom db datom)
  (let [{a-ident :ident} (dbu/attr-info db (.-a datom))
        indexing? (dbu/indexing? db a-ident)
        schema? (or (ds/schema-attr? a-ident) (ds/entity-spec-attr? a-ident))
        keep-history? (and (dbi/-keep-history? db) (not (dbu/no-history? db a-ident)))
        op-count (:op-count db)]
    (if (datom-added datom)
      (cond-> db
        true (update-in [:eavt] #(di/-insert % datom :eavt op-count))
        true (update-in [:aevt] #(di/-insert % datom :aevt op-count))
        indexing? (update-in [:avet] #(di/-insert % datom :avet op-count))
        true (advance-max-eid (.-e datom))
        true (update :hash + (hash datom))
        schema? (-> (update-schema datom)
                    update-rschema)
        true (update :op-count inc))

      (if-some [removing ^Datom (first (dbi/search db [(.-e datom) (.-a datom) (.-v datom)]))]
        (cond-> db
          true (update-in [:eavt] #(di/-remove % removing :eavt op-count))
          true (update-in [:aevt] #(di/-remove % removing :aevt op-count))
          indexing? (update-in [:avet] #(di/-remove % removing :avet op-count))
          true (update :hash - (hash removing))
          schema? (-> (remove-schema datom) update-rschema)
          keep-history? (update-in [:temporal-eavt] #(di/-temporal-insert % removing :eavt op-count))
          keep-history? (update-in [:temporal-eavt] #(di/-temporal-insert % datom :eavt (inc op-count)))
          keep-history? (update-in [:temporal-aevt] #(di/-temporal-insert % removing :aevt op-count))
          keep-history? (update-in [:temporal-aevt] #(di/-temporal-insert % datom :aevt (inc op-count)))
          keep-history? (update :hash + (hash datom))
          (and keep-history? indexing?) (update-in [:temporal-avet] #(di/-temporal-insert % removing :avet op-count))
          (and keep-history? indexing?) (update-in [:temporal-avet] #(di/-temporal-insert % datom :avet (inc op-count)))
          true (update :op-count + (if (or keep-history? indexing?) 2 1)))
        db))))

(defn- with-temporal-datom [db ^Datom datom]
  (let [{a-ident :ident} (dbu/attr-info db (.-a datom))
        indexing? (dbu/indexing? db a-ident)
        schema? (ds/schema-attr? a-ident)
        current-datom ^Datom (first (dbi/search db [(.-e datom) (.-a datom) (.-v datom)]))
        history-datom ^Datom (first (dbs/search-temporal-indices db [(.-e datom) (.-a datom) (.-v datom) (.-tx datom)]))
        current? (not (nil? current-datom))
        history? (not (nil? history-datom))
        op-count (:op-count db)]
    (cond-> db
      current? (update-in [:eavt] #(di/-remove % current-datom :eavt op-count))
      current? (update-in [:aevt] #(di/-remove % current-datom :aevt op-count))
      (and current? indexing?) (update-in [:avet] #(di/-remove % current-datom :avet op-count))
      current? (update :hash - (hash current-datom))
      (and current? schema?) (-> (remove-schema datom) update-rschema)
      history? (update-in [:temporal-eavt] #(di/-remove % history-datom :eavt op-count))
      history? (update-in [:temporal-aevt] #(di/-remove % history-datom :aevt op-count))
      (and history? indexing?) (update-in [:temporal-avet] #(di/-remove % history-datom :avet op-count))
      (or current? history?) (update :op-count inc))))

(defn- queue-tuple [queue tuple idx db e v]
  (let [tuple-value  (or (get queue tuple)
                         (:v (first (dbi/datoms db :eavt [e tuple])))
                         (vec (repeat (-> db (dbi/-schema) (get tuple) :db/tupleAttrs count) nil)))
        tuple-value' (assoc tuple-value idx v)]
    (assoc queue tuple tuple-value')))

(defn- queue-tuples
  "Assuming the attribute we are concerned with is :a and its associated value is 'a',
   returns {:a+b+c [a nil nil], :a+d [a, nil]}"
  [queue tuples db e v]
  (reduce-kv
   (fn [queue tuple idx]
     (queue-tuple queue tuple idx db e v))
   queue
   tuples))

(defn validate-datom-upsert [db ^Datom datom]
  (when (dbu/is-attr? db (.-a datom) :db/unique)
    (when-let [old (first (dbi/datoms db :avet [(.-a datom) (.-v datom)]))]
      (when-not (= (.-e datom) (.-e ^Datom old))
        (raise "Cannot add " datom " because of unique constraint: " old
               {:error     :transact/unique
                :attribute (.-a datom)
                :datom     datom})))))

(defn- with-datom-upsert [db ^Datom datom]
  (validate-datom-upsert db datom)
  (let [indexing?     (dbu/indexing? db (.-a datom))
        {a-ident :ident} (dbu/attr-info db (.-a datom))
        schema?       (ds/schema-attr? a-ident)
        keep-history? (and (dbi/-keep-history? db) (not (dbu/no-history? db a-ident))
                           (not= :db/txInstant a-ident))
        op-count      (:op-count db)
        old-datom (first (di/-slice (:eavt db)
                                    (dd/datom (.-e datom) (.-a datom) nil (.-tx datom))
                                    (dd/datom (.-e datom) (.-a datom) nil (.-tx datom))
                                    :eavt))]
    (cond-> db
            ;; Optimistic removal of the schema entry (because we don't know whether it is already present or not)
      schema? (try
                (-> db (remove-schema datom) update-rschema)
                (catch ExceptionInfo _e
                  db))

      keep-history? (update-in [:temporal-eavt] #(di/-temporal-upsert % datom :eavt op-count old-datom))
      true          (update-in [:eavt] #(di/-upsert % datom :eavt op-count old-datom))

      keep-history? (update-in [:temporal-aevt] #(di/-temporal-upsert % datom :aevt op-count old-datom))
      true          (update-in [:aevt] #(di/-upsert % datom :aevt op-count old-datom))

      (and keep-history? indexing?) (update-in [:temporal-avet] #(di/-temporal-upsert % datom :avet op-count old-datom))
      indexing?                     (update-in [:avet] #(di/-upsert % datom :avet op-count old-datom))

      true    (update :op-count inc)
      true    (advance-max-eid (.-e datom))
      true    (update :hash + (hash datom))
      schema? (-> (update-schema datom) update-rschema))))

(defn- transact-report
  ([report datom] (transact-report report datom false))
  ([report datom upsert?]
   (let [db      (:db-after report)
         a       (:a datom)
         update-fn (if upsert? with-datom-upsert with-datom)
         report' (-> report
                     (update-in [:db-after] update-fn datom)
                     (update-in [:tx-data] conj datom))]
     (if (dbu/tuple-source? db a)
       (let [e      (:e datom)
             v      (if (datom-added datom) (:v datom) nil)
             queue  (or (-> report' ::queued-tuples (get e)) {})
             tuples (get (dbi/-attrs-by db :db/attrTuples) a)
             queue' (queue-tuples queue tuples db e v)]
         (update report' ::queued-tuples assoc e queue'))
       report'))))

(defn- check-upsert-conflict [entity acc]
  (let [[e a v] acc
        _e (:db/id entity)]
    (if (or (nil? _e)
            (tempid? _e)
            (nil? acc)
            (== _e e))
      acc
      (raise "Conflicting upsert: " [a v] " resolves to " e
             ", but entity already has :db/id " _e
             {:error :transact/upsert
              :entity entity
              :assertion acc}))))

(defn- upsert-eid [db entity tempids] ;; TODO: adjust to datascript?
  (when-let [unique-idents (not-empty (dbi/-attrs-by db :db.unique/identity))]
    (->>
     (reduce-kv
      (fn [acc a-ident v-original]                                 ;; acc = [e a v]
        (if-not (contains? unique-idents a-ident)
          acc
          (let [a (if (:attribute-refs? (dbi/-config db))
                    (dbi/-ref-for db a-ident)
                    a-ident)
                tempid-val (and (dbu/ref? db a-ident) (tempid? v-original))
                v (if tempid-val
                    (tempids v-original)
                    v-original)]
            (if-some [e (when v
                          (validate-val v [nil nil a v nil] db)
                          (:e (first (dbi/datoms db :avet [a v]))))]
              (cond
                (nil? acc) [e a v]                    ;; first upsert
                (= (get acc 0) e) acc                 ;; second+ upsert, but does not conflict
                :else
                (let [[_e _a _v] acc]
                  (raise "Conflicting upserts: " [_a _v] " resolves to " _e
                         ", but " [a v] " resolves to " e
                         {:error :transact/upsert
                          :entity entity
                          :assertion [e a v]
                          :conflict [_e _a _v]})))
              acc))))                                   ;; upsert attr, but resolves to nothing                                      ;; non-upsert attr
      nil
      entity)
     (check-upsert-conflict entity)
     first)))                                         ;; getting eid from acc

;; multivals/reverse can be specified as coll or as a single value, trying to guess
(defn- maybe-wrap-multival [db a-ident vs]
  (cond
    ;; not a multival context
    (not (or (dbu/reverse-ref? a-ident)
             (dbu/multival? db a-ident)))
    [vs]

    ;; not a collection at all, so definitely a single value
    (not (or (arrays/array? vs)
             (and (coll? vs) (not (map? vs)))))
    [vs]

    ;; probably lookup ref, but not an entity spec
    (and (= (count vs) 2)
         (keyword? (first vs))
         (dbu/is-attr? db (first vs) :db.unique/identity)
         (not (ds/entity-spec-attr? a-ident)))
    [vs]

    :else vs))

(defn- explode [db entity]
  (let [eid (:db/id entity)
        attribute-refs? (:attribute-refs? (dbi/-config db))
        _ (when (and attribute-refs? (contains? (dbi/-system-entities db) eid))
            (raise "Entity with ID " eid " is a system attribute " (dbi/-ident-for db eid) " and cannot be changed"
                   {:error :transact/syntax, :eid eid, :attribute (dbi/-ident-for db eid) :context entity}))
        ensure (:db/ensure entity)
        entities (for [[a-ident vs] entity
                       :when (not (or (= a-ident :db/id) (= a-ident :db/ensure)))
                       :let [_ (dbu/validate-attr-ident a-ident {:db/id eid, a-ident vs} db)
                             reverse? (dbu/reverse-ref? a-ident)
                             straight-a-ident (if reverse? (dbu/reverse-ref a-ident) a-ident)
                             straight-a (if attribute-refs?
                                          (dbi/-ref-for db straight-a-ident) ;; translation to datom format
                                          straight-a-ident)
                             _ (when (and reverse? (not (dbu/ref? db straight-a-ident)))
                                 (raise "Bad attribute " a-ident ": reverse attribute name requires {:db/valueType :db.type/ref} in schema"
                                        {:error :transact/syntax, :attribute a-ident, :context {:db/id eid, a-ident vs}}))]
                       v (maybe-wrap-multival db a-ident vs)]
                   (if (and (dbu/ref? db straight-a-ident) (map? v)) ;; another entity specified as nested map
                     (assoc v (dbu/reverse-ref a-ident) eid)
                     (if reverse?
                       [:db/add v straight-a eid]
                       [:db/add eid straight-a
                        (if (and attribute-refs?
                                 (dbu/is-attr? db straight-a-ident :db/systemAttribRef)
                                 (ds/is-system-keyword? v)) ;; translation of system enums
                          (dbi/-ref-for db v)
                          v)])))]
    (if ensure
      (let [{:keys [:db.entity/attrs :db.entity/preds]} (-> db :schema ensure)]
        (if (empty? attrs)
          (if (empty? preds)
            entities
            (concat entities [[:db.ensure/preds eid ensure preds]]))
          (if (empty? preds)
            (concat entities [[:db.ensure/attrs eid ensure attrs]])
            (concat entities [[:db.ensure/attrs eid ensure attrs]
                              [:db.ensure/preds eid ensure preds]]))))
      entities)))

(defn- transact-add [{:keys [db-after] :as report} [_ e a v tx :as ent]]
  (let [a (dbu/normalize-and-validate-attr a ent db-after)
        _ (validate-val v ent db-after)
        attribute-refs? (:attribute-refs? (dbi/-config db-after))
        tx (or tx (current-tx report))
        db db-after
        e (dbu/entid-strict db e)
        a-ident (if attribute-refs? (dbi/-ident-for db a) a)
        v (if (dbu/ref? db a-ident) (dbu/entid-strict db v) v)
        new-datom (datom e a v tx)
        upsert? (not (dbu/multival? db a))]
    (transact-report report new-datom upsert?)))

(defn- transact-retract-datom
  ([report ^Datom d] (transact-retract-datom report d false))
  ([report ^Datom d keep-tx-id]
   (let [txid (or (and keep-tx-id (datom-tx d)) (current-tx report))]
     (transact-report report (datom (.-e d) (.-a d) (.-v d) txid false)))))

(defn- transact-purge-datom [report ^Datom d]
  (update-in report [:db-after] with-temporal-datom d))

(defn- retract-components [db datoms]
  (into #{} (comp
             (filter (fn [^Datom d] (dbu/component? db (.-a d))))
             (map (fn [^Datom d] [:db.fn/retractEntity (.-v d)]))) datoms))

(defn- purge-components [db datoms]
  (let [xf (comp
            (filter (fn [^Datom d] (dbu/component? db (.-a d))))
            (map (fn [^Datom d] [:db.purge/entity (.-v d)])))]
    (into #{} xf datoms)))

(declare transact-tx-data)

(defn- retry-with-tempid [initial-report report es tempid upserted-eid]
  (if (contains? (:tempids initial-report) tempid)
    (raise "Conflicting upsert: " tempid " resolves"
           " both to " upserted-eid " and " (get-in initial-report [:tempids tempid])
           {:error :transact/upsert})
    ;; try to re-run from the beginning
    ;; but remembering that `tempid` will resolve to `upserted-eid`
    (let [tempids' (-> (:tempids report)
                       (assoc tempid upserted-eid))
          report' (assoc initial-report :tempids tempids')]
      (transact-tx-data report' es))))

(defn assert-preds [db [_ e _ preds]]
  #?(:cljs (throw (ex-info "tx predicate resolution is not supported in cljs at this time" {:e e :preds preds}))
     :clj
     (reduce
      (fn [coll pred]
        (if ((resolve pred) db e)
          coll
          (conj coll pred)))
      #{} preds)))

(def builtin-op?
  #{:db.fn/call
    :db.fn/cas
    :db/cas
    :db/add
    :db/retract
    :db.fn/retractAttribute
    :db.fn/retractEntity
    :db/retractEntity
    :db/purge
    :db.ensure/attrs
    :db.ensure/preds
    :db.purge/entity
    :db.purge/attribute
    :db.history.purge/before})

(defn flush-tuples
  "Generates all the add or retract operations needed for updating the states of composite tuples.
  E.g., if '::queued-tuples' contains {100 {:a+b+c [123 nil nil]}}, this function creates this vector [:db/add 100 :a+b+c [123 nil nil]]"
  [report]
  (let [db (:db-after report)]
    (reduce-kv
     (fn [entities eid tuples+values]
       (reduce-kv
        (fn [entities tuple value]
          (let [value   (if (every? nil? value) nil value)
                current (:v (first (dbi/datoms db :eavt [eid tuple])))]
            (cond
              (= value current) entities
                ;; adds ::internal to meta-data to mean that these datoms were generated internally.
              (nil? value)      (conj entities ^::internal [:db/retract eid tuple current])
              :else             (conj entities ^::internal [:db/add eid tuple value]))))
        entities
        tuples+values))
     []
     (::queued-tuples report))))

(defn flush-tx-meta
  "Generates add-operations for transaction meta data."
  [{:keys [tx-meta db-before] :as report}]
  (let [;; tx-meta (merge {:db/txInstant (get-date)} tx-meta)
        tid (current-tx report)
        {:keys [attribute-refs?]} (dbi/-config db-before)]
    (reduce-kv
     (fn [entities attribute value]
       (let [straight-a (if attribute-refs? (dbi/-ref-for db-before attribute) attribute)]
         (if (some? straight-a)
           (conj entities
                 [:db/add
                  tid
                  straight-a
                  value
                  tid])
           (raise "Bad transaction meta attribute " attribute " at " tx-meta ", not defined in system or current schema"
                  {:error :transact/schema :attribute attribute :context tx-meta}))))
     []
     tx-meta)))

(defn check-schema-update [db entity new-eid]
  (when (ds/schema-entity? entity)
    (when (and (contains? entity :db/ident)
               (ds/is-system-keyword? (:db/ident entity)))
      (raise "Using namespace 'db' for attribute identifiers is not allowed"
             {:error :transact/schema :entity entity}))
    (if-let [attr-name (get-in db [:schema new-eid])]
      (when-let [invalid-updates (ds/find-invalid-schema-updates entity (get-in db [:schema attr-name]))]
        (when-not (empty? invalid-updates)
          (raise "Update not supported for these schema attributes"
                 {:error :transact/schema :entity entity :invalid-updates invalid-updates})))
      (when (= :write (get-in db [:config :schema-flexibility]))
        (when (or (:db/cardinality entity) (:db/valueType entity))
          (when-not (ds/schema? entity)
            (raise "Incomplete schema transaction attributes, expected :db/ident, :db/valueType, :db/cardinality"
                   {:error :transact/schema :entity entity})))))))

(defn entity-map->op-vec [db {:keys [tempids] :as report} entity]
  (let [old-eid (:db/id entity)
        tx? (tx-id? old-eid) ;; :db/current-tx / "datomic.tx"
        resolved-eid (cond tx?                   (current-tx report)
                           (sequential? old-eid) (dbu/entid-strict db old-eid)
                           :else                 old-eid)
        updated-entity (assoc entity :db/id resolved-eid)
        updated-report (cond-> report
                         tx? (allocate-eid old-eid resolved-eid))
        resolved-tempid (tempids resolved-eid)
        upserted-eid (upsert-eid db updated-entity tempids)]
    (if (and (some? upserted-eid)
             resolved-tempid
             (not= upserted-eid resolved-tempid))
      {:retry? true :old-eid resolved-eid :upserted-eid upserted-eid}
      (let [new-eid (cond
                      (some? upserted-eid)   upserted-eid
                      (nil? resolved-eid)    (next-eid db)
                      (tempid? resolved-eid) (or resolved-tempid (next-eid db))
                      (number? resolved-eid) resolved-eid
                      :else (raise "Expected number, string or lookup ref for :db/id, got " old-eid
                                   {:error :entity-id/syntax, :entity updated-entity}))
            new-entity (assoc updated-entity :db/id new-eid)]
        (check-schema-update db updated-entity new-eid)
        {:new-report (allocate-eid updated-report resolved-eid new-eid)
         :new-entities (explode db new-entity)}))))

(defn compare-and-swap [db report op-vec]
  (let [[_ e a ov nv] op-vec
        e (dbu/entid-strict db e)
        _ (dbu/validate-attr a op-vec db)
        nv (if (dbu/ref? db a) (dbu/entid-strict db nv) nv)
        datoms (dbi/search db [e a])]
    (if (nil? ov)
      (if (empty? datoms)
        [(transact-add report [:db/add e a nv]) []]
        (raise ":db.fn/cas failed on datom [" e " " a " " (if (dbu/multival? db a) (map :v datoms) (:v (first datoms))) "], expected nil"
               {:error :transact/cas, :old (if (dbu/multival? db a) datoms (first datoms)), :expected ov, :new nv}))
      (let [ov (if (dbu/ref? db a) (dbu/entid-strict db ov) ov)]
        (validate-val nv op-vec db)
        (if (dbu/multival? db a)
          (if (some (fn [^Datom d] (= (.-v d) ov)) datoms)
            [(transact-add report [:db/add e a nv]) []]
            (raise ":db.fn/cas failed on datom [" e " " a " " (map :v datoms) "], expected " ov
                   {:error :transact/cas, :old datoms, :expected ov, :new nv}))
          (let [v (:v (first datoms))]
            (if (= v ov)
              [(transact-add report [:db/add e a nv]) []]
              (raise ":db.fn/cas failed on datom [" e " " a " " v "], expected " ov
                     {:error :transact/cas, :old (first datoms), :expected ov, :new nv}))))))))

(defn retract-entity [db report op-vec]
  (let [[_ e] op-vec]
    (if-let [e (dbu/entid db e)]
      (let [e-datoms (vec (dbi/search db [e]))
            v-datoms (->> (dbi/-attrs-by db :db.type/ref)
                          (map (partial dbi/-ident-for db))
                          (mapcat (fn [a] (dbi/search db [nil a e])))
                          vec)]
        [(reduce transact-retract-datom report (concat e-datoms v-datoms))
         (retract-components db e-datoms)])
      [report []])))

(defn check-tuple [db op-vec]
  (let [[_ _ a v] op-vec
        attr-schema (-> db dbi/-schema (get a))]
    (cond (:db/tupleType attr-schema)
          (cond (> (count v) 8)
                (raise "Cannot store more than 8 values for homogeneous tuple: " op-vec
                       {:error :transact/syntax, :tx-data op-vec})

                (not (apply = (map type v)))
                (raise "Cannot store homogeneous tuple with values of different type: " op-vec
                       {:error :transact/syntax, :tx-data op-vec})

                (not (s/valid? (-> db dbi/-schema a :db/tupleType) (first v)))
                (raise "Cannot store homogeneous tuple. Values are of wrong type: " op-vec
                       {:error :transact/syntax, :tx-data op-vec}))
          (:db/tupleTypes attr-schema)
          (cond (not (= (count v) (count (:db/tupleTypes attr-schema))))
                (raise (str "Cannot store heterogeneous tuple: expecting " (count (:db/tupleTypes attr-schema)) " values, got " (count v))
                       {:error :transact/syntax, :tx-data op-vec})

                (not (apply = (map s/valid? (:db/tupleTypes attr-schema) v)))
                (raise (str "Cannot store heterogeneous tuple: there is a mismatch between values " v " and their types " (:db/tupleTypes attr-schema))
                       {:error :transact/syntax, :tx-data op-vec}))
          (and (:db/tupleAttrs attr-schema)
               (not (::internal (meta op-vec))))
          (raise "Can’t modify tuple attrs directly: " op-vec
                 {:error :transact/syntax, :tx-data op-vec}))))

(defn- filter-before [datoms ^Date before-date db]
  (let [before-pred (fn [^Datom d]
                      (.before ^Date (.-v d) before-date))
        filtered-tx-ids (dbu/filter-txInstant datoms before-pred db)]
    (filter
     (fn [^Datom d]
       (contains? filtered-tx-ids (datom-tx d)))
     datoms)))

(defn apply-db-op [db report op-vec]
  (let [[op e a v] op-vec]
    (case op

      :db/add [(transact-add report op-vec) []]

      :db/retract (if-some [e (dbu/entid db e)]
                    (let [a (dbu/normalize-and-validate-attr a op-vec db)
                          pattern (if (nil? v)
                                    [e a]
                                    (let [v (if (dbu/ref? db a) (dbu/entid-strict db v) v)]
                                      (validate-val v op-vec db)
                                      [e a v]))
                          datoms (vec (dbi/search db pattern))]
                      [(reduce transact-retract-datom report datoms) []])
                    [report []])

      :db.fn/retractAttribute (if-let [e (dbu/entid db e)]
                                (let [a (dbu/normalize-and-validate-attr a op-vec db)
                                      datoms (vec (dbi/search db [e a]))]
                                  [(reduce transact-retract-datom report datoms)
                                   (retract-components db datoms)])
                                [report []])

      :db.fn/retractEntity (retract-entity db report op-vec)

      :db/retractEntity (retract-entity db report op-vec)

      :db/purge (if (dbi/-keep-history? db)
                  (let [history (HistoricalDB. db)]
                    (if-some [e (dbu/entid history e)]
                      (let [v (if (dbu/ref? history a) (dbu/entid-strict history v) v)
                            old-datoms (dbi/search history [e a v])]
                        [(reduce transact-purge-datom report old-datoms) []])
                      (raise "Can't find entity with ID " e " to be purged"
                             {:error :transact/purge, :operation op, :tx-data op-vec})))
                  (raise "Purge is only available in temporal databases."
                         {:error :transact/purge :operation op :tx-data op-vec}))

      :db.purge/attribute (if (dbi/-keep-history? db)
                            (let [history (HistoricalDB. db)]
                              (if-let [e (dbu/entid history e)]
                                (let [datoms (vec (dbi/search history [e a]))]
                                  [(reduce transact-purge-datom report datoms)
                                   (purge-components history datoms)])
                                (raise "Can't find entity with ID " e " to be purged"
                                       {:error :transact/purge, :operation op, :tx-data op-vec})))
                            (raise "Purge attribute is only available in temporal databases."
                                   {:error :transact/purge :operation op :tx-data op-vec}))

      :db.purge/entity (if (dbi/-keep-history? db)
                         (let [history (HistoricalDB. db)]
                           (if-let [e (dbu/entid history e)]
                             (let [e-datoms (vec (dbi/search history [e]))
                                   v-datoms (vec (mapcat (fn [a] (dbi/search history [nil a e]))
                                                         (dbi/-attrs-by history :db.type/ref)))]
                               [(reduce transact-purge-datom report (concat e-datoms v-datoms))
                                (purge-components history e-datoms)])
                             (raise "Can't find entity with ID " e " to be purged"
                                    {:error :transact/purge, :operation op, :tx-data op-vec})))
                         (raise "Purge entity is only available in temporal databases."
                                {:error :transact/purge :operation op :tx-data op-vec}))

      :db.history.purge/before (if (dbi/-keep-history? db)
                                 (let [history (HistoricalDB. db)
                                       into-sorted-set #(apply sorted-set-by dd/cmp-datoms-eavt-quick %)
                                       e-datoms (-> (clojure.set/difference
                                                     (into-sorted-set (dbs/search-temporal-indices db nil))
                                                     (into-sorted-set (dbs/search-current-indices db nil)))
                                                    (filter-before e db)
                                                    vec)]
                                   [(reduce transact-purge-datom report e-datoms)
                                    (purge-components history e-datoms)])
                                 (raise "Purge entity is only available in temporal databases."
                                        {:error :transact/purge :operation op :tx-data op-vec}))

      :db.ensure/attrs (let [{:keys [tx-data]} report
                             asserting-datoms (filter (fn [^Datom d] (= e (.-e d))) tx-data)
                             asserting-attributes (map (fn [^Datom d] (.-a d)) asserting-datoms)
                             diff (clojure.set/difference (set v) (set asserting-attributes))]
                         (if (empty? diff)
                           [report []]
                           (raise "Entity " e " missing attributes " diff " of spec " a
                                  {:error :transact/ensure :operation op :tx-data op-vec
                                   :asserting-datoms asserting-datoms})))

      :db.ensure/preds (let [{:keys [db-after]} report
                             preds (assert-preds db-after op-vec)]
                         (if-not (empty? preds)
                           (raise "Entity " e " failed predicates " preds " of spec " a
                                  {:error :transact/ensure :operation op :tx-data op-vec})
                           [report []]))

      :db.fn/cas (compare-and-swap db report op-vec)

      :db/cas (compare-and-swap db report op-vec)

      :db.fn/call (let [[_ f & args] op-vec]
                    [report (apply f db args)])

      (if (and (keyword? op)
               (not (builtin-op? op)))
        (if-some [ident (dbu/entid db op)]
          (let [fun (-> (dbi/search db [ident :db/fn]) first :v)
                args (next op-vec)]
            (if (fn? fun)
              [report (apply fun db args)]
              (raise "Entity " op " expected to have :db/fn attribute with fn? value"
                     {:error :transact/syntax, :operation :db.fn/call, :tx-data op-vec})))
          (raise "Can’t find entity for transaction fn " op
                 {:error :transact/syntax, :operation :db.fn/call, :tx-data op-vec}))
        (raise (str "Unknown operation at " op-vec ", expected " (str/join "," builtin-op?)
                    " or an ident corresponding to an installed transaction function"
                    " (e.g. {:db/ident <keyword> :db/fn <Ifn>}, usage of :db/ident requires {:db/unique :db.unique/identity} in schema)")
               {:error :transact/syntax, :operation op, :tx-data op-vec})))))

(defn transact-tx-data [{:keys [db-before] :as initial-report} initial-es]
  (when-not (or (nil? initial-es)
                (sequential? initial-es))
    (raise "Bad transaction data " initial-es ", expected sequential collection"
           {:error :transact/syntax, :tx-data initial-es}))
  (let [has-tuples? (seq (dbi/-attrs-by (:db-after initial-report) :db.type/tuple))
        initial-es' (if has-tuples?
                      (interleave initial-es (repeat ::flush-tuples))
                      initial-es)
        initial-report (update initial-report :tx-meta
                               #(merge {:db/txInstant (get-date)} %))
        meta-entities (flush-tx-meta initial-report)]
    (loop [report (update initial-report :db-after transient)
           es (if (dbi/-keep-history? db-before)
                (concat meta-entities
                        initial-es')
                initial-es')]
      (let [[entity & entities] es
            {:keys [tempids db-after]} report
            db db-after]
        (cond
          (empty? es)
          (-> report
              (assoc-in [:tempids :db/current-tx] (current-tx report))
              (update-in [:db-after :max-tx] inc)
              (update :db-after persistent!))

          (nil? entity)
          (recur report entities)

          (= ::flush-tuples entity)
          (if (contains? report ::queued-tuples)
            (recur
             (dissoc report ::queued-tuples)
             (concat (flush-tuples report) entities))
            (recur report entities))

          (map? entity)
          (let [{:keys [new-report new-entities retry? old-eid upserted-eid]} (entity-map->op-vec db report entity)]
            (if retry?
              (retry-with-tempid initial-report report initial-es old-eid upserted-eid)
              (recur new-report (concat new-entities entities))))

          (sequential? entity)
          (let [[op e a v] entity]
            (when (dbu/tuple? db a)
              (check-tuple db entity))
            (cond

              (tx-id? e)
              (recur (allocate-eid report e (current-tx report)) (cons [op (current-tx report) a v] entities))

              (and (dbu/ref? db a) (tx-id? v))
              (recur (allocate-eid report v (current-tx report)) (cons [op e a (current-tx report)] entities))

              (tempid? e)
              (if (not= op :db/add)
                (raise "Can't use tempid in '" entity "'. Tempids are allowed in :db/add only"
                       {:error :transact/syntax, :op entity})
                (let [upserted-eid (when (dbu/is-attr? db a :db.unique/identity)
                                     (:e (first (dbi/datoms db :avet [a v]))))
                      allocated-eid (get tempids e)]
                  (if (and upserted-eid allocated-eid (not= upserted-eid allocated-eid))
                    (retry-with-tempid initial-report report initial-es e upserted-eid)
                    (let [eid (or upserted-eid allocated-eid (next-eid db))]
                      (recur (allocate-eid report e eid) (cons [op eid a v] entities))))))

              (and (dbu/ref? db a) (tempid? v))
              (if-let [vid (get tempids v)]
                (recur report (cons [op e a vid] entities))
                (recur (allocate-eid report v (next-eid db)) es))

              :else
              (let [[new-report new-entities] (apply-db-op db report entity)]
                (recur new-report (concat new-entities entities)))))

          (datom? entity)
          (let [[e a v tx added] entity]
            (if added
              (recur (transact-add report [:db/add e a v tx]) entities)
              (recur (transact-retract-datom report entity true) entities)))

          :else
          (raise "Bad entity type at " entity ", expected map or vector"
                 {:error :transact/syntax, :tx-data entity}))))))

(defn transact-entities-directly [initial-report initial-es]
  (loop [report (update initial-report :db-after transient)
         es initial-es
         migration-state (or (get-in initial-report [:db-before :migration]) {})]
    (let [[entity & entities] es
          {:keys [config] :as db} (:db-after report)
          [e a v t op] entity
          a-ident (if (and (number? a) (:attribute-refs? config))
                    (dbi/-ident-for db a)
                    a)
          a (if (:attribute-refs? config)
              (dbi/-ref-for db a-ident)
              (if (number? a)
                (raise "Configuration mismatch: import data with attribute references can not be imported into a database with no attribute references."
                       {:error :import/mismatch :data entity})
                a-ident))
          max-eid (next-eid db)
          max-tid (inc (get-in report [:db-after :max-tx]))]
      (cond
        (empty? es)
        (-> report
            (update-in [:db-after :max-tx] inc)
            (update-in [:db-after :migration] #(if %
                                                 (merge % migration-state)
                                                 migration-state))
            (update :db-after persistent!))

        (= :db.install/attribute a-ident)
        (recur report entities migration-state)

        ;; meta entity
        (ds/meta-attr? a-ident)
        (let [new-t (get-in migration-state [:tids t] max-tid)
              new-datom (dd/datom new-t a v new-t op)
              new-e (.-e new-datom)
              upsert? (not (dbu/multival? db a-ident))]
          (recur (-> (transact-report report new-datom upsert?)
                     (assoc-in [:db-after :max-tx] max-tid))
                 entities
                 (-> migration-state
                     (assoc-in [:tids e] new-e)
                     (assoc-in [:eids e] new-e))))

        ;; tx not added yet
        (nil? (get-in migration-state [:tids t]))
        (recur (update-in report [:db-after :max-tx] inc) es (assoc-in migration-state [:tids t] max-tid))

        ;; ref not added yet
        (and (dbu/ref? db a) (nil? (get-in migration-state [:eids v])))
        (recur (allocate-eid report max-eid) es (assoc-in migration-state [:eids v] max-eid))

        :else
        (let [new-datom ^Datom (dd/datom
                                (or (get-in migration-state [:eids e]) max-eid)
                                a
                                (if (dbu/ref? db a)
                                  (get-in migration-state [:eids v])
                                  v)
                                (get-in migration-state [:tids t])
                                op)
              upsert? (and (not (dbu/multival? db a-ident))
                           op)]
          (recur (transact-report report new-datom upsert?) entities (assoc-in migration-state [:eids e] (.-e new-datom))))))))
