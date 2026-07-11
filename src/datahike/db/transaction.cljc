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
   [datahike.bitemporal.platform :as bp]
   [datahike.constants :refer [tx0]]
   [datahike.tools :refer [get-date date->epoch-ms]]
   [replikativ.logging :as log]
   [datahike.schema :as ds]
   [datahike.index.secondary :as sec]
   [hasch.core :as hasch]
   [org.replikativ.persistent-sorted-set.arrays :as arrays])
  #?(:cljs (:require-macros [datahike.datom :refer [datom]]))
  #?(:clj (:import [clojure.lang ExceptionInfo]
                   [datahike.datom Datom]
                   [datahike.db HistoricalDB]
                   [java.util Date])))

(defn validate-datom [db ^Datom datom]
  (when (and (datom-added datom)
             (dbu/is-attr? db (.-a datom) :db/unique))
    (when-let [found (not-empty (dbi/datoms db :avet [(.-a datom) (.-v datom)]))]
      (log/raise "Cannot add " datom " because of unique constraint: " found
                 {:error :transact/unique :attribute (.-a datom) :datom datom}))))

(defn- validate-val [v [_ _ a _ _ :as at] {:keys [config schema ref-ident-map] :as db}]
  (when (nil? v)
    (log/raise "Cannot store nil as a value at " at
               {:error :transact/syntax, :value v, :context at}))
  (let [{:keys [attribute-refs? schema-flexibility]} config
        a-ident (if (and attribute-refs? (number? a)) (dbi/ident-for db a :error-on-missing) a)
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
          (log/raise "Bad entity value " v-ident " at " at ", value does not match schema definition. Must be conform to: "
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
        a-ident (if attribute-refs? (dbi/ident-for db a :error-on-missing) a)
        v-ident (if (and attribute-refs? (contains? (dbi/-system-entities db) v))
                  (dbi/ident-for db v :error-on-missing)
                  v)]
    (when (and attribute-refs? (contains? (dbi/-system-entities db) e))
      (log/raise "System schema entity cannot be changed"
                 {:error :transact/schema :entity-id e}))
    (if (= a-ident :db/ident)
      (if (schema v-ident)
        (log/raise (str "Schema with attribute " v-ident " already exists")
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

(defn- last-tx-instant
  "Read the :db/txInstant value of the most-recently-committed tx
   (`:max-tx`). Returns nil only on a truly empty DB that has not
   even seen the bootstrap tx — in practice always returns at least
   the epoch sentinel from `tx0`."
  [db]
  (let [max-tx (:max-tx db)
        ctx    (dbi/-search-context db)
        a      (if (:attribute-refs? (dbi/-config db))
                 (dbi/-ref-for db :db/txInstant)
                 :db/txInstant)]
    (some-> #?(:clj ^Datom (first (dbi/-datoms db :eavt [max-tx a] ctx))
               :cljs (first (dbi/-datoms db :eavt [max-tx a] ctx)))
            .-v)))

(defn ^:dynamic next-tx-instant
  "Strictly-monotonic `:db/txInstant` allocator. Returns
   `max(get-date, prev-tx-instant + 1ms)` so back-to-back writes
   that share a wall-clock millisecond still get distinct,
   strictly-ordered instants — matching Datomic's contract for
   auto-stamped `:db/txInstant` and removing the `d/as-of <Date>`
   tied-instant ambiguity at the source.

   Reads the wall-clock via `datahike.tools/get-date`, so the
   existing dynamic-binding clock-pinning patterns keep working.
   A pinned constant clock turns into a *logical clock* for free:
   the allocator advances by 1ms per tx, producing deterministic
   monotonic stamps across the whole binding without any separate
   logical-clock mode.

   User-provided `:db/txInstant` in `:tx-meta` still wins over the
   allocator's default; historical imports and SCD2 surgery tests
   that intentionally back-date are unaffected.

   Cost: one EAVT seek (~1µs on PSS in-memory at 10k txes; <0.3%
   of `d/transact`). The `^:dynamic` shape leaves room for a
   future caller-supplied allocator (e.g., HLC) without breaking
   the default path.

   See ADR for the design rationale."
  [db-before]
  (let [now-ms  (date->epoch-ms (get-date))
        prev-ms (some-> (last-tx-instant db-before) date->epoch-ms)
        ms      (if (or (nil? prev-ms) (< (long prev-ms) (long now-ms)))
                  (long now-ms)
                  (inc (long prev-ms)))]
    #?(:clj  (Date. ms)
       :cljs (js/Date. ms))))

#?(:clj
   (defn- attrs-have-datoms?
     "True iff at least one of `attrs` has any datoms in `db`'s AEVT index.
      Used by `instantiate-secondary` to skip the async backfill path when
      the index is being registered on an empty (or empty-for-these-attrs)
      database — eliminating the writer race between
      `build-secondary-index!` and subsequent user writes that would
      otherwise clobber the live updates with an empty rebuild.

      Attrs not declared in the schema have no datoms by definition;
      `dbi/-datoms` validates the attr ident and throws otherwise. A
      secondary-index spec can legitimately reference an attr that
      hasn't been declared yet (schema-flexibility :read, or the index
      is registered before any data writes touch the attr), so we
      gate the lookup on schema membership."
     [db attrs]
     (let [ctx    (dbi/-search-context db)
           schema (dbi/-schema db)]
       (boolean
        (some (fn [a]
                (and (contains? schema a)
                     (seq (dbi/-datoms db :aevt [a] ctx))))
              attrs)))))

#?(:clj
   (defn- instantiate-secondary
     "Create the secondary-index instance for `idx-ident` from a fully
      populated schema entry. Used at end-of-tx so the factory sees the
      complete config (including `:db.secondary/config`, which may have
      been applied as a later datom within the same tx).

      Status auto-detection: if AEVT has no datoms for any of the
      indexed attrs, the index is ready immediately (no backfill
      needed) and status is `:ready`. Otherwise status is `:building`
      and the writer auto-dispatches `build-secondary-index!` to
      backfill from AEVT. Common case (register on a fresh-or-empty
      db) avoids the async-build race entirely."
     [db idx-ident idx-schema]
     (let [idx-type (:db.secondary/type idx-schema)
           idx-attrs (set (:db.secondary/attrs idx-schema))
           idx-config (cond-> (merge (:db.secondary/config idx-schema)
                                     {:attrs idx-attrs})
                        (seq (:ident-ref-map db))
                        (assoc :ident-ref-map (:ident-ref-map db)))
           idx (sec/create-index idx-type idx-config nil)
           needs-backfill? (attrs-have-datoms? db idx-attrs)
           base (-> db
                    (assoc-in [:secondary-indices idx-ident] idx)
                    (assoc-in [:schema idx-ident :db.secondary/status]
                              (if needs-backfill? :building :ready)))]
       (if needs-backfill?
         (assoc-in base [:schema idx-ident :db.secondary/building-since-tx]
                   (:max-tx db))
         base)))
   :cljs
   (defn- instantiate-secondary [db _idx-ident _idx-schema] db))

#?(:clj
   (defn finalize-secondary-indices
     "Walk the post-tx schema for secondary-index entities that have
      `:db.secondary/type` + `:db.secondary/attrs` but no instance yet,
      and create them with their full config.

      Called at the end of `transact-tx-data` so the factory sees a
      complete schema entry — fixes the race where instantiating per-
      datom would call the factory before `:db.secondary/config` had
      been applied. The writer's auto-backfill (status :building →
      `build-secondary-index!`) populates the new index from AEVT, so
      data datoms applied in the same tx are picked up asynchronously."
     [db]
     (reduce-kv
      (fn [d k entry]
        (if (and (keyword? k)
                 (map? entry)
                 (:db.secondary/type entry)
                 (:db.secondary/attrs entry)
                 (not (get-in d [:secondary-indices k])))
          (instantiate-secondary d k entry)
          d))
      db (:schema db)))
   :cljs
   (defn finalize-secondary-indices [db] db))

(defn remove-schema [db ^Datom datom]
  (let [schema (dbi/-schema db)
        attribute-refs? (:attribute-refs? (dbi/-config db))
        e (.-e datom)
        a (.-a datom)
        v (.-v datom)
        a-ident (if attribute-refs? (dbi/ident-for db a :error-on-missing) a)
        v-ident (if (and attribute-refs? (contains? (dbi/-system-entities db) v))
                  (dbi/ident-for db v :error-on-missing)
                  v)]
    (when (and attribute-refs? (contains? (dbi/-system-entities db) e))
      (log/raise "System schema entity cannot be changed"
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

(def meta-attrs-for-secondary
  "Tx-meta attrs surfaced to secondary indices on each `-transact` call.
   See `datahike.index.secondary/ISecondaryIndex` — adapters that want
   vt-pushdown read these from `tx-report :tx-meta`."
  #{:db/txInstant :db.valid/from :db.valid/to})

(defn meta-for-tx-id
  "Return the meta-attrs map for the given tx-id by EAVT-seeking the tx
   entity. Public so writing/build-secondary-index! can reconstruct
   tx-meta during backfill (where the writing tx is not the in-progress
   tx). Returns nil when no meta-attrs are set on that tx."
  [db ^long tx-id]
  (let [config (dbi/-config db)
        ref? (:attribute-refs? config)
        ident (fn [a] (if (and ref? (number? a)) (dbi/-ident-for db a) a))
        m (reduce
           (fn [m ^Datom d]
             (let [a-ident (ident (.-a d))]
               (if (contains? meta-attrs-for-secondary a-ident)
                 (assoc m a-ident (.-v d))
                 m)))
           {}
           (dbi/-datoms db :eavt [tx-id] (dbi/-search-context db)))]
    (not-empty m)))

(defn- tx-meta-for-secondary
  "Tx-meta for the *current* in-progress tx. We use `(inc max-tx)` —
   incrementing matches the final bump in the transact loop's exit
   branch — rather than `(dd/datom-tx datom)`. Retract datoms carry
   the original asserting tx's id, but vt-aware adapters need the
   writing tx's meta to close `_valid_to` correctly."
  [db ^Datom _datom]
  (meta-for-tx-id db (inc (long (:max-tx db)))))

(defn- update-secondary-indices
  "Update all secondary indices that cover the given attribute.
   When the index supports ITransientSecondaryIndex (batch mode), uses
   -transact! (mutates in place, no assoc). Otherwise falls back to
   -transact (persistent, returns new instance).
   Returns updated db with modified secondary-indices map.

   Per bitemporal-v1, `tx-report` also carries `:tx-meta` — the
   tx-entity's `:db/txInstant` / `:db.valid/from` / `:db.valid/to`
   attrs — so adapters that implement `IValidTimeAware` can persist
   the tx's valid-time window alongside their content keys."
  [db a-ident ^Datom datom added?]
  (let [sec-idx-map (get-in db [:rschema :db.secondary/index a-ident])]
    (if (seq sec-idx-map)
      (let [tx-report (cond-> {:datom datom :added? added?}
                        true (assoc :tx-meta (tx-meta-for-secondary db datom)))]
        (reduce (fn [db' idx-ident]
                  (let [status (get-in db' [:schema idx-ident :db.secondary/status])]
                    ;; Skip disabled indices — they are no longer maintained
                    (if (= :disabled status)
                      db'
                      (if-let [idx (get-in db' [:secondary-indices idx-ident])]
                        (if (satisfies? sec/ITransientSecondaryIndex idx)
                          (do (sec/-transact! idx tx-report) db')
                          (assoc-in db' [:secondary-indices idx-ident]
                                    (sec/-transact idx tx-report)))
                        db'))))
                db sec-idx-map))
      db)))

(defn secondary-only-hash
  "Content hash (string) of `v`, via hasch — the value the primary indexes hold
   for a `:db.secondary/only` attribute. Deterministic, so retraction re-hashes
   to find the stored datom and identical values dedup."
  [v]
  (str (hasch/uuid v)))

(defn- project-primary
  "For an *added* `:db.secondary/only` datom, a copy with its value replaced by
   the content hash (what the primary EAVT/AEVT/AVET store). Preserves e/a/tx.
   The full-value `datom` still flows to the secondary index. Retraction datoms
   already carry the stored hash (the retract handler searches by hash), so they
   pass through unchanged — no double-hashing."
  [secondary-only? ^Datom datom]
  (if (and secondary-only? (datom-added datom))
    (dd/datom (.-e datom) (.-a datom) (secondary-only-hash (.-v datom)) (.-tx datom) (datom-added datom))
    datom))

(defn- with-datom [db ^Datom datom]
  (validate-datom db datom)
  (let [{a-ident :ident} (dbu/attr-info db (.-a datom) :error-on-missing)
        indexing? (dbu/indexing? db a-ident)
        schema? (or (ds/schema-attr? a-ident) (ds/entity-spec-attr? a-ident)
                    (ds/secondary-index-attr? a-ident))
        keep-history? (and (dbi/-keep-history? db) (not (dbu/no-history? db a-ident)))
        op-count (:op-count db)
        has-secondary? (seq (get-in db [:rschema :db.secondary/index a-ident]))
        secondary-only? (dbu/secondary-only? db a-ident)
        ;; primary indexes hold the content hash for :db.secondary/only attrs;
        ;; the full value goes only to the secondary index (`datom` below).
        prim ^Datom (project-primary secondary-only? datom)]
    (when (and secondary-only? (datom-added datom) (not has-secondary?))
      (log/raise "Attribute " a-ident " is :db.secondary/only but no secondary index covers it — its value would be lost"
                 {:error :transact/secondary-only-uncovered :attribute a-ident :datom datom}))
    (if (datom-added datom)
      (cond-> db
        true (update-in [:eavt] #(di/-insert % prim :eavt op-count))
        true (update-in [:aevt] #(di/-insert % prim :aevt op-count))
        indexing? (update-in [:avet] #(di/-insert % prim :avet op-count))
        has-secondary? (update-secondary-indices a-ident datom true)
        true (advance-max-eid (.-e datom))
        true (update :hash + (hash prim))
        schema? (-> (update-schema datom)
                    update-rschema)
        true (update :op-count inc))

      (if-some [removing ^Datom (first (dbi/search db [(.-e prim) (.-a prim) (.-v prim)]))]
        (cond-> db
          true (update-in [:eavt] #(di/-remove % removing :eavt op-count))
          true (update-in [:aevt] #(di/-remove % removing :aevt op-count))
          indexing? (update-in [:avet] #(di/-remove % removing :avet op-count))
          has-secondary? (update-secondary-indices a-ident datom false)
          true (update :hash - (hash removing))
          schema? (-> (remove-schema datom) update-rschema)
          keep-history? (update-in [:temporal-eavt] #(di/-temporal-insert % removing :eavt op-count))
          keep-history? (update-in [:temporal-eavt] #(di/-temporal-insert % prim :eavt (inc op-count)))
          keep-history? (update-in [:temporal-aevt] #(di/-temporal-insert % removing :aevt op-count))
          keep-history? (update-in [:temporal-aevt] #(di/-temporal-insert % prim :aevt (inc op-count)))
          keep-history? (update :hash + (hash prim))
          (and keep-history? indexing?) (update-in [:temporal-avet] #(di/-temporal-insert % removing :avet op-count))
          (and keep-history? indexing?) (update-in [:temporal-avet] #(di/-temporal-insert % prim :avet (inc op-count)))
          true (update :op-count + (if (or keep-history? indexing?) 2 1)))
        db))))

(defn- with-temporal-datom [db ^Datom datom]
  (let [{a-ident :ident} (dbu/attr-info db (.-a datom) :error-on-missing)
        indexing? (dbu/indexing? db a-ident)
        schema? (ds/schema-attr? a-ident)
        current-datom ^Datom (first (dbi/search db [(.-e datom) (.-a datom) (.-v datom)]))
        history-datom ^Datom (first (dbs/search-temporal-indices db [(.-e datom) (.-a datom) (.-v datom) (.-tx datom)]))
        current? (not (nil? current-datom))
        history? (not (nil? history-datom))
        op-count (:op-count db)
        has-secondary? (seq (get-in db [:rschema :db.secondary/index a-ident]))]
    (cond-> db
      current? (update-in [:eavt] #(di/-remove % current-datom :eavt op-count))
      current? (update-in [:aevt] #(di/-remove % current-datom :aevt op-count))
      (and current? indexing?) (update-in [:avet] #(di/-remove % current-datom :avet op-count))
      current? (update :hash - (hash current-datom))
      ;; Secondary indices represent current state only; emit a retraction
      ;; event so they drop the purged datom. Skip when the datom was only
      ;; in history (already retracted earlier — never reached the secondary).
      (and current? has-secondary?) (update-secondary-indices a-ident current-datom false)
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
        (log/raise "Cannot add " datom " because of unique constraint: " old
                   {:error     :transact/unique
                    :attribute (.-a datom)
                    :datom     datom})))))

(defn- with-datom-upsert [db ^Datom datom]
  (validate-datom-upsert db datom)
  (let [indexing?     (dbu/indexing? db (.-a datom))
        {a-ident :ident} (dbu/attr-info db (.-a datom) :error-on-missing)
        schema?       (or (ds/schema-attr? a-ident)
                          (ds/secondary-index-attr? a-ident))
        keep-history? (and (dbi/-keep-history? db) (not (dbu/no-history? db a-ident))
                           (not= :db/txInstant a-ident))
        op-count      (:op-count db)
        old-datom (first (di/-slice (:eavt db)
                                    (dd/datom (.-e datom) (.-a datom) nil (.-tx datom))
                                    (dd/datom (.-e datom) (.-a datom) nil (.-tx datom))
                                    :eavt))
        has-secondary? (seq (get-in db [:rschema :db.secondary/index a-ident]))
        secondary-only? (dbu/secondary-only? db a-ident)
        ;; primary indexes hold the content hash for :db.secondary/only attrs;
        ;; the full value goes only to the secondary index (`datom` below).
        prim ^Datom (project-primary secondary-only? datom)]
    (when (and secondary-only? (not has-secondary?))
      (log/raise "Attribute " a-ident " is :db.secondary/only but no secondary index covers it — its value would be lost"
                 {:error :transact/secondary-only-uncovered :attribute a-ident :datom datom}))
    (cond-> db
            ;; Optimistic removal of the schema entry (because we don't know whether it is already present or not)
      schema? (try
                (-> db (remove-schema datom) update-rschema)
                (catch ExceptionInfo _e
                  db))

      keep-history? (update-in [:temporal-eavt] #(di/-temporal-upsert % prim :eavt op-count old-datom))
      true          (update-in [:eavt] #(di/-upsert % prim :eavt op-count old-datom))

      keep-history? (update-in [:temporal-aevt] #(di/-temporal-upsert % prim :aevt op-count old-datom))
      true          (update-in [:aevt] #(di/-upsert % prim :aevt op-count old-datom))

      (and keep-history? indexing?) (update-in [:temporal-avet] #(di/-temporal-upsert % prim :avet op-count old-datom))
      indexing?                     (update-in [:avet] #(di/-upsert % prim :avet op-count old-datom))

      ;; Secondary indices: retract old, assert new (full value)
      (and has-secondary? old-datom) (update-secondary-indices a-ident old-datom false)
      has-secondary? (update-secondary-indices a-ident datom true)

      true    (update :op-count inc)
      true    (advance-max-eid (.-e datom))
      true    (update :hash + (hash prim))
      schema? (-> (update-schema datom)
                  update-rschema))))

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
      (log/raise "Conflicting upsert: " [a v] " resolves to " e
                 ", but entity already has :db/id " _e
                 {:error :transact/upsert
                  :entity entity
                  :assertion acc}))))

(defn- upsert-eid [db entity tempids] ;; TODO: adjust to datascript?
  (when-let [unique-idents (not-empty (dbi/-attrs-by db :db.unique/identity))]
    (let [unique-tuple-idents (clojure.set/intersection
                               (dbi/-attrs-by db :db.type/tuple)
                               unique-idents)
          found-eav
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
                       (log/raise "Conflicting upserts: " [_a _v] " resolves to " _e
                                  ", but " [a v] " resolves to " e
                                  {:error :transact/upsert
                                   :entity entity
                                   :assertion [e a v]
                                   :conflict [_e _a _v]})))
                   acc))))                                   ;; upsert attr, but resolves to nothing                                      ;; non-upsert attr
           nil
           entity)

          found-eav-including-composite-tuples
          (reduce
           (fn [acc a-tuple]
             (let [tuple-attrs             (get-in (dbi/-schema db) [a-tuple :db/tupleAttrs])
                   contains-tuple-attrs?   (clojure.set/subset?
                                            (set tuple-attrs)
                                            (set (keys entity)))
                   tuple-contains-tempids? (and contains-tuple-attrs?
                                                (some (fn [a] (and (dbu/ref? db a)
                                                                   (tempid? (get entity a))))
                                                      tuple-attrs))
                   v-tuple                 (and contains-tuple-attrs?
                                                (not tuple-contains-tempids?)
                                                (mapv (fn [a]
                                                        (let [v (get entity a)]
                                                          (validate-val v [nil nil a v nil] db)
                                                          (if (dbu/ref? db a)
                                                            (dbu/entid-strict db v)
                                                            v)))
                                                      tuple-attrs))]
               (if-let [e (and contains-tuple-attrs?
                               (not tuple-contains-tempids?)
                               (:e (first (dbi/datoms db :avet [a-tuple v-tuple]))))]
                 (cond
                   (nil? acc) [e a-tuple v-tuple]        ;; first upsert
                   (= (get acc 0) e) acc                 ;; second+ upsert, but does not conflict
                   :else
                   (let [[_e _a _v] acc]
                     (log/raise "Conflicting upserts: " [_a _v] " resolves to " _e
                                ", but " [a-tuple v-tuple] " resolves to " e
                                {:error :transact/upsert
                                 :entity entity
                                 :assertion [e a-tuple v-tuple]
                                 :conflict [_e _a _v]})))
                 acc)))                                   ;; upsert attr, but resolves to nothing                                      ;; non-upsert attr
           found-eav
           unique-tuple-idents)]
      (->> found-eav-including-composite-tuples
           (check-upsert-conflict entity)
           first))))                                         ;; getting eid from acc

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
            (log/raise "Entity with ID " eid " is a system attribute " (dbi/ident-for db eid :error-on-missing) " and cannot be changed"
                       {:error :transact/syntax, :eid eid, :attribute (dbi/ident-for db eid :error-on-missing) :context entity}))
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
                                 (log/raise "Bad attribute " a-ident ": reverse attribute name requires {:db/valueType :db.type/ref} in schema"
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

(defn- vt-meta-attr?
  "Cheap branch — true iff the resolved a-ident is one of the bitemporal
   tx-meta attrs. Two keyword compares; called once per :db/add."
  [a-ident]
  (or (= a-ident :db.valid/from) (= a-ident :db.valid/to)))

(defn- transact-add [{:keys [db-after] :as report} [_ e a v tx :as ent]]
  (let [a (dbu/normalize-and-validate-attr a ent db-after)
        _ (validate-val v ent db-after)
        attribute-refs? (:attribute-refs? (dbi/-config db-after))
        tx (or tx (current-tx report))
        db db-after
        e (dbu/entid-strict db e)
        a-ident (if attribute-refs? (dbi/ident-for db a :error-on-missing) a)
        v (if (dbu/ref? db a-ident) (dbu/entid-strict db v) v)
        new-datom (datom e a v tx)
        upsert? (not (dbu/multival? db a))
        ;; Cross-tx vf<vt guard: when a :db.valid/from or :db.valid/to
        ;; is written onto a PRIOR tx-entity, queue that tx-eid for
        ;; end-of-loop validation. Pattern mirrors ::queued-tuples
        ;; (composite-tuple recomputation). Validation itself is
        ;; deferred so that the same tx writing BOTH halves on the
        ;; same prior tx-entity is checked against the final combined
        ;; state, not the half-way state. Retracts can only broaden
        ;; the window so they're skipped — a retract+add in the same
        ;; tx is caught by the add side.
        report (cond-> report
                 (and (vt-meta-attr? a-ident)
                      (>= ^long e ^long tx0)
                      (not= e (current-tx report)))
                 (update ::pending-vt-validation (fnil conj #{}) e))]
    (transact-report report new-datom upsert?)))

(defn- transact-retract-datom
  ([report] report)
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
    (log/raise "Conflicting upsert: " tempid " resolves"
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
           (log/raise "Bad transaction meta attribute " attribute " at " tx-meta ", not defined in system or current schema"
                      {:error :transact/schema :attribute attribute :context tx-meta}))))
     []
     tx-meta)))

(defn check-schema-update [db entity new-eid]
  (when (ds/schema-entity? entity)
    (when (and (contains? entity :db/ident)
               (ds/is-system-keyword? (:db/ident entity)))
      (log/raise "Using namespace 'db' for attribute identifiers is not allowed"
                 {:error :transact/schema :entity entity}))
    (if-let [attr-name (get-in db [:schema new-eid])]
      (when-let [invalid-updates (ds/find-invalid-schema-updates entity (get-in db [:schema attr-name]))]
        (when-not (empty? invalid-updates)
          (log/raise "Update not supported for these schema attributes"
                     {:error :transact/schema :entity entity :invalid-updates invalid-updates})))
      (when (= :write (get-in db [:config :schema-flexibility]))
        ;; Secondary index entities only need :db/ident + :db.secondary/type + :db.secondary/attrs
        (if (ds/secondary-index-entity? entity)
          (when-not (and (:db/ident entity) (:db.secondary/type entity) (:db.secondary/attrs entity))
            (log/raise "Incomplete secondary index schema, expected :db/ident, :db.secondary/type, :db.secondary/attrs"
                       {:error :transact/schema :entity entity}))
          (when (or (:db/cardinality entity) (:db/valueType entity))
            (when-not (ds/schema? entity)
              (log/raise "Incomplete schema transaction attributes, expected :db/ident, :db/valueType, :db/cardinality"
                         {:error :transact/schema :entity entity}))))))))

(defn entity-map->op-vec [db {:keys [tempids] :as report} entity]
  (let [old-eid (:db/id entity)
        tx? (tx-id? old-eid) ;; :db/current-tx / "datomic.tx"
        resolved-eid (cond tx?                   (current-tx report)
                           (sequential? old-eid) (dbu/entid-strict db old-eid)
                           (keyword? old-eid)    (dbu/entid-strict db [:db/ident old-eid])
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
                      :else (log/raise "Expected number, string, keyword or lookup ref for :db/id, got " old-eid
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
        (log/raise ":db.fn/cas failed on datom [" e " " a " " (if (dbu/multival? db a) (map :v datoms) (:v (first datoms))) "], expected nil"
                   {:error :transact/cas, :old (if (dbu/multival? db a) datoms (first datoms)), :expected ov, :new nv}))
      (let [ov (if (dbu/ref? db a) (dbu/entid-strict db ov) ov)]
        (validate-val nv op-vec db)
        (if (dbu/multival? db a)
          (if (some (fn [^Datom d] (= (.-v d) ov)) datoms)
            [(transact-add report [:db/add e a nv]) []]
            (log/raise ":db.fn/cas failed on datom [" e " " a " " (map :v datoms) "], expected " ov
                       {:error :transact/cas, :old datoms, :expected ov, :new nv}))
          (let [v (:v (first datoms))]
            (if (= v ov)
              [(transact-add report [:db/add e a nv]) []]
              (log/raise ":db.fn/cas failed on datom [" e " " a " " v "], expected " ov
                         {:error :transact/cas, :old (first datoms), :expected ov, :new nv}))))))))

(defn retract-entity [db report op-vec]
  (let [[_ e] op-vec]
    (if-let [e (dbu/entid db e)]
      (let [e-datoms (vec (dbi/search db [e]))
            v-datoms (into []
                           (mapcat (fn [attr]
                                     ;; TODO: Consider using
                                     ;; (or (dbi/-ref-for db attr) attr)
                                     ;; once warning has been removed from
                                     ;; the -ref-for implementation in datahike.db.
                                     (let [a (if (dbu/attr-has-ref? db attr)
                                               (dbi/-ref-for db attr)
                                               attr)]
                                       (dbi/search db [nil a e]))))
                           (dbi/-attrs-by db :db.type/ref))]
        [(transduce cat transact-retract-datom report [e-datoms v-datoms])
         (retract-components db e-datoms)])
      [report []])))

(defn check-tuple [db op-vec]
  (let [[_ _ a v] op-vec
        attr-schema (-> db dbi/-schema (get a))]
    (cond (:db/tupleType attr-schema)
          (cond (> (count v) 8)
                (log/raise "Cannot store more than 8 values for homogeneous tuple: " op-vec
                           {:error :transact/syntax, :tx-data op-vec})

                (not (apply = (map type v)))
                (log/raise "Cannot store homogeneous tuple with values of different type: " op-vec
                           {:error :transact/syntax, :tx-data op-vec})

                (not (s/valid? (-> db dbi/-schema a :db/tupleType) (first v)))
                (log/raise "Cannot store homogeneous tuple. Values are of wrong type: " op-vec
                           {:error :transact/syntax, :tx-data op-vec}))
          (:db/tupleTypes attr-schema)
          (cond (not (= (count v) (count (:db/tupleTypes attr-schema))))
                (log/raise (str "Cannot store heterogeneous tuple: expecting " (count (:db/tupleTypes attr-schema)) " values, got " (count v))
                           {:error :transact/syntax, :tx-data op-vec})

                (not (apply = (map s/valid? (:db/tupleTypes attr-schema) v)))
                (log/raise (str "Cannot store heterogeneous tuple: there is a mismatch between values " v " and their types " (:db/tupleTypes attr-schema))
                           {:error :transact/syntax, :tx-data op-vec}))
          (and (:db/tupleAttrs attr-schema)
               (not (::internal (meta op-vec))))
          (log/raise "Can’t modify tuple attrs directly: " op-vec
                     {:error :transact/syntax, :tx-data op-vec}))))

(defn- filter-before [datoms before-date db]
  (let [before-pred (fn [^Datom d]
                      (bp/date-before? (.-v d) before-date))
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
                                      ;; :db.secondary/only stores the content hash in the
                                      ;; primary indexes, so search by hash to find the datom.
                                      [e a (if (dbu/secondary-only? db a) (secondary-only-hash v) v)]))
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
                      (log/raise "Can't find entity with ID " e " to be purged"
                                 {:error :transact/purge, :operation op, :tx-data op-vec})))
                  (log/raise "Purge is only available in temporal databases."
                             {:error :transact/purge :operation op :tx-data op-vec}))

      :db.purge/attribute (if (dbi/-keep-history? db)
                            (let [history (HistoricalDB. db)]
                              (if-let [e (dbu/entid history e)]
                                (let [datoms (vec (dbi/search history [e a]))]
                                  [(reduce transact-purge-datom report datoms)
                                   (purge-components history datoms)])
                                (log/raise "Can't find entity with ID " e " to be purged"
                                           {:error :transact/purge, :operation op, :tx-data op-vec})))
                            (log/raise "Purge attribute is only available in temporal databases."
                                       {:error :transact/purge :operation op :tx-data op-vec}))

      :db.purge/entity (if (dbi/-keep-history? db)
                         (let [history (HistoricalDB. db)]
                           (if-let [e (dbu/entid history e)]
                             (let [e-datoms (vec (dbi/search history [e]))
                                   v-datoms (vec (mapcat (fn [a] (dbi/search history [nil a e]))
                                                         (dbi/-attrs-by history :db.type/ref)))]
                               [(reduce transact-purge-datom report (concat e-datoms v-datoms))
                                (purge-components history e-datoms)])
                             (log/raise "Can't find entity with ID " e " to be purged"
                                        {:error :transact/purge, :operation op, :tx-data op-vec})))
                         (log/raise "Purge entity is only available in temporal databases."
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
                                 (log/raise "Purge entity is only available in temporal databases."
                                            {:error :transact/purge :operation op :tx-data op-vec}))

      :db.ensure/attrs (let [{:keys [tx-data]} report
                             asserting-datoms (filter (fn [^Datom d] (= e (.-e d))) tx-data)
                             asserting-attributes (map (fn [^Datom d] (.-a d)) asserting-datoms)
                             diff (clojure.set/difference (set v) (set asserting-attributes))]
                         (if (empty? diff)
                           [report []]
                           (log/raise "Entity " e " missing attributes " diff " of spec " a
                                      {:error :transact/ensure :operation op :tx-data op-vec
                                       :asserting-datoms asserting-datoms})))

      :db.ensure/preds (let [{:keys [db-after]} report
                             preds (assert-preds db-after op-vec)]
                         (if-not (empty? preds)
                           (log/raise "Entity " e " failed predicates " preds " of spec " a
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
              (log/raise "Entity " op " expected to have :db/fn attribute with fn? value"
                         {:error :transact/syntax, :operation :db.fn/call, :tx-data op-vec})))
          (log/raise "Can’t find entity for transaction fn " op
                     {:error :transact/syntax, :operation :db.fn/call, :tx-data op-vec}))
        (log/raise (str "Unknown operation at " op-vec ", expected " (str/join "," builtin-op?)
                        " or an ident corresponding to an installed transaction function"
                        " (e.g. {:db/ident <keyword> :db/fn <Ifn>}, usage of :db/ident requires {:db/unique :db.unique/identity} in schema)")
                   {:error :transact/syntax, :operation op, :tx-data op-vec})))))

(defn- validate-cross-tx-vt-windows!
  "Cross-tx vf<vt guard. For each prior tx-entity that received a
   retroactive :db.valid/from or :db.valid/to write in this tx (see
   `transact-add` ::pending-vt-validation queue), look up the
   resulting (vf, vt) pair on db-after and raise if vf >= vt.

   Single EAVT seek per affected tx-entity (3 datoms returned —
   txInstant + vf + vt). No-op when no such writes occurred (the
   ::pending-vt-validation set is absent).

   Mirrors the pre-loop guard at the top of `transact-tx-data` which
   checks the *current* tx's :tx-meta; this one covers the closure
   formed by editing a *prior* tx-entity's vt-meta.

   Throws `:transact/invalid-valid-times-cross-tx` on first violation."
  [{:keys [db-after] :as report}]
  (when-let [pending (::pending-vt-validation report)]
    (let [attr-refs? (:attribute-refs? (dbi/-config db-after))
          vf-a (if attr-refs? (dbi/-ref-for db-after :db.valid/from) :db.valid/from)
          vt-a (if attr-refs? (dbi/-ref-for db-after :db.valid/to) :db.valid/to)
          sc (dbi/-search-context db-after)]
      (doseq [tx-eid pending]
        (let [datoms (dbi/-datoms db-after :eavt [tx-eid] sc)
              vf (some (fn [^Datom d] (when (= vf-a (.-a d)) (.-v d))) datoms)
              vt (some (fn [^Datom d] (when (= vt-a (.-a d)) (.-v d))) datoms)]
          (when (and vf vt (not (bp/date-before? vf vt)))
            (log/raise (str "Invalid cross-tx valid-time window: tx-entity "
                            tx-eid " would have :db.valid/from >= :db.valid/to "
                            "(from=" vf ", to=" vt ") after this commit")
                       {:error :transact/invalid-valid-times-cross-tx
                        :tx-eid tx-eid
                        :db.valid/from vf
                        :db.valid/to vt})))))))

(defn transact-tx-data [{:keys [db-before] :as initial-report} initial-es]
  (when-not (or (nil? initial-es)
                (sequential? initial-es))
    (log/raise "Bad transaction data " initial-es ", expected sequential collection"
               {:error :transact/syntax, :tx-data initial-es}))
  (let [has-tuples? (seq (dbi/-attrs-by (:db-after initial-report) :db.type/tuple))
        initial-es' (if has-tuples?
                      (interleave initial-es (repeat ::flush-tuples))
                      initial-es)
        initial-report (update initial-report :tx-meta
                               #(merge {:db/txInstant (next-tx-instant db-before)} %))
        ;; Reject zero-width or reverse valid-time windows. A tx
        ;; with `:db.valid/from >= :db.valid/to` would produce a
        ;; tx-entity that no `d/valid-at` query can ever match
        ;; (the AVET predicate is `vf <= at < vt`, unsatisfiable
        ;; when from >= to) — a silent data-quality bug. Throw at
        ;; the transactor so it surfaces immediately.
        _ (let [tm (:tx-meta initial-report)
                vf (:db.valid/from tm)
                vt (:db.valid/to tm)]
            (when (and vf vt (not (bp/date-before? vf vt)))
              (log/raise (str "Invalid valid-time window: :db.valid/from "
                              "must be strictly before :db.valid/to "
                              "(got from=" vf ", to=" vt ")")
                         {:error :transact/invalid-valid-times
                          :db.valid/from vf
                          :db.valid/to vt})))
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
          (do
            ;; Cross-tx vf<vt validation: any prior tx-entity touched
            ;; by this commit's vt-meta writes is checked against the
            ;; final combined state. Throws on invalid window. The
            ;; ::pending-vt-validation bookkeeping is stripped before
            ;; the report exits, matching the ::queued-tuples cleanup
            ;; discipline.
            (validate-cross-tx-vt-windows! report)
            (-> report
                (dissoc ::pending-vt-validation)
                (assoc-in [:tempids :db/current-tx] (current-tx report))
                (update-in [:db-after :max-tx] inc)
                (update :db-after persistent!)
                (update :db-after finalize-secondary-indices)))

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
                (log/raise "Can't use tempid in '" entity "'. Tempids are allowed in :db/add only"
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
          (log/raise "Bad entity type at " entity ", expected map or vector"
                     {:error :transact/syntax, :tx-data entity}))))))

(defn transact-entities-directly [initial-report initial-es]
  (loop [report (update initial-report :db-after transient)
         es initial-es
         migration-state (get-in initial-report [:db-before :migration] {})]
    (if (empty? es)
      (-> report
          (update-in [:db-after :max-tx] inc)
          (update-in [:db-after :migration] #(if %
                                               (merge % migration-state)
                                               migration-state))
          (update :db-after persistent!)
          (update :db-after finalize-secondary-indices))
      (let [[entity & entities] es
            {:keys [config] :as db} (:db-after report)
            [e a v t op] entity
            a-ident (if (and (number? a) (:attribute-refs? config))
                      (dbi/-ident-for db a)
                      a)
            a (if (:attribute-refs? config)
                (dbi/-ref-for db a-ident)
                (if (number? a)
                  (log/raise "Configuration mismatch: import data with attribute references can not be imported into a database with no attribute references."
                             {:error :import/mismatch :data entity})
                  a-ident))
            max-eid (next-eid db)
            max-tid (inc (get-in report [:db-after :max-tx]))]
        (cond
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
            (recur (transact-report report new-datom upsert?) entities (assoc-in migration-state [:eids e] (.-e new-datom)))))))))
