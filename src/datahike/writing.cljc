(ns datahike.writing
  "Manage all state changes and access to state of durable store."
  (:require [datahike.connections :refer [delete-connection! *connections*]]
            [datahike.db :as db]
            [datahike.db.utils :as dbu]
            [datahike.db.interface :as dbi]
            [datahike.index :as di]
            [datahike.index.secondary :as sec]
            [datahike.store :as ds]
            [datahike.tools :as dt]
            [datahike.core :as core]
            [datahike.query :as dq]
            [datahike.config :as dc]
            [datahike.schema-cache :as sc]
            [datahike.online-gc :as online-gc]
            [konserve.core :as k]
            [konserve.store :as ks]
            [replikativ.logging :as log]
            [hasch.core :refer [uuid squuid]]
            [hasch.platform]
            [clojure.core.async :as async :refer [go put!]]
            [superv.async #?(:clj :refer :cljs :refer-macros) [go-try- <?-]]
            [konserve.utils :refer [#?(:clj async+sync) multi-key-capable? *default-sync-translation*]
             #?@(:cljs [:refer-macros [async+sync]])]))

;; mapping to storage

(defn stored-db? [obj]
  ;; TODO use proper schema to match?
  (let [keys-to-check [:eavt-key :aevt-key :avet-key :config
                       :max-tx :max-eid :op-count :hash :meta]]
    (= (count (select-keys obj keys-to-check))
       (count keys-to-check))))

(defn get-and-clear-pending-kvs!
  "Retrieves and clears pending key-value pairs from the store's pending-writes atom.
  Assumes :pending-writes in store's storage holds an atom of a collection of [key value] pairs."
  [store]
  (let [pending-writes-atom (-> store :storage :pending-writes) ; Assumes :storage key holds the CachedStorage
        kvs-to-write (atom [])]
    (when pending-writes-atom
      ;; Atomically get current KVs and reset the pending-writes atom.
      (swap! pending-writes-atom (fn [old-kvs] (reset! kvs-to-write old-kvs) [])))
    @kvs-to-write))

(defn db->stored
  "Maps memory db to storage layout. Index flushes will add [k v] pairs to pending-writes."
  [db flush?]
  (when-not (dbu/db? db)
    (log/raise "Argument is not a database."
               {:type     :argument-is-not-a-db
                :argument db}))
  (let [{:keys [eavt aevt avet temporal-eavt temporal-aevt temporal-avet
                schema rschema system-entities ident-ref-map ref-ident-map config
                max-tx max-eid op-count hash meta store]} db
        schema-meta {:schema schema
                     :rschema rschema
                     :system-entities system-entities
                     :ident-ref-map ident-ref-map
                     :ref-ident-map ref-ident-map}
        schema-meta-key (uuid schema-meta)
        backend                                           (di/konserve-backend (:index config) store)
        not-in-memory?                                    (not= :memory (-> config :store :backend))
        flush! (and flush? not-in-memory?)
        ;; Prepare schema meta KV pair for writing, but don't write it here.
        schema-meta-kv-to-write (when-not (sc/write-cache-has? (:store config) schema-meta-key)
                                  (sc/add-to-write-cache (:store config) schema-meta-key)
                                  [schema-meta-key schema-meta])]
    (when-not (sc/cache-has? schema-meta-key)
      (sc/cache-miss schema-meta-key schema-meta))
    [schema-meta-kv-to-write ;; Return [key value] pair or nil
     (merge
      {:schema-meta-key  schema-meta-key
       :config          config
       :meta            meta
       :hash            hash
       :max-tx          max-tx
       :max-eid         max-eid
       :op-count        op-count
       ;; di/-flush will now add [k v] to pending-writes via CachedStorage
       :eavt-key        (cond-> eavt flush! (di/-flush backend))
       :aevt-key        (cond-> aevt flush! (di/-flush backend))
       :avet-key        (cond-> avet flush! (di/-flush backend))}
      (when (:keep-history? config)
        {:temporal-eavt-key (cond-> temporal-eavt flush! (di/-flush backend))
         :temporal-aevt-key (cond-> temporal-aevt flush! (di/-flush backend))
         :temporal-avet-key (cond-> temporal-avet flush! (di/-flush backend))}))]))

(defn- reconstruct-secondary-indices
  "Scan schema for secondary index definitions and recreate empty index instances.
   Indices with status :ready or :building will be recreated (empty) and need backfill.
   Returns a map of {idx-ident -> index-instance} and updated rschema mappings."
  [schema ident-ref-map]
  #?(:clj
     (let [sec-indices (reduce-kv
                        (fn [acc ident entry]
                          (if (and (map? entry) (:db.secondary/type entry))
                            (let [idx-type (:db.secondary/type entry)
                                  idx-attrs (set (:db.secondary/attrs entry))
                                  idx-config (merge (:db.secondary/config entry)
                                                    {:attrs idx-attrs})
                                  idx-config (cond-> idx-config
                                               (seq ident-ref-map)
                                               (assoc :ident-ref-map ident-ref-map))]
                              (try
                                (assoc acc ident (sec/create-index idx-type idx-config nil))
                                (catch Exception e
                                  (log/warn :datahike/secondary-index-reconstruct-failed {:ident ident :error (.getMessage e)})
                                  acc)))
                            acc))
                        {} schema)]
       sec-indices)
     :cljs {}))

(defn stored->db
  "Constructs in-memory db instance from stored map value."
  [stored-db store]
  (let [{:keys [eavt-key aevt-key avet-key
                temporal-eavt-key temporal-aevt-key temporal-avet-key
                schema rschema system-entities ref-ident-map ident-ref-map
                config max-tx max-eid op-count hash meta schema-meta-key]
         :or   {op-count 0}} stored-db
        schema-meta (or (sc/cache-lookup schema-meta-key)
                        ;; not in store in case we load an old db where the schema meta data was inline
                        (when-let [schema-meta (k/get store schema-meta-key nil {:sync? true})]
                          (sc/cache-miss schema-meta-key schema-meta)
                          schema-meta))
        effective-schema (or (:schema schema-meta) schema)
        effective-ident-ref-map (or (:ident-ref-map schema-meta) ident-ref-map)
        sec-indices (reconstruct-secondary-indices effective-schema effective-ident-ref-map)
        empty       (db/empty-db nil config store)]
    (merge
     (assoc empty
            :max-tx max-tx
            :max-eid max-eid
            :config config
            :meta meta
            :schema schema
            :hash hash
            :op-count op-count
            :eavt eavt-key
            :aevt aevt-key
            :avet avet-key
            :temporal-eavt temporal-eavt-key
            :temporal-aevt temporal-aevt-key
            :temporal-avet temporal-avet-key
            :rschema rschema
            :system-entities system-entities
            :ident-ref-map ident-ref-map
            :ref-ident-map ref-ident-map
            :store store)
     (when (seq sec-indices)
       {:secondary-indices sec-indices})
     schema-meta)))

(defn branch-heads-as-commits [store parents]
  (set (doall (for [p parents]
                (do
                  (when (nil? p)
                    (log/raise "Parent cannot be nil." {:type :parent-cannot-be-nil
                                                        :parent p}))
                  (if-not (keyword? p) p
                          (let [{{:keys [datahike/commit-id]} :meta :as old-db}
                                (k/get store p nil {:sync? true})]
                            (when-not old-db
                              (log/raise "Parent does not exist in store."
                                         {:type   :parent-does-not-exist-in-store
                                          :parent p}))
                            commit-id)))))))

(defn create-commit-id [db]
  (let [{:keys [hash max-tx max-eid meta config]} db
        content-uuid (uuid [hash max-tx max-eid meta])]
    (if (:crypto-hash? config)
      content-uuid
      (squuid content-uuid))))  ;; Sequential UUID for better index locality

(defn write-pending-kvs!
  "Writes a collection of key-value pairs to the store.
  Handles synchronous and asynchronous writes.
  Assumes it's called within a go-try- block if sync? is false."
  [store kvs sync?]
  (if sync?
    (doseq [[k v] kvs]
      (k/assoc store k v {:sync? true}))
    (let [pending-ops (mapv (fn [[k v]] (k/assoc store k v {:sync? false})) kvs)]
      (go-try- (doseq [op pending-ops] (<?- op))))))

(defn commit!
  ([db parents]
   (commit! db parents true))
  ([db parents sync?]
   (async+sync sync? *default-sync-translation*
               (go-try-
                (let [{:keys [store config]} db
                      parents       (or parents #{(get config :branch)})
                      parents       (branch-heads-as-commits store parents)
                      cid           (create-commit-id db)
                      db            (-> db
                                        (assoc-in [:meta :datahike/commit-id] cid)
                                        (assoc-in [:meta :datahike/parents] parents))
                      ;; db->stored now returns [schema-meta-kv-to-write db-to-store]
                      ;; and index flushes will have populated pending-writes
                      [schema-meta-kv-to-write db-to-store] (db->stored db true)
                      ;; Get all pending [k v] pairs (e.g., from index flushes)
                      pending-kvs   (get-and-clear-pending-kvs! store)]

                  (if (multi-key-capable? store)
                    (let [[meta-key meta-val] schema-meta-kv-to-write
                          writes-map (cond-> (into {} pending-kvs) ; Initialize with pending KVs
                                       schema-meta-kv-to-write (assoc meta-key meta-val)
                                       true                    (assoc cid db-to-store)
                                       true                    (assoc (:branch config) db-to-store))]
                      (<?- (k/multi-assoc store writes-map {:sync? sync?})))
                    ;; Then write schema-meta, commit-log, branch
                    (let [[meta-key meta-val] schema-meta-kv-to-write
                          schema-meta-written (when schema-meta-kv-to-write
                                                (k/assoc store meta-key meta-val {:sync? sync?}))

                          ;; Make sure all pointed to values are written before the commit log and branch
                          _ (when schema-meta-kv-to-write (<?- schema-meta-written))
                          _ (<?- (write-pending-kvs! store pending-kvs sync?))

                          commit-log-written (k/assoc store cid db-to-store {:sync? sync?})
                          branch-written     (k/assoc store (:branch config) db-to-store {:sync? sync?})]
                      (when-not sync?
                        (<?- commit-log-written)
                        (<?- branch-written))))

                  ;; Online GC: delete freed addresses after writes are committed
                  (when (get-in config [:online-gc :enabled?])
                    (<?- (online-gc/online-gc! store (assoc (:online-gc config) :sync? false))))

                  db)))))

(defn complete-db-update [old tx-report]
  (let [{:keys [writer]} old
        {:keys [db-after tx-data]
         {:keys [db/txInstant]} :tx-meta} tx-report
        new-meta  (assoc (:meta db-after) :datahike/updated-at txInstant)
        db        (assoc db-after :meta new-meta :writer writer)
        ;; Propagate query result cache from old DB to new DB
        ;; Extract modified attributes from tx-data for selective invalidation
        _         (try
                    (let [rim (:ref-ident-map db)
                          modified-attrs (into #{}
                                               (comp (map :a)
                                                     (filter some?)
                                                     (map (fn [a] (if (and rim (number? a)) (get rim a a) a))))
                                               tx-data)]
                      (dq/propagate-query-cache old db modified-attrs))
                    (catch #?(:clj Exception :cljs :default) e
                      (log/error "propagate-query-cache error:" e)))
        tx-report (assoc tx-report :db-after db)]
    tx-report))

(defprotocol PDatabaseManager
  (-create-database [config opts])
  (-delete-database [config])
  (-database-exists? [config]))

(defn -database-exists?* [config]
  (let [p (dt/throwable-promise)]
    (go
      (put! p (try
                (let [config (dc/load-config config)
                      store-config (:store config)
                      ;; First check if store exists (avoids exception when store not in registry)
                      store-exists? (<?- (ks/store-exists? store-config {:sync? false}))]
                  (if store-exists?
                    ;; Store exists, now check if it contains a database
                    (let [raw-store (<?- (ks/connect-store store-config {:sync? false}))
                          store (ds/add-cache-and-handlers raw-store config)
                          stored-db (<?- (k/get store :db nil {:sync? false}))]
                      ;; Release store and await completion
                      (<?- (ks/release-store store-config store {:sync? false}))
                      (some? stored-db))
                    ;; Store doesn't exist, so database doesn't exist
                    false))
                (catch #?(:clj Exception :cljs js/Error) e
                  e))))
    p))

(defn -create-database* [config deprecated-config]
  (go-try-
   (let [opts {:sync? false}
         {:keys [keep-history?] :as config} (dc/load-config config deprecated-config)
         store-config (:store config)
         store (ds/add-cache-and-handlers (<?- (ks/create-store store-config opts)) config)
         stored-db (<?- (k/get store :db nil opts))
         _ (when stored-db
             (log/raise "Database already exists."
                        {:type :db-already-exists :config store-config}))
         {:keys [eavt aevt avet temporal-eavt temporal-aevt temporal-avet
                 schema rschema system-entities ref-ident-map ident-ref-map
                 config max-tx max-eid op-count hash meta] :as db}
         (db/empty-db nil config store)
         backend (di/konserve-backend (:index config) store)
         cid (create-commit-id db)
         meta (assoc meta :datahike/commit-id cid)
         schema-meta {:schema schema
                      :rschema rschema
                      :system-entities system-entities
                      :ident-ref-map ident-ref-map
                      :ref-ident-map ref-ident-map}
         schema-meta-key (uuid schema-meta)
         ;; di/-flush calls will populate pending-writes via CachedStorage
         db-to-store (merge {:max-tx          max-tx
                             :max-eid         max-eid
                             :op-count        op-count
                             :hash            hash
                             :schema-meta-key schema-meta-key
                             :config          (update config :initial-tx (comp not empty?))
                             :meta            meta
                             :eavt-key        (di/-flush eavt backend)
                             :aevt-key        (di/-flush aevt backend)
                             :avet-key        (di/-flush avet backend)}
                            (when keep-history?
                              {:temporal-eavt-key (di/-flush temporal-eavt backend)
                               :temporal-aevt-key (di/-flush temporal-aevt backend)
                               :temporal-avet-key (di/-flush temporal-avet backend)}))]
     ;;we just created the first data base in this store, so the write cache is empty
     (<?- (k/assoc store schema-meta-key schema-meta opts))
     (sc/add-to-write-cache (:store config) schema-meta-key)
     (when-not (sc/cache-has? schema-meta-key)
       (sc/cache-miss schema-meta-key schema-meta))

     ;; Process pending KVs from index flushes synchronously
     (let [pending-kvs (get-and-clear-pending-kvs! store)]
       (<?- (write-pending-kvs! store pending-kvs false)))

     (<?- (k/assoc store :branches #{:db} opts))
     (<?- (k/assoc store cid db-to-store opts))
     (<?- (k/assoc store :db db-to-store opts))
     (ks/release-store store-config store)
     config)))

(defn -delete-database* [config]
  (go-try-
   (let [config (dc/load-config config {})
         config-store-id (ds/store-identity (:store config))
         active-conns (filter (fn [[store-id _branch]]
                                (= store-id config-store-id))
                              (keys @*connections*))]
     (sc/clear-write-cache (:store config))
     (doseq [conn active-conns]
       (log/warn :datahike/delete-unreleased-connections {:connection conn})
       (delete-connection! conn))
     (ks/delete-store (:store config)))))

(extend-protocol PDatabaseManager
  #?(:clj String :cljs string)
  (-create-database #?(:clj [uri & opts] :cljs [uri opts])
    (-create-database (dc/uri->config uri) opts))

  (-delete-database [uri]
    (-delete-database (dc/uri->config uri)))

  (-database-exists? [uri]
    (-database-exists? (dc/uri->config uri)))

  #?(:clj clojure.lang.IPersistentMap :cljs PersistentArrayMap)
  (-database-exists? [config]
    (-database-exists?* config))
  (-create-database [config opts]
    (-create-database* config opts))
  (-delete-database [config]
    (-delete-database* config))

  #?(:cljs PersistentHashMap)
  #?(:cljs
     (-database-exists? [config]
                        (-database-exists?* config)))
  #?(:cljs (-create-database [config opts] (-create-database* config opts)))
  #?(:cljs (-delete-database [config] (-delete-database* config))))

;; public API

(defn create-database
  ([]
   (-create-database {} nil))
  ([config & opts]
   (-create-database config opts)))

(defn delete-database
  ([]
   (-delete-database {}))
  ;;deprecated
  ([config]
   ;; TODO log deprecation notice with #54
   (-delete-database config)))

(defn database-exists?
  ([]
   (-database-exists? {}))
  ([config]
   ;; TODO log deprecation notice with #54
   (-database-exists? config)))

#?(:clj
   (defn build-secondary-index!
     "Backfill a secondary index by scanning AEVT for all covered attributes.
      This is a synchronous writer operation that blocks until complete.
      Sets :db.secondary/status to :ready when done."
     [old idx-ident]
     (log/info :datahike/build-secondary-index {:idx-ident idx-ident})
     (let [db old
           idx (get-in db [:secondary-indices idx-ident])
           _ (when-not idx
               (log/raise "Secondary index not found" {:idx-ident idx-ident}))
           attrs (sec/-indexed-attrs idx)
           ;; Use transient batch mode if available
           use-transient? (satisfies? sec/ITransientSecondaryIndex idx)
           t-idx (if use-transient? (sec/-as-transient idx) idx)
           ;; Scan AEVT for each covered attribute and feed datoms
           populated-idx (reduce
                          (fn [current-idx attr]
                            (let [datoms (dbi/datoms db :aevt [attr])]
                              (log/debug :datahike/backfilling {:attr attr :count (count (seq datoms))})
                              (reduce
                               (fn [idx d]
                                 (let [tx-report {:datom d :added? true}]
                                   (if use-transient?
                                     (do (sec/-transact! idx tx-report) idx)
                                     (sec/-transact idx tx-report))))
                               current-idx datoms)))
                          t-idx attrs)
           final-idx (if use-transient?
                       (sec/-persistent! populated-idx)
                       populated-idx)
           ;; Update db: replace index and set status to :ready
           db-after (-> db
                        (assoc-in [:secondary-indices idx-ident] final-idx)
                        (assoc-in [:schema idx-ident :db.secondary/status] :ready)
                        (update-in [:schema idx-ident] dissoc :db.secondary/building-since-tx))]
       (log/info :datahike/secondary-index-built {:idx-ident idx-ident})
       (complete-db-update old {:db-before old
                                :db-after db-after
                                :tx-data []
                                :tx-meta {}}))))

(defn transact! [old {:keys [tx-data tx-meta]}]
  (log/debug :datahike/transact {:tx-count (count tx-data)})
  (log/trace :datahike/transact-detail {:tx-data tx-data :tx-meta tx-meta})
  (complete-db-update old (core/with old tx-data tx-meta)))

(defn load-entities [old entities]
  (log/debug :datahike/load-entities {:entity-count (count entities)})
  (complete-db-update old (core/load-entities-with old entities nil)))
