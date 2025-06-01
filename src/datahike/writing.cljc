(ns datahike.writing
  "Manage all state changes and access to state of durable store."
  (:require [datahike.connections :refer [delete-connection! *connections*]]
            [datahike.db :as db]
            [datahike.db.utils :as dbu]
            [datahike.index :as di]
            [datahike.store :as ds]
            [datahike.tools :as dt]
            [datahike.core :as core]
            [datahike.config :as dc]
            [datahike.schema-cache :as sc]
            [konserve.core :as k]
            [taoensso.timbre :as log]
            [hasch.core :refer [uuid]]
            [superv.async :refer [go-try- <?-]]
            [konserve.utils :refer [#?(:clj async+sync :cljs async+sync) multi-key-capable? *default-sync-translation*]
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
    (dt/raise "Argument is not a database."
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
        not-in-memory?                                    (not= :mem (-> config :store :backend))
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
     schema-meta)))

(defn branch-heads-as-commits [store parents]
  (set (doall (for [p parents]
                (do
                  (when (nil? p)
                    (dt/raise "Parent cannot be nil." {:type :parent-cannot-be-nil
                                                       :parent p}))
                  (if-not (keyword? p) p
                          (let [{{:keys [datahike/commit-id]} :meta :as old-db}
                                (k/get store p nil {:sync? true})]
                            (when-not old-db
                              (dt/raise "Parent does not exist in store."
                                        {:type   :parent-does-not-exist-in-store
                                         :parent p}))
                            commit-id)))))))

(defn create-commit-id [db]
  (let [{:keys [hash max-tx max-eid meta]} db]
    (uuid [hash max-tx max-eid meta])))

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
                  db)))))

(defn complete-db-update [old tx-report]
  (let [{:keys [writer]} old
        {:keys [db-after]
         {:keys [db/txInstant]} :tx-meta} tx-report
        new-meta  (assoc (:meta db-after) :datahike/updated-at txInstant)
        db        (assoc db-after :meta new-meta :writer writer)
        tx-report (assoc tx-report :db-after db)]
    tx-report))

(defprotocol PDatabaseManager
  (-create-database [config opts])
  (-delete-database [config])
  (-database-exists? [config]))

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
    (let [config (dc/load-config config)
          store-config (:store config)
          raw-store (ds/connect-store store-config)]
      (if (not (nil? raw-store))
        (let [store (ds/add-cache-and-handlers raw-store config)
              stored-db (k/get store :db nil {:sync? true})]
          (ds/release-store store-config store)
          (not (nil? stored-db)))
        (do
          (ds/release-store store-config raw-store)
          false))))

  (-create-database [config deprecated-config]
    (let [{:keys [keep-history?] :as config} (dc/load-config config deprecated-config)
          store-config (:store config)
          store (ds/add-cache-and-handlers (ds/empty-store store-config) config)
          stored-db (k/get store :db nil {:sync? true})
          _ (when stored-db
              (dt/raise "Database already exists."
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
      (k/assoc store schema-meta-key schema-meta {:sync? true})
      (sc/add-to-write-cache (:store config) schema-meta-key)
      (when-not (sc/cache-has? schema-meta-key)
        (sc/cache-miss schema-meta-key schema-meta))

      ;; Process pending KVs from index flushes synchronously
      (let [pending-kvs (get-and-clear-pending-kvs! store)]
        (write-pending-kvs! store pending-kvs true))

      (k/assoc store :branches #{:db} {:sync? true})
      (k/assoc store cid db-to-store {:sync? true})
      (k/assoc store :db db-to-store {:sync? true})
      (ds/release-store store-config store)
      config))

  (-delete-database [config]
    (let [config (dc/load-config config {})
          config-store-id (ds/store-identity (:store config))
          active-conns (filter (fn [[store-id _branch]]
                                 (= store-id config-store-id))
                               (keys @*connections*))]
      (sc/clear-write-cache (:store config))
      (doseq [conn active-conns]
        (log/warn "Deleting database without releasing all connections first: " conn "."
                  "All connections will be released now, but this cannot be ensured for remote readers.")
        (delete-connection! conn))
      (ds/delete-store (:store config)))))

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

(defn transact! [old {:keys [tx-data tx-meta]}]
  (log/debug "Transacting" (count tx-data) "objects")
  (log/trace "Transaction data" tx-data  "with meta:" tx-meta)
  (complete-db-update old (core/with old tx-data tx-meta)))

(defn load-entities [old entities]
  (log/debug "Loading" (count entities) " entities.")
  (complete-db-update old (core/load-entities-with old entities nil)))