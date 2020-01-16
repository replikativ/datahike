(ns datahike.connector
  (:require [datahike.db :as db]
            [datahike.core :as d]
            [datahike.index :as di]
            [datahike.store :as ds]
            [hitchhiker.tree.bootstrap.konserve :as kons]
            [konserve.core :as k]
            [konserve.cache :as kc]
            [superv.async :refer [<?? S]]
            [datahike.config :as dc]
            [clojure.spec.alpha :as s]
            [clojure.core.cache :as cache])
  (:import [java.net URI]))

(s/def ::connection #(instance? clojure.lang.Atom %))

(defmulti transact!
  "Transacts new data to database"
  {:arglists '([conn tx-data])}
  (fn [conn tx-data] (type tx-data)))

(defmethod transact! clojure.lang.PersistentVector
  [connection tx-data]
  (transact! connection {:tx-data tx-data}))

(defmethod transact! clojure.lang.PersistentArrayMap
  [connection {:keys [tx-data]}]
  {:pre [(d/conn? connection)]}
  (future
    (locking connection
      (let [{:keys [db-after] :as tx-report} @(d/transact connection tx-data)
            {:keys [eavt aevt avet temporal-eavt temporal-aevt temporal-avet schema rschema config max-tx]} db-after
            store (:store @connection)
            backend (kons/->KonserveBackend store)
            eavt-flushed (di/-flush eavt backend)
            aevt-flushed (di/-flush aevt backend)
            avet-flushed (di/-flush avet backend)
            temporal-index? (:temporal-index config)
            temporal-eavt-flushed (when temporal-index? (di/-flush temporal-eavt backend))
            temporal-aevt-flushed (when temporal-index? (di/-flush temporal-aevt backend))
            temporal-avet-flushed (when temporal-index? (di/-flush temporal-avet backend))]
        (<?? S (k/assoc-in store [:db]
                           (merge
                            {:schema   schema
                             :rschema  rschema
                             :config   config
                             :max-tx max-tx
                             :eavt-key eavt-flushed
                             :aevt-key aevt-flushed
                             :avet-key avet-flushed}
                            (when temporal-index?
                              {:temporal-eavt-key temporal-eavt-flushed
                               :temporal-aevt-key temporal-aevt-flushed
                               :temporal-avet-key temporal-avet-flushed}))))
        (reset! connection (assoc db-after
                             :eavt eavt-flushed
                             :aevt aevt-flushed
                             :avet avet-flushed
                             :temporal-eavt temporal-eavt-flushed
                             :temporal-aevt temporal-aevt-flushed
                             :temporal-avet temporal-avet-flushed))
        tx-report))))

(defn transact [connection tx-data]
  (try
    (deref (transact! connection tx-data))
    (catch Exception e
      (throw (.getCause e)))))

(defn release [conn]
  (ds/release-store (get-in @conn [:config :storage]) (:store @conn)))

(defprotocol IConfiguration
  (connect [config])
  (-create-database [config opts])
  (delete-database [config])
  (database-exists? [config]))

(extend-protocol IConfiguration
  String
  (connect [uri]
    (connect (dc/uri->config uri)))

  (-create-database [uri & opts]
    (apply -create-database (dc/uri->config uri) opts))

  (delete-database [uri]
    (delete-database (dc/uri->config uri)))

  (database-exists? [uri]
    (database-exists? (dc/uri->config uri)))
  
  clojure.lang.PersistentArrayMap
  (database-exists? [config]
    (let [raw-store (ds/connect-store config)]
      (if (not (nil? raw-store)) 
          (let [store (kons/add-hitchhiker-tree-handlers
                       (kc/ensure-cache
                        raw-store
                        (atom (cache/lru-cache-factory {} :threshold 1000))))
                stored-db (<?? S (k/get-in store [:db]))]
            (ds/release-store config store)
            (not (nil? stored-db)))
        (do
          (ds/release-store config raw-store)
          false))))

  (connect [config]
    (let [raw-store (ds/connect-store config)
          _ (when-not raw-store
              (throw (ex-info "Backend does not exist." {:type :backend-does-not-exist
                                                          :config config})))
          store (kons/add-hitchhiker-tree-handlers
                 (kc/ensure-cache
                  raw-store
                  (atom (cache/lru-cache-factory {} :threshold 1000))))
          stored-db (<?? S (k/get-in store [:db]))
          _ (when-not stored-db
              (ds/release-store config store)
              (throw (ex-info "Database does not exist." {:type :db-does-not-exist
                                                          :config config})))
          {:keys [eavt-key aevt-key avet-key temporal-eavt-key temporal-aevt-key temporal-avet-key schema rschema config max-tx]} stored-db
          empty (db/empty-db nil :datahike.index/hitchhiker-tree :config config)]
      (d/conn-from-db
       (assoc empty
              :max-tx max-tx
              :config config
              :schema schema
              :max-eid (db/init-max-eid eavt-key)
              :eavt eavt-key
              :aevt aevt-key
              :avet avet-key
              :temporal-eavt temporal-eavt-key
              :temporal-aevt temporal-aevt-key
              :temporal-avet temporal-avet-key
              :rschema rschema
              :store store))))

  (-create-database [store-config
                    {:keys [initial-tx schema-on-read temporal-index]
                     :or {schema-on-read false temporal-index true}
                     :as opt-config}]
    (dc/validate-config store-config)
    (dc/validate-config-attribute :datahike.config/schema-on-read schema-on-read opt-config)
    (dc/validate-config-attribute :datahike.config/temporal-index temporal-index opt-config)
    (let [store (kc/ensure-cache
                 (ds/empty-store store-config)
                 (atom (cache/lru-cache-factory {} :threshold 1000)))
          stored-db (<?? S (k/get-in store [:db]))]

      (if stored-db
        false
        (let [db-config {:schema-on-read schema-on-read
                         :temporal-index temporal-index
                         :storage        store-config}
              {:keys [eavt aevt avet temporal-eavt temporal-aevt temporal-avet schema rschema config max-tx]}
              (db/empty-db
                {:db/ident {:db/unique :db.unique/identity}}
                (ds/scheme->index store-config)
                :config db-config)
              backend (kons/->KonserveBackend store)]
          (<?? S (k/assoc-in store [:db]
                             (merge {:schema   schema
                                     :max-tx   max-tx
                                     :rschema  rschema
                                     :config   db-config
                                     :eavt-key (di/-flush eavt backend)
                                     :aevt-key (di/-flush aevt backend)
                                     :avet-key (di/-flush avet backend)}
                                    (when temporal-index
                                      {:temporal-eavt-key (di/-flush temporal-eavt backend)
                                       :temporal-aevt-key (di/-flush temporal-aevt backend)
                                       :temporal-avet-key (di/-flush temporal-avet backend)}))))
          (ds/release-store store-config store)
          (when initial-tx
            (let [conn (connect store-config)]
              (transact conn initial-tx)
              (release conn)))
          true))))

  (delete-database [config]
    (ds/delete-store config)))

(defn create-database [config & opts]
  (-create-database config opts))
