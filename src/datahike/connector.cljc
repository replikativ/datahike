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

(defn update-and-flush-db [connection tx-data update-fn]
  (let [{:keys [db-after] :as tx-report} @(update-fn connection tx-data)
        {:keys [eavt aevt avet temporal-eavt temporal-aevt temporal-avet schema rschema config max-tx hash]} db-after
        store (:store @connection)
        backend (kons/->KonserveBackend store)
        eavt-flushed (di/-flush eavt backend)
        aevt-flushed (di/-flush aevt backend)
        avet-flushed (di/-flush avet backend)
        keep-history? (:keep-history? config)
        temporal-eavt-flushed (when keep-history? (di/-flush temporal-eavt backend))
        temporal-aevt-flushed (when keep-history? (di/-flush temporal-aevt backend))
        temporal-avet-flushed (when keep-history? (di/-flush temporal-avet backend))]
    (<?? S (k/assoc-in store [:db]
                       (merge
                        {:schema   schema
                         :rschema  rschema
                         :config   config
                         :hash hash
                         :max-tx max-tx
                         :eavt-key eavt-flushed
                         :aevt-key aevt-flushed
                         :avet-key avet-flushed}
                        (when keep-history?
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
    tx-report))


(defmethod transact! clojure.lang.IPersistentMap
  [connection {:keys [tx-data]}]
  {:pre [(d/conn? connection)]}
  (future
    (locking connection
      (update-and-flush-db connection tx-data d/transact))))

(defn transact [connection tx-data]
  (try
    (deref (transact! connection tx-data))
    (catch Exception e
      (throw (.getCause e)))))

(defn load-entities [connection entities]
  (future
    (locking connection
      (update-and-flush-db connection entities d/load-entities))))

(defn release [connection]
  (ds/release-store (get-in @connection [:config :store]) (:store @connection)))

;; deprecation begin
(defprotocol IConfiguration
  (-connect [config])
  (-create-database [config opts])
  (-delete-database [config])
  (-database-exists? [config]))

(extend-protocol IConfiguration
  String
  (-connect [uri]
    (-connect (dc/uri->config uri)))

  (-create-database [uri & opts]
    (apply -create-database (dc/uri->config uri) opts))

  (-delete-database [uri]
    (-delete-database (dc/uri->config uri)))

  (-database-exists? [uri]
    (-database-exists? (dc/uri->config uri)))

  clojure.lang.IPersistentMap
  (-database-exists? [config]
    (let [config (dc/load-config config)
          store-config (:store config)
          raw-store (ds/connect-store store-config)]
      (if (not (nil? raw-store))
        (let [store (kons/add-hitchhiker-tree-handlers
                     (kc/ensure-cache
                      raw-store
                      (atom (cache/lru-cache-factory {} :threshold 1000))))
              stored-db (<?? S (k/get-in store [:db]))]
          (ds/release-store store-config store)
          (not (nil? stored-db)))
        (do
          (ds/release-store store-config raw-store)
          false))))

  (-connect [config]
    (let [config (dc/load-config config)
          store-config (:store config)
          raw-store (ds/connect-store store-config)
          _ (when-not raw-store
              (throw (ex-info "Backend does not exist." {:type :backend-does-not-exist
                                                         :config store-config})))
          store (kons/add-hitchhiker-tree-handlers
                 (kc/ensure-cache
                  raw-store
                  (atom (cache/lru-cache-factory {} :threshold 1000))))
          stored-db (<?? S (k/get-in store [:db]))
          _ (when-not stored-db
              (ds/release-store store-config store)
              (throw (ex-info "Database does not exist." {:type :db-does-not-exist
                                                          :config config})))
          {:keys [eavt-key aevt-key avet-key temporal-eavt-key temporal-aevt-key temporal-avet-key schema rschema config max-tx hash]} stored-db
          empty (db/empty-db nil config)]
      (d/conn-from-db
       (assoc empty
              :max-tx max-tx
              :config config
              :schema schema
              :hash hash
              :max-eid (db/init-max-eid eavt-key)
              :eavt eavt-key
              :aevt aevt-key
              :avet avet-key
              :temporal-eavt temporal-eavt-key
              :temporal-aevt temporal-aevt-key
              :temporal-avet temporal-avet-key
              :rschema rschema
              :store store))))

  (-create-database [config & deprecated-config]
    (let [{:keys [keep-history? initial-tx] :as config} (dc/load-config config deprecated-config)
          store-config (:store config)
          store (kc/ensure-cache
                 (ds/empty-store store-config)
                 (atom (cache/lru-cache-factory {} :threshold 1000)))
          stored-db (<?? S (k/get-in store [:db]))
          _ (when stored-db
              (throw (ex-info "Database already exists." {:type :db-already-exists :config store-config})))
          {:keys [eavt aevt avet temporal-eavt temporal-aevt temporal-avet schema rschema config max-tx hash]}
          (db/empty-db nil config)
          backend (kons/->KonserveBackend store)]
      (<?? S (k/assoc-in store [:db]
                         (merge {:schema   schema
                                 :max-tx max-tx
                                 :hash hash
                                 :rschema  rschema
                                 :config   config
                                 :eavt-key (di/-flush eavt backend)
                                 :aevt-key (di/-flush aevt backend)
                                 :avet-key (di/-flush avet backend)}
                                (when keep-history?
                                  {:temporal-eavt-key (di/-flush temporal-eavt backend)
                                   :temporal-aevt-key (di/-flush temporal-aevt backend)
                                   :temporal-avet-key (di/-flush temporal-avet backend)}))))
      (ds/release-store store-config store)
      (when initial-tx
        (let [conn (-connect config)]
          (transact conn initial-tx)
          (release conn)))))

  (-delete-database [config]
    (let [config (dc/load-config config {})]
      (ds/delete-store (:store config)))))

(defn connect
  ([]
   (-connect {}))
  ([config]
   (-connect config)))
;;deprecation end

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
