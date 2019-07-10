(ns datahike.connector
  (:require [datahike.db :as db]
            [datahike.core :as d]
            [datahike.index :as di]
            [datahike.store :as ds]
            [hitchhiker.konserve :as kons]
            [konserve.core :as k]
            [konserve.cache :as kc]
            [superv.async :refer [<?? S]]
            [clojure.core.cache :as cache])
  (:import [java.net URI]))

(defn- parse-uri [uri]
  (let [base-uri (URI. uri)
        scheme (.getScheme base-uri)
        sub-uri (URI. (.getSchemeSpecificPart base-uri))
        store-scheme (.getScheme sub-uri)
        path (.getPath sub-uri)]
    [scheme store-scheme path]))

(defn create-database
  ([uri]
   (create-database uri nil))
  ([uri schema]
   (let [[m store-scheme path] (parse-uri uri)
         _ (when-not m
             (throw (ex-info "URI cannot be parsed." {:uri uri})))
         store (kc/ensure-cache
                  (ds/empty-store store-scheme path)
                  (atom (cache/lru-cache-factory {} :threshold 1000)))
         stored-db (<?? S (k/get-in store [:db]))
         _ (when stored-db
             (throw (ex-info "Database already exists." {:type :db-already-exists :uri uri})))
         {:keys [eavt aevt avet rschema]} (db/empty-db schema (ds/scheme->index store-scheme))
         backend (kons/->KonserveBackend store)]
     (<?? S (k/assoc-in store [:db]
                        {:schema schema
                         :eavt-key (di/-flush eavt backend)
                         :aevt-key (di/-flush aevt backend)
                         :avet-key (di/-flush avet backend)
                         :rschema rschema}))
     (ds/release-store store-scheme store)
     nil)))

(defn delete-database [uri]
  (let [[m store-scheme path] (parse-uri uri)]
    (ds/delete-store store-scheme path)))

(defn connect [uri]
  (let [[scheme store-scheme path] (parse-uri uri)
        store (kons/add-hitchhiker-tree-handlers
               (kc/ensure-cache
                (ds/connect-store store-scheme path)
               (atom (cache/lru-cache-factory {} :threshold 1000))))
        stored-db (<?? S (k/get-in store [:db]))
        ;_ (prn stored-db)
        _ (when-not stored-db
            (ds/release-store store-scheme store)
            (throw (ex-info "Database does not exist." {:type :db-does-not-exist
                                                  :uri uri})))
        {:keys [eavt-key aevt-key avet-key schema rschema]} stored-db
        empty (db/empty-db)]
    (d/conn-from-db
     (assoc empty
            :schema schema
            :max-eid (db/init-max-eid eavt-key)
            :eavt eavt-key
            :aevt aevt-key
            :avet avet-key
            :rschema rschema
            :store store
            :uri uri))))

(defn release [conn]
  (let [[m store-scheme path] (parse-uri (:uri @conn))]
    (ds/release-store store-scheme (:store @conn))))

(defn transact [connection tx-data]
  {:pre [(d/conn? connection)]}
  (future
    (locking connection
      (let [{:keys [db-after] :as tx-report} @(d/transact connection tx-data)
            {:keys [eavt aevt avet schema rschema]} db-after
            store (:store @connection)
            backend (kons/->KonserveBackend store)
            eavt-flushed (di/-flush eavt backend)
            aevt-flushed (di/-flush aevt backend)
            avet-flushed (di/-flush avet backend)]
        (<?? S (k/assoc-in store [:db]
                           {:schema schema
                            :rschema rschema
                            :eavt-key eavt-flushed
                            :aevt-key aevt-flushed
                            :avet-key avet-flushed}))
        (reset! connection (assoc db-after
                                  :eavt eavt-flushed
                                  :aevt aevt-flushed
                                  :avet avet-flushed))
        tx-report))))

(defn transact! [connection tx-data]
  (deref (transact connection tx-data)))
