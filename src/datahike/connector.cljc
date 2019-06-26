(ns datahike.connector
  (:require [datahike.db :as db]
            [datahike.core :as d]
            [datahike.index :as di]
            [hitchhiker.konserve :as kons]
            [konserve.filestore :as fs]
            [konserve-leveldb.core :as kl]
            [konserve.core :as k]
            [konserve.cache :as kc]
            [konserve.memory :as mem]
            [superv.async :refer [<?? S]]
            [clojure.spec.alpha :as s]
            [clojure.core.cache :as cache])
  (:import [java.net URI]))

(defmulti empty-store
  "Creates an empty store"
  {:arglists '([store-scheme path])}
  (fn [store-scheme path] store-scheme))

(defmulti delete-store
  "Deletes an existing store"
  {:arglists '([store-scheme path])}
  (fn [store-scheme path] store-scheme))

(defmulti release-store
  "Releases the connection to an existing store (optional)."
  {:arglists '([store-scheme store])}
  (fn [store-scheme store] store-scheme))

(defmethod release-store :default [_ _] nil)

(defmulti connect-store
  "Makes a connection to an existing store"
  {:arglists '([store-scheme path])}
  (fn [store-scheme store] store-scheme))

(defmulti store-index
  "Returns the index type to use for this store"
  {:arglists '([store-scheme])}
  (fn [store-scheme] store-scheme))

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
                  (empty-store store-scheme path)
                  (atom (cache/lru-cache-factory {} :threshold 1000))) ;; TODO: move store to separate ns
         stored-db (<?? S (k/get-in store [:db]))
         _ (when stored-db
             (throw (ex-info "Database already exists." {:type :db-already-exists :uri uri})))
         {:keys [eavt aevt avet rschema]} (db/empty-db schema (store-index store-scheme))
         backend (kons/->KonserveBackend store)]
     (<?? S (k/assoc-in store [:db]
                        {:schema schema
                         :eavt-key (di/-flush eavt backend)
                         :aevt-key (di/-flush aevt backend)
                         :avet-key (di/-flush avet backend)
                         :rschema rschema}))
     (release-store store-scheme store)
     nil)))

(defn delete-database [uri]
  (let [[m store-scheme path] (parse-uri uri)]
    (delete-store store-scheme path)))

(defn connect [uri]
  (let [[scheme store-scheme path] (parse-uri uri)
        store (kons/add-hitchhiker-tree-handlers
               (kc/ensure-cache
                (connect-store store-scheme path)
               (atom (cache/lru-cache-factory {} :threshold 1000))))
        stored-db (<?? S (k/get-in store [:db]))
        ;_ (prn stored-db)
        _ (when-not stored-db
            (release-store store-scheme store)
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
    (release-store store-scheme (:store @conn))))

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

;; mem store

(def memory (atom {}))

(defmethod connect-store "mem" [_ uri]
  (@memory uri))

(defmethod empty-store "mem" [_ uri]
  (let [store (<?? S (mem/new-mem-store))]
    (swap! memory assoc uri store)
    store))

(defmethod delete-store "mem" [_ uri]
  (swap! memory dissoc uri))

(defmethod store-index "mem" [_]
  :datahike.index/persistent-set)

;; file

(defmethod empty-store "file" [_ uri]
  (kons/add-hitchhiker-tree-handlers
    (<?? S (fs/new-fs-store uri))))

(defmethod connect-store "file" [_ uri]
  (<?? S (fs/new-fs-store uri)))

(defmethod delete-store  "file" [_ uri]
  (fs/delete-store uri))

(defmethod store-index "file" [_]
  :datahike.index/hitchhiker-tree)

;; level

(defmethod empty-store "level" [_ uri]
  (kons/add-hitchhiker-tree-handlers
    (<?? S (kl/new-leveldb-store uri))))

(defmethod connect-store "level" [_ uri]
  (<?? S (kl/new-leveldb-store uri)))

(defmethod delete-store "level" [_ uri]
  (kl/delete-store uri))

(defmethod release-store "level" [_ store]
  (kl/release store))

(defmethod store-index "level" [_]
  :datahike.index/hitchhiker-tree)
