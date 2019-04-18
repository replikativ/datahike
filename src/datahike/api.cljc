(ns datahike.api
  (:refer-clojure :exclude [filter])
  (:require [datahike.db :as db]
            [datahike.core :as d]
            [hitchhiker.konserve :as kons]
            [hitchhiker.tree.core :as hc]
            [konserve.filestore :as fs]
            [konserve-leveldb.core :as kl]
            [konserve.core :as k]
            [konserve.cache :as kc]
            [konserve.memory :as mem]
            [superv.async :refer [<?? S]]
            [clojure.core.cache :as cache]
            [datahike.index :as di])
  (:import [java.net URI]
           [datahike.index HitchhikerTree]))

(def memory (atom {}))

(defn parse-uri [uri]
  (let [base-uri (URI. uri)
        m (.getScheme base-uri)
        sub-uri (URI. (.getSchemeSpecificPart base-uri))
        proto (.getScheme sub-uri)
        path (.getPath sub-uri)]
    [m proto path]))

(defn connect [uri]
  (let [[m proto path] (parse-uri uri) #_(re-find #"datahike:(.+)://(/.+)" uri)
        _ (when-not m
            (throw (ex-info "URI cannot be parsed." {:uri uri})))
        store (kons/add-hitchhiker-tree-handlers
               (kc/ensure-cache
                (case proto
                 "mem"
                 (@memory uri)
                 "file"
                  (<?? S (fs/new-fs-store path))
                 "level"
                  (<?? S (kl/new-leveldb-store path)))
               (atom (cache/lru-cache-factory {} :threshold 1000))))
        stored-db (<?? S (k/get-in store [:db]))
        _ (prn stored-db)
        _ (when-not stored-db
            (case proto
              "level"
              (kl/release store)
              nil)
            (throw (ex-info "DB does not exist." {:type :db-does-not-exist
                                                  :uri uri})))
        {:keys [eavt-key aevt-key avet-key schema rschema]} stored-db
        empty (db/empty-db)
        eavt (di/hitchhiker-tree :eavt eavt-key)
        aevt (di/hitchhiker-tree :aevt aevt-key)
        avet (di/hitchhiker-tree :avet avet-key)]
    (d/conn-from-db
     (assoc empty
            :schema schema
            :max-eid (db/init-max-eid eavt)
            :eavt eavt
            :aevt aevt
            :avet avet
            :rschema rschema
            :store store
            :uri uri))))

(defn create-database-with-schema [uri schema]
  (let [[m proto path] (parse-uri uri)
        _ (when-not m
            (throw (ex-info "URI cannot be parsed." {:uri uri})))
        store (kc/ensure-cache
               (case proto
                 "mem"
                 (let [store (<?? S (mem/new-mem-store))]
                   (swap! memory assoc uri store)
                   store)
                 "file"
                 (kons/add-hitchhiker-tree-handlers
                  (<?? S (fs/new-fs-store path)))
                 "level"
                 (kons/add-hitchhiker-tree-handlers
                  (<?? S (kl/new-leveldb-store path))))
               (atom (cache/lru-cache-factory {} :threshold 1000)))
        stored-db (<?? S (k/get-in store [:db]))
        _ (when stored-db
            (throw (ex-info "DB already exist." {:type :db-already-exists
                                                 :uri uri})))
        {:keys [eavt aevt avet rschema]} (db/empty-db schema)
        backend (kons/->KonserveBackend store)
        eavt-key (di/-iterator eavt)
        aevt-key (di/-iterator aevt)
        avet-key (di/-iterator avet)]
    (<?? S (k/assoc-in store [:db]
                       {:schema schema
                        :eavt-key (:tree (hc/<?? (hc/flush-tree-without-root eavt-key backend)))
                        :aevt-key (:tree (hc/<?? (hc/flush-tree-without-root aevt-key backend)))
                        :avet-key (:tree (hc/<?? (hc/flush-tree-without-root avet-key backend)))
                        :rschema rschema}))
    (case proto
      "level"
      (kl/release store)
      nil)
    nil))

(defn create-database [uri]
  (create-database-with-schema uri nil))

(defn delete-database [uri]
  (let [[m proto path] (parse-uri uri)]
    (case proto
      "mem"
      (swap! memory dissoc uri)
      "file"
      (fs/delete-store path)
      "level"
      (kl/delete-store path))))

(defn transact [connection tx-data]
  {:pre [(d/conn? connection)]}
  (future
    (locking connection
      (let [{:keys [db-after] :as tx-report} @(d/transact connection tx-data)
            {:keys [ eavt  aevt avet schema rschema]} db-after
            store (:store @connection)
            backend (kons/->KonserveBackend store)
            _ (clojure.pprint/pprint store)
            eavt-flushed (:tree (hc/<?? (hc/flush-tree-without-root (di/-iterator eavt) backend)))
            aevt-flushed (:tree (hc/<?? (hc/flush-tree-without-root (di/-iterator aevt) backend)))
            avet-flushed (:tree (hc/<?? (hc/flush-tree-without-root (di/-iterator avet) backend)))
            _ (di/-set-iterator! eavt eavt-flushed)
            _ (di/-set-iterator! aevt aevt-flushed)
            _ (di/-set-iterator! avet avet-flushed)]
        (<?? S (k/assoc-in store [:db]
                           {:schema schema
                            :rschema rschema
                            :eavt-key eavt-flushed
                            :aevt-key aevt-flushed
                            :avet-key avet-flushed}))
        (reset! connection db-after)
        tx-report))))

(defn release [conn]
  (let [[m proto path] (re-find #"datahike:(.+)://(/.+)" (:uri @conn))]
    (case proto
      "mem"
      nil
      "file"
      nil
      "level"
      (kl/release (:store @conn)))))

(def pull d/pull)

(def pull-many d/pull-many)

(def q d/q)

(def seek-datoms d/seek-datoms)

(def tempid d/tempid)

(def entity d/entity)

(def entity-db d/entity-db)

(def filter d/filter)

(defn db [conn]
  @conn)

(def with d/with)
