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
            [clojure.core.cache :as cache]))


(def memory (atom {}))

(defn connect [uri]
  (let [[m proto path] (re-find #"datahike:(.+)://(/.+)" uri)
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
        ;_ (prn stored-db)
        _ (when-not stored-db
            (case proto
              "level"
              (kl/release store)
              nil)
            (throw (ex-info "DB does not exist." {:type :db-does-not-exist
                                                  :uri uri})))
        {:keys [eavt-key aevt-key avet-key schema rschema]} stored-db
        empty (db/empty-db)
        eavt-durable eavt-key]
    ;(prn eavt-durable)
    (d/conn-from-db
     (assoc empty
            :schema schema
            :max-eid (db/init-max-eid (:eavt empty) eavt-durable)
            :eavt-durable eavt-durable
            :aevt-durable aevt-key
            :avet-durable avet-key
            :rschema rschema
            :store store
            :uri uri))))



(defn create-database-with-schema [uri schema]
  (let [[m proto path] (re-find #"datahike:(.+)://(/.+)" uri)
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
        {:keys [eavt-durable aevt-durable avet-durable rschema] :as new-db} (db/empty-db schema)
        backend (kons/->KonserveBackend store)]
    (<?? S (k/assoc-in store [:db]
                        {:schema schema
                         :eavt-key (:tree (hc/<?? (hc/flush-tree-without-root eavt-durable backend)))
                         :aevt-key (:tree (hc/<?? (hc/flush-tree-without-root aevt-durable backend)))
                         :avet-key (:tree (hc/<?? (hc/flush-tree-without-root avet-durable backend)))
                         :rschema rschema}))
    (case proto
      "level"
      (kl/release store)
      nil)
    nil))

(defn create-database [uri]
  (create-database-with-schema uri nil))

(defn delete-database [uri]
  (let [[m proto path] (re-find #"datahike:(.+)://(/.+)" uri)]
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
            {:keys [eavt-durable aevt-durable avet-durable schema rschema]} db-after
            store (:store @connection)
            backend (kons/->KonserveBackend store)
            eavt-flushed (:tree (hc/<?? (hc/flush-tree-without-root eavt-durable backend)))
            aevt-flushed (:tree (hc/<?? (hc/flush-tree-without-root aevt-durable backend)))
            avet-flushed (:tree (hc/<?? (hc/flush-tree-without-root avet-durable backend)))]
        (<?? S (k/assoc-in store [:db]
                           {:schema schema
                            :rschema rschema
                            :eavt-key eavt-flushed
                            :aevt-key aevt-flushed
                            :avet-key avet-flushed}))
        (reset! connection (assoc db-after
                                  :eavt-durable eavt-flushed
                                  :aevt-durable aevt-flushed
                                  :avet-durable avet-flushed))
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

