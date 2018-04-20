(ns datahike.api
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
        _ (when-not stored-db
            (case proto
              "level"
              (kl/release store))
            (throw (ex-info "DB does not exist." {:uri uri})))
        {:keys [eavt-key aevt-key avet-key]} stored-db
        empty (db/empty-db)
        eavt-durable (hc/<?? (kons/create-tree-from-root-key store eavt-key))]
    (d/conn-from-db
     (assoc empty
            :max-eid (db/init-max-eid (:eavt empty) eavt-durable)
            :eavt-durable eavt-durable
            :aevt-durable (hc/<?? (kons/create-tree-from-root-key store aevt-key))
            :avet-durable (hc/<?? (kons/create-tree-from-root-key store avet-key))
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
            (throw (ex-info "DB already exist." {:uri uri})))
        {:keys [eavt-durable aevt-durable avet-durable] :as new-db} (db/empty-db schema)
        backend (kons/->KonserveBackend store)]
    (<?? S (k/assoc-in store [:db]
                       {:eavt-key (kons/get-root-key (:tree (hc/<?? (hc/flush-tree eavt-durable backend))))
                        :aevt-key (kons/get-root-key (:tree (hc/<?? (hc/flush-tree aevt-durable backend))))
                        :avet-key (kons/get-root-key (:tree (hc/<?? (hc/flush-tree avet-durable backend))))}

                       ))
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
            {:keys [eavt-durable aevt-durable avet-durable]} db-after
            store (:store @connection)
            backend (kons/->KonserveBackend store)
            eavt-flushed (:tree (hc/<?? (hc/flush-tree eavt-durable backend)))
            aevt-flushed (:tree (hc/<?? (hc/flush-tree aevt-durable backend)))
            avet-flushed (:tree (hc/<?? (hc/flush-tree avet-durable backend)))]
        (<?? S (k/assoc-in store [:db]
                           {:eavt-key (kons/get-root-key eavt-flushed)
                            :aevt-key (kons/get-root-key aevt-flushed)
                            :avet-key (kons/get-root-key avet-flushed)}))
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

