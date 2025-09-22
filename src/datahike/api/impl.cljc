(ns ^:no-doc datahike.api.impl
  "API input and backwards compatibility. This namespace only ensures
  compatibility and does not implement underlying functionality."
  (:refer-clojure :exclude [filter])
  (:require [datahike.connector :as dc]
            [datahike.config :as config]
            [clojure.spec.alpha :as s]
            [datahike.writer :as dw]
            [datahike.writing :as writing]
            [datahike.constants :as const]
            [datahike.core :as dcore]
            [datahike.spec :as spec]
            [datahike.pull-api :as dp]
            [datahike.query :as dq]
            [datahike.schema :as ds]
            [datahike.tools :as dt]
            [datahike.db :as db #?@(:cljs [:refer [HistoricalDB AsOfDB SinceDB FilteredDB]])]
            [datahike.db.interface :as dbi]
            [datahike.db.transaction :as dbt]
            [datahike.impl.entity :as de])
  #?(:clj
     (:import [clojure.lang Keyword PersistentArrayMap]
              [datahike.db HistoricalDB AsOfDB SinceDB FilteredDB]
              [datahike.impl.entity Entity])))

(defn transact! [connection arg-map]
  (let [arg (cond
              (map? arg-map)      (if (contains? arg-map :tx-data)
                                    arg-map
                                    (dt/raise "Bad argument to transact, map missing key :tx-data."
                                              {:error         :transact/syntax
                                               :argument-keys (keys arg-map)}))
              (or (vector? arg-map)
                  (seq? arg-map)) {:tx-data arg-map}
              :else               (dt/raise "Bad argument to transact, expected map, vector or sequence."
                                            {:error         :transact/syntax
                                             :argument-type (type arg-map)}))]
    (dw/transact! connection arg)))

(defn transact [connection arg-map]
  @(transact! connection arg-map))

;; necessary to support initial-tx shorthand, which really should have been avoided
(defn create-database [& args]
  (let [config @(apply dw/create-database args)]
    (when-let [txs (:initial-tx config)]
      (let [conn (dc/connect config)]
        (transact conn txs)
        (dc/release conn)))
    config))

(defn delete-database [& args]
  @(apply dw/delete-database args))

(defmulti datoms
  (fn
    ([_db arg-map]
     (type arg-map))
    ([_db index & _components]
     (type index))))

(defmethod datoms PersistentArrayMap
  [db {:keys [index components]}]
  (dbi/datoms db index components))

(defmethod datoms Keyword
  [db index & components]
  (if (nil? components)
    (dbi/datoms db index [])
    (dbi/datoms db index components)))

(defmulti seek-datoms
  (fn
    ([_db arg-map]
     (type arg-map))
    ([_db index & _components]
     (type index))))

(defmethod seek-datoms PersistentArrayMap
  [db {:keys [index components]}]
  (dbi/seek-datoms db index components))

(defmethod seek-datoms Keyword
  [db index & components]
  (if (nil? components)
    (dbi/seek-datoms db index [])
    (dbi/seek-datoms db index components)))

(defn with
  ([db arg-map]
   (let [tx-data (if (:tx-data arg-map) (:tx-data arg-map) arg-map)
         tx-meta (if (:tx-meta arg-map) (:tx-meta arg-map) nil)]
     (with db tx-data tx-meta)))
  ([db tx-data tx-meta]
   (if (dcore/is-filtered db)
     (dt/raise "Filtered DB cannot be modified" {:error :transaction/filtered})
     (dbt/transact-tx-data (db/map->TxReport
                            {:db-before db
                             :db-after  db
                             :tx-data   []
                             :tempids   {}
                             :tx-meta   tx-meta}) tx-data))))

(defn db-with [db tx-data]
  (:db-after (with db tx-data)))

(defn db [conn]
  @conn)

(defn since [db time-point]
  (if (dbi/-temporal-index? db)
    (SinceDB. db time-point)
    (dt/raise "since is only allowed on temporal indexed databases." {:config (dbi/-config db)})))

(defn as-of [db time-point]
  (if (dbi/-temporal-index? db)
    (if (int? time-point)
      (if (<= const/tx0 time-point)
        (AsOfDB. db time-point)
        (dt/raise (str "Invalid transaction ID. Must be bigger than " const/tx0 ".")
                  {:time-point time-point}))
      (AsOfDB. db time-point))
    (dt/raise "as-of is only allowed on temporal indexed databases." {:config (dbi/-config db)})))

(defn history [db]
  (if (dbi/-temporal-index? db)
    (HistoricalDB. db)
    (dt/raise "history is only allowed on temporal indexed databases." {:config (dbi/-config db)})))

(defn index-range [db {:keys [attrid start end]}]
  (dbi/index-range db attrid start end))

(defn schema [db]
  (reduce-kv
   (fn [m k v]
     (cond
       (and (keyword? k)
            (not (or (ds/entity-spec-attr? k)
                     (ds/schema-attr? k)
                     (ds/sys-ident? k)))) (update m k #(merge % v))
       (number? k)                        (update m v #(merge % {:db/id k}))
       :else                              m))
   {}
   (dbi/-schema db)))

(defn reverse-schema [db]
  (reduce-kv
   (fn [m k v]
     (let [attrs (->> v
                      (remove #(or (ds/entity-spec-attr? %)
                                   (ds/sys-ident? %)
                                   (ds/schema-attr? %)))
                      (into #{}))]
       (if (empty? attrs)
         m
         (assoc m k attrs))))
   {}
   (dbi/-rschema db)))
