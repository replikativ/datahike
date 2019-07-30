(ns datahike.api
  (:refer-clojure :exclude [filter])
  (:require [datahike.core :as d]
            [datahike.connector :as dc]
            [datahike.db :as db #?@(:cljs [:refer [CurrentDB]])]
            [superv.async :refer [<?? S]])
  #?(:clj
     (:import [datahike.db HistoricalDB AsOfDB SinceDB]
              [java.util Date])))

(def connect dc/connect)

(def
  ^{:arglists '([uri] [{:keys [uri initial-tx schema-on-read temporal-index]}])
    :doc "Creates a database with optional configuration"}
  create-database
  dc/create-database)

(def delete-database dc/delete-database)

(def transact dc/transact)

(def transact! dc/transact!)

(def release dc/release)

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

(defn history [conn]
  (if (db/-temporal-index? @conn)
    (HistoricalDB. @conn)
    (throw (ex-info "as-of is only allowed on temporal indexed dbs" conn))))

(defn- platform-date? [d]
  #?(:cljs (instance? js/Date d)
     :clj (instance? Date d)))

(defn- get-time [d]
  #?(:cljs (.getTime d)
     :clj (.getTime ^Date d)))

(defn as-of [conn date]
  (if (db/-temporal-index? @conn)
    (AsOfDB. @conn (if (platform-date? date) (get-time date) date))
    (throw (ex-info "as-of is only allowed on temporal indexed dbs"))))

(defn since [conn date]
  (if (db/-temporal-index? @conn)
    (SinceDB. @conn (if (platform-date? date) (get-time date) date))
    (throw (ex-info "since is only allowed on temporal indexed dbs"))))

(def with d/with)
