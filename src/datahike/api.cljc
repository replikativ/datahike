(ns datahike.api
  (:refer-clojure :exclude [filter])
  (:require [datahike.core :as d]
            [datahike.connector :as dc]
            [datahike.db :as db #?@(:cljs [:refer [CurrentDB]])]
            [superv.async :refer [<?? S]])
  #?(:clj
     (:import [datahike.db CurrentDB])))

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
  (CurrentDB. @conn))

(defn history [conn]
  @conn)

(def with d/with)
