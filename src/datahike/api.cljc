(ns datahike.api
  (:refer-clojure :exclude [filter])
  (:require [datahike.db :as db]
            [datahike.core :as d]
            [datahike.connector :as dc]
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
  (:import [java.net URI]))

(def connect dc/connect)

(def create-database dc/create-database)

(def create-database-with-schema dc/create-database)

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

(def with d/with)
