(ns datahike.index.fdb
  (:require [datahike.datom :as dd]
            [fdb.core :as fc]
            [fdb.keys :as fk])
  )

(defn -slice [db from to index-type]
  (fc/get-range index-type from to))

(def -seq seq)

(def -count count)

(def -all identity)

(def -flush identity)

(def -transient identity)

(def -persistent! identity)


(defn -insert [db datom index-type]
  (fc/insert index-type datom))

(defn -remove [db datom index-type]
  )

(defn empty-db [index-type]
  ;; TODO: Check why it goes here 3 times !?
  (fc/empty-db))
