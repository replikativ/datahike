(ns datahike.index.fdb
  (:require [datahike.datom :as dd]
            [fdb.core :as f])
  )

(def -slice identity)

(def -seq seq)

(def -count count)

(def -all identity)

(def -flush identity)

(def -transient identity)

(def -persistent! identity)


(defn -insert [db datom index-type]
  (f/insert index-type datom))

(defn -remove [db datom index-type]
  )

(defn empty-db [index-type]
  ;; TODO: Check why it goes here 3 times !?
  (f/empty-db))
