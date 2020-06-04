(ns datahike.index.fdb
  (:require [datahike.datom :as dd]
            [fdb.core :as fc]
            [fdb.keys :as fk])
  )


(defn- index-type->datom-fn [index-type]
  (case index-type
    :aevt (fn [[a e v tx]] (dd/datom e a v tx true))
    :avet (fn [[a v e tx]] (dd/datom e a v tx true))
    (fn [[e a v tx]] (dd/datom e a v tx true))))


(defn -slice [db from to index-type]
  (map (index-type->datom-fn index-type)
    (fc/get-range index-type from to)))

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
