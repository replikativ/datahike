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
  (let [db-after (fc/insert index-type datom)]
    ;;(println ".----........db before - after insert: " db " ------ " db-after)
    db))

;; TODO: implement
(defn -remove [db datom index-type]
  (let [_ (fc/clear index-type datom)]
    (println "....... remove called! " datom " ---- " index-type)
    db))

;; Called 3 times, one for each index.
(defn empty-db [index-type]
  (let [db (fc/empty-db)]
    ;; (println "db is nil?:" db)
    db))
