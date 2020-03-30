(ns performance.db.datomic
  (:require [datomic.api :as d]
            [performance.db.interface :as db]))


(defmethod db/connect :datomic [_ uri] (d/connect uri))

(defmethod db/transact :datomic [_ conn tx] (d/transact conn tx))

(defmethod db/release :datomic [_ conn] (d/release conn))

(defmethod db/db :datomic [_ conn] (d/db conn))

(defmethod db/q :datomic [_ query db] (d/q query db))

(defmethod db/init :datomic [_ uri _]
  (d/delete-database uri)
  (d/create-database uri))
