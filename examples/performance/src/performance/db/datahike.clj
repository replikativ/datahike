(ns performance.db.datahike
  (:require [datahike.api :as d]
            [performance.db.interface :as db]))


(defmethod db/connect :datahike [_ uri] (d/connect uri))

(defmethod db/transact :datahike [_ conn tx] (d/transact conn tx))

(defmethod db/release :datahike [_ conn] (d/release conn))

(defmethod db/db :datahike [_ conn] (d/db conn))

(defmethod db/q :datahike [_ query db] (d/q query db))

(defmethod db/init :datahike [_ uri args]
  (d/delete-database uri)
  (apply d/create-database uri args))
