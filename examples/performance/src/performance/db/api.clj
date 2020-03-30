(ns performance.db.api
  (:require [performance.db.interface :as db]
            [performance.db.datahike]
            [performance.db.datomic]
            [performance.db.hitchhiker]))


;; Convenience functions for setup and cleanup
;; Do not use for function to test


;; Functions from interface

(defn connect [lib uri] (db/connect lib uri))
(defn transact [lib conn tx] (db/transact lib conn tx))
(defn release [lib conn] (db/release lib conn))
(defn db [lib conn] (db/db lib conn))
(defn q [lib query db] (db/q lib query db))
(defn init [lib uri args] (db/init lib uri args))


;; Others

(defn init-and-connect [lib uri & args]
  (init lib uri args)
  (connect lib uri))

(defn init-schema-and-connect [lib uri schema & args]
  (let [conn (apply init-and-connect lib uri args)]
    (transact lib conn schema)
    conn))

(defn prepare-db-and-connect [lib uri schema tx & args]
  (let [conn (apply init-and-connect lib uri args)]
    (transact lib conn schema)
    (transact lib conn tx)
    conn))

(defn prepare-db [lib uri schema tx & args]
  (let [conn (apply init-and-connect lib uri args)]
    (transact lib conn schema)
    (transact lib conn tx)
    (release lib conn)))




