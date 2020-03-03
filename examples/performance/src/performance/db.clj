(ns performance.db
  (:require [datahike.api :as d]
            [datomic.api :as da]))

;; Convenience functions for setup and cleanup
;; Do not use for function to test

(defn transact [lib conn tx]
  (print "tx")
  (case lib
    "datahike" (d/transact conn tx)
    "datomic" @(da/transact conn tx)))

(defn connect [lib uri]
  (print "c")
  (case lib
    "datahike" (d/connect uri)
    "datomic" (da/connect uri)))

(defn release [lib conn]
  (print "rel")
  (case lib
    "datahike" (d/release conn)
    "datomic" (da/release conn)))

(defn db [lib conn]
  (print "db")
  (case lib
    "datahike" (d/db conn)
    "datomic" (da/db conn)))

(defn q [lib query db]
  (print "q")
  (case lib
    "datahike" (d/q query db)
    "datomic" (da/q query db)))

(defn init-and-connect [lib uri & args]
  (print "ic")
  (case lib
    "datahike"
    (do
      (d/delete-database uri)
      (apply d/create-database uri args)
      (d/connect uri))
    "datomic"
    (do
      (da/delete-database uri)
      (da/create-database uri)
      (da/connect uri))))

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
