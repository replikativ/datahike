(ns performance.db
  (:require [datahike.api :as d]
            [datomic.api :as da]
            [performance.hitchhiker :as hh]))

;; Convenience functions for setup and cleanup
;; Do not use for function to test

(defn connect [lib uri]
  (case lib
    "datahike" (d/connect uri)
    "datomic" (da/connect uri)
    "hitchhiker" (hh/connect uri)))

(defn transact [lib conn tx]
  (case lib
    "datahike" (d/transact conn tx)
    "datomic" @(da/transact conn tx)
    "hitchhiker" (hh/transact conn tx)                                        ;; (hmsg/insert tree (datom->node datom :eavt) nil)
    ))

(defn release [lib conn]
  (case lib
    "datahike" (d/release conn)
    "datomic" (da/release conn)
    "hitchhiker" nil))

(defn db [lib conn]
  (case lib
    "datahike" (d/db conn)
    "datomic" (da/db conn)
    "hitchhiker" nil))

(defn q [lib query db]
  (case lib
    "datahike" (d/q query db)
    "datomic" (da/q query db)
    "hitchhiker" nil))

(defn init [lib uri args]
  (case lib
    "datahike" (do (d/delete-database uri)
                   (apply d/create-database uri args))
    "datomic" (do (da/delete-database uri)
                  (da/create-database uri))
    "hitchhiker" (do (hh/delete uri)
                     (hh/create uri))))


(defn init-and-connect [lib uri & args]
  (do (init lib uri args)
      (connect lib uri)))

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




