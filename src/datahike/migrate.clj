(ns ^:no-doc datahike.migrate
  (:require [datahike.api :as api]
            [datahike.datom :as d]
            [datahike.db :as db]
            [clj-cbor.core :as cbor]))

(defn export-db
  "Export the database in a flat-file of datoms at path."
  [db path]
  (cbor/spit-all path (map seq (sort-by
                                (juxt d/datom-tx :e)
                                (api/datoms (if (db/-keep-history? db) (api/history db) db) :eavt)))))

(defn update-max-tx
  "Find bigest tx in datoms and update max-tx of db.
  Note: the last tx might not be the biggest if the db
  has been imported before."
  [db datoms]
  (assoc db :max-tx (reduce #(max %1 (nth %2 3)) 0 datoms)))

(defn- instance-to-date [v]
  (if (instance? java.time.Instant v) (java.util.Date/from v) v))

(defn import-db
  "Import a flat-file of datoms at path into your database."
  [conn path]
  (println "Preparing import of" path "in batches of 1000")
  (let [datoms (->> (cbor/slurp-all path)
                    (map #(-> (apply d/datom %) (update :v instance-to-date))))]
    (swap! conn update-max-tx datoms)
    (print "Importing ")
    (api/transact conn (vec datoms))))
