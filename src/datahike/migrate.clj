(ns ^:no-doc datahike.migrate
  (:require [datahike.api :as api]
            [datahike.constants :as c]
            [datahike.datom :as d]
            [datahike.db :as db]
            [clj-cbor.core :as cbor]))

(defn export-db
  "Export the database in a flat-file of datoms at path."
  [conn path]
  (let [db @conn
        cfg (:config db)]
    (cbor/spit-all path (cond->> (sort-by
                                  (juxt d/datom-tx :e)
                                  (api/datoms (if (:keep-history? cfg) (api/history db) db) :eavt))
                          (:attribute-refs? cfg) (remove #(= (d/datom-tx %) c/tx0))
                          true (map seq)))))

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
