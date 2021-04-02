(ns ^:no-doc datahike.migrate
  (:require [datahike.api :as api]
            [datahike.db :as db]
            [clojure.java.io :as io]))

(defn export-db
  "Export the database in a flat-file of datoms at path."
  [db path]
  (with-open [f (io/output-stream path)
              w (io/writer f)]
    (binding [*out* w]
      (doseq [d (datahike.db/-datoms db :eavt [])]
        (prn d)))))

(defn update-max-tx-from-file
  "Find bigest tx in file and update max-tx of db.
  Note: the last tx might not be the biggest if the db
  has been imported before."
  [db file]
  (let [max-tx (->> (line-seq (io/reader file))
                    (map read-string)
                    (reduce #(max %1 (nth %2 3)) 0))]
    (assoc db :max-tx max-tx)))

(defn import-db
  "Import a flat-file of datoms at path into your database."
  [conn path]
  (println "Preparing import of" path "in batches of 1000")
  (swap! conn update-max-tx-from-file path)
  (print "Importing ")
  (time
   (doseq  [datoms (->> (line-seq (io/reader path))
                        (map read-string)
                        (partition 1000 1000 nil))]
     (print ".")
     (api/transact conn (vec datoms)))))

