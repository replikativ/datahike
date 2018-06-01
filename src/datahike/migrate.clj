(ns datahike.migrate
  (:require [datahike.db :as db]
            [datahike.api :as api]
            [clojure.java.io :as io]))


(defn export-db
  "Export the database in a flat-file of datoms at path."
  [db path]
  (with-open [f (io/output-stream path)
              w (io/writer f)]
    (binding [*out* w]
      (doseq [d (db/-datoms db :eavt [])]
        (prn d)))))


(defn import-db
  "Import a flat-file of datoms at path into your database."
  [conn path]
  (doseq  [datoms (->> (line-seq (io/reader path))
                       (map read-string)
                       (partition 1000 1000 nil))]
    @(api/transact conn datoms)))
