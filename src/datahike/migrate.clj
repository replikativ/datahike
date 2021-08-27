(ns ^:no-doc datahike.migrate
  (:require [clojure.java.io :as io]
            [clj-cbor.core :as cbor]))

#_(defn export-db
  "Export the database in a flat-file of datoms at path."
  [db path]
  (with-open [f (io/output-stream path)
              w (io/writer f)]
    (binding [*out* w]
      (doseq [d (datahike.db/-datoms db :eavt [])]
        (prn d)))))

#_(defn update-max-tx-from-file
  "Find bigest tx in file and update max-tx of db.
  Note: the last tx might not be the biggest if the db
  has been imported before."
  [db file]
  (let [max-tx (->> (line-seq (io/reader file))
                    (map read-string)
                    (reduce #(max %1 (nth %2 3)) 0))]
    (assoc db :max-tx max-tx)))

#_(defn import-db
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

(defn coerce-tx [tx]
  (update tx :data (fn [data] (mapv (comp vec seq) data))))

(defmulti export-tx-log
  "Exports tx log"
  {:arglists '([tx-log meta-data path {:keys [format]}])}
  (fn [_ _ _ {:keys [format]}] format))

(defmethod export-tx-log :default [_ _ _ {:keys [format]}]
  (throw (IllegalArgumentException. (str "Can not write unknown export format: " format))))

(defmethod export-tx-log :edn [tx-log meta-data path _]
  (with-open [f (io/output-stream path)
              w (io/writer f)]
    (binding [*out* w]
      (prn meta-data)
      (doseq [tx tx-log]
        (prn (coerce-tx tx))))))

(defmethod export-tx-log :cbor [tx-log meta-data path _]
  (->> tx-log
       (map coerce-tx)
       (cons meta-data)
       (cbor/spit-all path)))
