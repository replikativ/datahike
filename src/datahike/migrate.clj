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

(comment
  ;; For the record, for now
  (require '[datahike.api :as api]
           '[clj-cbor.core :as cbor]
           ;; note: the following no longer required in deps.edn
           '[wanderung.datahike :as wd]
           '[tablecloth.api :as tc])

  (defn export-db-wanderung
    "Export the database in a flat-file of datoms at path."
    [db path]
    (let [txs (sort-by first (api/q wd/find-tx-datoms db (java.util.Date. 70)))
          query {:query wd/find-datoms-in-tx
                 :args [(if (db/-keep-history? db) (api/history db) db)]}]
      (cbor/spit-all path (mapcat (fn [[tid tinst]]
                                    (->> (api/q (update-in query [:args] conj tid))
                                         (sort-by first)
                                         (into [[tid :db/txInstant tinst tid true]])))
                                  txs))))

  ;; Dealbreaker: Cannot work with byte arrays
  (defn export-db-tc
    "Export the database in a flat-file of datoms at path."
    [db path]
    (let [datoms (api/datoms (if (db/-keep-history? db) (api/history db) db) :eavt)
          datoms-table (tc/dataset (map seq datoms)
                                   {:layout :as-rows
                                    :column-names [:eid :attr :val :txid :assert]})]
      (cbor/spit-all path (tc/rows (tc/order-by datoms-table [:txid :eid]) :as-seq)))))
