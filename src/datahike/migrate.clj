(ns ^:no-doc datahike.migrate
  (:require [datahike.api :as api]
            [datahike.datom :as d]
            [datahike.db :as db]
            [clj-cbor.core :as cbor]
            [clojure.java.io :as io]
            [wanderung.datahike :as wd]
            [tablecloth.api :as tc]))

(defn export-db
  "Export the database in a flat-file of datoms at path."
  [db path]
  (with-open [f (io/output-stream path)
              w (io/writer f)]
    (binding [*out* w]
      (doseq [d (datahike.db/-datoms db :eavt [])]
        (prn d)))))

(defn export-db-wanderung
  "Export the database in a flat-file of datoms at path."
  [db path]
  (with-open [f (io/output-stream path)
              w (io/writer f)]
    (binding [*out* w]
      (let [txs (sort-by first (api/q wd/find-tx-datoms db (java.util.Date. 70)))
            query {:query wd/find-datoms-in-tx
                   :args [(api/history db)]}]
        (cbor/spit-all path (mapcat (fn [[tid tinst]]
                                      (->> (api/q (update-in query [:args] conj tid))
                                           (sort-by first)
                                           (into [[tid :db/txInstant tinst tid true]])))
                                    txs))))))

(defn export-db-tc
  "Export the database in a flat-file of datoms at path."
  [db path]
  (with-open [f (io/output-stream path)
              w (io/writer f)]
    (binding [*out* w]
      (let [datoms (api/datoms (api/history db) :eavt)
            datoms-table (tc/dataset (map seq datoms)
                                     {:layout :as-rows
                                      :column-names [:eid :attr :val :txid :assert]})]
        (cbor/spit-all path (tc/rows (tc/order-by datoms-table [:txid :eid]) :as-seq))))))

(defn export-db-clj
  "Export the database in a flat-file of datoms at path."
  [db path]
  (with-open [f (io/output-stream path)
              w (io/writer f)]
    (binding [*out* w]
      (cbor/spit-all path (map seq (sort-by (juxt d/datom-tx :e) (api/datoms (api/history db) :eavt)))))))

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
    (time
     (doseq [partitions (partition 1000 1000 nil datoms)]
       (print ".")
       (api/transact conn (vec datoms))))))

(comment
  (require '[datahike.api :as api]
           '[datahike.datom :as datom])

  (def schema [{:db/ident       :name
                :db/cardinality :db.cardinality/one
                :db/index       true
                :db/unique      :db.unique/identity
                :db/valueType   :db.type/string}
               {:db/ident       :parents
                :db/cardinality :db.cardinality/many
                :db/valueType   :db.type/ref}
               {:db/ident       :age
                :db/cardinality :db.cardinality/one
                :db/valueType   :db.type/long}])

  (def export-cfg {:store              {:backend :mem
                                        :id      "export"}
                   :keep-history?      true
                   :schema-flexibility :write
                   :attribute-refs?    false})
  (do
    (api/delete-database export-cfg)
    (api/create-database export-cfg))
  (def export-conn (api/connect export-cfg))

  (api/transact export-conn schema)
  (api/transact export-conn {:tx-data [{:name "Alice" :age  25}
                                       {:name "Bob" :age  35}
                                       {:name    "Charlie"
                                        :age     5
                                        :parents [[:name "Alice"] [:name "Bob"]]}
                                       {:name "Daisy" :age 20}
                                       {:name "Erhard" :age 20}]})
  (api/transact export-conn {:tx-data [{:name "Erhard" :age 20}]})
  (api/transact export-conn {:tx-data [[:db/retractEntity [:name "Erhard"]]]})
  (api/transact export-conn {:tx-data [{:name "Donald" :age 25}]})
  (api/transact export-conn {:tx-data [{:name "Donald" :age 20}]})

  (export-db-wanderung @export-conn "/tmp/dh.dump")
  (export-db-tc @export-conn "/tmp/dh.dump")
  (export-db-clj @export-conn "/tmp/dh.dump")

  (def import-cfg {:store              {:backend :mem
                                        :id      "import"}
                   :keep-history?      true
                   :schema-flexibility :write
                   :attribute-refs?    false})
  (do
    (api/delete-database import-cfg)
    (api/create-database import-cfg))
  (def import-conn (api/connect import-cfg))
  (datahike.migrate/import-db import-conn "/tmp/dh.dump")

  (defn temporal-datoms [db] (api/datoms (api/history db) :eavt))
  (defn imported-datoms [db] (filter #(< (datom/datom-tx %) (:max-tx db)) (temporal-datoms db)))
  imported-datoms
  (= (temporal-datoms @export-conn)
     (imported-datoms @import-conn)))
