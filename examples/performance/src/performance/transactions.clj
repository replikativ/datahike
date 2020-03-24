(ns performance.transactions
  (:require [performance.measure :refer [measure-tx-times]]
            [performance.db :as db]
            [performance.error :as e]
            [incanter.core :as ic]
            [incanter.io]
            [performance.conf :as c])
  (:import (java.util Date UUID)
           (java.text SimpleDateFormat)))


(def schema [{:db/ident       :name
              :db/valueType   :db.type/string
              :db/cardinality :db.cardinality/one}])


(defn create-n-transactions [n]
  (->> (repeatedly #(str (UUID/randomUUID)))
       (take n)
       (mapv (fn [id] {:name id}))))


(defn run-combinations [uris iterations]
  "Returns observation in following order:
   [:backend :schema-on-read :temporal-index :datoms :mean :sd]"
  (println "Getting transaction times...")
  (let [header [:backend :schema-on-read :temporal-index :datoms :mean :sd]
        res (for [d-count [1 2 4 8 16 32 64 128 256 512 1024]
                  uri uris]
              (do
                (println " Number of datoms:" d-count " Uri:" uri)
                (try
                  (let [sor (:schema-on-read uri)
                        ti (:temporal-index uri)
                        conn (db/init-schema-and-connect (:lib uri) (:uri uri) (if sor [] schema) :schema-on-read sor :temporal-index ti)
                        t (measure-tx-times iterations (:lib uri) conn #(create-n-transactions d-count))]
                    (db/release (:lib uri) conn)
                    (println "  Mean Time:" (:mean t) "ms")
                    (println "  Standard deviation:" (:sd t) "ms")
                    [(:name uri) sor ti d-count (:mean t) (:sd t)])
                  (catch Exception e (e/short-report e)))))]
    [header res]))


(defn get-tx-times [file-suffix]
  (let [[header result] (run-combinations (concat c/hitchhiker-configs c/uris)
                                          100)
        data (ic/dataset header (remove nil? result))]
    (print "Save transaction times...")
    (ic/save data (str c/data-dir "/" (.format c/date-formatter (Date.)) "-" file-suffix ".dat"))
    (print " saved\n")))


(get-tx-times "tx-times")