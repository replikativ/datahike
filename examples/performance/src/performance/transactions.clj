(ns performance.transactions
  (:require [performance.measure :refer [measure-transaction-times]]
            [performance.db.api :as db]
            [performance.common :refer [short-error-report int-linspace]]
            [incanter.core :as ic]
            [incanter.io]
            [performance.config :as c])
  (:import (java.util UUID)))


(def schema [{:db/ident       :name
              :db/valueType   :db.type/string
              :db/cardinality :db.cardinality/one}])


(defn create-n-transactions [n]
  (->> (repeatedly #(str (UUID/randomUUID)))
       (take n)
       (mapv (fn [id] {:name id}))))


(defn run-combinations
  "Returns observation in following order:
   [:backend :schema-on-read :temporal-index :datoms :mean :sd]"
  [uris iterations]
  (println "Getting transaction times...")
  (let [header [:backend :schema-on-read :temporal-index :datoms :mean :sd]
        res (for [d-count (int-linspace 0 1000 17)
                  uri uris]
              (do
                (println " Number of datoms:" d-count " Uri:" uri)
                (try
                  (let [sor (:schema-on-read uri)
                        ti (:temporal-index uri)
                        conn (db/init-schema-and-connect (:lib uri) (:uri uri) (if sor [] schema) :schema-on-read sor :temporal-index ti)
                        t (measure-transaction-times iterations (:lib uri) conn #(create-n-transactions d-count))]
                    (db/release (:lib uri) conn)
                    (println "  Mean Time:" (:mean t) "ms")
                    (println "  Standard deviation:" (:sd t) "ms")
                    [(:name uri) sor ti d-count (:mean t) (:sd t)])
                  (catch Exception e (short-error-report e)))))]
    [header res]))


(defn get-tx-times [file-suffix]
  (let [[header result] (run-combinations (concat c/hitchhiker-configs c/uris) 100)
        data (ic/dataset header (remove nil? result))]
    (print "Save transaction times...")
    (ic/save data (c/filename file-suffix))
    (print " saved\n")))
