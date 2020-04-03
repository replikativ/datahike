(ns performance.transaction
  (:require [performance.measure :refer [measure-transaction-times]]
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
  (let [header [:backend :schema-on-read :temporal-index :datoms :db-size :mean :sd]
        res (for [db-datom-count (int-linspace 0 1000 11)
                  tx-datom-count (assoc (int-linspace 0 1000 11) 0 1)
                  uri uris]
              (do
                (println " TRANSACT: Number of datoms in db:" db-datom-count " Number of datoms per transaction:" tx-datom-count " Uri:" uri)
                (try
                  (let [sor (:schema-on-read uri)
                        ti (:temporal-index uri)
                        t (measure-transaction-times (:lib uri) uri schema create-n-transactions tx-datom-count db-datom-count iterations)]
                    (println "  Mean Time:" (:mean t) "ms")
                    (println "  Standard deviation:" (:sd t) "ms")
                    [(:name uri) sor ti tx-datom-count db-datom-count (:mean t) (:sd t)])
                  (catch Exception e (short-error-report e)))))]
    [header res]))


(defn get-transaction-times [file-suffix iterations]
  (let [[header result] (run-combinations (concat c/hitchhiker-configs c/uris) iterations)
        data (ic/dataset header (remove nil? result))]
    (print "Save transaction times...")
    (ic/save data (c/filename file-suffix))
    (print " saved\n")))
