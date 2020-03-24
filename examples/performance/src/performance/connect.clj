(ns performance.connect
  (:require [performance.measure :refer [measure-connect-times]]
            [performance.db :as db]
            [performance.conf :as c]
            [performance.error :as e]
            [incanter.io]
            [incanter.core :as ic])
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
  (println "Getting connection times...")
  (let [header [:backend :schema-on-read :temporal-index :datoms :mean :sd]
        res (for [d-count [1 2 4 8 16 32 64 128 256 512 1024 2048 4096] ;; for 8192 memory exception
                  uri uris]
              (do
                (println " Number of datoms:" d-count " Uri:" uri)
                (try
                  (let [tx (create-n-transactions d-count)
                        sor (:schema-on-read uri)
                        ti (:temporal-index uri)]
                    (db/prepare-db (:lib uri) (:uri uri) (if sor [] schema) tx :schema-on-read sor :temporal-index ti)
                    (let [t (measure-connect-times iterations (:lib uri) (:uri uri))]
                      (println "  Mean Time:" (:mean t) "ms")
                      (println "  Standard deviation:" (:sd t) "ms")
                      [(:name uri) sor ti d-count (:mean t) (:sd t)]))
                  (catch Exception e (e/short-report e)))))]
    [header res]))


(defn get-connect-times [file-suffix]
  (let [[header res] (run-combinations (remove #(= "Datomic Mem" (:name %)) c/uris) 100)
        data (ic/dataset header (remove nil? res))]
    (print "Save connection times...")
    (ic/save data (str c/data-dir "/" (.format c/date-formatter (Date.)) "-" file-suffix ".dat"))
    (print " saved\n")))


;;(get-connect-times "conn-times")