(ns performance.transactions
  (:require [performance.measure :refer [measure-tx-times]]
            [performance.uri :as uri]
            [performance.db :as db]
            [incanter.core :as ic]
            [incanter.io]
            [performance.const :as c])
  (:import (java.util Date UUID)
           (java.text SimpleDateFormat)))


(def schema [{:db/ident       :name
              :db/valueType   :db.type/string
              :db/cardinality :db.cardinality/one}])


(defn create-n-transactions [n]
  (->> (repeatedly #(str (UUID/randomUUID)))
       (take n)
       (mapv (fn [id] {:name id}))))


(defn run-combinations [iterations]
  "Returns observation in following order:
   [:backend :schema-on-read :temporal-index :datoms :mean :sd]"
  (let [header [:backend :schema-on-read :temporal-index :datoms :mean :sd]
        res (for [d-count [1 2 4 8 16 32 64 128 256 512 1024]
                  uri uri/all
                  :let [_ (println "Datoms " d-count " " uri)
                        sor (:schema-on-read uri)
                        ti (:temporal-index uri)
                        conn (db/init-schema-and-connect (:lib uri) (:uri uri) (if sor [] schema) :schema-on-read sor :temporal-index ti)
                        t (measure-tx-times iterations (:lib uri) conn #(create-n-transactions d-count))]]
              (do (db/release (:lib uri) conn)
                  [(:name uri) sor ti d-count (:mean t) (:sd t)]))]
    [header res]))


(defn get-tx-times [file-suffix]
  (let [[header result] (run-combinations 256)
        data (ic/dataset header result)]
    (ic/save data (str c/data-dir "/" (.format c/date-formatter (Date.)) "-" file-suffix ".dat"))))


;;(get-tx-times "tx-times")