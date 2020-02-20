(ns performance.transactions
  (:require [performance.measure :refer [measure-tx-times]]
            [performance.uri :as uri]
            [datahike.api :as d]
            [datomic.api :as da]
            [incanter.stats :as is]
            [incanter.charts :as charts]
            [incanter.core :as ic])
  (:import (java.util Date)))

(def schema [{:db/ident       :name
              :db/valueType   :db.type/string
              :db/cardinality :db.cardinality/one}])

(defn create-n-transactions [n]
  (->> (repeatedly #(str (java.util.UUID/randomUUID)))
       (take n)
       (mapv (fn [id] {:name id}))))

(defn connect-datahike [uri & args]
  (do
    (d/delete-database uri)
    (apply d/create-database uri args)
    (d/connect uri)))

(defn connect-datomic [uri]
  (do
    (da/delete-database uri)
    (da/create-database uri)
    (da/connect uri)))


(defn run-combinations [iterations]
  "Returns observation in following order:
   [:datoms :backend :mean :sd :schema-on-read :temporal-index]"
  (-> (for [d-count [1 2 4 8 16 32 64 128 256 512 1024]
            uri uri/all]
        (if (not= "Datomic" (key uri))

          (for [sor [true false] ti [true false]]
            (let [conn (connect-datahike (val uri) :schema-on-read sor :temporal-index ti :initial-tx (if sor schema []))
                  ti (measure-tx-times iterations true conn #(create-n-transactions d-count))]
              (d/release conn)
              [d-count (key uri) (is/mean ti) (is/sd ti) sor ti]))

          (let [conn (connect-datomic (val uri))
                _ @(da/transact conn schema)
                ti (measure-tx-times iterations false conn #(create-n-transactions d-count))]
            (da/release conn)
            [[d-count (key uri) (is/mean ti) (is/sd ti) false false]])))
      concat
      vec))


(defn overall-means-datoms-plot [data]
  (ic/with-data data
                (doto
                  (charts/xy-plot
                    :datoms
                    :mean
                    :group-by :backend
                    :points true
                    :legend true
                    :title "transaction time vs. datoms per transaction of available datahike backends"
                    :y-label "transaction time (ms)")
                  (charts/set-axis :x (charts/log-axis :base 2, :label "Number of datoms per transaction")))))


(defn make-tx-plots [config file-suffix]
  (let [result (run-combinations 256)                       ;; test!
        header [:datoms :backend :mean :sd :schema-on-read :temporal-index]
        data (ic/dataset (mapv (fn [observation] (zipmap header observation)) result))
        ;; means-plot (overall-means-datoms-plot data)
        ]
    (ic/save data (str (:data-dir config) "/" (.format (:date-formatter config) (Date.)) "-" file-suffix ".dat"))
    ;;(ic/save means-plot (str (:data-dir config) "/" (.format (:date-formatter config) (Date.)) "-" file-suffix ".png") :width 1000 :height 750)
    ))
