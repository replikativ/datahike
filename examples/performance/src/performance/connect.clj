(ns performance.connect
  (:require [performance.measure :refer [measure-connect-times]]
            [performance.uri :as uri]
            [datahike.api :as d]
            [datomic.api :as da]
            [incanter.stats :as is]
            [incanter.charts :as charts]
            [incanter.core :as ic])
  (:import (java.util Date UUID)))

(def schema [{:db/ident       :name
              :db/valueType   :db.type/string
              :db/cardinality :db.cardinality/one}])

(defn create-n-transactions [n]
  (->> (repeatedly #(str (UUID/randomUUID)))
       (take n)
       (mapv (fn [id] {:name id}))))

(defn prepare-datahike [uri tx & args]
  (do
    (d/delete-database uri)
    (apply d/create-database uri args)
    (let [conn (d/connect uri)]
      (d/transact conn tx)
      (d/release conn))))

(defn prepare-datomic [uri schema tx]
  (do
    (da/delete-database uri)
    (da/create-database uri)
    (let [conn (da/connect uri)]
      @(da/transact conn schema)
      @(da/transact conn tx)
      (da/release conn))))


(defn run-combinations [iterations]
  "Returns observation in following order:
   [:datoms :backend :mean :sd :schema-on-read :temporal-index]"
  (-> (for [d-count [1 2 4 8 16 32 64 128 256 512 1024]
            uri uri/all]
        (let [tx (create-n-transactions d-count)]
          (if (not= "Datomic" (key uri))
            (for [sor [true false] ti [true false]]
              (let [_ (prepare-datahike (val uri) tx :schema-on-read sor :temporal-index ti :initial-tx (if sor schema []))
                    ti (measure-connect-times iterations true uri)]
                [d-count (key uri) (is/mean ti) (is/sd ti) sor ti]))
            (let [_ (prepare-datomic (val uri) schema tx)
                  ti (measure-connect-times iterations false uri)]
              [[d-count (key uri) (is/mean ti) (is/sd ti) false false]]))))
      concat
      vec))



(defn make-connect-plots [config file-suffix]
  (let [result (run-combinations 256)                       ;; test!
        header [:datoms :backend :mean :sd :schema-on-read :temporal-index]
        data (ic/dataset (mapv (fn [observation] (zipmap header observation)) result))]
    (ic/save data (str (:data-dir config) "/" (.format (:date-formatter config) (Date.)) "-" file-suffix ".dat"))))