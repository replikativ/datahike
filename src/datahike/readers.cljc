(ns datahike.readers
  (:require [datahike.connections :refer [get-connection]]
            [datahike.writing :as dsi]
            [datahike.datom :refer [datom]]
            [datahike.core :refer [init-db tempid]]
            [konserve.core :as k])
  (:import [datahike.db HistoricalDB AsOfDB SinceDB]))

(defn db-from-reader [{:keys [schema datoms store-id commit-id] :as raw-db}]
  (if (and store-id commit-id)
    #?(:cljs (throw (ex-info "Reader not supported." {:type   :reader-not-supported
                                                      :raw-db db}))
       :clj
       (let [store (:store @(get-connection store-id))]
         (when-let [raw-db (k/get store commit-id nil {:sync? true})]
           (dsi/stored->db raw-db store))))
    (init-db (map (fn [[e a v tx]] (datom e a v tx)) datoms) schema)))

(defn history-from-reader [{:keys [origin]}]
  (HistoricalDB. origin))

(defn since-from-reader [{:keys [origin time-point]}]
  (AsOfDB. origin time-point))

(defn as-of-from-reader [{:keys [origin time-point]}]
  (SinceDB. origin time-point))

;; Data Readers

(def ^{:doc "Data readers for EDN readers. In CLJS they’re registered automatically. In CLJ, if `data_readers.clj` do not work, you can always do

             ```
             (clojure.edn/read-string {:readers data-readers} \"...\")
             ```"}
  data-readers
  {'datahike/Datom        datahike.datom/datom-from-reader
   'datahike/DB           datahike.readers/db-from-reader
   'datahike/HistoricalDB datahike.readers/history-from-reader
   'datahike/SinceDB      datahike.readers/since-from-reader
   'datahike/AsOfDB       datahike.readers/as-of-from-reader
   'datahike/Connection   datahike.readers/read-connection
   'db/id                 tempid})

#?(:cljs
   (doseq [[tag cb] data-readers] (cljs.reader/register-tag-parser! tag cb)))

