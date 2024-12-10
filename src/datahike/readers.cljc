(ns datahike.readers
  (:require [datahike.connections :refer [get-connection *connections*]]
            [datahike.writing :as dw]
            [datahike.datom :refer [datom] :as dd]
            #?(:cljs [datahike.db :refer [HistoricalDB AsOfDB SinceDB]])
            [datahike.impl.entity :as de]
            [datahike.core :refer [init-db] :as dc]
            [datahike.tools :refer [raise]]
            [konserve.core :as k])
  #?(:clj
     (:import [datahike.datom Datom]
              [datahike.db HistoricalDB AsOfDB SinceDB])))

(def tempid dc/tempid)

(def datom-from-reader dd/datom-from-reader)

(defn db-from-reader [{:keys [schema datoms store-id commit-id] :as raw-db}]
  (if (and store-id commit-id)
    #?(:cljs (throw (ex-info "Reader not supported." {:type   :reader-not-supported
                                                      :raw-db raw-db}))
       :clj
       (if-let [conn (get-connection store-id)]
         (let [store (:store @conn)]
           (when-let [raw-db (k/get store commit-id nil {:sync? true})]
             (dw/stored->db raw-db store)))
         (raise (ex-info "Could not find active connection. Did you connect already?"
                         {:type :no-connection-for-db
                          :raw-db raw-db}))))
    (init-db (map (fn [[e a v tx]] (datom e a v tx)) datoms) schema)))

(defn history-from-reader [{:keys [origin]}]
  (HistoricalDB. origin))

(defn since-from-reader [{:keys [origin time-point]}]
  (AsOfDB. origin time-point))

(defn as-of-from-reader [{:keys [origin time-point]}]
  (SinceDB. origin time-point))

(defn connection-from-reader [conn-id]
  (:conn (@*connections* conn-id)))

(defn entity-from-reader [{:keys [db eid]}]
  (de/entity db eid))

(def ^{:doc "Data readers for EDN readers. In CLJS they’re registered automatically. In CLJ, if `data_readers.clj` do not work, you can always do

             ```
             (clojure.edn/read-string {:readers data-readers} \"...\")
             ```"}
  edn-readers
  {'datahike/Datom        datahike.readers/datom-from-reader
   'datahike/DB           datahike.readers/db-from-reader
   'datahike/HistoricalDB datahike.readers/history-from-reader
   'datahike/SinceDB      datahike.readers/since-from-reader
   'datahike/AsOfDB       datahike.readers/as-of-from-reader
   'datahike/Connection   datahike.readers/connection-from-reader
   'db/id                 datahike.readers/tempid})

#?(:cljs
   (doseq [[tag cb] edn-readers] (cljs.reader/register-tag-parser! tag cb)))
