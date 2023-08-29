(ns datahike.transit
  "Transit related translations."
  (:require [datahike.store :refer [store-identity]]
            [datahike.readers :as readers]
            [datahike.connector]
            [datahike.datom :as dd]
            [cognitect.transit :as transit])
  #?(:clj
    (:import [datahike.datom Datom]
             [datahike.impl.entity Entity]
             [datahike.db HistoricalDB AsOfDB SinceDB])))

(def read-handlers
  {"datahike/Connection"
   (transit/read-handler datahike.readers/connection-from-reader)
   "datahike/DB"
   (transit/read-handler datahike.readers/db-from-reader)
   "datahike/HistoricalDB"
   (transit/read-handler datahike.readers/history-from-reader)
   "datahike/SinceDB"
   (transit/read-handler datahike.readers/since-from-reader)
   "datahike/AsOfDB"
   (transit/read-handler datahike.readers/as-of-from-reader)
   "datahike/Entity"
   (transit/read-handler datahike.readers/entity-from-reader)
   "datahike/TxReport"
   (transit/read-handler datahike.db/map->TxReport)})

(defn config->store-id [config]
  [(store-identity (:store config))
   (:branch config)])

(defn db->map [db]
  (let [{:keys [config meta max-eid max-tx]} db]
    {:store-id  (config->store-id config)
     :commit-id (:datahike/commit-id meta)
     :max-eid   max-eid
     :max-tx    max-tx}))

(def write-handlers
  {datahike.connector.Connection
   (transit/write-handler "datahike/Connection"
                          #(config->store-id (:config @(:wrapped-atom %))))

   datahike.datom.Datom
   (transit/write-handler "datahike/Datom"
                          (fn [^Datom d]
                            [(.-e d) (.-a d) (.-v d) (dd/datom-tx d) (dd/datom-added d)]))

   datahike.db.TxReport
   (transit/write-handler "datahike/TxReport" #(into {} %))

   datahike.db.DB
   (transit/write-handler "datahike/DB" db->map)

   datahike.db.HistoricalDB
   (transit/write-handler "datahike/HistoricalDB"
                          (fn [{:keys [origin-db]}]
                            {:origin origin-db}))

   datahike.db.SinceDB
   (transit/write-handler "datahike/SinceDB"
                          (fn [{:keys [origin-db time-point]}]
                            {:origin     origin-db
                             :time-point time-point}))

   datahike.db.AsOfDB
   (transit/write-handler "datahike/AsOfDB"
                          (fn [{:keys [origin-db time-point]}]
                            {:origin     origin-db
                             :time-point time-point}))

   datahike.impl.entity.Entity
   (transit/write-handler "datahike/Entity"
                          (fn [^Entity e]
                            (assoc (into {} e)
                                   :db (.-db e)
                                   :eid (.-eid e))))})
