(ns datahike.remote
  "Literals that can function as lightweight remote pointers to connections and
  dbs."
  (:require [cognitect.transit :as transit]
            [datahike.datom :as dd])
  #?(:clj
     (:import [clojure.lang IDeref])))

;; Remote peer currently operated on. This is used to allow the tagged literal
;; readers to attach the remote again.
(def ^:dynamic *remote-peer* nil)

(defmulti remote-deref (fn [{:keys [remote-peer]}] (:backend remote-peer)))

(defrecord RemoteConnection [store-id remote-peer]
  #?@(:clj
      [IDeref
       (deref [conn] (remote-deref conn))]))

(defn remote-connection [store-id]
  (RemoteConnection. store-id *remote-peer*))

#?(:clj
   (defmethod print-method RemoteConnection
     [^RemoteConnection conn ^java.io.Writer w]
     (.write w "#datahike/RemoteConnection")
     (.write w (pr-str (:store-id conn)))))

(defrecord RemoteDB [store-id max-tx max-eid commit-id remote-peer])

(defn remote-db [m]
  (assoc (map->RemoteDB m) :remote-peer *remote-peer*))

#?(:clj
   (defmethod print-method RemoteDB
     [^RemoteDB db ^java.io.Writer w]
     (.write w "#datahike/RemoteDB")
     (.write w (pr-str  (into {} (dissoc db :remote-peer))))))

(defrecord RemoteHistoricalDB [origin remote-peer])

(defn remote-historical-db [m]
  (assoc (map->RemoteHistoricalDB m) :remote-peer *remote-peer*))

#?(:clj
   (defmethod print-method RemoteHistoricalDB
     [^RemoteDB db ^java.io.Writer w]
     (.write w "#datahike/RemoteHistoricalDB")
     (.write w (pr-str  (into {} (dissoc db :remote-peer))))))

(defrecord RemoteSinceDB [origin time-point remote-peer])

(defn remote-since-db [m]
  (assoc (map->RemoteSinceDB m) :remote-peer *remote-peer*))

#?(:clj
   (defmethod print-method RemoteSinceDB
     [^RemoteDB db ^java.io.Writer w]
     (.write w "#datahike/RemoteSinceDB")
     (.write w (pr-str  (into {} (dissoc db :remote-peer))))))

(defrecord RemoteAsOfDB [origin time-point remote-peer])

(defn remote-as-of-db [m]
  (assoc (map->RemoteAsOfDB m) :remote-peer *remote-peer*))

#?(:clj
   (defmethod print-method RemoteAsOfDB
     [^RemoteDB db ^java.io.Writer w]
     (.write w "#datahike/RemoteAsOfDB")
     (.write w (pr-str  (into {} (dissoc db :remote-peer))))))

(defrecord RemoteEntity [db eid remote-peer])

(defn remote-entity [m]
  (assoc (map->RemoteEntity m) :remote-peer *remote-peer*))

#?(:clj
   (defmethod print-method RemoteEntity
     [^RemoteEntity e ^java.io.Writer w]
     (.write w "#datahike/RemoteEntity")
     (.write w (pr-str (dissoc (into {} e) :remote-peer)))))

(defn edn-replace-remote-literals [s]
  (reduce (fn [^String s [^String from ^String to]]
            (.replace s from to))
          s
          [["#datahike/RemoteConnection" "#datahike/Connection"]
           ["#datahike/RemoteDB" "#datahike/DB"]
           ["#datahike/RemoteHistoricalDB" "#datahike/HistoricalDB"]
           ["#datahike/RemoteSinceDB" "#datahike/SinceDB"]
           ["#datahike/RemoteAsOfDB" "#datahike/AsOfDB"]]))

(def edn-readers {'datahike/Connection   remote-connection
                  'datahike/DB           remote-db
                  'datahike/HistoricalDB remote-historical-db
                  'datahike/AsOfDB       remote-as-of-db
                  'datahike/SinceDB      remote-since-db
                  'datahike/Datom        datahike.datom/datom-from-reader
                  'datahike.db.TxReport  datahike.db/map->TxReport})

(def transit-write-handlers {datahike.remote.RemoteConnection   (transit/write-handler "datahike/Connection" #(:store-id %))
                             datahike.remote.RemoteDB           (transit/write-handler "datahike/DB" #(dissoc (into {} %) :remote-peer))
                             datahike.remote.RemoteHistoricalDB (transit/write-handler "datahike/HistoricalDB" #(dissoc (into {} %) :remote-peer))
                             datahike.remote.RemoteSinceDB      (transit/write-handler "datahike/SinceDB" #(dissoc (into {} %) :remote-peer))
                             datahike.remote.RemoteAsOfDB       (transit/write-handler "datahike/AsOfDB" #(dissoc (into {} %) :remote-peer))
                             datahike.remote.RemoteEntity       (transit/write-handler "datahike/Entity" #(into {} %))})

(def transit-read-handlers {"datahike/Connection"   (transit/read-handler remote-connection)
                            "datahike/Datom"        (transit/read-handler #(apply dd/datom %))
                            "datahike/TxReport"     (transit/read-handler datahike.db/map->TxReport)
                            "datahike/DB"           (transit/read-handler remote-db)
                            "datahike/HistoricalDB" (transit/read-handler remote-historical-db)
                            "datahike/SinceDB"      (transit/read-handler remote-since-db)
                            "datahike/AsOfDB"       (transit/read-handler remote-as-of-db)
                            "datahike/Entity"       (transit/read-handler remote-entity)})

