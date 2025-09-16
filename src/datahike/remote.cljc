(ns ^:no-doc datahike.remote
  "Literals that can function as lightweight remote pointers to connections and
  dbs."
  (:require [cognitect.transit :as transit]
            [jsonista.core :as j]
            [jsonista.tagged :as jt]
            [clojure.edn :as edn]
            [datahike.json :refer [json-base-handlers]]
            [datahike.datom :as dd])
  #?(:clj
     (:import [clojure.lang IDeref]
              [datahike.datom Datom]
              [com.fasterxml.jackson.core JsonGenerator])))

;; https://github.com/thi-ng/color/issues/10
;; fixes lein repl / Cloure 1.10.0 
(prefer-method print-method java.util.Map clojure.lang.IDeref)

;; fixes lein repl / Clojure 1.10.1
(prefer-method print-method clojure.lang.IPersistentMap clojure.lang.IDeref)

;; fixes CIDER / Clojure 1.9.0 / 1.10.0 / 1.10.1
(prefer-method clojure.pprint/simple-dispatch clojure.lang.IPersistentMap clojure.lang.IDeref)

;; Remote peer currently operated on. This is used to allow the tagged literal
;; readers to attach the remote again.
(def ^:dynamic *remote-peer* nil)

(defmulti remote-deref (fn [{:keys [remote-peer]}] (:backend remote-peer)))

(defprotocol PRemotePeer
  (-remote-peer [_] "Retrieve remote peer."))

(extend-protocol PRemotePeer
  Object
  (-remote-peer [_] nil))

(defn remote-peer [obj] (-remote-peer obj))

(defrecord RemoteConnection [store-id remote-peer]
  #?@(:clj
      [IDeref
       (deref [conn] (remote-deref conn))
       PRemotePeer
       (-remote-peer [_] remote-peer)]))

(defn remote-connection [store-id]
  (RemoteConnection. store-id *remote-peer*))

#?(:clj
   (defmethod print-method RemoteConnection
     [^RemoteConnection conn ^java.io.Writer w]
     (.write w "#datahike/RemoteConnection")
     (.write w (pr-str (:store-id conn)))))

(defrecord RemoteDB [store-id max-tx max-eid commit-id remote-peer]
  PRemotePeer
  (-remote-peer [_] remote-peer))

(defn remote-db [m]
  (assoc (map->RemoteDB m) :remote-peer *remote-peer*))

#?(:clj
   (defmethod print-method RemoteDB
     [^RemoteDB db ^java.io.Writer w]
     (.write w "#datahike/RemoteDB")
     (.write w (pr-str  (into {} (dissoc db :remote-peer))))))

(defrecord RemoteHistoricalDB [origin remote-peer]
  PRemotePeer
  (-remote-peer [_] remote-peer))

(defn remote-historical-db [m]
  (assoc (map->RemoteHistoricalDB m) :remote-peer *remote-peer*))

#?(:clj
   (defmethod print-method RemoteHistoricalDB
     [^RemoteDB db ^java.io.Writer w]
     (.write w "#datahike/RemoteHistoricalDB")
     (.write w (pr-str  (into {} (dissoc db :remote-peer))))))

(defrecord RemoteSinceDB [origin time-point remote-peer]
  PRemotePeer
  (-remote-peer [_] remote-peer))

(defn remote-since-db [m]
  (assoc (map->RemoteSinceDB m) :remote-peer *remote-peer*))

#?(:clj
   (defmethod print-method RemoteSinceDB
     [^RemoteDB db ^java.io.Writer w]
     (.write w "#datahike/RemoteSinceDB")
     (.write w (pr-str  (into {} (dissoc db :remote-peer))))))

(defrecord RemoteAsOfDB [origin time-point remote-peer]
  PRemotePeer
  (-remote-peer [_] remote-peer))

(defn remote-as-of-db [m]
  (assoc (map->RemoteAsOfDB m) :remote-peer *remote-peer*))

#?(:clj
   (defmethod print-method RemoteAsOfDB
     [^RemoteDB db ^java.io.Writer w]
     (.write w "#datahike/RemoteAsOfDB")
     (.write w (pr-str  (into {} (dissoc db :remote-peer))))))

(defrecord RemoteEntity [db eid remote-peer]
  PRemotePeer
  (-remote-peer [_] remote-peer))

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

(defn map-without-remote [r]
  (dissoc (into {} r) :remote-peer))

(defn datom-as-vec [^Datom d]
  [(.-e d) (.-a d) (.-v d) (dd/datom-tx d) (dd/datom-added d)])

(defn datom-from-vec [v] (apply dd/datom v))

(def transit-write-handlers {datahike.remote.RemoteConnection   (transit/write-handler "datahike/Connection" #(:store-id %))
                             datahike.datom.Datom               (transit/write-handler "datahike/Datom" datom-as-vec)
                             datahike.remote.RemoteDB           (transit/write-handler "datahike/DB" map-without-remote)
                             datahike.remote.RemoteHistoricalDB (transit/write-handler "datahike/HistoricalDB" map-without-remote)
                             datahike.remote.RemoteSinceDB      (transit/write-handler "datahike/SinceDB" map-without-remote)
                             datahike.remote.RemoteAsOfDB       (transit/write-handler "datahike/AsOfDB" map-without-remote)
                             datahike.remote.RemoteEntity       (transit/write-handler "datahike/Entity" #(into {} %))})

(def transit-read-handlers {"datahike/Connection"   (transit/read-handler remote-connection)
                            "datahike/Datom"        (transit/read-handler datom-from-vec)
                            "datahike/TxReport"     (transit/read-handler datahike.db/map->TxReport)
                            "datahike/DB"           (transit/read-handler remote-db)
                            "datahike/HistoricalDB" (transit/read-handler remote-historical-db)
                            "datahike/SinceDB"      (transit/read-handler remote-since-db)
                            "datahike/AsOfDB"       (transit/read-handler remote-as-of-db)
                            "datahike/Entity"       (transit/read-handler remote-entity)})

(declare json-mapper)

(defn write-to-generator [f]
  (fn [x ^JsonGenerator gen]
    (let [json-out (j/write-value-as-string (f x) json-mapper)]
      (.writeRawValue gen json-out))))

(def json-mapper-opts
  {:encode-key-fn true
   :decode-key-fn true
   :modules       [(jt/module
                    {:handlers
                     (merge
                      json-base-handlers
                      {datahike.remote.RemoteConnection
                       {:tag    "!datahike/Connection"
                        :encode (write-to-generator :store-id)
                        :decode remote-connection}

                       datahike.datom.Datom
                       {:tag    "!datahike/Datom"
                        :encode (write-to-generator datom-as-vec)
                        :decode datom-from-vec}

                       datahike.db.TxReport
                       {:tag    "!datahike/TxReport"
                        :encode (write-to-generator #(into {} %))
                        :decode datahike.db/map->TxReport}

                       datahike.remote.RemoteDB
                       {:tag    "!datahike/DB"
                        :encode (write-to-generator map-without-remote)
                        :decode remote-db}

                       datahike.remote.RemoteHistoricalDB
                       {:tag    "!datahike/HistoricalDB"
                        :encode (write-to-generator map-without-remote)
                        :decode remote-historical-db}

                       datahike.remote.RemoteSinceDB
                       {:tag    "!datahike/SinceDB"
                        :encode (write-to-generator map-without-remote)
                        :decode remote-since-db}

                       datahike.remote.RemoteAsOfDB
                       {:tag    "!datahike/AsOfDB"
                        :encode (write-to-generator map-without-remote)
                        :decode remote-as-of-db}

                       datahike.remote.RemoteEntity
                       {:tag    "!datahike/Entity"
                        :encode (write-to-generator #(into {} %))
                        :decode remote-entity}})})]})

(def json-mapper (j/object-mapper json-mapper-opts))
