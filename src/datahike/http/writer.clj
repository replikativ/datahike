(ns datahike.http.writer
  "Remote writer implementation for datahike.http.server through datahike.http.client."
  (:require [datahike.writer :refer [PWriter create-writer create-database delete-database]]
            [datahike.transit :refer [read-handlers write-handlers]]
            [datahike.http.client :refer [request-transit] :as client]
            [datahike.tools :as dt :refer [throwable-promise]]
            [taoensso.timbre :as log]
            [clojure.core.async :refer [promise-chan put!]]))

(defrecord DatahikeServerWriter [remote-peer conn]
  PWriter
  (-dispatch! [_ arg-map]
    (let [{:keys [op args]} arg-map
          p (promise-chan)]
      (log/debug "Sending operation to datahike-server:" op)
      (log/trace "Arguments:" arg-map)
      (put! p
            (try
              (request-transit :post
                               (str op "-writer") remote-peer (vec (concat [conn] args))
                               read-handlers write-handlers)
              (catch Exception e
                e)))
      p))
  (-shutdown [_])
  (-streaming? [_] false))

(defmethod create-writer :datahike-server
  [config connection]
  (log/debug "Creating datahike-server writer for " connection config)
  (->DatahikeServerWriter config connection))

(defmethod create-database :datahike-server
  [& args]
  (let [p (throwable-promise)
        {:keys [writer] :as config} (first args)]
    ;; redirect call to remote-peer as writer config
    (deliver p (->
                (request-transit :post
                                 "create-database-writer"
                                 writer
                                 (vec (concat [(-> config
                                                   (assoc :remote-peer writer)
                                                   (dissoc :writer))] (rest args))))
                (dissoc :remote-peer)))
    p))

(defmethod delete-database :datahike-server
  [& args]
  (let [p (throwable-promise)
        {:keys [writer] :as config} (first args)]
    ;; redirect call to remote-peer as writer config
    (deliver p (-> (request-transit :post
                                    "delete-database-writer"
                                    writer
                                    (vec (concat [(assoc config :remote-peer writer)] (rest args))))
                   (dissoc :remote-peer)))
    p))
