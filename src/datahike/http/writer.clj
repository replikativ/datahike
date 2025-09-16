(ns ^:no-doc datahike.http.writer
  "Remote writer implementation for datahike.http.server through datahike.http.client."
  (:require [datahike.writer :refer [PWriter create-writer create-database delete-database]]
            [datahike.http.client :refer [request-json] :as client]
            [datahike.json :as json]
            [datahike.tools :as dt :refer [throwable-promise]]
            [taoensso.timbre :as log]
            [clojure.core.async :refer [promise-chan put!]]))

(defrecord DatahikeServerWriter [remote-peer conn]
  PWriter
  (-dispatch! [_ arg-map]
    (let [{:keys [op args]} arg-map
          p (promise-chan)
          config (:config @(:wrapped-atom conn))]
      (log/debug "Sending operation to datahike-server:" op)
      (log/trace "Arguments:" arg-map)
      (put! p
            (try
              (request-json :post
                            (str op "-writer")
                            remote-peer
                            (vec (concat [config] args))
                            json/mapper)
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
    (deliver p (try (->
                     (request-json :post
                                   "create-database-writer"
                                   writer
                                   (vec (concat [(-> config
                                                     (assoc :remote-peer writer)
                                                     (dissoc :writer))]
                                                (rest args))))
                     (dissoc :remote-peer))
                    (catch Exception e
                      e)))
    p))

(defmethod delete-database :datahike-server
  [& args]
  (let [p (throwable-promise)
        {:keys [writer] :as config} (first args)]
    ;; redirect call to remote-peer as writer config
    (deliver p (try
                 (-> (request-json :post
                                   "delete-database-writer"
                                   writer
                                   (vec (concat [(-> config
                                                     (assoc  :remote-peer writer)
                                                     (dissoc :writer))]
                                                (rest args))))
                     (dissoc :remote-peer))
                 (catch Exception e
                   e)))
    p))
