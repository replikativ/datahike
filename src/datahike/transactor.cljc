(ns ^:no-doc datahike.transactor
  (:require [superv.async :refer [S thread-try]]
            [taoensso.timbre :as log]
            [datahike.core]
            [datahike.storing :refer [update-and-flush-db]]
            [clojure.core.async :refer [chan close! promise-chan put! go-loop <!]])
  (:import [clojure.lang ExceptionInfo]))

(defprotocol PTransactor
  ; Send a transaction. Returns a channel that resolves when the transaction finalizes.
  (dispatch! [_ tx-fn tx-data tx-meta])
  ; Returns a channel that resolves when the transactor has shut down.
  (shutdown [_])
  (streaming? [_]))

(defrecord LocalTransactor [rx-queue rx-thread streaming?]
  PTransactor
  (dispatch! [_ tx-fn tx-data tx-meta]
    (let [p (promise-chan)]
      (put! rx-queue {:tx-data tx-data :tx-meta tx-meta :callback p :tx-fn tx-fn})
      p))
  (shutdown [_]
    (close! rx-queue)
    rx-thread)
  (streaming? [_] streaming?))

(defn create-rx-thread
  "Creates new transaction thread"
  [connection rx-queue]
  (thread-try
   S
   (let [resolve-fn {'datahike.core/transact datahike.core/transact
                     'datahike.core/load-entities datahike.core/load-entities}]
     (go-loop []
       (if-let [{:keys [tx-data tx-meta callback tx-fn]} (<! rx-queue)]
         (do
           (let [update-fn (resolve-fn tx-fn)
                 tx-report (try (update-and-flush-db connection tx-data tx-meta update-fn)
                                 ; Only catch ExceptionInfo here (intentionally rejected transactions).
                                 ; Any other exceptions should crash the transactor and signal the supervisor.
                                (catch ExceptionInfo e e))]
             (when (some? callback)
               (put! callback tx-report)))
           (recur))
         (log/debug "Transactor rx thread gracefully closed"))))))

(defmulti create-transactor
  (fn [transactor-config _]
    (or (:backend transactor-config) :local)))

(defmethod create-transactor :local
  [{:keys [rx-buffer-size]} connection]
  (let [rx-queue (chan rx-buffer-size)
        rx-thread (create-rx-thread connection rx-queue)]
    (map->LocalTransactor
     {:rx-queue  rx-queue
      :rx-thread rx-thread
      :streaming? true})))
