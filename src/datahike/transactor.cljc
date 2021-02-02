(ns datahike.transactor
  (:require [datahike.core :as d]
            [superv.async :refer [<?? <??- S thread-try]]
            [taoensso.timbre :as log]
            [clojure.core.async :refer [>!! chan close! promise-chan]]))

(defprotocol PTransactor
  ; Send a transaction. Returns a channel that resolves when the transaction finalizes.
  (send-transaction! [_ tx-data update-fn])
  ; Returns a channel that resolves when the transactor has shut down.
  (shutdown [_]))

(defrecord LocalTransactor
  [rx-queue rx-thread]
  PTransactor
  (send-transaction! [_ tx-data update-fn]
    (let [p (promise-chan)]
      (>!! rx-queue {:tx-data tx-data :callback p :update-fn update-fn})
      p))

  (shutdown [_]
    (close! rx-queue)
    rx-thread))

(defn- create-rx-thread
  [connection rx-queue update-and-flush-db]
  (thread-try
    S
    (loop []
      (if-let [{:keys [tx-data callback update-fn]} (<??- rx-queue)]
        (do
          (let [tx-report (try (update-and-flush-db connection tx-data update-fn)
                               ; Only catch ExceptionInfo here (intentionally rejected transactions).
                               ; Any other exceptions should crash the transactor and signal the supervisor.
                               (catch clojure.lang.ExceptionInfo e e))]
            (when (some? callback)
              (>!! callback tx-report)))
          (recur))
        (do
          (log/debug "Transactor rx thread gracefully closed"))))))

(defmulti create-transactor
  (fn [transactor-config conn update-and-flush-db]
    (or (:backend transactor-config) :local)))

(defmethod create-transactor :local
  [{:keys [rx-buffer-size]} connection update-and-flush-db]
  (let [rx-queue (chan rx-buffer-size)
        rx-thread (create-rx-thread connection rx-queue update-and-flush-db)]
    (map->LocalTransactor
      {:rx-queue  rx-queue
       :rx-thread rx-thread})))