(ns ^:no-doc datahike.transactor
  (:require [superv.async :refer [<??- S thread-try]]
            [taoensso.timbre :as log]
            [clojure.core.async :refer [>!! chan close! promise-chan put!]])
  (:import [clojure.lang ExceptionInfo]))

(defprotocol PTransactor
  ; Send a transaction. Returns a channel that resolves when the transaction finalizes.
  (send-transaction! [_ tx-data tx-meta tx-fn])
  ; Returns a channel that resolves when the transactor has shut down.
  (shutdown [_]))

(defrecord LocalTransactor [rx-queue rx-thread]
  PTransactor
  (send-transaction! [_ tx-data tx-meta tx-fn]
    (let [p (promise-chan)]
      (>!! rx-queue {:tx-data tx-data :tx-meta tx-meta :callback p :tx-fn tx-fn})
      p))

  (shutdown [_]
    (close! rx-queue)
    rx-thread))

(defn create-rx-thread
  "Creates new transaction thread"
  [connection rx-queue update-and-flush-db]
  (thread-try
   S
   (let [resolve-fn (memoize resolve)]
     (loop []
       (if-let [{:keys [tx-data tx-meta callback tx-fn]} (<??- rx-queue)]
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
  (fn [transactor-config conn update-and-flush-db]
    (or (:backend transactor-config) :local)))

(defmethod create-transactor :local
  [{:keys [rx-buffer-size]} connection update-and-flush-db]
  (let [rx-queue (chan rx-buffer-size)
        rx-thread (create-rx-thread connection rx-queue update-and-flush-db)]
    (map->LocalTransactor
     {:rx-queue  rx-queue
      :rx-thread rx-thread})))
