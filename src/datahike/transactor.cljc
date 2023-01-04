(ns ^:no-doc datahike.transactor
  (:require [superv.async :refer [S thread-try]]
            [taoensso.timbre :as log]
            [datahike.core]
            [datahike.storing :refer [update-and-flush-db]]
            [clojure.core.async :refer [chan close! promise-chan put! go-loop <!]])
  (:import [clojure.lang ExceptionInfo]))

(defprotocol PTransactor
  ; Send a transaction. Returns a channel that resolves when the transaction finalizes.
  (-dispatch! [_ arg-map])
  ; Returns a channel that resolves when the transactor has shut down.
  (-shutdown [_])
  (-streaming? [_]))

(defrecord LocalTransactor [rx-queue rx-thread streaming?]
  PTransactor
  (-dispatch! [_ arg-map]
    (let [p (promise-chan)]
      (put! rx-queue (assoc arg-map :callback p))
      p))
  (-shutdown [_]
    (close! rx-queue)
    rx-thread)
  (-streaming? [_] streaming?))

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

;; public API

(defmulti create-transactor
  (fn [transactor-config _]
    (:backend transactor-config)))

(defmethod create-transactor :local
  [{:keys [rx-buffer-size]} connection]
  (let [rx-queue (chan rx-buffer-size)
        rx-thread (create-rx-thread connection rx-queue)]
    (map->LocalTransactor
     {:rx-queue  rx-queue
      :rx-thread rx-thread
      :streaming? true})))

(defn dispatch! [transactor arg-map]
  (-dispatch! transactor arg-map))

(defn shutdown [transactor]
  (-shutdown transactor))

(defn streaming? [transactor]
  (-streaming? transactor))
