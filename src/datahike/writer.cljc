(ns ^:no-doc datahike.writer
  (:require [superv.async :refer [S thread-try]]
            [taoensso.timbre :as log]
            [datahike.core]
            [datahike.writing :as w]
            [datahike.tools :as dt :refer [throwable-promise]]
            [clojure.core.async :refer [chan close! promise-chan put! go go-loop <!]])
  (:import [clojure.lang ExceptionInfo]))

(defprotocol PWriter
  ; Send a transaction. Returns a channel that resolves when the transaction finalizes.
  (-dispatch! [_ arg-map])
  ; Returns a channel that resolves when the writer has shut down.
  (-shutdown [_])
  (-streaming? [_]))

(defrecord LocalWriter [rx-queue rx-thread streaming?]
  PWriter
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
  [connection rx-queue write-fn-map]
  (thread-try
   S
   (go-loop []
     (if-let [{:keys [op args callback] :as invocation} (<! rx-queue)]
       (do
         (let [op-fn (write-fn-map op)
               res   (try
                       (apply op-fn connection args)
                       ;; Only catch ExceptionInfo here (intentionally rejected transactions).
                       ;; Any other exceptions should crash the writer and signal the supervisor.
                       (catch Exception e
                         (log/errorf "Error during invocation" invocation e)
                         e))]
           (when (some? callback)
             (put! callback res)))
         (recur))
       (log/debug "Writer rx thread gracefully closed")))))

;; public API

(declare transact load-entities)

(def default-write-fn-map {'transact      w/transact!
                           'load-entities w/load-entities})

(defmulti create-writer
  (fn [writer-config _]
    (:backend writer-config)))

(defmethod create-writer :self
  [{:keys [rx-buffer-size write-fn-map]} connection]
  (let [rx-queue (chan rx-buffer-size)
        rx-thread (create-rx-thread connection rx-queue
                                    (merge default-write-fn-map
                                           write-fn-map))]
    (map->LocalWriter
     {:rx-queue  rx-queue
      :rx-thread rx-thread
      :streaming? true})))

(defn dispatch! [writer arg-map]
  (-dispatch! writer arg-map))

(defn shutdown [writer]
  (-shutdown writer))

(defn streaming? [writer]
  (-streaming? writer))

(defn backend-dispatch [config & _args]
  (get-in config [:writer :backend] :self))

(defmulti create-database backend-dispatch)

(defmethod create-database :self [& args]
  (let [p (throwable-promise)]
    (deliver p (apply w/create-database args))
    p))

(defmulti delete-database backend-dispatch)

(defmethod delete-database :self [& args]
  (let [p (throwable-promise)]
    (deliver p (apply w/delete-database args))
    p))

(defn transact!
  [connection {:keys [tx-data tx-meta]}]
  (let [p (throwable-promise)
        writer (:writer @(:wrapped-atom connection))]
    (go
      (let [tx-report (<! (dispatch! writer
                                     {:op 'transact
                                      :args [{:tx-data tx-data
                                              :tx-meta tx-meta}]}))]
        (deliver p tx-report)))
    p))

(defn load-entities [connection entities]
  (let [p (throwable-promise)
        writer (:writer @(:wrapped-atom connection))]
    (go
      (let [tx-report (<! (dispatch! writer
                                     {:op 'load-entities
                                      :args [entities]}))]
        (deliver p tx-report)))
    p))
