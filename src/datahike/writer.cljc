(ns ^:no-doc datahike.writer
  (:require [superv.async :refer [S thread-try <?-]]
            [taoensso.timbre :as log]
            [datahike.core]
            [datahike.writing :as w]
            [datahike.tools :as dt :refer [throwable-promise]]
            [clojure.core.async :refer [chan close! promise-chan put! go go-loop <!]])
  (:import [clojure.lang ExceptionInfo]))

(defprotocol PWriter
  (-dispatch! [_ arg-map] "Returns a channel that resolves when the transaction finalizes.")
  (-shutdown [_] "Returns a channel that resolves when the writer has shut down.")
  (-streaming? [_] "Returns whether the transactor is streaming updates directly into the connection, so it does not need to fetch from store on read."))

(defrecord LocalWriter [queue thread streaming?]
  PWriter
  (-dispatch! [_ arg-map]
    (let [p (promise-chan)]
      (put! queue (assoc arg-map :callback p))
      p))
  (-shutdown [_]
    (close! queue)
    thread)
  (-streaming? [_] streaming?))

(defn create-thread
  "Creates new transaction thread"
  [connection queue write-fn-map]
  (thread-try
   S
   (go-loop []
     (if-let [{:keys [op args callback] :as invocation} (<! queue)]
       (do
         (let [op-fn (write-fn-map op)
               res   (try
                       (<?- (apply op-fn connection args))
                       ;; Only catch ExceptionInfo here (intentionally rejected transactions).
                       ;; Any other exceptions should crash the writer and signal the supervisor.
                       (catch Exception e
                         (log/errorf "Error during invocation" invocation e)
                         ;; take a guess that a NPE was triggered by an invalid connection
                         (if (= (type e) NullPointerException)
                           (ex-info "Null pointer encountered in invocation. Connection may have been invalidated, e.g. through db deletion, and needs to be released everywhere."
                                    {:type       :writer-error-during-invocation
                                     :invocation invocation
                                     :connection connection
                                     :error      e})
                           e)))]
           (when (some? callback)
             (put! callback res)))
         (recur))
       (log/debug "Writer thread gracefully closed")))))

;; public API

(declare transact load-entities)

(def default-write-fn-map {'transact      w/transact!
                           'load-entities w/load-entities})

(defmulti create-writer
  (fn [writer-config _]
    (:backend writer-config)))

(defmethod create-writer :self
  [{:keys [buffer-size write-fn-map]} connection]
  (let [queue (chan buffer-size)
        thread (create-thread connection queue
                              (merge default-write-fn-map
                                     write-fn-map))]
    (map->LocalWriter
     {:queue  queue
      :thread thread
      :streaming? true})))

(defn dispatch! [writer arg-map]
  (-dispatch! writer arg-map))

(defn shutdown [writer]
  (-shutdown writer))

(defn streaming? [writer]
  (-streaming? writer))

(defn backend-dispatch [& args]
  (get-in (first args) [:writer :backend] :self))

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
