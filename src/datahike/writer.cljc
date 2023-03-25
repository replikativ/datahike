(ns ^:no-doc datahike.writer
  (:require [superv.async :refer [S thread-try <?-]]
            [taoensso.timbre :as log]
            [datahike.core]
            [datahike.writing :as w]
            [datahike.tools :as dt :refer [throwable-promise get-time]]
            [clojure.core.async :refer [chan close! promise-chan put! go go-loop <! >! poll!]])
  (:import [clojure.lang ExceptionInfo]))

(defprotocol PWriter
  (-dispatch! [_ arg-map] "Returns a channel that resolves when the transaction finalizes.")
  (-shutdown [_] "Returns a channel that resolves when the writer has shut down.")
  (-streaming? [_] "Returns whether the transactor is streaming updates directly into the connection, so it does not need to fetch from store on read."))

(defrecord LocalWriter [queue thread streaming? queue-size]
  PWriter
  (-dispatch! [_ arg-map]
    (let [p (promise-chan)]
      (put! queue (assoc arg-map :callback p))
      p))
  (-shutdown [_]
    (close! queue)
    thread)
  (-streaming? [_] streaming?))

(def ^:const default-queue-size 100000)

(defn create-thread
  "Creates new transaction thread"
  [connection queue write-fn-map queue-size]
  (thread-try
   S
    (let [pending-txs-ch (chan queue-size)
          store (:store @(:wrapped-atom connection))]
      ;; processing loop
      (go-loop []
        (if-let [{:keys [op args callback] :as invocation} (<! queue)]
          (do
            (let [op-fn (write-fn-map op)
                  res   (try
                          (apply op-fn connection args)
                          ;; Only catch ExceptionInfo here (intentionally rejected transactions).
                          ;; Any other exceptions should crash the writer and signal the supervisor.
                          (catch Exception e
                            (log/errorf "Error during invocation" invocation e args)
                            ;; take a guess that a NPE was triggered by an invalid connection
                            (when (some? callback)
                              ;; short circuit on errors
                              (put! callback
                                    (if (= (type e) NullPointerException)
                                      (ex-info "Null pointer encountered in invocation. Connection may have been invalidated, e.g. through db deletion, and needs to be released everywhere."
                                               {:type       :writer-error-during-invocation
                                                :invocation invocation
                                                :connection connection
                                                :error      e})
                                      e))
                              :error)))]
              (when-not (= res :error)
                (>! pending-txs-ch [res callback])))
            (recur))
          (do
            (close! pending-txs-ch)
            (log/debug "Writer thread gracefully closed"))))
      ;; commit loop
      (go-loop [tx (<! pending-txs-ch)]
        (when tx
          (let [txs (atom [tx])]
            ;; empty channel of pending transactions
            (loop [tx (poll! pending-txs-ch)]
              (when tx
                (swap! txs conj tx)
                (recur (poll! pending-txs-ch))))
            (log/trace "Batched transaction count: " (count @txs))
            ;; commit latest tx to disk
            (let [db       (:db-after (first (last @txs)))]
              (try
                (let [start-ts  (.getTime (get-time))
                      {{:keys [datahike/commit-id]} :meta
                       :as _commit-db} (<?- (w/commit! store (:config db) db nil))]
                  (log/trace "Commit time (ms): " (- (.getTime (get-time)) start-ts))
                  ;; notify all processes that transaction is complete
                  (doseq [[res callback] @txs]
                    (put! callback (assoc-in res [:tx-meta :db/commitId] commit-id))))
                (catch Exception e
                  (doseq [[_ callback] @txs]
                    (put! callback e))
                  (log/error "Writer thread shutting down because of commit error " e)
                  (close! pending-txs-ch)))
              (recur (<! pending-txs-ch)))))))))

;; public API

(declare transact load-entities)

(def default-write-fn-map {'transact      w/transact!
                           'load-entities w/load-entities})

(defmulti create-writer
  (fn [writer-config _]
    (:backend writer-config)))


(defmethod create-writer :self
  [{:keys [queue-size write-fn-map]} connection]
  (let [queue-size (or queue-size default-queue-size)
        queue (chan queue-size)
        thread (create-thread connection queue
                              (merge default-write-fn-map
                                     write-fn-map)
                              queue-size)]
    (map->LocalWriter
     {:queue  queue
      :queue-size queue-size
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
