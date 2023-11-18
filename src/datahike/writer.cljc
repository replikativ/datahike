(ns ^:no-doc datahike.writer
  (:require [superv.async :refer [S thread-try <?-]]
            [taoensso.timbre :as log]
            [datahike.core]
            [datahike.writing :as w]
            [datahike.tools :as dt :refer [throwable-promise get-time-ms]]
            [clojure.core.async :refer [chan close! promise-chan put! go go-loop <! >! poll! buffer timeout]])
  (:import [clojure.lang ExceptionInfo]))

(defprotocol PWriter
  (-dispatch! [_ arg-map] "Returns a channel that resolves when the transaction finalizes.")
  (-shutdown [_] "Returns a channel that resolves when the writer has shut down.")
  (-streaming? [_] "Returns whether the transactor is streaming updates directly into the connection, so it does not need to fetch from store on read."))

(defrecord LocalWriter [thread streaming? transaction-queue-size commit-queue-size
                        transaction-queue commit-queue]
  PWriter
  (-dispatch! [_ arg-map]
    (let [p (promise-chan)]
      (put! transaction-queue (assoc arg-map :callback p))
      p))
  (-shutdown [_]
    (close! transaction-queue)
    thread)
  (-streaming? [_] streaming?))

(def ^:const default-queue-size 100000)

(defn create-thread
  "Creates new transaction thread"
  [connection write-fn-map transaction-queue-size commit-queue-size]
  (let [transaction-queue-buffer    (buffer transaction-queue-size)
        transaction-queue           (chan transaction-queue-buffer)
        commit-queue-buffer         (buffer commit-queue-size)
        commit-queue                (chan commit-queue-buffer)]
    [transaction-queue commit-queue
     (thread-try
      S
      (let [store (:store @(:wrapped-atom connection))]
        ;; processing loop
        (go-loop []
          (if-let [{:keys [op args callback] :as invocation} (<?- transaction-queue)]
            (do
              (when (> (count transaction-queue-buffer) (* 0.9 transaction-queue-size))
                (log/warn "Transaction queue buffer more than 90% full, "
                          (count transaction-queue-buffer) "of" transaction-queue-size  " filled."
                          "Reduce transaction frequency."))
              (let [op-fn (write-fn-map op)
                    res   (try
                            (apply op-fn connection args)
                            ;; Only catch ExceptionInfo here (intentionally rejected transactions).
                            ;; Any other exceptions should crash the writer and signal the supervisor.
                            (catch Exception e
                              (log/errorf "Error during invocation" invocation e args)
                              ;; take a guess that a NPE was triggered by an invalid connection
                              ;; short circuit on errors
                              (put! callback
                                    (if (= (type e) NullPointerException)
                                      (ex-info "Null pointer encountered in invocation. Connection may have been invalidated, e.g. through db deletion, and needs to be released everywhere."
                                               {:type       :writer-error-during-invocation
                                                :invocation invocation
                                                :connection connection
                                                :error      e})
                                      e))
                              :error))]
                (when-not (= res :error)
                  (when (> (count commit-queue-buffer) (/ commit-queue-size 2))
                    (log/warn "Commit queue buffer more than 50% full, "
                              (count commit-queue-buffer) "of" commit-queue-size  " filled."
                              "Throttling transaction processing. Reduce transaction frequency.")
                    (<! (timeout 50)))
                  (put! commit-queue [res callback])))
              (recur))
            (do
              (close! commit-queue)
              (log/debug "Writer thread gracefully closed"))))
        ;; commit loop
        (go-loop [tx (<?- commit-queue)]
          (when tx
            (let [txs (atom [tx])]
              ;; empty channel of pending transactions
              (loop [tx (poll! commit-queue)]
                (when tx
                  (swap! txs conj tx)
                  (recur (poll! commit-queue))))
              (log/trace "Batched transaction count: " (count @txs))
              ;; commit latest tx to disk
              (let [db (:db-after (first (last @txs)))]
                (try
                  (let [start-ts (get-time-ms)
                        {{:keys [datahike/commit-id]} :meta
                         :as                          commit-db} (<?- (w/commit! store (:config db) db nil false))
                        commit-time (- (get-time-ms) start-ts)]
                    (log/trace "Commit time (ms): " commit-time)
                    (reset! connection commit-db)
                    ;; notify all processes that transaction is complete
                    (doseq [[res callback] @txs]
                      (put! callback
                            (-> res
                                (assoc-in [:tx-meta :db/commitId] commit-id)
                                (assoc-in [:db-after :meta :datahike/commit-id] commit-id)))))
                  (catch Exception e
                    (doseq [[_ callback] @txs]
                      (put! callback e))
                    (log/error "Writer thread shutting down because of commit error " e)
                    (close! commit-queue)
                    (close! transaction-queue)))
                (recur (<! commit-queue))))))))]))

;; public API

(def default-write-fn-map {'transact!     w/transact!
                           'load-entities w/load-entities})

(defmulti create-writer
  (fn [writer-config _]
    (:backend writer-config)))

(defmethod create-writer :self
  [{:keys [transaction-queue-size commit-queue-size write-fn-map]} connection]
  (let [transaction-queue-size (or transaction-queue-size default-queue-size)
        commit-queue-size (or commit-queue-size default-queue-size)
        [transaction-queue commit-queue thread]
        (create-thread connection
                       (merge default-write-fn-map
                              write-fn-map)
                       transaction-queue-size
                       commit-queue-size)]
    (map->LocalWriter
     {:transaction-queue transaction-queue
      :transaction-queue-size transaction-queue-size
      :commit-queu commit-queue
      :commit-queue-size commit-queue-size
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
  [connection arg-map]
  (let [p (throwable-promise)
        writer (:writer @(:wrapped-atom connection))]
    (go
      (let [tx-report (<! (dispatch! writer
                                     {:op 'transact!
                                      :args [arg-map]}))]
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
