(ns ^:no-doc datahike.writer
  (:require [superv.async :refer [S thread-try <?- go-try]]
            [taoensso.timbre :as log]
            [datahike.core]
            [datahike.writing :as w]
            [datahike.gc :as gc]
            [datahike.tools :as dt :refer [throwable-promise get-time-ms]]
            [clojure.core.async :refer [chan close! promise-chan put! go go-loop <! >! poll! buffer timeout]]
            #?(:cljs [cljs.core.async.impl.channels :refer [ManyToManyChannel]]))
  #?(:clj (:import [clojure.core.async.impl.channels ManyToManyChannel])))

(defn chan? [x]
  (instance? ManyToManyChannel x))

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

(def ^:const DEFAULT_QUEUE_SIZE 120000)

;; minimum wait time between commits in ms
;; this reduces write pressure on the storage
;; at the cost of higher latency
(def ^:const DEFAULT_COMMIT_WAIT_TIME 0) ;; in ms

(defn create-thread
  "Creates new transaction thread"
  [connection write-fn-map transaction-queue-size commit-queue-size commit-wait-time]
  (let [transaction-queue-buffer    (buffer transaction-queue-size)
        transaction-queue           (chan transaction-queue-buffer)
        commit-queue-buffer         (buffer commit-queue-size)
        commit-queue                (chan commit-queue-buffer)]
    [transaction-queue commit-queue
     (#?(:clj thread-try :cljs try)
      S
      (do
        ;; processing loop
        (go-try S
         ;; delay processing until the writer we are part of in connection is set
                (while (not (:writer @(:wrapped-atom connection)))
                  (<! (timeout 10)))
                (loop [old @(:wrapped-atom connection)]
                  (if-let [{:keys [op args callback] :as invocation} (<?- transaction-queue)]
                    (do
                      (when (> (count transaction-queue-buffer) (* 0.9 transaction-queue-size))
                        (log/warn "Transaction queue buffer more than 90% full, "
                                  (count transaction-queue-buffer) "of" transaction-queue-size  " filled."
                                  "Reduce transaction frequency."))
                      (let [;; TODO remove this after import is ported to writer API
                            old (if-not (= (:max-tx old) (:max-tx @(:wrapped-atom connection)))
                                  (assoc old :max-tx (:max-tx @(:wrapped-atom connection)))
                                  old)

                            op-fn (write-fn-map op)
                            res   (try
                                    (apply op-fn old args)
                            ;; Only catch ExceptionInfo here (intentionally rejected transactions).
                            ;; Any other exceptions should crash the writer and signal the supervisor.
                                    (catch #?(:clj Exception :cljs js/Error) e
                                      (log/error "Error during invocation" invocation e args)
                              ;; take a guess that a NPE was triggered by an invalid connection
                              ;; short circuit on errors
                                      #?(:cljs (put! callback e)
                                         :clj
                                         (put! callback
                                               (if (= (type e) NullPointerException)
                                                 (ex-info "Null pointer encountered in invocation. Connection may have been invalidated, e.g. through db deletion, and needs to be released everywhere."
                                                          {:type       :writer-error-during-invocation
                                                           :invocation invocation
                                                           :connection connection
                                                           :error      e})
                                                 e)))
                                      :error))]
                        (cond (chan? res)
                              ;; async op, run in parallel in background, no sequential commit handling needed
                              (do
                                (go (>! callback (<! res)))
                                (recur old))

                              (not= res :error)
                              (do
                                (when (> (count commit-queue-buffer) (/ commit-queue-size 2))
                                  (log/warn "Commit queue buffer more than 50% full, "
                                            (count commit-queue-buffer) "of" commit-queue-size  " filled."
                                            "Throttling transaction processing. Reduce transaction frequency and check your storage throughput.")
                                  (<! (timeout 50)))
                                (put! commit-queue [res callback])
                                (recur (:db-after res)))
                              :else
                              (recur old))))
                    (do
                      (close! commit-queue)
                      (log/debug "Writer thread gracefully closed")))))
        ;; commit loop
        (go-try S
                (loop [tx (<?- commit-queue)]
                  (when tx
                    (let [txs (into [tx] (take-while some?) (repeatedly #(poll! commit-queue)))]
              ;; empty channel of pending transactions
                      (log/trace "Batched transaction count: " (count txs))
              ;; commit latest tx to disk
                      (let [db (:db-after (first (peek txs)))]
                        (try
                          (let [start-ts (get-time-ms)
                                {{:keys [datahike/commit-id]} :meta
                                 :as commit-db} (<?- (w/commit! db nil false))
                                commit-time (- (get-time-ms) start-ts)]
                            (log/trace "Commit time (ms): " commit-time)
                            (reset! connection commit-db)
                    ;; notify all processes that transaction is complete
                            (doseq [[tx-report callback] txs]
                              (let [tx-report (-> tx-report
                                                  (assoc-in [:tx-meta :db/commitId] commit-id)
                                                  (assoc :db-after commit-db))]
                                (put! callback tx-report))))
                          (catch #?(:clj Exception :cljs js/Error) e
                            (doseq [[_ callback] txs]
                              (put! callback e))
                            (log/error "Writer thread shutting down because of commit error." e)
                            (close! commit-queue)
                            (close! transaction-queue)))
                        (<! (timeout commit-wait-time))
                        (recur (<?- commit-queue)))))))))]))

;; public API to internal mapping
(def default-write-fn-map {'transact!     w/transact!
                           'load-entities w/load-entities
                           ;; async operations that run in background
                           'gc-storage!   gc/gc-storage!})

(defmulti create-writer
  (fn [writer-config _]
    (:backend writer-config)))

(defmethod create-writer :self
  [{:keys [transaction-queue-size commit-queue-size write-fn-map commit-wait-time]} connection]
  (let [transaction-queue-size (or transaction-queue-size DEFAULT_QUEUE_SIZE)
        commit-queue-size (or commit-queue-size DEFAULT_QUEUE_SIZE)
        commit-wait-time (or commit-wait-time DEFAULT_COMMIT_WAIT_TIME)
        [transaction-queue commit-queue thread]
        (create-thread connection
                       (merge default-write-fn-map
                              write-fn-map)
                       transaction-queue-size
                       commit-queue-size
                       commit-wait-time)]
    (map->LocalWriter
     {:transaction-queue transaction-queue
      :transaction-queue-size transaction-queue-size
      :commit-queue commit-queue
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
    (#?(:clj deliver :cljs put!) p (apply w/create-database args))
    p))

(defmulti delete-database backend-dispatch)

(defmethod delete-database :self [& args]
  (let [p (throwable-promise)]
    (#?(:clj deliver :cljs put!) p (apply w/delete-database args))
    p))

(defn transact!
  [connection arg-map]
  (let [p (throwable-promise)
        writer (:writer @(:wrapped-atom connection))]
    (go
      (let [tx-report (<! (dispatch! writer
                                     {:op 'transact!
                                      :args [arg-map]}))]
        (when (map? tx-report) ;; not error
          (doseq [[_ callback] (some-> (:listeners (meta connection)) (deref))]
            (callback tx-report)))
        (#?(:clj deliver :cljs put!) p tx-report)))
    p))

(defn load-entities [connection entities]
  (let [p (throwable-promise)
        writer (:writer @(:wrapped-atom connection))]
    (go
      (let [tx-report (<! (dispatch! writer
                                     {:op 'load-entities
                                      :args [entities]}))]
        (#?(:clj deliver :cljs put!) p tx-report)))
    p))

(defn gc-storage! [conn & args]
  (let [p (throwable-promise)
        writer (:writer @(:wrapped-atom conn))]
    (go
      (let [result (<! (dispatch! writer
                                  {:op 'gc-storage!
                                   :args (vec args)}))]
        (#?(:clj deliver :cljs put!) p result)))
    p))