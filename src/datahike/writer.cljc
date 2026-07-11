(ns ^:no-doc datahike.writer
  (:require [superv.async :refer [S thread-try <?- go-try]]
            [replikativ.logging :as log]
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
      ;; put! on a CLOSED queue returns false and would leave p silent — the
      ;; caller's deref would hang forever. Deliver the failure instead.
      (when-not (put! transaction-queue (assoc arg-map :callback p))
        (put! p (ex-info "Writer is shut down (a previous fatal error closed it); release and reconnect."
                         {:type :writer-shut-down})))
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
                        (log/warn :datahike/tx-queue-pressure "Transaction queue buffer >90% full" {:count (count transaction-queue-buffer) :size transaction-queue-size}))
                      (let [;; TODO remove this after import is ported to writer API
                            old (if-not (= (:max-tx old) (:max-tx @(:wrapped-atom connection)))
                                  (assoc old :max-tx (:max-tx @(:wrapped-atom connection)))
                                  old)

                            op-fn (write-fn-map op)
                            res   (try
                                    (apply op-fn old args)
                            ;; Catch all Throwables to handle AssertionError and other Errors
                            ;; These should crash the writer, but we deliver to callback first to prevent hangs
                                    (catch #?(:clj Throwable :cljs js/Error) e
                                      (log/error :datahike/write-error {:invocation invocation :error e :args args})
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
                              ;; Re-throw Errors (AssertionError, OutOfMemoryError, etc.) to crash the writer
                              ;; Only Exceptions should be handled and allow the writer to continue.
                              ;; CLOSE the queues first: a dead loop with open queues would accept
                              ;; further transacts whose callbacks can never be delivered — every
                              ;; subsequent transact would hang silently instead of failing loudly.
                                      #?(:clj (when (instance? Error e)
                                                (close! transaction-queue)
                                                (close! commit-queue)
                                                (throw e)))
                                      :error))]
                        (cond (chan? res)
                              ;; async op, run in parallel in background, no sequential commit handling needed
                              (do
                                (go (>! callback (<! res)))
                                (recur old))

                              (not= res :error)
                              (do
                                (when (> (count commit-queue-buffer) (/ commit-queue-size 2))
                                  (log/warn :datahike/commit-queue-pressure "Commit queue buffer >50% full" {:count (count commit-queue-buffer) :size commit-queue-size})
                                  (<! (timeout 50)))
                                (put! commit-queue [res callback])
                                (recur (:db-after res)))
                              :else
                              (recur old))))
                    (do
                      (close! commit-queue)
                      (log/debug :datahike/writer-closed "Writer thread gracefully closed")))))
        ;; commit loop
        (go-try S
                (loop [tx (<?- commit-queue)
                       ;; last committed cid of OUR branch: nil on the first
                       ;; iteration (commit! falls back to the storage read),
                       ;; threaded through afterwards so ordinary commits skip
                       ;; the per-commit branch-head read (S3: 3 requests)
                       last-cid nil]
                  (when tx
                    (let [txs (into [tx] (take-while some?) (repeatedly #(poll! commit-queue)))]
              ;; empty channel of pending transactions
                      (log/trace :datahike/batch-commit {:batch-size (count txs)})
              ;; commit latest tx to disk
                      (let [db (:db-after (first (peek txs)))
                            ;; Check for merge parents (set by merge-writer!)
                            merge-parents (get-in db [:meta :datahike/merge-parents])
                            ;; Clear merge-parents from db meta before persisting
                            db (if merge-parents
                                 (update db :meta dissoc :datahike/merge-parents)
                                 db)]
                        (try
                          (let [start-ts (get-time-ms)
                                {{:keys [datahike/commit-id]} :meta
                                 :as commit-db} (<?- (w/commit! db merge-parents false last-cid))
                                commit-time (- (get-time-ms) start-ts)]
                            (log/trace :datahike/commit-time {:duration-ms commit-time})
                            (reset! connection commit-db)
                    ;; notify all processes that transaction is complete
                            (doseq [[tx-report callback] txs]
                              (let [tx-report (-> tx-report
                                                  (assoc-in [:tx-meta :db/commitId] commit-id)
                                                  (assoc :db-after commit-db))]
                                (>! callback tx-report))))
                          (catch #?(:clj Throwable :cljs js/Error) e
                            ;; Close the queues BEFORE delivering the failed
                            ;; callbacks. Delivering first unblocks the caller
                            ;; while the queues are still open, so a subsequent
                            ;; transact could race into the still-open queue and
                            ;; commit AFTER the fatal error (writer_error_test
                            ;; saw the "dead" writer accept a further write).
                            ;; Closing first makes that transact observe the
                            ;; closed queue and fail loudly (:writer-shut-down).
                            (close! commit-queue)
                            (close! transaction-queue)
                            (doseq [[_ callback] txs]
                              (put! callback e))
                            (log/error :datahike/writer-shutdown {:error e})
                            ;; Re-throw Errors (AssertionError, OutOfMemoryError, etc.) to crash the writer
                            #?(:clj (when (instance? Error e)
                                      (throw e)))))
                        (<! (timeout commit-wait-time))
                        (recur (<?- commit-queue)
                               (get-in @connection [:meta :datahike/commit-id])))))))))]))

;; public API to internal mapping
(def default-write-fn-map {'transact!     w/transact!
                           'load-entities w/load-entities
                           ;; async operations that run in background
                           'gc-storage!   gc/gc-storage!
                           ;; secondary index backfill (async, runs in background)
                           #?@(:clj ['build-secondary-index! w/build-secondary-index!
                                     'install-secondary-index! w/install-secondary-index!])
                           ;; merge with multi-parent commit tracking
                           'merge! w/merge-writer!})

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

;; Note: :kabel backend is implemented in datahike.kabel.writer
;; Require that namespace to register the defmethod

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
    (go
      (#?(:clj deliver :cljs put!) p (<! (apply w/create-database args))))
    p))

(defmulti delete-database backend-dispatch)

(defmethod delete-database :self [& args]
  (let [p (throwable-promise)]
    (go
      (let [res (<! (apply w/delete-database args))]
        #?(:clj (deliver p res) :cljs (if (nil? res) (close! p) (put! p res)))))
    p))

(defn- detect-new-building-indices
  "Detect secondary indices that *transitioned* into :building in this tx,
   i.e. they are :building in db-after but were not already :building in
   db-before. Returns a seq of idx-idents that need a one-time backfill.

   Comparing against db-before is essential: any transaction applied while
   an index is still building would otherwise re-dispatch a full backfill,
   and a second backfill that runs after the first one's
   install-secondary-index! has dissoc'd :db.secondary/building-since-tx
   loses the snapshot guard and re-delivers post-creation datoms that were
   already applied live — double-counting them in the index."
  [tx-report]
  (let [before (get-in tx-report [:db-before :schema])
        after  (get-in tx-report [:db-after :schema])]
    (when after
      (keep (fn [[ident entry]]
              (when (and (map? entry)
                         (:db.secondary/type entry)
                         (= :building (:db.secondary/status entry))
                         (not= :building (get-in before [ident :db.secondary/status])))
                ident))
            after))))

(defn transact!
  [connection arg-map]
  (let [p (throwable-promise)
        writer (:writer @(:wrapped-atom connection))]
    (go
      (let [tx-report (<! (dispatch! writer
                                     {:op 'transact!
                                      :args [arg-map]}))]
        (when (map? tx-report) ;; not error
          ;; Dispatch backfill for any newly created secondary indices
          #?(:clj
             (doseq [idx-ident (detect-new-building-indices tx-report)]
               (log/trace :datahike/dispatch-backfill {:idx-ident idx-ident})
               ;; build-secondary-index! is async (returns channel).
               ;; When it completes, dispatch install to swap in the result.
               (go
                 (let [build-result (<! (dispatch! writer {:op 'build-secondary-index!
                                                           :args [idx-ident]}))]
                   (when (map? build-result)
                     (dispatch! writer {:op 'install-secondary-index!
                                        :args [build-result]}))))))
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

(defn merge-db!
  "Merge parent branches/commits into the current branch through the writer.
   Parents is a set of branch keywords or commit UUIDs.
   tx-data contains the merged changes."
  [connection {:keys [parents tx-data tx-meta] :as arg-map}]
  (let [p (throwable-promise)
        writer (:writer @(:wrapped-atom connection))]
    (go
      (let [tx-report (<! (dispatch! writer
                                     {:op 'merge!
                                      :args [arg-map]}))]
        (when (map? tx-report)
          (doseq [[_ callback] (some-> (:listeners (meta connection)) (deref))]
            (callback tx-report)))
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