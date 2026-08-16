(ns ^:no-doc datahike.writer
  (:require [superv.async :refer [S thread-try <?- go-try]]
            [replikativ.logging :as log]
            [datahike.core]
            [datahike.writing :as w]
            [datahike.tx-preds :as txp]
            [datahike.gc :as gc]
            [datahike.tools :as dt :refer [throwable-promise get-time-ms]]
            [clojure.string :as str]
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
  "Creates new transaction thread.

   `streaming?` (default true) is datahike's single-writer assumption: this JVM
   owns the branch, so the head commit-id is kept in memory and never re-read.
   With `streaming?` false every transaction instead re-reads the branch head
   from storage before it is applied, so a database can be handed between
   processes that write to it one after another. See [[create-writer]]."
  [connection write-fn-map transaction-queue-size commit-queue-size commit-wait-time
   streaming?]
  (let [transaction-queue-buffer    (buffer transaction-queue-size)
        transaction-queue           (chan transaction-queue-buffer)
        commit-queue-buffer         (buffer commit-queue-size)
        commit-queue                (chan commit-queue-buffer)
        ;; Non-streaming only: the commit loop signals here after each batch
        ;; has landed in storage. The transaction loop waits for that signal
        ;; before it re-reads the head for the next transaction — otherwise a
        ;; second transaction could re-read a head that does not yet contain
        ;; the first one and silently drop it. It also holds batches to one
        ;; commit, which is what makes the head cid we hand the commit loop
        ;; (below) exactly the head that transaction was applied to.
        commit-done                 (chan)]
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
                      (let [;; NON-STREAMING: another process may have committed
                            ;; to this branch since our last transaction, so the
                            ;; db we hold is not necessarily the head. Re-read it
                            ;; (one storage read) and apply on top of that. Safe
                            ;; to read here because the previous transaction's
                            ;; commit has already landed — see commit-done below.
                            ;;
                            ;; A FAILED read is surfaced to THIS caller and not
                            ;; retried. It must not escape the loop either: that
                            ;; would kill the loop without closing
                            ;; transaction-queue, so every later -dispatch! would
                            ;; still enqueue successfully and hang forever. And
                            ;; retrying here would block every queued transaction
                            ;; unboundedly on a raise
                            ;; (:branch-head-does-not-exist-in-store) that cannot
                            ;; tell a storage hiccup from a deleted database —
                            ;; retry belongs in the konserve backend.
                            old (if streaming?
                                  old
                                  (try (w/reload-branch-head old)
                                       (catch #?(:clj Throwable :cljs js/Error) e
                                         (log/error :datahike/head-reload-failed
                                                    {:invocation invocation :error e})
                                         (put! callback e)
                                         ::reload-failed)))
                            reload-failed? (= ::reload-failed old)
                            ;; TODO remove this after import is ported to writer API
                            ;; Skipped when non-streaming: `old` was just read
                            ;; from the head, so its :max-tx is authoritative,
                            ;; while the connection's may lag another process.
                            old (if (and streaming?
                                         (not= (:max-tx old)
                                               (:max-tx @(:wrapped-atom connection))))
                                  (assoc old :max-tx (:max-tx @(:wrapped-atom connection)))
                                  old)
                            ;; The head cid `old` was applied to. Handed to the
                            ;; commit loop so commit! records the right parent
                            ;; without reading the head a SECOND time.
                            head-cid (when-not streaming?
                                       (get-in old [:meta :datahike/commit-id]))

                            op-fn (write-fn-map op)
                            res   (try
                                    ;; Nothing to apply when the head read
                                    ;; failed: `old` is only a sentinel and the
                                    ;; caller already holds that error — which is
                                    ;; also the more useful one, so the unknown-op
                                    ;; check below is skipped rather than
                                    ;; delivering a second error nobody sees.
                                    (if reload-failed?
                                      :error
                                      (do
                                        ;; The op has to be NAMED before it is
                                        ;; applied. `write-fn-map` is a plain map,
                                        ;; so an op it does not hold gave `nil` and
                                        ;; `(apply nil …)` threw a
                                        ;; NullPointerException — which the handler
                                        ;; below then rewrote as "connection may
                                        ;; have been invalidated, e.g. through db
                                        ;; deletion". A caller whose only fault was
                                        ;; naming an operation this writer does not
                                        ;; have was sent to look at their storage.
                                        ;;
                                        ;; That is the version-skew case: a newer
                                        ;; client against an older remote writer
                                        ;; sends an op the server has never heard
                                        ;; of. There is no version exchange on this
                                        ;; wire to catch it earlier, so the honest
                                        ;; thing is to say which op is missing and
                                        ;; which exist.
                                        (when-not op-fn
                                          (throw (ex-info (str "This writer has no operation `" op "`. It supports: "
                                                               (str/join ", " (sort (map str (keys write-fn-map))))
                                                               ". A remote writer older than the client is the usual cause.")
                                                          {:type :writer/unknown-op
                                                           :op op
                                                           :supported (set (keys write-fn-map))})))
                                        (apply op-fn old args)))
                            ;; Catch all Throwables to handle AssertionError and other Errors
                            ;; These should crash the writer, but we deliver to callback first to prevent hangs
                                    (catch #?(:clj Throwable :cljs js/Error) e
                                      (log/error :datahike/write-error {:invocation invocation :error e :args args})
                              ;; short circuit on errors
                                      #?(:cljs (put! callback e)
                                         :clj
                                         (put! callback
                              ;; An NPE from INSIDE an op is still most often a
                              ;; released connection, so the hint stays — but as
                              ;; a hint, not a rewrite. It used to replace the
                              ;; exception outright, on a guess its own comment
                              ;; admitted to ("take a guess"), which meant every
                              ;; unrelated NPE arrived wearing that explanation
                              ;; and the real stack trace only in `:error`.
                                               (if (= (type e) NullPointerException)
                                                 (ex-info (str "NullPointerException during `" op "`. If this connection's "
                                                               "database was deleted or released elsewhere, that is the "
                                                               "usual cause; otherwise see :error for the original.")
                                                          {:type       :writer-error-during-invocation
                                                           :op         op
                                                           :invocation invocation
                                                           :connection connection
                                                           :error      e}
                                                          e)
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
                        (cond reload-failed?
                              ;; Resume from the last committed db rather than
                              ;; the sentinel, and NEVER fall through to the
                              ;; commit path below — it would park on a
                              ;; commit-done that no commit will ever signal.
                              (recur @(:wrapped-atom connection))

                              (chan? res)
                              ;; async op, run in parallel in background, no sequential commit handling needed
                              (do
                                ;; `>!` REFUSES nil, so forwarding a closed `res`
                                ;; throws inside this bare `go` — which closes the
                                ;; go's own channel, silently, and the callback is
                                ;; never delivered. The caller's promise then never
                                ;; resolves: a HANG, not an error. `res` closes
                                ;; whenever the op failed in a way `go-try-` could
                                ;; not turn into a value (a JVM Error, a cljs throw
                                ;; of a non-js/Error), and `gc-storage` reaches
                                ;; here on every call.
                                (go (let [v (<! res)]
                                      (>! callback
                                          (if (nil? v)
                                            (ex-info (str "The " op " operation produced no result"
                                                          " — its channel closed without a value,"
                                                          " which means it failed in a way that"
                                                          " could not be reported.")
                                                     {:error :async/no-result
                                                      :type :writer-no-result
                                                      :op op})
                                            v))))
                                (recur old))

                              (not= res :error)
                              (do
                                (when (> (count commit-queue-buffer) (/ commit-queue-size 2))
                                  (log/warn :datahike/commit-queue-pressure "Commit queue buffer >50% full" {:count (count commit-queue-buffer) :size commit-queue-size})
                                  (<! (timeout 50)))
                                (put! commit-queue [res callback head-cid])
                                ;; Non-streaming: do not take the next
                                ;; transaction until this one is durable, so the
                                ;; head we re-read above already contains it.
                                (when-not streaming?
                                  (<! commit-done))
                                ;; Non-streaming: recur on the COMMITTED db, not
                                ;; the report db — its meta carries the cid the
                                ;; commit loop just wrote, which is what lets the
                                ;; next reload-branch-head recognise an unmoved
                                ;; head and skip rebuilding. Safe: `reset!
                                ;; connection commit-db` happens before the
                                ;; commit-done we just took.
                                (recur (if streaming?
                                         (:db-after res)
                                         @(:wrapped-atom connection))))
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
                       ;; the per-commit branch-head read (one S3 GET).
                       ;;
                       ;; Non-streaming writers do NOT thread it: their head can
                       ;; move under them between commits. The transaction loop
                       ;; re-read the head anyway and passes the cid it applied
                       ;; to along with the transaction, so the parent is still
                       ;; correct and still costs only that one read.
                       last-cid nil]
                  (when tx
                    (let [txs (into [tx] (take-while some?) (repeatedly #(poll! commit-queue)))]
              ;; empty channel of pending transactions
                      (log/trace :datahike/batch-commit {:batch-size (count txs)})
              ;; commit latest tx to disk
                      (let [last-cid (if streaming? last-cid (nth (peek txs) 2 nil))
                            db (:db-after (first (peek txs)))
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
                            ;; Release a non-streaming transaction loop that is
                            ;; parked on commit-done, or it never observes the
                            ;; closed transaction-queue and never shuts down.
                            (close! commit-done)
                            (doseq [[_ callback] txs]
                              (put! callback e))
                            (log/error :datahike/writer-shutdown {:error e})
                            ;; Re-throw Errors (AssertionError, OutOfMemoryError, etc.) to crash the writer
                            #?(:clj (when (instance? Error e)
                                      (throw e)))))
                        ;; Signalled AFTER the head flip (or after the failure
                        ;; path closed everything), so the transaction loop's
                        ;; next head read sees this commit.
                        (when-not streaming?
                          (put! commit-done true))
                        (<! (timeout commit-wait-time))
                        (recur (<?- commit-queue)
                               ;; Non-throwing read, for two reasons that meet
                               ;; here: `@connection` routes through `deref-conn`,
                               ;; which throws once the connection is released
                               ;; (`release` marks it released before shutting the
                               ;; writer down, so closing the queue unparks the
                               ;; `<?-` above and this argument would deref an
                               ;; already-released connection — #929); and on a
                               ;; NON-STREAMING connection it would additionally
                               ;; round-trip to storage. The wrapped atom holds
                               ;; the same value with neither hazard.
                               (when streaming?
                                 (get-in @(:wrapped-atom connection) [:meta :datahike/commit-id]))))))))))]))

(defn- with-tx-pred
  "Wrap a report-producing write-fn so a store-level tx-pred (if registered)
   runs on the fully-resolved report before it is enqueued for commit. The
   tx-pred throws an Exception (NOT an Error/AssertionError) to abort — the
   transaction loop's Exception path then rejects the tx, delivers the error to
   the caller, and never enqueues a commit (nothing persists, chain unchanged).
   Ungoverned stores pay a single map lookup. EXPERIMENTAL/internal seam."
  [write-fn]
  (fn [old & args]
    (txp/check-report (apply write-fn old args))))

;; public API to internal mapping
(def default-write-fn-map {'transact!     (with-tx-pred w/transact!)
                           'load-entities (with-tx-pred w/load-entities)
                           ;; import-internal; see writing/load-entities-migrating
                           'load-entities-migrating (with-tx-pred w/load-entities-migrating)
                           ;; async operations that run in background — NOT report
                           ;; producers, must not be wrapped (they return channels)
                           'gc-storage!   gc/gc-storage!
                           ;; secondary index backfill (async, runs in background)
                           #?@(:clj ['build-secondary-index! w/build-secondary-index!
                                     'install-secondary-index! w/install-secondary-index!])
                           ;; merge with multi-parent commit tracking
                           'merge! (with-tx-pred w/merge-writer!)
                           ;; bulk import: indexes built outside, substituted here.
                           ;; NOT wrapped — its :tx-data is empty by construction,
                           ;; so a tx-pred has nothing to judge (see w/publish-built-db!)
                           'publish-built-db! w/publish-built-db!})

(defmulti create-writer
  "Create the writer described by the connection's `:writer` config.

   The `:self` backend (the default, `{:backend :self}`) transacts in this JVM
   and takes these options:

   - `:streaming?` (default `true`) — keep the branch head in memory between
     commits. `false` re-reads the branch head from storage before every
     transaction and after every commit.

     COST: one branch-head GET per commit (~10-40 ms on S3, ~$0.0000004), plus
     the loss of commit batching: each transaction becomes its own commit.

     REQUIRED whenever more than one process may hold a writer for this
     database. The serverless case is the reason it exists: each AWS Lambda
     execution environment is a separate JVM that believes it is the only
     writer, and Lambda keeps several of them warm and routes to them
     alternately. With the default, each environment commits from its own
     stale head and silently overwrites the other's transactions.

     It does NOT detect the race, it avoids it by serialisation. Two processes
     writing CONCURRENTLY still lose updates — the loser's head write simply
     lands last. Preventing that needs head fencing (compare-and-set on the
     branch head, issue #878). So `:streaming? false` is correct only under an
     external guarantee that the processes never overlap (e.g. Lambda reserved
     concurrency 1), not by construction."
  (fn [writer-config _]
    (:backend writer-config)))

(def self-writer-keys
  "Every key the `:self` writer understands. Closed, and checked at
   create-writer: a typo like `:streaming` (no `?`) would otherwise select the
   unsafe default silently, and the whole point of the option is to be safe in
   a setting where the failure is silent data loss. A spec cannot do this —
   `s/keys` accepts unqualified keys it does not list."
  #{:backend :streaming? :transaction-queue-size :commit-queue-size
    :commit-wait-time :write-fn-map})

(defmethod create-writer :self
  [{:keys [transaction-queue-size commit-queue-size write-fn-map commit-wait-time
           streaming?]
    :or   {streaming? true}
    :as   writer-config}
   connection]
  (when-let [unknown (seq (remove self-writer-keys (keys writer-config)))]
    (log/raise "Unknown key(s) in the :self writer config."
               {:type    :unknown-self-writer-config-keys
                :unknown (vec unknown)
                :known   (vec (sort self-writer-keys))}))
  (let [transaction-queue-size (or transaction-queue-size DEFAULT_QUEUE_SIZE)
        commit-queue-size (or commit-queue-size DEFAULT_QUEUE_SIZE)
        commit-wait-time (or commit-wait-time DEFAULT_COMMIT_WAIT_TIME)
        [transaction-queue commit-queue thread]
        (create-thread connection
                       (merge default-write-fn-map
                              write-fn-map)
                       transaction-queue-size
                       commit-queue-size
                       commit-wait-time
                       streaming?)]
    (map->LocalWriter
     {:transaction-queue transaction-queue
      :transaction-queue-size transaction-queue-size
      :commit-queue commit-queue
      :commit-queue-size commit-queue-size
      :thread thread
      :streaming? streaming?})))

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

(defn- dispatch-load!
  "`args` is the argument vector as the write-fn will receive it AFTER `old` —
   so it must match that op's arity exactly. Passing a trailing `nil` for the
   plain op sent three arguments to a two-arity `writing/load-entities`; that
   was invisible while the extra arity existed to absorb it."
  [connection op args]
  (let [p (throwable-promise)
        writer (:writer @(:wrapped-atom connection))]
    (go
      (let [tx-report (<! (dispatch! writer {:op op :args args}))]
        (#?(:clj deliver :cljs put!) p tx-report)))
    p))

(defn load-entities
  [connection entities]
  (dispatch-load! connection 'load-entities [entities]))

(defn ^:no-doc load-entities-migrating
  "`load-entities` threading an import's id mapping. **Internal to
   `datahike.migrate`** — see `datahike.writing/load-entities-migrating` for why
   this is a separate function rather than an arity.

   It also dispatches under its OWN op symbol. The writer's op name is part of
   the writer protocol, not just a local detail: a remote or replicated writer
   reads it off the wire. Sharing `'load-entities` for both shapes would have
   left the separation cosmetic — one dispatch path, two argument counts, and
   nothing on the receiving end able to tell which contract it was being held
   to."
  [connection entities migration]
  (dispatch-load! connection 'load-entities-migrating [entities migration]))

(defn publish-built-db!
  "Publish a bulk-built database through the writer.

   The promise resolves only after the commit loop has committed and `reset!` the
   connection, which is what lets `migrate/run-index-build` hold the GC guard
   across the whole build-then-publish sequence: bulk-built nodes are written
   before anything references them, and the guard must not close until the root
   that references them has landed."
  [connection fields]
  (let [p (throwable-promise)
        writer (:writer @(:wrapped-atom connection))]
    (go
      (let [tx-report (<! (dispatch! writer {:op 'publish-built-db! :args [fields]}))]
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