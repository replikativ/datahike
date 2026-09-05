(ns ^:no-doc datahike.writer
  (:require [superv.async :refer [S thread-try <?- go-try]]
            [replikativ.logging :as log]
            [datahike.core]
            [datahike.config :as dc]
            [datahike.metrics :as metrics]
            [datahike.store :as ds]
            [datahike.writing :as w]
            [datahike.tx-preds :as txp]
            [datahike.gc :as gc]
            [datahike.tools :as dt :refer [throwable-promise get-time-ms]]
            [konserve.core :as k]
            [clojure.string :as str]
            [clojure.core.async :refer [chan close! promise-chan put! go go-loop <! >! poll! buffer timeout]]
            #?(:cljs [cljs.core.async.impl.channels :refer [ManyToManyChannel]]))
  #?(:clj (:import [clojure.core.async.impl.channels ManyToManyChannel])))

(defn chan? [x]
  (instance? ManyToManyChannel x))

(defprotocol PWriter
  (-dispatch! [_ arg-map] "Returns a channel that resolves when the transaction finalizes.")
  (-shutdown [_] "Returns a channel that resolves when the writer has shut down.")
  (-streaming? [_] "Returns whether the transactor streams its completed writes into the connection."))

(defprotocol PConnectionRefresh
  (-refresh-on-deref? [_] "Returns whether dereferencing the connection must refresh its branch head from storage."))

(defrecord LocalWriter [thread writer-ownership transaction-queue-size commit-queue-size
                        transaction-queue commit-queue]
  PWriter
  (-dispatch! [_ arg-map]
    (let [p (promise-chan)]
      ;; put! on a CLOSED queue returns false and would leave p silent — the
      ;; caller's deref would hang forever. Deliver the failure instead.
      ;; The op runs on the writer thread; carry the caller's dynamic bindings
      ;; over so that what it bound (the query function resolver, for one)
      ;; holds for the transaction as well.
      (when-not (put! transaction-queue (assoc arg-map :callback p
                                               #?@(:clj [:bindings (get-thread-bindings)])))
        (put! p (ex-info "Writer is shut down (a previous fatal error closed it); release and reconnect."
                         {:type :writer-shut-down})))
      p))
  (-shutdown [_]
    (close! transaction-queue)
    thread)
  ;; A local writer always installs its own committed db-after in the connection.
  ;; Shared ownership still refreshes on deref because OTHER writers do not stream
  ;; their commits into this process.
  (-streaming? [_] true)
  PConnectionRefresh
  (-refresh-on-deref? [_] (= :shared writer-ownership)))

(def ^:const DEFAULT_QUEUE_SIZE 120000)

;; minimum wait time between commits in ms
;; this reduces write pressure on the storage
;; at the cost of higher latency
(def ^:const DEFAULT_COMMIT_WAIT_TIME 0) ;; in ms

;; How many transactions a SHARED writer chains onto one head read before
;; it stops and waits for them to commit. The head read synchronises us with
;; OTHER processes. If another writer commits during the batch, conditional head
;; publication rejects this batch and its transactions are re-applied against a
;; fresh head, so one read per BATCH is as correct as one per transaction and far
;; cheaper.
;;
;; The bound is not a tuning knob, it is a safety limit. The commit loop signals
;; commit-done once per committed transaction, and those signals are pending puts
;; on an unbuffered channel until we take them. core.async allows exactly 1024
;; pending puts and THROWS on the 1025th — inside the commit loop that means
;; closing every queue and killing the writer. In-flight transactions can never
;; exceed this bound, so 1024 is unreachable. (DEFAULT_QUEUE_SIZE is 120000, so
;; an unbounded chain would reach it easily.)
(def ^:const MAX_SHARED_WRITER_BATCH
  "Default for `:max-batch`. See [[create-writer]] — this is a contention/latency
   lever as much as a throughput one, because the batch is also the window a
   competing writer can slip into."
  64)

(def ^:const MAX_HEAD_CONFLICT_RETRIES
  "How many times a transaction is re-applied after the branch head moved under it
   before the conflict is handed to its caller.

   Bounded because retrying is not free and not guaranteed to converge: under
   sustained contention an unbounded loop livelocks, and each attempt re-applies
   the transaction AND re-flushes the secondary indices — for a Lucene-backed one
   that is a re-index. Retry is the safety net; keeping writers serialized is
   still the way to be fast. Default for `:head-conflict-retries`."
  3)

(def ^:const DEFAULT_HEAD_CONFLICT_BACKOFF_MS
  "Default for `:head-conflict-backoff-ms`: the base of an exponential, JITTERED
   wait before a rejected transaction is re-applied.

   Zero would be the worst possible policy. Every attempt would land inside the
   same contention window that just rejected it, so a writer that lost one race
   is likely to lose all of its retries and surface a failure that a few
   milliseconds would have avoided. The jitter matters as much as the delay: two
   writers backing off by the same amount collide again in lockstep."
  25)

(defn- notify-commit-listeners!
  "Notify a stable snapshot of the connection's durable-commit listeners.

   A listener observes an already-durable head and therefore cannot veto it.
   Isolate failures so one integration callback cannot kill the writer or hide
   the commit from the remaining listeners."
  [connection event]
  (doseq [[key callback]
          (some-> (:commit-listeners (meta connection)) deref)]
    (try
      (callback event)
      (catch #?(:clj Throwable :cljs :default) error
        (log/error :datahike/commit-listener-error
                   {:key key
                    ;; DB values and reports can retain indexes and make an
                    ;; otherwise small listener error enormous.
                    :event (dissoc event :db-before :db-after :tx-reports)
                    :error error}))))
  nil)

(def retryable-ops
  "Operations a head conflict may re-apply on the caller's behalf.

   `transact!` and `load-entities` are functions of `db -> report`: re-applying
   them to a newer head is the same operation against a newer world, which is
   what the caller asked for.

   `merge!` is deliberately absent. It carries EXPLICIT parents, so re-applying it
   against a head that moved would silently change what the merge means; that
   conflict belongs to the caller. Async ops never reach here — they write no
   head."
  #{'transact! 'load-entities})

#?(:clj
   (defn- fail-queued-invocations!
     "Deliver `e` to every invocation still sitting in a CLOSED `queue`.

      Closing a core.async channel does not discard its buffer, and nothing takes
      from the transaction queue except the loop that is about to die. Without
      this, those callers deref a promise-chan nobody ever delivers or closes — a
      permanent hang with no diagnostic, which is strictly worse than the error
      they should have got. (`-dispatch!` only covers callers who arrive AFTER
      the closure; these were already inside.)

      clj only: the Error paths that call it are themselves `#?(:clj …)`."
     [queue e]
     (loop []
       (when-let [{:keys [callback]} (poll! queue)]
         (put! callback e)
         (recur)))))

(defn create-thread
  "Creates new transaction thread.

   `shared?` selects the safe shared-ownership mode: the branch head is re-read
   before a batch is applied and conditionally published. With exclusive
   ownership this JVM keeps the head in memory. See [[create-writer]]."
  [connection write-fn-map transaction-queue-size commit-queue-size commit-wait-time
   shared? {:keys [max-batch retries backoff]
            :or   {max-batch MAX_SHARED_WRITER_BATCH
                   retries   MAX_HEAD_CONFLICT_RETRIES
                   backoff   DEFAULT_HEAD_CONFLICT_BACKOFF_MS}}]
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
        commit-done                 (chan)
        ;; Invocations handed BACK by the commit loop after a head conflict, for
        ;; the transaction loop to re-apply against a freshly read head. A
        ;; dedicated channel rather than re-queueing onto transaction-queue:
        ;; core.async is FIFO, so a re-queued transaction would land BEHIND work
        ;; that arrived after it, and under repeated conflicts could be starved by
        ;; a steady stream of new writes. Bounded by the batch size — only a
        ;; committing group can conflict, and a group is at most one batch.
        retry-queue-buffer          (buffer (max 1 max-batch))
        retry-queue                 (chan retry-queue-buffer)
        ;; Set before the fatal path closes anything. A retry is only worth
        ;; queueing while a loop remains to drain it, and there is no
        ;; non-destructive way to ask a core.async channel whether it is closed —
        ;; `poll!` would CONSUME an invocation to find out.
        writer-down?                (atom false)]
    [transaction-queue commit-queue
     (#?(:clj thread-try :cljs try)
      S
      (do
        ;; processing loop
        (go-try S
         ;; delay processing until the writer we are part of in connection is set
                (while (not (:writer @(:wrapped-atom connection)))
                  (<! (timeout 10)))
                ;; needs-reload? — has our previous batch committed, so the head
                ;;   must be re-read before applying anything else? INVARIANT:
                ;;   needs-reload? is true exactly when pending is 0, so a failed
                ;;   reload can never strand undrained commit-done signals.
                ;;   `close-batch` below is the ONLY place that sets it true with
                ;;   a drain, which is what keeps the two in step.
                ;; pending    — transactions enqueued for commit but not yet
                ;;   confirmed; the number of commit-done signals we still owe a
                ;;   take. Bounded by MAX_SHARED_WRITER_BATCH.
                (loop [old @(:wrapped-atom connection)
                       needs-reload? true
                       pending 0]
                  ;; THE WHOLE BATCHING POLICY, in one place and BEFORE we can
                  ;; park on an empty queue. A batch stays open only while there
                  ;; is queued work to chain onto it and it is under the bound;
                  ;; the moment either stops holding we wait for it to land and
                  ;; re-arm the head read.
                  ;;
                  ;; Deciding this per-arm instead is what makes it wrong: the
                  ;; arms that neither commit nor close (a rejected transaction,
                  ;; an async op) would leave a batch open across the park on
                  ;; `<?- transaction-queue`, and the next transaction — seconds
                  ;; or hours later — would skip the head read and clobber
                  ;; whatever another process committed in between. Bounding a
                  ;; batch by COUNT is not enough; it has to be bounded in TIME,
                  ;; and "about to wait for work" is that boundary.
                  ;;
                  ;; Only this loop takes from transaction-queue, so a count seen
                  ;; here cannot shrink under us — an open batch is closed early
                  ;; at worst, never left open wrongly.
                  (if (and shared?
                           (pos? pending)
                           (or (>= pending max-batch)
                               (zero? (count transaction-queue-buffer))))
                    (do
                      ;; One signal per COMMITTED TRANSACTION, so this balances
                      ;; however the commit loop grouped them into commits.
                      (loop [n pending]
                        (when (pos? n)
                          (<! commit-done)
                          (recur (dec n))))
                      ;; Resume from the COMMITTED db, not a report db: its meta
                      ;; carries the cid the commit loop just wrote, which is what
                      ;; lets the next reload recognise an unmoved head and skip
                      ;; rebuilding. Safe — `reset! connection commit-db` happens
                      ;; before the signals we just took.
                      (recur @(:wrapped-atom connection) true 0))
                    ;; RETRIES FIRST, and only with `pending` 0 — which by the
                    ;; loop's invariant means the head was just re-read. A retry
                    ;; taken mid-batch would be chained onto the very db that was
                    ;; rejected, which is the opposite of re-applying it. Nothing
                    ;; is starved: a non-empty transaction-queue keeps the batch
                    ;; open only until the bound, after which it closes, `pending`
                    ;; reaches 0, and the retry is taken.
                    (if-let [{:keys [op args callback bindings] :as invocation}
                             (or (when (and (zero? pending) (pos? (count retry-queue-buffer)))
                                   (let [inv (poll! retry-queue)
                                         n   (get inv :datahike/attempt 1)]
                                     ;; BACKOFF, exponential and JITTERED, before
                                     ;; re-applying. Zero would put every attempt
                                     ;; back inside the contention window that just
                                     ;; rejected it, so a writer that lost one race
                                     ;; loses all of its retries. The jitter matters
                                     ;; as much as the delay: without it two writers
                                     ;; back off by the same amount and collide again
                                     ;; in lockstep. Waiting HERE rather than in the
                                     ;; commit loop keeps the commit loop free to
                                     ;; drain other groups meanwhile.
                                     ;; The shift is CAPPED. `:head-conflict-retries`
                                     ;; is a user number, and `bit-shift-left` on a
                                     ;; long takes its count modulo 64: attempt 65
                                     ;; shifts by 0 and the backoff collapses back to
                                     ;; one unit, while the attempts just below it
                                     ;; ask for a wait measured in millennia. Neither
                                     ;; is a delay anyone chose. 2^16 units is already
                                     ;; far past any useful contention window.
                                     (when (pos? backoff)
                                       (<! (timeout (+ (* backoff (bit-shift-left 1 (min 16 (dec n))))
                                                       (rand-int (inc backoff))))))
                                     inv))
                                 (<?- transaction-queue))]
                      (do
                        (when (> (count transaction-queue-buffer) (* 0.9 transaction-queue-size))
                          (log/warn :datahike/tx-queue-pressure "Transaction queue buffer >90% full" {:count (count transaction-queue-buffer) :size transaction-queue-size}))
                        (let [;; SHARED: another process may have committed
                              ;; to this branch since our last transaction, so the
                              ;; db we hold is not necessarily the head. Re-read it
                              ;; (one storage read) and apply on top of that. Safe
                              ;; to read here because the previous transaction's
                              ;; commit has already landed — see commit-done below.
                              ;;
                              ;; A failed read is surfaced to THIS caller and not
                              ;; retried. An EXCEPTION must not escape the loop
                              ;; either: that would kill it without closing
                              ;; transaction-queue, so every later -dispatch! would
                              ;; still enqueue successfully and hang forever. And
                              ;; retrying here would block every queued transaction
                              ;; unboundedly on a raise
                              ;; (:branch-head-does-not-exist-in-store) that cannot
                              ;; tell a storage hiccup from a deleted database —
                              ;; retry belongs in the konserve backend.
                              ;;
                              ;; An ERROR is a different thing and is treated the
                              ;; same way the op-apply path below treats it: an
                              ;; AssertionError out of `reload-branch-head` means
                              ;; an invariant about the stored head does not hold,
                              ;; and a writer that keeps committing after that is
                              ;; writing on top of state it has already failed to
                              ;; validate. Close the queues FIRST so later callers
                              ;; fail loudly instead of enqueueing into a dead
                              ;; writer, then crash.
                              ;;
                              ;; Only the FIRST transaction of a batch re-reads
                              ;; the head; the rest chain onto it, exactly as the
                              ;; streaming path chains onto :db-after. See
                              ;; MAX_SHARED_WRITER_BATCH for why that is sound.
                              old (if (or (not shared?) (not needs-reload?))
                                    old
                                    (try (<?- (w/reload-branch-head old false))
                                         (catch #?(:clj Throwable :cljs js/Error) e
                                           (log/error :datahike/head-reload-failed
                                                      {:invocation (dissoc invocation :bindings) :error e})
                                           (put! callback e)
                                           #?(:clj (when (instance? Error e)
                                                     (reset! writer-down? true)
                                                     (close! transaction-queue)
                                                     (close! commit-queue)
                                                     (close! commit-done)
                                                     (close! retry-queue)
                                                     (fail-queued-invocations! transaction-queue e)
                                                     (fail-queued-invocations! retry-queue e)
                                                     (throw e)))
                                           ::reload-failed)))
                              reload-failed? (= ::reload-failed old)
                              ;; TODO remove this after import is ported to writer API
                              ;; Skipped when shared: `old` was just read
                              ;; from the head, so its :max-tx is authoritative,
                              ;; while the connection's may lag another process.
                              old (if (and (not shared?)
                                           (not= (:max-tx old)
                                                 (:max-tx @(:wrapped-atom connection))))
                                    (assoc old :max-tx (:max-tx @(:wrapped-atom connection)))
                                    old)
                              ;; The head cid `old` was applied to. Handed to the
                              ;; commit loop so commit! records the right parent
                              ;; without reading the head a SECOND time.
                              ;; It doubles as the BATCH BOUNDARY MARKER: set on
                              ;; the first transaction of a batch, nil on every
                              ;; chained one. nil tells the commit loop "your own
                              ;; previous commit is my parent" — which it is, since
                              ;; a chained transaction is only enqueued after the
                              ;; one before it, and the tx loop reads no head in
                              ;; between. Recording the batch's head cid on all of
                              ;; them instead would make each commit in the batch
                              ;; claim the SAME parent, orphaning every commit but
                              ;; the last.
                              head-cid (when (and shared? needs-reload?)
                                         (get-in old [:meta :datahike/commit-id]))
                              ;; The konserve revision the head was read at, for the
                              ;; commit's fence. Stamped on the SAME transaction as
                              ;; head-cid and for the same reason: it belongs to the
                              ;; head this batch was applied to, and a chained
                              ;; transaction shares it.
                              head-rev (when (and shared? needs-reload?)
                                         (get old ::w/head-revision))

                              op-fn (write-fn-map op)
                              res   (try
                                      ;; Nothing to apply when the head read
                                      ;; failed: `old` is only a sentinel and the
                                      ;; caller already holds that error — which is
                                      ;; also the more useful one, so the
                                      ;; unknown-op check below is skipped rather
                                      ;; than delivering a second error nobody sees.
                                      (if reload-failed?
                                        :error
                                        (do
                                          ;; The op has to be NAMED before it is
                                          ;; applied. `write-fn-map` is a plain map,
                                          ;; so an op it does not hold gave `nil`
                                          ;; and `(apply nil …)` threw a
                                          ;; NullPointerException — which the
                                          ;; handler below then rewrote as
                                          ;; "connection may have been invalidated,
                                          ;; e.g. through db deletion". A caller
                                          ;; whose only fault was naming an
                                          ;; operation this writer does not have was
                                          ;; sent to look at their storage.
                                          ;;
                                          ;; That is the version-skew case: a newer
                                          ;; client against an older remote writer
                                          ;; sends an op the server has never heard
                                          ;; of. There is no version exchange on
                                          ;; this wire to catch it earlier, so the
                                          ;; honest thing is to say which op is
                                          ;; missing and which exist.
                                          (when-not op-fn
                                            (throw (ex-info (str "This writer has no operation `" op "`. It supports: "
                                                                 (str/join ", " (sort (map str (keys write-fn-map))))
                                                                 ". A remote writer older than the client is the usual cause.")
                                                            {:type :writer/unknown-op
                                                             :op op
                                                             :supported (set (keys write-fn-map))})))
                                          #?(:clj (if bindings
                                                    (with-bindings* bindings apply op-fn old args)
                                                    (apply op-fn old args))
                                             :cljs (apply op-fn old args))))
                              ;; Catch all Throwables to handle AssertionError and other Errors
                              ;; These should crash the writer, but we deliver to callback first to prevent hangs
                                      (catch #?(:clj Throwable :cljs js/Error) e
                                        (log/error :datahike/write-error {:invocation (dissoc invocation :bindings) :error e :args args})
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
                                                             :invocation (dissoc invocation :bindings)
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
                                                  (reset! writer-down? true)
                                                  (close! transaction-queue)
                                                  (close! commit-queue)
                                                  (close! commit-done)
                                                  (close! retry-queue)
                                                  (fail-queued-invocations! transaction-queue e)
                                                  (fail-queued-invocations! retry-queue e)
                                                  (throw e)))
                                        :error))]
                          (cond reload-failed?
                                ;; Resume from the last committed db rather than
                                ;; the sentinel, and NEVER fall through to the
                                ;; commit path below — it would park on a
                                ;; commit-done that no commit will ever signal.
                                ;; pending is 0 here by the loop invariant: we only
                                ;; reload when the previous batch has drained.
                                (recur @(:wrapped-atom connection) true 0)

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
                                  ;; Async op: nothing committed, so the whole
                                  ;; batch state carries over untouched. It is safe
                                  ;; for this arm to be ignorant of batching — the
                                  ;; check at the top of the loop closes the batch
                                  ;; if this turns out to be the last work there is.
                                  (recur old needs-reload? pending))

                                (not= res :error)
                                (do
                                  (when (> (count commit-queue-buffer) (/ commit-queue-size 2))
                                    (log/warn :datahike/commit-queue-pressure "Commit queue buffer >50% full" {:count (count commit-queue-buffer) :size commit-queue-size})
                                    (<! (timeout 50)))
                                  (cond
                                    ;; Same hazard as -dispatch!, one queue further
                                    ;; in: a fatal commit error closes commit-queue
                                    ;; while transaction-queue still holds buffered
                                    ;; invocations, and this loop drains and applies
                                    ;; them before it observes the closure. put!
                                    ;; then returns false, and without this their
                                    ;; callers deref a promise nobody delivers.
                                    ;; Nothing was enqueued, so nothing will be
                                    ;; signalled: leave `pending` alone rather than
                                    ;; owe a take that nothing owes back.
                                    (not (put! commit-queue
                                               [res callback head-cid head-rev
                                                ;; The invocation rides along ONLY
                                                ;; when it could actually be
                                                ;; replayed. It holds the whole
                                                ;; :tx-data, so carrying it for ops
                                                ;; that are never retried — or for a
                                                ;; writer configured not to retry at
                                                ;; all — keeps every queued
                                                ;; transaction's data alive for the
                                                ;; depth of the commit queue, which
                                                ;; defaults to a large number.
                                                (when (and (pos? retries)
                                                           (contains? retryable-ops op))
                                                  invocation)]))
                                    (do (put! callback
                                              (ex-info "Writer is shut down (a previous fatal error closed it); release and reconnect."
                                                       {:type :writer-shut-down}))
                                        (recur old needs-reload? pending))

                                    (not shared?)
                                    (recur (:db-after res) false 0)

                                    ;; Chain onto this transaction's db-after
                                    ;; without re-reading the head, so a batch
                                    ;; becomes ONE commit and ONE head read. The
                                    ;; commit loop commits the LAST db of a batch,
                                    ;; which contains all of them precisely because
                                    ;; they are chained. Whether the batch may stay
                                    ;; open is not decided here — see the top of
                                    ;; the loop.
                                    :else
                                    (recur (:db-after res) false (inc pending))))

                                :else
                                ;; Failed op: same as above — no commit, so the
                                ;; batch state is unchanged, and the top of the loop
                                ;; closes the batch if nothing else is queued.
                                (recur old needs-reload? pending))))
                      ;; Graceful shutdown. Any batch still open was closed by
                      ;; the check at the top of the loop before we could park
                      ;; here, so there is nothing left owed.
                      (do
                        (close! commit-queue)
                        (log/debug :datahike/writer-closed "Writer thread gracefully closed"))))))
        ;; commit loop
        (go-try S
                (loop [tx (<?- commit-queue)
                       ;; last committed cid of OUR branch: nil on the first
                       ;; iteration (commit! falls back to the storage read),
                       ;; threaded through afterwards so ordinary commits skip
                       ;; the per-commit branch-head read (one S3 GET).
                       ;;
                       ;; Non-streaming writers thread it only WITHIN a batch:
                       ;; their head can move under them between batches, so the
                       ;; transaction loop re-reads it and stamps the cid it
                       ;; applied to onto the batch's first transaction. That
                       ;; stamp overrides whatever we threaded; the threaded
                       ;; value carries the rest of the batch. Either way the
                       ;; parent is correct and costs no extra read.
                       last-cid nil
                       ;; The head revision OUR last commit created. Threaded for
                       ;; the same reason as last-cid, and needed for the same
                       ;; case: a batch can produce several commit groups, and
                       ;; after the first lands the head has moved, so the next
                       ;; must fence against what we just wrote rather than the
                       ;; stamp the batch opened with.
                       last-rev nil]
                  (when tx
                    (let [txs (into [tx] (take-while some?) (repeatedly #(poll! commit-queue)))]
              ;; empty channel of pending transactions
                      (log/trace :datahike/batch-commit {:batch-size (count txs)})
              ;; commit latest tx to disk
                      (let [;; FIRST, not peek: only a batch's opening
                            ;; transaction carries a cid, and it is the parent of
                            ;; the commit we are about to make. The chained ones
                            ;; carry nil and mean "you committed my parent
                            ;; yourself" — which is `last-cid`. A drained group
                            ;; can never put a stamped transaction after a nil
                            ;; one: the transaction loop does not enqueue a new
                            ;; batch until the previous one is confirmed durable.
                            last-cid (if-not shared?
                                       last-cid
                                       (or (nth (first txs) 2 nil) last-cid))
                            ;; Same source as last-cid: the batch's opening
                            ;; transaction carries the revision it was applied to.
                            ;; A chained one carries nil, which is correct — the
                            ;; commit that precedes it in this batch moved the head,
                            ;; so its own revision is stale by construction and the
                            ;; commit loop re-reads (see below).
                            head-rev (when shared?
                                       (or (nth (first txs) 3 nil) last-rev))
                            db (:db-after (first (peek txs)))
                            ;; Check for merge parents (set by merge-writer!)
                            merge-parents (get-in db [:meta :datahike/merge-parents])
                            ;; Clear merge-parents from db meta before persisting
                            db (if merge-parents
                                 (update db :meta dissoc :datahike/merge-parents)
                                 db)
                            build-cleanup-complete? (atom false)]
                        (try
                          (let [start-ts (get-time-ms)
                                {{:keys [datahike/commit-id]} :meta
                                 :as commit-db} (<?- (w/commit! db merge-parents false last-cid head-rev))
                                commit-time (- (get-time-ms) start-ts)
                                ;; Finalize once, then hand these SAME report
                                ;; values to both the durable-batch listener and
                                ;; each transaction's caller. This preserves
                                ;; transaction boundaries while making every
                                ;; :db-after the head that actually landed.
                                finalized-txs
                                (mapv (fn [[tx-report callback]]
                                        [(-> tx-report
                                             (assoc-in [:tx-meta :db/commitId] commit-id)
                                             (assoc :db-after commit-db))
                                         callback])
                                      txs)
                                tx-reports (mapv first finalized-txs)]
                            (log/trace :datahike/commit-time {:duration-ms commit-time})
                            (metrics/commit! (:config db)
                                             commit-time
                                             (count txs)
                                             (reduce + 0 (map (comp count :tx-data first) txs)))
                            ;; The head is durable now, so the background build's
                            ;; pin can be released. Do this BEFORE publishing
                            ;; `commit-db` through the connection or callback:
                            ;; observing :ready must mean its GC lifecycle is
                            ;; finished, not merely that cleanup is about to run
                            ;; in this loop's `finally`.
                            #?(:clj
                               (doseq [[tx-report _] txs
                                       :let [build-guard
                                             (:secondary-index-build-guard
                                              tx-report)]
                                       :when build-guard]
                                 (w/finish-secondary-index-build! build-guard)))
                            (reset! build-cleanup-complete? true)
                            (reset! connection commit-db)
                            ;; This is the one exact durable-commit boundary. A
                            ;; drained group may contain many transaction reports,
                            ;; but commit! wrote one immutable commit and flipped
                            ;; one branch head. Transaction listeners cannot infer
                            ;; that grouping because every report below receives
                            ;; the same final db-after and commit id.
                            (notify-commit-listeners!
                             connection
                             {:type :datahike/commit
                              :store-id (ds/canonical-store-id
                                         (:store commit-db)
                                         (get-in commit-db [:config :store]))
                              :branch (get-in commit-db [:config :branch])
                              :commit-id commit-id
                              :parent-commit-ids
                              (or (get-in commit-db [:meta :datahike/parents]) #{})
                              :max-tx (:max-tx commit-db)
                              :tx-count (count txs)
                              ;; Rich process-local view. Attaching these is
                              ;; cheap: the writer already retains them until
                              ;; callers are notified. Consumers that keep the
                              ;; event also keep the immutable DB values alive.
                              :db-before (:db-before (first tx-reports))
                              :db-after commit-db
                              :tx-reports tx-reports})
                    ;; notify all processes that transaction is complete
                            (doseq [[tx-report callback] finalized-txs]
                              (>! callback tx-report)))
                          (catch #?(:clj Throwable :cljs js/Error) e
                            (cond
                              ;; NOT FATAL, and NOT RETRIED. The connection demands
                              ;; fencing and this head has no revision to fence
                              ;; against — a legacy branch head on an upgraded
                              ;; database. Retrying cannot change that (the re-read
                              ;; would find the same revisionless key), and killing
                              ;; the writer turns a migration condition into an
                              ;; outage: every caller after the first would get
                              ;; :writer-shut-down instead of the error that says
                              ;; what to do. Fail this group's callers with the
                              ;; message and keep the writer alive.
                              (= :datahike/fencing-unavailable (:type (ex-data e)))
                              (do
                                (log/warn :datahike/fencing-unavailable
                                          {:branch (:branch (:config db))})
                                (doseq [[_ callback] txs]
                                  (put! callback e)))

                              (= :konserve/revision-mismatch (:type (ex-data e)))
                              ;; NOT FATAL. Another writer moved the branch head
                              ;; between our head read and our head write, so this
                              ;; commit did not land — which is the fence doing its
                              ;; job, not the writer breaking. Report it and carry
                              ;; on; the queues stay open and the writer stays
                              ;; alive.
                              ;;
                              ;; The whole GROUP fails together, and correctly: a
                              ;; chained transaction was applied to the previous
                              ;; one's :db-after, so if the first never became
                              ;; durable the rest descend from a db that never
                              ;; existed. Later groups of the same batch carry a
                              ;; nil stamp and fall back to the threaded revision,
                              ;; which is now stale, so they fail the same way —
                              ;; which is the outcome we want, reached without a
                              ;; special case.
                              ;;
                              ;; `connection` is deliberately NOT reset: nothing was
                              ;; committed, so the db this writer holds is still the
                              ;; last one that was.
                              (do
                                (log/warn :datahike/head-conflict
                                          {:branch (:branch (:config db)) :transactions (count txs)})
                                ;; EXACTLY ONE outcome per invocation: it is either
                                ;; handed back for replay or its caller is told.
                                ;; Never both — the caller is still waiting on that
                                ;; one callback.
                                (doseq [[_ callback _ _ invocation] txs]
                                  (let [attempt (inc (get invocation :datahike/attempt 0))
                                        op      (:op invocation)]
                                    (if (and (contains? retryable-ops op)
                                             (<= attempt retries)
                                             invocation
                                             ;; A retry is only worth queueing while
                                             ;; a loop remains to drain it. `put!`
                                             ;; alone cannot tell us that — it
                                             ;; returns true on an OPEN channel
                                             ;; whatever its buffer state — so the
                                             ;; flag is the real check and the `put!`
                                             ;; below is the last line of defence for
                                             ;; the narrow race where the writer goes
                                             ;; down between the two: `put!` returns
                                             ;; false on a CLOSED channel, and a
                                             ;; dropped invocation whose caller is
                                             ;; still holding the callback is exactly
                                             ;; the permanent hang we are closing.
                                             (not @writer-down?)
                                             (put! retry-queue
                                                   (assoc invocation :datahike/attempt attempt)))
                                      (do
                                        (metrics/head-conflict! (:config db) :retried)
                                        (log/trace :datahike/head-conflict-retry {:op op :attempt attempt}))
                                      (do
                                        (metrics/head-conflict! (:config db) :failed)
                                        (put! callback
                                              (ex-info (str "The branch head moved while this transaction was being "
                                                            "prepared, so it was NOT applied — another writer committed "
                                                            "first. Nothing was lost and nothing partially applied; "
                                                            "re-read and transact again.")
                                                       {:type    :datahike/head-conflict
                                                        :branch  (:branch (:config db))
                                                        :op      op
                                                        :attempt attempt
                                                        :error   e})))))))
                              :else
                              (do
                            ;; Close the queues BEFORE delivering the failed
                            ;; callbacks. Delivering first unblocks the caller
                            ;; while the queues are still open, so a subsequent
                            ;; transact could race into the still-open queue and
                            ;; commit AFTER the fatal error (writer_error_test
                            ;; saw the "dead" writer accept a further write).
                            ;; Closing first makes that transact observe the
                            ;; closed queue and fail loudly (:writer-shut-down).
                                (reset! writer-down? true)
                                (close! commit-queue)
                                (close! transaction-queue)
                            ;; Release a shared-writer transaction loop that is
                            ;; parked on commit-done, or it never observes the
                            ;; closed transaction-queue and never shuts down.
                                (close! commit-done)
                            ;; And the retry queue, which NOTHING else drains once
                            ;; this loop is gone. A head conflict racing a fatal
                            ;; error hands its whole group here; with no loop left,
                            ;; every one of those callers derefs a promise that is
                            ;; never delivered. That is a silent permanent hang, so
                            ;; the queued invocations get the fatal error instead.
                                (close! retry-queue)
                                #?(:clj (fail-queued-invocations! retry-queue e))
                                (doseq [[_ callback] txs]
                                  (put! callback e))
                                (log/error :datahike/writer-shutdown {:error e})
                            ;; Re-throw Errors (AssertionError, OutOfMemoryError, etc.) to crash the writer
                                #?(:clj (when (instance? Error e)
                                          (throw e))))))
                          (finally
                            ;; A background secondary build holds a GC guard
                            ;; until the commit that publishes its ready key-map
                            ;; has either landed or definitively failed.
                            (when-not @build-cleanup-complete?
                              #?(:clj
                                 (doseq [[tx-report _] txs
                                         :let [build-guard
                                               (:secondary-index-build-guard
                                                tx-report)]
                                         :when build-guard]
                                   (w/finish-secondary-index-build! build-guard))))))
                        ;; Signalled AFTER the head flip (or after the failure
                        ;; path closed everything), so the transaction loop's
                        ;; next head read sees this commit.
                        ;;
                        ;; ONE SIGNAL PER TRANSACTION, not per commit: how this
                        ;; loop groups queued transactions into commits is its own
                        ;; business and the transaction loop cannot predict it, so
                        ;; counting commits would leave the two sides out of step
                        ;; — a permanently parked writer if we under-signal, and a
                        ;; growing pile of pending puts if we over-signal. Puts
                        ;; are capped at 1024 and THROW past it; MAX_SHARED_WRITER_BATCH
                        ;; keeps the count far below that.
                        (when shared?
                          (dotimes [_ (count txs)]
                            (put! commit-done true)))
                        (<! (timeout commit-wait-time))
                        (recur (<?- commit-queue)
                               ;; Non-throwing read, for two reasons that meet
                               ;; here: `@connection` routes through `deref-conn`,
                               ;; which throws once the connection is released
                               ;; (`release` marks it released before shutting the
                               ;; writer down, so closing the queue unparks the
                               ;; `<?-` above and this argument would then deref an
                               ;; already-released connection — #929); and on a
                               ;; SHARED connection it would additionally
                               ;; round-trip to storage. The wrapped atom holds the
                               ;; same value with neither hazard, for both writers.
                               (get-in @(:wrapped-atom connection) [:meta :datahike/commit-id])
                               ;; The revision our commit just created, read off the
                               ;; db `commit!` handed back rather than from storage —
                               ;; the write returned it, which is the whole point of
                               ;; asking for it.
                               (get @(:wrapped-atom connection) ::w/head-revision)))))))))]))

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
                           ;; The scan is asynchronous; install/delta replay is
                           ;; a short serialized report-producing operation.
                           #?@(:clj ['build-secondary-index! w/build-secondary-index!
                                     'install-secondary-index! w/install-secondary-index!
                                     'cancel-secondary-index-build!
                                     w/cancel-secondary-index-build!
                                     ;; Recovery re-anchors the snapshot boundary
                                     ;; before rebuilding (see connector).
                                     'reset-secondary-index-build-boundary!
                                     w/reset-secondary-index-build-boundary!])
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

   - `:writer-ownership` (default `:shared`) — `:shared` re-reads the branch
     head from storage before every batch and conditionally publishes it.
     `:exclusive` keeps the branch head in memory when one process owns it.

     COST: one branch-head GET per BATCH (~10-40 ms on S3, ~$0.0000004).
     Transactions that are already queued when one commits are chained onto it
     and share its head read, exactly as the exclusive writer chains onto
     `:db-after` — so a burst of concurrent writers pays a handful of reads, not
     one per transaction, and commit batching is preserved. A caller that waits
     for each transaction before issuing the next has nothing to batch and does
     pay one read per commit.

     The chain is bounded (`MAX_SHARED_WRITER_BATCH`) and never waits for more
     work to arrive, so batching costs no latency. It does widen the window
     between the head read and the commit that lands on it — see the fencing
     note below, which is what actually closes that window.

     This is the safe default because more than one process may accidentally
     hold a writer for the database. The serverless case makes that common:
     each AWS Lambda
     execution environment is a separate JVM that believes it is the only
     writer, and Lambda keeps several of them warm and routes to them
     alternately. With opt-in exclusive ownership, each environment commits from
     its own stale head and silently overwrites the other's transactions.

     Serialisation alone would not cover processes that OVERLAP: the loser's
     head write would simply land last. So the head write is also CONDITIONAL on
     the revision that was read (issue #878) — the commit is rejected rather than
     applied over another process's, and the transaction is re-applied against
     the head that moved. Nothing is lost and nothing is partially applied: what
     a commit writes before the head flip is immutable and content-addressed, so
     a rejected commit leaves collectable orphans, never a dangling pointer.

     Fencing needs a store that can compare-and-set, and konserve reports how far
     a store's guarantee reaches as a domain: `:process` (memory), `:machine`
     (filestore, via an OS advisory lock), `:global` (S3, via `If-Match`). It is
     used where available and skipped where it is not, so single-writer setups
     are unchanged on every backend.

   - `:require-fencing` (default `nil`) — the domain this deployment NEEDS.
     Refuses the connect on a store that cannot fence that far, instead of
     running unfenced. Skipping is silent by design, which is exactly why a
     deployment that depends on fencing should demand it: `:machine` for several
     processes on one host, `:global` for several hosts. A store offering more
     than asked passes. Requires `:writer-ownership :shared`.

   - `:head-conflict-retries` (default `MAX_HEAD_CONFLICT_RETRIES`) — how many
     times a rejected transaction is re-applied against the re-read head before
     its caller is told. `0` reports `:datahike/head-conflict` immediately, which
     is what you want when the caller must see every conflict. Only `transact!`
     and `load-entities` are ever retried: re-running a branch merge against a
     head that moved would silently change what the merge means.

   - `:head-conflict-backoff-ms` (default `DEFAULT_HEAD_CONFLICT_BACKOFF_MS`) —
     base for the jittered exponential backoff between retries. `0` disables the
     wait, which puts every attempt straight back into the window that just
     rejected it.

   - `:max-batch` (default `MAX_SHARED_WRITER_BATCH`) — upper bound on the chain
     described above."
  (fn [writer-config _]
    (:backend writer-config)))

(def self-writer-keys
  "Every key the `:self` writer understands. Closed, and checked at
   create-writer: a typo in `:writer-ownership` must not silently ignore a
   caller's ownership choice, especially when `:exclusive` opts into a
   single-process assumption whose failure is silent data loss. A spec cannot do this —
   `s/keys` accepts unqualified keys it does not list."
  #{:backend :writer-ownership :transaction-queue-size :commit-queue-size
    :commit-wait-time :write-fn-map
    :max-batch :head-conflict-retries :head-conflict-backoff-ms
    :require-fencing})

(defn check-fencing!
  "Raise unless `store` can fence branch-head writes as far as `require-fencing`
   asks. No-op when it asks for nothing.

   A separate fn because `connect` has two paths and only one of them builds a
   writer. Left inside `create-writer`, the check was skipped entirely whenever
   the connection came out of the registry cache — so the SECOND `connect` in a
   process, the one that makes concurrency possible in the first place, was the
   one that ran unchecked."
  [require-fencing writer-ownership store]
  (when require-fencing
    (when-not (contains? (set k/conditional-write-domains) require-fencing)
      (log/raise ":require-fencing must name a conditional-write domain."
                 {:type :invalid-require-fencing
                  :require-fencing require-fencing
                  :known (vec k/conditional-write-domains)}))
    (when (= :exclusive writer-ownership)
      (log/raise ":require-fencing needs :writer-ownership :shared. An exclusive writer keeps the branch head in memory and never re-reads it, so it has no revision to fence against — the option would be silently inert."
                 {:type :fencing-requires-shared-writer}))
    (let [have (k/conditional-write-domain store)]
      (when-not (k/conditional-write? store require-fencing)
        (log/raise (str "This store cannot fence branch-head writes as far as :require-fencing asks, "
                        "so the connection was refused rather than run unfenced.")
                   {:type            :insufficient-conditional-write-domain
                    :required        require-fencing
                    :store-offers    have
                    :note            (if have
                                       "The store fences, but not that far."
                                       "The store cannot compare-and-set at all; concurrent writers would silently overwrite each other.")})))))

(defmethod create-writer :self
  [writer-config connection]
  (let [{:keys [transaction-queue-size commit-queue-size write-fn-map commit-wait-time
                writer-ownership max-batch head-conflict-retries head-conflict-backoff-ms
                require-fencing]
         :as writer-config} (dc/normalize-writer-config writer-config)
        shared? (= :shared writer-ownership)]
    (when-let [unknown (seq (remove self-writer-keys (keys writer-config)))]
      (log/raise "Unknown key(s) in the :self writer config."
                 {:type    :unknown-self-writer-config-keys
                  :unknown (vec unknown)
                  :known   (vec (sort self-writer-keys))}))
  ;; FENCING IS A PRECONDITION, NOT A PREFERENCE — when asked for, it is checked
  ;; HERE, at connect, and refused rather than silently skipped.
  ;;
  ;; Without this datahike degrades quietly: a store that cannot compare-and-set
  ;; reports no domain, `reload-branch-head` reads no revision, and `commit!`
  ;; writes the head unconditionally. That is correct for a single writer and
  ;; exactly wrong for the deployment that asked for fencing — it would run
  ;; believing concurrent writers were safe, which is the failure this whole
  ;; mechanism exists to remove, reappearing one layer above konserve.
  ;;
  ;; The domain says how far the guarantee reaches, so state what the deployment
  ;; needs: `:machine` for several processes on one host (dthk across shells),
  ;; `:global` for several hosts (Lambda on S3). A store offering MORE than asked
  ;; passes; a memory store asked for :machine does not.
    (check-fencing! require-fencing writer-ownership (:store @(:wrapped-atom connection)))
    (doseq [[k v lo] [[:max-batch max-batch 1]
                      [:head-conflict-retries head-conflict-retries 0]
                      [:head-conflict-backoff-ms head-conflict-backoff-ms 0]]]
      (when (and (some? v) (not (and (integer? v) (>= v lo))))
        (log/raise (str k " in the :self writer config must be an integer >= " lo ".")
                   {:type :invalid-writer-config-value :key k :value v})))
    (let [transaction-queue-size (or transaction-queue-size DEFAULT_QUEUE_SIZE)
          commit-queue-size (or commit-queue-size DEFAULT_QUEUE_SIZE)
          commit-wait-time (or commit-wait-time DEFAULT_COMMIT_WAIT_TIME)
          retry-policy {:max-batch (or max-batch MAX_SHARED_WRITER_BATCH)
                        :retries   (or head-conflict-retries MAX_HEAD_CONFLICT_RETRIES)
                        :backoff   (or head-conflict-backoff-ms DEFAULT_HEAD_CONFLICT_BACKOFF_MS)}
          [transaction-queue commit-queue thread]
          (create-thread connection
                         (merge default-write-fn-map
                                write-fn-map)
                         transaction-queue-size
                         commit-queue-size
                         commit-wait-time
                         shared?
                         retry-policy)]
      (map->LocalWriter
       {:transaction-queue transaction-queue
        :transaction-queue-size transaction-queue-size
        :commit-queue commit-queue
        :commit-queue-size commit-queue-size
        :thread thread
        :writer-ownership writer-ownership}))))

;; Note: :kabel backend is implemented in datahike.kabel.writer
;; Require that namespace to register the defmethod

(defn dispatch! [writer arg-map]
  (-dispatch! writer arg-map))

(defn shutdown [writer]
  (-shutdown writer))

(defn streaming? [writer]
  (-streaming? writer))

(defn refresh-on-deref?
  "Whether dereferencing a connection backed by `writer` must refresh its head.

   The fallback preserves compatibility for third-party PWriter implementations:
   historically a non-streaming writer was exactly the case that refreshed."
  [writer]
  (if (satisfies? PConnectionRefresh writer)
    (-refresh-on-deref? writer)
    (not (-streaming? writer))))

(defn writer-ownership [writer]
  (:writer-ownership writer))

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

   Comparing against db-before is essential: only the schema transaction that
   creates or re-enables the index owns the initial backfill."
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
        db @(:wrapped-atom connection)
        writer (:writer db)
        local-writer? (= :self (get-in db [:config :writer :backend] :self))]
    (go
      (let [tx-report (<! (dispatch! writer
                                     {:op 'transact!
                                      :args [arg-map]}))]
        (when (map? tx-report) ;; not error
          #?(:clj
             (doseq [idx-ident (detect-new-building-indices tx-report)]
               (let [building-since-tx
                     (get-in tx-report
                             [:db-after :schema idx-ident
                              :db.secondary/building-since-tx])]
                 (log/trace :datahike/dispatch-backfill {:idx-ident idx-ident})
                 (go
                   (let [build-result (<! (dispatch! writer
                                                     {:op 'build-secondary-index!
                                                      :args [idx-ident]}))]
                     (if-not (map? build-result)
                       (do
                         (log/warn :datahike/secondary-index-build-failed
                                   {:idx-ident idx-ident :error build-result})
                         ;; A failed scan is a failed declaration, not a
                         ;; permanently :building one. The generation token
                         ;; prevents this cleanup from touching a replacement.
                         (let [cancel-result
                               (<! (dispatch!
                                    writer
                                    {:op 'cancel-secondary-index-build!
                                     :args
                                     [idx-ident building-since-tx
                                      {:type (or (some-> build-result ex-data :type)
                                                 :secondary-index-build-failed)
                                       :message (if (instance? Throwable build-result)
                                                  (ex-message build-result)
                                                  (str build-result))}]}))]
                           (when-not (map? cancel-result)
                             (log/warn
                              :datahike/secondary-index-build-failure-cleanup-failed
                              {:idx-ident idx-ident
                               :building-since-tx building-since-tx
                               :error cancel-result}))))
                       ;; Awaiting here does not block the writer. The install is
                       ;; merely queued behind transactions that may have arrived
                       ;; during the scan; it replays their journaled deltas.
                       (let [install-result
                             (<! (dispatch! writer
                                            {:op 'install-secondary-index!
                                             :args [build-result]}))]
                         ;; If release shut the local queue between scan and
                         ;; install, no commit report exists to release the guard.
                         ;; The build ran in this JVM, so clean it up here.
                         (when (and local-writer? (not (map? install-result)))
                           (w/finish-secondary-index-build! build-result)))))))))
          (doseq [[_ callback] (some-> (:listeners (meta connection)) (deref))]
            (callback tx-report)))
        (#?(:clj deliver :cljs put!) p tx-report)))
    p))

#?(:clj
   (defn cancel-secondary-index-build!
     "Cancel one exact asynchronous secondary-index build generation.

      Returns a throwable promise containing the committed transaction report.
      A generation mismatch is delivered as an ExceptionInfo and leaves the
      current declaration untouched."
     [connection idx-ident building-since-tx]
     (let [p (throwable-promise)
           db @(:wrapped-atom connection)
           writer (:writer db)]
       (go
         (let [tx-report
               (<! (dispatch! writer
                              {:op 'cancel-secondary-index-build!
                               :args [idx-ident building-since-tx]}))]
           (when (map? tx-report)
             (doseq [[_ callback]
                     (some-> (:listeners (meta connection)) deref)]
               (callback tx-report)))
           (deliver p tx-report)))
       p)))

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
