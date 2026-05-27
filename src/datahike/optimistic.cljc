(ns datahike.optimistic
  "Optimistic-overlay helpers over a Datahike connection.

  Layers pending client-submitted transactions on top of the durable @conn
  value via `d/with`, exposing the result as `effective-db`.

  An entry remains visible until @conn has demonstrably caught up to the
  transaction's effect (the writer-assigned `:max-tx`), the dispatch
  failed, or a per-entry TTL elapsed. See `doc/optimistic-overlay.md` for
  the full consistency model.

  Usage:

    (require '[datahike.optimistic :as opt])

    (opt/register! conn
      {:ttl-ms 30000
       :on-conflict (fn [conflicts] ...)})
    (opt/listen!   conn ::ui (fn [eff-db] (rerender! eff-db)))
    (opt/transact! conn [{:db/id -1 :name \"alice\"}])
    ...
    (opt/unlisten!   conn ::ui)
    (opt/unregister! conn)

  Identity assumption: identify entities by a stable attribute
  (`:entity/uuid` minted client-side) rather than by EID — tempid
  resolution may differ between the overlay's `d/with` and the durable
  writer."
  (:require [datahike.api :as d]
            #?(:clj  [clojure.core.async :as a :refer [chan put! alts! timeout close!]]
               :cljs [clojure.core.async :as a :refer [chan put! alts! timeout close!]])
            [superv.async #?(:clj :refer :cljs :refer-macros) [<?-]]))

;; -----------------------------------------------------------------------------
;; Per-connection state
;; -----------------------------------------------------------------------------

(defonce ^:private *state (atom {}))

(defn- conn-state [conn]
  (get @*state conn))

(defn- now-ms []
  #?(:clj  (System/currentTimeMillis)
     :cljs (.getTime (js/Date.))))

(defn- throwable? [v]
  #?(:clj  (instance? Throwable v)
     :cljs (instance? js/Error v)))

;; -----------------------------------------------------------------------------
;; Effective DB + conflict detection
;; -----------------------------------------------------------------------------

(defn- recompute
  "Pure fold over overlay entries.
   Returns {:db <db-after-all-non-conflicting-applied>
            :conflicts {ov-id error}}.

   An entry whose `d/with` throws is excluded from the produced db and
   recorded in `:conflicts`. The overlay itself is not mutated here —
   the caller is responsible for surfacing conflict status."
  [base-db overlay-vec]
  (reduce (fn [{:keys [db conflicts]} {:keys [ov-id tx-data]}]
            (try
              {:db (:db-after (d/with db tx-data)) :conflicts conflicts}
              (catch #?(:clj Throwable :cljs :default) e
                {:db db :conflicts (assoc conflicts ov-id e)})))
          {:db base-db :conflicts {}}
          overlay-vec))

(defn effective-db
  "Return the db value with all currently-applicable optimistic entries
  on top. Entries whose `d/with` would throw against the current @conn
  (Case J — conflict) are excluded from the view but stay in the overlay
  until their dispatch resolves or their TTL expires.

  Returns @conn unchanged if `conn` is not registered."
  [conn]
  (if-let [{:keys [overlay]} (conn-state conn)]
    (:db (recompute @conn @overlay))
    @conn))

(defn pending
  "Return current pending entries for conn (empty vector if none / not
  registered). Internal fields (`:result-ch`, `:result-delivered?`)
  are stripped — entries carry `:ov-id`, `:tx-data`, `:submitted-at`,
  `:expires-at`, `:expected-max-tx`, `:conflicting?`,
  `:last-conflict-error`."
  [conn]
  (if-let [{:keys [overlay]} (conn-state conn)]
    (mapv #(dissoc % :result-ch :result-delivered?) @overlay)
    []))

;; -----------------------------------------------------------------------------
;; Listener firing
;; -----------------------------------------------------------------------------

(defn- mark-conflicts! [overlay conflict-map]
  (swap! overlay
         (fn [entries]
           (mapv (fn [e]
                   (let [err (get conflict-map (:ov-id e))]
                     (assoc e
                            :conflicting? (some? err)
                            :last-conflict-error err)))
                 entries))))

(defn- fire-listeners! [conn]
  (when-let [{:keys [overlay listeners on-conflicts last-conflict-ids]}
             (conn-state conn)]
    (let [snapshot @overlay
          {:keys [db conflicts]} (recompute @conn snapshot)
          new-conflict-ids (set (keys conflicts))]
      (mark-conflicts! overlay conflicts)
      (doseq [[_ f] @listeners]
        (try (f db)
             (catch #?(:clj Throwable :cljs :default) e
               (println "datahike.optimistic listener error:" e))))
      (when (not= new-conflict-ids @last-conflict-ids)
        (reset! last-conflict-ids new-conflict-ids)
        (let [sanitized (->> @overlay
                             (filter :conflicting?)
                             (mapv #(dissoc % :result-ch :result-delivered?)))]
          (doseq [[_ f] @on-conflicts]
            (try (f sanitized)
                 (catch #?(:clj Throwable :cljs :default) e
                   (println "datahike.optimistic on-conflict error:" e)))))))))

;; -----------------------------------------------------------------------------
;; Exactly-once :result delivery
;; -----------------------------------------------------------------------------

(defn- deliver-result! [entry value]
  ;; Whichever code path (dispatch success/failure, TTL expiry,
  ;; unregister!) gets here first wins. Others are no-ops, so a
  ;; full :result-ch buffer can never block a producer. Entries
  ;; that were injected directly into the overlay (tests / advanced
  ;; users) won't carry a `:result-delivered?` atom — skip them.
  (when-let [flag (:result-delivered? entry)]
    (when (compare-and-set! flag false true)
      (put! (:result-ch entry) value))))

;; -----------------------------------------------------------------------------
;; Watcher: drop entries whose @conn caught up
;; -----------------------------------------------------------------------------

(defn- drop-caught-up!
  "Atomically remove entries whose `:expected-max-tx` is <= the new
  @conn's `:max-tx`. The dispatch already delivered its :reply on
  `:result-ch` when it set `:expected-max-tx` — there is no further
  signal to fire on the result channel here.

  Returns true iff any entry was dropped."
  [overlay new-max]
  (let [[old new] (swap-vals! overlay
                              (fn [v]
                                (filterv
                                 (fn [{:keys [expected-max-tx]}]
                                   (not (and expected-max-tx
                                             new-max
                                             (>= new-max expected-max-tx))))
                                 v)))]
    (not= (count old) (count new))))

(defn- on-conn-advance [conn old-db new-db]
  (when (not= (:max-tx old-db) (:max-tx new-db))
    (when-let [{:keys [overlay]} (conn-state conn)]
      (drop-caught-up! overlay (:max-tx new-db)))
    ;; Fire listeners whether or not anything was dropped — @conn moved,
    ;; so the effective-db moved with it, and consumers expect a tick.
    (fire-listeners! conn)))

;; -----------------------------------------------------------------------------
;; TTL heartbeat
;; -----------------------------------------------------------------------------

(def ^:private heartbeat-tick-ms 1000)

(defn- expire-due!
  "Atomically remove entries whose TTL has elapsed; deliver a
  TimeoutException on each one's :result-ch (exactly-once via
  `deliver-result!`); fire listeners if anything moved."
  [conn]
  (when-let [{:keys [overlay]} (conn-state conn)]
    (let [now (now-ms)
          expired? (fn [{:keys [expires-at]}]
                     (and expires-at (< expires-at now)))
          [old new] (swap-vals! overlay #(filterv (complement expired?) %))
          gone (filterv expired? old)]
      (when (seq gone)
        (doseq [e gone]
          (deliver-result! e
                           (ex-info "Optimistic transaction timed out"
                                    {:type :optimistic/timeout
                                     :ov-id (:ov-id e)
                                     :submitted-at (:submitted-at e)
                                     :expires-at (:expires-at e)})))
        (fire-listeners! conn)))))

(defn- start-heartbeat! [conn stop-ch]
  (a/go-loop []
    (let [[_ port] (alts! [stop-ch (timeout heartbeat-tick-ms)])]
      (when-not (= port stop-ch)
        (try (expire-due! conn)
             (catch #?(:clj Throwable :cljs :default) e
               (println "datahike.optimistic heartbeat error:" e)))
        (recur)))))

;; -----------------------------------------------------------------------------
;; Dispatch normalization
;; -----------------------------------------------------------------------------

(defn- default-dispatch
  "Wrap the conn's writer (`d/transact!`) and conform to the
  `{:reply :max-tx}` contract."
  [conn tx-data]
  (let [out (chan 1)]
    #?(:clj
       (a/thread
         (try
           (let [report @(d/transact! conn tx-data)]
             (put! out {:reply report
                        :max-tx (:max-tx (:db-after report))}))
           (catch Throwable e (put! out e))))
       :cljs
       (a/go
         (let [report (a/<! (d/transact! conn tx-data))]
           (if (throwable? report)
             (put! out report)
             (put! out {:reply report
                        :max-tx (:max-tx (:db-after report))})))))
    out))

(defn- run-user-dispatch
  "Invoke the user's `:dispatch-fn` and route the result (or any
  throw / yielded-error) onto a single channel. Caller reads with
  `<?-` to unify throw and yield-an-error."
  [dispatch-fn]
  (let [out (chan 1)]
    #?(:clj
       (a/thread
         (try
           (let [v @(dispatch-fn)]
             ;; @ on a `throwable-promise` throws; on a plain promise
             ;; with a Throwable value, we'd get the value back — pass
             ;; it through and let the consumer's <?- normalize.
             (put! out v))
           (catch Throwable e (put! out e))))
       :cljs
       (a/go
         (try
           (let [v (a/<! (dispatch-fn))]
             (put! out v))
           (catch :default e (put! out e)))))
    out))

;; -----------------------------------------------------------------------------
;; Public API
;; -----------------------------------------------------------------------------

(def ^:private default-ttl-ms 30000)

(defn register!
  "Register a conn for optimistic updates. Idempotent — a second call
  with the same conn has no effect. Returns conn.

  Options (all optional):
    :ttl-ms       Default per-entry TTL in ms. Defaults to 30000.
                  Pass `nil` to disable the TTL by default for this conn.
                  Per-call override via `transact!`'s opts.
    :on-conflict  `(fn [conflicts])` — fired when the set of conflicting
                  entries changes. `conflicts` is a vector of pending
                  entries currently un-applicable on top of @conn (see
                  Case J in the doc). Each carries `:ov-id`, `:tx-data`,
                  `:last-conflict-error`, etc.

  Additional on-conflict listeners can be registered later via
  `on-conflict!`."
  ([conn] (register! conn {}))
  ([conn {:keys [ttl-ms on-conflict] :or {ttl-ms default-ttl-ms}}]
   (when-not (conn-state conn)
     (let [overlay           (atom [])
           listeners         (atom {})
           on-conflicts      (atom (if on-conflict
                                     {::default-on-conflict on-conflict}
                                     {}))
           last-conflict-ids (atom #{})
           heartbeat-stop    (chan)
           watch-key         (keyword "datahike.optimistic"
                                      (str "watch-" (random-uuid)))]
       (swap! *state assoc conn
              {:overlay           overlay
               :listeners         listeners
               :on-conflicts      on-conflicts
               :last-conflict-ids last-conflict-ids
               :heartbeat-stop    heartbeat-stop
               :ttl-ms            ttl-ms
               :watch-key         watch-key})
       (add-watch conn watch-key
                  (fn [_ _ old new] (on-conn-advance conn old new)))
       (start-heartbeat! conn heartbeat-stop)))
   conn))

(defn unregister!
  "Detach the overlay from conn. Cancels any in-flight pending entries
  by delivering an `:optimistic/cancelled` error on their :result chan.
  Stops the heartbeat and clears all overlay state."
  [conn]
  (when-let [{:keys [watch-key heartbeat-stop overlay]} (conn-state conn)]
    (remove-watch conn watch-key)
    (close! heartbeat-stop)
    (doseq [e @overlay]
      (deliver-result! e
                       (ex-info "Optimistic conn unregistered while entry pending"
                                {:type  :optimistic/cancelled
                                 :ov-id (:ov-id e)})))
    (swap! *state dissoc conn))
  nil)

(defn listen!
  "Register a listener `(fn [effective-db])` under key `k`. Fires
  whenever the overlay changes or @conn advances. The argument is the
  effective db — @conn with all currently-applicable overlay entries
  layered on top (conflicting entries excluded; see `:on-conflict`)."
  [conn k f]
  (when-not (conn-state conn)
    (throw (ex-info "Conn not registered for optimistic updates"
                    {:conn conn})))
  (swap! (:listeners (conn-state conn)) assoc k f)
  nil)

(defn unlisten! [conn k]
  (when-let [{:keys [listeners]} (conn-state conn)]
    (swap! listeners dissoc k))
  nil)

(defn on-conflict!
  "Register a conflict-listener `(fn [conflicts])` under key `k`.
  Fires whenever the set of conflicting entries changes."
  [conn k f]
  (when-not (conn-state conn)
    (throw (ex-info "Conn not registered for optimistic updates"
                    {:conn conn})))
  (swap! (:on-conflicts (conn-state conn)) assoc k f)
  nil)

(defn off-conflict! [conn k]
  (when-let [{:keys [on-conflicts]} (conn-state conn)]
    (swap! on-conflicts dissoc k))
  nil)

(defn transact!
  "Optimistic transact.

  Eagerly validates `tx-data` via `d/with` against the current
  effective-db; on validation failure throws synchronously and the
  overlay is untouched. Otherwise appends a pending entry, fires
  listeners with the optimistic effective-db, and dispatches the write
  in the background.

  Returns `{:ov-id uuid :result <chan>}`:
   - `:ov-id`  — client-minted UUID identifying this entry.
   - `:result` — 1-buffer channel; yields the dispatch's :reply on
                 success or a `Throwable`/`js/Error` on failure
                 (validation, server rejection, TTL timeout, or
                 unregister! while pending).

  Dispatch defaults to the conn's writer (`d/transact!`); the wrapper
  extracts `(:max-tx (:db-after tx-report))` and uses it as the
  watermark for dropping the entry from the overlay.

  Pass `:dispatch-fn` to substitute your own RPC. Contract:
   - takes no arguments
   - returns a deref-able (CLJ) or `core.async` channel (CLJS) that
     either yields `{:reply X :max-tx N}` on success — where `N` is the
     `:max-tx` of the durable commit produced by your RPC — or
     throws / yields a `Throwable` / `js/Error` on failure.
   - The wrapper normalizes throw vs yield-an-error onto the same
     failure path.
   - `X` is what gets put on `:result`.

  Pass `:ttl-ms` to override the conn's default TTL for this call;
  pass `:ttl-ms nil` to disable the TTL for this call."
  ([conn tx-data] (transact! conn tx-data {}))
  ([conn tx-data {:keys [dispatch-fn] :as opts}]
   (let [{:keys [overlay] :as st}
         (or (conn-state conn)
             (throw (ex-info "Conn not registered for optimistic updates"
                             {:conn conn})))
         ;; Eager validation — throws on schema/type violations,
         ;; nothing enters the overlay.
         _validate (d/with (effective-db conn) tx-data)
         ov-id        (random-uuid)
         submitted-at (now-ms)
         ttl-ms       (if (contains? opts :ttl-ms) (:ttl-ms opts) (:ttl-ms st))
         expires-at   (when ttl-ms (+ submitted-at ttl-ms))
         result-ch    (chan 1)
         entry        {:ov-id               ov-id
                       :tx-data             tx-data
                       :submitted-at        submitted-at
                       :expires-at          expires-at
                       :expected-max-tx     nil
                       :conflicting?        false
                       :last-conflict-error nil
                       :result-ch           result-ch
                       :result-delivered?   (atom false)}]
     (swap! overlay conj entry)
     (fire-listeners! conn)
     (a/go
       (try
         (let [val (<?- (if dispatch-fn
                          (run-user-dispatch dispatch-fn)
                          (default-dispatch conn tx-data)))]
           (when-not (and (map? val) (contains? val :reply))
             (throw (ex-info
                     "dispatch-fn returned an invalid shape; expected {:reply :max-tx}"
                     {:type :optimistic/invalid-dispatch :got val})))
           (let [{:keys [reply max-tx]} val
                 ;; Mark the entry with its watermark — the watcher
                 ;; will drop it when @conn :max-tx catches up. If the
                 ;; entry is no longer present (heartbeat / unregister!
                 ;; raced ahead), this swap! is a no-op.
                 [_old new-overlay]
                 (swap-vals! overlay
                             (fn [v]
                               (mapv (fn [e]
                                       (if (= (:ov-id e) ov-id)
                                         (assoc e :expected-max-tx max-tx)
                                         e))
                                     v)))
                 still-present? (some #(= (:ov-id %) ov-id) new-overlay)]
             (when still-present?
               ;; Sync drop: if @conn already advanced past max-tx
               ;; (default `:self` writer commits before returning),
               ;; the watcher already fired but couldn't drop because
               ;; :expected-max-tx wasn't set yet. Catch that here.
               (when (and max-tx (>= (:max-tx @conn) max-tx))
                 (swap! overlay #(filterv (fn [e] (not= (:ov-id e) ov-id)) %)))
               ;; If the user's dispatch-fn neglected :max-tx, fall
               ;; back to drop-on-resolve so the entry doesn't linger
               ;; forever (silently — documented contract requires it).
               (when (nil? max-tx)
                 (swap! overlay #(filterv (fn [e] (not= (:ov-id e) ov-id)) %))))
             (deliver-result! entry reply)))
         (catch #?(:clj Throwable :cljs :default) e
           ;; Drop the entry if still present; fire listeners; deliver
           ;; the error. `deliver-result!` is exactly-once, so a prior
           ;; TTL/cancel won't be clobbered.
           (let [[old new] (swap-vals! overlay
                                       #(filterv (fn [x] (not= (:ov-id x) ov-id)) %))]
             (when (not= (count old) (count new))
               (fire-listeners! conn)))
           (deliver-result! entry e))))
     {:ov-id ov-id :result result-ch})))
