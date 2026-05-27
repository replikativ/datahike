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
            [datahike.datom :as dd]
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
;; Datom helpers for tx-report deltas
;; -----------------------------------------------------------------------------

(defn- key-eav [d]
  [(.-e d) (.-a d) (.-v d)])

(defn- negate
  "Flip the added? flag on every datom — assertions become retracts and
  vice versa. Used to roll back an entry's effect on the consumer's
  view when the dispatch failed / TTL fired / a conflict appeared.

  Both directions matter: if the original tx-data was a retract (e.g.
  `[[:db/retract 1 :name \"old\"]]`), the `:overlay-add` event shipped
  the retract to the consumer; rolling it back means re-asserting."
  [datoms]
  (mapv (fn [d]
          (dd/datom (.-e d) (.-a d) (.-v d) (dd/datom-tx d)
                    (not (dd/datom-added d))))
        datoms))

(defn- retract-stale
  "Return retract-form datoms for any *added* datoms in `predicted` whose
  `[e a v]` does NOT appear (as an addition) in `realized`. Used to roll
  back stale local-EID predictions when the durable writer commits the
  same logical entity at a different EID. When predicted and realized
  EIDs match (common case), returns an empty vector."
  [predicted realized]
  (let [realized-keys (into #{} (comp (filter dd/datom-added) (map key-eav)) realized)]
    (->> predicted
         (filter dd/datom-added)
         (remove (fn [d] (realized-keys (key-eav d))))
         (mapv (fn [d] (dd/datom (.-e d) (.-a d) (.-v d) (dd/datom-tx d) false))))))

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

(defn- emit-tx!
  "Fire all tx-listeners with the given tx-report. Skips silently when
  no tx-listeners are registered."
  [conn report]
  (when-let [{:keys [tx-listeners]} (conn-state conn)]
    (when (seq @tx-listeners)
      (doseq [[_ f] @tx-listeners]
        (try (f report)
             (catch #?(:clj Throwable :cljs :default) e
               (println "datahike.optimistic tx-listener error:" e)))))))

(defn- emit-conflict-transitions!
  "Compare prior and current conflict sets; emit `:overlay-conflict`
  tx-reports for entries that just became un-applicable, and
  `:overlay-resolve` tx-reports for entries that just became applicable
  again. Each emitted report carries the entry's predicted-tx-data
  (retracted for new conflicts, re-added for resolutions)."
  [conn old-conflict-ids new-conflict-ids entries-by-id db-before db-after]
  (let [newly-conflict (clojure.set/difference new-conflict-ids old-conflict-ids)
        newly-resolved (clojure.set/difference old-conflict-ids new-conflict-ids)]
    (when (seq newly-conflict)
      (let [retracts (vec (mapcat (fn [ov-id]
                                    (negate
                                     (:predicted-tx-data (entries-by-id ov-id))))
                                  newly-conflict))]
        (when (seq retracts)
          (emit-tx! conn {:db-before db-before
                          :db-after  db-after
                          :tx-data   retracts
                          :origin    :overlay-conflict}))))
    (when (seq newly-resolved)
      (let [restored (vec (mapcat (fn [ov-id]
                                    (:predicted-tx-data (entries-by-id ov-id)))
                                  newly-resolved))]
        (when (seq restored)
          (emit-tx! conn {:db-before db-before
                          :db-after  db-after
                          :tx-data   restored
                          :origin    :overlay-resolve}))))))

(defn- fire-listeners! [conn]
  (when-let [{:keys [overlay listeners on-conflicts last-conflict-ids last-effective-db]}
             (conn-state conn)]
    (let [snapshot @overlay
          {:keys [db conflicts]} (recompute @conn snapshot)
          new-conflict-ids (set (keys conflicts))
          old-conflict-ids @last-conflict-ids
          db-before (or @last-effective-db @conn)]
      (mark-conflicts! overlay conflicts)
      (doseq [[_ f] @listeners]
        (try (f db)
             (catch #?(:clj Throwable :cljs :default) e
               (println "datahike.optimistic listener error:" e))))
      ;; Conflict-list listeners (high-level set-changed event)
      (when (not= new-conflict-ids old-conflict-ids)
        (reset! last-conflict-ids new-conflict-ids)
        (let [sanitized (->> @overlay
                             (filter :conflicting?)
                             (mapv #(dissoc % :result-ch :result-delivered?)))]
          (doseq [[_ f] @on-conflicts]
            (try (f sanitized)
                 (catch #?(:clj Throwable :cljs :default) e
                   (println "datahike.optimistic on-conflict error:" e))))))
      ;; Tx-report emissions for conflict transitions
      (when (not= new-conflict-ids old-conflict-ids)
        (let [entries-by-id (into {} (map (juxt :ov-id identity)) @overlay)]
          (emit-conflict-transitions! conn old-conflict-ids new-conflict-ids
                                      entries-by-id db-before db)))
      (reset! last-effective-db db))))

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
  `deliver-result!`); fire eff-db listeners; emit one :ttl tx-report
  per expired entry (retracting its predicted-tx-data)."
  [conn]
  (when-let [{:keys [overlay last-effective-db]} (conn-state conn)]
    (let [now (now-ms)
          expired? (fn [{:keys [expires-at]}]
                     (and expires-at (< expires-at now)))
          db-before (or @last-effective-db @conn)
          [old _] (swap-vals! overlay #(filterv (complement expired?) %))
          gone (filterv expired? old)]
      (when (seq gone)
        (doseq [e gone]
          (deliver-result! e
                           (ex-info "Optimistic transaction timed out"
                                    {:type :optimistic/timeout
                                     :ov-id (:ov-id e)
                                     :submitted-at (:submitted-at e)
                                     :expires-at (:expires-at e)})))
        ;; Compute new effective-db once and fire eff-db listeners.
        (fire-listeners! conn)
        ;; Emit a :ttl tx-report per expired entry — each retracts its
        ;; predicted additions (unless the entry was already conflicting,
        ;; in which case its prediction was already retracted via
        ;; :overlay-conflict and there's nothing further to roll back).
        (let [db-after (effective-db conn)]
          (doseq [e gone
                  :when (and (not (:conflicting? e))
                             (seq (:predicted-tx-data e)))]
            (emit-tx! conn {:db-before db-before
                            :db-after  db-after
                            :tx-data   (negate (:predicted-tx-data e))
                            :origin    :ttl
                            :ov-id     (:ov-id e)})))))))

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
  ([conn {:keys [on-conflict] :as opts}]
   ;; Resolve ttl-ms via `contains?` so an explicit `:ttl-ms nil`
   ;; reaches us as nil (= disable), instead of being overwritten by
   ;; an `:or` default.
   (let [ttl-ms (if (contains? opts :ttl-ms) (:ttl-ms opts) default-ttl-ms)]
     (when-not (conn-state conn)
       (let [overlay           (atom [])
             listeners         (atom {})
             tx-listeners      (atom {})
             on-conflicts      (atom (if on-conflict
                                       {::default-on-conflict on-conflict}
                                       {}))
             last-conflict-ids (atom #{})
             last-effective-db (atom nil)
             heartbeat-stop    (chan)
             watch-key         (keyword "datahike.optimistic"
                                        (str "watch-" (random-uuid)))
             writer-listen-key (keyword "datahike.optimistic"
                                        (str "writer-" (random-uuid)))]
         (swap! *state assoc conn
                {:overlay           overlay
                 :listeners         listeners
                 :tx-listeners      tx-listeners
                 :on-conflicts      on-conflicts
                 :last-conflict-ids last-conflict-ids
                 :last-effective-db last-effective-db
                 :heartbeat-stop    heartbeat-stop
                 :ttl-ms            ttl-ms
                 :watch-key         watch-key
                 :writer-listen-key writer-listen-key})
       ;; The conn's writer-listener fires for every successful
       ;; `d/transact!` through this conn — whether it came from our
       ;; own `opt/transact!`'s default dispatch OR a direct call by
       ;; other code. We emit a `:conn-advance` tx-report carrying the
       ;; writer's `:tx-data` so consumers stay in sync with all
       ;; durable changes, not just our own.
       ;;
       ;; This fires AFTER `reset!` (so @conn is advanced) and BEFORE
       ;; the d/transact! caller's promise resolves — meaning by the
       ;; time our own dispatch's go-block resumes, the :conn-advance
       ;; has already been emitted, and the dispatch path only needs
       ;; to emit any stale-retract follow-up.
         (d/listen conn writer-listen-key
                   (fn [tx-report]
                     (let [db-before (or @last-effective-db @conn)]
                       (emit-tx! conn {:db-before db-before
                                       :db-after  (effective-db conn)
                                       :tx-data   (vec (:tx-data tx-report))
                                       :tempids   (:tempids tx-report)
                                       :tx-meta   (:tx-meta tx-report)
                                       :origin    :conn-advance}))))
         (add-watch conn watch-key
                    (fn [_ _ old new] (on-conn-advance conn old new)))
         (start-heartbeat! conn heartbeat-stop))))
   conn))

(defn unregister!
  "Detach the overlay from conn. Cancels any in-flight pending entries
  by delivering an `:optimistic/cancelled` error on their :result chan.
  Stops the heartbeat and clears all overlay state."
  [conn]
  (when-let [{:keys [watch-key writer-listen-key heartbeat-stop overlay]}
             (conn-state conn)]
    (remove-watch conn watch-key)
    (d/unlisten conn writer-listen-key)
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

(defn listen-tx!
  "Register a tx-report listener `(fn [tx-report])` under key `k`.
  Fires once per logical change to the overlay or @conn, with a
  Datahike-style tx-report:

    {:db-before <effective-db at the start of the event>
     :db-after  <effective-db at the end>
     :tx-data   <vector of #datahike/Datom [e a v t added?]>
     :origin    :overlay-add | :conn-advance | :overlay-drop
              | :overlay-conflict | :overlay-resolve | :ttl
     :ov-id     <uuid, only for entry-scoped origins>}

  The `:tx-data` is the *delta* a consumer should apply to their
  derived view to bring it in line with `:db-after`. For successful
  optimistic writes, an `:overlay-add` event ships the predicted
  datoms; a follow-up `:conn-advance` event ships the durable writer's
  datoms plus retracts of any prediction datoms that landed at
  different EIDs (the EID-shift handling — see
  `doc/optimistic-overlay.md`). On failure / TTL / conflict, the
  prediction's additions are emitted as retracts so a consumer
  applying tx-data incrementally always converges to `@conn`.

  Foreign-peer writes that bypass `d/transact!` do not currently emit
  tx-reports (v1 limit). Standard `listen!` consumers still see
  `effective-db` updates from foreign writes."
  [conn k f]
  (when-not (conn-state conn)
    (throw (ex-info "Conn not registered for optimistic updates"
                    {:conn conn})))
  (swap! (:tx-listeners (conn-state conn)) assoc k f)
  nil)

(defn unlisten-tx! [conn k]
  (when-let [{:keys [tx-listeners]} (conn-state conn)]
    (swap! tx-listeners dissoc k))
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
         ;; Eager validation against the current effective-db — throws
         ;; on schema/type violations, nothing enters the overlay. Reuse
         ;; the result to cache the predicted tx-data shipped to
         ;; tx-listeners on :overlay-add.
         db-before-add     (effective-db conn)
         validate-report   (d/with db-before-add tx-data)
         predicted-tx-data (vec (:tx-data validate-report))
         ov-id        (random-uuid)
         submitted-at (now-ms)
         ttl-ms       (if (contains? opts :ttl-ms) (:ttl-ms opts) (:ttl-ms st))
         expires-at   (when ttl-ms (+ submitted-at ttl-ms))
         result-ch    (chan 1)
         entry        {:ov-id               ov-id
                       :tx-data             tx-data
                       :predicted-tx-data   predicted-tx-data
                       :submitted-at        submitted-at
                       :expires-at          expires-at
                       :expected-max-tx     nil
                       :conflicting?        false
                       :last-conflict-error nil
                       :result-ch           result-ch
                       :result-delivered?   (atom false)}]
     (swap! overlay conj entry)
     (fire-listeners! conn)
     ;; tx-report for the optimistic add. db-after is the new effective-db.
     ;; :tempids and :tx-meta come from the eager d/with — same shape as
     ;; a Datahike TxReport so consumers can use one code path for both
     ;; optimistic and durable reports.
     (emit-tx! conn {:db-before db-before-add
                     :db-after  (effective-db conn)
                     :tx-data   predicted-tx-data
                     :tempids   (:tempids validate-report)
                     :tx-meta   (:tx-meta validate-report)
                     :origin    :overlay-add
                     :ov-id     ov-id})
     (a/go
       (try
         (let [val (<?- (if dispatch-fn
                          (run-user-dispatch dispatch-fn)
                          (default-dispatch conn tx-data)))]
           (when-not (and (map? val) (contains? val :reply) (contains? val :max-tx))
             (throw (ex-info
                     "dispatch-fn returned an invalid shape; expected {:reply :max-tx}"
                     {:type :optimistic/invalid-dispatch :got val})))
           (let [{:keys [reply max-tx]} val
                 ;; Mark the entry with its watermark — the watcher
                 ;; will drop it when @conn :max-tx catches up.
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
               (let [conn-max (:max-tx @conn)
                     ;; `dispatch-fn` contract requires :max-tx, so we
                     ;; drop the entry iff @conn has caught up. The
                     ;; watcher will drop it later if @conn is still
                     ;; behind at this point.
                     dropping? (and max-tx conn-max (>= conn-max max-tx))
                     db-before-drop (effective-db conn)
                     _ (when dropping?
                         (swap! overlay
                                #(filterv (fn [e] (not= (:ov-id e) ov-id)) %)))
                     ;; The writer's tx-data was already emitted as
                     ;; :conn-advance by our writer-listener. The only
                     ;; remaining work is the stale-retract follow-up:
                     ;; if the writer landed our datoms at different
                     ;; EIDs than our local prediction (tempid shift),
                     ;; retract the stale prediction here. Use the
                     ;; reply's :tx-data when we have it (default
                     ;; dispatch); else empty.
                     server-tx-data (or (some-> reply :tx-data vec) [])]
                 (when dropping?
                   (let [stale-retracts (retract-stale predicted-tx-data
                                                       server-tx-data)]
                     (when (seq stale-retracts)
                       (emit-tx! conn {:db-before db-before-drop
                                       :db-after  (effective-db conn)
                                       :tx-data   stale-retracts
                                       :origin    :overlay-realized
                                       :ov-id     ov-id}))))))
             (deliver-result! entry reply)))
         (catch #?(:clj Throwable :cljs :default) e
           ;; Drop the entry if still present; fire eff-db listeners;
           ;; emit :overlay-drop tx-report retracting predictions;
           ;; deliver the error.
           (let [db-before-drop (effective-db conn)
                 [old new] (swap-vals! overlay
                                       #(filterv (fn [x] (not= (:ov-id x) ov-id)) %))
                 dropped-entry (first (filter #(= (:ov-id %) ov-id) old))]
             (when (not= (count old) (count new))
               (fire-listeners! conn)
               (when (and dropped-entry
                          (not (:conflicting? dropped-entry))
                          (seq (:predicted-tx-data dropped-entry)))
                 (emit-tx! conn
                           {:db-before db-before-drop
                            :db-after  (effective-db conn)
                            :tx-data   (negate
                                        (:predicted-tx-data dropped-entry))
                            :origin    :overlay-drop
                            :ov-id     ov-id}))))
           (deliver-result! entry e))))
     {:ov-id ov-id :result result-ch})))
