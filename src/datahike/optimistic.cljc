(ns datahike.optimistic
  "Optimistic-overlay helpers over a Datahike connection.

  Layers pending client-submitted transactions on top of the durable @conn
  value via `d/with`, exposing the effect as a tx-report stream to
  `listen!` and the resulting effective db value via `effective-db`.

  An entry remains visible until @conn has demonstrably caught up to the
  transaction's effect (the writer-assigned `:max-tx`), the durable
  transact failed, or a per-entry TTL elapsed. See
  `doc/optimistic-overlay.md` for the full consistency model.

  Usage:

    (require '[datahike.optimistic :as opt])

    (opt/register! conn
      {:ttl-ms 30000
       :on-conflict (fn [conflicts] ...)})
    (opt/listen!   conn ::ui
                   (fn [{:keys [tx-data db-after origin]}]
                     (apply-deltas-or-rerender ...)))
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
            [replikativ.logging :as log]
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
  view when the durable transact failed / TTL fired / a conflict appeared.

  Both directions matter: if the original tx-data was a retract (e.g.
  `[[:db/retract 1 :name \"old\"]]`), the `:overlay-add` event shipped
  the retract to the consumer; rolling it back means re-asserting."
  [datoms]
  (mapv (fn [d]
          (dd/datom (.-e d) (.-a d) (.-v d) (dd/datom-tx d)
                    (not (dd/datom-added d))))
        datoms))

;; -----------------------------------------------------------------------------
;; Writer-tx cache (ov-id → tx-data)
;; -----------------------------------------------------------------------------
;;
;; Cache is keyed by the per-entry `:ov-id`, NOT by `:max-tx`. The
;; writer can batch multiple transactions into one commit; in a batch
;; of N, all N tx-reports get the *same* `:max-tx` (the writer
;; overwrites `:db-after` with the batch's commit-db before delivering
;; — see `writer.cljc` commit-loop). Keying by max-tx would collide
;; across batched entries. Keying by ov-id keeps each entry's writer-
;; tx-data distinct.
;;
;; Each call to `d/transact!` returns its own per-call promise that
;; yields THIS specific tx's tx-report — even when the writer batched
;; the commits internally. We cache the writer's `:tx-data` keyed by
;; `:ov-id` inside `transact!`'s go-block, where both pieces of
;; information are naturally in scope when the per-call promise resolves.

(defn- cache-writer-tx!
  [cache ov-id tx-data]
  (when ov-id
    (swap! cache assoc ov-id tx-data)))

(defn- take-writer-tx!
  "Pop the cached writer tx-data for `ov-id`, removing it from the
  cache. Returns nil if absent (e.g. foreign `d/transact!`s that
  bypassed our overlay)."
  [cache ov-id]
  (when ov-id
    (let [[old _] (swap-vals! cache dissoc ov-id)]
      (get old ov-id))))

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
  no listeners are registered."
  [conn report]
  (when-let [{:keys [listeners]} (conn-state conn)]
    (when (seq @listeners)
      (doseq [[k f] @listeners]
        (try (f report)
             (catch #?(:clj Throwable :cljs :default) e
               (log/error :datahike.optimistic/listener-error
                          {:key k :origin (:origin report) :error e})))))))

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

(defn- recompute-and-emit-conflicts!
  "Recompute effective-db, mark conflicts on overlay entries, fire
  on-conflict callbacks if the conflict set changed, and emit
  conflict-transition tx-reports. Updates `last-effective-db` to the
  current effective view. Does NOT fire `listen!` callbacks (each
  caller emits per-event tx-reports via `emit-tx!`)."
  [conn]
  (when-let [{:keys [overlay on-conflicts last-conflict-ids last-effective-db]}
             (conn-state conn)]
    (let [snapshot @overlay
          {:keys [db conflicts]} (recompute @conn snapshot)
          new-conflict-ids (set (keys conflicts))
          old-conflict-ids @last-conflict-ids
          db-before (or @last-effective-db @conn)]
      (mark-conflicts! overlay conflicts)
      (when (not= new-conflict-ids old-conflict-ids)
        (reset! last-conflict-ids new-conflict-ids)
        (let [sanitized (->> @overlay
                             (filter :conflicting?)
                             (mapv #(dissoc % :result-ch :result-delivered?)))]
          (doseq [[k f] @on-conflicts]
            (try (f sanitized)
                 (catch #?(:clj Throwable :cljs :default) e
                   (log/error :datahike.optimistic/on-conflict-error
                              {:key k :error e})))))
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
;; Catch-up: drop overlay entries whose @conn caught up, emit :overlay-realized
;; -----------------------------------------------------------------------------
;;
;; Two events can cause an entry to "catch up" (= `@conn :max-tx` reaches the
;; entry's `:expected-max-tx`): the dispatch resuming with its watermark
;; (`@conn` already advanced by the writer), or a later `@conn` change
;; (the writer's `reset!` lagged the per-call promise delivery). Both
;; events go through the same code:
;;
;;   * `drop-caught-up!`  — atomic `swap-vals!`; returns only the entries
;;                          this swap actually removed (race-free against a
;;                          concurrent caller — exactly one swap wins per
;;                          entry).
;;   * `emit-overlay-realized!` — for each dropped entry, retract-stale via
;;                                 the cache when present, else full-negate.
;;
;; `try-drop-and-emit-realized!` is the single entry point both triggers call.

(defn- drop-caught-up!
  "Atomically remove entries that have caught up:
   - `:expected-max-tx` is <= `new-max` (writer-dispatched entries), OR
   - `:caught-up-pred` returns truthy for `db` (local-only entries whose
     durable write arrives out-of-band, e.g. a message dispatched through
     an application channel and echoed back via store sync).
  Returns the entries this swap actually removed (race-free: a concurrent
  caller's swap returns the entries IT removed, with no overlap)."
  [overlay new-max db]
  (let [caught-up? (fn [{:keys [expected-max-tx caught-up-pred]}]
                     (or (and expected-max-tx new-max (>= new-max expected-max-tx))
                         (and caught-up-pred db
                              (try (boolean (caught-up-pred db))
                                   (catch #?(:clj Throwable :cljs :default) _ false)))))
        [old _new] (swap-vals! overlay
                               (fn [v] (filterv (complement caught-up?) v)))]
    (filterv caught-up? old)))

(defn- emit-overlay-realized!
  "Emit one `:overlay-realized` tx-report per dropped entry.

  `retract-stale` is accurate for both matching-EID (returns empty →
  event suppressed) and EID-shift (returns only the stale prediction
  datoms). For entries whose writer-tx-data was never cached (foreign
  `d/transact!`s that bypassed our overlay), fall back to full-negate
  as best-effort cleanup."
  [conn dropped db-before db-after]
  (when-let [{:keys [writer-tx-cache]} (conn-state conn)]
    (doseq [e dropped]
      (let [server-tx-data (take-writer-tx! writer-tx-cache (:ov-id e))
            retracts (if server-tx-data
                       (retract-stale (:predicted-tx-data e) server-tx-data)
                       (negate (:predicted-tx-data e)))]
        (when (seq retracts)
          (emit-tx! conn {:db-before db-before
                          :db-after  db-after
                          :tx-data   retracts
                          :origin    :overlay-realized
                          :ov-id     (:ov-id e)}))))))

(defn- try-drop-and-emit-realized!
  "Single entry point for both the watcher and dispatch triggers. Drops
  any caught-up entries and emits `:overlay-realized` for the entries
  THIS call actually removed."
  [conn]
  (when-let [{:keys [overlay last-effective-db]} (conn-state conn)]
    (let [db-before (or @last-effective-db @conn)
          dropped (drop-caught-up! overlay (:max-tx @conn) @conn)]
      (when (seq dropped)
        (emit-overlay-realized! conn dropped db-before (effective-db conn))))))

(defn- on-conn-advance [conn old-db new-db]
  (when (not= (:max-tx old-db) (:max-tx new-db))
    ;; @conn moved — recompute conflict set, then catch up any entries
    ;; whose `:expected-max-tx` was reached by this advance.
    (recompute-and-emit-conflicts! conn)
    (try-drop-and-emit-realized! conn)))

;; -----------------------------------------------------------------------------
;; TTL heartbeat
;; -----------------------------------------------------------------------------

(def ^:private heartbeat-tick-ms 1000)

(defn- expire-due!
  "Atomically remove entries whose TTL has elapsed; deliver a
  TimeoutException on each one's :result-ch (exactly-once via
  `deliver-result!`); recompute conflict set; emit one :ttl tx-report
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
        (recompute-and-emit-conflicts! conn)
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
               (log/error :datahike.optimistic/heartbeat-error {:error e})))
        (recur)))))

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
                  entries currently un-applicable on top of @conn. Each
                  carries `:ov-id`, `:tx-data`, `:last-conflict-error`,
                  etc.

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
             on-conflicts      (atom (if on-conflict
                                       {::default-on-conflict on-conflict}
                                       {}))
             last-conflict-ids (atom #{})
             ;; Seed `last-effective-db` with the current state so the
             ;; very first emitted tx-report has an accurate
             ;; `:db-before` even if a foreign `d/transact!` lands
             ;; before any overlay activity has fired.
             last-effective-db (atom @conn)
             heartbeat-stop    (chan)
             watch-key         (keyword "datahike.optimistic"
                                        (str "watch-" (random-uuid)))
             writer-listen-key (keyword "datahike.optimistic"
                                        (str "writer-" (random-uuid)))
             ;; Cache of writer's tx-data per `:ov-id`, populated by
             ;; `transact!`'s go-block when its per-call promise
             ;; resolves (ov-id is in scope there) and consumed by
             ;; `emit-overlay-realized!` to compose correct
             ;; `:overlay-realized` stale-retracts. Bounded by the
             ;; number of in-flight optimistic transacts — each ov-id
             ;; is consumed exactly once on its drop.
             writer-tx-cache   (atom {})]
         (swap! *state assoc conn
                {:overlay           overlay
                 :listeners         listeners
                 :on-conflicts      on-conflicts
                 :last-conflict-ids last-conflict-ids
                 :last-effective-db last-effective-db
                 :writer-tx-cache   writer-tx-cache
                 :heartbeat-stop    heartbeat-stop
                 :ttl-ms            ttl-ms
                 :watch-key         watch-key
                 :writer-listen-key writer-listen-key})
         ;; Emit :conn-advance for every successful d/transact!
         ;; through this conn — our own AND foreign — so consumers
         ;; stay in sync with all durable changes. The writer-tx-cache
         ;; is populated by `transact!`'s go-block (where ov-id is in
         ;; scope), not here.
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
  "Register a tx-report listener `(fn [tx-report])` under key `k`.
  Fires once per logical change to the overlay or @conn, with a
  Datahike-style tx-report:

    {:db-before <effective-db at the start of the event>
     :db-after  <effective-db at the end>
     :tx-data   <vector of #datahike/Datom [e a v t added?]>
     :origin    :overlay-add | :conn-advance | :overlay-realized
              | :overlay-drop | :overlay-conflict | :overlay-resolve
              | :ttl
     :ov-id     <uuid, only for entry-scoped origins>}

  The `:tx-data` is the *delta* a consumer should apply to their
  derived view to bring it in line with `:db-after`. For successful
  optimistic writes, an `:overlay-add` event ships the predicted
  datoms; a follow-up `:conn-advance` event ships the durable
  writer's datoms; and an `:overlay-realized` event ships any
  retracts of stale-EID predictions (the EID-shift case — empty
  and so suppressed when predicted EIDs already matched the
  writer's, the common case under stable-attribute identity). On
  failure / TTL / conflict, the prediction's additions are emitted
  as retracts so a consumer applying tx-data incrementally always
  converges to `@conn`.

  For UIs that just re-render from a db value, ignore `:tx-data`
  and use `:db-after` directly.

  Foreign-peer writes that bypass `d/transact!` do not currently
  emit `:conn-advance` tx-reports (v1 limit)."
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
  Fires whenever the set of conflicting entries changes — useful for
  one-off UX hooks like 'show a toast when conflicts exist'.

  This is *not* a tx-report stream — for tx-data deltas on conflict
  transitions, use `listen!` and filter for the `:overlay-conflict`
  and `:overlay-resolve` origins."
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

(defn transact-local!
  "Overlay-only optimistic transact — NO writer dispatch.

  For writes whose durable path is an APPLICATION channel rather than
  this conn's writer (e.g. a chat message dispatched through a discourse
  room server-side and echoed back into this replica via store sync).
  The entry renders immediately through `effective-db` exactly like
  `transact!`, and reconciles when `caught-up-pred` (a predicate over
  the advancing durable db, e.g. entity-exists-by-uuid) turns truthy on
  a conn advance. TTL applies as usual: if the echo never arrives the
  entry expires and its `:result` channel yields a TimeoutException —
  surface that as a delivery failure.

  Returns `{:ov-id uuid :result <chan>}`."
  ([conn tx-data caught-up-pred] (transact-local! conn tx-data caught-up-pred {}))
  ([conn tx-data caught-up-pred opts]
   (let [{:keys [overlay] :as st}
         (or (conn-state conn)
             (throw (ex-info "Conn not registered for optimistic updates"
                             {:conn conn})))
         db-before-add     (effective-db conn)
         validate-report   (d/with db-before-add tx-data)
         predicted-tx-data (vec (:tx-data validate-report))
         ov-id             (random-uuid)
         submitted-at      (now-ms)
         ttl-ms            (if (contains? opts :ttl-ms) (:ttl-ms opts) (:ttl-ms st))
         expires-at        (when ttl-ms (+ submitted-at ttl-ms))
         result-ch         (chan 1)
         entry             {:ov-id               ov-id
                            :tx-data             tx-data
                            :predicted-tx-data   predicted-tx-data
                            :submitted-at        submitted-at
                            :expires-at          expires-at
                            :expected-max-tx     nil
                            :caught-up-pred      caught-up-pred
                            :conflicting?        false
                            :last-conflict-error nil
                            :result-ch           result-ch
                            :result-delivered?   (atom false)}]
     (swap! overlay conj entry)
     (recompute-and-emit-conflicts! conn)
     (emit-tx! conn {:db-before db-before-add
                     :db-after  (effective-db conn)
                     :tx-data   predicted-tx-data
                     :tempids   (:tempids validate-report)
                     :tx-meta   (:tx-meta validate-report)
                     :origin    :overlay-add
                     :ov-id     ov-id})
     ;; The durable write may ALREADY be visible (echo raced the add).
     (try-drop-and-emit-realized! conn)
     {:ov-id ov-id :result result-ch})))

(defn transact!
  "Optimistic transact.

  Eagerly validates `tx-data` via `d/with` against the current
  effective-db; on validation failure throws synchronously and the
  overlay is untouched. Otherwise appends a pending entry, emits an
  `:overlay-add` tx-report to listeners, and routes the durable write
  through the conn's writer (`d/transact!`) in the background.

  Returns `{:ov-id uuid :result <chan>}`:
   - `:ov-id`  — client-minted UUID identifying this entry.
   - `:result` — 1-buffer channel; yields the writer's tx-report on
                 success or a `Throwable`/`js/Error` on failure
                 (validation, server rejection, TTL timeout, or
                 `unregister!` while pending).

  Pass `:ttl-ms` to override the conn's default TTL for this call;
  pass `:ttl-ms nil` to disable the TTL for this call."
  ([conn tx-data] (transact! conn tx-data {}))
  ([conn tx-data opts]
   (let [{:keys [overlay writer-tx-cache] :as st}
         (or (conn-state conn)
             (throw (ex-info "Conn not registered for optimistic updates"
                             {:conn conn})))
         ;; Eager validation against the current effective-db — throws
         ;; on schema/type violations, nothing enters the overlay. Reuse
         ;; the result to cache the predicted tx-data shipped to
         ;; listeners on :overlay-add.
         db-before-add     (effective-db conn)
         validate-report   (d/with db-before-add tx-data)
         predicted-tx-data (vec (:tx-data validate-report))
         ov-id             (random-uuid)
         submitted-at      (now-ms)
         ttl-ms            (if (contains? opts :ttl-ms) (:ttl-ms opts) (:ttl-ms st))
         expires-at        (when ttl-ms (+ submitted-at ttl-ms))
         result-ch         (chan 1)
         entry             {:ov-id               ov-id
                            :tx-data             tx-data
                            :predicted-tx-data   predicted-tx-data
                            :submitted-at        submitted-at
                            :expires-at          expires-at
                            :expected-max-tx     nil
                            :conflicting?        false
                            :last-conflict-error nil
                            :result-ch           result-ch
                            :result-delivered?   (atom false)}
         ;; Test-only escape hatch: a nullary fn returning a channel
         ;; that yields either `{:reply X :max-tx N}` on success or a
         ;; Throwable / js/Error on failure. NOT part of the public
         ;; contract — used by the test suite to gate dispatch timing
         ;; for race / TTL / conflict scenarios.
         test-dispatch     (::dispatch-fn opts)]
     (swap! overlay conj entry)
     (recompute-and-emit-conflicts! conn)
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
         (let [[reply max-tx]
               (if test-dispatch
                 (let [v (<?- (test-dispatch))]
                   [(:reply v) (:max-tx v)])
                 (let [report (<?- (d/transact! conn tx-data))]
                   (cache-writer-tx! writer-tx-cache ov-id (vec (:tx-data report)))
                   [report (:max-tx (:db-after report))]))]
           ;; Stamp the watermark and let the single drop+emit path
           ;; handle the rest. On the happy path @conn has already
           ;; caught up (the writer's `reset!` ran before our promise
           ;; delivered), so `try-drop-and-emit-realized!` drops the
           ;; entry here. Otherwise it stays until the watcher's
           ;; `on-conn-advance` triggers the same path on the next
           ;; @conn change.
           (swap! overlay
                  (fn [v]
                    (mapv (fn [e]
                            (if (= (:ov-id e) ov-id)
                              (assoc e :expected-max-tx max-tx)
                              e))
                          v)))
           (try-drop-and-emit-realized! conn)
           (deliver-result! entry reply))
         (catch #?(:clj Throwable :cljs :default) e
           ;; Drop the entry if still present; recompute conflicts;
           ;; emit :overlay-drop tx-report retracting predictions;
           ;; deliver the error.
           (let [db-before-drop (effective-db conn)
                 [old new] (swap-vals! overlay
                                       #(filterv (fn [x] (not= (:ov-id x) ov-id)) %))
                 dropped-entry (first (filter #(= (:ov-id %) ov-id) old))]
             (when (not= (count old) (count new))
               (recompute-and-emit-conflicts! conn)
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
