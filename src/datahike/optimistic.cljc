(ns datahike.optimistic
  "Optimistic-overlay helpers over a Datahike connection.

  Layers pending client-submitted transactions on top of the durable @conn
  value via `d/db-with`, exposing the result as `effective-db`. Pending
  entries are dropped automatically when @conn advances past their
  expected max-tx, and on transact failure they are dropped immediately
  with listeners notified.

  Usage:

    (require '[datahike.optimistic :as opt])

    (opt/register! conn)
    (opt/listen! conn ::ui (fn [eff-db] (rerender! eff-db)))
    (opt/transact! conn [{:db/id -1 :name \"alice\"}])
    ...
    (opt/unlisten! conn ::ui)
    (opt/unregister! conn)

  Identity assumption: applications should identify entities by a stable
  attribute (e.g. :entity/uuid minted client-side) rather than by EID,
  since tempid resolution may differ between the overlay's `d/with` and
  the underlying transact."
  (:require [datahike.api :as d]
            #?(:clj  [clojure.core.async :as a :refer [chan put!]]
               :cljs [clojure.core.async :as a :refer [chan put!]
                      :refer-macros [go]])))

;; -----------------------------------------------------------------------------
;; Per-connection state
;; -----------------------------------------------------------------------------

(defonce ^:private *state (atom {}))

(defn- conn-state [conn]
  (get @*state conn))

;; -----------------------------------------------------------------------------
;; Effective DB
;; -----------------------------------------------------------------------------

(defn- effective-db*
  "Reduce overlay entries with `d/db-with` over base-db. Entries marked
  `:transacting?` are skipped — those have already been handed to the
  underlying writer and are about to land in @conn, so re-applying them
  would double their datoms in the view."
  [base-db overlay-vec]
  (reduce (fn [db {:keys [tx-data]}]
            (:db-after (d/with db tx-data)))
          base-db
          (remove :transacting? overlay-vec)))

(defn effective-db
  "Return the db value with all pending optimistic entries applied on top.
  Returns @conn unchanged if conn is not registered."
  [conn]
  (if-let [{:keys [overlay]} (conn-state conn)]
    (effective-db* @conn @overlay)
    @conn))

(defn pending
  "Return a vector of pending overlay entries for conn (empty if none /
  not registered)."
  [conn]
  (if-let [{:keys [overlay]} (conn-state conn)]
    @overlay
    []))

;; -----------------------------------------------------------------------------
;; Listeners
;; -----------------------------------------------------------------------------

(defn- fire-listeners! [conn]
  (when-let [{:keys [listeners]} (conn-state conn)]
    (let [eff-db (try
                   (effective-db conn)
                   (catch #?(:clj Throwable :cljs :default) e
                     ;; Overlay re-application failed against current @conn.
                     ;; Fall back to base; UI may temporarily lose optimistic
                     ;; visibility for entries that became invalid.
                     (println "datahike.optimistic: effective-db failed, falling back:" e)
                     @conn))]
      (doseq [[_ f] @listeners]
        (try
          (f eff-db)
          (catch #?(:clj Throwable :cljs :default) e
            (println "datahike.optimistic listener error:" e)))))))

(defn- on-conn-advance [conn old-db new-db]
  (when (not= (:max-tx old-db) (:max-tx new-db))
    ;; @conn changed — UI needs the new effective-db. We do *not* drop
    ;; pending entries here: matching is by `:ov-id` and is handled in
    ;; `transact!` itself (success and failure paths). Predicted max-tx
    ;; would be unreliable under concurrent writers, so we don't try.
    (fire-listeners! conn)))

;; -----------------------------------------------------------------------------
;; Public API
;; -----------------------------------------------------------------------------

(defn register!
  "Register a conn for optimistic updates. Idempotent. Returns conn."
  [conn]
  (when-not (conn-state conn)
    (let [overlay   (atom [])
          listeners (atom {})
          watch-key (keyword "datahike.optimistic"
                             (str "watch-" (random-uuid)))]
      (swap! *state assoc conn
             {:overlay overlay :listeners listeners :watch-key watch-key})
      (add-watch conn watch-key
                 (fn [_ _ old new] (on-conn-advance conn old new)))))
  conn)

(defn unregister!
  "Detach overlay from conn. Drops all pending entries (no rollback fired)."
  [conn]
  (when-let [{:keys [watch-key]} (conn-state conn)]
    (remove-watch conn watch-key)
    (swap! *state dissoc conn))
  nil)

(defn listen!
  "Register a listener `f` of one arg (effective-db) under key `k`.
  The listener fires whenever @conn advances or the overlay changes
  (entry added on transact!, entry dropped on conn-advance or rejection)."
  [conn k f]
  (when-not (conn-state conn)
    (throw (ex-info "Conn not registered for optimistic updates"
                    {:conn conn})))
  (let [{:keys [listeners]} (conn-state conn)]
    (swap! listeners assoc k f))
  nil)

(defn unlisten!
  [conn k]
  (when-let [{:keys [listeners]} (conn-state conn)]
    (swap! listeners dissoc k))
  nil)

(defn transact!
  "Optimistic transact.

  Eagerly validates `tx-data` via `d/with`, appends an overlay entry,
  fires listeners with the optimistic effective-db, then dispatches the
  write. Validation errors throw synchronously (overlay untouched);
  dispatch errors are delivered on the `:result` channel and the entry
  is dropped before listeners re-fire with the rolled-back effective-db.

  Returns `{:ov-id uuid :result <chan>}`:
   - `:ov-id`  — client-minted UUID identifying this entry; useful for
                 correlating with listener events.
   - `:result` — a 1-buffer channel that yields exactly one value: the
                 server reply on success, or a Throwable (CLJ) /
                 js/Error (CLJS) on failure.

  Dispatch defaults to `(d/transact! conn tx-data)` — routes through the
  conn's writer (`:self`, `:kabel`, …) and yields a Datahike tx-report.

  Pass `:dispatch-fn` in opts to substitute your own RPC — for cases
  where the server side does more than a plain Datahike transact (e.g.
  an `invoke-remote` that creates additional entities, runs server-side
  validation, etc.). Contract:
   - takes no arguments
   - returns a deref-able (CLJ) / `core.async` channel (CLJS) that
     yields the server reply on success or throws/yields-an-error
   - the value it yields is what gets put on `:result` — the wrapper
     does not interpret it."
  ([conn tx-data] (transact! conn tx-data {}))
  ([conn tx-data {:keys [dispatch-fn] :as _opts}]
   (let [{:keys [overlay]} (or (conn-state conn)
                               (throw (ex-info "Conn not registered for optimistic updates"
                                               {:conn conn})))
         ov-id     (random-uuid)
        ;; Eager validation: run `d/with` against the current
        ;; effective-db so malformed/schema-violating tx surfaces
        ;; synchronously before we add anything to the overlay.
        ;; The result is otherwise discarded — entries are matched and
        ;; removed by `:ov-id`, so we don't need the predicted max-tx.
         _validate (d/with (effective-db conn) tx-data) ;; throws on invalid tx
         entry     {:ov-id        ov-id
                    :tx-data      tx-data
                    :status       :pending
                    :submitted-at #?(:clj  (System/currentTimeMillis)
                                     :cljs (.getTime (js/Date.)))}
         result-ch (chan 1)]
     (swap! overlay conj entry)
     (fire-listeners! conn)
    ;; Mark :transacting? right before dispatch so the conn-watch fires
    ;; during d/transact!'s swap! sees effective-db = new-base (without our
    ;; entry being re-applied via d/with on top of datoms that just landed).
     (swap! overlay
            (fn [entries]
              (mapv #(if (= (:ov-id %) ov-id)
                       (assoc % :transacting? true)
                       %)
                    entries)))
     (let [drop-entry! (fn []
                         (swap! overlay
                                (fn [entries]
                                  (filterv (fn [e] (not= (:ov-id e) ov-id))
                                           entries))))]
      ;; Default dispatch: route through the conn's writer (`d/transact!`).
      ;; Custom dispatch is for app-level RPCs whose server side does more
      ;; than a plain Datahike transact (e.g. multi-entity bootstrap).
      ;; In CLJ d/transact! returns a Future-like; in CLJS a promise-chan.
       #?(:clj
          (a/thread
            (try
              (let [reply (if dispatch-fn
                            @(dispatch-fn)
                            @(d/transact! conn tx-data))]
                (drop-entry!) ;; conn-watch already ran fire-listeners! with
                             ;; our :transacting? entry skipped — effective
                             ;; -db is already correct; no fire needed here.
                (put! result-ch reply))
              (catch Throwable e
                (drop-entry!)
                (fire-listeners! conn)
                (put! result-ch e))))
          :cljs
          (a/go
            (let [reply (try
                          (a/<! (if dispatch-fn
                                  (dispatch-fn)
                                  (d/transact! conn tx-data)))
                          (catch :default e e))]
              (drop-entry!)
              (when (instance? js/Error reply)
                (fire-listeners! conn))
              (put! result-ch reply)))))
     {:ov-id ov-id :result result-ch})))
