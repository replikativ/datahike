(ns datahike.optimistic
  "Serialized optimistic views over a Datahike connection.

  Overlay events are snapshot transitions, not Datahike TxReports. `:changes`,
  when present, is an exact current-EAV membership difference. A nil value is
  an explicit invalidation: use `:db-after`."
  (:require [clojure.set :as set]
            [clojure.core.async :as a]
            [datahike.api :as d]
            [replikativ.logging :as log]
            [superv.async #?(:clj :refer :cljs :refer-macros) [<?-]]))

(def ^:private empty-queue
  #?(:clj clojure.lang.PersistentQueue/EMPTY
     :cljs cljs.core/PersistentQueue.EMPTY))

(defrecord Overlay [conn state watch-key stop-ch])

(defn overlay? [x] (instance? Overlay x))

(defn- now-ms []
  #?(:clj (System/currentTimeMillis)
     :cljs (.getTime (js/Date.))))

(defn- non-negative-number? [x]
  (and (number? x) (not (neg? x))))

(defn- validate-open-options! [opts]
  (doseq [key [:max-pending :max-queue]
          :let [value (get opts key)]
          :when (contains? opts key)]
    (when-not (and (integer? value) (not (neg? value)))
      (throw (ex-info (str key " must be a non-negative integer")
                      {:type :optimistic/invalid-option :option key :value value}))))
  (doseq [key [:prediction-timeout-ms :reconciliation-timeout-ms]
          :let [value (get opts key)]
          :when (contains? opts key)]
    (when-not (or (nil? value) (non-negative-number? value))
      (throw (ex-info (str key " must be nil or a non-negative number")
                      {:type :optimistic/invalid-option :option key :value value}))))
  opts)

(defn- key-eav [datom]
  [(.-e datom) (.-a datom) (.-v datom)])

(defn- with-entry
  "Apply either a transaction collection or a Datahike transaction arg-map.
  On replay, preserve the metadata (especially :db/txInstant) minted by the
  entry's first successful application."
  [db tx-data overlay-tx-meta]
  (d/with db
          (cond-> (if (map? tx-data) tx-data {:tx-data tx-data})
            overlay-tx-meta (assoc :tx-meta overlay-tx-meta))))

(defn- replay
  "Replay entries in order and retain the tx-data applied to this exact base."
  [base entries]
  (reduce
   (fn [{:keys [db entries conflicts]} entry]
     (try
       (let [report (with-entry db (:tx-data entry) (:overlay-tx-meta entry))
             entry' (assoc entry
                           :applied-tx-data (vec (:tx-data report))
                           ;; Replaying must not mint a fresh txInstant for every
                           ;; transition. Keep the first successful application
                           ;; metadata with the logical overlay entry.
                           :overlay-tx-meta (or (:overlay-tx-meta entry)
                                                (:tx-meta report))
                           :conflicting? false
                           :last-conflict-error nil)]
         {:db (:db-after report)
          :entries (conj entries entry')
          :conflicts conflicts})
       (catch #?(:clj Throwable :cljs :default) error
         {:db db
          :entries (conj entries
                         (assoc entry
                                :applied-tx-data []
                                :conflicting? true
                                :last-conflict-error error))
          :conflicts (assoc conflicts (:ov-id entry) error)})))
   {:db base :entries [] :conflicts {}}
   entries))

(defn- datom-at [db [e a v :as k]]
  (some #(when (= k (key-eav %)) %) (d/datoms db :eavt e a v)))

(defn- applied-keys [entries]
  (into #{} (comp (mapcat :applied-tx-data) (map key-eav)) entries))

(defn- same-snapshot?
  "True when two DB values represent the same Datahike content. Shared writers
  can return a newly stamped DB record when an unchanged fenced head is read,
  so object identity alone is not a durable snapshot identity. Datahike hashes
  are additive and therefore not collision-proof; transaction and entity
  watermarks are part of the snapshot identity as well."
  [a b]
  (or (identical? a b)
      (and (some? (:hash a))
           (= [(:hash a) (:max-tx a) (:max-eid a)]
              [(:hash b) (:max-tx b) (:max-eid b)]))))

(defn- membership-changes [before after candidates]
  (reduce
   (fn [{:keys [added removed] :as out} k]
     (let [b (datom-at before k)
           a (datom-at after k)]
       (cond
         (and b (nil? a)) (assoc out :removed (conj removed b))
         (and a (nil? b)) (assoc out :added (conj added a))
         :else out)))
   {:added [] :removed []}
   candidates))

(defn- public-entry [entry]
  (dissoc entry :result :tx-data :applied-tx-data :reconciled?
          :overlay-tx-meta))

(defn db
  "Return the overlay's committed effective database in O(1)."
  [overlay]
  (get-in @(:state overlay) [:model :effective-db]))

(def effective-db db)

(defn error?
  "True for a tagged overlay result representing rejection. Portable across
  Clojure and ClojureScript; callers do not need runtime-specific Throwable
  checks."
  [result]
  (= :rejected (:status result)))

(defn pending [overlay]
  (mapv public-entry (get-in @(:state overlay) [:model :entries])))

(defn- notify! [kind listeners event]
  (doseq [[key listener] listeners]
    (try
      (listener event)
      (catch #?(:clj Throwable :cljs :default) error
        (log/error kind {:key key :event event :error error})))))

(defn- deliver! [entry result]
  (when-let [ch (:result entry)]
    (a/put! ch result)
    (a/close! ch)))

(defn- status-event [model entry status extra]
  (merge {:revision (:revision model)
          :ov-id (:ov-id entry)
          :kind (:kind entry)
          :status status}
         extra))

(defn- conflict-events [old-model new-model]
  (let [old-ids (set (keys (:conflicts old-model)))
        new-ids (set (keys (:conflicts new-model)))]
    (concat
     (for [entry (:entries new-model)
           :let [id (:ov-id entry)]
           :when (and (new-ids id) (not (old-ids id)))]
       (status-event new-model entry :conflicting
                     {:error (get (:conflicts new-model) id)}))
     (for [entry (:entries new-model)
           :let [id (:ov-id entry)]
           :when (and (old-ids id) (not (new-ids id)))]
       (status-event new-model entry :applicable {})))))

(defn- publish-model!
  [overlay old-model model cause changes?]
  (let [state (:state overlay)
        new-model (assoc model :revision (inc (:revision old-model)))
        base (:base-db new-model)
        db (:effective-db new-model)
        entries (:entries new-model)
        candidates (when changes?
                     (set/union (applied-keys (:entries old-model))
                                (applied-keys entries)))
        changes (when changes?
                  (try
                    (membership-changes (:effective-db old-model) db candidates)
                    (catch #?(:clj Throwable :cljs :default) error
                      ;; Preserve the snapshot transition and downgrade to an
                      ;; honest invalidation if an index lookup cannot produce
                      ;; the bounded diff.
                      (log/error :datahike.optimistic/change-diff-error
                                 {:cause cause :error error})
                      nil)))
        event {:revision (:revision new-model)
               :db-before (:effective-db old-model)
               :db-after db
               :base-max-tx (:max-tx base)
               :cause cause
               :changes changes}
        _ (swap! state assoc :model new-model)
        snapshot @state]
    (notify! :datahike.optimistic/listener-error (:listeners snapshot) event)
    (doseq [status (conflict-events old-model new-model)]
      (notify! :datahike.optimistic/status-listener-error
               (:status-listeners snapshot) status))
    new-model))

(defn- commit-view!
  [overlay base entries cause changes?]
  (let [old-model (:model @(:state overlay))
        {:keys [db entries conflicts]} (replay base entries)]
    (publish-model! overlay old-model
                    {:base-db base
                     :effective-db db
                     :entries entries
                     :conflicts conflicts}
                    cause changes?)))

(defn- find-entry [entries ov-id]
  (some #(when (= ov-id (:ov-id %)) %) entries))

(defn- remove-entry [entries ov-id]
  (filterv #(not= ov-id (:ov-id %)) entries))

(declare enqueue!)

(defn- start-writer-dispatch! [overlay ov-id tx-data]
  (a/go
    (try
      (let [report (<?- (d/transact! (:conn overlay) tx-data))]
        (enqueue! overlay {:op :writer-result :ov-id ov-id :report report}))
      (catch #?(:clj Throwable :cljs :default) error
        (enqueue! overlay {:op :writer-result :ov-id ov-id :error error})))))

(defn- process-submit! [overlay {:keys [kind ov-id tx-data result opts reconciled?]}]
  (let [state (:state overlay)
        old-model (:model @state)
        entries (:entries old-model)]
    (if (>= (count entries) (:max-pending @state))
      (let [entry {:ov-id ov-id :kind kind :result result}
            error (ex-info "Optimistic overlay is full"
                           {:type :optimistic/overloaded
                            :max-pending (:max-pending @state)})]
        (deliver! entry {:status :rejected :error error})
        (notify! :datahike.optimistic/status-listener-error
                 (:status-listeners @state)
                 (status-event old-model entry :rejected {:error error})))
      (try
        (when (and (= kind :prediction) (not (ifn? reconciled?)))
          (throw (ex-info "A prediction requires a reconciliation predicate"
                          {:type :optimistic/invalid-reconciliation-predicate
                           :value reconciled?})))
        (let [report (with-entry (:effective-db old-model) tx-data nil)
              submitted-at (now-ms)
              timeout-ms (when (= kind :prediction)
                           (if (contains? opts :timeout-ms)
                             (:timeout-ms opts)
                             (:prediction-timeout-ms @state)))
              _ (when-not (or (nil? timeout-ms) (non-negative-number? timeout-ms))
                  (throw (ex-info ":timeout-ms must be nil or a non-negative number"
                                  {:type :optimistic/invalid-option
                                   :option :timeout-ms :value timeout-ms})))
              entry {:ov-id ov-id
                     :kind kind
                     :tx-data tx-data
                     :result result
                     :submitted-at submitted-at
                     :expires-at (when timeout-ms (+ submitted-at timeout-ms))
                     :expected-max-tx nil
                     :branch (:branch opts)
                     :acknowledged? false
                     :reconciled? reconciled?
                     :conflicting? false
                     :last-conflict-error nil
                     :overlay-tx-meta (:tx-meta report)
                     :applied-tx-data (vec (:tx-data report))}
              model (publish-model!
                     overlay old-model
                     {:base-db (:base-db old-model)
                      :effective-db (:db-after report)
                      :entries (conj entries entry)
                      :conflicts (:conflicts old-model)}
                     {:type :overlay-added :ov-ids [ov-id]} true)
              committed-entry (find-entry (:entries model) ov-id)]
          (notify! :datahike.optimistic/status-listener-error
                   (:status-listeners @state)
                   (status-event model committed-entry :visible {}))
          (when (= kind :writer)
            (start-writer-dispatch! overlay ov-id tx-data)))
        (catch #?(:clj Throwable :cljs :default) error
          (let [entry {:ov-id ov-id :kind kind :result result}]
            (deliver! entry {:status :rejected :error error})
            (notify! :datahike.optimistic/status-listener-error
                     (:status-listeners @state)
                     (status-event old-model entry :rejected {:error error}))))))))

(defn- reconcile-on-base [entries new-db]
  (reduce
   (fn [{:keys [kept removed errors]} entry]
     (let [writer-caught? (and (= :writer (:kind entry))
                               (:expected-max-tx entry)
                               (:max-tx new-db)
                               (>= (:max-tx new-db) (:expected-max-tx entry)))
           [prediction-caught? predicate-error]
           (if (and (= :prediction (:kind entry)) (:reconciled? entry))
             (try
               [(boolean ((:reconciled? entry) new-db)) nil]
               (catch #?(:clj Throwable :cljs :default) error
                 [false error]))
             [false nil])]
       (if (or writer-caught? prediction-caught?)
         {:kept kept :removed (conj removed entry) :errors errors}
         {:kept (conj kept entry)
          :removed removed
          :errors (cond-> errors predicate-error (conj [entry predicate-error]))})))
   {:kept [] :removed [] :errors []}
   entries))

(defn- process-base! [overlay new-db]
  (let [state (:state overlay)
        old-model (:model @state)]
    (when-not (same-snapshot? (:base-db old-model) new-db)
      (let [{:keys [kept removed errors]} (reconcile-on-base (:entries old-model) new-db)
            model (commit-view! overlay new-db kept
                                {:type :base-advanced
                                 :reconciled-ids (mapv :ov-id removed)} false)]
        (doseq [entry removed
                :when (= :prediction (:kind entry))]
          (deliver! entry {:status :reconciled})
          (notify! :datahike.optimistic/status-listener-error
                   (:status-listeners @state)
                   (status-event model entry :reconciled {})))
        (doseq [[entry error] errors]
          (notify! :datahike.optimistic/status-listener-error
                   (:status-listeners @state)
                   (status-event model entry :reconciliation-error {:error error})))))))

(defn- process-writer-result! [overlay {:keys [ov-id report error]}]
  (let [state (:state overlay)
        old-model (:model @state)
        entry (find-entry (:entries old-model) ov-id)]
    (when entry
      (if (or error (nil? (get-in report [:db-after :max-tx])))
        (let [model (commit-view! overlay (:base-db old-model)
                                  (remove-entry (:entries old-model) ov-id)
                                  {:type :overlay-removed :reason :writer-rejected
                                   :ov-ids [ov-id]} true)
              error (or error
                        (ex-info "Writer returned no commit watermark"
                                 {:type :optimistic/invalid-writer-report
                                  :report report}))]
          (deliver! entry {:status :rejected :error error})
          (notify! :datahike.optimistic/status-listener-error
                   (:status-listeners @state)
                   (status-event model entry :rejected {:error error})))
        (let [expected (get-in report [:db-after :max-tx])
              base (:base-db old-model)
              caught? (and expected (:max-tx base) (>= (:max-tx base) expected))
              entries (if caught?
                        (remove-entry (:entries old-model) ov-id)
                        (mapv #(if (= ov-id (:ov-id %))
                                 (assoc %
                                        :expected-max-tx expected
                                        :acknowledged? true
                                        :reconcile-deadline-at
                                        (when-let [timeout-ms (:reconciliation-timeout-ms @state)]
                                          (+ (now-ms) timeout-ms)))
                                 %)
                              (:entries old-model)))
              model (if caught?
                      (commit-view! overlay base entries
                                    {:type :overlay-removed :reason :writer-committed
                                     :ov-ids [ov-id]} true)
                      (do (swap! state assoc-in [:model :entries] entries)
                          (:model @state)))]
          (deliver! entry {:status :committed :tx-report report})
          (notify! :datahike.optimistic/status-listener-error
                   (:status-listeners @state)
                   (status-event model entry :committed {:max-tx expected})))))))

(defn- process-ack! [overlay ov-id receipt]
  (let [state (:state overlay)
        model (:model @state)
        entry (find-entry (:entries model) ov-id)]
    (when (and entry (= :prediction (:kind entry)) (not (:acknowledged? entry)))
      (swap! state update-in [:model :entries]
             (fn [entries]
               (mapv #(if (= ov-id (:ov-id %))
                        (assoc %
                               :acknowledged? true
                               :expires-at nil
                               :reconcile-deadline-at
                               (when-let [timeout-ms (:reconciliation-timeout-ms @state)]
                                 (+ (now-ms) timeout-ms)))
                        %) entries)))
      (deliver! entry {:status :accepted :receipt receipt})
      (notify! :datahike.optimistic/status-listener-error
               (:status-listeners @state)
               (status-event (:model @state) entry :acknowledged {:receipt receipt})))))

(defn- remove-prediction-with-status! [overlay ov-id status extra]
  (let [state (:state overlay)
        old-model (:model @state)
        entry (find-entry (:entries old-model) ov-id)]
    (when (and entry
               (= :prediction (:kind entry))
               ;; Acceptance is terminal for reject!: a delayed failure
               ;; callback must not retract work already reported accepted.
               ;; abandon! remains the explicit post-acceptance escape hatch.
               (or (= status :abandoned) (not (:acknowledged? entry))))
      (let [model (commit-view! overlay (:base-db old-model)
                                (remove-entry (:entries old-model) ov-id)
                                {:type :overlay-removed :reason status :ov-ids [ov-id]} true)]
        (deliver! entry (merge {:status status} extra))
        (notify! :datahike.optimistic/status-listener-error
                 (:status-listeners @state)
                 (status-event model entry status extra))))))

(defn- process-expire! [overlay now]
  (let [model (:model @(:state overlay))
        expired (filterv #(and (= :prediction (:kind %))
                               (:expires-at %) (<= (:expires-at %) now))
                         (:entries model))
        stalled (filterv #(and (:acknowledged? %)
                               (:reconcile-deadline-at %)
                               (<= (:reconcile-deadline-at %) now))
                         (:entries model))]
    (when (seq expired)
      (let [ids (set (map :ov-id expired))
            kept (filterv #(not (ids (:ov-id %))) (:entries model))
            new-model (commit-view! overlay (:base-db model) kept
                                    {:type :overlay-removed :reason :expired
                                     :ov-ids (mapv :ov-id expired)} true)]
        (doseq [entry expired]
          (deliver! entry {:status :expired :outcome :unknown})
          (notify! :datahike.optimistic/status-listener-error
                   (:status-listeners @(:state overlay))
                   (status-event new-model entry :expired {:outcome :unknown})))))
    ;; Acknowledged work must not be retracted merely because synchronization is
    ;; slow: the durable outcome is already known. Report the stalled
    ;; reconciliation once and keep the prediction visible until the base
    ;; catches up or the owner explicitly abandons it.
    (when (seq stalled)
      (let [ids (set (map :ov-id stalled))]
        (swap! (:state overlay) update-in [:model :entries]
               (fn [entries]
                 (mapv #(if (ids (:ov-id %))
                          (assoc % :reconcile-deadline-at nil :reconciliation-stalled? true)
                          %)
                       entries)))
        (doseq [entry stalled]
          (notify! :datahike.optimistic/status-listener-error
                   (:status-listeners @(:state overlay))
                   (status-event (:model @(:state overlay)) entry
                                 :reconciliation-stalled {})))))))

(defn- process-close! [overlay]
  (let [state (:state overlay)
        ;; Reject commands that were ordered after close. In particular, every
        ;; accepted submit must have its promise channel completed.
        [before-close _] (swap-vals! state assoc :phase :closing
                                     :queue empty-queue :queued-submits 0
                                     :base-queued? false :expire-queued? false)
        queued (:queue before-close)
        model (:model before-close)
        entries (:entries model)
        new-model (if (seq entries)
                    (commit-view! overlay (:base-db model) []
                                  {:type :overlay-closed :ov-ids (mapv :ov-id entries)} true)
                    model)]
    (doseq [entry entries]
      (let [result (cond
                     (and (:acknowledged? entry) (= :writer (:kind entry)))
                     {:status :detached :outcome :committed :reason :overlay-closed}

                     (:acknowledged? entry)
                     {:status :unknown :accepted? true :reason :overlay-closed}

                     (= :writer (:kind entry))
                     {:status :unknown :reason :overlay-closed}

                     :else
                     {:status :abandoned :reason :overlay-closed})]
        (deliver! entry result)
        (notify! :datahike.optimistic/status-listener-error
                 (:status-listeners @state)
                 (status-event new-model entry (:status result) (dissoc result :status)))))
    (doseq [{:keys [op result ov-id kind]} queued
            :when (= :submit op)]
      (let [entry {:ov-id ov-id :kind kind :result result}
            error (ex-info "Overlay closed before submission was processed"
                           {:type :optimistic/closed})]
        (deliver! entry {:status :rejected :error error})
        (notify! :datahike.optimistic/status-listener-error
                 (:status-listeners @state)
                 (status-event new-model entry :rejected {:error error}))))
    (remove-watch (:conn overlay) (:watch-key overlay))
    (a/close! (:stop-ch overlay))
    (swap! state assoc :phase :closed :listeners {} :status-listeners {}
           :queue empty-queue)))

(defn- process-command! [overlay command]
  (case (:op command)
    :submit (process-submit! overlay command)
    ;; A watch callback is only a wake-up signal. Reading the connection in the
    ;; serialized drainer prevents a delayed callback (or the initial resync)
    ;; from replacing a newer base with a stale snapshot.
    :base (process-base! overlay @(:conn overlay))
    :writer-result (process-writer-result! overlay command)
    :ack (process-ack! overlay (:ov-id command) (:receipt command))
    :reject (remove-prediction-with-status! overlay (:ov-id command) :rejected {:error (:error command)})
    :abandon (remove-prediction-with-status! overlay (:ov-id command) :abandoned {:reason (:reason command)})
    :expire (process-expire! overlay (:now command))
    :listen (swap! (:state overlay) assoc-in [:listeners (:key command)] (:listener command))
    :unlisten (swap! (:state overlay) update :listeners dissoc (:key command))
    :listen-status (swap! (:state overlay) assoc-in [:status-listeners (:key command)] (:listener command))
    :unlisten-status (swap! (:state overlay) update :status-listeners dissoc (:key command))
    :close (process-close! overlay)
    nil))

(defn- take-command! [state]
  (let [[before _]
        (swap-vals! state
                    (fn [s]
                      (cond
                        (= :closed (:phase s))
                        (assoc s :queue empty-queue :queued-submits 0
                               :base-queued? false :expire-queued? false
                               :running? false)

                        (seq (:queue s))
                        (let [op (:op (peek (:queue s)))]
                          (cond-> (assoc s :queue (pop (:queue s)))
                            (= :submit op) (update :queued-submits dec)
                            (= :base op) (assoc :base-queued? false)
                            (= :expire op) (assoc :expire-queued? false)))

                        :else
                        (assoc s :running? false))))]
    (when (and (not= :closed (:phase before)) (seq (:queue before)))
      (peek (:queue before)))))

(defn- drain! [overlay]
  (loop []
    (when-let [command (take-command! (:state overlay))]
      (try
        (process-command! overlay command)
        (catch #?(:clj Throwable :cljs :default) error
          (log/error :datahike.optimistic/command-error {:op (:op command) :error error})
          (when-let [result (:result command)]
            (a/put! result {:status :rejected :error error})
            (a/close! result))))
      (recur))))

(defn- enqueue! [overlay command]
  (let [state (:state overlay)
        [before after]
        (swap-vals! state
                    (fn [s]
                      (cond
                        (not= :active (:phase s))
                        s

                        (and (= :base (:op command)) (:base-queued? s))
                        s

                        (and (= :expire (:op command)) (:expire-queued? s))
                        s

                        (and (= :submit (:op command))
                             (>= (:queued-submits s) (:max-queue s)))
                        s

                        :else
                        (-> s
                            (update :queue conj command)
                            (cond-> (= :submit (:op command))
                              (update :queued-submits inc))
                            (cond-> (= :base (:op command))
                              (assoc :base-queued? true))
                            (cond-> (= :expire (:op command))
                              (assoc :expire-queued? true))
                            (assoc :running? true)))))
        outcome (cond
                  (not= :active (:phase before)) :closed
                  (and (= :submit (:op command))
                       (>= (:queued-submits before) (:max-queue before))) :overloaded
                  :else :accepted)
        owner? (and (= :accepted outcome)
                    (not (:running? before))
                    (:running? after))]
    (when owner? (drain! overlay))
    outcome))

(defn open
  "Create an explicit optimistic overlay over `conn`."
  ([conn] (open conn {}))
  ([conn opts]
   (let [opts (validate-open-options! opts)
         base @conn
         state (atom {:phase :active
                      :model {:base-db base :effective-db base :entries []
                              :conflicts {} :revision 0}
                      :listeners {} :status-listeners {}
                      :queue empty-queue :queued-submits 0
                      :base-queued? false :expire-queued? false
                      :running? false
                      :max-pending (if (contains? opts :max-pending)
                                     (:max-pending opts) 1024)
                      :max-queue (if (contains? opts :max-queue)
                                   (:max-queue opts) 1024)
                      :prediction-timeout-ms
                      (if (contains? opts :prediction-timeout-ms)
                        (:prediction-timeout-ms opts) 30000)
                      :reconciliation-timeout-ms
                      (if (contains? opts :reconciliation-timeout-ms)
                        (:reconciliation-timeout-ms opts) 30000)})
         watch-key (keyword "datahike.optimistic" (str "base-" (random-uuid)))
         stop-ch (a/chan)
         overlay (->Overlay conn state watch-key stop-ch)]
     (add-watch conn watch-key
                (fn [_ _ old-db new-db]
                  (when-not (identical? old-db new-db)
                    (enqueue! overlay {:op :base}))))
     (enqueue! overlay {:op :base})
     (a/go-loop []
       (let [[_ port] (a/alts! [stop-ch (a/timeout 1000)])]
         (when-not (= port stop-ch)
           (enqueue! overlay {:op :expire :now (now-ms)})
           (recur))))
     overlay)))

(defn listen! [overlay key listener]
  (when-not (= :accepted (enqueue! overlay {:op :listen :key key :listener listener}))
    (throw (ex-info "Overlay is closed" {:type :optimistic/closed})))
  nil)

(defn unlisten! [overlay key]
  (enqueue! overlay {:op :unlisten :key key}) nil)

(defn listen-status! [overlay key listener]
  (when-not (= :accepted (enqueue! overlay {:op :listen-status :key key :listener listener}))
    (throw (ex-info "Overlay is closed" {:type :optimistic/closed})))
  nil)

(defn unlisten-status! [overlay key]
  (enqueue! overlay {:op :unlisten-status :key key}) nil)

(defn- submit! [overlay kind tx-data reconciled? opts]
  (let [ov-id (random-uuid)
        result (a/promise-chan)
        command {:op :submit :kind kind :ov-id ov-id :tx-data tx-data
                 :reconciled? reconciled? :opts opts :result result}]
    (case (enqueue! overlay command)
      :closed
      (deliver! {:result result}
                {:status :rejected
                 :error (ex-info "Overlay is closed" {:type :optimistic/closed})})

      :overloaded
      (deliver! {:result result}
                {:status :rejected
                 :error (ex-info "Optimistic command queue is full"
                                 {:type :optimistic/overloaded
                                  :max-queue (:max-queue @(:state overlay))})})

      nil)
    {:ov-id ov-id :result result}))

(defn transact!
  "Submit a writer-backed optimistic transaction. No visibility TTL is applied."
  ([overlay tx-data] (transact! overlay tx-data {}))
  ([overlay tx-data opts] (submit! overlay :writer tx-data nil opts)))

(defn predict!
  "Add a prediction whose durable operation is owned externally."
  ([overlay tx-data reconciled?] (predict! overlay tx-data reconciled? {}))
  ([overlay tx-data reconciled? opts]
   (submit! overlay :prediction tx-data reconciled? opts)))

(defn ack!
  ([overlay ov-id] (ack! overlay ov-id nil))
  ([overlay ov-id receipt]
   (enqueue! overlay {:op :ack :ov-id ov-id :receipt receipt}) nil))

(defn reject! [overlay ov-id error]
  (enqueue! overlay {:op :reject :ov-id ov-id :error error}) nil)

(defn abandon!
  ([overlay ov-id] (abandon! overlay ov-id :user))
  ([overlay ov-id reason]
   (enqueue! overlay {:op :abandon :ov-id ov-id :reason reason}) nil))

(defn close!
  "Detach the overlay without claiming to cancel dispatched durable writes."
  [overlay]
  (enqueue! overlay {:op :close}) nil)
