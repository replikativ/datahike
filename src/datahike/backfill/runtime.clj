(ns ^:no-doc datahike.backfill.runtime
  "Owned process-local AVET capture. Call staging only after predicates accept;
   call committed! once after a successful durable commit with original reports.
   No worker launch, schema activation, recovery or GC protection lives here."
  (:require [datahike.backfill.journal :as journal]
            [replikativ.logging :as log]))

(def ^:private safe-ops
  '#{transact! begin-avet-build! cancel-avet-build! try-install-avet-build!})

(defn- fail! [type message]
  (throw (ex-info message {:type type})))

(defn create!
  "Create an owner without I/O. Complete transactions must fit one journal frame.
   Context and reader limits bound queued generations and retained read leases."
  [options]
  (when-not (or (nil? options) (map? options))
    (fail! ::invalid-options "Runtime options must be a map."))
  (when (some #(not (contains? #{:journal :max-contexts :max-readers} %)) (keys options))
    (fail! ::invalid-options "Unknown runtime option."))
  (let [options (merge {:max-contexts 8 :max-readers 8} options)]
    (doseq [limit (map options [:max-contexts :max-readers])]
      (when-not (and (integer? limit) (<= 1 limit 1024))
        (fail! ::invalid-options "Runtime limits must be integers from 1 to 1024.")))
    {:options (assoc options :journal (journal/validate-options! (:journal options)))
     :state (atom {:closed? false :contexts {} :leases {}})}))

(defn- open! [state]
  (when (:closed? @state) (fail! ::closed "AVET runtime is closed.")))

(defn- cursor [id descriptor sequence]
  {:generation id :journal-id (:id descriptor) :offset (:end descriptor) :sequence sequence})

(defn- cleanup! [state]
  (doseq [[id context] (:contexts @state)
          :when (and (:retired? context)
                     (not-any? #(= id (:generation %)) (vals (:leases @state))))]
    (try (journal/dispose! (:descriptor context))
         (swap! state update :contexts dissoc id)
         (catch Exception e
           (log/warn :datahike/avet-journal-cleanup-failed
                     {:generation id :message (ex-message e)})))))

(defn normalize-report
  "Pure proposed durable retirement, before predicates. Reading local ownership
   does not mutate it or the source DB; rejected reports retire nothing."
  [owner report op]
  (if-not (:db-after report)
    report
    (let [after (:db-after report)
          id (get-in after [:avet-build :id])
          context (get-in @(:state owner) [:contexts id])
          lost? (and id (= id (get-in report [:db-before :avet-build :id]))
                     (or (not context) (:retired? context)))
          invalid? (and id (or lost? (:avet-build-invalidated? after)
                               (not (contains? safe-ops op))))]
      (cond
        invalid? (update report :db-after
                         #(-> %
                              (dissoc :avet-build :avet-build-effects :avet-build-invalidated?
                                      :avet-build-journal ::lost-lineage?)
                              (assoc :avet-build-result
                                     {:id id :status :failed
                                      :reason (cond
                                                (or lost? (::lost-lineage? after)) :owner-lost
                                                (:avet-build-invalidated? after) :schema-changed
                                                :else :unsupported-operation)})))
        (not id) (update report :db-after dissoc :avet-build-effects :avet-build-invalidated?
                         :avet-build-journal ::lost-lineage?)
        :else report))))

(defn prepare-report!
  "Append one complete transaction, including an empty-effects watermark.
   Does not advance committed state. Unsupported operations or schema taint
   fail the build in the accepted report, not the user's transaction."
  ([owner report op] (prepare-report! owner report op false))
  ([owner report op normalized?]
   (if-not (and (:db-after report)
                (or (get-in report [:db-before :avet-build])
                    (get-in report [:db-after :avet-build])))
     (if (get-in report [:db-after ::lost-lineage?])
       (update report :db-after dissoc ::lost-lineage?)
       report)
     (let [state (:state owner)]
       (locking state
         (open! state)
         (let [proposed (normalize-report owner report op)
               _ (when (and normalized?
                            (not= (select-keys (:db-after report) [:avet-build :avet-build-result])
                                  (select-keys (:db-after proposed) [:avet-build :avet-build-result])))
                   (throw (ex-info "AVET ownership changed after predicate validation; retry transaction."
                                   {:type ::normalization-conflict :retryable? true})))
               report proposed
               after (:db-after report)
               id (get-in after [:avet-build :id])]
           (if-not id
             (update report :db-after dissoc :avet-build-effects :avet-build-invalidated?
                     :avet-build-journal ::lost-lineage?)
             (let [existing (get-in @state [:contexts id])
                   previous (when (= id (get-in report [:db-before :avet-build :id]))
                              (get-in report [:db-before :avet-build-journal]))
                   _ (when (and existing
                                (or (:retired? existing) (not= previous (:staged existing))))
                       (fail! ::stale-generation "Staged AVET lineage is no longer current."))
                   _ (when (and (not existing) previous)
                       (fail! ::stale-generation "AVET journal belongs to another runtime."))
                   _ (when (and (not existing)
                                (= id (get-in report [:db-before :avet-build :id])))
                       (fail! ::stale-generation "A recovered build requires a fresh begin generation."))
                   _ (when (and (not existing)
                                (>= (count (:contexts @state)) (get-in owner [:options :max-contexts])))
                       (fail! ::context-limit "Too many pending AVET generations."))
                   descriptor (or (:descriptor existing) (journal/create! (get-in owner [:options :journal])))
                   sequence (inc (get-in existing [:staged :cursor :sequence] 0))]
               (try
                 (let [base (or (get-in existing [:staged :descriptor]) descriptor)
                       appended (journal/append! base
                                                 [{:kind :transaction :generation id :sequence sequence
                                                   :max-tx (:max-tx after)
                                                   :effects (get after :avet-build-effects [])}])
                       staged {:descriptor appended :cursor (cursor id appended sequence)}]
                   (swap! state assoc-in [:contexts id]
                          (assoc (or existing {:descriptor descriptor :retired? false :started? false})
                                 :staged staged))
                   (-> report
                       (update :db-after dissoc :avet-build-effects :avet-build-invalidated?)
                       (assoc-in [:db-after :avet-build-journal] staged)))
                 (catch Throwable e
                   (when-not existing
                     (try (journal/dispose! descriptor)
                          (catch Throwable cleanup-error
                           ;; Failed staging still owns the file. Keep it counted
                           ;; until a later cleanup can actually remove it.
                            (swap! state assoc-in [:contexts id]
                                   {:descriptor descriptor :retired? true :started? false})
                            (.addSuppressed e cleanup-error))))
                   (throw e)))))))))))

(defn committed!
  "Publish one durable group's final prefix; original reports identify retired
   generations without retiring newer speculative generations queued afterward.
   Returns a new worker's exact source DB/cursor, or nil. Never launches workers."
  [owner final-db original-reports]
  (let [state (:state owner)]
    (locking state
      (when-not (:closed? @state)
        (let [id (get-in final-db [:avet-build :id])
              prefix (:avet-build-journal final-db)
              context (get-in @state [:contexts id])
              touched (into #{} (keep identity)
                            (mapcat (fn [report]
                                      [(get-in report [:db-before :avet-build :id])
                                       (get-in report [:db-after :avet-build :id])])
                                    original-reports))
              start? (and context (not (:retired? context)) (not (:started? context)))]
          ;; A byte offset and sequence must describe the SAME accepted frame
          ;; boundary, not merely fit independently inside the staged ranges.
          ;; The group's original final report is the bounded authority for the
          ;; published prefix. Empty groups may retry cleanup, not advance it.
          (when (and (seq original-reports)
                     (let [last-db (:db-after (last original-reports))]
                       (or (not= (:avet-build final-db) (:avet-build last-db))
                           (not= prefix (:avet-build-journal last-db)))))
            (fail! ::invalid-commit "Final AVET state differs from the group's last accepted report."))
          (when (and (not id) prefix)
            (fail! ::invalid-commit "An inactive AVET generation cannot publish a journal prefix."))
          (when id
            (when (or (not context) (:retired? context))
              (fail! ::invalid-commit "Committed AVET generation has no live owned context."))
            (when (and (empty? original-reports) (not= prefix (:committed context)))
              (fail! ::invalid-commit "An empty commit group cannot advance an AVET prefix."))
            (when-not (and (= id (get-in prefix [:cursor :generation]))
                           (= (get-in context [:descriptor :id]) (get-in prefix [:cursor :journal-id]))
                           (= (dissoc (:descriptor context) :end) (dissoc (:descriptor prefix) :end))
                           (= (get-in prefix [:descriptor :end]) (get-in prefix [:cursor :offset]))
                           (integer? (get-in prefix [:cursor :sequence]))
                           (<= (get-in context [:committed :cursor :sequence] 0)
                               (get-in prefix [:cursor :sequence])
                               (get-in context [:staged :cursor :sequence]))
                           (<= (get-in context [:committed :cursor :offset] 0)
                               (get-in prefix [:cursor :offset] -1)
                               (get-in context [:staged :cursor :offset])))
              (fail! ::invalid-commit "Committed AVET prefix does not belong to this lineage.")))
          ;; Validate the entire publication before retiring any old context.
          (doseq [retired (disj touched id)]
            (when (get-in @state [:contexts retired])
              (swap! state assoc-in [:contexts retired :retired?] true)))
          (when id
            (swap! state update-in [:contexts id] assoc :committed prefix
                   :committed-db final-db :started? true))
          (cleanup! state)
          (when start? {:generation id :source-db final-db :cursor (:cursor prefix)}))))))

(defn acquire-prefix!
  "Lease the current committed prefix. Release in finally after use; the caller
   must not concurrently release a lease while its range reader is using it."
  [owner id]
  (let [state (:state owner)]
    (locking state
      (open! state)
      (let [context (get-in @state [:contexts id])]
        (when (or (not context) (:retired? context) (not (:committed context)))
          (fail! ::unavailable "No live committed AVET prefix."))
        (when (>= (count (:leases @state)) (get-in owner [:options :max-readers]))
          (fail! ::reader-limit "Too many AVET readers."))
        (let [lease (assoc (:committed context) :id (random-uuid) :generation id
                           :source-db (:committed-db context))]
          (swap! state assoc-in [:leases (:id lease)] lease)
          lease)))))

(defn release-prefix! [owner lease]
  (let [state (:state owner)]
    (locking state
      (when-let [held (get-in @state [:leases (:id lease)])]
        (when-not (= held lease) (fail! ::invalid-lease "Modified AVET read lease."))
        (swap! state update :leases dissoc (:id lease))
        (cleanup! state)))))

(defn reduce-prefix
  "Read a leased range. f receives accumulator, complete transaction and next
   cursor. The start must be a previously supplied cursor (or offset/sequence 0
   with the same generation/journal identity). No runtime lock is held during I/O."
  [owner lease start f init]
  (let [state (:state owner)]
    (locking state
      (when-not (= lease (get-in @state [:leases (:id lease)]))
        (fail! ::invalid-lease "Unknown AVET read lease.")))
    (when-not (and (= (select-keys start [:generation :journal-id])
                      (select-keys (:cursor lease) [:generation :journal-id]))
                   (integer? (:offset start)) (integer? (:sequence start))
                   (<= 0 (:offset start) (get-in lease [:cursor :offset]))
                   (<= 0 (:sequence start) (get-in lease [:cursor :sequence]))
                   (or (< (:offset start) (get-in lease [:cursor :offset]))
                       (= (:sequence start) (get-in lease [:cursor :sequence]))))
      (fail! ::invalid-cursor "AVET range cursor has the wrong identity or bounds."))
    (let [sequence (volatile! (:sequence start))]
      (journal/reduce-range (:descriptor lease) (:offset start)
                            (fn [acc transaction end]
                              (when-not (and (= :transaction (:kind transaction))
                                             (= (:generation lease) (:generation transaction))
                                             (= (inc @sequence) (:sequence transaction)))
                                (fail! ::invalid-cursor "AVET transaction sequence is not contiguous."))
                              (vreset! sequence (:sequence transaction))
                              (f acc transaction (assoc (:cursor lease) :offset end :sequence @sequence)))
                            init))))

(defn invalidate!
  "Retire every local lineage after a definite commit conflict or lost owner.
   Does not close the runtime: fresh begin generations may subsequently stage.
   Existing read leases retain only their old committed prefixes until released.
   The orchestrator must also retire the durable descriptor through an accepted
   report; it must not copy this runtime's journal onto a reloaded foreign head."
  [owner]
  (let [state (:state owner)]
    (locking state
      (open! state)
      (let [ids (set (keys (:contexts @state)))]
        (swap! state update :contexts
               #(into {} (map (fn [[id context]] [id (assoc context :retired? true)])) %))
        (cleanup! state)
        ids))))

(defn fail-generation!
  "Retire only the named local generation when its internal cancellation could
   not be accepted. No I/O or durable mutation: the next accepted transaction
   reconciles the lost lineage through ordinary predicates. Cleanup remains
   owned by committed!, lease release, reconciliation, or close!."
  [owner id reason]
  (when-not (and (uuid? id) (contains? #{:cancel-rejected :dispatch-failed} reason))
    (fail! ::invalid-failure "Invalid local generation failure."))
  (let [state (:state owner)]
    (locking state
      (when (get-in @state [:contexts id])
        (swap! state #(-> %
                          (assoc-in [:contexts id :retired?] true)
                          (assoc :last-failure {:generation id :reason reason}))))))
  nil)

(defn reconcile-db!
  "Reconcile ONLY an authoritative head after a shared batch has drained.
   Retire foreign/recovered/conflicted local lineage without rejecting ordinary
   user transactions. The taint becomes durable cancellation only if the next
   report is accepted; a rejected predicate never publishes a metadata change."
  [owner database]
  (let [state (:state owner)]
    (locking state
      (open! state)
      (let [id (get-in database [:avet-build :id])
            context (get-in @state [:contexts id])
            had-live? (some #(not (:retired? %)) (vals (:contexts @state)))
            committed (:committed-db context)
            same? (and id context (not (:retired? context))
                       (= (:avet-build-journal database) (:committed context))
                       (= (:staged context) (:committed context))
                       (= (get-in database [:meta :datahike/commit-id])
                          (get-in committed [:meta :datahike/commit-id]))
                       (= (:datahike.writing/head-revision database)
                          (:datahike.writing/head-revision committed))
                       (identical? (:store database) (:store committed))
                       (every? #(= (get database %) (get committed %))
                               [:avet-build :schema :rschema :ident-ref-map :ref-ident-map
                                :config :max-tx :max-eid :op-count :hash :system-entities
                                :secondary-indices :secondary-index-keys])
                       (every? #(identical? (get database %) (get committed %))
                               [:eavt :aevt :avet :temporal-eavt :temporal-aevt :temporal-avet]))]
        (if same?
          database
          (do
            (invalidate! owner)
            (cond-> database
              (or id had-live?) (assoc ::lost-lineage? true)
              id (assoc :avet-build-invalidated? true))))))))

(defn close!
  "Fence staging/reads and retire all contexts. Existing leases remain readable
   until explicitly released; their files must not be unlinked underneath them."
  [owner]
  (let [state (:state owner)]
    (locking state
      (swap! state (fn [s] (-> s (assoc :closed? true)
                               (update :contexts #(into {} (map (fn [[id context]]
                                                                  [id (assoc context :retired? true)])) %)))))
      (cleanup! state))))
