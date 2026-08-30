(ns datahike.index.secondary.proximum
  "Proximum (vector similarity search) integration with Datahike secondary indices.

   Require this namespace to register the :proximum index type:
     (require 'datahike.index.secondary.proximum)

   Then declare in schema:
     {:idx/embeddings {:db.secondary/type :proximum
                       :db.secondary/attrs [:person/embedding]
                       :db.secondary/config {:dim 384 :distance :cosine
                                             :store-config {...}}}}"
  (:require
   [proximum.warm :as prox-warm]
   [datahike.index.audit :as audit]
   [datahike.index.secondary :as sec]
   [datahike.index.entity-set :as es]
   [proximum.core :as prox]
   [proximum.generations :as gen]
   [proximum.hnsw.internal :as phi]
   [replikativ.logging :as log]
   [clojure.core.async :as async]))

(defn- exact-filter-cheaper?
  "Estimate the distance-computation break-even for a filtered top-k.

   An exact filtered scan computes `allowed` distances.  An ANN traversal must
   discover roughly `k / selectivity`, or `k * total / allowed`, nodes before
   it can admit k uniformly distributed matches.  Prefer exact when the former
   is no larger.  This decision uses the runtime EntityBitSet cardinality, so a
   prepared plan adapts to each execution rather than baking in selectivity."
  [prox-idx k entity-filter]
  (let [allowed (long (es/entity-bitset-cardinality entity-filter))
        total (long (prox/count-vectors prox-idx))
        k (long (max 1 (or k 1)))]
    (and (pos? total)
         (<= (* allowed allowed) (* k total)))))

(defn- search-results
  [prox-idx {:keys [vector k ef filter-strategy]} entity-filter]
  (let [filter-strategy (or filter-strategy
                            (when (and entity-filter
                                       (exact-filter-cheaper? prox-idx k entity-filter))
                              :exact))
        opts (cond-> {}
               ef (assoc :ef ef)
               filter-strategy (assoc :filter-strategy filter-strategy))]
    (if entity-filter
      ;; Proximum translates this lazy Roaring64 ID view directly into its
      ;; native internal-id bitset. Do not materialize a second boxed HashSet;
      ;; sparse SQL filters then cost O(matches), while the native bitset is the
      ;; one dense representation HNSW actually consumes.
      (prox/search-filtered prox-idx vector k
                            (es/entity-bitset-seq entity-filter) opts)
      (prox/search prox-idx vector k opts))))

(defn- close-generation! [generation]
  (when generation
    (async/<!! (gen/close-view! generation))))

(defprotocol ^:private IUnpublishedProximumGenerations
  (-take-publication-holds! [index next-state])
  (-reserve-derivation! [index])
  (-release-derivation! [index]))

(defn- take-holds! [holds*]
  (locking holds*
    (let [holds @holds*]
      (reset! holds* [])
      holds)))

(defn- transfer-holds!
  [holds* publication-state* next-state]
  (locking publication-state*
    (case @publication-state*
      :published []
      :unpublished
      (let [holds (take-holds! holds*)]
        (when-not (seq holds)
          (throw (ex-info "An unpublished Proximum generation has no publication hold."
                          {:type :secondary/proximum-missing-publication-hold})))
        (reset! publication-state* next-state)
        holds)
      :deriving
      (if (= :transferred next-state)
        (let [holds (take-holds! holds*)]
          (when-not (seq holds)
            (throw (ex-info "A derived Proximum generation has no publication hold."
                            {:type :secondary/proximum-missing-publication-hold})))
          (reset! publication-state* next-state)
          holds)
        (throw (ex-info "A Proximum derivation can only transfer its publication ownership."
                        {:type :secondary/proximum-publication-owner-conflict
                         :state :deriving
                         :requested-state next-state})))
      (throw (ex-info "A Proximum generation already has an exclusive publication owner."
                      {:type :secondary/proximum-publication-owner-conflict
                       :state @publication-state*
                       :requested-state next-state})))))

(defn- complete-holds! [holds complete!]
  (let [failures
        (reduce (fn [errors hold]
                  (try
                    (complete! hold)
                    errors
                    (catch Throwable failure
                      (conj errors failure))))
                [] holds)]
    (when (seq failures)
      (throw (ex-info "One or more Proximum publication holds failed to close"
                      {:type :secondary/proximum-publication-cleanup-failed
                       :failure-count (count failures)}
                      (first failures))))))

(defn- reserve-derivation!
  [publication-state*]
  (locking publication-state*
    (case @publication-state*
      :published false
      :unpublished (do (reset! publication-state* :deriving) true)
      (throw (ex-info "This Proximum generation already has an exclusive publication owner."
                      {:type :secondary/proximum-publication-owner-conflict
                       :state @publication-state*})))))

(defn- release-derivation!
  [publication-state*]
  (locking publication-state*
    (when (= :deriving @publication-state*)
      (reset! publication-state* :unpublished)
      true)))

(declare make-proximum-index)

(defrecord ProximumPreparation [prepared-index publication-holds owns-prepared?
                                publication-state* release-state]
  sec/IPreparedSecondaryGeneration
  (-sec-generation-index [_] prepared-index)
  (-sec-release [_ outcome]
    (async/thread
      (try
        (locking release-state
          (let [previous @release-state
                status (:status outcome)]
            (cond
              (#{:committed :aborted} previous)
              nil

              (and (= :unknown previous) (= :committed status))
              (do
                (complete-holds! publication-holds gen/root-publication!)
                (reset! publication-state* :published)
                (reset! release-state :committed))

              (and (= :unknown previous) (= :unknown status))
              nil

              (= :committed status)
              (do
                (complete-holds! publication-holds gen/root-publication!)
                (reset! publication-state* :published)
                (reset! release-state :committed))

              (and (= :aborted status) (nil? previous))
              (let [completed? (atom false)]
                (try
                  (complete-holds! publication-holds gen/abort-publication!)
                  (reset! completed? true)
                  (finally
                    (when owns-prepared?
                      (async/<!! (gen/close-view! (:generation prepared-index))))))
                (when @completed?
                  (reset! publication-state* :aborted)
                  (reset! release-state :aborted)))

              (= :unknown status)
              (do
                ;; The head write may still land after returning. Keep every
                ;; publication hold until an authoritative reconciliation says
                ;; committed or definitively aborted. The live mmap is only a
                ;; cache and can be closed; a landed generation restores by id.
                (when owns-prepared?
                  (async/<!! (gen/close-view! (:generation prepared-index))))
                (reset! publication-state* :unknown)
                (reset! release-state :unknown))

              :else
              (throw (ex-info "An ambiguous secondary generation cannot later be aborted."
                              {:type :secondary/ambiguous-generation-abort
                               :previous previous
                               :outcome outcome})))))
        true
        (catch Throwable failure failure)))))

(defn- proximum-view [index]
  (some-> (:generation index) gen/generation-index))

(defn- proximum-key-map-failure-reason [key-map]
  (cond
    ;; The pre-generation adapter stored a native commit/branch pointer. It is
    ;; not the private mmap generation introduced by format 1, even though
    ;; both identifiers happen to be UUIDs.
    (and (= :proximum (:type key-map))
         (contains? key-map :commit-id)
         (not (contains? key-map :generation-id)))
    :legacy-proximum-commit-root

    (not= :proximum (:type key-map)) :wrong-type
    (not= 2 (:format-version key-map)) :unsupported-format-version
    (not= :external (:storage-owner key-map)) :wrong-storage-owner
    (not= :full-mmap-copy (:generation-strategy key-map))
    :unsupported-generation-strategy
    (nil? (:external-store-id key-map)) :invalid-external-store-id
    (not (uuid? (:generation-id key-map))) :invalid-generation-id
    :else nil))

(defn- validate-proximum-generation-key-map [key-map]
  (when-let [reason (proximum-key-map-failure-reason key-map)]
    (throw (ex-info
            "Invalid Proximum generation key-map."
            {:type :secondary/invalid-proximum-generation
             :reason reason
             :key-map key-map
             :expected {:type :proximum
                        :format-version 2
                        :storage-owner :external
                        :generation-strategy :full-mmap-copy
                        :external-store-id :stable-konserve-store-id
                        :generation-id :uuid}})))
  key-map)

(defn- ensure-builder! [builder* source-index generation-config]
  (or @builder*
      (let [builder (if-let [source (proximum-view source-index)]
                      (gen/begin-generation source)
                      (gen/begin-generation-from-config generation-config))]
        (reset! builder* builder)
        builder)))

(defn- take-pending-inserts! [pending-inserts*]
  (locking pending-inserts*
    (let [pending @pending-inserts*]
      (reset! pending-inserts* [])
      pending)))

(defn- flush-pending-inserts!
  [builder* pending-inserts* source-index generation-config config]
  (let [pending (take-pending-inserts! pending-inserts*)]
    (when (seq pending)
      (let [builder (ensure-builder! builder* source-index generation-config)
            opts (cond-> {}
                   (:ingest-parallelism config)
                   (assoc :parallelism (:ingest-parallelism config)))]
        (gen/put-batch! builder (mapv second pending) (mapv first pending) opts)))))

(defn- seal-builder-view!
  "Seal once and split its native query view from its lightweight publication
   hold. Every partial transition cleans up both ownership domains."
  [builder]
  (let [sealed* (atom nil)
        view* (atom nil)
        hold* (atom nil)]
    (try
      (let [sealed (gen/seal! builder)
            _ (reset! sealed* sealed)
            view (gen/take-generation-view! sealed)
            _ (reset! view* view)
            hold (gen/take-publication-hold! sealed)
            _ (reset! hold* hold)]
        [view hold])
      (catch Throwable failure
        (try
          (if-let [hold @hold*]
            (gen/abort-publication! hold)
            (if-let [sealed @sealed*]
              (async/<!! (gen/discard! sealed))
              (when (#{:open :failed} @(:status builder))
                (async/<!! (gen/discard! builder)))))
          (finally
            (when-let [view @view*]
              (async/<!! (gen/close-view! view)))))
        (throw failure)))))

(defrecord TransientProximumIndex [builder* pending-inserts* source-index attrs
                                   config generation-config dirty?
                                   source-reserved?]
  sec/ISecondaryIndex
  (-search [_ query-spec entity-filter]
    (flush-pending-inserts! builder* pending-inserts* source-index
                            generation-config config)
    (if-let [index (if-let [builder @builder*]
                     (gen/builder-index builder)
                     (proximum-view source-index))]
      (es/entity-bitset-from-longs
       (map :id (search-results index query-spec entity-filter)))
      (es/entity-bitset)))
  (-estimate [_ query-spec]
    (flush-pending-inserts! builder* pending-inserts* source-index
                            generation-config config)
    (if-let [index (if-let [builder @builder*]
                     (gen/builder-index builder)
                     (proximum-view source-index))]
      (min (:k query-spec 10) (prox/count-vectors index))
      0))
  (-can-order? [_ _ direction] (= :asc direction))
  (-slice-ordered [_ query-spec entity-filter _ _ limit]
    (flush-pending-inserts! builder* pending-inserts* source-index
                            generation-config config)
    (let [query-spec (if limit
                       (update query-spec :k #(min % limit))
                       query-spec)]
      (mapv (fn [{:keys [id distance]}]
              {:entity-id id :distance distance})
            (if-let [index (if-let [builder @builder*]
                             (gen/builder-index builder)
                             (proximum-view source-index))]
              (search-results index query-spec entity-filter)
              []))))
  (-indexed-attrs [_] attrs)
  (-transact [this tx-report]
    (sec/-transact! this tx-report)
    this)

  sec/ITransientSecondaryIndex
  (-as-transient [this] this)
  (-transact! [_ {:keys [datom added?]}]
    (let [eid (.-e datom)
          value (.-v datom)]
      (if added?
        (if (instance? (Class/forName "[F") value)
          (do
            ;; Open the guard before accepting the first buffered write. The
            ;; vector itself remains in memory until the bounded batch flushes.
            (ensure-builder! builder* source-index generation-config)
            (reset! dirty? true)
            (swap! pending-inserts* conj [eid value])
            (when (>= (count @pending-inserts*)
                      (long (or (:ingest-batch-size config) 256)))
              (flush-pending-inserts! builder* pending-inserts* source-index
                                      generation-config config)))
          (log/warn :datahike/non-float-array-vector
                    {:eid eid :type (type value)}))
        (let [_ (flush-pending-inserts! builder* pending-inserts* source-index
                                        generation-config config)
              builder (ensure-builder! builder* source-index generation-config)]
          (reset! dirty? true)
          (gen/delete! builder eid)))))
  (-persistent! [_]
    (try
      (flush-pending-inserts! builder* pending-inserts* source-index
                              generation-config config)
      (if-not @dirty?
        source-index
        (let [builder @builder*]
          (let [[view hold] (seal-builder-view! builder)
                adopted-holds* (atom [hold])]
            (try
              (let [parent-holds
                    (if (satisfies? IUnpublishedProximumGenerations source-index)
                      (-take-publication-holds! source-index :transferred)
                      [])
                    _ (reset! adopted-holds* (conj (vec parent-holds) hold))
                    result (make-proximum-index view attrs config
                                                generation-config @adopted-holds*)]
                ;; A child can share CAS objects written under every ancestor's
                ;; cutoff. Carry those lightweight guards to the one primary
                ;; publication instead of releasing the predecessor early.
                result)
              (catch Throwable failure
                (try
                  (complete-holds! @adopted-holds* gen/abort-publication!)
                  (finally
                    (async/<!! (gen/close-view! view))))
                (throw failure))))))
      (finally
        (when source-reserved?
          (-release-derivation! source-index)))))

  sec/IDurableSecondaryTransient
  (-durable-persistent-result? [_] true)

  sec/IAbortableSecondaryTransient
  (-abort-transient! [_]
    (reset! pending-inserts* [])
    (try
      (when-let [builder @builder*]
        (when (#{:open :failed} @(:status builder))
          (async/<!! (gen/discard! builder))))
      (finally
        (when source-reserved?
          (-release-derivation! source-index))))))

(defrecord ProximumIndex [generation attrs config generation-config
                          publication-holds* publication-state*]
  java.io.Closeable
  (close [_]
    (locking publication-state*
      (when (= :deriving @publication-state*)
        (throw (ex-info "Cannot close a Proximum generation while a transient derivation owns it."
                        {:type :secondary/proximum-publication-owner-conflict
                         :state :deriving})))
      (when (= :unpublished @publication-state*)
        ;; Do not drain the only strong references until every completion
        ;; succeeds. A repeated close is then a genuine cleanup retry.
        (complete-holds! @publication-holds* gen/abort-publication!)
        (reset! publication-holds* [])
        (reset! publication-state* :aborted)))
    (close-generation! generation))

  IUnpublishedProximumGenerations
  (-take-publication-holds! [_ next-state]
    (transfer-holds! publication-holds* publication-state* next-state))
  (-reserve-derivation! [_]
    (reserve-derivation! publication-state*))
  (-release-derivation! [_]
    (release-derivation! publication-state*))

  sec/ISecondaryIndex
  (-search [_ query-spec entity-filter]
    (if generation
      (es/entity-bitset-from-longs
       (map :id (search-results (gen/generation-index generation)
                                query-spec entity-filter)))
      (es/entity-bitset)))
  (-estimate [_ query-spec]
    (if generation
      (min (:k query-spec 10)
           (prox/count-vectors (gen/generation-index generation)))
      0))
  (-can-order? [_ _ direction] (= :asc direction))
  (-slice-ordered [_ query-spec entity-filter _ _ limit]
    (if-not generation
      []
      (let [query-spec (if limit
                         (update query-spec :k #(min % limit))
                         query-spec)]
        (mapv (fn [{:keys [id distance]}]
                {:entity-id id :distance distance})
              (search-results (gen/generation-index generation)
                              query-spec entity-filter)))))
  (-indexed-attrs [_] attrs)
  (-transact [this tx-report]
    (let [transient-index (sec/-as-transient this)]
      (sec/-transact! transient-index tx-report)
      (sec/-persistent! transient-index)))

  sec/ISecondaryCandidateScan
  (-candidate-page [_ query-spec entity-filter page-request]
    ;; A continuation pins one immutable Proximum generation. Materialized mode
    ;; pages a fixed ANN result set; iterative mode retains its HNSW frontier and
    ;; resumes traversal without observing a newer generation. Exhaustion covers
    ;; the discovered HNSW candidates, not the full corpus: exact primary recheck
    ;; can validate returned rows, but cannot repair ANN false negatives, hence
    ;; :recall remains :approximate.
    (if-not generation
      {:candidates []
       :precision :recheck
       :recall :approximate
       :ordering :exact
       :exhausted? true
       :continuation nil
       :stop-reason :source-exhausted}
      (let [{:keys [vector candidate-limit ef query-id scan-mode strict-order?
                    max-visited max-distance-computations timeout-ms
                    max-frontier-nodes]} query-spec
            page-limit (:limit page-request)
            resumed-scan (:continuation page-request)
            scan (or resumed-scan
                     (prox/start-candidate-scan
                      (gen/generation-index generation)
                      vector
                      (cond-> {:candidate-limit (or candidate-limit page-limit)
                               :page-size page-limit
                               ;; Detached generations do not publish a mutable
                               ;; Proximum branch commit. Their immutable id is
                               ;; the owning snapshot identity for this cursor.
                               :primary-snapshot-id (gen/generation-id generation)}
                        scan-mode (assoc :mode scan-mode)
                        entity-filter
                        (assoc :entity-filter
                               (es/entity-bitset-seq entity-filter))
                        (some? strict-order?) (assoc :strict-order? strict-order?)
                        ef (assoc :ef ef)
                        query-id (assoc :query-id query-id)
                        max-visited (assoc :max-visited max-visited)
                        max-distance-computations
                        (assoc :max-distance-computations
                               max-distance-computations)
                        timeout-ms (assoc :timeout-ms timeout-ms)
                        max-frontier-nodes
                        (assoc :max-frontier-nodes max-frontier-nodes))))]
        (try
          (let [page (prox/candidate-page scan)
                attr (first attrs)
                candidates (mapv (fn [{:keys [id] :as candidate}]
                                   (-> candidate
                                       (dissoc :id)
                                       (assoc :entity-id (long id)
                                              :attribute attr)))
                                 (:candidates page))]
            {:candidates candidates
             :precision :recheck
             :recall :approximate
             :ordering (if (= :approximate (get-in page [:metadata :ordering]))
                         :approximate
                         :exact)
             :exhausted? (:exhausted? page)
             :continuation (:continuation page)
             :stop-reason (:stop-reason page)
             :stats (select-keys (:metadata page)
                                 [:visited-count :distance-computations
                                  :emitted-count :strict-order-drops
                                  :search-nanos])})
          (catch Throwable failure
            ;; On the first page no caller owns `scan` yet. A resumed scan is
            ;; still owned by the caller and core's wrapper closes it on error.
            (when-not resumed-scan
              (prox/close-candidate-scan! scan))
            (throw failure))))))

  sec/ISecondaryCandidateScanLifecycle
  (-close-candidate-scan [_ continuation]
    (prox/close-candidate-scan! continuation))

  sec/ITransientSecondaryIndex
  (-as-transient [this]
    (let [reserved? (-reserve-derivation! this)]
      (try
        (->TransientProximumIndex
         (atom nil) (atom []) this attrs config generation-config (atom false)
         reserved?)
        (catch Throwable failure
          (when reserved? (-release-derivation! this))
          (throw failure)))))
  (-transact! [_ _]
    (throw (IllegalStateException.
            "Call -as-transient before mutating a Proximum generation.")))
  (-persistent! [this] this)

  sec/ISecondaryWarmable
  (-sec-warm! [_ opts]
    (if generation
      (prox-warm/warm! (gen/generation-index generation) opts)
      {:fetched 0 :ms 0.0 :budget-exhausted? false}))

  sec/ISecondaryScannable
  (-sec-value [_ _ eid]
    (when generation
      (prox/get-vector (gen/generation-index generation) eid)))

  sec/IDurableSecondaryIndex
  (-sec-generation-key-map [_]
    {:type :proximum
     :format-version 2
     :storage-owner :external
     :generation-strategy :full-mmap-copy
     ;; The connected store is authoritative. A supplied `:store` can differ
     ;; from the advisory store-config, and publishing that alias would make an
     ;; external collector mark the wrong store.
     :external-store-id (or (some-> generation gen/generation-store-id)
                            (get-in config [:store-config :id]))
     :generation-id (some-> generation gen/generation-id)})
  (-sec-prepare [this _]
    (let [ch (async/promise-chan)]
      (try
        (let [[prepared holds owns?]
              (cond
                (= :unpublished @publication-state*)
                (let [holds (-take-publication-holds! this :preparing)]
                  (try
                    [(->ProximumIndex (gen/retain-generation-view generation)
                                      attrs config generation-config (atom [])
                                      publication-state*)
                     holds true]
                    (catch Throwable failure
                      (complete-holds! holds gen/abort-publication!)
                      (reset! publication-state* :aborted)
                      (throw failure))))

                (and generation (= :published @publication-state*))
                [this [] false]

                generation
                (throw (ex-info "This Proximum generation already has a publication owner."
                                {:type :secondary/proximum-publication-owner-conflict
                                 :state @publication-state*}))

                :else
                (let [builder (gen/begin-generation-from-config
                               generation-config)]
                  (try
                    (let [[view hold] (seal-builder-view! builder)]
                      [(->ProximumIndex view attrs config generation-config
                                        (atom []) (atom :preparing))
                       [hold] true])
                    (catch Throwable failure
                      (when (#{:open :failed} @(:status builder))
                        (async/<!! (gen/discard! builder)))
                      (throw failure)))))]
          (async/put! ch (->ProximumPreparation prepared holds owns?
                                                (:publication-state* prepared)
                                                (atom nil))))
        (catch Throwable failure
          (async/put! ch failure)))
      ch))
  (-sec-restore [_ _ key-map]
    (let [{:keys [generation-id external-store-id]}
          (validate-proximum-generation-key-map key-map)
          restored (gen/open-generation generation-config generation-id)]
      (if (= external-store-id (gen/generation-store-id restored))
        (->ProximumIndex restored attrs config generation-config
                         (atom []) (atom :published))
        (do
          (close-generation! restored)
          (throw (ex-info
                  "The Proximum generation belongs to a different live store."
                  {:type :secondary/proximum-generation-store-mismatch
                   :expected external-store-id
                   :actual (gen/generation-store-id restored)
                   :generation-id generation-id}))))))

  audit/IAuditable
  (-merkle-root [_]
    (when (:crypto-hash? config)
      (some-> generation gen/generation-index phi/commit-id)))
  (-recompute-merkle-root [_]
    (if-let [recompute (and generation
                            (try
                              (requiring-resolve
                               'proximum.audit/-recompute-merkle-root)
                              (catch Throwable _ nil)))]
      (recompute (gen/generation-index generation))
      {:status :unsupported :reason :proximum-generation-verifier-unavailable})))

(defn- make-proximum-index
  [generation attrs config generation-config publication-holds]
  (->ProximumIndex generation (set attrs) config generation-config
                   (atom (vec publication-holds))
                   (atom (if (seq publication-holds)
                           :unpublished
                           :published))))

(sec/register-index-type!
 :proximum
 {:create
  (fn [config _db]
   ;; ONE attribute per index, refused rather than silently mixed.
   ;;
   ;; proximum is keyed by an EXTERNAL ID, and this adapter uses the entity id
   ;; for it — so an index covering two attributes stores both of an entity's
   ;; vectors under the same key. What follows is not hypothetical, both halves
   ;; measured:
   ;;
   ;;   * one entity with both attributes -> proximum itself refuses, with
   ;;     "External id already exists", from inside the async index update,
   ;;     naming neither the attribute nor the real cause.
   ;;   * two attributes on DISJOINT entities -> nothing refuses, and reads are
   ;;     wrong: `-sec-value` ignores its `attr` argument (it can only fetch by
   ;;     id), so asking an entity for the attribute it does NOT have returns
   ;;     the vector of the one it does.
   ;;
   ;; Refusing at declaration is also what makes `-sec-value` ignoring `attr`
   ;; honest: with one covered attribute there is nothing to disambiguate.
   ;; Widening this means keying proximum by [eid attr] rather than eid — and a
   ;; single HNSW graph spanning two embedding spaces, under one `:dim` and one
   ;; distance, is a doubtful thing to want in the first place.
   (let [attrs (:attrs config)]
     (when (> (count attrs) 1)
       (throw (ex-info (str "A :proximum index covers exactly one attribute; this one declares "
                            (count attrs) " " (pr-str (vec attrs)) ". It is keyed by entity id, so "
                            "two attributes would store both vectors under the same key — "
                            "colliding where an entity has both, and answering for the wrong "
                            "attribute where it has one. Declare one index per vector attribute.")
                       {:error :secondary/proximum-multi-attr
                        :attrs (vec attrs)}))))
   (when-not (pos? (long (or (:ingest-batch-size config) 256)))
     (throw (ex-info ":ingest-batch-size must be a positive integer."
                     {:error :secondary/proximum-invalid-ingest-batch-size
                      :ingest-batch-size (:ingest-batch-size config)})))
   (when-not (get-in config [:store-config :id])
     (throw (ex-info "A durable :proximum secondary requires a stable :store-config :id."
                     {:error :secondary/proximum-store-id-required})))
   (when (= (::sec/store-id config) (get-in config [:store-config :id]))
     (throw
      (ex-info
       (str "A :proximum secondary must not use Datahike's primary Konserve store. "
            "Its generation is externally owned and Datahike's collector marks no "
            "objects in that store; sharing it would make live vector data collectable.")
       {:type :secondary/proximum-primary-store
        :store-id (::sec/store-id config)})))
   (let [mmap-dir (or (:mmap-dir config)
                      (str (System/getProperty "java.io.tmpdir")
                           "/datahike-proximum-" (random-uuid)))
         generation-config
         (merge {:type :hnsw
                 :mmap-dir mmap-dir
                 ;; Proximum validates this against `kp/store-id` on the live
                 ;; external Konserve store before it writes or restores a
                 ;; generation. The config-id equality check above is only an
                 ;; early diagnostic; aliases cannot bypass the live check.
                 :forbidden-store-id (::sec/store-id config)}
                (cond-> (select-keys config [:dim :distance :store :store-config :capacity
                                             :ef-construction :ef-search
                                             :crypto-hash?])
                  (:m config) (assoc :M (:m config))))]
     (make-proximum-index nil (:attrs config) config generation-config nil)))
  :storage-owner :external
  :validate-generation validate-proximum-generation-key-map
  ;; Proximum generations live in their own Konserve store, so there are no
  ;; reachable objects to mark in Datahike's primary store.
  :mark-generation (fn [_ _] #{})
  :external-root
  (fn [key-map]
    {:secondary-type :proximum
     :external-store-id (:external-store-id key-map)
     :generation-id (:generation-id key-map)})})

;; Proximum generations currently live in the adapter's own Konserve store.
;; Datahike therefore marks no keys in its store; the external store must retain
;; the generation ids named by Datahike roots when its own collector is enabled.

;; ---------------------------------------------------------------------------
;; KNN as a Datalog clause (external-engine)
;;
;; With the query-spec-fn mechanism, a Proximum vector search is a first-class
;; :where clause the planner recognizes — no manual EntityBitSet plumbing:
;;
;;   ;; retrieval — bind entity + cosine distance, then join like any relation
;;   (d/q '[:find ?name ?distance
;;          :in $ ?qvec
;;          :where
;;          [(datahike.index.secondary.proximum/knn :idx/embeddings ?qvec 10)
;;           [[?e ?distance]]]
;;          [?e :doc/name ?name]]
;;        db query-float-array)
;;
;;   ;; filter — entities only (1 binding var)
;;   [(datahike.index.secondary.proximum/knn :idx/embeddings ?qvec 10) [?e ...]]
;;
;; Planner-only: the base (relational) engine has no external-engine mechanism.
(defn knn
  "Vector k-nearest-neighbour search over a Proximum secondary index, callable
   from a Datalog :where clause. Args: <idx-ident> <query-vector (float[])> <k>.
   Binds [[?e ?distance]] (retrieval) or [?e ...] (filter). See the ns comment."
  {:datahike/external-engine
   {:index-key 0                                  ;; idx-ident is the first arg
    :binding-columns [:entity-id :distance]       ;; 1 var → :filter, 2 → :retrieval
    :accepts-entity-filter? true
    ;; Proximum's own query-spec shape (not the full-text {:query :field} default)
    :query-spec-fn (fn [query-args]
                     {:vector (first query-args) :k (second query-args)})
    :input-vars :all-bound
    :cost-model (fn [_db _idx-ident args _n-cols]
                  (let [k (nth (vec args) 2 10)]
                    {:estimated-card (if (number? k) k 10)
                     :cost-per-result 0.01}))}}
  ;; Body is only the legacy/bare-fn fallback — the planner path calls the index
  ;; directly through the executor. Returning the query-spec keeps it usable.
  [_idx-ident query-vector k]
  {:vector query-vector :k k})
