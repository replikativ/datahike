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

(defn- search-results [prox-idx {:keys [vector k ef]} entity-filter]
  (let [opts (cond-> {} ef (assoc :ef ef))]
    (if entity-filter
      ;; Proximum can translate a set of external IDs through its persistent
      ;; external-id map.  Passing a predicate forces it to visit every vector
      ;; merely to construct its internal bitset, turning a selective SQL
      ;; predicate into O(index-size) work before HNSW even starts.
      (prox/search-filtered prox-idx vector k
                            (into #{} (map long)
                                  (es/entity-bitset-seq entity-filter))
                            opts)
      (prox/search prox-idx vector k opts))))

(defn- close-generation! [generation]
  (when generation
    (async/<!! (gen/close-view! generation))))

(declare make-proximum-index)

(defrecord ProximumPreparation [prepared-index sealed-generation owns-prepared?
                                release-state]
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
              (reset! release-state :committed)

              (= :committed status)
              (do
                (when sealed-generation
                  (gen/rooted! sealed-generation)
                  (async/<!! (gen/close-view! sealed-generation)))
                (reset! release-state :committed))

              (and (= :aborted status) (nil? previous))
              (do
                (when sealed-generation
                  (async/<!! (gen/discard! sealed-generation)))
                (when owns-prepared?
                  (async/<!! (gen/close-view! (:generation prepared-index))))
                (reset! release-state :aborted))

              (= :unknown status)
              (do
                ;; The head write has returned, but its response was ambiguous.
                ;; There is no longer a values-then-pointer window to fence: if
                ;; the head landed, Datahike's retained roots keep this exact id;
                ;; if it did not, the generation is ordinary immutable garbage.
                ;; Acknowledging the sealed generation releases the Konserve
                ;; guard without moving any pointer. Close attempt-owned local
                ;; mmaps; a landed root remains exactly restorable by id.
                (when sealed-generation
                  (gen/rooted! sealed-generation)
                  (async/<!! (gen/close-view! sealed-generation)))
                (when owns-prepared?
                  (async/<!! (gen/close-view! (:generation prepared-index))))
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

(defrecord TransientProximumIndex [builder* source-index attrs config
                                   generation-config dirty?]
  sec/ISecondaryIndex
  (-search [_ query-spec entity-filter]
    (if-let [index (if-let [builder @builder*]
                     (gen/builder-index builder)
                     (proximum-view source-index))]
      (es/entity-bitset-from-longs
       (map :id (search-results index query-spec entity-filter)))
      (es/entity-bitset)))
  (-estimate [_ query-spec]
    (if-let [index (if-let [builder @builder*]
                     (gen/builder-index builder)
                     (proximum-view source-index))]
      (min (:k query-spec 10) (prox/count-vectors index))
      0))
  (-can-order? [_ _ direction] (= :asc direction))
  (-slice-ordered [_ query-spec entity-filter _ _ limit]
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
          (let [builder (ensure-builder! builder* source-index generation-config)]
            (reset! dirty? true)
            (gen/put! builder eid value))
          (log/warn :datahike/non-float-array-vector
                    {:eid eid :type (type value)}))
        (let [builder (ensure-builder! builder* source-index generation-config)]
          (reset! dirty? true)
          (gen/delete! builder eid)))))
  (-persistent! [_]
    (if-not @dirty?
      source-index
      (let [builder @builder*]
        (try
          (let [sealed (gen/seal! builder)]
            (try
              (let [generation-id (gen/generation-id sealed)
                    view (gen/open-generation generation-config generation-id)
                    result (make-proximum-index view attrs config generation-config
                                                sealed)]
                ;; A backfill result may itself be an unpublished sealed
                ;; generation. Once its child is sealed, the child's complete
                ;; roots retain every required object and the intermediate
                ;; guard can be released.
                (when-let [parent-sealed (:sealed-generation source-index)]
                  (async/<!! (gen/discard! parent-sealed)))
                result)
              (catch Throwable open-failure
                (async/<!! (gen/discard! sealed))
                (throw open-failure))))
          (catch Throwable seal-failure
            (when (= :open @(:status builder))
              (async/<!! (gen/discard! builder)))
            (when (= :failed @(:status builder))
              (async/<!! (gen/discard! builder)))
            (throw seal-failure))))))

  sec/IDurableSecondaryTransient
  (-durable-persistent-result? [_] true)

  sec/IAbortableSecondaryTransient
  (-abort-transient! [_]
    (when-let [builder @builder*]
      (when (#{:open :failed} @(:status builder))
        (async/<!! (gen/discard! builder))))))

(defrecord ProximumIndex [generation attrs config generation-config sealed-generation]
  java.io.Closeable
  (close [_]
    (when sealed-generation (async/<!! (gen/discard! sealed-generation)))
    (close-generation! generation))

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
    ;; Freeze one ANN result set on the first page. A continuation owns that
    ;; immutable set, so resuming never observes another Proximum generation.
    ;; Exhaustion covers the discovered HNSW candidates, not the full corpus:
    ;; exact primary recheck can validate returned rows, but cannot repair ANN
    ;; false negatives, hence :recall remains :approximate.
    (if-not generation
      {:candidates []
       :precision :recheck
       :recall :approximate
       :ordering :exact
       :exhausted? true
       :continuation nil}
      (let [{:keys [vector candidate-limit ef query-id]} query-spec
            page-limit (:limit page-request)
            scan (or (:continuation page-request)
                     (prox/start-candidate-scan
                      (gen/generation-index generation)
                      vector
                      (cond-> {:candidate-limit (or candidate-limit page-limit)
                               :page-size page-limit
                               ;; Detached generations do not publish a mutable
                               ;; Proximum branch commit. Their immutable id is
                               ;; the owning snapshot identity for this cursor.
                               :primary-snapshot-id (gen/generation-id generation)}
                        ef (assoc :ef ef)
                        query-id (assoc :query-id query-id))))
            page (prox/candidate-page scan)
            attr (first attrs)
            candidates
            (into []
                  (keep (fn [{:keys [id] :as candidate}]
                          (when (or (nil? entity-filter)
                                    (es/entity-bitset-contains?
                                     entity-filter (long id)))
                            (-> candidate
                                (dissoc :id)
                                (assoc :entity-id (long id)
                                       :attribute attr)))))
                  (:candidates page))]
        {:candidates candidates
         :precision :recheck
         :recall :approximate
         :ordering :exact
         :exhausted? (:exhausted? page)
         :continuation (:continuation page)})))

  sec/ITransientSecondaryIndex
  (-as-transient [_]
    (->TransientProximumIndex
     (atom nil) _ attrs config generation-config (atom false)))
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
     :external-store-id (get-in config [:store-config :id])
     :generation-id (some-> generation gen/generation-id)})
  (-sec-prepare [this _]
    (let [ch (async/promise-chan)]
      (try
        (let [[prepared sealed owns?]
              (cond
                sealed-generation
                [(->ProximumIndex (gen/retain-generation-view generation)
                                  attrs config generation-config nil)
                 sealed-generation true]

                generation
                [this nil false]

                :else
                (let [builder (gen/begin-generation-from-config
                               generation-config)]
                  (try
                    (let [sealed (gen/seal! builder)
                          generation-id (gen/generation-id sealed)
                          view (gen/open-generation generation-config
                                                    generation-id)]
                      [(->ProximumIndex view attrs config generation-config nil)
                       sealed true])
                    (catch Throwable failure
                      (async/<!! (gen/discard! builder))
                      (throw failure)))))]
          (async/put! ch (->ProximumPreparation prepared sealed owns?
                                                (atom nil))))
        (catch Throwable failure
          (async/put! ch failure)))
      ch))
  (-sec-restore [_ _ key-map]
    (let [{:keys [generation-id]}
          (validate-proximum-generation-key-map key-map)
          restored (gen/open-generation generation-config generation-id)]
      (->ProximumIndex restored attrs config generation-config nil)))

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
  [generation attrs config generation-config sealed-generation]
  (->ProximumIndex generation (set attrs) config generation-config
                   sealed-generation))

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
