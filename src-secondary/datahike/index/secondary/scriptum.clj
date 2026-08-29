(ns datahike.index.secondary.scriptum
  "Scriptum/Lucene full-text secondary index backed by immutable generations.

   `:path` is a disposable local segment cache. The authoritative identity is
   the immutable Scriptum snapshot address stored in the Datahike root."
  (:require
   [clojure.core.async :as async]
   [datahike.index.audit :as audit]
   [datahike.index.entity-set :as es]
   [datahike.index.secondary :as sec]
   [datahike.migrate.fs :as fs]
   [konserve.memory :refer [new-mem-store]]
   [replikativ.logging :as log]
   [scriptum.core :as sc]))

(defn- delivered [value]
  (let [ch (async/promise-chan)]
    (async/put! ch value)
    ch))

(defn- scriptum-key-map-failure-reason [key-map]
  (cond
    (not= :scriptum (:type key-map)) :wrong-type
    ;; Scriptum v1 named the old mutable/path-backed representation. A v1
    ;; address is not a v2 sealed Lucene manifest and cannot be upgraded by
    ;; relabelling its envelope.
    (or (= 1 (:format-version key-map))
        (and (nil? (:format-version key-map))
             (or (contains? key-map :path)
                 (contains? key-map :branch))))
    :legacy-scriptum-v1-generation
    (not= 2 (:format-version key-map)) :unsupported-format-version
    (not= :datahike (:storage-owner key-map)) :wrong-storage-owner
    (not (uuid? (:snapshot-address key-map))) :invalid-snapshot-address
    :else nil))

(defn- validate-scriptum-generation-key-map [key-map]
  (when-let [reason (scriptum-key-map-failure-reason key-map)]
    (throw (ex-info
            "Invalid Scriptum generation key-map."
            {:type :secondary/invalid-scriptum-generation
             :reason reason
             :key-map key-map
             :expected {:type :scriptum
                        :format-version 2
                        :storage-owner :datahike
                        :snapshot-address :uuid}})))
  key-map)

(defn- attr-name [a]
  ;; Preserve the namespace. `(name :foo/body)` and `(name :bar/body)` both
  ;; produced "body", so equal values collided in `_key` and retracting one
  ;; attribute deleted the other.
  (if (keyword? a) (pr-str a) (str a)))

(defn- doc-key [eid a value-hash]
  (str eid "|" (attr-name a) "|" value-hash))

(defn- query->lucene [{:keys [query field fields] :as query-spec}]
  (cond
    (instance? org.apache.lucene.search.Query query) query
    (= :all query) :all
    (and field (string? query)) (sc/text-query field query)
    (and (seq fields) (string? query))
    (sc/multi-field-query (map attr-name fields) query)
    :else (throw (ex-info "Invalid scriptum query-spec" {:spec query-spec}))))

(defn- filtered-lucene-query [query-spec entity-filter]
  (let [query (query->lucene query-spec)]
    (if entity-filter
      (let [eids (vec (es/entity-bitset-seq entity-filter))]
        {:query (sc/bool-query
                 [[query :must]
                  [(sc/terms-query :_entity_id (map str eids)) :filter]])
         ;; Scriptum validates this value on every resume. Retaining the exact
         ;; immutable filter avoids hash-collision or mutable-filter ambiguity.
         :query-id [(:query-id query-spec) eids]})
      {:query query :query-id (:query-id query-spec)})))

(defn- result-eid [result]
  (when-let [eid-str (get result "_entity_id")]
    (try
      (Long/parseLong eid-str)
      (catch NumberFormatException _
        (log/warn :datahike/invalid-lucene-eid {:eid-str eid-str})
        nil))))

(defn- result-attr [attrs result]
  (let [stored (get result "_attr")]
    (or (some #(when (= stored (attr-name %)) %) attrs)
        stored)))

(defn- result-candidate [attrs result]
  (when-let [eid (result-eid result)]
    {:entity-id eid
     :attribute (result-attr attrs result)
     :value-hash (get result "_vhash")
     :score (:score result)}))

(defn- reduce-result-candidates [attrs rf init results]
  (reduce (fn [acc result]
            (if-let [candidate (result-candidate attrs result)]
              (rf acc candidate)
              acc))
          init results))

(defn- reduce-matching-results
  "Reduce one logical, pre-filtered candidate stream without materializing it."
  [snapshot attrs query-spec entity-filter limit rf init]
  (if-not snapshot
    init
    (let [{:keys [query query-id]}
          (filtered-lucene-query query-spec entity-filter)]
      (if-let [limit (or limit (:limit query-spec))]
        (reduce-result-candidates
         attrs rf init
         (sc/search-store-snapshot snapshot query {:limit limit}))
        ;; Complete set operations page in exact, score-free index order. The
        ;; authoritative Datahike/PostgreSQL predicate owns visible ranking.
        (loop [after nil
               acc init]
          (let [page (sc/candidate-page
                      snapshot query
                      {:page-size 1024
                       :after after
                       :query-id query-id
                       :order :doc-id
                       :fields ["_entity_id" "_attr" "_vhash"]})
                acc (reduce-result-candidates attrs rf acc (:candidates page))]
            (if (:exhausted? page)
              acc
              (recur (:continuation page) acc))))))))

(defn- matching-results [snapshot attrs query-spec entity-filter limit]
  (persistent!
   (reduce-matching-results snapshot attrs query-spec entity-filter limit
                            conj! (transient []))))

(declare make-scriptum-index)

(defprotocol ^:private IUnpublishedScriptumGenerations
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
          (throw (ex-info "An unpublished Scriptum generation has no publication hold."
                          {:type :secondary/scriptum-missing-publication-hold})))
        (reset! publication-state* next-state)
        holds)
      :deriving
      (if (= :transferred next-state)
        (let [holds (take-holds! holds*)]
          (when-not (seq holds)
            (throw (ex-info "A derived Scriptum generation has no publication hold."
                            {:type :secondary/scriptum-missing-publication-hold})))
          (reset! publication-state* next-state)
          holds)
        (throw (ex-info "A Scriptum derivation can only transfer its publication ownership."
                        {:type :secondary/scriptum-publication-owner-conflict
                         :state :deriving
                         :requested-state next-state})))
      (throw (ex-info "A Scriptum generation already has an exclusive publication owner."
                      {:type :secondary/scriptum-publication-owner-conflict
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
      (throw (ex-info "One or more Scriptum publication holds failed to close"
                      {:type :secondary/scriptum-publication-cleanup-failed
                       :failure-count (count failures)}
                      (first failures))))))

(defn- reserve-derivation!
  [publication-state*]
  (locking publication-state*
    (case @publication-state*
      :published false
      :unpublished (do (reset! publication-state* :deriving) true)
      (throw (ex-info "This Scriptum generation already has an exclusive publication owner."
                      {:type :secondary/scriptum-publication-owner-conflict
                       :state @publication-state*})))))

(defn- release-derivation!
  [publication-state*]
  (locking publication-state*
    (when (= :deriving @publication-state*)
      (reset! publication-state* :unpublished)
      true)))

(defn- seal-generation-view!
  [generation store cache message]
  (let [snapshot* (atom nil)
        hold* (atom nil)]
    (try
      (let [address (sc/seal-generation! generation message)
            snapshot (sc/open-store-snapshot store cache address)
            _ (reset! snapshot* snapshot)
            hold (sc/take-generation-publication-hold! generation)
            _ (reset! hold* hold)]
        [address snapshot hold])
      (catch Throwable failure
        (try
          (if-let [hold @hold*]
            (sc/abort-generation-publication! hold)
            (sc/abort-generation! generation))
          (finally
            (when-let [snapshot @snapshot*]
              (.close ^java.io.Closeable snapshot))))
        (throw failure)))))

(defrecord ScriptumPreparation [prepared-index publication-holds owns-prepared?
                                publication-state* release-state]
  sec/IPreparedSecondaryGeneration
  (-sec-generation-index [_] prepared-index)
  (-sec-release [_ outcome]
    (try
      (locking release-state
        (let [previous @release-state
              status (:status outcome)]
          (cond
            (#{:committed :aborted} previous) nil

            (and (= :unknown previous) (= :committed status))
            (do
              (complete-holds! publication-holds
                               sc/root-generation-publication!)
              (reset! publication-state* :published)
              (reset! release-state :committed))

            (and (= :unknown previous) (= :unknown status)) nil

            (= :committed status)
            (do
              (complete-holds! publication-holds
                               sc/root-generation-publication!)
              (reset! publication-state* :published)
              (reset! release-state :committed))

            (and (= :aborted status) (nil? previous))
            (let [completed? (atom false)]
              (try
                (complete-holds! publication-holds
                                 sc/abort-generation-publication!)
                (reset! completed? true)
                (finally
                  (when owns-prepared?
                    (.close ^java.io.Closeable prepared-index))))
              (when @completed?
                (reset! publication-state* :aborted)
                (reset! release-state :aborted)))

            (= :unknown status)
            (do
              ;; The head may still land. Keep the lightweight holds for
              ;; reconciliation but release the local snapshot cache.
              (when owns-prepared?
                (.close ^java.io.Closeable prepared-index))
              (reset! publication-state* :unknown)
              (reset! release-state :unknown))

            :else
            (throw (ex-info "An ambiguous secondary generation cannot later be aborted."
                            {:type :secondary/ambiguous-generation-abort
                             :previous previous
                             :outcome outcome})))))
      (delivered true)
      (catch Throwable failure
        (delivered failure)))))

(defn- ensure-generation! [generation* source-index config store cache store-id]
  (or @generation*
      (let [base-address
            (:snapshot-address (sec/-sec-generation-key-map source-index))
            generation
            (sc/begin-generation
             store cache base-address
             {:store-id store-id
              :logical-name (str (or (::sec/index-ident config) :scriptum))
              :workspace-id (str (when-let [attempt (::sec/build-attempt config)]
                                   (str attempt "-"))
                                 (random-uuid))
              :max-merged-segment-mb (:max-merged-segment-mb config)
              :ram-buffer-mb (:ram-buffer-mb config)})]
        (reset! generation* generation)
        generation)))

(defrecord TransientScriptumIndex [generation* source-index attrs config store
                                   cache store-id dirty? frozen?
                                   source-reserved?]
  sec/ISecondaryIndex
  (-search [_ query-spec entity-filter]
    (if-let [generation @generation*]
      (let [results (sc/search generation (query->lucene query-spec)
                               {:limit (or (:limit query-spec) 1000)})
            bitset (es/entity-bitset)]
        (doseq [result results
                :let [eid (result-eid result)]
                :when (and eid
                           (or (nil? entity-filter)
                               (es/entity-bitset-contains? entity-filter eid)))]
          (es/entity-bitset-add! bitset eid))
        bitset)
      (sec/-search source-index query-spec entity-filter)))
  (-estimate [_ query-spec]
    (if @generation* (or (:limit query-spec) 100)
        (sec/-estimate source-index query-spec)))
  (-can-order? [_ _ direction] (= :desc direction))
  (-slice-ordered [_ query-spec entity-filter _ _ limit]
    (if-let [generation @generation*]
      (let [results (sc/search generation (query->lucene query-spec)
                               {:limit (or limit 1000)})]
        (into []
              (keep (fn [result]
                      (when-let [eid (result-eid result)]
                        (when (or (nil? entity-filter)
                                  (es/entity-bitset-contains? entity-filter eid))
                          {:entity-id eid :score (:score result)}))))
              results))
      (sec/-slice-ordered source-index query-spec entity-filter nil :desc limit)))
  (-indexed-attrs [_] attrs)
  (-transact [this tx-report]
    (sec/-transact! this tx-report)
    this)

  sec/ITransientSecondaryIndex
  (-as-transient [this] this)
  (-transact! [_ {:keys [datom added? value-hash secondary-only?]}]
    (let [eid (.-e datom)
          attr (.-a datom)
          value (.-v datom)
          value-hash (or value-hash (sec/secondary-only-hash value))
          key (doc-key eid attr value-hash)
          _ (when (and added? secondary-only? (not (string? value)))
              (throw (ex-info
                      "Scriptum can be authoritative for :db.secondary/only only for string values."
                      {:type :secondary/scriptum-secondary-only-requires-string
                       :attribute attr
                       :value-type (type value)})))
          generation (ensure-generation! generation* source-index config store
                                         cache store-id)]
      (reset! dirty? true)
      (if added?
        (sc/add-doc generation
                    {:_entity_id {:value (str eid) :type :string :store? true}
                     :_attr {:value (attr-name attr) :type :string :store? true}
                     :_key {:value key :type :string :store? true}
                     :_vhash {:value value-hash :type :string :store? true}
                     :value {:value (if (string? value) value (str value))
                             :type :text :store? true}})
        (sc/delete-docs generation "_key" key))))
  (-persistent! [_]
    (try
      (if-not @dirty?
        source-index
        (let [generation @generation*]
          (let [[address snapshot hold]
                (seal-generation-view! generation store cache
                                       "datahike-secondary-generation")
                adopted-holds* (atom [hold])]
            (try
              (let [parent-holds
                    (if (satisfies? IUnpublishedScriptumGenerations source-index)
                      (-take-publication-holds! source-index :transferred)
                      [])
                    _ (reset! adopted-holds* (conj (vec parent-holds) hold))
                    result (make-scriptum-index
                            snapshot attrs config store cache store-id address
                            @adopted-holds* nil)]
                ;; A child can share immutable segments written under every
                ;; ancestor's cutoff. Carry all lightweight holds to the one
                ;; primary publication rather than releasing its parent early.
                (reset! frozen? true)
                result)
              (catch Throwable failure
                (try
                  (complete-holds! @adopted-holds*
                                   sc/abort-generation-publication!)
                  (finally
                    (.close ^java.io.Closeable snapshot)))
                (throw failure))))))
      (finally
        (when source-reserved?
          (-release-derivation! source-index)))))

  sec/ISecondaryHashAddressable
  (-sec-value-by-hash [_ attr eid value-hash]
    (some (fn [result]
            (when (= value-hash (get result "_vhash"))
              (get result "value")))
          (if-let [generation @generation*]
            (sc/search generation {:term [:_key (doc-key eid attr value-hash)]}
                       {:limit 1})
            [])))

  sec/IDurableSecondaryTransient
  (-durable-persistent-result? [_] true)

  sec/IAbortableSecondaryTransient
  (-abort-transient! [_]
    (try
      (when (and (not @frozen?) @generation*)
        (sc/abort-generation! @generation*))
      (finally
        (when source-reserved?
          (-release-derivation! source-index))))))

(defn- make-scriptum-index
  [snapshot attrs config store cache store-id address publication-holds
   supplied-publication-state*]
  (let [attrs (set attrs)
        publication-holds* (atom (vec publication-holds))
        publication-state* (or supplied-publication-state*
                               (atom (if (seq publication-holds)
                                       :unpublished
                                       :published)))]
    (reify
      java.io.Closeable
      (close [_]
        (locking publication-state*
          (when (= :deriving @publication-state*)
            (throw (ex-info "Cannot close a Scriptum generation while a transient derivation owns it."
                            {:type :secondary/scriptum-publication-owner-conflict
                             :state :deriving})))
          (when (= :unpublished @publication-state*)
            (complete-holds! @publication-holds*
                             sc/abort-generation-publication!)
            (reset! publication-holds* [])
            (reset! publication-state* :aborted)))
        (when snapshot (.close ^java.io.Closeable snapshot)))

      IUnpublishedScriptumGenerations
      (-take-publication-holds! [_ next-state]
        (transfer-holds! publication-holds* publication-state* next-state))
      (-reserve-derivation! [_]
        (reserve-derivation! publication-state*))
      (-release-derivation! [_]
        (release-derivation! publication-state*))

      sec/ISecondaryIndex
      (-search [_ query-spec entity-filter]
        (reduce-matching-results
         snapshot attrs query-spec entity-filter nil
         (fn [bitset {:keys [entity-id]}]
           (es/entity-bitset-add! bitset entity-id)
           bitset)
         (es/entity-bitset)))
      (-estimate [_ query-spec]
        (if snapshot
          (sc/count-store-snapshot snapshot (query->lucene query-spec))
          0))
      (-can-order? [_ _ direction] (= :desc direction))
      (-slice-ordered [_ query-spec entity-filter _ _ limit]
        (mapv #(select-keys % [:entity-id :score])
              (matching-results snapshot attrs query-spec entity-filter
                                (or limit 1000))))
      (-indexed-attrs [_] attrs)
      (-transact [this tx-report]
        (let [transient-index (sec/-as-transient this)]
          (sec/-transact! transient-index tx-report)
          (sec/-persistent! transient-index)))

      sec/ITransientSecondaryIndex
      (-as-transient [this]
        (when-not (and store store-id)
          (throw (ex-info "Scriptum generation has no storage context."
                          {:type :secondary/scriptum-missing-store})))
        (let [reserved? (-reserve-derivation! this)]
          (try
            (->TransientScriptumIndex
             (atom nil) this attrs config store cache store-id (atom false)
             (atom false) reserved?)
            (catch Throwable failure
              (when reserved? (-release-derivation! this))
              (throw failure)))))
      (-transact! [_ _]
        (throw (IllegalStateException.
                "Call -as-transient before mutating a Scriptum generation.")))
      (-persistent! [this] this)

      sec/ISecondaryCandidateScan
      (-candidate-page [_ query-spec entity-filter page-request]
        (let [precision :recheck
              recall :complete
              ordering :none]
          (if-not snapshot
            {:candidates [] :precision precision :recall recall
             :ordering ordering :exhausted? true :continuation nil
             :stop-reason :source-exhausted}
            (let [{:keys [query query-id]}
                  (filtered-lucene-query query-spec entity-filter)
                  page (sc/candidate-page
                        snapshot query
                        {:page-size (:limit page-request)
                         :after (:continuation page-request)
                         :query-id query-id
                         :order :doc-id
                         :fields ["_entity_id" "_attr" "_vhash"]})
                candidates
                (into []
                      (keep (partial result-candidate attrs))
                      (:candidates page))]
            {:candidates candidates
             :precision precision
             :recall recall
             :ordering ordering
             :exhausted? (:exhausted? page)
             :continuation (:continuation page)
             :stop-reason (when (:exhausted? page) :source-exhausted)}))))

      sec/ISecondaryScannable
      (-sec-value [_ attr eid]
        (some (fn [result]
                (when (= (attr-name attr) (get result "_attr"))
                  (get result "value")))
              (if snapshot
                (sc/search-store-snapshot
                 snapshot {:term [:_entity_id (str eid)]} {:limit 64})
                [])))

      sec/ISecondaryHashAddressable
      (-sec-value-by-hash [_ attr eid value-hash]
        (some (fn [result]
                (when (and (= (attr-name attr) (get result "_attr"))
                           (= value-hash (get result "_vhash")))
                  (get result "value")))
              (if snapshot
                (sc/search-store-snapshot
                 snapshot {:term [:_key (doc-key eid attr value-hash)]}
                 {:limit 1})
                [])))

      sec/ISecondaryWarmable
      (-sec-warm! [_ _]
        {:fetched 0 :ms 0.0 :budget-exhausted? false :unsupported? true})

      sec/IDurableSecondaryIndex
      (-sec-generation-key-map [_]
        {:type :scriptum
         :format-version 2
         :storage-owner :datahike
         :snapshot-address address})
      (-sec-prepare [this _]
        (try
          (let [[prepared holds owns? prepared-state*]
                (if (= :unpublished @publication-state*)
                  (let [holds (-take-publication-holds! this :preparing)]
                    (try
                      [(make-scriptum-index
                        (sc/retain-store-snapshot snapshot)
                        attrs config store cache store-id address []
                        publication-state*)
                       holds true publication-state*]
                      (catch Throwable failure
                        (complete-holds! holds
                                         sc/abort-generation-publication!)
                        (reset! publication-state* :aborted)
                        (throw failure))))
                  (if (and address (= :published @publication-state*))
                    [this [] false publication-state*]
                    (if address
                      (throw (ex-info "This Scriptum generation already has a publication owner."
                                      {:type :secondary/scriptum-publication-owner-conflict
                                       :state @publication-state*}))
                      (let [generation (sc/begin-generation
                                        store cache nil
                                        {:store-id store-id
                                         :logical-name
                                         (str (or (::sec/index-ident config)
                                                  :scriptum))})
                            [address snapshot hold]
                            (seal-generation-view!
                             generation store cache
                             "datahike-empty-secondary-generation")
                            prepared-state* (atom :preparing)
                            prepared (make-scriptum-index
                                      snapshot attrs config store cache
                                      store-id address [] prepared-state*)]
                        [prepared [hold] true prepared-state*]))))]
            (delivered (->ScriptumPreparation prepared holds owns?
                                              prepared-state*
                                              (atom nil))))
          (catch Throwable failure
            (delivered failure))))
      (-sec-restore [_ restore-store key-map]
        (let [restore-address (:snapshot-address
                               (validate-scriptum-generation-key-map key-map))
              restored (sc/open-store-snapshot restore-store cache
                                               restore-address)]
          (make-scriptum-index restored attrs config restore-store cache store-id
                               restore-address [] nil)))

      audit/IAuditable
      (-merkle-root [_] (when (uuid? address) address))
      (-recompute-merkle-root [_]
        (if address
          (let [{:keys [status recomputed-root errors] :as result}
                (sc/verify-generation store address)]
            (cond-> {:status status :root recomputed-root}
              (seq errors) (assoc :errors errors)
              (:objects result) (assoc :objects (:objects result))))
          {:status :unsupported :reason :empty-generation})))))

(sec/register-index-type!
 :scriptum
 {:create
  (fn [config _db]
    (let [provided-store (::sec/store config)
          store (or provided-store (new-mem-store (atom {}) {:sync? true}))
          store-id (or (::sec/store-id config) (random-uuid))
          cache (or (:path config) (fs/temp-dir! "datahike-scriptum-"))]
      (make-scriptum-index nil (:attrs config) config store cache store-id nil [] nil)))
  :storage-owner :datahike
  :validate-generation validate-scriptum-generation-key-map
  :mark-generation
  (fn [key-map store]
    (sc/mark-generation store (:snapshot-address key-map)))})
