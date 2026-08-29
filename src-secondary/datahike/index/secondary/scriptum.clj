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

(defn- search-results [snapshot query-spec limit]
  (if-not snapshot
    []
    (if-let [limit (or limit (:limit query-spec))]
      (sc/search-store-snapshot snapshot (query->lucene query-spec)
                                {:limit limit})
      ;; ISecondaryIndex/-search is a complete set operation, not a top-N
      ;; convenience API. Lucene's search call requires a bound, so enumerate
      ;; its immutable snapshot with searchAfter pages instead of silently
      ;; truncating every result at the old hard-coded 1000 documents.
      (loop [after nil
             acc []]
        (let [page (sc/candidate-page
                    snapshot (query->lucene query-spec)
                    {:page-size 1024
                     :after after
                     :query-id (:query-id query-spec)})
              acc (into acc (:candidates page))]
          (if (:exhausted? page)
            acc
            (recur (:continuation page) acc)))))))

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

(defn- matching-results [snapshot attrs query-spec entity-filter limit]
  (into []
        (keep (fn [result]
                (when-let [eid (result-eid result)]
                  (when (or (nil? entity-filter)
                            (es/entity-bitset-contains? entity-filter eid))
                    {:entity-id eid
                     :attribute (result-attr attrs result)
                     :score (:score result)}))))
        (search-results snapshot query-spec limit)))

(declare make-scriptum-index)

(defprotocol ^:private IUnpublishedScriptumGeneration
  (-release-unpublished-generation! [index]))

(defn- take-sealed-generation! [generation*]
  (locking generation*
    (let [generation @generation*]
      (reset! generation* nil)
      generation)))

(defrecord ScriptumPreparation [prepared-index generation owns-prepared?
                                release-state]
  sec/IPreparedSecondaryGeneration
  (-sec-generation-index [_] prepared-index)
  (-sec-release [_ outcome]
    (try
      (locking release-state
        (when-not @release-state
          (try
            (when generation
              (case (:status outcome)
                :committed (sc/release-generation! generation)
                :aborted (sc/abort-generation! generation)
                :unknown (sc/abort-generation! generation)))
            (finally
              ;; A release hook is one-shot even when cleanup itself fails.
              ;; Re-entering Lucene cleanup after a partial close is not a safe
              ;; retry protocol, and visibility was already decided by the
              ;; Datahike head before this hook ran.
              (try
                (when (and owns-prepared? (not= :committed (:status outcome)))
                  (.close ^java.io.Closeable prepared-index))
                (finally
                  (reset! release-state (:status outcome))))))))
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
                                   cache store-id dirty? frozen?]
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
    (if-not @dirty?
      source-index
      (let [generation @generation*]
        (try
          (let [address (sc/seal-generation! generation
                                             "datahike-secondary-generation")]
            (try
              (let [snapshot (sc/open-store-snapshot store cache address)
                    result (make-scriptum-index snapshot attrs config store cache
                                                store-id address generation)]
                ;; Writer batching may derive several sealed generations before
                ;; publishing only the last one.  Once the child is complete it
                ;; roots every shared Lucene object it needs, so the unpublished
                ;; parent's guard can be released.  Keep its snapshot readable
                ;; for an intermediate tx-report db-before; only ownership of
                ;; the preparation handle moves forward.
                (when (satisfies? IUnpublishedScriptumGeneration source-index)
                  (-release-unpublished-generation! source-index))
                (reset! frozen? true)
                result)
              (catch Throwable open-failure
                (sc/abort-generation! generation)
                (throw open-failure))))
          (catch Throwable seal-failure
            (sc/abort-generation! generation)
            (throw seal-failure))))))

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
    (when (and (not @frozen?) @generation*)
      (sc/abort-generation! @generation*))))

(defn- make-scriptum-index
  [snapshot attrs config store cache store-id address sealed-generation]
  (let [attrs (set attrs)
        sealed-generation* (atom sealed-generation)]
    (reify
      java.io.Closeable
      (close [_]
        (try
          (when snapshot (.close ^java.io.Closeable snapshot))
          (finally
            (when-let [generation (take-sealed-generation! sealed-generation*)]
              (sc/abort-generation! generation)))))

      IUnpublishedScriptumGeneration
      (-release-unpublished-generation! [_]
        (when-let [generation (take-sealed-generation! sealed-generation*)]
          (sc/release-generation! generation)))

      sec/ISecondaryIndex
      (-search [_ query-spec entity-filter]
        (let [bitset (es/entity-bitset)]
          (doseq [{:keys [entity-id]}
                  (matching-results snapshot attrs query-spec entity-filter
                                    nil)]
            (es/entity-bitset-add! bitset entity-id))
          bitset))
      (-estimate [_ query-spec] (if snapshot (or (:limit query-spec) 100) 0))
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
        (->TransientScriptumIndex
         (atom nil) this attrs config store cache store-id (atom false)
         (atom false)))
      (-transact! [_ _]
        (throw (IllegalStateException.
                "Call -as-transient before mutating a Scriptum generation.")))
      (-persistent! [this] this)

      sec/ISecondaryCandidateScan
      (-candidate-page [_ query-spec entity-filter page-request]
        (let [precision (or (:precision query-spec) :exact)
              recall (or (:recall query-spec) :complete)
              ordering (or (:ordering query-spec) :exact)]
          (if-not snapshot
            {:candidates [] :precision precision :recall recall
             :ordering ordering :exhausted? true :continuation nil}
            (let [page (sc/candidate-page
                      snapshot (query->lucene query-spec)
                      {:page-size (:limit page-request)
                       :after (:continuation page-request)
                       :query-id (:query-id query-spec)
                       :fields ["_entity_id" "_attr"]})
                candidates
                (into []
                      (keep (fn [result]
                              (when-let [eid (result-eid result)]
                                (when (or (nil? entity-filter)
                                          (es/entity-bitset-contains?
                                           entity-filter eid))
                                  {:entity-id eid
                                   :attribute (result-attr attrs result)
                                   :score (:score result)}))))
                      (:candidates page))]
            {:candidates candidates
             :precision precision
             :recall recall
             :ordering ordering
             :exhausted? (:exhausted? page)
             :continuation (:continuation page)}))))

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
          (let [[prepared generation owns?]
                (if-let [generation
                         (take-sealed-generation! sealed-generation*)]
                  [(make-scriptum-index (sc/retain-store-snapshot snapshot)
                                        attrs config store cache store-id address
                                        nil)
                   generation true]
                  (if address
                    [this nil false]
                    (let [generation (sc/begin-generation
                                      store cache nil
                                      {:store-id store-id
                                       :logical-name
                                       (str (or (::sec/index-ident config)
                                                :scriptum))})]
                      (try
                        (let [address (sc/seal-generation!
                                       generation
                                       "datahike-empty-secondary-generation")
                              snapshot (sc/open-store-snapshot store cache address)]
                          [(make-scriptum-index snapshot attrs config store cache
                                                store-id address nil)
                           generation true])
                        (catch Throwable failure
                          (sc/abort-generation! generation)
                          (throw failure))))))]
            (delivered (->ScriptumPreparation prepared generation owns?
                                              (atom nil))))
          (catch Throwable failure
            (delivered failure))))
      (-sec-restore [_ restore-store key-map]
        (let [restore-address (:snapshot-address
                               (validate-scriptum-generation-key-map key-map))
              restored (sc/open-store-snapshot restore-store cache
                                               restore-address)]
          (make-scriptum-index restored attrs config restore-store cache store-id
                               restore-address nil)))

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
      (make-scriptum-index nil (:attrs config) config store cache store-id nil nil)))
  :validate-generation validate-scriptum-generation-key-map
  :mark-generation
  (fn [key-map store]
    (sc/mark-generation store (:snapshot-address key-map)))})
