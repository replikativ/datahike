(ns datahike.index.secondary.scriptum
  "Scriptum/Lucene full-text secondary index backed by immutable generations.

   `:path` is a disposable local segment cache. The authoritative identity is
   the immutable Scriptum snapshot address stored in the Datahike root."
  (:require
   [clojure.core.async :as async]
   [datahike.index.audit :as audit]
   [datahike.index.entity-set :as es]
   [datahike.index.secondary :as sec]
   [datahike.index.secondary.publication :as publication]
   [datahike.migrate.fs :as fs]
   [konserve.memory :refer [new-mem-store]]
   [replikativ.logging :as log]
   [scriptum.core :as sc])
  (:import [datahike.datom Datom]
           [org.apache.lucene.document Document Field$Store LongField StringField
            TextField]))

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
    (not (#{2 3} (:format-version key-map))) :unsupported-format-version
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
                        :format-version #{2 3}
                        :storage-owner :datahike
                        :snapshot-address :uuid}})))
  key-map)

(defn- generation-format-version [config]
  ;; Format 3 changes `_entity_id` from a decimal StringField to LongField.
  ;; Keeping it distinct makes every format-2 adapter fail closed rather than
  ;; silently issuing string filters and deletes against numeric postings.
  (if (= :candidate-only (:payload-mode config)) 3 2))

(defn- validate-scriptum-layout-key-map [key-map config]
  (let [key-map (validate-scriptum-generation-key-map key-map)
        expected (generation-format-version config)]
    (when-not (= expected (:format-version key-map))
      (throw (ex-info
              "Scriptum generation layout does not match its schema configuration."
              {:type :secondary/invalid-scriptum-generation
               :reason :layout-format-mismatch
               :key-map key-map
               :expected-format-version expected
               :payload-mode (:payload-mode config)})))
    key-map))

(defn- attr-name [a]
  ;; Preserve the namespace. `(name :foo/body)` and `(name :bar/body)` both
  ;; produced "body", so equal values collided in `_key` and retracting one
  ;; attribute deleted the other.
  (str a))

(defn- doc-key [eid a value-hash]
  (cond-> (str eid "|" (attr-name a))
    value-hash (str "|" value-hash)))

(defn- add-candidate-doc!
  "Add the fixed, non-authoritative full-text layout without routing every
   field through Scriptum's flexible map decoder. This is the bulk-backfill
   hot path; the Lucene objects themselves are still owned by Scriptum's
   generation writer."
  [generation entity-id value]
  (let [doc (Document.)]
    ;; Datahike entity ids are signed 64-bit integers. Lucene's numeric field
    ;; avoids encoding every distinct decimal id into the term dictionary while
    ;; still supporting exact/set filters, deletion, and stored candidate ids.
    (.add doc (LongField. "_entity_id" (long entity-id) Field$Store/YES))
    (.add doc (TextField. "value" (if (string? value) value (str value))
                          Field$Store/NO))
    (sc/add-document generation doc)))

(defn- query->lucene [{:keys [query field fields] :as query-spec}]
  (cond
    (instance? org.apache.lucene.search.Query query) query
    (= :all query) :all
    (and field (string? query)) (sc/text-query field query)
    (and (seq fields) (string? query))
    (sc/multi-field-query (map attr-name fields) query)
    :else (throw (ex-info "Invalid scriptum query-spec" {:spec query-spec}))))

(defn- filtered-lucene-query [query-spec entity-filter config]
  (let [query (query->lucene query-spec)]
    (if entity-filter
      (let [eids (vec (es/entity-bitset-seq entity-filter))]
        {:query (sc/bool-query
                 [[query :must]
                  [(if (= :candidate-only (:payload-mode config))
                     (LongField/newSetQuery "_entity_id" (long-array eids))
                     (sc/terms-query :_entity_id (map str eids))) :filter]])
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
        ;; A candidate-only generation deliberately omits stored payload and
        ;; can infer its attribute only when the index covers exactly one.
        (when (and (nil? stored) (= 1 (count attrs))) (first attrs))
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
  [snapshot attrs config query-spec entity-filter limit rf init]
  (if-not snapshot
    init
    (let [{:keys [query query-id]}
          (filtered-lucene-query query-spec entity-filter config)]
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

(defn- matching-results [snapshot attrs config query-spec entity-filter limit]
  (persistent!
   (reduce-matching-results snapshot attrs config query-spec entity-filter limit
                            conj! (transient []))))

(declare make-scriptum-index)

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
      (let [{:keys [query]} (filtered-lucene-query query-spec entity-filter config)
            results (sc/search generation query
                               {:limit (or (:limit query-spec) 1000)})
            bitset (es/entity-bitset)]
        (doseq [result results
                :let [eid (result-eid result)]
                :when eid]
          (es/entity-bitset-add! bitset eid))
        bitset)
      (sec/-search source-index query-spec entity-filter)))
  (-estimate [_ query-spec]
    (if @generation* (or (:limit query-spec) 100)
        (sec/-estimate source-index query-spec)))
  (-can-order? [_ _ direction] (= :desc direction))
  (-slice-ordered [_ query-spec entity-filter _ _ limit]
    (if-let [generation @generation*]
      (let [{:keys [query]} (filtered-lucene-query query-spec entity-filter config)
            results (sc/search generation query
                               {:limit (or limit 1000)})]
        (into []
              (keep (fn [result]
                      (when-let [eid (result-eid result)]
                        {:entity-id eid :score (:score result)})))
              results))
      (sec/-slice-ordered source-index query-spec entity-filter nil :desc limit)))
  (-indexed-attrs [_] attrs)
  (-transact [this tx-report]
    (sec/-transact! this tx-report)
    this)

  sec/ITransientSecondaryIndex
  (-as-transient [this] this)
  (-transact! [_ {:keys [^Datom datom added? value-hash secondary-only?]}]
    (let [eid (.-e datom)
          attr (.-a datom)
          value (.-v datom)
          candidate-only? (= :candidate-only (:payload-mode config))
          _ (when (and added? secondary-only? (not (string? value)))
              (throw (ex-info
                      "Scriptum can be authoritative for :db.secondary/only only for string values."
                      {:type :secondary/scriptum-secondary-only-requires-string
                       :attribute attr
                       :value-type (type value)})))
          _ (when (and added? secondary-only? candidate-only?)
              (throw (ex-info
                      "A candidate-only Scriptum index cannot own secondary-only values."
                      {:type :secondary/scriptum-candidate-only-cannot-own-values
                       :attribute attr})))
          ;; Candidate-only/cardinality-one documents are replaced by the
          ;; stable entity+attribute key. Their value remains authoritative in
          ;; Datahike, so computing a cryptographic secondary-only hash per row
          ;; is pure allocation and CPU. Stored/cardinality-many layouts retain
          ;; the value hash needed to distinguish and recover individual
          ;; values.
          value-hash (or value-hash
                         (when-not candidate-only?
                           (sec/secondary-only-hash value)))
          entity-id (str eid)
          key (when-not candidate-only?
                (doc-key eid attr value-hash))
          generation (ensure-generation! generation* source-index config store
                                         cache store-id)]
      (reset! dirty? true)
      (if added?
        (if candidate-only?
          (add-candidate-doc! generation eid value)
          (sc/add-doc generation
                      {:_entity_id {:value entity-id :type :string :store? true}
                       :value {:value (if (string? value) value (str value))
                               :type :text :store? true}
                       ;; `_key` is queried for replacement/retraction but
                       ;; never returned. Lucene postings suffice.
                       :_key {:value key :type :string :store? false}
                       :_attr {:value (attr-name attr)
                               :type :string :store? true}
                       :_vhash {:value value-hash
                                :type :string :store? true}}))
        ;; The sole cardinality-one attribute has at most one document per
        ;; entity, so its already-indexed entity ID is also the update key.
        (if candidate-only?
          (sc/delete-query generation
                           (LongField/newExactQuery "_entity_id" (long eid)))
          (sc/delete-docs generation "_key" key)))))
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
                    (if (satisfies? publication/IUnpublishedGeneration
                                    source-index)
                      (publication/-take-publication-holds!
                       source-index :transferred)
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
                  (publication/complete-holds!
                   (publication/publication-owner :scriptum [])
                   @adopted-holds* sc/abort-generation-publication!)
                  (finally
                    (.close ^java.io.Closeable snapshot)))
                (throw failure))))))
      (finally
        (when source-reserved?
          (publication/-release-derivation! source-index)))))

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
          (publication/-release-derivation! source-index))))))

(defn- make-scriptum-index
  [snapshot attrs config store cache store-id address publication-holds
   supplied-publication-owner]
  (let [attrs (set attrs)
        publication-owner
        (or supplied-publication-owner
            (publication/publication-owner :scriptum publication-holds))]
    (reify
      java.io.Closeable
      (close [_]
        (publication/abort-unpublished!
         publication-owner sc/abort-generation-publication!)
        (when snapshot (.close ^java.io.Closeable snapshot)))

      publication/IUnpublishedGeneration
      (-take-publication-holds! [_ next-state]
        (publication/take-publication-holds! publication-owner next-state))
      (-reserve-derivation! [_]
        (publication/reserve-derivation! publication-owner))
      (-release-derivation! [_]
        (publication/release-derivation! publication-owner))

      sec/ISecondaryIndex
      (-search [_ query-spec entity-filter]
        (reduce-matching-results
         snapshot attrs config query-spec entity-filter nil
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
              (matching-results snapshot attrs config query-spec entity-filter
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
        (let [reserved? (publication/-reserve-derivation! this)]
          (try
            (->TransientScriptumIndex
             (atom nil) this attrs config store cache store-id (atom false)
             (atom false) reserved?)
            (catch Throwable failure
              (when reserved? (publication/-release-derivation! this))
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
                  (filtered-lucene-query query-spec entity-filter config)
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
         :format-version (generation-format-version config)
         :storage-owner :datahike
         :snapshot-address address})
      (-sec-prepare [this _]
        (publication/completed
         (try
           (let [[prepared holds owns? prepared-owner]
                (if (= :unpublished
                       (publication/publication-state publication-owner))
                  (let [holds (publication/-take-publication-holds!
                               this :preparing)]
                    (try
                      [(make-scriptum-index
                        (sc/retain-store-snapshot snapshot)
                        attrs config store cache store-id address []
                        publication-owner)
                       holds true publication-owner]
                      (catch Throwable failure
                        (publication/complete-holds!
                         publication-owner holds
                         sc/abort-generation-publication!)
                        (publication/set-publication-state!
                         publication-owner :aborted)
                        (throw failure))))
                  (if (and address
                           (= :published
                              (publication/publication-state
                               publication-owner)))
                    [this [] false publication-owner]
                    (if address
                      (throw (ex-info "This Scriptum generation already has a publication owner."
                                      {:type :secondary/scriptum-publication-owner-conflict
                                       :state (publication/publication-state
                                               publication-owner)}))
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
                            prepared-owner
                            (publication/publication-owner :scriptum [])
                            _ (publication/set-publication-state!
                               prepared-owner :preparing)
                            prepared (make-scriptum-index
                                      snapshot attrs config store cache
                                      store-id address [] prepared-owner)]
                        [prepared [hold] true prepared-owner]))))]
             (publication/prepared-generation
              prepared holds owns? prepared-owner
              {:root! sc/root-generation-publication!
               :abort! sc/abort-generation-publication!
               :close-prepared! #(.close ^java.io.Closeable %)}))
           (catch Throwable failure
             failure))))
      (-sec-restore [_ restore-store key-map]
        (let [restore-address (:snapshot-address
                               (validate-scriptum-layout-key-map key-map config))
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
    (let [attrs (set (:attrs config))
          _ (when (and (= :candidate-only (:payload-mode config))
                       (not= 1 (count attrs)))
              (throw (ex-info
                      "A candidate-only Scriptum index must cover exactly one attribute."
                      {:type :secondary/scriptum-candidate-only-requires-one-attribute
                       :attributes attrs})))
          _ (when (and (= :candidate-only (:payload-mode config))
                       (not= :one (:cardinality config)))
              (throw (ex-info
                      "A candidate-only Scriptum index requires cardinality-one values."
                      {:type :secondary/scriptum-candidate-only-requires-cardinality-one
                       :cardinality (:cardinality config)})))
          provided-store (::sec/store config)
          store (or provided-store (new-mem-store (atom {}) {:sync? true}))
          store-id (or (::sec/store-id config) (random-uuid))
          cache (or (:path config) (fs/temp-dir! "datahike-scriptum-"))]
      (make-scriptum-index nil attrs config store cache store-id nil [] nil)))
  :storage-owner :datahike
  :validate-generation validate-scriptum-generation-key-map
  :mark-generation
  (fn [key-map store]
    (sc/mark-generation store (:snapshot-address key-map)))})
