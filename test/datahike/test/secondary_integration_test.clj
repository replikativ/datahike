(ns datahike.test.secondary-integration-test
  "Integration tests for Proximum and Scriptum secondary index implementations."
  (:require
   [clojure.test :refer [deftest testing is]]
   [clojure.core.async :as async]
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.gc :as gc]
   [datahike.index.secondary :as sec]
   [datahike.index.entity-set :as es]
   [datahike.query :as q]
   [datahike.query.execute :as execute]
   [datahike.writing :as writing]
   [datahike.index.secondary.scriptum]
   [datahike.index.secondary.stratum]
   [datahike.migrate :as m]
   [datahike.migrate.fs :as fs]
   [konserve.core :as k]
   [konserve.gc-guard :as guard]
   [konserve.memory :refer [new-mem-store]]
   [stratum.api :as st]
   [datahike.test.query-aggregates-test :refer [aggregate-contract]]))

;; Proximum requires Java 22+ (class file version 66.0).
;; Load lazily so the test file compiles on older JVMs.
(def ^:private proximum-available?
  (try
    (require 'datahike.index.secondary.proximum)
    true
    (catch Throwable _ false)))

(declare thrown-data)

(deftest proximum-exact-filter-strategy-reaches-native-search
  (when proximum-available?
    (let [search-results (ns-resolve 'datahike.index.secondary.proximum
                                     'search-results)
          prox-search (requiring-resolve 'proximum.core/search-filtered)
          call* (atom nil)]
      (with-redefs-fn
        {prox-search (fn [& args]
                       (reset! call* args)
                       [])}
        #(search-results ::index
                         {:vector (float-array [1.0])
                          :k 3
                          :ef 40
                          :filter-strategy :exact}
                         (es/entity-bitset-from-longs [10 20])))
      (is (= ::index (nth @call* 0)))
      (is (= 3 (nth @call* 2)))
      (is (= [10 20] (vec (nth @call* 3))))
      (is (= {:ef 40 :filter-strategy :exact}
             (nth @call* 4))))))

(defn lifecycle-search
  "Test-only external engine used to exercise the central lifecycle gate."
  {:datahike/external-engine
   {:index-key 0
    :binding-columns [:entity-id :score]
    :query-spec-fn (fn [args] {:query (first args)})
    :input-vars :all-bound
    :cost-model (fn [_db _idx-ident _args _n-cols]
                  {:estimated-card 1 :cost-per-result 0.01})}}
  [_idx-ident query]
  {:query query})

(defn prepared-secondary-search
  "Test marker for a value-free prepared external-engine plan. Its deliberately
   high cost lets a small primary relation run first, proving that an unrelated
   multi-tuple relation does not obscure the scalar query-spec input."
  {:datahike/external-engine
   {:index-key 0
    :binding-columns [:entity-id]
    :query-spec-fn first
    :input-vars :all-bound
    :cost-model (fn [_db _idx-ident _args _n-cols]
                  {:estimated-card 1000 :cost-per-result 1.0})}}
  [_idx-ident query-spec]
  query-spec)

(defn filtered-secondary-search
  "Test marker for an engine that must consume an upstream entity relation."
  {:datahike/external-engine
   {:index-key 0
    :binding-columns [:entity-id]
    :query-spec-fn first
    :input-vars :all-bound
    :accepts-entity-filter? true
    :requires-entity-filter? true
    :cost-model (fn [_db _idx-ident _args _n-cols]
                  {:estimated-card 100 :cost-per-result 0.02})}}
  [_idx-ident query-spec]
  query-spec)

(defn filtered-secondary-retrieval
  "Test marker for ordered retrieval constrained by an upstream relation."
  {:datahike/external-engine
   {:index-key 0
    :binding-columns [:entity-id :score]
    :query-spec-fn first
    :input-vars :all-bound
    :accepts-entity-filter? true
    :requires-entity-filter? true
    :cost-model (fn [_db _idx-ident _args _n-cols]
                  {:estimated-card 100 :cost-per-result 0.02})}}
  [_idx-ident query-spec]
  query-spec)

(deftest summarizing-domain-bounds-primary-recheck
  (testing "a complete lossy interval domain drives bounded EAVT reads"
    (let [domain-calls (atom [])
          idx (reify
                sec/ISecondaryIndex
                (-search [_ _ _] (es/entity-bitset))
                (-estimate [_ _] 4)
                (-can-order? [_ _ _] false)
                (-slice-ordered [_ _ _ _ _ _] [])
                (-indexed-attrs [_] #{:person/age})
                (-transact [this _] this)

                sec/ISecondaryCandidateDomain
                (-candidate-domain [_ query-spec entity-filter]
                  (swap! domain-calls conj [query-spec entity-filter])
                  ;; False positives 2 and 7 are intentional. The ranges still
                  ;; cover every true match and omit three irrelevant entities.
                  {:domain :entity-intervals
                   :intervals [[2 5] [7 9]]
                   :precision :recheck
                   :recall :complete}))
          base (-> (db/empty-db {:person/age {:db/index true}
                                 :idx/age-summary {:db.secondary/status :ready}})
                   (d/db-with (mapv (fn [[eid age]]
                                      {:db/id eid :person/age age})
                                    [[1 10] [2 20] [3 50] [4 80]
                                     [5 25] [6 30] [7 40] [8 60]])))
          domain (sec/candidate-domain
                  base :idx/age-summary idx {:minimum 50} nil)
          scan-datoms @#'execute/scan-datoms
          candidates (vec (scan-datoms base
                                       '[?e :person/age ?age]
                                       :eavt [] [] 8 domain))
          avet-candidates (vec (scan-datoms base
                                            '[?e :person/age ?age]
                                            :avet [{:op '>=
                                                    :const-val 50
                                                    :var '?age}]
                                            [] 8 domain))
          rechecked (into #{}
                          (comp (filter #(>= (long (.-v ^datahike.datom.Datom %)) 50))
                                (map #(.-e ^datahike.datom.Datom %)))
                          candidates)
          exact (set (d/q '[:find [?e ...]
                            :where
                            [?e :person/age ?age]
                            [(>= ?age 50)]]
                          base))]
      (is (= #{2 3 4 7 8}
             (set (map #(.-e ^datahike.datom.Datom %) candidates)))
          "the physical read touches only the two candidate intervals")
      (is (= #{3 4 8}
             (set (map #(.-e ^datahike.datom.Datom %) avet-candidates)))
          "an entity domain filters, but never replaces value-bounded AVET")
      (is (= exact rechecked #{3 4 8})
          "ordinary primary-value recheck removes every false positive")
      (is (= [[{:minimum 50} nil]] @domain-calls)))))

(deftest external-engine-consumes-upstream-entity-filter
  (testing "a filter-requiring secondary runs after and receives primary candidates"
    (let [seen-filter (atom nil)
          idx (reify sec/ISecondaryIndex
                (-search [_ _ entity-filter]
                  (reset! seen-filter entity-filter)
                  (es/entity-bitset-from-longs [1 2 3]))
                (-estimate [_ _] 3)
                (-can-order? [_ _ _] false)
                (-slice-ordered [_ _ entity-filter _ _ _]
                  (reset! seen-filter entity-filter)
                  [{:entity-id 1 :score 0.1}
                   {:entity-id 2 :score 0.2}
                   {:entity-id 3 :score 0.3}])
                (-indexed-attrs [_] #{:person/priority})
                (-transact [this _] this))
          base (-> (db/empty-db {:person/priority {}
                                 :idx/filter {:db.secondary/status :ready}})
                   (d/db-with [{:db/id 1 :person/priority 10}
                               {:db/id 2 :person/priority 20}
                               {:db/id 3 :person/priority 20}])
                   (assoc :secondary-indices {:idx/filter idx}))
          query '[:find [?e ...]
                  :where
                  [?e :person/priority 20]
                  [(datahike.test.secondary-integration-test/filtered-secondary-search
                    :idx/filter :all) [?e ...]]]]
      (binding [q/*disable-planner* false
                q/*query-result-cache?* false]
        (is (= #{2 3} (set (d/q query base))))
        (is (= #{2 3} (set (es/entity-bitset-seq @seen-filter))))

        (testing "ordered retrieval receives the same upstream bitmap"
          (let [retrieval-query
                '[:find ?e ?score
                  :where
                  [?e :person/priority 20]
                  [(datahike.test.secondary-integration-test/filtered-secondary-retrieval
                    :idx/filter :all) [[?e ?score]]]]]
            (reset! seen-filter nil)
            (is (= #{[2 0.2] [3 0.3]}
                   (set (d/q retrieval-query base))))
            (is (= #{2 3}
                   (set (es/entity-bitset-seq @seen-filter))))))

        (testing "a required filter fails closed when no producer binds it"
          (is (thrown-with-msg?
               clojure.lang.ExceptionInfo
               #"requires a bound entity filter"
               (d/q '[:find [?e ...]
                      :where
                      [(datahike.test.secondary-integration-test/filtered-secondary-search
                        :idx/filter :all) [?e ...]]]
                    base))))))))

(defn- await-secondary-status [conn ident expected]
  (let [deadline (+ (System/currentTimeMillis) 10000)]
    (loop []
      (let [status (get-in (d/db conn) [:schema ident :db.secondary/status])]
        (cond
          (= expected status) status
          (>= (System/currentTimeMillis) deadline)
          (throw (ex-info "Timed out waiting for secondary-index status."
                          {:index ident :expected expected :actual status}))
          :else (do (Thread/sleep 20) (recur)))))))

(defn- thrown-data [f]
  (try
    (f)
    nil
    (catch clojure.lang.ExceptionInfo failure
      (ex-data failure))))

(defn fail-after-scriptum-builder-opens
  "Test transaction function: prove the preceding datom opened Scriptum's
   guarded generation, then fail the primary transaction deliberately."
  [_db store-id]
  (when-not (guard/in-flight? store-id)
    (throw (ex-info "Scriptum builder was not open before the test failure."
                    {:type :test/scriptum-builder-not-open})))
  (throw (ex-info "Deliberate failure after Scriptum mutation."
                  {:type :test/fail-after-scriptum-mutation})))

;; ---------------------------------------------------------------------------
;; Proximum (Vector Search) Tests

(deftest test-proximum-lifecycle
  (when-not proximum-available?
    (is (not proximum-available?) "SKIP: proximum requires Java 22+"))
  (when proximum-available?
    (testing "create, insert, search, delete"
      (let [idx (sec/create-index :proximum
                                  {:attrs #{:person/embedding}
                                   :dim 4 :distance :cosine
                                   :store-config {:backend :memory :id (random-uuid)}}
                                  nil)]
        (is (= #{:person/embedding} (sec/-indexed-attrs idx)))

      ;; Insert 3 vectors via -transact
        (let [d1 (datahike.datom/datom 1 :person/embedding (float-array [1.0 0.0 0.0 0.0]))
              d2 (datahike.datom/datom 2 :person/embedding (float-array [0.0 1.0 0.0 0.0]))
              d3 (datahike.datom/datom 3 :person/embedding (float-array [0.7 0.7 0.0 0.0]))
              idx (-> idx
                      (sec/-transact {:datom d1 :added? true})
                      (sec/-transact {:datom d2 :added? true})
                      (sec/-transact {:datom d3 :added? true}))]

        ;; Estimate
          (is (= 2 (sec/-estimate idx {:k 2})))
          (is (= 3 (sec/-estimate idx {:k 10})))

        ;; Search: all 3 entities returned
          (let [results (sec/-search idx {:vector (float-array [1.0 0.0 0.0 0.0]) :k 3} nil)]
            (is (= 3 (es/entity-bitset-cardinality results)))
            (is (= #{1 2 3} (set (es/entity-bitset-seq results)))))

        ;; Search with entity filter
          (let [filter-bs (es/entity-bitset-from-longs [1 3])
                results (sec/-search idx {:vector (float-array [1.0 0.0 0.0 0.0]) :k 3} filter-bs)]
            (is (= #{1 3} (set (es/entity-bitset-seq results)))))

        ;; Ordered results (by distance ascending)
          (is (sec/-can-order? idx :person/embedding :asc))
          (is (not (sec/-can-order? idx :person/embedding :desc)))
          (let [ordered (sec/-slice-ordered idx {:vector (float-array [1.0 0.0 0.0 0.0]) :k 3}
                                            nil nil :asc nil)]
            (is (= 3 (count ordered)))
            (is (= 1 (:entity-id (first ordered)))) ;; closest
            (is (< (:distance (first ordered)) (:distance (second ordered)))))

          ;; PostgreSQL-style consumers need more than a top-k convenience
          ;; call: candidates must page over one immutable generation and say
          ;; independently whether rows need recheck and whether recall is
          ;; complete. HNSW distances are ordered within the discovered set,
          ;; while corpus membership remains approximate.
          (let [query-spec {:vector (float-array [1.0 0.0 0.0 0.0])
                            :candidate-limit 3}
                page-1 (sec/-candidate-page idx query-spec nil {:limit 2})
                page-2 (sec/-candidate-page
                        idx query-spec nil
                        {:limit 2 :continuation (:continuation page-1)})
                pages (sec/validate-candidate-scan [page-1 page-2])
                candidates (mapcat :candidates pages)]
            (is (= [:recheck :approximate :exact]
                   ((juxt :precision :recall :ordering) page-1)))
            (is (false? (:exhausted? page-1)))
            (is (true? (:exhausted? page-2)))
            (is (= #{1 2 3} (set (map :entity-id candidates))))
            (is (= #{:person/embedding} (set (map :attribute candidates))))
            (is (every? :distance candidates)))

          (let [filter-bs (es/entity-bitset-from-longs [2 3])
                page (sec/-candidate-page
                      idx
                      {:vector (float-array [1.0 0.0 0.0 0.0])
                       :candidate-limit 1}
                      filter-bs
                      {:limit 1})]
            (is (= [3] (mapv :entity-id (:candidates page)))
                "excluded nearest nodes do not consume the filtered page"))

          (let [query-spec {:vector (float-array [1.0 0.0 0.0 0.0])
                            :scan-mode :iterative
                            :strict-order? false
                            :ef 2}
                pages
                (loop [request {:limit 1} pages []]
                  (let [page (sec/-candidate-page idx query-spec nil request)
                        pages (conj pages page)]
                    (if (:exhausted? page)
                      pages
                      (recur {:limit 1 :continuation (:continuation page)}
                             pages))))
                pages (sec/validate-candidate-scan pages)
                final-page (peek pages)]
            (is (= :approximate (:ordering (first pages)))
                "relaxed iterative HNSW does not claim global SQL order")
            (is (= :frontier-empty (:stop-reason final-page)))
            (is (pos? (get-in final-page [:stats :visited-count])))
            (is (= (count (mapcat :candidates pages))
                   (count (distinct (map :entity-id
                                         (mapcat :candidates pages)))))))

          (let [page (sec/-candidate-page
                      idx
                      {:vector (float-array [1.0 0.0 0.0 0.0])
                       :scan-mode :iterative
                       :strict-order? false
                       :ef 2}
                      nil
                      {:limit 1})
                continuation (:continuation page)]
            (is (some? continuation))
            (is (nil? (sec/close-candidate-scan! idx continuation)))
            (is (nil? (sec/close-candidate-scan! idx continuation))
                "early LIMIT/error cleanup is idempotent through the adapter"))

        ;; Non-vector value is silently skipped
          (let [d-str (datahike.datom/datom 4 :person/embedding "not-a-vector")
                idx2 (sec/-transact idx {:datom d-str :added? true})
                results (sec/-search idx2 {:vector (float-array [1.0 0.0 0.0 0.0]) :k 10} nil)]
            (is (= 3 (es/entity-bitset-cardinality results))))

        ;; Delete entity 2. This is the one linear child of `idx`; deriving an
        ;; additional dirty child from an unpublished generation is refused.
          (let [idx-del (sec/-transact idx {:datom d2 :added? false})
                results (sec/-search idx-del {:vector (float-array [0.0 1.0 0.0 0.0]) :k 3} nil)]
            (is (not (es/entity-bitset-contains? results 2)))
            (is (= 2 (es/entity-bitset-cardinality results)))))))))

(deftest proximum-transient-batches-consecutive-inserts
  (when-not proximum-available?
    (is (not proximum-available?) "SKIP: proximum requires Java 22+"))
  (when proximum-available?
    (let [put-batch-var (requiring-resolve 'proximum.generations/put-batch!)
          original-put-batch @put-batch-var
          batch-sizes (atom [])
          idx (sec/create-index
               :proximum
               {:attrs #{:person/embedding}
                :dim 4 :distance :cosine
                :ingest-batch-size 2
                :ingest-parallelism 2
                :store-config {:backend :memory :id (random-uuid)}}
               nil)
          persistent* (atom nil)]
      (try
        (with-redefs-fn
          {put-batch-var
           (fn [builder vectors ids opts]
             (swap! batch-sizes conj [(count vectors) (:parallelism opts)])
             (original-put-batch builder vectors ids opts))}
          (fn []
            (let [transient (sec/-as-transient idx)]
              (doseq [eid (range 1 6)]
                (sec/-transact!
                 transient
                 {:datom (datahike.datom/datom
                          eid :person/embedding
                          (float-array [(float eid) 0.0 0.0 0.0]))
                  :added? true}))
              (reset! persistent* (sec/-persistent! transient)))))
        (is (= [[2 2] [2 2] [1 2]] @batch-sizes)
            "bounded insertion runs use one Proximum fork per batch")
        (is (= 5
               (es/entity-bitset-cardinality
                (sec/-search @persistent*
                             {:vector (float-array [1.0 0.0 0.0 0.0]) :k 5}
                             nil))))
        (finally
          (when @persistent*
            (.close ^java.io.Closeable @persistent*))
          (.close ^java.io.Closeable idx))))))

(deftest proximum-batch-flush-preserves-delete-order
  (when-not proximum-available?
    (is (not proximum-available?) "SKIP: proximum requires Java 22+"))
  (when proximum-available?
    (let [idx (sec/create-index
               :proximum
               {:attrs #{:person/embedding}
                :dim 4 :distance :cosine
                :ingest-batch-size 16
                :store-config {:backend :memory :id (random-uuid)}}
               nil)
          datom (datahike.datom/datom
                 1 :person/embedding (float-array [1.0 0.0 0.0 0.0]))
          transient (sec/-as-transient idx)
          persistent* (atom nil)]
      (try
        (sec/-transact! transient {:datom datom :added? true})
        (sec/-transact! transient {:datom datom :added? false})
        (sec/-transact! transient {:datom datom :added? true})
        (reset! persistent* (sec/-persistent! transient))
        (is (= [1]
               (vec
                (es/entity-bitset-seq
                 (sec/-search @persistent*
                              {:vector (float-array [1.0 0.0 0.0 0.0]) :k 4}
                              nil))))
            "insert/delete/insert is not globally reordered by batching")
        (finally
          (when @persistent*
            (.close ^java.io.Closeable @persistent*))
          (.close ^java.io.Closeable idx))))))

(deftest proximum-unpublished-generation-has-one-linear-owner
  (when-not proximum-available?
    (is (not proximum-available?) "SKIP: proximum requires Java 22+"))
  (when proximum-available?
    (let [idx (sec/create-index
               :proximum
               {:attrs #{:person/embedding}
                :dim 4 :distance :cosine
                :store-config {:backend :memory :id (random-uuid)}}
               nil)
          unpublished (sec/-transact
                       idx
                       {:datom (datahike.datom/datom
                                1 :person/embedding
                                (float-array [1.0 0.0 0.0 0.0]))
                        :added? true})]
      (try
        (let [derivation (sec/-as-transient unpublished)]
          (is (= :secondary/proximum-publication-owner-conflict
                 (:type (thrown-data #(sec/-as-transient unpublished))))
              "fan-out is refused as soon as one transient reserves the source")
          (is (= :secondary/proximum-publication-owner-conflict
                 (:type (thrown-data #(.close ^java.io.Closeable unpublished))))
              "closing cannot invalidate a transient's reserved source")
          (is (identical? unpublished (sec/-persistent! derivation))
              "a clean transient releases its reservation back to its source"))
        (let [retry-derivation (sec/-as-transient unpublished)]
          (sec/-abort-transient! retry-derivation))
        (let [preparation (async/<!! (sec/-sec-prepare unpublished {}))
              second-preparation (async/<!! (sec/-sec-prepare unpublished {}))]
          (is (instance? clojure.lang.ExceptionInfo second-preparation))
          (is (= :secondary/proximum-publication-owner-conflict
                 (:type (ex-data second-preparation))))
          (async/<!! (sec/-sec-release preparation {:status :aborted})))
        (finally
          (.close ^java.io.Closeable unpublished)
          (.close ^java.io.Closeable idx))))))

(deftest proximum-close-retries-publication-cleanup
  (when-not proximum-available?
    (is (not proximum-available?) "SKIP: proximum requires Java 22+"))
  (when proximum-available?
    (let [store-id (random-uuid)
          idx (sec/create-index
               :proximum
               {:attrs #{:person/embedding}
                :dim 4 :distance :cosine
                :store-config {:backend :memory :id store-id}}
               nil)
          unpublished (sec/-transact
                       idx
                       {:datom (datahike.datom/datom
                                1 :person/embedding
                                (float-array [1.0 0.0 0.0 0.0]))
                        :added? true})
          original-done! guard/done!
          calls (atom 0)]
      (try
        (with-redefs [guard/done!
                      (fn [sid token]
                        (if (= 1 (swap! calls inc))
                          (throw (ex-info "injected guard completion failure"
                                          {:type :test/guard-completion-failure}))
                          (original-done! sid token)))]
          (is (= :secondary/proximum-publication-cleanup-failed
                 (:type (thrown-data
                         #(.close ^java.io.Closeable unpublished)))))
          (is (guard/in-flight? store-id)
              "the failed close retains its publication fence")
          (.close ^java.io.Closeable unpublished)
          (is (= 2 @calls))
          (is (not (guard/in-flight? store-id))))
        (finally
          (when (guard/in-flight? store-id)
            (.close ^java.io.Closeable unpublished))
          (.close ^java.io.Closeable idx))))))

(deftest chained-proximum-generations-retain-the-oldest-cutoff
  (when-not proximum-available?
    (is (not proximum-available?) "SKIP: proximum requires Java 22+"))
  (when proximum-available?
    (let [store-id (random-uuid)
          idx (sec/create-index
               :proximum
               {:attrs #{:person/embedding}
                :dim 4 :distance :cosine
                :store-config {:backend :memory :id store-id}}
               nil)
          mutate (fn [source eid x]
                   (sec/-transact
                    source
                    {:datom (datahike.datom/datom
                             eid :person/embedding
                             (float-array [x 0.0 0.0 0.0]))
                     :added? true}))
          first-generation (mutate idx 1 1.0)
          oldest-safe-point (guard/safe-point store-id)
          _ (Thread/sleep 5)
          final-generation (mutate first-generation 2 2.0)]
      (try
        (is (= oldest-safe-point (guard/safe-point store-id)))
        (let [preparation (async/<!! (sec/-sec-prepare final-generation {}))]
          (async/<!! (sec/-sec-release preparation {:status :committed})))
        (is (not (guard/in-flight? store-id)))
        (finally
          (.close ^java.io.Closeable first-generation)
          (.close ^java.io.Closeable final-generation)
          (.close ^java.io.Closeable idx))))))

(deftest proximum-generation-key-maps-fail-closed
  (when-not proximum-available?
    (is (not proximum-available?) "SKIP: proximum requires Java 22+"))
  (when proximum-available?
    (let [idx (sec/create-index
               :proximum
               {:attrs #{:person/embedding}
                :dim 4 :distance :cosine
                :store-config {:backend :memory :id (random-uuid)}}
               nil)
          store-id (random-uuid)
          cases [[{:type :proximum
                   :branch "db"
                   :commit-id (random-uuid)}
                  :legacy-proximum-commit-root]
                 [{:type :not-proximum
                   :format-version 2
                   :storage-owner :external
                   :external-store-id store-id
                   :generation-id (random-uuid)}
                  :wrong-type]
                 [{:type :proximum
                   :format-version 1
                   :storage-owner :external
                   :external-store-id store-id
                   :generation-id (random-uuid)}
                  :unsupported-format-version]
                 [{:type :proximum
                   :format-version 2
                   :storage-owner :datahike
                   :external-store-id store-id
                   :generation-id (random-uuid)}
                  :wrong-storage-owner]
                 [{:type :proximum
                   :format-version 2
                   :storage-owner :external
                   :generation-id (random-uuid)}
                  :invalid-external-store-id]
                 [{:type :proximum
                   :format-version 2
                   :storage-owner :external
                   :external-store-id store-id}
                  :invalid-generation-id]]]
      (try
        (doseq [[key-map reason] cases]
          (let [data (thrown-data #(sec/-sec-restore idx nil key-map))]
            (is (= :secondary/invalid-proximum-generation (:type data)))
            (is (= reason (:reason data)))))
        (doseq [[key-map reason] (remove #(= :wrong-type (second %)) cases)]
          (is (= reason
                 (:reason (thrown-data #(sec/mark-from-key-map key-map nil))))))
        (testing "the obsolete cache-strategy annotation remains harmless"
          (let [legacy-key-map {:type :proximum
                                :format-version 2
                                :storage-owner :external
                                :generation-strategy :full-mmap-copy
                                :external-store-id store-id
                                :generation-id (random-uuid)}]
            (is (= legacy-key-map
                   ((ns-resolve 'datahike.index.secondary.proximum
                                'validate-proximum-generation-key-map)
                    legacy-key-map)))))
        (finally
          (.close ^java.io.Closeable idx))))))

(deftest proximum-refuses-datahikes-primary-store
  (when-not proximum-available?
    (is (not proximum-available?) "SKIP: proximum requires Java 22+"))
  (when proximum-available?
    (let [store-id (random-uuid)
          data (thrown-data
                #(sec/create-index
                  :proximum
                  {:attrs #{:person/embedding}
                   :dim 4 :distance :cosine
                   ::sec/store-id store-id
                   :store-config {:backend :memory :id store-id}}
                  nil))]
      (is (= :secondary/proximum-primary-store (:type data)))
      (is (= store-id (:store-id data))))

    (testing "the live Konserve identity wins over an aliased config id"
      (let [live-id (random-uuid)
            alias-id (random-uuid)
            live-store (k/create-store {:backend :memory :id live-id}
                                       {:sync? true})
            idx (sec/create-index
                 :proximum
                 {:attrs #{:person/embedding}
                  :dim 4 :distance :cosine
                  ::sec/store-id live-id
                  ;; The config UUID differs, but the connected store is the
                  ;; primary store. Only the live identity can detect this.
                  :store live-store
                  :store-config {:backend :memory :id alias-id}}
                 nil)]
        (try
          (let [failure (async/<!! (sec/-sec-prepare idx {}))]
            (is (instance? clojure.lang.ExceptionInfo failure))
            (is (= :proximum/generation-store-forbidden
                   (:type (ex-data failure))))
            (is (= live-id (:store-id (ex-data failure)))))
          (finally
            (.close ^java.io.Closeable idx)))))

    (testing "the published external root names the connected store, not its alias"
      (let [primary-id (random-uuid)
            live-id (random-uuid)
            alias-id (random-uuid)
            live-store (k/create-store {:backend :memory :id live-id}
                                       {:sync? true})
            idx (sec/create-index
                 :proximum
                 {:attrs #{:person/embedding}
                  :dim 4 :distance :cosine
                  ::sec/store-id primary-id
                  :store live-store
                  :store-config {:backend :memory :id alias-id}}
                 nil)
            preparation (async/<!! (sec/-sec-prepare idx {}))
            prepared (sec/-sec-generation-index preparation)]
        (try
          (is (= live-id
                 (:external-store-id
                  (sec/-sec-generation-key-map prepared))))
          (async/<!! (sec/-sec-release preparation {:status :aborted}))
          (finally
            (.close ^java.io.Closeable idx)))))))

(deftest proximum-ambiguous-publication-retains-its-guard
  (when-not proximum-available?
    (is (not proximum-available?) "SKIP: proximum requires Java 22+"))
  (when proximum-available?
    (let [store-id (random-uuid)
          idx (sec/create-index
               :proximum
               {:attrs #{:person/embedding}
                :dim 4 :distance :cosine
                :store-config {:backend :memory :id store-id}}
               nil)
          preparation (async/<!! (sec/-sec-prepare idx {}))
          prepared (sec/-sec-generation-index preparation)
          key-map (sec/-sec-generation-key-map prepared)]
      (try
        (is (= store-id (:external-store-id key-map)))
        (is (= {:secondary-type :proximum
                :external-store-id store-id
                :generation-id (:generation-id key-map)}
               (sec/external-root-from-key-map key-map)))
        (is (guard/in-flight? store-id)
            "the unpublished mmap generation initially holds the store fence")
        (async/<!! (sec/-sec-release preparation {:status :unknown}))
        (is (guard/in-flight? store-id)
            "the head may still land after an indeterminate response, so GC remains fenced")
        (let [restored (sec/-sec-restore idx nil key-map)]
          (try
            (is (= key-map (sec/-sec-generation-key-map restored))
                "the immutable generation remains restorable if the head landed")
            (finally
              (.close ^java.io.Closeable restored))))
        ;; A later authoritative read of the primary head reconciles unknown to
        ;; committed and is the point at which the publication hold can end.
        (async/<!! (sec/-sec-release preparation {:status :committed}))
        (is (not (guard/in-flight? store-id))
            "authoritative reconciliation releases the retained fence")
        (finally
          (.close ^java.io.Closeable prepared)
          (.close ^java.io.Closeable idx))))))

(deftest proximum-delayed-head-publication-reconciles-authoritatively
  (when-not proximum-available?
    (is (not proximum-available?) "SKIP: proximum requires Java 22+"))
  (when proximum-available?
    (let [external-store-id (random-uuid)
          primary-store-id (random-uuid)
          primary-store (k/create-store
                         {:backend :memory :id primary-store-id} {:sync? true})
          idx (sec/create-index
               :proximum
               {:attrs #{:person/embedding}
                :dim 4 :distance :cosine
                ::sec/store-id primary-store-id
                :store-config {:backend :memory :id external-store-id}}
               nil)
          preparation (async/<!! (sec/-sec-prepare idx {}))
          prepared (sec/-sec-generation-index preparation)
          key-map (sec/-sec-generation-key-map prepared)
          attempt-id (random-uuid)
          cid (random-uuid)
          outcome {:status :unknown
                   :attempt-id attempt-id
                   :store primary-store
                   :branch :db
                   :primary-commit-id cid
                   :secondary-index-keys {:idx/vector key-map}}]
      (try
        (async/<!!
         (writing/release-secondary-generations!
          {:idx/vector preparation} outcome))
        (is (guard/in-flight? external-store-id))
        (is (false? (async/<!!
                     (writing/reconcile-secondary-publication! attempt-id)))
            "an error can arrive while the primary head is still absent")
        (is (guard/in-flight? external-store-id))

        ;; Model the storage operation landing after its caller already saw the
        ;; indeterminate result. Only this authoritative root authorizes release.
        (k/assoc primary-store :db
                 {:meta {:datahike/commit-id cid}
                  :secondary-index-keys {:idx/vector key-map}}
                 {:sync? true})
        (is (true? (async/<!!
                    (writing/reconcile-secondary-publication! attempt-id))))
        (is (not (guard/in-flight? external-store-id)))
        (let [restored (sec/-sec-restore idx nil key-map)]
          (try
            (is (= key-map (sec/-sec-generation-key-map restored)))
            (finally
              (.close ^java.io.Closeable restored))))
        (finally
          (.close ^java.io.Closeable prepared)
          (.close ^java.io.Closeable idx))))))

;; ---------------------------------------------------------------------------
;; Scriptum (Full-Text Search) Tests

(deftest test-scriptum-lifecycle
  (testing "create, index documents, search, delete"
    (let [idx (sec/create-index :scriptum
                                {:attrs #{:person/name :person/bio}
                                 :path (fs/temp-dir! "scriptum-test-")}
                                nil)]
      (is (= #{:person/name :person/bio} (sec/-indexed-attrs idx)))

      ;; Index documents
      (let [d1 (datahike.datom/datom 1 :person/name "Alice Johnson")
            d2 (datahike.datom/datom 1 :person/bio "Expert in machine learning and NLP")
            d3 (datahike.datom/datom 2 :person/name "Bob Smith")
            d4 (datahike.datom/datom 2 :person/bio "Database engineer")
            d5 (datahike.datom/datom 3 :person/name "Charlie Brown")
            d6 (datahike.datom/datom 3 :person/bio "Machine learning researcher")]
        (let [t-idx (sec/-as-transient idx)
              _ (doseq [datom [d1 d2 d3 d4 d5 d6]]
                  (sec/-transact! t-idx {:datom datom :added? true}))
              idx (sec/-persistent! t-idx)]

        ;; Search for "machine learning"
          (let [results (sec/-search idx {:query "machine learning" :field :value :limit 10} nil)]
            (is (= #{1 3} (set (es/entity-bitset-seq results)))))
          (is (= 2 (sec/-estimate idx {:query "machine learning"
                                       :field :value})))

        ;; Search for "database"
          (let [results (sec/-search idx {:query "database" :field :value :limit 10} nil)]
            (is (= #{2} (set (es/entity-bitset-seq results)))))
          (is (= 1 (sec/-estimate idx {:query "database" :field :value})))

        ;; Filtered search
          (let [filter-bs (es/entity-bitset-from-longs [3])
                results (sec/-search idx {:query "machine learning" :field :value} filter-bs)]
            (is (= #{3} (set (es/entity-bitset-seq results)))))

        ;; Ordered results
          (is (sec/-can-order? idx :person/bio :desc))
          (is (not (sec/-can-order? idx :person/bio :asc)))
          (let [ordered (sec/-slice-ordered idx {:query "machine learning" :field :value}
                                            nil nil :desc 10)]
            (is (= 2 (count ordered)))
            (is (every? #(contains? % :score) ordered))
            (is (every? #(contains? #{1 3} (:entity-id %)) ordered)))

          ;; Delete one value into a new immutable generation.
          (let [idx' (sec/-transact idx {:datom d1 :added? false})
                results (sec/-search idx' {:query "Alice" :field :value :limit 10} nil)]
            (is (zero? (es/entity-bitset-cardinality results)))
            (is (= #{1}
                   (set (es/entity-bitset-seq
                         (sec/-search idx {:query "Alice" :field :value} nil))))
                "deriving a generation does not mutate its source")))))))

(deftest scriptum-complete-search-pages-past-lucene-top-n
  (testing "set-valued search does not silently truncate at 1000 matches"
    (let [idx (sec/create-index :scriptum
                                {:attrs #{:doc/body}
                                 :path (str "/tmp/scriptum-complete-"
                                            (random-uuid))}
                                nil)
          transient (sec/-as-transient idx)]
      (try
        (doseq [eid (range 1 1106)]
          (sec/-transact! transient
                          {:datom (datahike.datom/datom eid :doc/body
                                                        "common token")
                           :added? true}))
        (let [persistent (sec/-persistent! transient)
              results (sec/-search persistent
                                   {:query "common" :field :value}
                                   nil)]
          (try
            (is (= 1105 (es/entity-bitset-cardinality results)))
            (is (= #{1 1105}
                   (set (filter #{1 1105}
                                (es/entity-bitset-seq results)))))
            (let [filter-bs (es/entity-bitset-from-longs [1105])
                  page (sec/-candidate-page
                        persistent
                        {:query "common" :field :value}
                        filter-bs
                        {:limit 1})]
              (is (= [:recheck :complete :none]
                     ((juxt :precision :recall :ordering) page)))
              (is (= [1105] (mapv :entity-id (:candidates page)))
                  "Lucene intersects entity IDs before candidate LIMIT"))
            (finally
              (.close ^java.io.Closeable persistent))))
        (finally
          ;; A successfully frozen transient transfers its generation to the
          ;; persistent index; abort is consequently a no-op.
          (sec/-abort-transient! transient)
          (.close ^java.io.Closeable idx))))))

(deftest stratum-candidate-pages-preserve-exact-order
  (let [idx (sec/create-index :stratum
                              {:attrs #{:item/price}
                               :ident-ref-map {:item/price 42}}
                              nil)
        transient (sec/-as-transient idx)]
    (doseq [[eid price] [[1 30] [2 10] [3 20] [4 40]]]
      (sec/-transact! transient
                      {:datom (datahike.datom/datom eid 42 price)
                       :added? true}))
    (let [persistent (sec/-persistent! transient)
          spec {:attribute :item/price :direction :asc}
          page-1 (sec/-candidate-page persistent spec nil {:limit 2})
          page-2 (sec/-candidate-page
                  persistent spec nil
                  {:limit 2 :continuation (:continuation page-1)})]
      (is (= [:exact :complete :exact]
             ((juxt :precision :recall :ordering) page-1)))
      (is (= [[2 10] [3 20] [1 30] [4 40]]
             (mapv (juxt :entity-id :value)
                   (mapcat :candidates
                           (sec/validate-candidate-scan [page-1 page-2])))))
      (let [filter-bs (es/entity-bitset-from-longs [4])
            filtered (sec/-candidate-page persistent spec filter-bs {:limit 1})]
        (is (= [[4 40]]
               (mapv (juxt :entity-id :value) (:candidates filtered)))
            "Stratum applies entity filters before exact top-N")))))

(deftest stratum-candidate-pages-have-one-total-order
  (let [idx (sec/create-index :stratum
                              {:attrs #{:item/price :item/other}
                               :ident-ref-map {:item/price 42
                                               :item/other 43}}
                              nil)
        transient (sec/-as-transient idx)]
    ;; Four equal keys exercise the otherwise-unspecified primary-key ties. The
    ;; fifth row carries Stratum's nullable-int64 sentinel so NULL itself crosses
    ;; a one-row page boundary.
    (doseq [eid (range 1 5)]
      (sec/-transact! transient
                      {:datom (datahike.datom/datom eid 42 7)
                       :added? true}))
    (sec/-transact! transient
                    {:datom (datahike.datom/datom 5 42 Long/MIN_VALUE)
                     :added? true})
    (sec/-transact! transient
                    {:datom (datahike.datom/datom 5 43 99)
                     :added? true})
    (let [persistent (sec/-persistent! transient)
          scan (fn [spec]
                 (loop [request {:limit 1}
                        pages []]
                   (let [page (sec/-candidate-page persistent spec nil request)
                         pages (conj pages page)]
                     (if (:exhausted? page)
                       (sec/validate-candidate-scan pages)
                       (recur {:limit 1
                               :continuation (:continuation page)}
                              pages)))))
          asc-pages (scan {:attribute :item/price :direction :asc})
          desc-pages (scan {:attribute :item/price :direction :desc})]
      (is (= [[1 7] [2 7] [3 7] [4 7] [5 nil]]
             (mapv (juxt :entity-id :value)
                   (mapcat :candidates asc-pages)))
          "equal primary keys use eid ASC and NULL remains last for ASC")
      (is (= [[5 nil] [1 7] [2 7] [3 7] [4 7]]
             (mapv (juxt :entity-id :value)
                   (mapcat :candidates desc-pages)))
          "NULL crosses the first DESC page boundary without skipping a row")
      (let [page-1 (sec/-candidate-page
                    persistent {:attribute :item/price :direction :asc}
                    nil {:limit 1})
            continuation (:continuation page-1)
            failure-data
            (fn [index spec entity-filter]
              (try
                (sec/-candidate-page index spec entity-filter
                                     {:limit 1 :continuation continuation})
                nil
                (catch clojure.lang.ExceptionInfo e
                  (ex-data e))))]
        (is (= :secondary/stratum-continuation-mismatch
               (:type (failure-data
                       persistent
                       {:attribute :item/price :direction :desc}
                       nil)))
            "a continuation cannot be spliced into a different query")
        (is (= :secondary/stratum-continuation-mismatch
               (:type (failure-data
                       persistent
                       {:attribute :item/price :direction :asc}
                       (es/entity-bitset-from-longs [1 2 3 4 5]))))
            "a continuation cannot be spliced into another candidate universe")
        (let [next-transient (sec/-as-transient persistent)]
          (sec/-transact! next-transient
                          {:datom (datahike.datom/datom 6 42 7)
                           :added? true})
          (sec/-transact! next-transient
                          {:datom (datahike.datom/datom 6 43 99)
                           :added? true})
          (let [next-generation (sec/-persistent! next-transient)]
            (is (= :secondary/stratum-continuation-mismatch
                   (:type (failure-data
                           next-generation
                           {:attribute :item/price :direction :asc}
                           nil)))
                "an offset continuation is bound to its immutable generation")))))))

(deftest secondary-declaration-removal-is-an-atomic-root-transition
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :writer {:backend :self :writer-ownership :exclusive}
             :schema-flexibility :write
             :max-string-length 0}]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :item/price
                           :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}
                          {:item/price 10}
                          {:item/price 20}])
        (d/transact conn [{:db/ident :idx/price
                           :db.secondary/type :stratum
                           :db.secondary/attrs [:item/price]
                           :db.secondary/config {}}])
        (await-secondary-status conn :idx/price :ready)
        (let [before (d/db conn)
              index-entity (d/q '{:find [?entity .]
                                  :where [[?entity :db/ident :idx/price]]}
                                before)]
          (is (some? (get-in before [:secondary-indices :idx/price])))
          (d/transact conn [[:db/retractEntity index-entity]])
          (let [after (d/db conn)]
            (is (nil? (get-in after [:schema :idx/price])))
            (is (nil? (get-in after [:secondary-indices :idx/price])))
            (is (nil? (get-in (k/get (:store after)
                                     (get-in after [:config :branch])
                                     nil {:sync? true})
                              [:secondary-index-keys :idx/price]))
                "the durable branch root no longer names the generation")
            (is (some? (get-in before [:schema :idx/price])))
            (is (some? (get-in before [:secondary-indices :idx/price])))))
        (finally
          (d/release conn)
          (d/delete-database cfg))))))

;; ---------------------------------------------------------------------------
;; Cross-Index Composition Tests

(deftest test-cross-index-bitmap-composition
  (when-not proximum-available?
    (is (not proximum-available?) "SKIP: proximum requires Java 22+"))
  (when proximum-available?
    (testing "RoaringBitmap flows between Proximum and Scriptum"
      (let [;; Create both indices
            vec-idx (sec/create-index :proximum
                                      {:attrs #{:person/embedding}
                                       :dim 4 :distance :cosine
                                       :store-config {:backend :memory :id (random-uuid)}}
                                      nil)
            ft-idx (sec/create-index :scriptum
                                     {:attrs #{:person/bio}
                                      :path (fs/temp-dir! "scriptum-cross-")}
                                     nil)
          ;; Transact vectors
            vec-idx (-> vec-idx
                        (sec/-transact {:datom (datahike.datom/datom 1 :person/embedding
                                                                     (float-array [1.0 0.0 0.0 0.0]))
                                        :added? true})
                        (sec/-transact {:datom (datahike.datom/datom 2 :person/embedding
                                                                     (float-array [0.0 1.0 0.0 0.0]))
                                        :added? true})
                        (sec/-transact {:datom (datahike.datom/datom 3 :person/embedding
                                                                     (float-array [0.9 0.1 0.0 0.0]))
                                        :added? true}))
            ft-idx (-> ft-idx
                       (sec/-transact
                        {:datom (datahike.datom/datom 1 :person/bio "ML researcher")
                         :added? true})
                       (sec/-transact
                        {:datom (datahike.datom/datom 2 :person/bio "Database admin")
                         :added? true})
                       (sec/-transact
                        {:datom (datahike.datom/datom 3 :person/bio "ML engineer")
                         :added? true}))]

      ;; Fulltext "ML" → entities {1, 3}
        (let [ml-bits (sec/-search ft-idx {:query "ML" :field :value} nil)]
          (is (= #{1 3} (set (es/entity-bitset-seq ml-bits))))

        ;; Use as pre-filter for KNN
          (let [knn-filtered (sec/-search vec-idx
                                          {:vector (float-array [1.0 0.0 0.0 0.0]) :k 3}
                                          ml-bits)]
            (is (= #{1 3} (set (es/entity-bitset-seq knn-filtered))))
          ;; Entity 2 excluded by fulltext filter
            (is (not (es/entity-bitset-contains? knn-filtered 2))))

        ;; AND composition
          (let [knn-all (sec/-search vec-idx
                                     {:vector (float-array [1.0 0.0 0.0 0.0]) :k 2} nil)
                combined (es/entity-bitset-and knn-all ml-bits)]
          ;; KNN top-2 = {1, 3}, ML = {1, 3}, AND = {1, 3}
            (is (= #{1 3} (set (es/entity-bitset-seq combined))))))))))

;; ---------------------------------------------------------------------------
;; In-Transaction Maintenance via d/db-with

(deftest test-in-transaction-maintenance
  (testing "pure d/db-with refuses an externally durable secondary before opening a writer"
    (let [schema {:person/name {:db/index true}
                  :person/bio {}
                  :idx/fulltext {:db.secondary/type :scriptum
                                 :db.secondary/attrs [:person/name :person/bio]
                                 :db.secondary/config {:path (fs/temp-dir! "scriptum-tx-")}}}
          empty-db (db/empty-db schema)
          ft-idx (sec/create-index :scriptum
                                   {:attrs [:person/name :person/bio]
                                    :path (fs/temp-dir! "scriptum-tx-")}
                                   empty-db)
          db (assoc empty-db :secondary-indices {:idx/fulltext ft-idx})
          error (try
                  (d/db-with db [{:db/id 1 :person/name "Alice"
                                  :person/bio "ML researcher"}])
                  nil
                  (catch clojure.lang.ExceptionInfo e (ex-data e)))]
      (is (= :secondary/durable-db-with-unsupported (:type error)))
      (is (= :idx/fulltext (:index-ident error))))))

(deftest test-in-transaction-proximum
  (when-not proximum-available?
    (is (not proximum-available?) "SKIP: proximum requires Java 22+"))
  (when proximum-available?
    (testing "pure d/db-with refuses a durable vector builder before opening native writer state"
      (let [schema {:person/embedding {}
                    :idx/vectors {:db.secondary/type :proximum
                                  :db.secondary/attrs [:person/embedding]
                                  :db.secondary/config {:dim 4 :distance :cosine
                                                        :store-config {:backend :memory
                                                                       :id (random-uuid)}}}}
            empty-db (db/empty-db schema)
            vec-idx (sec/create-index :proximum
                                      {:attrs [:person/embedding]
                                       :dim 4 :distance :cosine
                                       :store-config {:backend :memory :id (random-uuid)}}
                                      empty-db)
            db (assoc empty-db :secondary-indices {:idx/vectors vec-idx})
            error (try
                    (d/db-with db [{:db/id 1
                                    :person/embedding
                                    (float-array [1.0 0.0 0.0 0.0])}])
                    nil
                    (catch clojure.lang.ExceptionInfo e (ex-data e)))]
        (is (= :secondary/durable-db-with-unsupported (:type error)))
        (is (= :idx/vectors (:index-ident error)))))))

(deftest test-proximum-knn-clause
  ;; KNN as a first-class Datalog :where clause via the external-engine
  ;; query-spec-fn. Planner-only (the base engine has no external-engine op).
  (when-not proximum-available?
    (is (not proximum-available?) "SKIP: proximum requires Java 22+"))
  (when proximum-available?
    (binding [q/*disable-planner* false]
      (let [external-store-id (random-uuid)
            cfg {:store {:backend :memory :id (random-uuid)} :schema-flexibility :write}]
        (d/create-database cfg)
        (try
          (let [conn (d/connect cfg)]
            (d/transact conn [{:db/ident :doc/name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
                              {:db/ident :doc/embedding :db/valueType :db.type/float-array
                               :db/cardinality :db.cardinality/one :db.secondary/only true}])
            (d/transact conn [{:db/ident :idx/emb :db.secondary/type :proximum :db.secondary/attrs [:doc/embedding]
                               :db.secondary/config {:dim 4 :distance :cosine :capacity 100
                                                     :store-config {:backend :memory
                                                                    :id external-store-id}}}])
            (Thread/sleep 800)
            (d/transact conn [{:doc/name "east"  :doc/embedding (float-array [1.0 0.0 0.0 0.0])}
                              {:doc/name "east2" :doc/embedding (float-array [0.9 0.1 0.0 0.0])}
                              {:doc/name "north" :doc/embedding (float-array [0.0 1.0 0.0 0.0])}])
            (Thread/sleep 500)
            (let [db (d/db conn)
                  roots (async/<!! (gc/reachable-external-secondary-roots db))
                  current-key-map
                  (get-in (k/get (:store db) (get-in db [:config :branch])
                                 nil {:sync? true})
                          [:secondary-index-keys :idx/emb])]
              (is (<= 2 (count roots))
                  "the current and retained pre-insert generation are both roots")
              (is (every? #(= external-store-id (:external-store-id %)) roots))
              (is (every? #(uuid? (:generation-id %)) roots))
              (is (contains? roots
                             {:secondary-type :proximum
                              :external-store-id external-store-id
                              :generation-id (:generation-id current-key-map)})))
            (testing "retrieval: [[?e ?distance]] binds entity + cosine distance and joins to a scalar attr"
              (let [rows (d/q '[:find ?name ?distance :in $ ?qvec
                                :where [(datahike.index.secondary.proximum/knn :idx/emb ?qvec 3) [[?e ?distance]]]
                                [?e :doc/name ?name]]
                              @conn (float-array [1.0 0.0 0.0 0.0]))
                    m (into {} (map (fn [[n d]] [n d]) rows))]
                (is (= #{"east" "east2" "north"} (set (keys m))))
                (is (< (m "east") (m "east2") (m "north")) "distances present and ordered")
                (is (== 0.0 (m "east")))))
            (testing "filter: [?e ...] composes with a Datalog predicate"
              (let [names (d/q '[:find [?name ...] :in $ ?qvec
                                 :where [(datahike.index.secondary.proximum/knn :idx/emb ?qvec 3) [?e ...]]
                                 [?e :doc/name ?name]
                                 [(clojure.string/starts-with? ?name "east")]]
                               @conn (float-array [1.0 0.0 0.0 0.0]))]
                (is (= #{"east" "east2"} (set names)))))
            (d/release conn))
          (finally (d/delete-database cfg)))))))

;; ---------------------------------------------------------------------------
;; Stratum Entity-Filter Aggregate Tests

(deftest test-stratum-entity-filter-aggregate
  (testing "IColumnarAggregate with entity-filter mask injection"
    (let [idx (sec/create-index :stratum
                                {:attrs #{:person/salary :person/dept}}
                                nil)
          datoms [(datahike.datom/datom 1 :person/salary 50000)
                  (datahike.datom/datom 1 :person/dept "eng")
                  (datahike.datom/datom 2 :person/salary 60000)
                  (datahike.datom/datom 2 :person/dept "eng")
                  (datahike.datom/datom 3 :person/salary 70000)
                  (datahike.datom/datom 3 :person/dept "sales")
                  (datahike.datom/datom 4 :person/salary 80000)
                  (datahike.datom/datom 4 :person/dept "sales")
                  (datahike.datom/datom 5 :person/salary 90000)
                  (datahike.datom/datom 5 :person/dept "eng")]
          t (sec/-as-transient idx)
          _ (doseq [d datoms] (sec/-transact! t {:datom d :added? true}))
          idx (sec/-persistent! t)]

      ;; Full aggregate (no filter)
      (let [result (sec/-columnar-aggregate idx
                                            {:agg [[:avg :salary]] :group [:dept]})]
        (is (= 2 (count result)))
        ;; eng: (50+60+90)/3 = 66666.67, sales: (70+80)/2 = 75000
        (let [eng (first (filter #(= "eng" (:dept %)) result))
              sales (first (filter #(= "sales" (:dept %)) result))]
          (is (< (abs (- (:avg eng) 66666.67)) 1.0))
          (is (== 75000.0 (:avg sales)))))

      ;; Filtered aggregate — only entities {1, 2, 3}
      (let [filter-bs (es/entity-bitset-from-longs [1 2 3])
            result (sec/-columnar-aggregate idx
                                            {:agg [[:avg :salary]] :group [:dept]}
                                            filter-bs)]
        (is (= 2 (count result)))
        ;; eng: (50+60)/2 = 55000, sales: 70/1 = 70000
        (let [eng (first (filter #(= "eng" (:dept %)) result))
              sales (first (filter #(= "sales" (:dept %)) result))]
          (is (== 55000.0 (:avg eng)))
          (is (== 70000.0 (:avg sales)))))

      ;; Filtered aggregate — only entity {5}
      (let [filter-bs (es/entity-bitset-from-longs [5])
            result (sec/-columnar-aggregate idx
                                            {:agg [[:sum :salary]]}
                                            filter-bs)]
        (is (= 1 (count result)))
        (is (== 90000 (:sum (first result))))))))

(deftest test-stratum-partial-coverage-aggregate
  (testing "aggregate with partial coverage: filter via PSS, aggregate via stratum"
    (let [schema {:person/name {:db/index true}
                  :person/salary {}
                  :person/dept {}
                  :idx/analytics {:db.secondary/type :stratum
                                  :db.secondary/attrs [:person/salary :person/dept]}}
          empty-db (db/empty-db schema)
          stratum-idx (sec/create-index :stratum
                                        {:attrs #{:person/salary :person/dept}}
                                        empty-db)
          db (assoc empty-db :secondary-indices {:idx/analytics stratum-idx})
          ;; Add people: some named "Ivan", some not
          db (d/db-with db [{:db/id 1 :person/name "Ivan" :person/salary 50000 :person/dept "eng"}
                            {:db/id 2 :person/name "Ivan" :person/salary 80000 :person/dept "sales"}
                            {:db/id 3 :person/name "Petr" :person/salary 60000 :person/dept "eng"}
                            {:db/id 4 :person/name "Ivan" :person/salary 70000 :person/dept "eng"}
                            {:db/id 5 :person/name "Petr" :person/salary 90000 :person/dept "sales"}])]

      ;; :person/name is NOT in stratum, but :person/salary IS.
      ;; Query: avg salary of Ivans — partial coverage
      (let [result (binding [datahike.query/*disable-planner* false]
                     (d/q '[:find (avg ?s) .
                            :where [?e :person/name "Ivan"] [?e :person/salary ?s]]
                          db))]
        ;; Ivan salaries: 50000 + 80000 + 70000 = 200000 / 3 ≈ 66666.67
        (is (some? result))
        (is (< (abs (- result 66666.67)) 1.0)))

      ;; Verify legacy gives same result
      (let [result-legacy (binding [datahike.query/*disable-planner* true]
                            (d/q '[:find (avg ?s) .
                                   :where [?e :person/name "Ivan"] [?e :person/salary ?s]]
                                 db))]
        (is (< (abs (- result-legacy 66666.67)) 1.0))))))

(deftest test-stratum-predicate-pushdown
  (testing "predicates on covered columns translated to stratum WHERE"
    (let [schema {:person/salary {}
                  :person/dept {}
                  :idx/analytics {:db.secondary/type :stratum
                                  :db.secondary/attrs [:person/salary :person/dept]}}
          empty-db (db/empty-db schema)
          stratum-idx (sec/create-index :stratum
                                        {:attrs #{:person/salary :person/dept}}
                                        empty-db)
          db (assoc empty-db :secondary-indices {:idx/analytics stratum-idx})
          db (d/db-with db [{:db/id 1 :person/salary 50000 :person/dept "eng"}
                            {:db/id 2 :person/salary 80000 :person/dept "sales"}
                            {:db/id 3 :person/salary 60000 :person/dept "eng"}
                            {:db/id 4 :person/salary 70000 :person/dept "eng"}
                            {:db/id 5 :person/salary 90000 :person/dept "sales"}])]

      ;; Predicate filter: salary > 65000
      (let [result (binding [datahike.query/*disable-planner* false]
                     (d/q '[:find (avg ?s) .
                            :where [?e :person/salary ?s] [(> ?s 65000)]]
                          db))]
        ;; Salaries > 65000: 80000, 70000, 90000 → avg = 80000
        (is (some? result))
        (is (== 80000.0 result)))

      ;; Verify legacy gives same result
      (let [result-legacy (binding [datahike.query/*disable-planner* true]
                            (d/q '[:find (avg ?s) .
                                   :where [?e :person/salary ?s] [(> ?s 65000)]]
                                 db))]
        (is (== 80000.0 result-legacy))))))

;; ---------------------------------------------------------------------------
;; Cross-Index Composition: Scriptum → EntityBitSet → Stratum Aggregate

(deftest test-cross-index-scriptum-to-stratum-aggregate
  (testing "scriptum search produces bitmap → constrains stratum aggregate"
    (let [cfg {:store {:backend :memory :id (random-uuid)}
               :writer {:backend :self :writer-ownership :exclusive}
               :keep-history? false :schema-flexibility :write}
          _ (d/create-database cfg)
          conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :person/name :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one :db/index true}
                          {:db/ident :person/bio :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :person/salary :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :person/dept :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}])
        (let [report (d/transact
                      conn [{:db/id -1 :person/name "Alice" :person/bio "ML researcher"
                             :person/salary 90000 :person/dept "eng"}
                            {:db/id -2 :person/name "Bob" :person/bio "Database admin"
                             :person/salary 60000 :person/dept "ops"}
                            {:db/id -3 :person/name "Charlie" :person/bio "ML engineer"
                             :person/salary 80000 :person/dept "eng"}
                            {:db/id -4 :person/name "Diana" :person/bio "PM"
                             :person/salary 70000 :person/dept "eng"}
                            {:db/id -5 :person/name "Eve" :person/bio "ML ops"
                             :person/salary 75000 :person/dept "ops"}])
              ml-eids (set (map (:tempids report) [-1 -3 -5]))]
          (d/transact conn [{:db/ident :idx/fulltext
                             :db.secondary/type :scriptum
                             :db.secondary/attrs [:person/bio]
                             :db.secondary/config
                             {:path (fs/temp-dir! "scriptum-cross-strat-")}}
                            {:db/ident :idx/analytics
                             :db.secondary/type :stratum
                             :db.secondary/attrs [:person/salary :person/dept]}])
          (await-secondary-status conn :idx/fulltext :ready)
          (await-secondary-status conn :idx/analytics :ready)

          (let [db (d/db conn)
                ft (get-in db [:secondary-indices :idx/fulltext])
                st (get-in db [:secondary-indices :idx/analytics])
                ml-entities (sec/-search ft {:query "ML" :field :value} nil)]
            (is (= ml-eids (set (es/entity-bitset-seq ml-entities))))
            (testing "prepared external-engine arguments resolve from scalar :in bindings"
              (let [query '[:find [?name ...]
                            :in $ ?query-spec
                            :where
                            [?e :person/name ?name]
                            [(datahike.test.secondary-integration-test/prepared-secondary-search
                              :idx/fulltext ?query-spec) [?e ...]]]
                    ;; This assertion is specifically about executing the
                    ;; prepared external-engine path. A process-global result
                    ;; cache hit would bypass that path and make test order
                    ;; observable, so keep only the plan cache under test.
                    names (binding [q/*disable-planner* false
                                    q/*query-result-cache?* false
                                    q/*fold-scalar-ins* false
                                    execute/*prepared-execution* true]
                            (d/q query db {:query "ML" :field :value}))]
                (is (= #{"Alice" "Charlie" "Eve"} (set names)))))
            (let [result (sec/-columnar-aggregate
                          st {:agg [[:avg :salary]] :group [:dept]} ml-entities)]
              (is (= 2 (count result)))
              (let [eng (first (filter #(= "eng" (:dept %)) result))
                    ops (first (filter #(= "ops" (:dept %)) result))]
                (is (== 85000.0 (:avg eng)))
                (is (== 75000.0 (:avg ops)))))))
        (finally
          (d/release conn)
          (d/delete-database cfg))))))

;; ---------------------------------------------------------------------------
;; Entity-Filter Constraining Fused Scan (General Non-Aggregate Path)

(deftest test-entity-filter-constrains-fused-scan
  (testing "secondary index search produces entity-filter that constrains PSS scan"
    (let [cfg {:store {:backend :memory :id (random-uuid)}
               :writer {:backend :self :writer-ownership :exclusive}
               :keep-history? false :schema-flexibility :write}
          _ (d/create-database cfg)
          conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :person/name :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one :db/index true}
                          {:db/ident :person/bio :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :person/salary :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}])
        (let [report (d/transact
                      conn [{:db/id -1 :person/name "Alice" :person/bio "ML researcher"
                             :person/salary 90000}
                            {:db/id -2 :person/name "Bob" :person/bio "Database admin"
                             :person/salary 60000}
                            {:db/id -3 :person/name "Charlie" :person/bio "ML engineer"
                             :person/salary 80000}
                            {:db/id -4 :person/name "Diana" :person/bio "PM"
                             :person/salary 70000}
                            {:db/id -5 :person/name "Eve" :person/bio "ML ops"
                             :person/salary 75000}])
              expected (set (map (fn [[tempid name]]
                                   [((:tempids report) tempid) name])
                                 [[-1 "Alice"] [-3 "Charlie"] [-5 "Eve"]]))]
          (d/transact conn [{:db/ident :idx/fulltext
                             :db.secondary/type :scriptum
                             :db.secondary/attrs [:person/bio]
                             :db.secondary/config
                             {:path (fs/temp-dir! "scriptum-fused-")}}])
          (await-secondary-status conn :idx/fulltext :ready)
          (let [db (d/db conn)
                ft (get-in db [:secondary-indices :idx/fulltext])
                ml-entities (sec/-search ft {:query "ML" :field :value} nil)
                all-names (d/q '[:find ?e ?n :where [?e :person/name ?n]] db)
                ml-names (filter (fn [[eid _]]
                                   (es/entity-bitset-contains? ml-entities eid))
                                 all-names)]
            (is (= (set (map first expected))
                   (set (es/entity-bitset-seq ml-entities))))
            (is (= expected (set ml-names)))))
        (finally
          (d/release conn)
          (d/delete-database cfg))))))

;; ---------------------------------------------------------------------------
;; Purge propagation — end-to-end
;;
;; Verifies the GDPR-relevant invariant against a real konserve-backed
;; secondary index: after :db.purge/entity, the purged entity must no
;; longer surface via the secondary index's own search path.

(deftest test-purge-removes-from-scriptum
  (testing "after purge the entity no longer surfaces via Scriptum fulltext"
    (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
               :keep-history? true
               :schema-flexibility :write}
          scriptum-path (fs/temp-dir! "scriptum-purge-test-")
          _ (d/create-database cfg)
          conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :person/name
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one
                           :db/unique :db.unique/identity
                           :db/index true}
                          {:db/ident :person/bio
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}])
        ;; Install scriptum BEFORE adding data so the index sees live add events
        ;; (no async backfill to wait on).
        (d/transact conn [{:db/ident :idx/fulltext
                           :db.secondary/type :scriptum
                           :db.secondary/attrs [:person/name :person/bio]
                           :db.secondary/config {:path scriptum-path}}])
        (Thread/sleep 500)

        (d/transact conn [{:person/name "Alice" :person/bio "Machine learning researcher"}
                          {:person/name "Bob" :person/bio "Database engineer"}])
        (Thread/sleep 500)

        ;; Sanity: Alice and her bio surface via fulltext before purge
        (let [ft (get-in (d/db conn) [:secondary-indices :idx/fulltext])
              by-name (sec/-search ft {:query "Alice" :field :value} nil)
              by-bio  (sec/-search ft {:query "machine" :field :value} nil)]
          (is (pos? (es/entity-bitset-cardinality by-name))
              "Alice should be findable by name before purge")
          (is (pos? (es/entity-bitset-cardinality by-bio))
              "Alice's bio should be findable before purge"))

        ;; Purge Alice's entity (all her datoms across covered attributes)
        (d/transact conn [[:db.purge/entity [:person/name "Alice"]]])
        (Thread/sleep 500)

        ;; After purge: Alice no longer surfaces via either covered attribute
        (let [ft (get-in (d/db conn) [:secondary-indices :idx/fulltext])
              by-name (sec/-search ft {:query "Alice" :field :value} nil)
              by-bio  (sec/-search ft {:query "machine" :field :value} nil)]
          (is (zero? (es/entity-bitset-cardinality by-name))
              "Alice should not be findable by name after purge")
          (is (zero? (es/entity-bitset-cardinality by-bio))
              "Alice's bio should not be findable after purge"))

        ;; Bob is untouched
        (let [ft (get-in (d/db conn) [:secondary-indices :idx/fulltext])
              by-name (sec/-search ft {:query "Bob" :field :value} nil)]
          (is (pos? (es/entity-bitset-cardinality by-name))
              "Bob should still be findable after Alice's purge"))

        (finally
          (d/release conn)
          (d/delete-database cfg))))))

;; ---------------------------------------------------------------------------
;; Columnar aggregate delegate: same contract as the reference implementation

(deftest test-stratum-aggregate-contract
  (testing "the columnar path answers datahike's aggregate contract"
    ;; A fast path may only claim an aggregate it provably computes to the
    ;; contract in `query-aggregates-test/aggregate-contract`. Mapping by NAME
    ;; alone is what let this path return the SAMPLE variance (÷n−1) where the
    ;; reference returns the population one, and ##NaN for a one-element group.
    ;; stratum's population ops are named :variance-pop / :stddev-pop, so the
    ;; adapter must translate rather than pass the name through.
    ;; Keep this test honest. Asserting that the MAPPING TABLE claims these
    ;; aggregates is not enough — it says nothing about whether the query
    ;; reaches the delegate. It did not: the query below was written with a
    ;; `.` (FindScalar) and `columnar-eligible?` requires a FindRel, so both
    ;; bindings ran the reference engine and every assertion compared it to
    ;; itself. Record the delegate's actual invocations instead.
    (is (datahike.index.secondary.stratum/stratum-compatible-aggs?
         [[:variance :num/v] [:stddev :num/v] [:median :num/v] [:avg :num/v]])
        "the columnar path must claim these aggregates")
    (doseq [{:keys [agg in expect note]} aggregate-contract]
      (let [schema {:num/v {}
                    :idx/analytics {:db.secondary/type :stratum
                                    :db.secondary/attrs [:num/v]}}
            empty-db (db/empty-db schema)
            stratum-idx (sec/create-index :stratum {:attrs #{:num/v}} empty-db)
            db (-> (assoc empty-db :secondary-indices {:idx/analytics stratum-idx})
                   (d/db-with (vec (map-indexed (fn [i v] {:db/id (inc i) :num/v v}) in))))
            ;; FindRel, NOT FindScalar: `columnar-eligible?` declines a `.`
            ;; find, which would route this straight past the delegate.
            query {:find [(list agg '?x)] :where '[[?e :num/v ?x]]}
            label (str "(" agg " " (pr-str in) ")" (when note (str " — " note)))
            used (atom [])
            real-agg datahike.index.secondary.stratum/columnar-aggregate
            real-maps datahike.index.secondary.stratum/columnar-aggregate-from-maps
            ;; The result cache MUST be off. The two calls below are the same
            ;; query against the same data, so the second one is a cache hit
            ;; that never executes — the "reference" value would just be the
            ;; columnar result handed back, and the comparison would again be
            ;; the fast path against itself.
            columnar (binding [q/*query-result-cache?* false]
                       (with-redefs [datahike.index.secondary.stratum/columnar-aggregate
                                     (fn [& args] (swap! used conj :cols) (apply real-agg args))
                                     datahike.index.secondary.stratum/columnar-aggregate-from-maps
                                     (fn [& args] (swap! used conj :maps) (apply real-maps args))]
                         (ffirst (binding [q/*disable-planner* false] (d/q query db)))))
            reference (binding [q/*query-result-cache?* false]
                        (ffirst (binding [q/*disable-planner* true] (d/q query db))))]
        (is (seq @used)
            (str label " — the delegate must actually run, or this compares "
                 "the reference engine to itself"))
        (is (and (number? columnar)
                 (< (abs (- (double expect) (double columnar))) 1e-9))
            (str label " columnar"))
        (is (= (double reference) (double columnar))
            (str label " — columnar and reference must agree"))
        ;; a truncating median or an integral avg passes tolerance while still
        ;; being the wrong answer, so pin the type too
        (is (= (double? reference) (double? columnar))
            (str label " — same numeric type on both paths"))))))

(deftest test-stratum-aggregate-eligibility-gates
  (testing "the columnar path declines what it cannot compute to the contract"
    ;; Two entry points reach the columnar aggregate and they did not share
    ;; guards: the fused one (execute-columnar-aggregate) checks arity and column
    ;; type, while the secondary-index one (try-secondary-index-aggregate)
    ;; checked neither, so it answered a different function than the query asked
    ;; for. Both gates now live on both paths.
    (let [mk (fn [vals]
               (let [schema {:num/v {}
                             :idx/analytics {:db.secondary/type :stratum
                                             :db.secondary/attrs [:num/v]}}
                     e (db/empty-db schema)
                     idx (sec/create-index :stratum {:attrs #{:num/v}} e)]
                 (-> (assoc e :secondary-indices {:idx/analytics idx})
                     (d/db-with (vec (map-indexed (fn [i v] {:db/id (inc i) :num/v v}) vals))))))
          agree? (fn [db query]
                   ;; the cache must be off, or the second call is a hit that
                   ;; returns the first engine's answer
                   (binding [q/*query-result-cache?* false]
                     (let [c (binding [q/*disable-planner* false] (d/q query db))
                           r (binding [q/*disable-planner* true] (d/q query db))]
                       [(= c r) c r])))]

      (testing "an aggregate with a count argument is a different function"
        ;; `(min 2 ?x)` returns the two smallest as a COLLECTION. Only the last
        ;; argument was read, so the count was dropped and a scalar returned.
        (doseq [q ['[:find (min 2 ?x) :where [?e :num/v ?x]]
                   '[:find (max 2 ?x) :where [?e :num/v ?x]]]]
          (let [[ok? c r] (agree? (mk [10 15 20 35 75]) q)]
            (is ok? (str q " — columnar " (pr-str c) " vs reference " (pr-str r))))))

      (testing "an aggregate applies to the DEDUPLICATED find projection"
        ;; A Datalog aggregate applies to the answer set. Aggregating inside the
        ;; index counts a repeated value once per datom, so `(count ?x)` over
        ;; 10,10,40 answered 3 where the projection has two members.
        (doseq [q ['[:find (count ?x) :where [?e :num/v ?x]]
                   '[:find (sum ?x) :where [?e :num/v ?x]]
                   '[:find (avg ?x) :where [?e :num/v ?x]]
                   '[:find (variance ?x) :where [?e :num/v ?x]]
                   '[:find (median ?x) :where [?e :num/v ?x]]]]
          (let [[ok? c r] (agree? (mk [10 10 40]) q)]
            (is ok? (str q " over duplicates — columnar " (pr-str c)
                         " vs reference " (pr-str r))))))

      (testing "duplicate-insensitive aggregates keep the fast path"
        (doseq [q ['[:find (min ?x) :where [?e :num/v ?x]]
                   '[:find (max ?x) :where [?e :num/v ?x]]
                   '[:find (count-distinct ?x) :where [?e :num/v ?x]]]]
          (let [[ok? c r] (agree? (mk [10 10 40]) q)]
            (is ok? (str q " — columnar " (pr-str c) " vs reference " (pr-str r)))))))))

(deftest test-building-index-is-not-queried
  (testing "an index that has not been backfilled must not answer"
    ;; A schema-declared secondary index is created with status :building and
    ;; backfilled by the WRITER. Build a db with `d/db-with` and there is no
    ;; writer, so it stays :building forever and accumulates only the datoms of
    ;; transactions made AFTER it was declared. Querying it is a silent wrong
    ;; answer, not a stale one — and the aggregate path never checked the status.
    (let [schema {:num/v {}
                  :idx/analytics {:db.secondary/type :stratum
                                  :db.secondary/attrs [:num/v]}}
          q '[:find (min ?x) :where [?e :num/v ?x]]
          agree? (fn [db]
                   (binding [q/*query-result-cache?* false]
                     [(binding [q/*disable-planner* false] (d/q q db))
                      (binding [q/*disable-planner* true] (d/q q db))]))]

      (testing "declared and populated through db-with — the broken route"
        (let [db (d/db-with (db/empty-db schema)
                            [{:db/id 1 :num/v 10} {:db/id 2 :num/v 30}])
              [planner reference] (agree? db)]
          (is (= :building (get-in db [:schema :idx/analytics :db.secondary/status]))
              "no writer means the backfill never runs")
          (is (= reference planner) "must not answer from a partial index")
          (is (= [[10]] planner))

          (testing "…and still not after a further transaction"
            ;; the index now holds only the LATER datom, so an unguarded read
            ;; answered 50
            (let [db2 (d/db-with db [{:db/id 3 :num/v 50}])
                  [p2 r2] (agree? db2)]
              (is (= r2 p2))
              (is (= [[10]] p2))))))

      (testing "a hand-assembled index is complete by construction and is used"
        (let [e (db/empty-db schema)
              idx (sec/create-index :stratum {:attrs #{:num/v}} e)
              db (-> (assoc e :secondary-indices {:idx/analytics idx})
                     (d/db-with [{:db/id 1 :num/v 10} {:db/id 2 :num/v 30}]))
              [planner reference] (agree? db)]
          (is (nil? (get-in db [:schema :idx/analytics :db.secondary/status])))
          (is (= reference planner))
          (is (= [[10]] planner)))))))

(deftest external-engine-respects-secondary-lifecycle
  (testing "explicit full-text/vector-style clauses fail before touching an unavailable index"
    (let [calls (atom [])
          idx (reify sec/ISecondaryIndex
                (-search [_ _ _]
                  (swap! calls conj :search)
                  (es/entity-bitset-from-longs [1]))
                (-estimate [_ _] 1)
                (-can-order? [_ _ _] true)
                (-slice-ordered [_ _ _ _ _ _]
                  (swap! calls conj :slice)
                  [{:entity-id 1 :score 0.25}])
                (-indexed-attrs [_] #{:doc/body})
                (-transact [this _] this))
          base (-> (db/empty-db {:doc/name {}
                                 :idx/lifecycle {:db.secondary/status :ready}})
                   (d/db-with [{:db/id 1 :doc/name "one"}])
                   (assoc :secondary-indices {:idx/lifecycle idx}))
          filter-q '[:find [?name ...]
                     :where [(datahike.test.secondary-integration-test/lifecycle-search
                              :idx/lifecycle "needle") [?e ...]]
                     [?e :doc/name ?name]]
          retrieval-q '[:find ?e ?score
                        :where [(datahike.test.secondary-integration-test/lifecycle-search
                                 :idx/lifecycle "needle") [[?e ?score]]]]]
      (binding [q/*disable-planner* false
                q/*query-result-cache?* false]
        (doseq [status [:building :disabled :failed]]
          (let [unavailable (assoc-in base [:schema :idx/lifecycle :db.secondary/status]
                                      status)]
            (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                  #"cannot answer queries"
                                  (d/q filter-q unavailable)))
            (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                  #"cannot answer queries"
                                  (d/q retrieval-q unavailable)))))
        (is (empty? @calls) "neither index protocol entry point was called")

        (testing ":ready and legacy nil status remain queryable"
          (is (= ["one"] (d/q filter-q base)))
          (is (= #{[1 0.25]} (d/q retrieval-q base)))
          (let [legacy (update-in base [:schema :idx/lifecycle]
                                  dissoc :db.secondary/status)]
            (is (= ["one"] (d/q filter-q legacy)))
            (is (= #{[1 0.25]} (d/q retrieval-q legacy))))
          (is (= [:search :slice :search :slice] @calls)))))))

;; ---------------------------------------------------------------------------
;; Retraction granularity: a retraction names ONE datom, not the entity

(defn- ^:private retraction-cfg []
  {:store {:backend :memory :id (java.util.UUID/randomUUID)}
   :index :datahike.index/persistent-set
   :keep-history? false :schema-flexibility :write})

(defn- exception-chain [failure]
  (take-while some? (iterate ex-cause failure)))

(deftest failed-transaction-aborts-scriptum-generation
  (testing "a primary failure after a secondary mutation releases every private write"
    (let [store-id (random-uuid)
          cfg {:store {:backend :memory :id store-id}
               :writer {:backend :self :writer-ownership :exclusive}
               :index :datahike.index/persistent-set
               :keep-history? false
               :schema-flexibility :write}
          _ (d/create-database cfg)
          conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :person/bio
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :person/age
                           :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}])
        (d/transact conn [{:db/ident :idx/bio
                           :db.secondary/type :scriptum
                           :db.secondary/attrs [:person/bio]
                           :db.secondary/config
                           {:path (str "/tmp/dh-scriptum-abort-" (random-uuid))}}])
        (await-secondary-status conn :idx/bio :ready)
        (let [eid (get-in (d/transact conn [{:db/id -1 :person/age 20}])
                          [:tempids -1])
              failure (try
                        (d/transact
                         conn [[:db/add eid :person/bio "must not survive"]
                               [:db.fn/call fail-after-scriptum-builder-opens
                                store-id]])
                        nil
                        (catch Throwable e e))]
          (is (some? failure))
          (is (some #{:test/fail-after-scriptum-mutation}
                    (keep (comp :type ex-data) (exception-chain failure)))
              "the tx function observed an open guarded Scriptum generation")
          (is (not (guard/in-flight? store-id))
              "the failed primary transaction aborts the detached generation")
          (is (nil? (sec/-sec-value
                     (get-in (d/db conn) [:secondary-indices :idx/bio])
                     :person/bio eid))
              "the unpublished value is absent from the still-current generation")

          (d/transact conn [[:db/add eid :person/bio "committed value"]])
          (is (= "committed value"
                 (sec/-sec-value
                  (get-in (d/db conn) [:secondary-indices :idx/bio])
                  :person/bio eid))
              "the writer remains usable after cleanup")
          (is (not (guard/in-flight? store-id)))

          (let [before (sec/-sec-generation-key-map
                        (get-in (d/db conn) [:secondary-indices :idx/bio]))]
            (d/transact conn [[:db/add eid :person/age 21]])
            (is (= before
                   (sec/-sec-generation-key-map
                    (get-in (d/db conn) [:secondary-indices :idx/bio])))
                "an unrelated transaction reuses the exact immutable generation")))
        (finally
          (d/release conn)
          (d/delete-database cfg))))))

(deftest chained-scriptum-generations-release-intermediate-guards
  (testing "the final linear owner carries every predecessor guard"
    (let [store-id (random-uuid)
          store (new-mem-store (atom {}) {:sync? true})
          idx (sec/create-index :scriptum
                                {:attrs #{:doc/body}
                                 ::sec/store store
                                 ::sec/store-id store-id
                                 ::sec/index-ident :idx/body
                                 :path (str "/tmp/dh-scriptum-chain-" (random-uuid))}
                                nil)
          mutate (fn [source eid value]
                   (let [transient (sec/-as-transient source)]
                     (sec/-transact! transient
                                     {:datom (datahike.datom/datom
                                              eid :doc/body value)
                                      :added? true})
                     (sec/-persistent! transient)))
          first-generation (mutate idx 1 "first")
          oldest-safe-point (guard/safe-point store-id)
          _ (Thread/sleep 5)
          final-generation (mutate first-generation 2 "second")]
      (try
        (is (guard/in-flight? store-id)
            "the final unpublished generation still protects its objects")
        (is (= oldest-safe-point (guard/safe-point store-id))
            "the child retains its predecessor's older cutoff rather than replacing it")
        (let [preparation (async/<!! (sec/-sec-prepare final-generation {}))]
          (is (satisfies? sec/IPreparedSecondaryGeneration preparation))
          (async/<!! (sec/-sec-release preparation {:status :committed})))
        (is (not (guard/in-flight? store-id))
            "the intermediate and final generation guards are both released")
        (finally
          (.close ^java.io.Closeable first-generation)
          (.close ^java.io.Closeable final-generation))))))

(deftest scriptum-unpublished-generation-has-one-linear-owner
  (let [store-id (random-uuid)
        store (new-mem-store (atom {}) {:sync? true})
        idx (sec/create-index :scriptum
                              {:attrs #{:doc/body}
                               ::sec/store store
                               ::sec/store-id store-id
                               ::sec/index-ident :idx/body
                               :path (str "/tmp/dh-scriptum-owner-" (random-uuid))}
                              nil)
        transient (sec/-as-transient idx)
        _ (sec/-transact! transient
                          {:datom (datahike.datom/datom 1 :doc/body "one")
                           :added? true})
        unpublished (sec/-persistent! transient)]
    (try
      (let [derivation (sec/-as-transient unpublished)]
        (is (= :secondary/scriptum-publication-owner-conflict
               (:type (thrown-data #(sec/-as-transient unpublished)))))
        (is (= :secondary/scriptum-publication-owner-conflict
               (:type (thrown-data #(.close ^java.io.Closeable unpublished)))))
        (is (identical? unpublished (sec/-persistent! derivation))))
      (let [retry-derivation (sec/-as-transient unpublished)]
        (sec/-abort-transient! retry-derivation))
      (let [preparation (async/<!! (sec/-sec-prepare unpublished {}))
            second-preparation (async/<!! (sec/-sec-prepare unpublished {}))]
        (is (instance? clojure.lang.ExceptionInfo second-preparation))
        (is (= :secondary/scriptum-publication-owner-conflict
               (:type (ex-data second-preparation))))
        (async/<!! (sec/-sec-release preparation {:status :aborted})))
      (finally
        (.close ^java.io.Closeable unpublished)
        (.close ^java.io.Closeable idx)))))

(deftest scriptum-close-retries-publication-cleanup
  (let [store-id (random-uuid)
        store (new-mem-store (atom {}) {:sync? true})
        idx (sec/create-index :scriptum
                              {:attrs #{:doc/body}
                               ::sec/store store
                               ::sec/store-id store-id
                               ::sec/index-ident :idx/body
                               :path (str "/tmp/dh-scriptum-close-retry-"
                                          (random-uuid))}
                              nil)
        transient (sec/-as-transient idx)
        _ (sec/-transact! transient
                          {:datom (datahike.datom/datom 1 :doc/body "one")
                           :added? true})
        unpublished (sec/-persistent! transient)
        original-done! guard/done!
        calls (atom 0)]
    (try
      (with-redefs [guard/done!
                    (fn [sid token]
                      (if (= 1 (swap! calls inc))
                        (throw (ex-info "injected guard completion failure"
                                        {:type :test/guard-completion-failure}))
                        (original-done! sid token)))]
        (is (= :secondary/scriptum-publication-cleanup-failed
               (:type (thrown-data
                       #(.close ^java.io.Closeable unpublished)))))
        (is (guard/in-flight? store-id))
        (.close ^java.io.Closeable unpublished)
        (is (= 2 @calls))
        (is (not (guard/in-flight? store-id))))
      (finally
        (when (guard/in-flight? store-id)
          (.close ^java.io.Closeable unpublished))
        (.close ^java.io.Closeable idx)))))

(deftest scriptum-ambiguous-publication-retains-every-guard
  (let [store-id (random-uuid)
        store (new-mem-store (atom {}) {:sync? true})
        idx (sec/create-index :scriptum
                              {:attrs #{:doc/body}
                               ::sec/store store
                               ::sec/store-id store-id
                               ::sec/index-ident :idx/body
                               :path (str "/tmp/dh-scriptum-unknown-" (random-uuid))}
                              nil)
        transient (sec/-as-transient idx)
        _ (sec/-transact! transient
                          {:datom (datahike.datom/datom 1 :doc/body "one")
                           :added? true})
        unpublished (sec/-persistent! transient)
        preparation (async/<!! (sec/-sec-prepare unpublished {}))]
    (try
      (is (guard/in-flight? store-id))
      (async/<!! (sec/-sec-release preparation {:status :unknown}))
      (is (guard/in-flight? store-id))
      (async/<!! (sec/-sec-release preparation {:status :committed}))
      (is (not (guard/in-flight? store-id)))
      (finally
        (.close ^java.io.Closeable unpublished)
        (.close ^java.io.Closeable idx)))))

(deftest scriptum-preserves-keyword-namespaces-in-document-identity
  (testing "same-local-name attributes with equal values do not collide"
    (let [cfg (retraction-cfg)
          _ (d/create-database cfg)
          conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :foo/body :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one
                           :db.secondary/only true}
                          {:db/ident :bar/body :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one
                           :db.secondary/only true}])
        (d/transact conn [{:db/ident :idx/namespaced-body
                           :db.secondary/type :scriptum
                           :db.secondary/attrs [:foo/body :bar/body]
                           :db.secondary/config
                           {:path (str "/tmp/dh-scriptum-ns-" (random-uuid))}}])
        (await-secondary-status conn :idx/namespaced-body :ready)
        (let [eid (get-in (d/transact conn [{:db/id -1
                                             :foo/body "same"
                                             :bar/body "same"}])
                          [:tempids -1])
              current-index #(get-in (d/db conn)
                                     [:secondary-indices :idx/namespaced-body])]
          (is (= "same" (sec/-sec-value (current-index) :foo/body eid)))
          (is (= "same" (sec/-sec-value (current-index) :bar/body eid)))
          (d/transact conn [[:db/retract eid :foo/body "same"]])
          (is (nil? (sec/-sec-value (current-index) :foo/body eid)))
          (is (= "same" (sec/-sec-value (current-index) :bar/body eid))
              "retracting :foo/body leaves :bar/body's equal value intact"))
        (finally
          (d/release conn)
          (d/delete-database cfg))))))

(deftest scriptum-refuses-non-string-authoritative-values
  (testing "Scriptum cannot stringify the only authoritative copy of a typed value"
    (let [cfg (retraction-cfg)
          _ (d/create-database cfg)
          conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :metric/value
                           :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one
                           :db.secondary/only true}])
        (d/transact conn [{:db/ident :idx/metric
                           :db.secondary/type :scriptum
                           :db.secondary/attrs [:metric/value]
                           :db.secondary/config
                           {:path (str "/tmp/dh-scriptum-typed-" (random-uuid))}}])
        (await-secondary-status conn :idx/metric :ready)
        (let [failure (try
                        (d/transact conn [{:metric/value 42}])
                        nil
                        (catch Throwable e e))]
          (is (some #{:secondary/scriptum-secondary-only-requires-string}
                    (keep (comp :type ex-data) (exception-chain failure)))))
        (finally
          (d/release conn)
          (d/delete-database cfg))))))

(deftest scriptum-generation-key-maps-fail-closed
  (let [idx (sec/create-index :scriptum
                              {:attrs #{:doc/body}
                               :path (str "/tmp/dh-scriptum-keymap-" (random-uuid))}
                              nil)
        cases [[{:type :scriptum
                 :format-version 2
                 :storage-owner :datahike}
                :invalid-snapshot-address]
               [{:type :scriptum
                 :format-version 1
                 :storage-owner :datahike
                 :snapshot-address (random-uuid)}
                :legacy-scriptum-v1-generation]
               [{:type :scriptum
                 :path "/tmp/old-scriptum"
                 :branch "db"}
                :legacy-scriptum-v1-generation]
               [{:type :scriptum
                 :format-version 2
                 :storage-owner :external
                 :snapshot-address (random-uuid)}
                :wrong-storage-owner]]]
    (doseq [[key-map reason] cases]
      (let [restore-data (thrown-data #(sec/-sec-restore idx nil key-map))
            mark-data (thrown-data #(sec/mark-from-key-map key-map nil))]
        (is (= :secondary/invalid-scriptum-generation (:type restore-data)))
        (is (= reason (:reason restore-data)))
        (is (= :secondary/invalid-scriptum-generation (:type mark-data)))
        (is (= reason (:reason mark-data)))))))

(deftest stratum-generation-key-maps-fail-closed
  (let [idx (sec/create-index :stratum {:attrs #{:doc/body}} nil)
        cases [[{:type :not-stratum
                 :format-version 1
                 :storage-owner :datahike
                 :dataset-commit-id (random-uuid)}
                :wrong-type]
               [{:type :stratum
                 :format-version 2
                 :storage-owner :datahike
                 :dataset-commit-id (random-uuid)}
                :unsupported-format-version]
               [{:type :stratum
                 :format-version 1
                 :storage-owner :external
                 :dataset-commit-id (random-uuid)}
                :wrong-storage-owner]
               [{:type :stratum
                 :format-version 1
                 :storage-owner :datahike}
                :invalid-dataset-commit-id]]]
    (doseq [[key-map reason] cases]
      (let [data (thrown-data #(sec/-sec-restore idx nil key-map))]
        (is (= :secondary/invalid-stratum-generation (:type data)))
        (is (= reason (:reason data)))))
    (doseq [[key-map reason] (remove #(= :wrong-type (second %)) cases)]
      (is (= reason
             (:reason (thrown-data #(sec/mark-from-key-map key-map nil))))))))

(deftest legacy-stratum-generation-envelope-restores-exactly
  (let [store (new-mem-store (atom {}) {:sync? true})
        skeleton (sec/create-index :stratum {:attrs #{:item/price}}
                                   nil)
        transient (sec/-as-transient skeleton)]
    (sec/-transact! transient
                    {:datom (datahike.datom/datom 7 :item/price 42)
                     :added? true})
    (let [index (sec/-persistent! transient)
          preparation (async/<!! (sec/-sec-prepare index {:store store}))
          prepared (sec/-sec-generation-index preparation)
          key-map (sec/-sec-generation-key-map prepared)
          legacy-key-map (-> key-map
                             (dissoc :format-version :storage-owner)
                             (assoc :branch "db"
                                    :merkle-root (:dataset-commit-id key-map)))
          restored (sec/-sec-restore skeleton store legacy-key-map)]
      (is (= [[7 42]]
             (mapv (juxt :entity-id :value)
                   (:candidates
                    (sec/-candidate-page
                     restored {:attribute :item/price :direction :asc}
                     nil {:limit 10})))))
      (is (= (select-keys key-map
                          [:type :format-version :storage-owner
                           :dataset-commit-id])
             (sec/-sec-generation-key-map restored)))
      (is (true? (async/<!! (sec/-sec-release
                             preparation {:status :committed})))))))

(deftest scriptum-retraction-keeps-the-entitys-other-attributes
  (testing "the retract branch deleted by `_entity_id` alone, which removed
            EVERY document the entity had — all of its other indexed attributes
            with it.

            Measured before the fix: one entity with :doc/body and :doc/title,
            both `:db.secondary/only` and both covered by one scriptum index;
            retracting :doc/body left `-sec-value` returning nil for :doc/title
            while the primary still held its content hash. So the database
            reported the attribute present and the only copy of its text was
            gone — the primary never holds it. `export-db` then refused
            (`:export/secondary-only-unresolvable`), which is how it surfaced;
            before the export learned to check hashes it wrote the HASH as the
            title's value and reported success.

            The fix is a composite `_ea` term, because `sc/delete-docs` takes a
            single Lucene Term and a conjunction is therefore not expressible."
    (let [cfg  (retraction-cfg)
          _    (do (d/delete-database cfg) (d/create-database cfg))
          conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :doc/body :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one :db.secondary/only true}
                          {:db/ident :doc/title :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one :db.secondary/only true}])
        (d/transact conn [{:db/ident :idx/ft :db.secondary/type :scriptum
                           :db.secondary/attrs [:doc/body :doc/title]
                           :db.secondary/config {:path (fs/temp-dir! "dh-retr-")}}])
        (Thread/sleep 800)
        (let [eid (get-in (d/transact conn [{:db/id -1 :doc/body "the whole document"
                                             :doc/title "My Title"}])
                          [:tempids -1])
              idx #(get (:secondary-indices @conn) :idx/ft)]
          (Thread/sleep 500)
          (is (= "the whole document" (sec/-sec-value (idx) :doc/body eid)))
          (is (= "My Title" (sec/-sec-value (idx) :doc/title eid)))

          ;; Retracted by the ORIGINAL value, not by the hash a query returns:
          ;; the retract path re-hashes its argument to find the stored datom,
          ;; so passing the hash back is a silent no-op with empty :tx-data.
          (d/transact conn [[:db/retract eid :doc/body "the whole document"]])
          (Thread/sleep 700)

          (is (nil? (sec/-sec-value (idx) :doc/body eid))
              "the retracted attribute's value is gone, which is the point")
          (is (= "My Title" (sec/-sec-value (idx) :doc/title eid))
              "and the attribute that was NOT retracted still has its value")

          (testing "so the database can still be backed up — the export refuses
                    any :db.secondary/only value it cannot recover, and this one
                    is recoverable again"
            (is (map? (m/export-db @conn (fs/temp-dir! "dh-retr-dump-") {})))))
        (finally (d/release conn))))))

(deftest proximum-covers-exactly-one-attribute
  (when-not proximum-available?
    (is (not proximum-available?) "SKIP: proximum requires Java 22+"))
  (when proximum-available?
    (testing "proximum is keyed by an external id, and this adapter uses the
              entity id — so an index covering two attributes stores both of an
              entity's vectors under one key. Both halves were measured:
              an entity holding both attributes made proximum itself throw
              \"External id already exists\" from inside the async index update,
              naming neither the attribute nor the cause; and two attributes on
              DISJOINT entities refused nothing at all, after which `-sec-value`
              — which ignores its `attr` argument, being able to fetch only by
              id — answered an entity's :p/face with its :p/emb vector.

              Refused at declaration instead, which is also what makes ignoring
              `attr` honest: with one covered attribute there is nothing to
              disambiguate."
      (let [cfg  (retraction-cfg)
            _    (do (d/delete-database cfg) (d/create-database cfg))
            conn (d/connect cfg)]
        (try
          (d/transact conn [{:db/ident :p/emb :db/valueType :db.type/float-array
                             :db/cardinality :db.cardinality/one}
                            {:db/ident :p/face :db/valueType :db.type/float-array
                             :db/cardinality :db.cardinality/one}])
          (let [decl (fn [attrs]
                       (try (d/transact conn [{:db/ident (keyword "idx" (str "v" (count attrs)))
                                               :db.secondary/type :proximum
                                               :db.secondary/attrs attrs
                                               :db.secondary/config
                                               {:dim 4 :distance :cosine
                                                :store-config {:backend :memory
                                                               :id (java.util.UUID/randomUUID)}}}])
                            :accepted
                            (catch Exception e (str (ex-message e)))))]
            (is (re-find #"covers exactly one attribute" (str (decl [:p/emb :p/face])))
                "two attributes are refused, and the message says why")
            (is (= :accepted (decl [:p/emb]))
                "while the supported shape — one index per vector attribute — is unaffected"))
          (finally (d/release conn)))))))

;; ---------------------------------------------------------------------------
;; Stratum: a transaction sets CELLS, and reads see the current row

(defn- ^:private stratum-conn [vt?]
  (let [cfg (retraction-cfg)]
    (d/delete-database cfg) (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn [{:db/ident :p/dept :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one}
                        {:db/ident :p/salary :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/one}])
      (d/transact conn [{:db/ident :idx/an :db.secondary/type :stratum
                         :db.secondary/attrs [:p/dept :p/salary]
                         :db.secondary/config (if vt? {:valid-time true} {})}])
      (Thread/sleep 700)
      conn)))

(deftest stratum-update-touches-one-cell-and-leaves-one-row
  (testing "the adapter used to hand-roll SCD2, and its transient could only
            append a whole row or drop a whole row — `pending-adds` held only
            the columns THIS transaction wrote and was treated as a complete
            row. So a card-one update left a duplicate whose sibling columns
            were nil, and `-sec-value` (`:limit 1`, unordered) returned the
            stale one. Measured before: 2 rows, `:dept nil` on the new one,
            salary 50000 after being set to 60000. `-columnar-aggregate` then
            threw ArrayIndexOutOfBoundsException where it worked on a clean
            dataset.

            It now delegates to stratum's `upsert!`, which merges the cells
            onto the existing row."
    (let [conn (stratum-conn false)]
      (try
        (let [eid (get-in (d/transact conn [{:db/id -1 :p/dept "eng" :p/salary 50000}])
                          [:tempids -1])
              idx #(get (:secondary-indices @conn) :idx/an)]
          (Thread/sleep 500)
          (d/transact conn [{:db/id eid :p/salary 60000}])
          (Thread/sleep 700)
          (is (= 1 (st/row-count (.-dataset (idx)))) "one row, not a duplicate")
          (is (= 60000 (sec/-sec-value (idx) :p/salary eid)) "the current value")
          (is (= "eng" (sec/-sec-value (idx) :p/dept eid))
              "and the column this transaction never mentioned survives")
          (is (= [{:dept "eng" :_count 1 :avg 60000.0}]
                 (sec/-columnar-aggregate (idx) {:group [:dept] :agg [[:avg :salary]]}))
              "the index's own aggregate entry point stops throwing"))
        (finally (d/release conn))))))

(deftest stratum-retraction-clears-one-cell
  (let [conn (stratum-conn false)]
    (try
      (let [eid (get-in (d/transact conn [{:db/id -1 :p/dept "eng" :p/salary 50000}])
                        [:tempids -1])
            idx #(get (:secondary-indices @conn) :idx/an)]
        (Thread/sleep 500)
        (d/transact conn [[:db/retract eid :p/salary 50000]])
        (Thread/sleep 700)
        (is (nil? (sec/-sec-value (idx) :p/salary eid)) "the retracted cell is cleared")
        (is (= "eng" (sec/-sec-value (idx) :p/dept eid))
            "and the entity's other attribute survives — `pending-retracts` names
             [entity attribute], where it used to name the entity and drop its row"))
      (finally (d/release conn)))))

(deftest stratum-vt-mode-reads-the-current-row-and-stamps-a-real-window
  (testing "two fixes that delegation does not give for free. `-sec-value` had
            no current-row selection, so it read a SUPERSEDED row. Corrections
            can leave several system-open belief rows, so the current primary
            value is the newest `[system-from, valid-from]` row — simply
            requiring an open valid interval would reject a finite current
            value. Separately, `tx-meta-for-secondary` found no
            `:db/txInstant` (the in-progress tx entity is not in EAVT yet), so
            every row was stamped `_valid_from 0` AND `_valid_to 0` — a
            zero-width window, valid at no instant."
    (let [conn (stratum-conn true)]
      (try
        (let [eid (get-in (d/transact conn [{:db/id -1 :p/dept "eng" :p/salary 50000}])
                          [:tempids -1])
              idx #(get (:secondary-indices @conn) :idx/an)]
          (Thread/sleep 500)
          (d/transact conn [{:db/id eid :p/salary 60000}])
          (Thread/sleep 700)
          (is (= 60000 (sec/-sec-value (idx) :p/salary eid))
              "the CURRENT generation, not a superseded one")
          (let [rows (st/q {:from (.-dataset (idx))
                            :select [:eid :_valid_from :_valid_to]})]
            (is (every? #(pos? (:_valid_from %)) rows)
                "every row carries a real instant, not 0")
            (is (not-any? #(= (:_valid_from %) (:_valid_to %)) rows)
                "and no row has a zero-width validity window")))
        (finally (d/release conn))))))
