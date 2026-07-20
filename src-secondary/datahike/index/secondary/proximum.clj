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
   [datahike.index.audit :as audit]
   [datahike.index.secondary :as sec]
   [datahike.index.entity-set :as es]
   [proximum.core :as prox]
   [proximum.crypto :as pcrypto]
   [proximum.protocols :as pproto]
   [proximum.writing :as pwr]
   [proximum.versioning :as pver]
   [proximum.hnsw.internal :as phi]
   [replikativ.logging :as log]
   [clojure.core.async :as async]))

(defn- make-proximum-index
  "Create an ISecondaryIndex backed by Proximum.
   Entity IDs are used as external keys in the Proximum index."
  [prox-idx config]
  (let [attrs (set (:attrs config))]
    (reify sec/ISecondaryIndex
      (-search [_ query-spec entity-filter]
        ;; query-spec: {:vector float-array, :k int}
        ;; Returns EntityBitSet of matching entity IDs
        (let [{:keys [vector k]} query-spec
              results (if entity-filter
                        (prox/search-filtered prox-idx vector k
                                              (fn [ext-id _meta]
                                                (es/entity-bitset-contains? entity-filter (long ext-id))))
                        (prox/search prox-idx vector k))]
          (es/entity-bitset-from-longs (map :id results))))

      (-estimate [_ query-spec]
        ;; For KNN, the result count is exactly k (or less if fewer vectors exist)
        (min (:k query-spec 10) (prox/count-vectors prox-idx)))

      (-can-order? [_ _attr direction]
        ;; Proximum results are naturally ordered by distance (ascending)
        (= direction :asc))

      (-slice-ordered [_ query-spec entity-filter _attr _direction limit]
        ;; KNN results are already distance-ordered; limit is just k
        (let [{:keys [vector k]} query-spec
              effective-k (if limit (min k limit) k)
              results (if entity-filter
                        (prox/search-filtered prox-idx vector effective-k
                                              (fn [ext-id _meta]
                                                (es/entity-bitset-contains? entity-filter (long ext-id))))
                        (prox/search prox-idx vector effective-k))]
          ;; Return as seq of {:entity-id :distance} for the caller to project
          (mapv (fn [{:keys [id distance]}]
                  {:entity-id id :distance distance})
                results)))

      (-indexed-attrs [_] attrs)

      sec/IVersionedSecondaryIndex
      (-sec-flush [_ _store branch]
        ;; Proximum manages its own konserve store internally. The
        ;; protocol method `proximum.protocols/sync!` returns a chan
        ;; that yields a NEW immutable index value carrying the post-
        ;; sync commit-id; the live `prox-idx` field is the pre-sync
        ;; value and stays that way (the bridge has nowhere to put the
        ;; new one). So we wait, read commit-id off `synced`, and
        ;; surface it under both :commit-id (for restore) and
        ;; :merkle-root (for audit's key-map fallback path b in
        ;; writing.cljc) — IAuditable below can't see the post-sync
        ;; state on the live record.
        ;;
        ;; NOTE on the <!!: writing.cljc invokes -sec-flush from inside
        ;; commit!'s go-try-, so blocking from here parks a core.async
        ;; pool thread. Under load this can deadlock the writer. The
        ;; correct fix is making -sec-flush async in the protocol;
        ;; tracked separately. This call is the minimal fix to stop
        ;; throwing NPE (which was caused by previously calling the
        ;; wrong, VectorStore-shaped sync! against an HnswIndex).
        (let [synced (async/<!! (pproto/sync! prox-idx))
              cid    (phi/commit-id synced)]
          {:type :proximum
           :branch (name branch)
           :commit-id cid
           :merkle-root cid
           :store-config (:store-config config)}))

      (-sec-restore [_ _store key-map]
        ;; Restore from proximum's own store using commit ID
        (let [restored (pwr/load-commit (:store-config key-map) (:commit-id key-map)
                                        {:branch (keyword (:branch key-map))})]
          (make-proximum-index restored config)))

      (-sec-branch [_ _store _from-branch new-branch]
        ;; Fork via proximum's native branching (reflink mmap + konserve COW)
        (let [branched (pver/branch! prox-idx (keyword new-branch))]
          (make-proximum-index branched config)))

      (-sec-mark [_]
        ;; Proximum uses its own konserve store, not datahike's
        #{})

      audit/IAuditable
      (-merkle-root [_]
        ;; Proximum's commit-id is a content hash of the HNSW graph
        ;; + vectors state. Returns nil pre-commit; never throws.
        (phi/commit-id prox-idx))
      (-recompute-merkle-root [this]
        ;; When proximum.audit (>= the audit-chain release) is on the
        ;; classpath, delegate the live-index walk to it — that gives
        ;; us actual chunk-level tamper detection (the older
        ;; verify-from-cold only checked existence). Older proximum
        ;; versions fall back to the local translation.
        (or (when-let [recompute (try (requiring-resolve 'proximum.audit/-recompute-merkle-root)
                                      (catch Throwable _ nil))]
              (recompute prox-idx))
            (let [store-config (:store-config config)
                  branch (or (:branch config) :main)
                  result (pcrypto/verify-from-cold store-config branch)
                  root (audit/-merkle-root this)]
              (if (:valid? result)
                {:status :ok :root root}
                {:status :mismatch :root nil
                 :errors [{:type :audit/merkle-mismatch
                           :address root
                           :expected root
                           :details result}]}))))

      (-transact [_ tx-report]
        ;; tx-report: {:datom datom :added? bool}
        (let [{:keys [datom added?]} tx-report
              eid (.-e datom)
              val (.-v datom)]
          (if added?
            ;; Insert: val should be a float-array vector, eid is external key
            (if (instance? (Class/forName "[F") val)
              (make-proximum-index
               (prox/insert prox-idx val eid)
               config)
              (do (log/warn :datahike/non-float-array-vector {:eid eid :type (type val)})
                  (make-proximum-index prox-idx config)))
            ;; Retract: delete by external entity ID
            (make-proximum-index
             (prox/delete prox-idx eid)
             config)))))))

(sec/register-index-type!
 :proximum
 (fn [config _db]
   (let [prox-config (merge {:type :hnsw}
                            (select-keys config [:dim :distance :store-config :mmap-dir
                                                 :capacity :m :ef-construction :ef-search]))
         prox-idx (prox/create-index prox-config)]
     (make-proximum-index prox-idx config))))

;; GC: proximum uses its own store, not datahike's konserve
(defmethod sec/mark-from-key-map :proximum [_ _] #{})

;; Branch: load from stored commit, branch via proximum native
(defmethod sec/branch-from-key-map :proximum [key-map _store _from-branch new-branch]
  (let [idx (pwr/load-commit (:store-config key-map) (:commit-id key-map)
                             {:branch (keyword (:branch key-map))})
        branched (pver/branch! idx (keyword new-branch))
        synced (async/<!! (pproto/sync! branched))
        new-commit-id (phi/commit-id synced)]
    (pproto/close! synced)
    (assoc key-map
           :branch (name new-branch)
           :commit-id new-commit-id)))

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
