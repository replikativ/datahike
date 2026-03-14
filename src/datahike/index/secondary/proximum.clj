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
   [datahike.index.secondary :as sec]
   [datahike.index.entity-set :as es]
   [proximum.core :as prox]
   [taoensso.timbre :as log]))

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
              (do (log/warn "Non-float-array value for entity" eid "in vector similarity index, skipping:"
                            (type val))
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
