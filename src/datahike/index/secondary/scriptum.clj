(ns datahike.index.secondary.scriptum
  "Scriptum (Lucene full-text search) integration with Datahike secondary indices.

   Require this namespace to register the :scriptum index type:
     (require 'datahike.index.secondary.scriptum)

   Then declare in schema:
     {:idx/fulltext {:db.secondary/type :scriptum
                     :db.secondary/attrs [:person/name :person/bio]
                     :db.secondary/config {:path \"/tmp/idx\" :analyzer :standard}}}"
  (:require
   [datahike.index.secondary :as sec]
   [datahike.index.entity-set :as es]
   [scriptum.core :as sc]
   [replikativ.logging :as log]))

(defn- make-scriptum-index
  "Create an ISecondaryIndex backed by Scriptum.
   Documents are stored with an '_entity_id' field for entity-level filtering.
   Each attribute value becomes a separate document: {_entity_id, _attr, <field-value>}."
  [writer config]
  (let [attrs (set (:attrs config))]
    (reify sec/ISecondaryIndex
      (-search [_ query-spec entity-filter]
        ;; query-spec: {:query string-or-Query, :field keyword, :limit int}
        ;; Returns EntityBitSet of matching entity IDs
        (let [{:keys [query field limit fields]} query-spec
              limit (or limit 1000)
              lucene-query (cond
                             (instance? org.apache.lucene.search.Query query)
                             query

                             (and field query (string? query))
                             (sc/text-query field query)

                             (and fields query (string? query))
                             (sc/multi-field-query (map name fields) query)

                             :else
                             (throw (ex-info "Invalid scriptum query-spec" {:spec query-spec})))
              results (sc/search writer lucene-query {:limit limit})]
          ;; Filter by entity-filter if provided, build EntityBitSet
          ;; Search results use string keys for stored fields
          (let [bs (es/entity-bitset)]
            (doseq [r results]
              (when-let [eid-str (get r "_entity_id")]
                (try
                  (let [eid (Long/parseLong eid-str)]
                    (when (or (nil? entity-filter)
                              (es/entity-bitset-contains? entity-filter eid))
                      (es/entity-bitset-add! bs eid)))
                  (catch NumberFormatException e
                    (log/warn :datahike/invalid-lucene-eid {:eid-str eid-str})))))
            bs)))

      (-estimate [_ query-spec]
        ;; Rough estimate — search with limit 0 would give TotalHits but
        ;; Scriptum API doesn't expose that. Use a heuristic.
        (or (:limit query-spec) 100))

      (-can-order? [_ _attr direction]
        ;; Lucene results are naturally ordered by relevance score (descending)
        (= direction :desc))

      (-slice-ordered [_ query-spec entity-filter _attr _direction limit]
        ;; Search with score ordering (Lucene natural order)
        (let [{:keys [query field fields]} query-spec
              lucene-query (cond
                             (instance? org.apache.lucene.search.Query query)
                             query
                             (and field query (string? query))
                             (sc/text-query field query)
                             (and fields query (string? query))
                             (sc/multi-field-query (map name fields) query)
                             :else
                             (throw (ex-info "Invalid scriptum query-spec" {:spec query-spec})))
              results (sc/search writer lucene-query {:limit (or limit 1000)})]
          (->> results
               (keep (fn [r]
                       (when-let [eid-str (get r "_entity_id")]
                         (try
                           (let [eid (Long/parseLong eid-str)]
                             (when (or (nil? entity-filter)
                                       (es/entity-bitset-contains? entity-filter eid))
                               {:entity-id eid :score (:score r)}))
                           (catch NumberFormatException _
                             (log/warn :datahike/invalid-lucene-eid {:eid-str eid-str})
                             nil)))))
               vec)))

      (-indexed-attrs [_] attrs)

      sec/IVersionedSecondaryIndex
      (-sec-flush [_ _store branch]
        ;; Scriptum manages its own storage (Lucene files), not konserve.
        ;; Commit the current state and return a key-map for restore.
        (sc/commit! writer "datahike-flush" {"datahike.branch" (name branch)})
        {:type :scriptum
         :path (:path config)
         :branch (or (:branch config) "main")})

      (-sec-restore [_ _store key-map]
        ;; Reopen the Lucene branch at the stored path
        (let [restored-writer (sc/open-branch (:path key-map) (:branch key-map)
                                              (select-keys config [:crypto-hash?]))]
          (make-scriptum-index restored-writer config)))

      (-sec-branch [_ _store _from-branch new-branch]
        ;; Fork the Lucene writer to a new branch (COW via segment sharing)
        (let [forked-writer (sc/fork writer (name new-branch))
              new-config (assoc config :branch (name new-branch))]
          (make-scriptum-index forked-writer new-config)))

      (-sec-mark [_]
        ;; Scriptum uses filesystem, not konserve — nothing to mark
        #{})

      (-transact [this tx-report]
        ;; tx-report: {:datom datom :added? bool}
        (let [{:keys [datom added?]} tx-report
              eid (.-e datom)
              attr (.-a datom)
              val (.-v datom)]
          (if added?
            ;; Add document with entity ID, attribute, and value
            (do
              (sc/add-doc writer
                          {:_entity_id {:value (str eid) :type :string :store? true}
                           :_attr {:value (if (keyword? attr) (name attr) (str attr))
                                   :type :string :store? true}
                           :value (if (string? val) val (str val))})
              this)
            ;; Retract: delete documents matching entity ID
            (do
              (sc/delete-docs writer "_entity_id" (str eid))
              this)))))))

(sec/register-index-type!
 :scriptum
 (fn [config _db]
   (let [path (or (:path config) (str "/tmp/scriptum-" (random-uuid)))
         branch (or (:branch config) "main")
         writer (sc/create-index path branch
                                 (select-keys config [:crypto-hash?]))]
     (make-scriptum-index writer (assoc config :path path :branch branch)))))

;; GC: scriptum uses filesystem, nothing in konserve to mark
(defmethod sec/mark-from-key-map :scriptum [_ _] #{})

;; Branch: fork via scriptum's native segment-sharing fork
(defmethod sec/branch-from-key-map :scriptum [key-map _store _from-branch new-branch]
  (let [writer (sc/open-branch (:path key-map) (:branch key-map))
        forked (sc/fork writer (name new-branch))]
    (sc/commit! forked "branch" {"datahike.branch" (name new-branch)})
    (sc/close! writer)
    (sc/close! forked)
    (assoc key-map :branch (name new-branch))))
