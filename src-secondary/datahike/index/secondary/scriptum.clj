(ns datahike.index.secondary.scriptum
  "Scriptum (Lucene full-text search) integration with Datahike secondary indices.

   Require this namespace to register the :scriptum index type:
     (require 'datahike.index.secondary.scriptum)

   Then declare in schema:
     {:idx/fulltext {:db.secondary/type :scriptum
                     :db.secondary/attrs [:person/name :person/bio]
                     :db.secondary/config {:path \"/tmp/idx\" :analyzer :standard}}}"
  (:require
   [datahike.index.audit :as audit]
   [datahike.index.secondary :as sec]
   [datahike.index.entity-set :as es]
   [scriptum.core :as sc]
   [replikativ.logging :as log]))

(defn- attr-name
  "The attribute as scriptum stores it. `a` is a keyword normally and a numeric
   ref under `:attribute-refs? true`, so both shapes must land on one spelling."
  [a]
  (if (keyword? a) (name a) (str a)))

(defn- ea-key
  "`\"<eid>|<attr>\"` — the term a retraction deletes by. `|` cannot occur in an
   entity id and is not produced by `name` on a keyword, so the two components
   cannot run together into a third meaning."
  [eid a]
  (str eid "|" (attr-name a)))

(defn- make-scriptum-index
  "Create an ISecondaryIndex backed by Scriptum.
   Documents are stored with an '_entity_id' field for entity-level filtering.
   Each attribute value becomes a separate document: {_entity_id, _attr, <field-value>}."
  [writer config]
  (let [attrs (set (:attrs config))]
    (reify
      java.io.Closeable
      (close [_] (sc/close! writer))

      sec/ISecondaryIndex
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

      sec/ISecondaryWarmable
      (-sec-warm! [_ opts]
        ;; Units are Lucene segment FILES (scriptum's own — budgets do not
        ;; translate across index families). Meaningful only for a
        ;; store-backed (konserve) scriptum, where a cold machine fetches its
        ;; segments ahead of Lucene's serial demand; this adapter still opens
        ;; scriptum by PATH, where the files are local by construction and
        ;; scriptum's warm! answers nil — normalized to a zero report here, so
        ;; the delegation is already right when the adapter moves to the
        ;; konserve backing.
        (or (sc/warm! writer opts)
            {:fetched 0 :ms 0.0 :budget-exhausted? false}))

      sec/IVersionedSecondaryIndex
      (-sec-flush [_ _store branch]
        ;; Scriptum manages its own storage (Lucene files), not konserve.
        ;; Commit the current state and return a key-map for restore.
        ;; The merkle-root for audit is exposed via IAuditable below,
        ;; computed from the writer's most recent content-hash.
        (sc/commit! writer "datahike-flush"
                    {"datahike.branch" (name branch)})
        {:type :scriptum
         :path (:path config)
         :branch (or (:branch config) "main")})

      (-sec-restore [_ _store key-map]
        ;; Reopen the Lucene branch at the stored path.
        ;; open-branch handles both main (root) and non-main (branches/) layouts.
        (let [branch-name (:branch key-map)
              restored-writer (sc/open-branch (:path key-map) branch-name
                                              (select-keys config [:crypto-hash?]))]
          (make-scriptum-index restored-writer (assoc config :path (:path key-map) :branch branch-name))))

      (-sec-branch [_ _store _from-branch new-branch]
        ;; Fork the Lucene writer to a new branch (COW via segment sharing)
        (let [forked-writer (sc/fork writer (name new-branch))
              new-config (assoc config :branch (name new-branch))]
          (make-scriptum-index forked-writer new-config)))

      (-sec-mark [_]
        ;; Scriptum uses filesystem, not konserve — nothing to mark
        #{})

      audit/IAuditable
      ;; Scriptum's content-hash is the merkle root over its Lucene
      ;; segments. Available only when the writer was constructed with
      ;; :crypto-hash? true.
      (-merkle-root [_]
        ;; Returns nil pre-commit / pre-crypto-hash; never throws.
        (let [bw (sc/->writer writer)
              h (.getLastContentHash bw)]
          (when h (java.util.UUID/fromString h))))
      (-recompute-merkle-root [_]
        ;; When scriptum.audit (>= 0.1.x audit-chain release) is on the
        ;; classpath we delegate the deep walk to it. Older scriptum
        ;; versions degrade to a local translation of
        ;; sc/verify-commit's {:valid? :commit-id :errors} shape.
        (or (when-let [recompute (try (requiring-resolve 'scriptum.audit/-recompute-merkle-root)
                                      (catch Throwable _ nil))]
              (recompute writer))
            (let [bw (sc/->writer writer)
                  h (.getLastContentHash bw)]
              (cond
                (nil? h)
                {:status :unsupported :reason :crypto-hash-disabled}

                :else
                (let [r (sc/verify-commit writer)
                      root (java.util.UUID/fromString h)]
                  (if (:valid? r)
                    {:status :ok :root root}
                    {:status :mismatch :root nil
                     :errors [{:type :audit/merkle-mismatch
                               :address root
                               :expected root
                               :details (:errors r)}]}))))))

      sec/ISecondaryScannable
      (-sec-value [_ attr eid]
        ;; A term query on the stored `_entity_id`, reading back the stored
        ;; `value`. `-search` above throws that field away — it builds an
        ;; EntityBitSet because that is what a query needs — but the text is
        ;; there: `add-doc` writes `:value` with no `:store?` key and scriptum's
        ;; default is `store? true`, i.e. `Field$Store/YES`.
        ;;
        ;; A point lookup rather than a scan, because the caller is streaming a
        ;; dump and must not accumulate a corpus in memory. `:limit` is small but
        ;; not 1: one entity can hold several attributes, each its own document.
        (let [want (if (keyword? attr) (name attr) (str attr))]
          (some (fn [r] (when (= want (get r "_attr")) (get r "value")))
                (sc/search writer {:term [:_entity_id (str eid)]} {:limit 64}))))

      (-transact [this tx-report]
        ;; tx-report: {:datom datom :added? bool}
        (let [{:keys [datom added?]} tx-report
              eid (.-e datom)
              attr (.-a datom)
              val (.-v datom)]
          (if added?
            ;; Add document with entity ID, attribute, and value.
            ;;
            ;; `_ea` is a COMPOSITE key, `"<eid>|<attr>"`, and it exists only so
            ;; that the retraction below can name this document. Composite
            ;; rather than a conjunction of `_entity_id` and `_attr` because
            ;; `sc/delete-docs` takes a single Lucene Term — so one field that
            ;; already holds both is what makes an exact delete expressible
            ;; without changing scriptum's API.
            (do
              (sc/add-doc writer
                          {:_entity_id {:value (str eid) :type :string :store? true}
                           :_attr {:value (attr-name attr) :type :string :store? true}
                           :_ea {:value (ea-key eid attr) :type :string :store? true}
                           :value (if (string? val) val (str val))})
              this)
            ;; Retract: delete the documents for THIS [entity, attribute].
            ;;
            ;; This used to delete by `_entity_id` alone, which removed every
            ;; document the entity had — all of its other indexed attributes
            ;; with it. Measured: one entity with `:doc/body` and `:doc/title`,
            ;; retracting `:doc/body` left `-sec-value` returning nil for
            ;; `:doc/title` while the primary still held it. For an ordinary
            ;; attribute that is index drift, repairable by a rebuild; for a
            ;; `:db.secondary/only` one the index was the only copy, so it was
            ;; permanent loss of a value the database still reported as present.
            ;;
            ;; Why not by VALUE as well, which would also separate the several
            ;; values of a cardinality-many attribute:
            ;;
            ;;   * for a `:db.secondary/only` attribute it is not possible — the
            ;;     retraction datom carries the content HASH (the transactor
            ;;     rewrites it to search the primary), not the text this index
            ;;     stored, so there is nothing here to match on. Resolving that
            ;;     needs a stored `_vhash`, which is the same field
            ;;     `ISecondaryHashAddressable` needs; until some index wants
            ;;     that, cardinality-many is refused on those attributes anyway,
            ;;     so `[eid attr]` names exactly one document.
            ;;   * for an ordinary cardinality-many attribute the value IS here
            ;;     and this over-deletes the entity's other values of the same
            ;;     attribute. That is the one case still imprecise — index drift
            ;;     with the primary intact, and strictly narrower than the
            ;;     everything-for-this-entity delete it replaces. Keying on the
            ;;     value would mean hashing it into a term on every add, which
            ;;     is new work on the transaction path for every scriptum user
            ;;     to fix a case none of them may have.
            (do
              (sc/delete-docs writer "_ea" (ea-key eid attr))
              this)))))))

(sec/register-index-type!
 :scriptum
 (fn [config _db]
   (let [path (or (:path config) (str "/tmp/scriptum-" (random-uuid)))
         branch (or (:branch config) "main")
         writer (sc/create-index path branch
                                 (select-keys config [:crypto-hash?]))]
     (make-scriptum-index writer (assoc config :path path :branch branch)))))

;; GC: a path-backed index keeps its segments in a Lucene directory on the
;; filesystem, so konserve holds nothing of it to mark and the empty set is the
;; honest answer.
;;
;; That stops being true the moment the index is konserve-backed
;; (scriptum.konserve: blobs under [:scriptum :blob <address>] reachable from
;; [:scriptum :manifest <branch>]). Then the empty set means "unreachable" and
;; the sweep deletes the whole index. So key off what the key-map DECLARES
;; rather than assuming — and refuse rather than guess, because the failure is
;; silent and total.
(defmethod sec/mark-from-key-map :scriptum [key-map _store]
  (if (sec/konserve-backed? key-map)
    (throw (ex-info (str "Konserve-backed scriptum index cannot be marked yet:"
                         " marking must return the branch manifest plus every"
                         " blob it references, or GC will delete the index.")
                    {:key-map key-map}))
    #{}))

;; Branch: open source, fork via scriptum's native segment-sharing fork
(defmethod sec/branch-from-key-map :scriptum [key-map _store _from-branch new-branch]
  (let [writer (sc/open-branch (:path key-map) (:branch key-map))]
    (let [forked (sc/fork writer (name new-branch))]
      (sc/commit! forked "branch" {"datahike.branch" (name new-branch)})
      (sc/close! forked))
    (sc/close! writer)
    (assoc key-map :branch (name new-branch))))
