# Secondary Indices

Datahike supports pluggable secondary indices that run alongside the primary B-tree (persistent sorted set) index. Secondary indices enable capabilities that B-trees are not designed for: full-text search, vector similarity (KNN), and columnar analytics.

**Status: Experimental** — The secondary index infrastructure and all integrations are functional but may receive breaking API changes.

## Available Index Types

| Type | Library | Capability | Java Version |
|------|---------|-----------|-------------|
| `:scriptum` | [Scriptum](https://github.com/replikativ/scriptum) | Full-text search (Lucene-based) | 11+ |
| `:proximum` | [Proximum](https://github.com/replikativ/proximum) | Vector similarity / KNN (HNSW) | 22+ |
| `:stratum` | [Stratum](https://github.com/replikativ/stratum) | Columnar aggregates (SIMD-accelerated) | 21+ |

All three are optional dependencies — add them to your `deps.edn` only if needed.

## Full-Text Search with Scriptum

Scriptum provides Lucene-powered full-text search. Define a secondary index via a schema transaction, and Datahike will automatically maintain it.

### Setup

```clojure
(require '[datahike.api :as d])
(require '[datahike.index.secondary :as sec])
(require '[datahike.index.entity-set :as es])
(require '[datahike.index.secondary.scriptum])

;; 1. Create database and define attribute schema
(def cfg {:store {:backend :memory :id (random-uuid)}
          :schema-flexibility :write})
(d/create-database cfg)
(def conn (d/connect cfg))

(d/transact conn [{:db/ident :person/name
                   :db/valueType :db.type/string
                   :db/cardinality :db.cardinality/one
                   :db/index true}
                  {:db/ident :person/bio
                   :db/valueType :db.type/string
                   :db/cardinality :db.cardinality/one}])

;; 2. Add data
(d/transact conn [{:person/name "Alice"   :person/bio "Machine learning researcher"}
                  {:person/name "Bob"     :person/bio "Database administrator"}
                  {:person/name "Charlie" :person/bio "Machine learning engineer"}])

;; 3. Dynamically add a secondary index — backfills existing data automatically
(d/transact conn [{:db/ident :idx/fulltext
                   :db.secondary/type :scriptum
                   :db.secondary/attrs [:person/name :person/bio]
                   :db.secondary/config {:path "/tmp/my-fulltext-index"}}])
;; Wait for backfill to complete (async writer operation)
(Thread/sleep 1000)
```

### Searching

```clojure
(def db (d/db conn))

;; Get the index from the database
(def ft (get-in db [:secondary-indices :idx/fulltext]))

;; Full-text search returns an EntityBitSet of matching entity IDs
(def ml-entities (sec/-search ft {:query "machine learning" :field :value} nil))
(es/entity-bitset-seq ml-entities)
;; => (1 3)

;; Search for "database"
(es/entity-bitset-seq (sec/-search ft {:query "database" :field :value} nil))
;; => (2)

;; Search with entity filter — only consider entities {1, 3}
(def filter-bs (es/entity-bitset-from-longs [1 3]))
(es/entity-bitset-seq (sec/-search ft {:query "database" :field :value} filter-bs))
;; => ()  — Bob (entity 2) excluded by filter
```

### Relevance-Ranked Results

```clojure
;; Ordered results with relevance scores (descending by score)
(sec/-slice-ordered ft {:query "machine learning" :field :value}
                    nil nil :desc 10)
;; => [{:entity-id 1, :score 0.85} {:entity-id 3, :score 0.72}]
```

## Vector Similarity with Proximum

Proximum provides HNSW-based approximate nearest neighbor search. Requires Java 22+.

### Setup

```clojure
(require '[datahike.index.secondary.proximum])

;; Add a vector index to an existing database
(d/transact conn [{:db/ident :person/embedding
                   :db/valueType :db.type/tuple
                   :db/cardinality :db.cardinality/one}])

(d/transact conn [{:db/ident :idx/vectors
                   :db.secondary/type :proximum
                   :db.secondary/attrs [:person/embedding]
                   :db.secondary/config {:dim 4
                                         :distance :cosine
                                         :store-config {:backend :memory
                                                        :id (random-uuid)}}}])
(Thread/sleep 1000)

;; Add vector data
(d/transact conn [{:person/embedding (float-array [1.0 0.0 0.0 0.0])}
                  {:person/embedding (float-array [0.0 1.0 0.0 0.0])}
                  {:person/embedding (float-array [0.9 0.1 0.0 0.0])}])
```

### KNN Search

```clojure
(def db (d/db conn))
(def vt (get-in db [:secondary-indices :idx/vectors]))

;; Find 2 nearest neighbors to query vector
(def results (sec/-search vt {:vector (float-array [1.0 0.0 0.0 0.0]) :k 2} nil))
(es/entity-bitset-seq results)
;; => (1 3)  — entities closest to [1,0,0,0]

;; With distance-ordered results
(sec/-slice-ordered vt {:vector (float-array [1.0 0.0 0.0 0.0]) :k 3}
                    nil nil :asc nil)
;; => [{:entity-id 1, :distance 0.0}
;;     {:entity-id 3, :distance 0.14}
;;     {:entity-id 2, :distance 1.0}]
```

## Columnar Aggregates with Stratum

Stratum provides SIMD-accelerated columnar operations. When a query's aggregate columns are fully covered by a Stratum index, the aggregate is pushed down to Stratum's native engine.

### Setup

```clojure
(require '[datahike.index.secondary.stratum])

(d/transact conn [{:db/ident :person/salary
                   :db/valueType :db.type/long
                   :db/cardinality :db.cardinality/one}
                  {:db/ident :person/dept
                   :db/valueType :db.type/string
                   :db/cardinality :db.cardinality/one}])

(d/transact conn [{:db/ident :idx/analytics
                   :db.secondary/type :stratum
                   :db.secondary/attrs [:person/salary :person/dept]}])
(Thread/sleep 1000)

(d/transact conn [{:person/salary 90000 :person/dept "eng"}
                  {:person/salary 60000 :person/dept "sales"}
                  {:person/salary 80000 :person/dept "eng"}])
```

### Aggregate Queries

Standard Datalog aggregate queries are automatically routed to Stratum when the covered attributes match:

```clojure
(d/q '[:find ?d (avg ?s)
       :where [?e :person/salary ?s] [?e :person/dept ?d]]
     (d/db conn))
;; => [["eng" 85000.0] ["sales" 60000.0]]

(d/q '[:find (avg ?s) .
       :where [?e :person/salary ?s]]
     (d/db conn))
;; => 76666.66666666667
```

Supported aggregate functions: `avg`, `sum`, `count`, `min`, `max`, `variance`, `stddev`, `count-distinct`, `median`.

## Index Lifecycle

Secondary indices are managed through schema transactions:

```clojure
;; Create — transact the index definition
(d/transact conn [{:db/ident :idx/my-index
                   :db.secondary/type :stratum
                   :db.secondary/attrs [:attr1 :attr2]}])

;; Status transitions: :building → :ready → :disabled
;; Check status:
(get-in (d/db conn) [:schema :idx/my-index :db.secondary/status])
;; => :ready

;; Disable — stops index maintenance for new transactions
(d/transact conn [{:db/ident :idx/my-index
                   :db.secondary/status :disabled}])
```

- **`:building`** — Index was just created, backfill in progress. Queries will not use it.
- **`:ready`** — Index is fully populated and maintained on every transaction.
- **`:disabled`** — Index is no longer maintained. Monotonic — cannot go back to `:ready`.

The `:db.secondary/type` and `:db.secondary/attrs` are immutable after creation. To change indexed attributes, create a new index with a different ident.

## Composing Indices with Entity Bitmaps

All secondary indices communicate through `EntityBitSet` — a RoaringBitmap of entity IDs. This enables composing indices:

```clojure
;; Step 1: Full-text search → entity bitmap
(def ml-entities (sec/-search ft-idx {:query "ML" :field :value} nil))
;; => EntityBitSet {1, 3}

;; Step 2: Pass bitmap as entity-filter to KNN
(def knn-results (sec/-search vec-idx
                              {:vector (float-array [1.0 0.0 0.0 0.0]) :k 3}
                              ml-entities))
;; => Only considers entities {1, 3} for KNN

;; Step 3: AND/OR composition
(def combined (es/entity-bitset-and knn-results ml-entities))

;; Step 4: Pass bitmap to columnar aggregate
(sec/-columnar-aggregate st-idx
                         {:agg [[:avg :salary]] :group [:dept]}
                         ml-entities)
;; => [{:dept "eng", :avg 85000.0}]  — only ML people
```

## Schema Reference

Secondary indices are declared via schema transactions:

```clojure
(d/transact conn [{:db/ident            :idx/my-index
                   :db.secondary/type   :scriptum     ;; :scriptum | :proximum | :stratum
                   :db.secondary/attrs  [:attr1 :attr2] ;; attributes to index
                   :db.secondary/config {...}}])       ;; type-specific configuration (optional)
```

### Scriptum Config

| Key | Description | Default |
|-----|-------------|---------|
| `:path` | Directory for Lucene index files | `/tmp/scriptum-<uuid>` |
| `:branch` | Git branch name for versioning | `"main"` |

### Proximum Config

| Key | Description | Default |
|-----|-------------|---------|
| `:dim` | Vector dimensionality | required |
| `:distance` | Distance metric (`:cosine`, `:euclidean`, etc.) | required |
| `:store-config` | Konserve store config for persistence | required |
| `:capacity` | Maximum index capacity | auto |
| `:m` | HNSW M parameter (connectivity) | auto |
| `:ef-construction` | HNSW construction search depth | auto |
| `:ef-search` | HNSW query search depth | auto |

### Stratum Config

| Key | Description | Default |
|-----|-------------|---------|
| `:attrs` | Set of attribute keywords to index | required |

Stratum requires no external storage — it maintains an in-memory columnar dataset that is updated transactionally alongside the primary index.

## Implementing Custom Secondary Indices

Implement the `ISecondaryIndex` protocol to add your own index type:

```clojure
(require '[datahike.index.secondary :as sec])

(defrecord MyIndex [...]
  sec/ISecondaryIndex
  (-search [this query-spec entity-filter]
    ;; Return an EntityBitSet of matching entity IDs
    ...)
  (-estimate [this query-spec]
    ;; Return estimated result count
    ...)
  (-indexed-attrs [this]
    ;; Return #{:attr1 :attr2}
    ...)
  (-transact [this {:keys [datom added?]}]
    ;; Return updated index
    ...)
  (-can-order? [this attr direction] false)
  (-slice-ordered [this query-spec entity-filter attr direction limit] nil))

;; Register your type
(sec/register-index-type! :my-index
  (fn [config db] (->MyIndex ...)))
```

For batch-optimized updates, also implement `ITransientSecondaryIndex`:

```clojure
sec/ITransientSecondaryIndex
(-as-transient [this] ...)    ;; return mutable version
(-transact! [this tx-report]) ;; mutate in place
(-persistent! [this] ...)     ;; freeze back to immutable
```

For columnar aggregate pushdown, implement `IColumnarAggregate`:

```clojure
sec/IColumnarAggregate
(-columnar-aggregate [this query-spec]
  ;; Return seq of result maps: [{:dept "eng" :avg 85000.0} ...]
  ...)
(-columnar-aggregate [this query-spec entity-filter]
  ;; Same but filtered by entity bitmap
  ...)
```
