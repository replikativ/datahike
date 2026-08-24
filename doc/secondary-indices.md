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
;; Backfill is asynchronous. Poll the schema status until it is :ready;
;; explicit secondary-index queries reject :building indices.
(get-in (d/db conn) [:schema :idx/fulltext :db.secondary/status])
;; => :building, then :ready
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

;; Add a vector index to an existing database. Embeddings are stored as a
;; :db.type/float-array — a whole vector is one scalar value, which is exactly
;; the float[] Proximum indexes.
(d/transact conn [{:db/ident :person/embedding
                   :db/valueType :db.type/float-array
                   :db/cardinality :db.cardinality/one
                   ;; embeddings are search-only derived data — keep them out of
                   ;; the primary index; the covering Proximum index holds them
                   ;; and the primary stores only a content hash. Optional.
                   :db.secondary/only true}])

(d/transact conn [{:db/ident :idx/vectors
                   :db.secondary/type :proximum
                   :db.secondary/attrs [:person/embedding]
                   :db.secondary/config {:dim 4
                                         :distance :cosine
                                         :store-config {:backend :memory
                                                        :id (random-uuid)}}}])
(Thread/sleep 1000)

;; Add vector data — plain float arrays
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

## Storing values only in the secondary (`:db.secondary/only`)

Large or unbounded string values — transcripts, web pages, document bodies — bloat
the primary EAVT/AEVT/AVET indices when stored inline. Flag such an attribute
`:db.secondary/only true` and the value is routed **only to the covering secondary
index**; the primary indices hold a small `hasch` content hash in its place:

```clojure
(d/transact conn [{:db/ident :doc/body
                   :db/valueType :db.type/string :db/cardinality :db.cardinality/one
                   :db.secondary/only true}
                  ;; a covering secondary is REQUIRED — the value lives there
                  {:db/ident :idx/ft :db.secondary/type :scriptum
                   :db.secondary/attrs [:doc/body]
                   :db.secondary/config {:path "/tmp/idx"}}])

(d/transact conn [{:db/id -1 :doc/body "…a very large document…"}])

(d/q '[:find ?v . :where [?e :doc/body ?v]] @conn)
;; => "5c0f…-…"   ; the content hash, NOT the document
```

Because the hash is a deterministic, normal value, uniqueness/cardinality and
retraction all work (`[:db/retract e a v]` re-hashes `v` to find the datom), and
identical content de-duplicates. There is no separate value store and no extra GC:
the value lives in the secondary, which already manages its own storage, GC
(`d/gc-storage`) and branch-on-fork.

Writing such a value with no covering secondary raises — the value would be lost.

### Backup, and which value a datom names

The secondary index is the only place the value exists, so `export-db` reads it
back from there through `ISecondaryScannable`. An index that does not implement
that protocol cannot be backed up losslessly and the export **refuses** rather
than writing a dump whose values are hashes.

`-sec-value` is keyed on `[attr eid]`, and that key does not always identify a
value. Two shapes it cannot express:

* **`:db.cardinality/many`** — one entity, several values, one key.
* **`{:history? true}` over an overwritten value** — the index holds current
  state, so a superseded value is no longer in it.

Both used to fail silently and produce a backup that restores to *different
data*. They no longer can: export knows the content hash each datom names — the
hash **is** the primary's value — and checks what the index hands back against
it. A value it cannot confirm is refused
(`:export/secondary-only-unresolvable`), never guessed.

An index can serve both shapes by implementing **`ISecondaryHashAddressable`**
(`-sec-value-by-hash [this attr eid hash]`). That is a claim about storage, not
about lookup: it says values are kept one per *datom* rather than one per
*entity*, and can be found by content hash. Of the indices shipped alongside
datahike, none declares it yet — Stratum is columnar with one cell per
`[eid column]` and Proximum is keyed by external id, so for those a second value
overwrites the first at write time and no read protocol could recover it.

Consequently a `:db.secondary/only` attribute may be `:db.cardinality/many`
**only** where a covering index declares `ISecondaryHashAddressable`; otherwise
the first such write is refused
(`:transact/secondary-only-multival-unstorable`) instead of silently keeping one
value.

**Semantics — findable, and recoverable only as far as the index allows.** A
full-text or vector index stores a *lossy projection* (tokens, embeddings) for
searching, but it may also retain the original — Scriptum stores the field, which
is what makes the round trip above work. Where it does not, declare the flag only
if the canonical value lives elsewhere (its source URL, a bounded summary
attribute stored normally, your own blob).

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

- **`:building`** — Index was just created and its snapshot is being scanned in the background. New transactions continue normally and their index changes are journaled for the serialized handoff. Explicit secondary-index queries are rejected; planner optimizations fall back to the primary index.
- **`:ready`** — Index is fully populated and maintained on every transaction. Safe for queries.
- **`:disabled`** — Index is no longer maintained. Monotonic — cannot go back to `:ready`. Queries may return stale results.

The `:db.secondary/type` and `:db.secondary/attrs` are immutable after creation. To change indexed attributes, create a new index with a different ident.

## Branching and Versioning

Secondary indices are first-class versioned state. When you branch a database, each secondary index is CoW-forked alongside the primary indices:

```clojure
(require '[datahike.versioning :as dv])

;; Branch — secondary indices are forked automatically
(dv/branch! conn :db :experiment)

;; Connect to the branch — indices are restored from durable storage
(def exp-conn (d/connect (assoc cfg :branch :experiment)))

;; Data and indices on each branch are independent
(d/transact exp-conn [{:person/name "New person on experiment branch"}])

;; Merge — routed through the writer for proper serialization
(dv/merge! conn #{:experiment} [{:person/name "New person on experiment branch"}] nil)
```

Each index type uses its native CoW mechanism:
- **Scriptum**: Lucene segment sharing via `BranchedDirectory` (~3-5ms fork)
- **Stratum**: PSS structural sharing via `dataset/fork` (O(1))
- **Proximum**: Reflink mmap + konserve CoW via `versioning/branch!`

Ready index state is persisted in commits via the `IVersionedSecondaryIndex`
protocol. A building generation is deliberately not flushed, branched, or
published as an audit/GC root: it may contain an arbitrary prefix. If a process
stops during backfill, reconnect creates an empty generation and restarts the
scan from AEVT. A branch made while an index is building likewise starts without
a secondary key-map and rebuilds independently.

Factories invoked for a backfill receive the ephemeral configuration keys
`:datahike.index.secondary/index-ident` and
`:datahike.index.secondary/build-attempt`. An adapter backed by external mutable
storage must namespace its private files/keys by `build-attempt`; the factory
must not reopen a partial generation abandoned by an earlier process. The
attempt identifier is runtime context, not schema, and a successful flush key-map
becomes the durable identity of the ready generation.

### Backfill scalability

The current beta path keeps the writer available during the snapshot scan. It
records concurrent changes in an in-memory, per-index delta journal, then
replays that journal in a short serialized install operation. This closes the
lost-write race for mutable and immutable adapters, but the journal is not
bounded. Registering an index while sustained write volume greatly exceeds
backfill throughput can therefore consume substantial memory.

For that reason this path currently requires the local self writer with
`:writer-ownership :exclusive`. Shared writers cannot coordinate an in-memory
journal across processes, and remote writers cannot transfer a native live
generation between build and install. If pre-existing data requires a backfill,
Datahike rejects those writer configurations before committing the index schema.
Empty indices can still become ready immediately because no asynchronous handoff
is needed.

The intended scalable follow-up is a resumable generation protocol:

1. Capture and durably pin a base commit.
2. Scan the snapshot in bounded, checkpointed batches.
3. Catch up through successive `datahike.experimental.diff/tx-range` windows.
4. Validate the build generation and install it inside one writer operation.

`tx-range` currently requires `:keep-history? true`, persistent-set indices,
and materializes each requested window, so it cannot yet replace the general
path. Its base commit also needs a persistent GC root. `datahike.gc-guard`
protects newly written nodes before their pointer is published; it does **not**
by itself pin old snapshot nodes for a long-running reader. The current builder
therefore holds a process-local read lease which makes offline and online GC
defer until installation. The experimental `feat/gc-roots` work is the relevant
starting point for a durable/resumable lease and must also be integrated with
online GC.

## Purge propagation

`:db/purge` / `:db.purge/entity` / `:db.purge/attribute` / `:db.history.purge/before` route a retraction event (`-transact` with `:added? false`) to every secondary index covering an affected attribute, the same way `:db/retract` does. After purge:

- **Scriptum** no longer returns the purged datom from full-text search.
- **Proximum** skips it on KNN queries (HNSW mark-delete).
- **Stratum** excludes it from columnar aggregates (columnar rewrite).

Two storage-layer caveats:

### Konserve-backed indices (Stratum, Proximum)

Stratum and Proximum store their durable state in konserve. `d/gc-storage` reclaims their unreachable blobs alongside the primary indices the same way: unreachable storage is swept once it ages past the grace-period cutoff, reachable structure persists. No extra step beyond the standard [purge + cutoff-GC](./gc.md) recipe.

### Scriptum (filesystem)

Scriptum's Lucene segments live on the writer node's local disk, not in konserve. Two consequences for erasure:

1. **`d/gc-storage` cannot reach Scriptum's segments.** Scriptum's `-sec-mark` returns the empty set, so the konserve sweep skips Lucene segment files.
2. **Lucene's own delete model is tombstones-until-segment-merge.** A purge marks the document as deleted in Scriptum's index, but the bytes linger inside the segment file until Lucene merges that segment away.

For full Scriptum erasure you typically need to:
- Force a Lucene segment merge so the purged document's bytes are physically removed from segments. Scriptum exposes this via its own API; see the [Scriptum repo](https://github.com/replikativ/scriptum).
- Confirm the writer's filesystem snapshot / backup policy doesn't pin old segment files (NFS, ZFS snapshots, filesystem-level backups all retain segments on their own terms).

For the full erasure procedure across the primary store and secondary indices, see [Garbage Collection: GC and purging together](./gc.md#gc-and-purging-together) and [Time-variance: Purge and storage](./time_variance.md#purge-and-storage).

## Distributed Deployment

Datahike supports distributed deployments with remote writers (`:http` or `:kabel` backends). Each secondary index type has different characteristics for distributed use:

### Stratum and Proximum (konserve-backed)

Stratum and Proximum store their data in konserve, the same key-value store that Datahike uses for primary indices. This means they are **automatically available to all readers** in a distributed setup — readers sync from konserve and can restore the index state.

### Scriptum (filesystem-backed)

Scriptum stores Lucene segments on the **writer node's local filesystem**, not in konserve. This means:

- **Writer node**: Has full read/write access to the Lucene index. Transactions maintain the index in real-time.
- **Reader nodes**: Cannot directly access the Lucene files. Fulltext search queries must be routed to the writer.

**Current approach (Option A)**: Scriptum is a writer-side index. Readers that need fulltext search results should query through the writer connection (via kabel/http). This is similar to how Elasticsearch routes search requests to the shards that hold the data.

```
┌───────────┐     transact      ┌────────────────┐
│  Client   │ ────────────────> │  Writer Node   │
│           │ <── tx-report ─── │   (scriptum)   │
└───────────┘                   └───────┬────────┘
                                        │ konserve sync
      ┌─────────────────────────────────┼──────────────────┐
      │                                 │                  │
┌─────▼──────┐                   ┌──────▼───────┐   ┌──────▼───────┐
│  Reader 1  │                   │  Reader 2    │   │  Reader 3    │
│  stratum ✓ │                   │  stratum ✓   │   │  stratum ✓   │
│  scriptum ✗│                   │  scriptum ✗  │   │  scriptum ✗  │
└────────────┘                   └──────────────┘   └──────────────┘

Readers have stratum/proximum (via konserve).
Scriptum queries must go through the writer.
```

**Future options** (not yet implemented):
- **Segment replication via konserve**: Store Lucene segments as blobs in konserve, implement a read-only `KonserveDirectory` for readers
- **NRT segment replication via kabel**: Use Lucene's built-in primary/replica protocol over kabel for near-real-time search replication

**Important**: Lucene does not support NFS or shared network filesystems. Do not mount the scriptum index path over NFS — this will cause index corruption.

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

## Index-like reads from the store itself: konserve-lmdb

The three types above are secondary *indices*: separate structures Datahike
maintains beside the primary EAVT/AEVT/AVET trees.
[konserve-lmdb](https://github.com/replikativ/konserve-lmdb) offers a different
trade — index-like reads out of the **stored blobs**, with no second structure to
build, maintain or garbage-collect.

**This is not a konserve-wide capability, and not a Datahike index type.**
konserve defines no secondary-index protocol; this is specific to the LMDB
backend, and there is no `:db.secondary/type :lmdb`. It is a storage-level
facility you use directly, not through `:db.secondary/*` schema.

What it provides (format v2):

- **Ordered range access** — `scan` and `scan-keys` walk a key range in order,
  `scan-keys` without touching value pages at all.
- **Projection** — `project` pulls one field out of every value in a range
  *without materialising the value*, and `project-reduce` folds several fields
  over that range in a single pass. The store's range picks the rows;
  [boring](https://github.com/replikativ/boring)'s navigator picks the columns
  inside the CBOR blob, which is where the JSONB-like part comes in.

The speed comes from LMDB's zero-copy memory-mapped layout combined with never
rebuilding the document. Measured against PostgreSQL JSONB on
`count(doc->'tail'->>'city')`, comparing Postgres' own `EXPLAIN ANALYZE` time:
6.7x at 400 bytes of padding, **49.5x at 3000**. Worth being precise about why,
because it is not a claim that the navigation is cleverer — JSONB's is arguably
better. It is that `PG_GETARG_JSONB_P` fully detoasts before its O(1) navigation
runs, so its cost tracks document size whatever field you asked for, while a
projection's cost tracks only how much CBOR sits in front of the field. An
optional index frame (`:index N`) replaces that walk with a jump.

Versioned stores are a separate, **experimental** facility within the library:
each write appends a version under an HLC coordinate, giving `latest`, `as-of`
and `history` at the store level, with `gc!` to collect versions no live pin can
reach.

Requires **Java 22+** (Project Panama FFI) and `liblmdb`.

> **Beta**, and moving. The API may change between releases, and the versioned
> layer is experimental even by that standard. If you try it for aggregate or
> range workloads that would otherwise want a Stratum index, reports of where it
> wins and where it does not are exactly what is useful right now.

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
