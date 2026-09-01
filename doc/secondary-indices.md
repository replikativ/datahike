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
identical content de-duplicates. There is no independent mutable value pointer:
the value lives in the immutable secondary generation named by the same
Datahike root as the primary hash.

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
*entity*, and can be found by content hash. Scriptum implements this protocol
for strings by storing one document per `[eid attr value-hash]`. Stratum is
columnar with one cell per `[eid column]` and Proximum is keyed by external id,
so those two cannot cover cardinality-many authoritative values.

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

A caller that gives up waiting for a build can cancel the exact generation it
observed. Capture the build boundary with the status, then pass both to the
writer operation:

```clojure
(require '[datahike.writer :as writer])

(def boundary
  (get-in (d/db conn)
          [:schema :idx/my-index :db.secondary/building-since-tx]))

@(writer/cancel-secondary-index-build!
  conn :idx/my-index boundary)
```

Cancellation is serialized with transactions. It retracts that declaration and
clears its in-memory delta journal in one commit. If the ident now names a
different generation—or has already become ready—the operation fails with
`:secondary-index-build-generation-mismatch` and changes nothing. The detached
scan may finish, but it is fenced from publication and releases its resources.
Datahike applies the same cancellation transition automatically when a
background scan fails, so a failed adapter cannot strand a permanently
`:building` declaration. The connection keeps a generation-keyed diagnostic at
`[:secondary-index-build-failures index-ident]` until a successful replacement
generation clears it or the connection is released. This diagnostic is runtime
state for the waiting caller; it is deliberately not a durable index root.

## Branching and Versioning

Secondary indices are first-class versioned state. A committed Datahike root
contains each ready index's immutable generation key-map. Branch creation copies
those addresses exactly; it does not ask an adapter to move a mutable native
branch pointer:

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

The adapters realize those immutable generations differently:

- **Scriptum** seals Lucene segments and manifests into Datahike's konserve;
  local `:path` is only a disposable cache/workspace.
- **Stratum** seals its column trees into Datahike's konserve.
- **Proximum** seals a generation in its configured external store. Linear
  Datahike generations use distinct logical vector-store handles over one
  reference-counted append-only mmap, so an ordinary changed transaction does
  not copy that file or depend on filesystem reflinks. Native Proximum branch
  divergence and unrelated historical opens still require independent mmap
  caches. Small transactions can nevertheless amplify immutable partial-vector
  and dirty-edge chunk writes; batching, smaller tail chunks, or a later
  append-overlay are performance refinements rather than publication-correctness
  requirements.

### Stored generation compatibility

Generation key-maps are a durable storage format, not a best-effort cache hint.
An adapter checks its type, format version, storage owner, and exact generation
address before it reads storage or participates in GC. A malformed map, a map
from another adapter, or an unsupported version fails the connection. It is
never interpreted as an empty index: that would be silent data loss when an
indexed attribute uses `:db.secondary/only`.

The immutable-generation transition deliberately does not read these earlier
experimental formats:

- Proximum maps containing native `:commit-id` / `:branch` pointers. Current
  format 2 requires an external `:generation-id`, a stable
  `:external-store-id`, and `:generation-strategy :full-mmap-copy`.
- Scriptum format 1 maps. Current format 2 names a sealed Lucene manifest with
  `:snapshot-address` in Datahike's store.

Stratum is the safe exception. Its older unversioned map already names an exact
immutable generation by UUID `:dataset-commit-id`; when the type and UUID are
valid, the adapter normalizes that map in memory to format 1 with
`:storage-owner :datahike`. The next successful commit writes the current
envelope. Missing, non-UUID, or otherwise malformed Stratum roots still fail
closed.

Do not change the version field to make one of those maps look current: the old
address names a different persistence protocol. If all canonical values remain
in the primary indexes, the recovery path is to run the matching older adapter,
remove/disable the old secondary, upgrade, and create a new index ident so its
backfill produces a current generation. The restore-failure `:drop` escape hatch
permits inspection/export but not writes while the adapter is absent; explicitly
remove the rebuildable secondary schema before writing. If any covered attribute
uses `:db.secondary/only`, first run the older stack and export or copy the
canonical values while its generation is still readable; the primary contains
only hashes and cannot reconstruct them. The current adapter therefore fails
closed rather than claiming to migrate such a database.

Ready state is persisted through `IDurableSecondaryIndex` and
`IPreparedSecondaryGeneration`. Preparation writes a complete immutable
generation first; the Datahike branch head is the only publication point;
release is cleanup, never another visibility transition. A building generation
is deliberately not prepared, branched, or
published as an audit/GC root: it may contain an arbitrary prefix. If a process
stops during backfill, reconnect creates an empty generation and restarts the
scan from AEVT. A branch made while an index is building likewise starts without
a secondary key-map and rebuilds independently.

Factories invoked for a backfill receive the ephemeral configuration keys
`:datahike.index.secondary/index-ident` and
`:datahike.index.secondary/build-attempt`. An adapter backed by external mutable
storage must namespace its private files/keys by `build-attempt`; the factory
must not reopen a partial generation abandoned by an earlier process. The
attempt identifier is runtime context, not schema, and a successfully prepared key-map
becomes the durable identity of the ready generation.

Pure `d/db-with` can update a durable index only when the adapter declares that
its transient is wholly in memory (`IPureSecondaryMutation`). Stratum does;
Scriptum and Proximum currently do not, because opening their builders performs
external writes. A pure transaction touching either fails before a builder is
opened. Connection transactions support all three. An immutable in-memory delta
overlay is the intended way to remove this limitation without leaking resources
from abandoned database values.

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
path. The snapshot the scan reads is already pinned with a [durable GC
root](./gc.md#durable-roots) (`:pin`, renewed in the background and released by
the ready commit); a build whose lease is lost is discarded at install instead
of being published over swept snapshot nodes. The adapter's unpublished build
generation is not yet named by a durable checkpoint. The shared Konserve guard
protects it exactly in-process, and the durable `:building` schema state makes a
collector in any process defer its sweep until the ready commit lands. This
pauses reclamation, not transactions or the backfill. A resumable
generation-specific `:checkpoint` root (or a durable Konserve fence) can later
allow collection to proceed safely during long builds.

## Purge propagation

`:db/purge` / `:db.purge/entity` / `:db.purge/attribute` / `:db.history.purge/before` route a retraction event (`-transact` with `:added? false`) to every secondary index covering an affected attribute, the same way `:db/retract` does. After purge:

- **Scriptum** no longer returns the purged datom from full-text search.
- **Proximum** skips it on KNN queries (HNSW mark-delete).
- **Stratum** excludes it from columnar aggregates (columnar rewrite).

Two storage-layer cases matter:

### Datahike-store generations (Stratum and Scriptum)

Stratum nodes and Scriptum's Lucene segments/manifests are immutable objects in
Datahike's konserve. `d/gc-storage` obtains their exact marks from every retained
Datahike branch/commit root. A divergent branch therefore keeps its older
generation alive, while a generation no retained root names is reclaimed after
the normal cutoff. Scriptum's `:path` cache is disposable and may be cleaned
independently.

Datahike must be the sole mark/sweep authority for that shared store. Do not
also publish independent native Scriptum, Stratum, or other application roots
there and run their standalone collectors: a common Konserve write guard fences
in-flight writes, but it cannot make one collector discover another owner's
roots. A future store-wide root-provider registry can support that topology;
today use separate stores or make every root reachable from a Datahike key-map,
store-ref, or durable GC root.

Lucene deletion is still tombstone-and-merge within a generation. In addition,
old immutable Datahike roots deliberately preserve the older generation. Full
physical erasure therefore requires removing every branch/history/GC root that
retains the value, then running the ordinary collector; it is not correct to
delete a segment still named by an old branch.

### External generations (Proximum)

Proximum's generation ID lives in the Datahike root, but its mmap objects live in
the configured Proximum store. The Proximum collector fails closed unless it is
given the complete set of generation IDs retained by Datahike. `d/gc-storage`
does not yet coordinate that external sweep automatically. Until a coordinator
collects roots across all branches, commits, and durable GC pins, collect only
the primary/Datahike store and retain Proximum generations.

`datahike.gc/reachable-external-secondary-roots` performs that complete
retained-root discovery and groups each generation with the durable external
store identity recorded in its key-map. It is intentionally not a sweep token:
a branch or pin can publish an old generation after a read-only walk returns.
External deletion remains disabled until a durable collection epoch fences all
root publications from the primary root snapshot through the external sweep.

For the full erasure procedure across the primary store and secondary indices, see [Garbage Collection: GC and purging together](./gc.md#gc-and-purging-together) and [Time-variance: Purge and storage](./time_variance.md#purge-and-storage).

## Distributed Deployment

Datahike supports distributed deployments with remote writers (`:http` or `:kabel` backends). Each secondary index type has different characteristics for distributed use:

Stratum and Scriptum restore their exact immutable generation from Datahike's
konserve. A reader needs access to that store and a writable local cache path;
it does not need a shared Lucene filesystem or a writer-side mutable index.
Proximum readers additionally need access to the same external generation store
named by `:store-config`. Remote transaction writers still need to satisfy the
backfill constraints described above; read availability and coordinated writes
are separate concerns.

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
| `:path` | Disposable local Lucene cache/workspace | a fresh directory under the system temp dir (`java.io.tmpdir`) |
| `:max-merged-segment-mb` | Optional Lucene merge-size tuning | Scriptum default |
| `:ram-buffer-mb` | Optional Lucene writer buffer tuning | Scriptum default |

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

Stratum requires no separate store configuration. Transactions construct an
immutable in-memory dataset value; commit preparation seals its column trees
into Datahike's konserve.

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

A transient that owns files, guards, or native memory must also implement
`IAbortableSecondaryTransient`; Datahike calls it if primary validation or a
backfill/install replay fails. If the transient freezes to a durable index,
implement `IDurableSecondaryTransient`. Implement `IPureSecondaryMutation` on
the persistent index only when those transient operations perform no external
writes, which opts the adapter into pure `d/db-with`.

Durable adapters additionally implement `IDurableSecondaryIndex`:

```clojure
sec/IDurableSecondaryIndex
(-sec-generation-key-map [this] ...) ; complete immutable generation envelope
(-sec-prepare [this context] ...)    ; => async IPreparedSecondaryGeneration
(-sec-restore [this store key-map] ...)
```

The key-map must include `:type`, `:format-version`, and `:storage-owner`, and
must fail closed if it cannot identify an exact generation. Register a
`mark-from-key-map` method for generations stored in Datahike's konserve. The
key-map of an externally owned generation must additionally carry a stable,
non-secret store identity and register `external-root-from-key-map`; Datahike
uses it to discover roots, not to open an arbitrary store configuration. The
prepared generation must remain readable across an `:unknown` publication
outcome; its release hook is idempotent cleanup, not a second commit point.

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
