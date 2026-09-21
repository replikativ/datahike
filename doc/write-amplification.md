# Reducing write amplification

*Experimental. The options here are opt-in, per-store, and default to off; a
store created without them is byte-for-byte identical to a store that never had
them.*

A datahike commit does not just write your new datoms. Because the indices are
persistent trees, changing one datom rewrites the nodes along the path from the
touched leaf up to the root, and the commit also writes two small records (one
under the commit id, one under the branch head). On a local filesystem the extra
objects are nearly free. On a **request-priced object store** (S3, R2, Tigris,
GCS) every object is a separate network round-trip and a separate billed
request, so the number of objects per commit — the *write amplification* — is the
dominant cost and the dominant latency.

This page describes four independent, composable options that reduce that
object count. They matter most for small, frequent commits against object
storage; on a local filesystem they change little and can be left off.

| Option | Config key | What it removes |
|---|---|---|
| Diff buffering | `:index-config {:diff-buf-size N}` | the interior/leaf node PUTs of a small commit |
| Root fusion | `:fuse-index-roots? true` | one root-node PUT per index (and one GET on cold open) |
| Commit-graph opt-out | `:commit-graph? false` | the per-commit provenance record |
| Superseded-only history | `:index-config {:temporal-superseded-only? true}` | the three temporal root PUTs of an append-only commit, and half the stored bytes |

With all three enabled on a small commit, the write collapses toward a **single
object write** — the new branch-head record — which is the round-trip floor for a
durable acknowledgement.

## The baseline write

For a commit that changes a handful of datoms, without any of these options:

- **Per index** (`:eavt`, `:aevt`, `:avet`, and their temporal counterparts when
  `:keep-history?` is on): roughly `depth + 1` node objects, because the whole
  root-to-leaf path is rewritten.
- **Two records**: the immutable commit record (keyed by the commit id) and the
  mutable branch-head record.

So a small commit on a shallow, history-off store is on the order of half a dozen
PUTs; a deep tree or `:keep-history? true` multiplies the per-index part.

## Diff buffering

```clojure
{:store {:backend :s3 ...}
 :index-config {:diff-buf-size 256}}   ;; default 0 = off
```

Instead of rewriting a small commit's entire root-to-leaf path, diff buffering
records the **content-only change** as a compact diff *inside* the nearest
ancestor node that has to be rewritten anyway, and re-points that child at its
existing durable node rather than writing a fresh one. The buffered diffs
accumulate in the ancestor up to a byte budget (`:diff-buf-size`); when a child's
buffered change grows past what fits, or a structural change (a node split or
merge) occurs, that child is flushed to its own object as usual.

The effect is that a stream of small commits writes on the order of **one node
object per index per commit** instead of `depth + 1`, while reads transparently
project the buffered diffs back onto the durable nodes as they are loaded.

**Tradeoffs and behaviour**

- **Point lookups and counts are unaffected** — counts stay exact, and a point
  lookup projects only the nodes on its path.
- **Cold full-range scans do more work** — the first scan over a subtree has to
  project the buffered diffs, roughly doubling that scan's node work versus an
  unbuffered store. Warm scans are unaffected.
- **Create-time-fixed.** `:diff-buf-size` shapes the on-disk representation, so
  it is fixed when the database is created and adopted automatically from the
  store on reconnect. Passing a conflicting value on connect raises (override
  with `:allow-unsafe-config`).
- **Works with `:crypto-hash?`.** A node's content address folds in its buffered
  diffs, so the merkle audit (`datahike.audit/verify-chain`) detects a tampered
  buffered diff like any other content change. The content hash is
  representation-dependent by design: the same logical data hashes differently
  under different `:diff-buf-size` settings.
- **Reader requirement.** The on-disk format needs persistent-sorted-set ≥
  0.4.126 (older readers refuse a diff-buffered store via the version guard);
  correct concurrent reads require ≥ 0.4.137, and the supported floor is ≥ 0.5.140,
  which carries the diff-buf and B-tree correctness pass.

## Root fusion

```clojure
{:store {:backend :s3 ...}
 :fuse-index-roots? true}              ;; default off
```

Each index's **root node** changes on essentially every commit, so it is the one
node guaranteed to be rewritten. Root fusion inlines the root node directly into
the db-record instead of writing it as a separate object. That removes **one PUT
per index per commit**, and on a cold connection the root arrives with the record
so it also removes **one GET per index** on first open. Deeper children stay lazy
and are fetched on demand as before.

For a tiny tenant whose whole index is a single leaf, that leaf *is* the root, so
fusion inlines the entire index — the commit collapses to just its records.

**Tradeoffs and behaviour**

- **Composes with diff buffering** — the fused root can itself carry buffered
  diffs.
- **Presence-based restore.** A fused record and a legacy (unfused) record both
  restore correctly; the flag is adopted from the store on reconnect.
- **Under `:crypto-hash?`, fusion still saves the GET but not the PUT.** With
  content-addressed nodes, a root's address can coincide with an interior child
  of another index, so the root object is kept to avoid dangling that shared
  reference. The root is still inlined in the record (saving the cold-open GET);
  it is simply also written as an object. Without crypto-hash, addresses are
  unique per node and the root PUT is dropped outright.

## Superseded-only history

```clojure
{:store {:backend :file ...}
 :keep-history? true
 :index-config {:temporal-superseded-only? true}}   ;; default false
```

With `:keep-history? true` a database keeps three temporal indexes next to the live
ones. By default a **cardinality-one** assertion is written to both: the temporal
indexes are a full copy of the live ones plus everything that was ever superseded. For
append-only data (logs, conversations, ledgers, events) nothing is ever superseded, so
the copy carries no information, yet every commit rewrites six index roots instead of
three and the store is twice the size.

Under this option the temporal indexes hold **only what the live indexes no longer
hold**: when a value is superseded or retracted, its original assertion (with its
original transaction) and the retraction move into history; a value that is still
current lives in the live index alone. `history`, `as-of` and `since` read the union of
both, which is what they already do for cardinality-many and `:db/noHistory`
attributes; this option extends that layout to cardinality-one.

Measured on a file store with `:diff-buf-size 128`, 40k datoms, no retractions: a
one-row commit goes from 7 objects / 80 KB to **4 objects / 41 KB**, the store from
42.5 MB to **21.2 MB**.

**Tradeoffs and behaviour**

- **Same answers.** `history`, `as-of` at every transaction, `since`, history queries and
  purge are tested to agree with the default layout over asserts, supersedes, retracts,
  re-asserts (including of a previously held value), cardinality-many, `:db/noHistory`,
  same-transaction assert+retract and `retractEntity`.
- **History reads merge two slices** where a cardinality-one read used one. Point lookups
  and range scans over current data are unaffected.
- **Create-time-fixed, and one-way.** The flag is adopted from the store on reconnect. A
  store created with it **cannot be opened without it, not even with
  `:allow-unsafe-config`**: read as a full-copy store it would silently lose every current
  value from history. The other direction is harmless (the default layout is a superset),
  and an older Datahike refuses such a store through the stored-version check.
- **`d/metrics`** `:temporal-count` then counts superseded and retracted rows, not
  history rows including live copies.
- **Backends.** Tested on persistent-set and hitchhiker-tree.

## Commit-graph opt-out

```clojure
{:store {:backend :s3 ...}
 :keep-history? false
 :commit-graph? false}                 ;; default true
```

Every commit normally also writes an immutable **provenance record** keyed by its
commit id. That record is what `datahike.audit/verify-chain`, ancestry walks,
branching from a commit id, and `dh://…?commit=<id>` references read. A store that
needs none of those — a typical `:keep-history? false` tenant — can skip writing
it, so a commit writes only its branch head.

The commit id is still computed and stamped in the db metadata, so one-step
lineage, streaming-reader deduplication, and the writer's head tracking are
unchanged. **Time travel is unaffected** — `as-of`/`history` read the temporal
indices, not the commit graph. What you give up is the ability to audit, walk
ancestry, branch from a bare commit id, or resolve `?commit=` references on that
store.

**Behaviour**

- Store-fixed and adopted from the store on reconnect, like the options above.
- Cannot be combined with `:crypto-hash? true` (an audit chain with no persisted
  chain is rejected at creation).
- Branching from a branch keyword still works; branching from a commit id fails
  with an explanatory error.

## Putting it together for object storage

For a single-writer tenant on an object store that does not need history or an
audit trail, the combination below makes a small commit approach the one-write
round-trip floor while keeping full query and time-independent semantics:

```clojure
{:store {:backend :s3 :bucket "tenant-42" :id tenant-id ...}
 :keep-history? false
 :index-config {:diff-buf-size 256}
 :fuse-index-roots? true
 :commit-graph? false}
```

- Diff buffering removes the interior node PUTs of the commit.
- Root fusion removes the per-index root PUTs and folds the roots into the record.
- Commit-graph opt-out removes the provenance record.

What remains is the branch-head write. Storage still grows with superseded nodes
over time; reclaim it with [garbage collection](./gc.md).

Enable these against a representative workload and measure — see
[Benchmarking](./benchmarking.md) — rather than assuming; the win depends on
commit size, tree depth, and your provider's per-request cost and latency.
