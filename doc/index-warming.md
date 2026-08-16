# Index Warming

> ⚠️ **EXPERIMENTAL FEATURE**
>
> The names and the option map may still change. Nothing here affects results:
> a warm only moves index nodes into the node cache earlier, and more
> concurrently, than a scan would. Skipping it — or having it run out of budget
> — costs round trips and never correctness.
>
> The ClojureScript arm is **not implemented**; `-warm!` reports
> `:unsupported :cljs` there. See [On ClojureScript](#on-clojurescript).

## The problem

A cold reader's wall time is `misses × RTT`, with nothing overlapping. A scan
asks for a node, blocks on the GET, and only then learns the address of the next
one. Measured against a local MinIO with 20 ms of injected latency, the marginal
cost of one more node was **24.96 ms against a ~25 ms round trip** — one node,
one round trip, zero overlap.

It does not have to be that way, and the fix needs no prediction. A branch node
holds *every* child address the moment it is materialized, so a whole level's
addresses are known one level in advance. Warming walks the tree breadth-first
and fetches each level concurrently.

Measured on the same store: fetching the same 197 keys at width 64 is **16.4×
faster** than serially at +20 ms.

## The API

Four functions, all on a database value:

```clojure
(require '[datahike.api :as d])

;; Warm what the matching scan will read. The one to reach for.
(d/warm-datoms! @conn :eavt [300])
(d/warm-datoms! @conn :avet [:item/id 300] {:depth :with-leaves})

;; Read ahead from a seek position.
(d/warm-seek! @conn :eavt [300] {:budget 64})

;; Every index at once, sharing one budget. The connect-time shape.
(d/warm-db! @conn {:depth :with-leaves :budget 500})

;; One index, raw bounds.
(d/warm-index! @conn :eavt {:depth :interior :budget 2000})
```

Each returns a report:

```clojure
{:fetched 37 :by-level [2 9 26] :rounds 3 :height 3
 :by-index {:eavt 37}
 :budget-left 1963 :budget-exhausted? false :budget-clamped? false
 :ms 246.3}
```

`:by-level` and `:budget-exhausted?` are the point of it: they make a warm that
is quietly decaying visible as a metric, before it becomes visible in p99.

## Two bounds, and why there are two

`:depth` bounds the **shape**, `:budget` bounds the **cost**, and whichever binds
first wins.

| `:depth` | expands while | effect |
|---|---|---|
| `:interior` (default) | level ≥ 2 | every branch level, no leaves |
| `:with-leaves` | level ≥ 1 | everything |
| an integer *n* | round < *n* | at most *n* levels below the start |

`:interior` stops exactly at the leaf boundary, and it is exact rather than
heuristic: leaves are level 0, and the root knows the tree's height.

Having both bounds is what keeps this free of latency cliffs. There is no
"preload everything" mode and no "warm the interior" mode to switch between — a
small database with a large budget runs out of frontier and has fetched itself
entirely; a large one hits the budget and stops. Same code, same config,
continuous in database size. A mode switch is a cliff, and databases grow.

Measured at 400 issues, +20 ms: `:with-leaves` with `:budget 8` warms 8 nodes and
takes the following query from 21 GETs to 12 — a partial warm buying a partial
saving, landing between naive and fully-warmed with no step anywhere.

## Sizing a budget

In a B-tree the interior is a geometric series, so `interior / total` is a
constant fraction independent of database size. Size it from the **measured**
fill, not from the branching factor: nodes run about half full, so the effective
fanout is ~`bf/2` and the interior is ~`2/bf` of the tree — twice the naive
estimate. Measured at bf 32: **93 interior nodes of 1504 total (6.2 %**, against
`1/32` = 3.1 %).

The budget is **clamped to 0.8× `:store-cache-size`**, with a warning when it
bites. That cache is entry-counted, so warming past it fetches nodes only to
evict them; the 0.8 leaves room for the query that follows to bring in its own
leaves without evicting the spine you just warmed.

## Warm what you are going to scan

`warm-datoms!` and `warm-seek!` take the same `[e a v tx]` component prefix that
`d/datoms` and `d/seek-datoms` take, and build their key bounds with datahike's
own `components->pattern` — the same call `datahike.db/contextual-datoms` makes.
That is the point of them: a warm and the scan it is warming for derive their
range from one function, so they agree by construction.

Hand-built `:from`/`:to` can disagree, silently. The pattern builder **permutes
components per index** (`:avet` reads `[a v e tx]` and produces
`datom(v, a, e, tx)`) and resolves idents and lookup refs along the way. Get that
wrong and you warm a valid-but-different subtree — no error, no wrong answer,
just a warm that misses and a query that quietly pays full price.

`:from`/`:to` on `warm-index!` exist for the cases where you genuinely have
datoms in hand.

## Where a warm does and does not help

It reaches the same end state as preloading the whole store — the query costs
0 GETs — for far less work, because it **walks**: it touches only reachable
nodes and never enumerates the store. Measured against datahike's existing
tiered-store preload (`ready-store :tiered` running konserve's
`populate-missing-strategy` on connect) at 400 issues, +20 ms:

| | fetches | time |
|---|---|---|
| tiered preload | 392 | 11 836 ms |
| BFS warm, `:with-leaves` | 37 | 438 ms |

The preload's cost is roughly `2 × objects`: it calls `-keys` on the backing
store (one GET per object) and then fetches every key again — including the
garbage, which for that tenant was 80 % of the store.

**What it cannot do:** this bounds *depth*. It does nothing for a caller that
issues many independent scans one after another — that breadth lives above
datahike, and no amount of prefetching below can see it. Such a caller has to
issue its seeks concurrently, which is safe: nodes are immutable and the node
cache is atom-based.

## Tuning `:width`

`:width` is the number of concurrent in-flight restores. The default is 64 on the
JVM: measured optimal against local MinIO (16.4× over serial at +20 ms), where
**128 regressed**. A real bucket tolerates far more — treat 64 as a starting
point to measure from, not a constant to inherit.

## Sync and async

Every entry point takes `:sync?`, defaulting to `true` on the JVM, and returns
a channel carrying the report when it is `false` — datahike's usual shape. Note
that the fan-out itself is a thread pool either way: persistent-sorted-set's JVM
`IStorage/restore` is synchronous, so `:sync? false` buys the *caller's* thread
back, not extra parallelism.

## On ClojureScript

Deliberately not implemented, and marked as such rather than faked. The walk
itself is shared `.cljc` and the code is written `async+sync`, so the seam is in
the right place; three platform primitives are missing:

1. **`child-bounds`** — persistent-sorted-set's cljs arm keeps its
   `binary-search-l` private, so there is no `searchFirst` to call.
2. **`fetch-wave!`** — a bounded `Promise.all` per level. The cljs
   `IStorage/restore` under `{:sync? false}` returns a partial-cps expression;
   the shape is to kick off `width` of them at once (each starts its IO
   immediately, and JS is single-threaded, so awaiting them in order still
   pipelines) and adapt each to a promise-chan.
3. **`tree-entry`** — the cljs `BTSet` keeps `root` as a raw field that is nil
   for an address-rooted set, and its materializing counterpart is private and
   async.

The port is not a port: cljs has no threads, but it does not need them — bounded
`Promise.all` per level is simpler than a thread pool, and the BFS shape is
natively async. Two things genuinely differ. `:width` cannot share a default (a
browser gives ~6 connections per origin on HTTP/1.1, so 64 merely queues), and
the value proposition inverts: datahike's cljs read path runs a synchronous query
engine over asynchronous storage, so a complete warm is what makes synchronous
querying *feasible* rather than merely fast.

See the `TODO(cljs)` markers in `datahike.index.persistent-set.warm`.

## Implementation notes

- `-warm!` is a method on the index protocol (`datahike.index.interface/IIndex`),
  implemented for the persistent-set index and a documented no-op for the
  hitchhiker-tree, whose async node resolution gives no one-level-ahead view of
  child addresses. Clojure protocols have no true defaults, so each index type
  says which it is.
- `warm-db!` shares **one** budget round-robin across the indices rather than
  giving each a slice or spending them in sequence: warming `eavt` to exhaustion
  while `avet` gets nothing is the wrong answer for a query that reads `avet`,
  and which index a query needs is not knowable at warm time. `:by-index` in the
  report says where the budget actually went.
- Concurrent `restore` is safe: nodes are immutable and content/uuid-keyed, and
  the node cache is a `clojure.core.cache.wrapped` atom. Two threads racing the
  same address duplicate a fetch — wasted work, never a wrong answer.
- The four operations are **not** exposed over HTTP, the pod, the CLI, or the
  FFI bindings. A warm prefetches into the node cache of the process holding the
  index; over any of those boundaries that is either a different process's cache
  or a cache that dies with the call.
