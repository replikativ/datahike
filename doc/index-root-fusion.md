# Index-root fusion (reduce write amplification)

*Branch: `feat/fuse-index-roots`. Status: design, pre-implementation.*

## Problem

A datahike commit writes `(count pending-writes)` index-node objects + 2
db-records (under the commit-id and under the branch). Measured ~7 PUTs/commit
for small commits. The index-node objects include each index's **root**, which
changes essentially every commit. On per-request object storage this
amplification is the dominant cost (see saas `doc/cost-model.md`).

## How the write path works today

- `db->stored` (`writing.cljc`) calls `di/-flush` on each index → `psset/store`
  walks dirty nodes and calls `CachedStorage.store` per node, which **appends
  `[address node]` to `pending-writes`** and returns the (content- or squuid-)
  address. The root's address becomes `pset._address`.
- The stored-db map references each index as a small record. The PSS konserve
  write-handler serializes a `PersistentSortedSet` to `{:meta, :address,
  :count}`; the **root node lives separately** at `:address`. Read-handler:
  `(PersistentSortedSet. meta cmp address @storage nil count settings 0)` — the
  5th arg (currently `nil`) is the in-memory `_root`.
- `commit!` drains `pending-writes` (`k/assoc store address node`, one PUT
  each), then writes the db-record under `cid` and under `branch`.

## The fusion seam

Inline each index's **root node** into its db-record reference
(`{:meta, :address, :count, :root <root-node>}`) and **drop the root from
`pending-writes`** so it isn't PUT separately. Restore passes the inlined node
as the constructor's 5th arg instead of `nil` — deeper children stay lazy.

Win profile (sharper than "−3 PUTs"):
- **Index = single leaf root (tiny tenant):** the *whole* index inlines → zero
  separate node PUTs for it. A few-datom tenant's commit collapses to ~2
  record PUTs.
- **Deeper tree:** saves exactly **1 PUT per index** (the root); the dirty
  leaf/intermediate path is still separate — that part is op-buf's job, later.
Also **−1 GET per index on cold open** (root arrives with the record).

## Options

- **A — explicit fused index-ref in `db->stored`/`stored->db`** *(recommended)*.
  Build `{:meta :address :count :root <node>}`, remove the root from
  `pending-writes`, reconstruct via the root-seeding constructor. Contained to
  `writing.cljc` + a small helper. Opt-in via config `:fuse-index-roots?` so
  it's measurable against baseline.
- **B — embed root in the PSS konserve write/read handler.** More automatic but
  the handler would need storage access at serialize time + a way to skip the
  separate write. Couples handler to pending state. Messier.
- **C — fusion + branch-as-pointer.** On top of A: write the fused object once
  under `cid`, a tiny `{:head cid}` under `branch`. Halves per-commit record
  bytes; costs a 2nd GET on branch-open. Optional follow-on.
- **D — inline the whole dirty path (op-buf / mini-WAL in the record).** The
  deeper convergence; this is the PSS op-buf work, explicitly *after* A.

## Implementation plan (Option A)

Touchpoints, all in datahike (PSS untouched):

1. **Config:** add `:fuse-index-roots?` (default false).
2. **`db->stored`:** when enabled, for each flushed index pull its root node
   (from `CachedStorage` cache at `pset._address`) and emit a fused ref; record
   the root address so it can be excluded from the drain.
3. **`commit!` drain:** filter the fused root addresses out of `pending-writes`
   before `k/assoc`-ing the rest. (We have `pset._address` per index.)
4. **`stored->db`:** detect the fused ref and reconstruct the index with the
   inlined root node seeded into `_root` (constructor 5th arg) + `_address` +
   storage for lazy children.
5. **Serialization:** the inlined root is a `Leaf`/`Branch` — already has
   konserve read/write handlers, so it nests in the record map for free.

## Caveats to resolve

1. **crypto-hash audit** (`index/persistent_set.cljc` `walk-pss-address!`)
   starts at the root *address* via `k/get` — with the root inlined there's no
   konserve object there. v1: gate fusion on `:crypto-hash? false`, or teach the
   walk to take the root from the record. (The merkle `:address` is still
   computable from the inlined node, so audit *can* be made to work.)
2. **GC / `mark`:** the fused root has no konserve object; the reachability/free
   path must not expect one at that address (don't add it to the konserve-key
   reachable set; its children's addresses still are).
3. **`pending-writes` skip must be exact:** only the per-index *root* address is
   removed; every deeper dirty node stays. Identify by `pset._address`.
4. **Backwards compat:** a fused db-record must be distinguishable from a legacy
   one on read (presence of `:root`), so old stores still restore.

## Validation

- Roundtrip: write → restore → `(= (vec before) (vec after))`, counts, slices,
  history (`as-of`) — at `:fuse-index-roots? true` and `false`.
- Measure with the saas `commit-cost` probe: PUTs/commit and cold-open GETs,
  baseline vs fused, across tiny (single-leaf) and deeper trees.
- Full datahike test suite green with the flag off (byte-identical) and on.
