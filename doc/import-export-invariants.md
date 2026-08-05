# Export / import — invariants

**What this is.** The testable contract of `datahike.migrate`: what must be true
of a dump, what export and import guarantee, and what is deliberately *not*
guaranteed. It is the reference a reviewer — human or agent — should check the
code against.

**What this is not.** It is not the rationale (see
[import-export-design.md](import-export-design.md) for why the format is what it
is) and not the user guide (see [backup.md](backup.md)).

**Why it is separate.** The design document was written with `file:line`
citations so every claim could be verified against source. That made it precise
on the day it was written and stale immediately afterwards, and a reader who
trusted it would have been misled — at the time this file was created, §14 stated
that the import's id-map "persists in the db value across `load-entities`
calls", which the code contradicts in two places by name. **Nothing in this file
cites a line number.** Every statement here should be checkable by reading a
namespace or running a test, and where a claim is unverified it says so.

---

## 1. Vocabulary

| term | meaning |
|---|---|
| **record** | `[e a v t op]` — the wire form. `a` is always a keyword ident, never a numeric attribute id. `op` is the added/retracted boolean. |
| **chunk** | one file (or store key) holding a bounded number of records as an [RFC 8742](https://www.rfc-editor.org/rfc/rfc8742) CBOR sequence, optionally compressed. |
| **dump** | a directory or store prefix: chunks + `manifest.edn` + any blobs. |
| **spool** | scratch written *during* an index-build import. Never part of a dump. |
| **medium** | filesystem or konserve store. The importer's seam is `{:chunks [..] :read (fn [descriptor opts])}` and it does not know which it has. |

---

## 2. Dump invariants

A dump produced by `export-db` satisfies all of these. A reader may assume them;
`verify` checks most of them.

1. **`manifest.edn` is written last.** It is the commit marker: a dump without it
   is incomplete, and its presence means every chunk it names was fully written.
2. **The manifest names every chunk, and only chunks it names may be read.** An
   undeclared file in the directory is a reason to refuse the dump, not to read
   it.
3. **Every chunk has a SHA-256 in the manifest.** A manifest with a chunk lacking
   one is refused rather than treated as unhashed — integrity **fails closed**.
   `:checksums :skip` is the only way past, and it warns.
4. **Attributes are keyword idents.** Numeric attribute ids are valid only in
   `:attribute-refs?` configs, so writing them would make the dump
   config-specific.
5. **System entities are referenced by `#datahike/sysref`, never by raw id**, so
   a restore translates them to the target's own ids rather than re-inserting
   them.
6. **Two exports of the same database are byte-identical** for the same options
   (boring's `:archival` profile; canonical CBOR).
7. **The semantic digest is order-independent** — it identifies content, not
   layout, so it survives a change of `:chunk-size` or codec.
8. **`:history? true` carries (current ∪ temporal)**; `:history? false` carries
   only what is currently true. This is the single most consequential option and
   §6 below depends on it.

---

## 3. Export guarantees

- **Type-exact.** Every builtin value type round-trips, including the array
  types.
- **Bounded memory.** Peak is a function of `:chunk-size` and `:sort-buffer`, not
  of database size. The external sort spills runs; the chunk writer hashes
  incrementally.
  *Status: the original 120 MB / 180k-datom demonstration predates the current
  code. Treat the bound as intended-and-tested-at-small-scale, not as re-measured
  — see §9.*
- **`:sort? false` needs no scratch space at all**, at the cost of one ordering
  property: a same-transaction cardinality-one replacement may be emitted out of
  order. Schema-before-data and tx-entity-before-data still hold.
- **Blobs** (`:db.type/store-ref`) travel with the dump and are verified by
  content hash.

---

## 4. Import: the two paths

`import-db` has **two** implementations behind one entry point. They must produce
the same database, and the ways they do not are enumerated in §6.

### 4.1 Streaming (default)

Reads one chunk at a time, resolves sysrefs, applies `:xform`, and feeds a
**tx-aligned batcher** that flushes to `load-entities` at `t` boundaries.
Commits once per batch. Ids are allocated incrementally by
`transact-entities-directly`.

**The id map is owned by the import and threaded through the batcher.** It is
*not* on the database value and *not* on the connection — a db value has two
holders (the connection atom and the writer's own loop), and putting it there
cost three bugs. It goes out of scope when the import returns, which is why
`finalize-import!` and the `:finalize?` option were REMOVED once this was
understood: the map lands on the tx-report, never on the db value, so the
`swap!` they performed had nothing to clear. The property they appeared to
guarantee — an import leaves no id map on the db — is now asserted directly in
`migrate-test`.

### 4.2 Index build (`:build-indexes? true`, beta)

Creates a **fresh** database by constructing its six index trees from sorted
input, then publishing them in **one** commit. Refuses a non-empty target and
refuses `:merge?`: trees built from sorted input cannot apply the upsert
semantics `load-entities` applies when datoms meet existing data.

Sequence: optional id pre-pass (only under `:eids :allocate`) → normalise the
dump to a compressed spool, collecting `:hash`, the schema datoms, the maxima and
the transaction count in the same pass → three external sorts → six trees → one
commit through the writer.

**Three sorts, not six.** The temporal comparator is a *refinement* of the
current one (`[e a v tx]` vs `[e a v tx added]`), so one sorted file feeds both
trees of a family, and the `added` tie-break is exactly what the currentness fold
needs.

**`:eids` defaults to `:preserve` here**, unlike the streaming path. An empty
target has nothing for a source id to collide with, so the dump's ids are kept —
which removes the O(entities) id map, removes one of the two reads of the dump,
and makes a `:history? true` restore id-identical to its source.

---

## 5. Derived fields an index build must reproduce

Six trees are not a database. These are the fields the build has to compute, and
the two that are not mechanical:

- **`:hash` is not the sum over the current index once history is kept.** It is
  an incrementally maintained additive sum, and the transactor never subtracts a
  value that a cardinality-one upsert superseded — the datom moves to the
  temporal index and stays counted. The rule that falls out: **sum over the
  asserted records**. That reproduces it in both history modes precisely because
  a dump's records are exactly (current ∪ temporal).
- **`:schema` is a stored artifact**, read back from `schema-meta-key` rather
  than derived from datoms at load time — so a build must produce it. It is
  produced by folding the dump's own schema datoms through the transactor's
  `update-schema`/`remove-schema`, i.e. the same code the streaming path runs, so
  the two cannot drift.

Plus `:rschema`, `:max-eid`, `:max-tx`, `:op-count`, the ident maps, and the
merkle roots.

---

## 6. Accepted divergences

These are **not** bugs. A test asserting equality on them is wrong.

| field | divergence | why |
|---|---|---|
| `:max-tx` | index build is **exactly 1 lower** than streaming | the streaming import ends by transacting, which bumps it; the index build never transacts |
| `:op-count` | differs between the paths | inert for persistent-set (every index op takes it unused); only hitchhiker-tree reads it, and that index is refused for index builds |
| transaction ids, `:history? false` | renumbered densely by **both** paths under `:eids :allocate` | a no-history dump carries only surviving datoms, so the transactions that produced them are sparse |
| ids generally, streaming path | never preserved | `load-entities` remaps. A restore is *semantically equivalent*, not id-identical. The index build under `:eids :preserve` is the exception. |
| `:hash` vs Σ current, history mode | deliberately unequal | see §5 |

---

## 7. Refusals

An import that cannot be honoured **throws with a reason** rather than degrading.
`:build-indexes? true` that silently fell back to the streaming path would turn a
configuration mistake into a mystifying performance report.

Refused for an index build: a merge, a non-empty target, a non-persistent-set
index, `:attribute-refs? true`, a dump whose schema declares a secondary index,
an `:eids` policy other than `:preserve`/`:allocate`, and a manifest carrying no
`:schema` key (the id pre-pass reads it to identify ref *values*; without it a
ref could pass through unmapped, which is invisible whenever the mapping happens
to be the identity and silently dangling when it is not).

Refused for any import: a dump format-version newer than this build, an
incompatible target config, and a non-empty target without `:merge?`.

All refusals for one call are reported **together**, so a caller learns
everything blocking them in a single run.

---

## 8. Option surface

Complete as of this file's creation. Options are read via both `(:k opts)` and
`(get opts :k default)`; that inconsistency is known.

**Export:** `:history?` `:chunk-size` `:sort-buffer` `:sort?` `:compression`
`:xform` `:sync?` `:progress-fn`

`:xform` on export is a transducer over records, applied **before the sort and
before the digest** — so counts, the semantic digest and `:stats` all describe
what the dump actually holds rather than needing correction afterwards. Its
motivating case is factoring one large database into per-tenant dumps:
multi-tenancy is usually worth adopting only once usage patterns are clear, i.e.
after a single database already holds everyone, and an export filter is the
migration out of that shape.

Two things such a filter must get right, neither enforced because both are
legitimate choices: **keep the schema datoms** (a dump of data with no schema
imports into a database that declares nothing), and expect refs *out* of the
retained set to dangle — `import-db`'s `:check-refs?` reports those.

**Import:** `:batch-size` `:verify?` `:on-error` `:xform` `:merge?`
`:eids` `:build-indexes?` `:check-refs?` `:checksums` `:sync?` `:progress-fn`
`:dangling-sample` `:spool-codec` `:spool-chunk-size` `:sort-buffer`

`:validate-records?` — records are checked for well-formedness inline on both
paths: 5 elements, integer `e`/`t`, keyword `a`, non-nil `v`, **boolean `op`**,
and `t >= tx0`. ~65 ns/record (0.3% of a streaming import), which is why it is
always-on rather than opt-in. `:validate-records? :skip` disables it — spelled as
a VALUE, like `:checksums :skip`, so it cannot arrive by a stray `true`.

`boolean? op` is the load-bearing clause. `op` is consumed by truthiness in six
places and `0` is truthy in Clojure, so a producer emitting CBOR `0`/`1` — the
natural encoding from any other language, and the documented seam invites foreign
producers — had **every retraction silently asserted**, with `:verified? true`.

Under `:on-error :collect` a malformed record is **collected and reported**
rather than aborting the import — the same contract `:collect` has for any other
bad datom. (An earlier version simply disabled validation there, which quietly
reintroduced the two shapes the check exists for.)

**`:xform` runs BEFORE validation**, so a caller can repair records ahead of the
check — a foreign producer's `0`/`1` for `op` is fixable with
`(map #(update % 4 pos?))` rather than being a wall. It also means a transducer
that *produces* a malformed record is caught.

The last four are **undocumented in `backup.md`** at time of writing.

`:xform` is a transducer over records. **One instance spans the whole import** on
the streaming path, so `(take n)` takes n from the *stream*. The index-build path
instantiates it once per pass over the dump and relies on it being pure — two
instances see the same input in the same order, so they produce the same output.
Records removed net are subtracted from the expected count, so a deliberate drop
is not reported as corruption.

---

## 9. Memory

**Intended:** every stage bounded by an explicit knob, independent of database
size — `:chunk-size` (one decoded chunk resident), `:sort-buffer` (one sort run),
`:batch-size` (streaming path), `:spool-chunk-size` (index build).

**Two findings worth keeping, because both were invisible to "it compiles" and
to a passing test suite:**

1. `mapcat` over a chunk-file *vector* decoded the **whole dump** before the
   consumer saw one record — `mapcat` is `(apply concat (map f coll))`, and `map`
   over a chunked collection evaluates `f` for a whole 32-element block. A dump
   has fewer than 32 chunks for any database that fits on a laptop. Measured:
   pulling one record decoded 14 of 14 chunks. Any new record source must be
   checked for this shape.
2. Because of (1), the OOM was **insensitive to `:sort-buffer`** across a 40×
   range. Insensitivity to the knob that is supposed to bound a stage is evidence
   the stage is not the bound.

**Measured after the fix:** a 1.39M-datom history import completes at **192 MB**.
The export-side claim in §3 has *not* been re-measured.

---

## 10. Portability

| component | JVM | ClojureScript / Node |
|---|---|---|
| export, streaming import, verify | yes | yes |
| index build (`:build-indexes?`) | yes, synchronous | yes, **async only** — `:sync? true` is refused where nothing can block |
| external merge sort | yes | yes (`fs` uses `readSync`/`writeSync`) |
| legacy single-file dumps | yes | refused by name |

The async index-build path is the thinnest-covered surface in the subsystem: one
JVM assertion and two Node tests. Treat it accordingly.

---

## 11. Error propagation

The rule: **a failure must not be reportable as success.** Three real regressions
motivate stating it explicitly, all of which passed review and tests first:

- an async function returning a channel had its cleanup in `try/finally`, so the
  `finally` fired the moment the channel was handed back — before a single chunk
  was read;
- a blob restore was not awaited under `:sync? false`, so every blob failure was
  discarded and the import reported `:verified? true` for a database whose refs
  pointed at blobs that were never written;
- a scratch writer was handed raw records where it expected encodings. On the JVM
  it threw; **on Node `(.-length record)` is `undefined`, so it wrote a 20-byte
  gzip of nothing and produced six empty trees with no error anywhere.**

The third is the shape to hunt for: a JVM-loud, cljs-silent failure. Verification
that is count-based will not catch it if the count is also zero.

---

## 12. Verification tiers

- **tier 0** — chunk path + SHA-256, checked on open. Reports `:ok`/`:none`/`:failed` and the number of chunks verified.
- **tier 1** — counts.
- **tier 2** — an id-independent multiset digest over `[a v op]` plus per-attribute ref counts and an out-degree histogram, so two databases with fully remapped ids compare equal. *Not meaningful after an `:xform`*, by design.
- **tier 3** — a sampled structural diff of `:db.unique` entities, reconstructing each entity's net current state from the dump and diffing against `pull` on the live database.

`verify` **reports** integrity findings; it does not throw for them. It refuses
things that are not dumps rather than certifying them.
