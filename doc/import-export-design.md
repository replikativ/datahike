# Robust Export / Import — design proposal

**Status:** Draft for discussion (targets Datahike 1.0's import/export revisit).
**Namespace:** `datahike.migrate` (today `^:no-doc`, "temporary solution, pending
Wanderung").
**Scope:** full-history, type-exact, verifiable dump/restore that round-trips a
real database — not just a toy EAVT snapshot.

This document is written to be checked against the live source. Every claim about
internals carries a `file:line` citation so a maintainer can verify it before any
code lands. Where an earlier private spec (v2) was wrong about the code, this doc
says so explicitly — the goal is to *not* ship a PR built on a stale mental model.

---

## 1. Why

`datahike.migrate/export-db`/`import-db` today is a ~45-line CBOR dump of the
current EAVT snapshot (`src/datahike/migrate.clj:8-45`). It works for small
databases and breaks on real ones, in ways that are already filed:

| Issue | State | Problem |
|---|---|---|
| [#262](https://github.com/replikativ/datahike/issues/262) | open | EAVT order interleaves schema after data → re-import fails when a data datom precedes its attribute's definition. |
| [#377](https://github.com/replikativ/datahike/issues/377) | open | tx-log / history not represented; no way to round-trip `history`/`as-of`/`since`. |
| [#508](https://github.com/replikativ/datahike/issues/508) | open | `:attribute-refs? true` dumps depend on the source's system-entity numbering staying stable across versions. |
| [#531](https://github.com/replikativ/datahike/issues/531) | open | restore into a schema-bearing / bootstrapped target collides on `:db/ident`. |
| [#633](https://github.com/replikativ/datahike/issues/633) | open | `:db.type/double` round-trips back as `float` — **a CBOR float-encoding bug** (the issue title says so). |
| [#552](https://github.com/replikativ/datahike/issues/552) | open | dumps carry no format version; cross-version import fails opaquely. |
| [#287](https://github.com/replikativ/datahike/issues/287) | **closed** | max-tx after import — fixed by batching ([#845]). |
| [#386](https://github.com/replikativ/datahike/issues/386) | **closed** | memory leak — closed. |

The current import path also uses `api/transact` with fresh tx-ids
(`migrate.clj:32-45`), so it *cannot* reproduce history even in principle. The
right primitive is `load-entities`, which remaps ids while preserving the
`[e a v t op]` structure — see §4.

---

## 2. Corrections to the v2 spec (read this first)

A prior spec (v2) drove this work. Grounding it against `main` invalidated
several load-bearing parts. These corrections are normative here:

1. **There is no `read-string` RCE to fix.** v2's headline "PR-A closes a latent
   remote-code-execution hole" assumed the importer `read-string`s dump lines. It
   does not — it uses `clj-cbor` (`migrate.clj:14,37`). That eval vector was
   removed when the format moved to CBOR ([#496]). Any EDN-lines codec we
   introduce (§5) still uses a **closed** reader map as basic hygiene, but this is
   defense-in-depth, not a fix for a live vuln — and the PR must not claim
   otherwise.

2. **The sort key must not put retractions first.** v2 mandated
   `(t, added, e, a, v)` with `added=false` before `true`, calling retract-first
   "the safe replay order." The direct transactor is **order-sensitive and does
   not reorder within a tx**: a card-one *add* upserts (replaces the prior `[e a]`
   value; `db/transaction.cljc:524`), and a *retract* matches an exact `[e a v]`
   and is a **no-op if that value was already replaced** (`:426`, `upsert?` at
   `:1511`). So forcing retract-before-add can silently drop a same-tx
   replacement's retraction. Correct rule (§6): order by `t`, force
   `:db/txInstant` (and other tx-entity `meta-attr?` datoms) **first** within a
   tx, and otherwise **preserve source datom order**. This matches the existing
   test's live `TODO` (`test/datahike/test/migrate_test.clj:307`).

3. **No core fix is needed for forward refs.** v2 proposed a separate core PR
   ("PR-B") to allocate a target eid when a ref value points at a not-yet-seen
   entity. `transact-entities-directly` **already does this**
   (`db/transaction.cljc:1499-1500`: on `(and (ref? …) (nil? (get-in
   migration-state [:eids v])))` it `allocate-eid`s and recurs on the same
   input). PR-B is dropped.

4. **Import is not resumable; don't design as if it were.** v2 made resume
   "conditional on the `:migration` id-map being durable." It is **not durable**:
   `db->stored` omits `:migration` and `stored->db` never restores it
   (`src/datahike/writing.cljc:56-58,227-281`) — it lives only on the in-memory db
   value and is dropped on reconnect. So a partially-imported target cannot be
   resumed correctly; the honest recovery is **recreate-and-restart**, and
   `import-db` refuses a non-empty target rather than double-applying. (Export
   *is* resumable — completed chunks are content-addressed.)

5. **The value-type table was incomplete.** Current builtin types
   (`src/datahike/schema.cljc:48-71`) include `:db.type/float-array` and
   `:db.type/double-array` (added in [#896]) and `:db.type/store-ref`, which v2's
   §7.4 table omits. A type-exact codec must cover them (§5.3).

6. **`migration-state` is O(entities) and rides in the db value** — clearing it
   after a verified import (`finalize-import!`) is still warranted (§8).

Stale incidentals also corrected: the file is `migrate.clj` (not `.cljc`); tests
live at `test/datahike/test/migrate_test.clj` and run via kaocha
(`ns-patterns ["datahike.test."]`), not `./bin/run-unittests`; cljfmt is `0.9.2`.

---

## 3. Goals / non-goals

**Goals.** (G1) complete — every datom needed to reconstruct the db; (G2)
full-history round-trip under `:history? true` (`history`/`as-of`/`since`
equivalent at every `t`); (G3) type-exact (fixes #633 and covers every builtin
value type incl. the array types); (G4) correct replay order (#262 + §2.2);
(G5) bounded memory independent of db size except the stated O(entities)
`migration-state`; (G6) verifiable + tamper-evident (per-chunk SHA-256 +
order-independent semantic digest); (G7) cross-version portable (dump
format-version, #552); (G8) resumable export, honest recreate-and-restart import;
(G9) backward-compatible 2-arity surface; (G10) secure-by-default (closed reader,
path validation, secret allowlist, tight file perms).

**Non-goals.** Incremental/differential backup (format leaves room); encryption
(dumps are *signable*, signing is external); on-disk store-format changes;
Datahike→Datomic bridging; history excision (Datahike has none — see §9 PII note).

---

## 4. How import actually works (grounding for the format)

Path: `datahike.api/load-entities` → `datahike.writer/load-entities` →
`datahike.writing/load-entities` (`writing.cljc:840`) →
`datahike.core/load-entities-with` (`core.cljc:149`) →
`db/transaction.cljc/transact-entities-directly` (`:1451`). `load-entities` is
`[connection entities]`, `:stability :stable` (`specification.cljc:232-242`),
returns a throwable-promise delivered from a `go` block (`writer.cljc:299-307`) —
so callers `@`-deref to block. `entities` is a seq of `[e a v t op]` 5-tuples
(a `Datom` `seq`s to exactly that, `datom.cljc:111-112`).

Consequences that drive the format:

- **Input is `[e a v t op]`** — exactly `(seq datom)` plus the added/retracted
  boolean. Export writes these tuples.
- **Ids are remapped, not preserved.** A per-import `migration-state` maps each
  source id to a freshly allocated target id and reuses it on later sight
  (`:1490-1492`, `:1499-1500`, `:else` at `:1502`). A round-tripped db is
  *semantically equivalent*, never byte-identical in ids → verification (§7)
  compares structure, never raw eids.
- **tx entities** are recognized via `meta-attr?`
  (`= #{:db/txInstant :db/retracted :db/noCommit :db.valid/from :db.valid/to}`,
  `schema.cljc:123`); their datom uses the remapped tx id as both `e` and tx, and
  populates `[:tids t]` (`db/transaction.cljc:1482-1492`). `max-tx` is maintained
  here — the old file-scan heuristic is unnecessary (fixes #287).
- **Attributes resolve against the *target* schema** via `-ident-for`/`-ref-for`
  (`db.cljc:344-354`). Keyword idents are valid in both ref and non-ref configs;
  numeric attr ids only in ref configs → **export always writes keyword idents**
  (§5.2, fixes #508 correctly: translate, never re-insert system datoms).
- **History `:eavt` includes retractions.** `(d/datoms (d/history db) :eavt [])`
  enumerates the full temporal set incl. `added=false`
  (`db.cljc:248,509-512`; `interface.cljc:65-70`) — so a history export is
  feasible directly.

---

## 5. Dump format

### 5.1 Layout (chunked, primary)

```
my-backup/
  manifest.edn            ; metadata + digests + chunk index (written LAST = commit marker)
  datoms-000001.edn       ; EDN-lines: one datom record per line, ordered
  datoms-000002.edn
  ...
```

In-progress chunks are `*.edn.tmp`, renamed on completion; a directory without
`manifest.edn` is incomplete by definition. A single-file **flat** format remains
for small dbs and the legacy 2-arity path.

### 5.2 Record

One EDN 5-vector per line: `[e a v t added]`. `a` is **always a keyword ident**
(even for `:attribute-refs? true` sources). Ref values to *system* entities are
written as `#datahike/sysref :the/ident` and resolved by ident lookup in the
target's own system table on import (translation, not insertion). `e`/`t` are
source longs, retained for ordering + verification, remapped on import.

### 5.3 Value encoding (fixes #633; covers all builtin types)

Encoded by declared `:db/valueType` when a schema exists, else by runtime class;
imported by coercing to the **target's** declared type, else the manifest schema,
else the tag. Full builtin set per `schema.cljc:48-71`:

| valueType | encoding | import coercion |
|---|---|---|
| keyword/string/boolean/long | native EDN | native / `long` |
| `double` | `pr-str` shortest round-trip | **force** `double` |
| `float` | `#datahike/float "…"` (string, no double-rounding) | **force** `float` |
| `float-array` / `double-array` | `#datahike/farray [..]` / `#datahike/darray [..]` | rebuild primitive array |
| `ref` | source long / `#datahike/sysref :ident` | remap / translate |
| `instant` | `#inst` | `java.util.Date` |
| `uuid` | `#uuid` | `UUID` |
| `bigint` | `123N` | `BigInt` |
| `bigdec` | `1.50M` | `BigDecimal` (scale kept) |
| `bytes` | `#datahike/bytes "base64"` | `byte[]` |
| `symbol` | `#datahike/symbol "ns/name"` | `symbol` |
| `tuple` | vector, element-wise per tupleType(s) | element-wise |
| `store-ref` | (uuid form) | as uuid |

Reader map is **closed**: the `#datahike/*` tags above + EDN `#inst`/`#uuid`;
unknown tag ⇒ `:import/unknown-tag`. (This is the #633 fix at root: EDN tagged
literals carry the exact type, so `double` never silently narrows to `float` the
way CBOR's float encoding does.)

> **Open question for maintainers (codec):** keep **CBOR** and fix its
> float handling, or move to **EDN-lines** as above? EDN-lines makes type-exactness,
> determinism (byte-identical re-export → signable), and closed-reader safety
> clean, and it *removes* the `clj-cbor` dependency; CBOR is more compact and
> faster to parse at 40M-datom scale. This doc implements EDN-lines behind a
> pluggable codec seam so CBOR can remain a variant. **This is the main thing to
> settle before the big PR.**

### 5.4 `manifest.edn`

Fixed key order (determinism). Carries: `format-version`, informational
`datahike-version`/`created-at`, `history?`, `serialization`, an **allowlisted**
`source-config` (`#{:attribute-refs? :keep-history? :schema-flexibility :index}`
+ backend keyword only — never the store map), `schema` (ident→attr),
`system-idents` (source system eid→ident, for `#datahike/sysref`), `stats`,
`semantic-digest` `{:xor :sum :count}`, and `chunks` (each `{:file :count :sha256
:first-t :last-t}`). Chunk `:file` validated as `^datoms-\d{6}\.edn$`, resolved
strictly under the dump dir — no `..`, no absolute, no symlink (`:import/bad-chunk-path`).

---

## 6. Ordering rule (corrected)

Emit **schema/ident datoms first** (small pass), then data. Within the stream,
order by `t` ascending (guarantees an attribute's defining tx precedes its use —
#262 — and causal history replay). **Within a single `t`**, force tx-entity
`meta-attr?` datoms (esp. `:db/txInstant`) first, then **preserve source order**
of the remaining datoms. Do **not** globally sort `added=false` before `true`
(§2.2). `e,a,v` break ties only among datoms with no inherent source order, to
keep bytes deterministic (§7 signing). Snapshot mode (`:history? false`): all
`added=true`; same pipeline.

---

## 7. Verification (fixes #728 direction)

Two digests, two threat models: **per-chunk SHA-256** (in the manifest;
tamper-evidence; checked before import touches the db) and an **order-independent
semantic digest** (`{:xor :sum :count}` over normalized records; compares a dump
to a live, id-remapped db; explicitly *not* a security control — it is linear,
hence forgeable). `verify` returns a tiered report: tier0 (hashes/paths), tier1
(counts total/per-attr/per-tx), tier2 (id-independent multiset digest over
`[a v added]`, refs excluded and covered by per-attr ref-counts + out-degree
histograms), tier3 (sampled `pull '[*]` diff on entities keyed by
`:db.unique/identity`). Honest limit, stated in the docstring: tier2 can't verify
ref topology without graph isomorphism; tier3 sampling + degree histograms is the
practical bound.

---

## 8. Finalization

`migration-state`'s `:eids` map is O(entities); for millions of entities that is
hundreds of MB riding inside the db value. `finalize-import!` removes `:migration`
after `verify` passes; on by default (`:finalize? true`), idempotent, and a
precondition for reporting `:finalized? true`.

---

## 9. Security & data protection (normative)

- **Closed EDN reader**, no `*read-eval*`, no `clojure.core/read-string` anywhere
  in the namespace (§5.3).
- **Path validation** for chunk files (§5.4); **record-line cap** (default 64
  MiB, `:import/record-too-large`); dump files/dirs created owner-only
  (`rw-------`/`rwx------`) where the platform supports it.
- **Secret allowlist** for `:source-config` (§5.4) — never emit the store map.
- **PII / right-to-erasure:** a `:history? true` export *resurrects retracted
  data* — retraction is not erasure in an immutable db, and "deleted" values are
  written into a portable file. Docstrings + the backup guide must say this
  prominently; operators under erasure obligations should use `:history? false`
  for data crossing a trust boundary. Datahike has no excision primitive and this
  PR does not add one.

---

## 10. Public API (additive; legacy 2-arities retained)

`export-db` (`[db target]` legacy | `[db target opts]`), `import-db`
(`[conn source]` legacy | `[conn source opts]`), `verify` (`[source]` |
`[conn-or-db source]`), `finalize-import!` `[conn]`, and low-level reusable pieces
`datom-reducible`, `write-dump`, `read-dump`, `semantic-digest`. Streaming
producers/consumers are **reducibles** (`IReduceInit`), not lazy seqs, so file
handles stay scoped to the reduction. `update-max-tx`/`update-max-tx-from-file`
deprecated (warn), not removed (G9).

Error taxonomy: every failure is `ex-info` with namespaced `:error` ∈
`#{:import/format-version :import/config-mismatch :import/non-empty-target
:import/bad-chunk-path :import/checksum-failed :import/unknown-tag
:import/record-too-large :import/corrupt-datom :import/cannot-resume
:import/verify-failed :export/target-exists :export/io}` plus precise `ex-data`.
Corruption policy: `:on-error :abort` (default, halt with the offending datom +
`t` + chunk + line) vs `:collect` (skip, itemize in the report; `verify` then
fails on counts, correctly). Never silently skip.

---

## 11. GC-during-export hazard

Exporting holds an immutable db value and streams its indices. Datahike GC marks
reachability only from branch heads back to a cutoff and does **not** pin
reader-held snapshots (`src/datahike/gc.cljc:229-254`) — so running GC during a
long export on a lazy-loading backend can sweep index segments the held value
still needs. The docstring warns; the backup guide states it as a rule.

---

## 12. Sequencing

One feature branch, commits ordered for review: format + digests → `datom-reducible`
→ `export-db` → `import-db` (+ emptiness/config/taxonomy/on-error) → `verify` +
`finalize-import!` → export resumability → docs/CHANGELOG. Because the corrections
in §2 removed the RCE "PR-A" and the forward-ref "PR-B", this is a single
coherent PR, best opened *after* the §5.3 codec question is settled with the
maintainers.

---

## 13. Open questions for maintainers

1. **Codec: CBOR-fixed vs EDN-lines** (§5.3) — the main fork.
2. Chunked-directory format acceptable, or prefer single-file only?
3. Is `datahike.migrate` the right home, or should this land under the Wanderung
   umbrella whilo mentioned for 1.0?
4. Target branch — `development` vs `main` (docs disagree in places).

---

## 14. Implementation status (what has landed vs. what this doc designs)

The committed implementation delivers the correctness core AND bounded-memory
streaming end-to-end — flat and chunked EDN formats, type-exact codec (fixing
#633), history + attribute-refs round-trip, manifest + per-chunk SHA-256 + semantic
digest, `verify`, `finalize-import!`, config/emptiness/format guards, closed-reader
+ path-validation + owner-only perms, and legacy-CBOR read compatibility — with a
green kaocha suite (`test/datahike/test/migrate_test.clj`) across the persistent-set
and hitchhiker-tree indices and under spec instrumentation.

**Streaming is implemented (G5).** Export orders via an external merge sort
(`datahike.migrate.sort`): datoms stream from the index, spill into sorted runs of
`:sort-buffer` records, and k-way merge into the chunk writer, which computes each
chunk's SHA-256 and the semantic digest incrementally. Import verifies chunk hashes
by streaming each file, then feeds a tx-aligned batcher that flushes to
`load-entities` at `t`-boundaries (a transaction spanning chunk files is kept whole
because batch state carries across files); readers are scoped to their reduction
(reducible, not an escaping lazy seq). Peak memory is bounded by `:sort-buffer`,
`:chunk-size`, and `:batch-size`, independent of database size — demonstrated by a
180k-datom history round-trip under a **120 MB heap** and by `streaming-scale-test`
(tiny buffers forcing many runs + chunks).

**External stores are supported (`datahike.migrate.store`).** Besides a filesystem
path, `export-db`/`import-db`/`estimate-import-memory` accept a **konserve store**
target (an open store `{:store s :prefix ..}` or a `{:backend :s3 ..}` config), so a
diskless container can dump straight to S3 / S3-compatible / JDBC / etc. — the same
storage layer datahike itself uses, no new hard dependency. Chunks + manifest are
store keys, manifest-last is the commit marker, and per-chunk SHA-256 covers
partial/eventually-consistent reads. Round-trip (incl. every value type + the
memory estimate) is tested against an in-memory konserve store, and an
integration test (`test/datahike/integration_test/migrate_s3_test.clj`) round-trips
over the **real S3 wire protocol** against Garage in docker (endpoint override +
path-style + SigV4, via `konserve-s3`; self-skips without docker — Garage chosen
over MinIO because it is actively maintained open source). Store chunks are held
in memory as one value each, so the store-target `:chunk-size` defaults lower
(50k) than the filesystem's (1M).

**No-scratch streaming export (`:sort? false`).** For a container with no writable
filesystem at all (hard read-only), export can skip the external sort entirely:
two lazy `:eavt` passes emit schema/tx-entity datoms then data, streamed straight
to the target with zero temp files. It relaxes global tx ordering (kept:
schema-before-data and tx-entity-before-data; the one shape it drops is a same-tx
card-one replacement). Tested round-trip on filesystem and store targets, non-ref
and attribute-refs. The default remains the sorted export (uses ephemeral local
scratch).

The verification and error-handling described in §§7–10 are fully implemented:

- **`verify` tiers 0–3 (§7).** Tier 0 = chunk-path + SHA-256 (validated on open),
  tier 1 = counts, tier 2 = an id-independent multiset digest over `[a v op]` plus
  per-attribute ref counts and an out-degree histogram (so two databases with
  fully remapped ids compare equal), tier 3 = a sampled structural diff of
  `:db.unique` entities that reconstructs each entity's *net current* state from
  the dump (asserts add, retracts remove) and diffs it against `pull '[*]'` on the
  live db.
- **`:on-error :collect` is per-datom (§8.10).** On a batch failure the importer
  narrows to per-transaction, then per-datom, so exactly the offending datoms are
  recorded and skipped while everything else lands — id-consistency holds because
  the `:migration` id-map persists in the db value across `load-entities` calls.
