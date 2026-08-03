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
   removed when the format moved to CBOR ([#496]). Any codec we
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
  datoms-000001.cbor      ; CBOR sequence (RFC 8742): one datom per top-level item
  datoms-000002.cbor
  ...
```

The manifest stays EDN on purpose: it is read *before* the codec is known, so it
cannot itself be in the codec, and a dump whose head is human-readable is worth
its bytes when you are recovering one.

In-progress chunks are `*.cbor.tmp`, renamed on completion; a directory without
`manifest.edn` is incomplete by definition. Export always writes a directory —
there is no single-file write path. Old flat dumps (a manifest line followed by
the CBOR sequence) are still READ on import, JVM-only, via
`datahike.migrate.legacy`; nothing produces them.

### 5.2 Record

One CBOR 5-vector per record: `[e a v t added]`, written as a CBOR sequence
(RFC 8742) with no delimiter — consecutive top-level items ARE the framing. `a` is **always a keyword ident**
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

> **SETTLED — see §13.** This was the open question, and the answer is CBOR via
> boring's `:archival` profile. The §5.3 encoding above is the EDN-lines codec it
> replaced, kept here because §5.3.1 measures against it and because the reasoning
> only makes sense with both sides visible. The shipped codec is
> `datahike.migrate.cbor`; none of the `#datahike/*` tags survive, because CBOR
> expresses every one of them natively.

### 5.3.1 Codec evidence (measured, not preference)

The §5.3 note above says EDN tags fix #633 "the way CBOR's float encoding does
[not]". That is true of *shortest-form* float encoding — which RFC 8949's own
deterministic profile prescribes — but it is an **encoder policy, not a property
of CBOR**. Measured against `clj-cbor` 1.1.1, which encodes floats by class
(`codec.clj`: `(instance? Float n)` → `writeFloat`, else `writeDouble`):

| value | bytes | note |
|---|---|---|
| `(double 1.5)` | `fb 3ff8000000000000` | f64 |
| `(double 2.0)` | `fb 4000000000000000` | f64 **even though 2.0 fits f32 exactly** — shortest-form would narrow it here |
| `(float 1.5)` | `fa 3fc00000` | f32 |
| `(bigint 1234…890)` | `c2 4d …` | tag 2, arbitrary precision |
| `1.50M` | `c4 82 21 1896` | tag 4 = `[-2 150]` — **scale preserved** |
| `1.5M` | `c4 82 20 0f` | tag 4 = `[-1 15]` — distinct from `1.50M` |
| `#inst "2026-01-01"` | `c1 1a 6955b900` | tag 1, epoch |
| `#uuid "…0001"` | `d825 50 …` | tag 37 |
| `(byte-array [0 1 127 -1])` | `44 00017fff` | major type 2, no base64 wrapper |

**Cross-language check.** Those exact bytes were read with Python `cbor2`:

```
bigdec  → Decimal('1.50')                      scale intact
instant → datetime(2026,1,1, tzinfo=utc)        native
uuid    → UUID('…0001')                         native
bytes   → b'\x00\x01\x7f\xff'                native
bigint  → 123456789012345678901234567890        exact
```

This is the argument for CBOR that size and speed do not make: **a foreign reader
produces native values while knowing nothing about datahike.** The EDN encoding
in §5.3 routes every non-trivial type through a `#datahike/*` tag, which is
portable in principle and Clojure-only in practice — and the scenario a dump
exists for is precisely the one where datahike is unavailable or not trusted.

**One measured gap, and it is narrow.** `clj-cbor` encodes zero, NaN and
±Infinity as f16 regardless of class, and f16 decodes to `Float` — so a `Double`
0.0 round-trips as a `Float`. That is #633 surviving for exactly three values. It
was the one thing a move to CBOR had to address, and it is addressed: boring's
`:float-policy :preserve-width` encodes by class, so all three round-trip as
`Double`. The inverted assertion is pinned in
`test/datahike/test/migrate_codec_test.clj/float-width-survives-test` — a codec
that reintroduces the narrowing now fails there.

All of the above is pinned as byte-level vectors in
`test/datahike/test/migrate_codec_test.clj`, so it is a contract rather than a
finding: any codec — `clj-cbor`, a cross-platform successor — has to satisfy the
same octets, which makes the *format* the commitment and the *library* an
implementation detail.

**Recommendation.** Freeze the format (tag table, float policy, canonical map
ordering) in this document and in those vectors; keep the codec behind the
pluggable seam §5.3 already provides. Then the library can be swapped on
evidence, and a dump written today stays readable by anything conformant.

---

### 5.4 `manifest.edn`

Fixed key order (determinism). Carries: `format-version`, informational
`datahike-version`/`created-at`, `history?`, `serialization`, an **allowlisted**
`source-config` (`#{:attribute-refs? :keep-history? :schema-flexibility :index}`
+ backend keyword only — never the store map), `schema` (ident→attr),
`system-idents` (source system eid→ident, for `#datahike/sysref`), `stats`,
`semantic-digest` `{:xor :sum :count}`, and `chunks` (each `{:file :count :sha256
:first-t :last-t}`). Chunk `:file` validated as `^datoms-\d{6}\.cbor(\.gz)?$`
(`manifest/chunk-re`), resolved
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

## 13. Codec: settled

**Resolved — CBOR, via `org.replikativ/boring` on its `:archival` profile.**
This section previously posed EDN-lines vs CBOR as the one open fork. It is
closed, and the reasoning is worth keeping because the fork looked genuinely
balanced at the time.

EDN-lines was never chosen on its merits. It existed because `clj-cbor` narrows
zero, NaN and ±Infinity doubles to float16 and reads them back as `Float` — #633
— and every `#datahike/*` tag it carried was a workaround for something EDN
cannot express. boring removes the cause, so the workaround goes with it.

What made the decision, in order:

1. **Type exactness is no longer a reason to prefer EDN.** boring's
   `:float-policy :preserve-width` encodes by class; `(double 0.0)`, NaN and
   ±Inf all round-trip as `Double`.
2. **Determinism did not have to be traded away.** The natural CBOR answer,
   `:canonical`, is *worse* than clj-cbor here — RFC 8949 §4.2.2 mandates
   shortest-form floats, so it narrows every `Double` that fits. `:archival`
   (added for this: sorted map keys, fixed-width floats) gives byte-identical
   re-export *and* type identity.
3. **Binary values stop being second-class.** `byte[]` is major type 2 and
   `float[]`/`double[]` are RFC 8746 typed arrays, natively. Under EDN each went
   through base64 — 33% before compression, and opaque to a foreign reader,
   which defeats the reason to use a standard codec at all.
4. **Size and speed**, measured: 1.53× smaller raw, ~9× faster to decode. Under
   zstd the size margin narrows to ~5%, so this was the weakest argument, not
   the strongest.
5. **It removed JVM coupling.** The EDN codec needed `java.util.Base64`,
   `java.nio.ByteBuffer` and `Float/toString`; `datahike.migrate.cbor` is
   `.cljc`.

One byte-level vector moved that was not a defect: instants are tag 0 (RFC 3339
string) rather than tag 1 (epoch integer). Both are registered and
DATAHIKE-REQUIREMENTS §2 permits either. It costs ~18% compressed on a
transaction-heavy dump, which is being taken up with boring rather than worked
around here.

The `write-record`/`read-record` seam this section anticipated turned out to
cover only *value encoding*, not framing — and framing is exactly what differs
between a line-oriented and a sequence-oriented codec. The seam is now over
record streams (`encode-record` / `decode-records`), which is what a second
codec would actually need.

---

## 14. Implementation status (what has landed vs. what this doc designs)

The committed implementation delivers the correctness core AND bounded-memory
streaming end-to-end — the chunked CBOR-sequence format (gzip by default),
type-exact codec (fixing #633), history + attribute-refs round-trip, manifest +
per-chunk SHA-256 + semantic digest, `verify`, `finalize-import!`,
config/emptiness/format guards, closed-reader + path-validation + owner-only
perms, and legacy-CBOR read compatibility — with a green kaocha suite
(`test/datahike/test/migrate_test.clj`) across the persistent-set and
hitchhiker-tree indices and under spec instrumentation.

Export and import both run on ClojureScript/Node as well, sharing one
implementation under `async+sync`; the external merge sort is portable
(`migrate/sort.cljc`).

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
