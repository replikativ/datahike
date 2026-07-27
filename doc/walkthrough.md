# Reviewer walkthrough — robust export/import (`import-export` branch)

*This file is a guide for an AI assistant (or a human) conducting a code
walkthrough of this branch with a reviewer. It gives the reading order, the
thinking behind each piece, and the questions worth pressing on. It is not
user documentation — that's [backup.md](backup.md); the design rationale is
[import-export-design.md](import-export-design.md).*

**If you are the assistant:** walk one stop at a time, in order. For each stop:
open the file, summarize what it does in 2–3 sentences, state the *decision*
embedded in it and why, then raise the review questions listed. Let the reviewer
steer — these notes are a map, not a script. Where the reviewer disagrees with a
decision, the design doc records the alternatives that were considered; bring
those in rather than defending the current choice.

---

## 0. Orientation (5 minutes)

One commit on top of `main`. Everything lives under `datahike.migrate`:

| File | Role |
|---|---|
| `src/datahike/migrate.clj` | Orchestrator: `export-db`, `import-db`, `verify`, `finalize-import!`, `estimate-import-memory` |
| `src/datahike/migrate/edn.clj` | Type-exact EDN codec + closed reader map |
| `src/datahike/migrate/digest.clj` | SHA-256 + order-independent semantic digest |
| `src/datahike/migrate/sort.clj` | External merge sort (bounded memory + bounded fan-in) |
| `src/datahike/migrate/store.clj` | Konserve-store dump medium (S3 / S3-compatible / any backend) |
| `test/datahike/test/migrate_test.clj` | 20 unit tests / 78 assertions |
| `test/datahike/integration_test/migrate_s3_test.clj` | Real-S3-wire round-trip vs Garage in docker |
| `dev/migrate_scale.clj` | Manual harness: build a db larger than heap, dump/restore under the cap |

Issues addressed: #633, #377, #262, #508/#531, #552-adjacent (format versioning);
#287 was already fixed upstream by batching.

Suggested first move: run the suite so everything after is discussed against
green tests:

```
clojure -M:test -m kaocha.runner :clj-pss --focus datahike.test.migrate-test
```

---

## 1. The design doc — read §2 before any code

**File:** `doc/import-export-design.md`

**Thinking:** this feature was first specified from memory of the codebase, and
several load-bearing assumptions were wrong. §2 lists each corrected assumption
with `file:line` evidence. Reading it first prevents re-litigating settled
questions and shows which behaviors are *load-bearing constraints of datahike
core*, not choices this branch made:

- The direct transactor is **order-sensitive** (upsert vs exact-match retract) —
  this is why the sort key preserves source order within a tx rather than
  sorting retractions first.
- `transact-entities-directly` **already allocates on a forward-ref miss** — no
  core change was needed.
- The `:migration` id-map is **memory-only** — which forces the
  "import is not resumable, recreate-and-restart" policy.

**Review questions**
- §13 lists the three calls that belong to the maintainer: codec (EDN vs CBOR),
  home (`datahike.migrate` vs Wanderung), target branch. These are genuinely
  open — the branch implements one defensible answer to each.
- §14 is the claims-vs-implementation ledger. Check it against the code as you
  go; anything overclaimed there is a review finding.

---

## 2. The codec — `migrate/edn.clj`

**What it is:** one datom = one EDN line `[e a v t added]`. Values encode by
**runtime class**, with a small closed tag set (`#datahike/float`, `/bytes`,
`/farray`, `/darray`, `/sysref`). Reading uses `clojure.edn` with exactly those
readers and a throwing `:default`.

**Thinking:**
- **#633's root cause is class loss**, not precision loss: clj-cbor routes
  zero/NaN/±Inf doubles through float16 and decodes them as `java.lang.Float`;
  datahike's schema predicates (`double?`) have no coercion, so the flipped
  class fails validation. Keying the encoder on the class makes the bug
  impossible rather than handled.
- **Closed readers** are defense-in-depth (there was never an eval hole — the
  old importer is CBOR), but a dump is untrusted input, so unknown tags error
  and `read-string`/`*read-eval*` never appear.
- `#datahike/float` carries a *string* to avoid double-rounding on read.
- `BigInteger` is coerced to `clojure.lang.BigInt` before printing so it reads
  back as a bigint rather than a long.

**Review questions**
- Is the tag set complete against `datahike.schema/builtin-value-types`? (Tuples
  ride as plain vectors, element-encoded; `store-ref` rides as `#uuid`.)
- The codec is EDN — **this is the main fork**. CBOR would be smaller (~40–60%)
  and faster to parse; EDN is type-exact by construction, deterministic
  (byte-identical re-export ⇒ signable dumps), human-readable, and drops the
  clj-cbor write dependency. The seam (`write-record`/`read-record`) is where a
  CBOR variant would plug in if you prefer it.

---

## 3. Digests — `migrate/digest.clj`

**Thinking:** two mechanisms, two threat models, deliberately not conflated:
- **Per-chunk SHA-256** = tamper evidence. Cryptographic, recorded in the
  manifest, checked before import touches the db, and (because dumps are
  deterministic) externally signable.
- **`{:xor :sum :count}` semantic digest** = order-independent comparison of a
  dump against a live *id-remapped* database. Linear, hence forgeable — the
  docstring says so explicitly. It is a comparison tool, not a security control.

**Review question:** confirm you're comfortable that the semantic digest is
*labelled* non-cryptographic; a reviewer skimming could mistake it for
integrity.

---

## 4. External sort — `migrate/sort.clj`

**What it is:** spill sorted runs of `:sort-buffer` records to temp files,
k-way merge with a priority queue, multi-pass when runs exceed a 64-file
fan-in.

**Thinking:**
- EAVT is ordered by `e`, not `t`; a dump must replay in transaction order
  (#262), so a sort is unavoidable *in the default mode* — and it must not hold
  the database in memory (the old exporter's `sort-by` OOM'd a 98M-datom
  export).
- **The sort key is `(t, txInstant-first, e, a)` — NOT retract-before-assert.**
  The first spec version mandated retract-first; grounding showed the direct
  transactor upserts on card-one adds and no-ops a retract whose value was
  already replaced, so reordering within a tx can silently drop datoms. Within
  a tx the key only forces the tx-entity datom first (matching the pre-existing
  `TODO` in the old test file).
- Bounded fan-in (64) keeps the merge under conservative OS fd limits.

**Review questions**
- The merge re-parses each line to recover its key (a known CPU cost, traded
  for simplicity). Acceptable, or should the key ride alongside the line?
- Temp-file hygiene: runs live in a `Files/createTempDirectory` dir deleted in
  a `finally`.

---

## 5. The store medium — `migrate/store.clj`

**What it is:** the dump can target a **konserve store** instead of a
filesystem: chunks and manifest become store keys under a prefix.

**Thinking:**
- Konserve is *datahike's own storage abstraction* — so S3, S3-compatible
  (endpoint override + path-style via konserve-s3), JDBC, etc. all work with no
  new hard dependency, reusing whatever credentials the deployment already has.
  This is what makes diskless/container deployments work.
- **Manifest-written-last is the commit marker** — deliberately *not* a rename,
  because object stores have no atomic rename. A dump without its manifest key
  is incomplete by definition.
- Per-chunk SHA-256 on read covers partial/eventually-consistent object reads.
- A store chunk is one konserve value, materialized as a string — hence the
  store-target `:chunk-size` default is 50k (vs 1M for files, which stream).

**Review questions**
- Is the key layout (`["datahike.migrate" prefix "datoms-NNNNNN"]`) acceptable
  as a de-facto format commitment?
- Would you rather chunk values be byte arrays (smaller, but loses greppability
  in store browsers)?

---

## 6. The orchestrator — `migrate.clj`

Walk it in this order:

### 6a. `export-records` / `export-records-streaming` and `export-db`
- Only user datoms (`tx > tx0`) are emitted — the bootstrap is never dumped, so
  attribute-refs targets can never collide on `:db/ident` (#531).
- Two orderings: the default external sort, and **`:sort? false`** — a
  zero-scratch two-pass stream (schema + tx-entity datoms, then data) for hard
  read-only containers. **Press on this trade-off:** it does not preserve a
  same-transaction card-one replacement (retract + re-assert of the same
  `[e a]` in one tx). That edge is documented, not detected. Is doc-level
  protection enough, or should export scan for the pattern and refuse?

### 6b. `datom->record` and the `#sysref` scheme (#508)
- Attributes are always written as **keyword idents**; ref values pointing at
  *system* entities are written as `#datahike/sysref :the/ident` and resolved
  by ident lookup **in the target's own system table** on import — translation,
  never insertion. This makes dumps independent of system-entity numbering
  across datahike versions, which is precisely the #508 hazard.
- Import seeds the target's `:migration` id-map with system-entity identity so
  `load-entities` reuses rather than re-allocates them. Verified empirically
  (`swap!` on the connection reaches the transactor's seed — see design doc).

### 6c. `run-import` — guards, batcher, report
- All guards run **before touching the db**: format-version, config
  compatibility, target emptiness, chunk checksums. Every failure is `ex-info`
  with a namespaced `:error`.
- The batcher is a single streaming reduction; a transaction is never split
  across a `load-entities` call, and batch state carries across chunk files.
- Non-empty targets are refused because **import is not resumable** — the
  id-map is memory-only, so resuming would double-apply. Recreate-and-restart
  is the honest recovery. (A future durable id-map could change this; that is a
  core-datahike question, not a migrate one.)

### 6d. `collect-apply!` — the `:on-error :collect` path
- On batch failure it narrows batch → transaction → datom, so exactly the bad
  datoms are recorded and skipped.
- **The safety argument matters:** a failed `load-entities` call applies
  nothing — the writer builds the new db value purely and only a successful
  result reaches the commit queue / `reset! connection`
  (`writer.cljc:105-112`, commit at `:140-143`). The test pins this by
  asserting every good datom landed *exactly once* after a mid-tx failure.
- **Review question:** confirm the writer's atomicity reading — this is the one
  place the feature leans on a core invariant that isn't in core's docstrings.

### 6e. `verify` — tiers 0–3
- Tier 2 is id-independent: multiset digest over non-ref `[a v op]` + per-attr
  ref counts + out-degree histogram. Tier 3 samples `:db.unique` entities,
  reconstructs their **net current state** from the dump (asserts add, retracts
  remove), and diffs against `pull '[*]'`.
- Honest limitation, stated in the docstring: tier 2/3 cannot fully verify ref
  *topology* without graph isomorphism; degree histograms + sampling are the
  practical bound.
- Subtle bug fixed during development worth knowing about: `pull` returns a
  vector for card-many **and** for tuple values — the comparator disambiguates
  by declared cardinality, never value shape.

### 6f. `estimate-import-memory`
- The one memory term that grows with data is the id-remap map
  (`~64 bytes × (entities + txs)`, held until `finalize-import!`). The manifest
  carries `:max-eid`/`:max-tx` so the estimate needs no scan. `import-db` runs
  it as a preflight and warns on stderr when `-Xmx` looks too small.
- **Review question:** the constants are calibrated against one workload; treat
  the estimate as an order-of-magnitude guide. Fine, or should it be labelled
  more conservatively?

---

## 7. Tests — what covers what

**Unit (`migrate_test.clj`):** round-trip of every builtin value type incl.
tuple, flat + chunked (#633 class assertions); attribute-refs round-trip with a
ref-resolution query (#508/#531); schema-evolution mid-history; schema-on-read;
no-scratch mode (file + store); store-target round-trip; tiered-verify positive
and tamper cases; per-datom collect with the exactly-once assertion; security
(unknown tag, `#=(...)` refusal, path traversal, chunk tamper, non-empty
target); determinism (byte-identical re-export); legacy CBOR import; memory
estimator. Runs on both index backends and under spec instrumentation.

**Integration (`migrate_s3_test.clj`):** provisions Garage v2.3.0 in docker
(config via `docker cp`, no volume mounts), creates bucket + access key through
Garage's CLI, and round-trips over the real S3 wire (SigV4, endpoint override,
path-style). Self-skips without docker. Garage was chosen over MinIO because
MinIO's community edition stopped receiving releases in 2025.

**Scale evidence (manual, `dev/migrate_scale.clj`):** a 1.2 GB file-backed
store (8.5× a 144 MB heap) exported to a 285 MB dump and re-imported +
verified under the same cap. One operational finding worth flagging to users:
`:store-cache-size` is **count-bounded, not byte-bounded** — with large values
it must be kept small on the exporting connection (documented in backup.md).

**What is *not* tested:** warm-heap throughput (the one benchmark is a
worst-case cold run); cljs (the namespace is JVM-only, as the old one was);
persistent backends in the unit suite (memory only; file/S3 covered by the
manual harness and integration test respectively).

---

## 8. The decisions that are yours (recap)

1. **Codec** — EDN-lines (implemented) vs CBOR (smaller/faster; loses
   determinism-for-signing and type-exactness-by-construction). Seam exists
   either way.
2. **Home** — `datahike.migrate` (implemented, additive, `^:no-doc` today) vs
   the Wanderung umbrella for 1.0.
3. **Target branch** — `development` vs `main`.
4. **`:sort? false` guard level** — documentation (current) vs detect-and-refuse.
5. **Chunk-key layout / value type** in the store medium, before it becomes a
   de-facto format.

If the preferred outcome is a reimplementation rather than a merge, the pieces
with the highest reuse value are: the §2 grounding facts (transactor order
sensitivity, memory-only id-map, forward-ref allocation), the class-keyed codec
approach with its closed reader, the manifest-last + per-chunk-SHA object-store
pattern, the sysref translation scheme, and the test suite — most of which is
implementation-independent.

---

## 9. Verification commands

```
# unit suite, both index backends
clojure -M:test -m kaocha.runner :clj-pss --focus datahike.test.migrate-test
clojure -M:test -m kaocha.runner :clj-hht --focus datahike.test.migrate-test

# real-S3 integration (needs docker; ~30s)
clojure -M:test -m kaocha.runner :integration --focus datahike.integration-test.migrate-s3-test

# formatting
clojure -M:format src/datahike/migrate.clj src/datahike/migrate test/datahike/test/migrate_test.clj

# capped-heap scale demonstration (writes ~1.5 GB under /tmp, several minutes)
clojure -J-Xmx140m -M:dev -e "(load-file \"dev/migrate_scale.clj\")(migrate-scale/run)"
```
