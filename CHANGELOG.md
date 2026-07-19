# Changelog

Datahike releases continuously: every merge to `main` produces a new version `0.{minor}.{commit-count}` and a Clojars release. The complete change feed is the [git log](https://github.com/replikativ/datahike/commits/main); this file highlights **user-facing** changes — new features, stability transitions, breaking changes, deprecations, and notable fixes. Internal refactors, CI tweaks, dep bumps, and test-only changes are intentionally omitted.

When something is added, it's typically marked *Experimental*. When the API contract solidifies, a separate **Status changes** entry records the promotion (with the version it happened in).

## 0.8

### Status changes

- **Query planner promoted to the default engine** — the compiled query planner ([#795]) now runs by default; the relational (base) engine remains a permanent fallback for the query shapes the planner declines (multi-source disjoint joins, nested temporal wrappers, stats). The selector dynamic var was renamed `datahike.query/*force-legacy*` → **`*disable-planner*`** (same polarity), and the opt-*in* env `DATAHIKE_QUERY_PLANNER=true` became an opt-*out* `DATAHIKE_QUERY_PLANNER=false`. CI now runs the full suite under both engines on every build. **Beta — newly the default**, so if a query returns wrong or surprising results please [open an issue](https://github.com/replikativ/datahike/issues); you can fall back any time with `DATAHIKE_QUERY_PLANNER=false` (or `(binding [datahike.query/*disable-planner* true] …)`). ([#844])

### Features

- **cljs async foundation — the storage seam speaks async, per-read** — groundwork for browser clients querying remote backends (IndexedDB, S3). The index storage layer (`CachedStorage.restore`) now honors `{:sync? false}`: a datahike-LRU hit resolves on the calling stack (partial-cps trampolining — all-warm async computations complete *synchronously*, the property the upcoming async engine's sync mode rests on); an LRU miss tries the synchronous store read first — sync-capability is a property of **each read**, not of the store: a tiered store serves its warm memory frontend synchronously and only a frontend miss needs the async backend, so the browser deployment's synced working set stays on the trampoline — and only reads the store genuinely cannot serve synchronously fall back to the channel-adapted async path, where real IO dominates the hop. The synchronous API surfaces such a read as an actionable error (`:storage/sync-read-unavailable`) instead of konserve's internal assertion. DataScript-style synchronous use over memory / synced tiered stores is unchanged. *Experimental.* ([#886])
- **Content identity for unstructured input (`:identity :content`)** — `datahike.experimental.unstructured` can now share structurally identical nested objects instead of duplicating them. By default every nested map is its own entity; `(transact-unstructured conn data {:identity :content})` gives each nested map a recursive `hasch` content id under a `:db.unique/identity` attribute, so identical value objects collapse to one shared entity — recursively (a shared grand-child is one entity though its parents differ) and immutably (change the content and the id changes, so you can never alias-and-mutate a value object). This is content-addressing one level up from index nodes and blobs: sharing appears in the datom graph as it does in the store. Content identity gives *value* semantics and merges coincidentally-identical maps, so for *record* semantics you still declare a natural `:db.unique/identity` key, which datahike's ordinary upsert dedups under either mode. Default behaviour is unchanged. See [Unstructured input](doc/unstructured.md). *Experimental.* ([#882])
- **Blobs and out-of-line values (`:db.type/store-ref`)** — a datom value can now **name an object** — a PDF, an image, model weights — and the garbage collector will **mark** it. The rule is *the database is the root set*: an object lives iff a datom names it; retract the datom and it becomes collectable; keep history and it stays live, because an `as-of` read can still reach the datom naming it. **Where the bytes live is your choice, and the same type covers both**: put them in the database's own konserve store (`k/bassoc` under a `hasch` content hash) and `d/gc-storage` reclaims them for you, portably, on every backend — or `PUT` them straight to a raw S3 prefix from the browser with a presigned URL and a content-type, so they never transit your JVM, and use the new **`datahike.gc/reachable-store-refs`** to get the live set and sweep your own prefix. Datahike owns the hard half (what is still referenced, across branches and through retained history, honouring `remove-before`); you own the listing and deleting. Schema shapes that would defeat the mark are **rejected** rather than documented: a store-ref inside a `:db/tupleType` (the tuple's own valueType is `:db.type/tuple`, so the nested keys are invisible to the mark) and `:db/noHistory` + store-ref (retracted values are not retained, so history could name a collected object) — checked on the *resulting* schema at the datom level, so adding `:db/noHistory` to a live store-ref in a **later** transaction (a partial entity map or a raw `[:db/add …]`) is rejected too, not only the all-in-one declaration. **Not for structured data** — if you would ever filter or join on something *inside* the value, it is a document: transact it as datoms or index it. **In-store blobs also replicate causally**: the konserve-sync walker moved into datahike (`datahike.kabel.walker`) and now follows store-ref values via the new **`datahike.gc/record-store-refs`**, so a blob is shipped to a subscriber *with* the datoms that name it — ahead of the branch head — rather than leaving a live reference to an object that never arrived (the same blind spot the GC mark had). See [Blobs and out-of-line values](doc/store-refs.md). *Experimental.* ([#881])
- **diff-buf write-buffering (per-store, opt-in)** — persistent-sorted-set's diff-buf buffers a commit's content-only child diffs inside the rewritten ancestor instead of rewriting the whole root→leaf spine, cutting small-commit object writes from ~`depth+1` PUTs per index to ~1 — the biggest win on request-priced object stores. Enable at database creation with `:index-config {:diff-buf-size 256}` (default `0` = off; the on-disk format is unchanged when off). The setting is create-time-fixed and adopted from the store at connect, so reconnects don't need to re-specify it (an explicitly conflicting value raises). Composes with `:crypto-hash?`: a branch's content address folds its buffered diffs, so the merkle audit (`datahike.audit/verify-chain :deep?`) detects slot tampering like any other content; note the hash is representation-dependent — identical logical content hashes differently under different `:diff-buf-size` settings. On-disk format requires persistent-sorted-set ≥ 0.4.126 (older readers refuse slot-bearing stores via the existing version guard); **correctness requires ≥ 0.4.137**, which is what this PR pins — 0.4.136 made a commit's settle atomically published against a pipelining writer's structural-sharing copies (NodeState), and 0.4.137 made diff-buf projection land on a copy so tree versions sharing a durable anchor through the node cache can't clobber each other's reads. See [Reducing write amplification](doc/write-amplification.md). *Experimental.* ([#867])
- **Index-root fusion (opt-in)** — `:fuse-index-roots? true` inlines each index's root node into the db-record so `commit!` skips writing those roots as separate objects: one fewer PUT per index per commit and one fewer GET per index on cold open; a single-leaf index (tiny tenant) collapses to ~1 object write per commit. Composes with diff-buf. Restore is presence-based (fused and legacy records both reconnect; the stored flag is adopted at connect), GC and the merkle audit walk seeded inlined roots, and under `:crypto-hash?` the roots stay separate objects (content-address dedup could alias a root as another tree's child) so fusion there saves the cold-open GETs only. See [Reducing write amplification](doc/write-amplification.md). *Experimental.* ([#867])
- **Commit graph opt-out (`:commit-graph? false`)** — every commit normally persists an immutable record under its commit-id, forming the provenance chain consumed by `datahike.audit/verify-chain`, ancestry walks, `branch!`-from-commit-id, and `dh://…?commit=` references. Stores that need none of these (typical `:keep-history? false` tenants) can now opt out at creation: commits then write only the branch head — the dominant per-commit garbage object disappears (measured ~87% of garbage objects under sustained load) and, combined with `:fuse-index-roots?` and diff-buf, a buffered commit becomes a **single object write**, which also makes S3-conditional-write fencing possible. The commit-id is still computed and stamped in `:meta` (identity, sync dedup, and the writer's head-cid threading are unaffected), one-step lineage is still recorded, and time travel (`as-of`/`history`) is unaffected — history lives in the temporal indices, not the commit graph. The flag is store-fixed and adopted at connect like `:fuse-index-roots?`; combining it with `:crypto-hash? true` is rejected at creation, and `branch!` from a commit-id fails with a self-explaining error (branching from a branch works unchanged). See [Reducing write amplification](doc/write-amplification.md). *Experimental.* ([#869])
- **Background concurrent GC (`datahike.gc/start-background-gc!`)** — periodic full-reachability mark-and-sweep that runs *concurrently* with an active writer: marking is read-only, and the sweep stops at the store's **safe point** rather than at `now`, so the objects of a commit that is mid-flush — written, but not yet named by any head — are spared. This requires the writers and the collector to be **in the same process** (see [#879]). Unlike the freed-address online GC it supports **multiple branches**, needs no mutation-time tracking (it reclaims garbage that freed-tracking cannot see, e.g. under a pipelining writer), and prunes commit-graph history when `:history-window-ms` is set (retention is governed entirely by that / `gc-storage!`'s `remove-before`). The two collectors compose: prefer online GC's freed-tracking + freelist recycling for single-branch bulk imports (O(garbage), keeps object counts flat), and this collector as the completeness pass. Cross-platform (JVM + cljs). *Experimental.* ([#868], [#879])
- **Cross-database references (`datahike.reference`)** — value-level references that point *across* stores in a distributed deployment, serialized as `dh://` URIs. A reference is the triple **(db-id, selector, temporal)**: store `:id` + a `[unique-attr value]` lookup ref or bare entity id + an optional version. The URI temporal is a standard URL query string — `dh://<db-id>/<attr>/<value>?tx=…&date=…&valid=…&commit=<uuid>&branch=…` — where `?commit=<uuid>` pins an exact content-addressed commit (resolved via `commit-as-db`), the most precise record reference. `render`/`parse` round-trip and encode a selector value of **any** datahike value type — readable tags for the identity-friendly scalars (untagged UUID, `str:`/`long:`/`kw:`/`bool:`/`inst:`), `flt:`/`b64:` for float/bytes, and an `edn:` fallback that losslessly preserves `bigint`, `bigdec` (with scale), `double`, `symbol`, and `tuple`. `resolve-reference` is strict-by-default (selector must be `:db/unique`) with explicit opt-in for non-unique selectors, and takes an injected `connect-fn` so deployments own peer/grant/branch selection. Outgoing links can be reified as datalog-queryable `:dh.ref/*` entities. *Experimental.* ([#852])
- **Attribute-value constraints** — opt-in per-attribute value validation. `:db/maxLength` bounds a value's length — chars for a `:db.type/string`, bytes for a `:db.type/bytes` — declarative and cljs-safe; `:db.attr/preds` names value predicates by symbol (like `:db.entity/preds`), each resolved via a process-local registry (`datahike.attr-preds/register-attr-pred!` — the cross-platform path) or, on clj, a `requiring-resolve`-able var (Datomic-style). Predicates are `(fn [value] -> truthy)`; a non-`true` return aborts the transaction with `:transact/attr-pred`. Enforced on assertion only (never on retract) under **both** `:read` and `:write` schema-flexibility, gated on an O(1) rschema lookup so unconstrained attributes pay nothing, and addable to already-defined attributes. Under `:attribute-refs? true` the constraint attrs get stable system entity IDs (like `:db.secondary/*`); a pre-existing attribute-refs store needs an ident-ref-map migration before it can use them. *Experimental.* ([#861])
- **Opt-in value-size caps** — value-size caps bound oversized values that would otherwise silently bloat the index and hit backend limits (e.g. a 32 KB string re-writes ~30 MB per neighbouring commit; DynamoDB items cap at 400 KB). They are **opt-in**: pass `:value-caps :default` at `create-database` for the Datomic-parity caps (`:db.type/string` → 4096 chars, `:db.type/bytes` → 4096 bytes, string slots inside a `:db.type/tuple` → 256 chars), or set `:max-string-length` / `:max-bytes-length` / `:max-tuple-string-length` individually (`0` disables one; an explicit key wins over the preset). A per-attribute `:db/maxLength` always wins and applies under both `:read` and `:write`; the config-level caps apply only under `:write` and to user attributes (`:db/doc` and other system string attrs, and `:db.secondary/only` values, are exempt), and raise `:transact/max-length`. **A database created without any of these is left unbounded** (unchanged from before the feature), and `create-database` logs a one-time warning nudging you to make the choice explicit. Store large payloads off-database as a pointer or in a secondary index. *Experimental.* ([#861])
- **Store-level transaction predicates** (`datahike.tx-preds`) — the `tx`-level member of the predicate family, complementing `:db.attr/preds` (per value, on assertion) and `:db.entity/preds` (per entity, opt-in via `:db/ensure`). A tx-pred is `(fn [tx-report] -> …)` registered per store-id (`datahike.tx-preds/register-tx-pred!`, out of band — never placed in the serialized config) and run on the fully-resolved report (`{:db-before :db-after :tx-data}`, real eids + retract flags) of **every** committed write; a thrown `Exception` (not an `Error`) rejects the transaction — the error reaches the caller, the chain does not advance, nothing persists. Unlike `:db/ensure` it is **mandatory** (fires regardless of the transaction's shape — the trust-boundary property a governed store needs) and, because it sees the resolved retract flags, it can also guard **destructive** operations. Ungoverned stores pay a single map lookup. *Experimental / internal.* ([#861])
- **`:db.secondary/only` — out-of-line values** — flag a string attribute `:db.secondary/only true` to store its value *only* in the covering secondary index (Scriptum/Lucene); the primary EAVT/AEVT/AVET hold a `hasch` content hash in its place, keeping the primary indices small for large/unbounded payloads (transcripts, web pages, agent context) while the value stays fully searchable. Retraction/uniqueness/dedup work on the hash. The whole `:db.secondary/*` schema family was graduated into the system schema (stable entity IDs, mirroring `:db.valid/*`) so secondary indices align across attribute-refs databases. **Search-only** — the value is not reproducible from the primary; declare it only where the canonical value lives elsewhere. *Experimental.* ([#840])
- **Optimistic overlay** — new `datahike.optimistic` primitive lets UIs render a transaction's effect immediately and re-fire listeners when the writer confirms (or fails). Most useful with a remote writer (e.g. KabelWriter over WebSockets). *Experimental.* (0.8.1690, [#822])
- **Versioning API promoted** — `datahike.experimental.versioning` (introduced in late 2022) is renamed to `datahike.versioning` as part of the planner / secondary-index / versioning PR; subsequently exposed in libdatahike + pydatahike. (0.8.1664, [#795], [#831])
- **Versioning bindings in libdatahike + pydatahike** — `branches`, `branch!`, `delete-branch!`, `merge-db`, `commit-id`, `parent-commit-ids` are now exposed in the native C and Python bindings, plus new `branch:NAME` / `commit:UUID` input formats for loading a DB at a specific branch or commit. (0.8.1689, [#831])
- **Tamper-evident audit chain** — under `:crypto-hash? true`, `create-commit-id` now hashes post-flush merkle leaves of the DB so the commit-id becomes a true merkle root; new `datahike.audit/verify-chain` walks parents and reports `:status :commits :mismatches :missing`, with an optional `:deep?` PSS walk that reads each node directly from konserve and detects bytes-level tampering. *Experimental.* (0.8.1682, [#823])
- **Query planner, secondary indices, and versioning API** — plan-based execution with fused EAVT/AEVT scan+merge, predicate pushdown, ORDER BY / offset / limit, recursive rules with semi-naive fixpoint, magic-set, and an attribute-dep-aware query result cache. Introduces `ISecondaryIndex` / `IColumnarAggregate` protocols with Proximum (vector), Scriptum (full-text), and Stratum (columnar) bridges, plus a `d/explain` plan view. Originally opt-in via `DATAHIKE_QUERY_PLANNER=true`; **now the default engine** (see Status changes). (0.8.1664, [#795])
- **Pull-pattern attrs tracked in query cache** — the query result cache now invalidates entries whose dependencies overlap with attributes referenced only inside `(pull ...)` expressions in `:find` (wildcard / variable pulls conservatively produce `:all` deps). (0.8.1671, [#810])
- **CLI `--tx-file` wired through** — `dthk transact --tx-file <path>` now reads file contents and injects them as the tx-data argument; the async `transact!` variant is no longer surfaced on the CLI. (0.8.1666, [#803])

### Notable fixes

- **Planner: a predicate on a join-eliminated variable no longer reads a foreign slot** — `[?p :code ?c] [(.startsWith ^String ?c "X")] [?b :parent ?p]` with `:find ?b` threw `Long.startsWith` under the planner; with a numeric predicate (`[(< 50 ?n)]`) the same shape returned **wrong results silently**. The fused direct path hoisted the first scan's attached predicate into post-processing — which indexes result tuples by the union of all group variables — but sent that scan down the collect-only producer path, which contributes no columns: the predicate then read the *consumer's* tuple layout, resolving `?c` to a join entity id. A producer whose columns post-ops need now materializes its tuples (probe-map path), so the predicate filters the actual values — before the join, where it also shrinks the probe set. Base engine was always correct; regression pins cover the throwing, silent-numeric, and control shapes. Surfaced by kontor's per-country tax-provider tests. ([#887])
- **GC clock correctness — write stamps and collection cutoffs share one monotonic source** — the sweep's safety argument ("every object a guarded write sequence produces carries a `last-write` ≥ the sequence's safe-point") compared timestamps from **two independent wall-clock reads** (datahike's gc-guard and konserve's write stamping) and silently assumed machine clocks are monotonic. They are not: an NTP step-back, VM suspend/resume or manual clock set inside the guard window could stamp a *live* mid-flush object before its cutoff, and the sweep would delete it — dangling pointers on the next cold read. konserve's `:last-write` is now a **monotone clock** (`max(wall-clock, previous-stamp)` — never retreats, reads as true wall time with zero drift under any write rate; deliberately non-strict, since a `+1`-per-stamp clock would outrun physical time above 1000 writes/second and stall collection after restarts), and every collector-side clock read — the gc-guard safe-point, `gc-storage!`'s cutoff, background-GC's retention boundary, online-GC's freed-address stamps and grace cutoff — reads the *same source*, making happens-before literal in the stamps instead of an assumption. Same-millisecond ties remain possible and are fail-safe — the sweep spares equality, retaining garbage one extra cycle at worst, never deleting a live object. Requires konserve ≥ 0.9.362 (replikativ/konserve#156). Single-process writers+collector, as before ([#879]). ([#885])
- **Planner no longer leaks a nested subquery literal's variables into the outer scope — and unresolvable clauses now raise on both engines** — a query chaining `[(datahike.api/q [:find ?p …] $a) ?v] [(count ?v) ?n] [(= 0 ?n) ?m]` returned `nil` under the planner where the base engine returns the value: the planner's recursive argument walk surfaced the literal's lexically-scoped `?p` as an outer-scope *input* no producer could ever bind, so ordering emitted the chain in arbitrary order and execution silently dropped the unrunnable clauses. The dual leak was worse — the ordering accumulator marked the literal's `?p` as *bound*, so a later consumer of a **real** outer `?p` could be ordered before `?p`'s actual producer and silently dropped, yielding wrong nil-padded rows. Both directions are fixed at every site (ordering, replan, direct-path eligibility, predicate gating): input vars come from a walk that treats data literals as opaque, and produced vars come from the binding form. Underneath sat a lost invariant: #814 guarded the executor against a function clause running with unbound inputs by *silently dropping it*; #815 removed the guard as "structurally unreachable" — a premise the ordering fallbacks never satisfied — after which the nil *wiped the whole context*. The invariant is now executable instead of prose: every function delegate raises the base engine's **"Cannot resolve any more clauses"**, unknown plan ops raise instead of silently skipping the constraint, and `query-engine-parity-test` pins engine agreement. Surfaced by kontor's datalog integrity invariants. ([#883])
- **Nested expression forms in function/predicate arguments are rejected** — `[(and (= ?x 999) (some? ?x)) ?out]` never meant what it reads as: arguments are resolved *flat* (only top-level `?var` symbols are substituted), so the inner comparisons were passed as literal lists and `?out` was bound to a truthy list regardless of `?x` — on **both** engines, silently. Such clauses now raise at clause-processing time with a pointer to the fix (bind each sub-expression in its own clause). `(quote x)` is the one seq form allowed, and it now actually works: the argument resolvers unwrap it to its constant, so `[(contains? '(:a :b) ?x)]` compares against the collection instead of the literal `(quote …)` list. **Breaking** for queries that relied on the truthy-literal accident — those were computing garbage. ([#883])
- **Schema changes are validated on the RESULTING state — the raw-datom bypass is closed** — schema-transition rules (`find-invalid-schema-updates`, completeness) only ran on the entity-map transaction path, so `[:db/add <attr> <schema-prop> <v>]` vectors bypassed them entirely: retroactive `:db/unique` over duplicates was accepted (upsert then *created a third entity* with the "unique" value; lookup-refs resolved arbitrarily), `:db/valueType` changes over existing data poisoned the index with mixed types whose comparison failures are silently swallowed, and cardinality `many→one` narrowing — accepted on **both** paths — left entities where `q` returned two values, `pull` one, and `d/entity` crashed. Validation now runs once at the end of the transaction loop on every *changed schema entry's resulting state* (the same deferred-chokepoint discipline as the store-ref `key-bearing-misuse` guard and the cross-tx valid-time check), uniformly over both paths, retracts and any datom order, with data-backed rules: `:db/valueType`/tuple-definition changes, entry removal, `:db/index` enablement and `:db/unique` addition require the attribute to be **unused** (retroactive uniqueness/indexing is unenforceable — pre-existing datoms are absent from AVET, which the constraint check consults); `many→one` requires no entity to hold multiple values; composite `:db/tupleAttrs` must not reference attributes defined as cardinality-many or as tuples (undeclared references keep their nil-slot semantics). **Breaking** for transactions that relied on the bypass — they were corrupting state. ([#883])
- **`:search-cache-size` works again — and defaults to off** — the per-snapshot datom search cache had been gated on `:cache-size`, a config key that stopped existing when #503 (Nov 2022) renamed the knob to `:search-cache-size` without updating the reader: the cache was silently dead for three years while the knob stayed spec'd, documented and env-configurable. The gate and threshold now read the real key, and the cache key is scoped to store + branch + snapshot (the bare additive `:hash` could collide across databases). Default is explicitly `0`: benchmarks show the cache *hurts* the default memory/PSS setup (~40% slower repeated entity access — LRU bookkeeping costs more than a PSS lookup) and helps lazy-loading stores like `:file` (~15%); opt in per database where it pays. A test now pins the config-key ↔ reader wiring. ([#883])
- **Planner: card-many attributes survive sharing a group with `get-else`, and projected-away card-many values no longer duplicate result tuples** — two pre-existing direct-path bugs found by the new generative differential test (see below). (1) A scan group containing an optional (`get-else`) merge was routed to the per-cursor merge pipeline, which does a single `lookupGE` per merge — card-ONE semantics — so a card-many attribute in the same group silently yielded only its first value (`[?e :name ?n] [?e :tag ?t] [(get-else $ ?e :nick "none") ?v]` lost `[100 :red]`). Mixed groups now route to the card-many merge, which learned to emit optional defaults on card-one misses. (2) With a card-many attribute as the *driving* scan and its value var projected away (`:find [?e]`), the dedup-strategy selector missed the duplicate projections and took the no-dedup fast path — the returned "set" contained the same tuple twice. The selector now recognizes card-many driving scans with projected-away value vars. ([#883])
- **Planner: sorted-merge no longer truncates a card-many DRIVING scan; columnar aggregates deduplicate and never return row indices** — three more pre-existing wrong-results bugs found by widening the generative grammar (stacked modifiers, clause-order permutation, aggregates, temporal wrappers) and sweeping 30k seeds. (1) When clause order promoted a card-many attribute to the driving scan (`[(some? ?n)] [?e :tag ?t] [?e :name ?n]`), the sorted-merge pipeline advanced per entity and emitted only the FIRST value; the pipeline selector now requires a card-one driver (card-many drivers use the per-cursor merge, which runs per scan datom). (2) The columnar aggregate bridge aggregated the fused scan's wide, UNDEDUPLICATED rows — `[:find (count ?e) …]` over two clauses returned the row count (10) instead of the entity count (4), and grouped counts/sums were inflated the same way; it now projects to the find-tuple space and deduplicates first, exactly like the relation engine. (3) Value aggregates (`min`/`max`/…) over non-numeric columns returned the engine's argmin-style ROW INDICES as values (`[:find ?e (min ?n)]` yielded `0,1,2…` instead of names); non-numeric value aggregates now fall through to the relation path. After the fixes a 30,000-seed differential sweep over the widened grammar finds zero divergences. ([#883])
- **Generative differential testing** — new `query-differential-test` generates seeded random query shapes (patterns, predicates, fn chains, get-else, not/or, card-many, refs) and asserts the planner agrees with the base engine on every one; a fixed seed keeps CI deterministic and cheap (~100 cases, ~1.5s), `DATAHIKE_DIFF_CASES=5000` fuzzes deeper locally with automatic shrinking. It found both bugs above within its first 250 generated cases. ([#883])
- **Planner: `get-else` over a named source keeps left-outer semantics** — `[$data ?e :name ?n] [(get-else $data ?e :nick "none") ?v]` silently dropped the entities the default is *for*: `LOptionalScan` discarded get-else's source argument, so the optional scan could not fuse with its sibling `$data` scan and ran standalone on the plain scan path — an inner join. It only ever worked on the default `$` source, where fusing turns it into an optional *merge*. The source is now carried through (named-source optional scans fuse again), and a genuinely standalone optional scan — e.g. its entity var bound by an `:in` collection, previously broken on the default source too — routes through the left-outer `bind-by-fn` path naming the op's actual source. ([#883], [#884])
- **`d/explain` reports the path a query actually takes** — it resolved only the `'$` source (NPE for named-source queries like `:in $data`), rebuilt a fresh plan (which could diverge from the cached plan execution uses), and printed `engine: planned` unconditionally — even for queries that execute on the legacy engine, split into cartesian components, or take the direct/columnar fast paths. It now resolves the database exactly like execution, shows the same cached plan, and reports the dispatch path (`legacy` / `cartesian-split` / `direct` / `columnar-aggregate` / `relation`). ([#883])
- **A concurrent collection no longer deletes the objects of a commit that is mid-flush** — `gc-storage!` captured `now`, marked from the branch heads, and swept every unreachable object written before `now`. But a commit writes every value the new head references and only *then* flips the head (the barrier invariant). Inside that window its objects are on disk, named by nothing, and **already older than `now`** — so a collection that started mid-flush classified them as garbage and deleted them, after which the head landed pointing at deleted objects. The next cold read threw `Node not found in storage`; a swept `schema-meta` was worse still, because `stored->db` falls back to the inline schema and loses the stored one *silently*. The previous release documented the opposite ("anything a concurrent commit writes is spared"): the argument holds for a commit that *starts* during a collection, and is false for one that was already in flight. Both entry points were affected — `start-background-gc!` and `d/gc-storage!`, the latter because the writer dispatches it as a background op rather than serializing it. The sweep now stops at the store's **safe point** (`datahike.gc-guard`): the instant before which every written object is either reachable or garbage — `now` when nothing is in flight, and the start of the oldest in-flight write sequence otherwise. `commit!`, `branch!`, `force-branch!` and `create-database` all hold the guard, so it covers the paths that never touch the writer as well. **The collector must run in the same process as the writers**; datahike cannot detect a second process for you, and a collector in one will sweep the first's live commits. ([#879])
- **`force-branch!` wrote its branch head before the values it names** — it published the branch into `:branches` *first*, then wrote the nodes and the head. Since GC builds its whitelist from `:branches`, a mark in that window found a branch whose head object did not exist and contributed nothing for it. Its atomic-multi-key path also passed a **map** to `multi-assoc`, discarding the ordering that lets a konserve-sync subscriber relay a commit in the order it was committed (the same defect fixed for `commit!` in [#875]). Now: values first, head next, `:branches` last — and an ordered batch. `commit!` had a smaller sibling of this bug: on the non-atomic path it *issued* the commit record and the branch-head write concurrently, so the mutable head could land before the immutable record it names, truncating `branch-history` on a crash. It now awaits the record first. ([#879])
- **`delete-database` now actually deletes — and has finished when it returns** — `-delete-database*` ended with `(ks/delete-store (:store config))` inside a `go-try-` **without awaiting it**. konserve's `delete-store` defaults to `{:sync? false}` and the async backends hand back a channel, so the `go-try-` yielded *that channel* as its value: `delete-database` resolved — returning a raw `core.async` channel to the caller — while the store was still being deleted. `(d/delete-database cfg)` followed immediately by `(d/database-exists? cfg)` still reported the database as present, and delete-then-recreate raced its own deletion. **On S3 nothing was deleted at all**, because konserve-s3's own `-delete-store` had the same missing await (konserve-s3#12) — so deleting a database (offboarding a tenant, GDPR erasure) silently did not happen: the objects stayed in the bucket and `database-exists?` kept returning `true`. `delete-database` now awaits the deletion. Requires konserve ≥ 0.9.357, which makes every backend's `-delete-store` honour `:sync?` — `:memory`/`:file` used to return a plain value where the contract promises a channel, and `:tiered` dropped its backend's channel entirely, so a tiered delete over S3 removed nothing (konserve#152). ([#877])
- **Commits are written as an ordered batch — the branch head goes last** — on a store with atomic multi-key writes (`:memory`, IndexedDB) `commit!` built its batch as a **map**: `(into {} pending-kvs)` with the branch head `assoc`'d in alongside the index nodes. A map has no order, so the causal relation the commit depends on — *every node the new head references is written before the head* — was thrown away at that line, and had to be reconstructed downstream by konserve-sync sorting on the *shape of the key* ("keywords are roots, send them last"), a heuristic that is silently wrong for any store whose keys don't fit the guess. The non-atomic path (S3, file, LMDB) never had this problem: it writes the nodes, awaits them, then the head. The batch is now an **ordered sequence with the mutable head last** on both paths, using konserve's ordered `multi-assoc` (sequence order = apply order). That makes the invariant explicit and portable: any prefix of the batch leaves the store consistent — the nodes are written but unreachable (collectable orphans), and the head flips only once everything it references exists — so a torn batch can never produce a head pointing at values that were never written, *without* requiring atomic multi-key writes, which object stores and filesystems cannot provide. A sync subscriber now also applies a commit in the order it was committed, rather than an order guessed back from key shapes. Requires konserve ≥ 0.9.356. *Experimental.* ([#875])
- **Streaming peer no longer exposes a db its indices don't back** — `connect-kabel` published a queryable db the moment the `:db` root arrived (cold path) or was found in the local cache (tiered shortcut); neither checked that the index NODES the root points at were actually present. The cold path was safe only by accident — konserve-sync ships nodes before the head — while the cached shortcut had **no node check at all**, so a peer reconnecting onto a partial cache (a root whose subtree an interrupted sync never finished writing) published a db whose first query walked into an absent node, tripping konserve's sync `-blob-exists?` on an async-only backend (browser, tiered memory+IndexedDB). Exposure now waits for konserve-sync's `:on-complete` — the handshake fully drained — for **both** paths, and the blind cached shortcut is gone; the cached root is merely a pre-fill, never a publish shortcut. The signal is exact: kabel acks a handshake batch only after applying every item in it, and the server sends `handshake-complete` only after that ack, so `:on-complete` arrives strictly after the last node *and* the head have landed. This needed a kabel fix to work at all — `subscribe!` accepted an `:on-handshake-complete` callback and silently dropped it, so the signal could never fire (kabel 0.3.100); a failed subscription now also raises instead of waiting forever on a handshake that will never come. Requires kabel ≥ 0.3.100. *Experimental.* ([#874])
- **Streaming reader's local cache now warms (tiered `:frontend-only`)** — a KabelWriter reader whose store is a tiered `{local, shared}` cache with `:write-policy :frontend-only` (e.g. `{lmdb, s3}` — a local LMDB node cache over the writer's shared S3, which the reader must never write) never populated its local frontend on live commits: konserve-sync's node push was subscribed on the *tiered* store, whose `:frontend-first` `exists?` check falls through to the shared backend — which already holds every content-addressed node — so every pushed node read as "already present" and was skipped, leaving the frontend permanently cold (served only by lazy read-through). The reader now subscribes konserve-sync to the frontend store directly, so streamed commit nodes land in — and existence is checked against — the local cache. Other tiered modes (write-through `{memory, indexeddb}`, where the backend *is* the local durable tier) are unchanged. Requires konserve ≥ 0.9.355 (the `:frontend-only` tiered write policy). *Experimental.* ([#873])
- **`gc-storage!` no longer NPEs on partially-swept lineage** — the mark phase destructured each commit record without a nil check, so a parent already removed by an earlier GC pass with a narrower window (or absent under `:commit-graph? false`) crashed the walk. Missing records now terminate that lineage cleanly. ([#869])
- **Fatal errors no longer hang transacts** — a fatal `Error` (assertion failure, OOM, …) inside the async write pipeline used to hang every in-flight and subsequent `transact` silently: the commit go-block only caught `Exception`, so an escaping `Error` killed the dispatch thread and left the writer parked forever on a silent channel; a dead transaction loop also left its queues open, accepting transacts whose callbacks could never fire. Now `commit!` converts fatal errors at the go boundary, a dying transaction loop closes its queues, and dispatching to a shut-down writer fails immediately with `:writer-shut-down`. Failure semantics are loud-and-safe: callers get the error, the writer shuts down, the store stays at the last good commit (the HEAD flip never happened). ([#867])
- **Commit crash on deep trees after reconnect (`NullPointerException` in `Branch.child`)** — reconnecting to a store and then mutating could leave an index's cached count unknown (persistent-sorted-set defers the subtree-count recomputation); the canonical root write handler then recomputed it on the storage-detached stored copy during the next commit, NPE-ing while materializing lazy children. Hit with deeper trees (small branching factor, or any large index whose SOFT-referenced children were collected under heap pressure) — the commit failed mid-write. Fixed upstream in persistent-sorted-set 0.4.132 (the root write handler serializes the cached count as-is — possibly "unknown" — and readers resolve it lazily with their own storage attached; this also covers ClojureScript, where storage is async). Guarded here by a seeded end-to-end regression test. Found by a diff-buf equivalence probe; pre-existing, independent of diff-buf. ([#867])
- **Query planner: variable-attribute cross-source Cartesian product** — a multi-source query with a variable-attribute pattern `[$b ?e ?a ?v]` (the attribute is a logic var — e.g. a join attribute retrieved from the data itself) whose value was produced by a function and reached through two or more linked entity-groups on the driving source was mis-ordered: the value-producing op ran *after* the variable-attribute scan, so the scan ran unconstrained and the cross-source join collapsed onto the non-selective attribute variable, yielding a Cartesian product. Such scans are now recognized as correlated joins and ordered after their attribute/value producers, so the join is selective. The legacy (relational) engine crashed outright on the same shape (`No matching clause: N` in `resolve-pattern-lookup-ref-at-index` — a relation-tuple position index exceeded the datom's five slots on a wide multi-source relation); such positions now resolve to the value unchanged. As a bonus, a variable-attribute scan whose attribute and value are both bound upstream now AVET point-seeks per `(attr, value)` pair instead of full-scanning — the dynamic-attribute join over a large target database drops from O(all datoms) to O(pairs · log n) (≈8000× on a 100k-datom benchmark). ([#865])
- **Query planner: as-of card-one merge skipped older visible values** — a date/tx `as-of` query whose timepoint fell between an attribute's initial and updated `:db/txInstant` could drop the older-but-still-visible card-one value (e.g. an entity's original `:age`): the fused merge found the current value, rejected it as too new, and stopped instead of looking back through temporal history. The merge now falls back to the assembled temporal slice. Surfaced by the native babashka-pod test suite; reproducible on JVM and JS. ([#863])
- **Query planner: `get-else` over `d/history` enumerated every version** — `[(get-else $ ?e :attr default) ?v]` is single-valued (the legacy engine returns one value or the default per entity), but on a `HistoricalDB` the planner forced the merge card-many and emitted one row per historical version (and, for card-many attributes, one row per value). `get-else` merges are now single-valued regardless of temporal type or attribute cardinality. ([#863])
- **Tiered store durability — silent data loss fixed** — writes through a tiered store with a memory frontend (the recommended config for browser/IndexedDB) could fail to persist index nodes to the durable backend. The memory frontend returns stored index roots by reference, so they carried the create-database connection's storage handle; a later connection then flushed new nodes into an orphaned buffer that commit never drained, leaving the backend with a root pointing at node blobs that were never written. In-process reads (served from the live frontend) masked it; the corruption surfaced as `Node not found in storage` on any cold read of the backend — process restart, a second peer, or dropping the frontend. Storage is now treated strictly as connection-scoped context: index roots are detached before entering the store and (re)bound to the reading connection on materialization (`datahike.index/with-storage`), so a flush always targets the connection's own storage regardless of how the value came back. Plain `:file`/`:memory` stores were unaffected. ([#854])
- **`commit-as-db` accepts a connection again** — `datahike.versioning/extract-store` detected connections via `instance? Connection`, which silently misfired when `deftype` recompilation (circular loads, REPL reloads) drifted the `Connection` class identity: a live connection fell through to the raw-store branch and `commit-as-db` threw a konserve `get-lock` NullPointerException instead of loading the commit. It now detects a connection via `IDeref`, matching the documented "connection, db value, or raw store" contract. ([#852])
- **Imports are now batched** — `datahike.migrate/import-db` now imports flat-files in configurable batches (`datahike.migrate/*import-batch-size*`, default `10000`) instead of one transaction. ([#845])
- **Query result cache bounded by size, not just snapshot count** — the attribute-dep-aware query result cache ([#795]) was an LRU capped only by DB-snapshot count (`*query-cache-size*`, default 64); each cached snapshot bucket can hold arbitrarily large result sets, so a handful of large queries could pin gigabytes of decoded tuples on the heap even though the snapshot-count cap never triggered. Adds a cumulative weight budget `*query-cache-weight-limit*` (default 1,000,000 result tuples; env `DATAHIKE_QUERY_CACHE_WEIGHT_LIMIT`; `set-query-cache-weight-limit!`; `0` disables) backed by a new size-aware `datahike.lru/weighted-lru` that evicts least-recently-used snapshots until the total is within budget, always retaining the most-recent snapshot. *Experimental.* (0.8, [#859])
- **Purge propagates to secondary indices** — `:db/purge` / `:db.purge/entity` / `:db.purge/attribute` / `:db.history.purge/before` now route a retraction event (`-transact` with `:added? false`) to every secondary index covering an affected attribute, the same way normal `:db/retract` does. Previously `with-temporal-datom` bypassed `update-secondary-indices` entirely, so purged datoms silently lingered in Scriptum (full-text), Proximum (vector), and Stratum (columnar) indices — a GDPR compliance gap. ([#832])
- **Query planner: stability fixes** — central `op-required-vars`; per-branch produced-vars in OR(-JOIN) bind contract; `not=` / `==` predicates pushed into AVET scan no longer silently dropped; NOT / predicate / or-join binding regressions; ctx-nil guard + nested form-args walker; CLJS-array tuple handling in `execute-pattern-scan` and `-collect`; rule bodies routed through the IR pipeline; function-only base cases in recursive rules; `:pushdown-preds` applied in temporal standalone pattern-scan. ([#813], [#814], [#815], [#816], [#818], [#819], [#821], [#825], [#826], [#827], [#806], [#807])
- **Query planner: temporal merge fast path** — a fully-unbound multi-attribute query over a history or as-of database (e.g. `[?e :name ?n] [?e :age ?a]` on `(d/history db)`) now drives the entity-group merge with a single forward cursor over the temporal index instead of one root-anchored seek per entity. On a 20k-entity history the `name + age` history join drops from ~65 ms to ~16 ms, with results bit-for-bit identical to the relational engine; Datahike now leads Datomic on all nine as-of/history shapes in the cross-database benchmark (see [query-engine.md](doc/query-engine.md#performance)). ([#844])
- **`d/explain` on temporal databases** — `explain` no longer throws when given a `HistoricalDB` / `AsOfDB` / `SinceDB` (the temporal wrappers carry no own indices); it now plans against the origin database's indices. ([#844])
- **Proximum secondary `-sec-flush` regression** — uses `IndexLifecycle/sync!` and surfaces the post-sync `:merkle-root` so the audit chain folds proximum nodes correctly; fixes NPE on every commit and stale commit-ids in the key-map. (0.8.1683, [#824])
- **`AsOfDB` / `SinceDB` reader swap** — `since-from-reader` was constructing `AsOfDB` and vice versa, causing silent incorrect behavior when deserializing `#datahike/AsOfDB` / `#datahike/SinceDB` tags via EDN or Transit (e.g. in the HTTP client). (0.8.1668, [#805])
- **Query cache: variable-in-attribute-position** — patterns like `[?e ?a ?v]` previously tagged cached entries with an incomplete dependency set, making them immune to invalidation; they now produce `:all` deps. (0.8.1667, [#804])
- **Missing-ident handling in `-ident-for`** — improved logic when an ident is missing; removes the prior warn-on-missing path. ([#800])
- **pydatahike: JSON Long coercion in transact** — JSON integers passed through transact are now coerced to Long correctly. ([#830])
- **pydatahike: tolerate cbor2 tag_hook signature variants** — works across cbor2 versions that vary the tag_hook callback signature. ([#829])

## 0.7

### Features

- **ClojureScript port + KabelWriter** — Datahike now runs in ClojureScript on Node.js and in the browser; introduces `KabelWriter` for distributed Datahike over WebSockets via kabel with Fressian serialization, plus a `TieredStore` (memory + IndexedDB) for browser persistence. Ships generated TypeScript definitions and an npm package. *Experimental.* (0.7.1615, [#748])
- **Malli migration of the API specification** — `datahike.api.types` defines malli function schemas for all 32 API operations, replacing `clojure.spec`. Adds `:categories`, `:stability`, `:accepts-stdin?`, and `:examples` metadata used by codegen for CLI, native, Python, TypeScript, and the HTTP server (now via `reitit.coercion.malli`). (0.7.1625, [#759])
- **Unified structured logging** — switches the whole library to `replikativ.logging` / trove for structured logs across CLJ, CLJS, native, and the Babashka pod. (0.7.1662, [#791])
- **ESM browser wrapper** — adds an `index.mjs` to the npm package generated via codegen; `package.json` `exports` resolves browser+import to ESM (browser+require still CJS), fixing Vite / Rollup bundler compatibility. (0.7.1661, [#792])
- **Online garbage collection** — opt-in `:online-gc` config runs incremental address deletion during commits with a configurable `:grace-period-ms` and `:max-batch` (sync or background). *Experimental.* (0.7.1643, [#775])
- **Autogenerated Babashka pod API** — the pod is now generated from the API specification via `datahike.codegen.pod`; supports variadic args (`datoms`) and per-op custom resolution. (0.7.1634, [#765])
- **Model-based generative test suite** — test.check-based modular suite with protocol-based invariant checking (sortedness + content) across EAVT / AVET / AEVT and historical consistency via as-of. ([#788])
- **Better IndexedDB error message** — `load-config` rejects `:indexeddb` directly with a clear message pointing at `TieredStore` (memory frontend + IndexedDB backend). ([#763])

### Notable fixes

- **AVET upsert: replace comparator inconsistency** — the AVET native ordering `(a,v,e)` is not a prefix of the replace comparator `(a,e)`, so `.replace` could find the wrong element or no element at all when values changed; switched to `disj`+`conj` for AVET. (0.7.1649, [#781])
- **Datomic compatibility: keyword in `:db/id` position** — transactions now accept a keyword in the `:db/id` slot. (0.7.1651, [#787])
- **Tuple value validation in search patterns** — retracts and merges on entities with 3+-element tuple attributes were rejected because `validate-pattern` only allowed 2-element vectors (assumed lookup-refs); tuple-typed attributes are now allowed any length. (0.7.1642, [#774])
- **Tuple schema transaction failing when `:db/ident` came last** — `attrTuples` used the threading macro on a numeric tempid; replaced with `get-in`. (0.7.1640, [#773])
- **`ClassCastException` when deleting branches** — `:branches` deserialized as a list from some stores (e.g. Postgres) crashed `disj`; the versioning ops now coerce to set defensively. (0.7.1645, [#778])
- **`database-exists?` now throws on invalid store config** — instead of silently returning `false`. ([#770])
- **TypeScript / JS API polish** — `pull` / `pullMany` arities, `keep-history?` key, namespace-truncation fix in JS API map-key conversion, npm browser build and externs for `:advanced` compilation, JS callback args converted to JS objects. ([#789] and follow-ups)
- **UUID handling in JS API** — UUID strings are no longer silently coerced; callers must use `d.uuid('...')` or `d.randomUuid()` explicitly. UUID values read back are returned as plain strings. *Breaking.*

## 0.6

### Features

- **Composite-tuple upsert** — `upsert-eid` now supports composite tuples and lookup refs/tempids in upsert position. (0.6.1611, [#740])
- **Transact unstructured data** — new `datahike.experimental.unstructured` namespace and `doc/unstructured.md` for transacting nested maps without a pre-declared schema. *Experimental.* (0.6.1596, [#730])
- **Multi-assoc support in versioning** — `merge-db` / branch ops gain multi-assoc support over konserve. ([#734])
- **GC promoted to public API** — `gc` is now part of the public `datahike.api`, including via the Java / libdatahike binding (with a `before_tx` argument). Schema and cache also broken out for cleaner reuse. (0.6.1592, [#716])
- **CLI: query inputs as EDN** — `datahike.cli` parses query input args as EDN; CLI logs to STDERR. (0.6.1590, [#714], [#702])
- **Composable history / as-of / since** — `history`, `as-of`, and `since` can now be nested in each other. (0.6.1569, [#683])
- **`as-of` / `since` time-point semantics** — `as-of` now always includes the time point; `since` now always excludes it. *Breaking.* (0.6.1589, [#713])
- **Promise impl on `CompletableFuture`** — internal promise implementation now uses `CompletableFuture`; `transact` and `transact!` unified. ([#700])
- **Disable consistency check via config** — new config setting to skip the connection consistency check; config-mismatch error message points at it. ([#693])
- **Cache size config entries ignored on reconnect** — no longer trigger a config-mismatch. ([#689])
- **Compare values of different types in queries** — fixes ordering of mixed-type values. (0.6.1567, [#685])
- **Pull patterns can be sets** — via updated `datalog-parser`. (0.6.1568, [#687])
- **Store `:db/id` as a keyword value** — supported in transactions. (0.6.1560, [#679])
- **`:db/ident` special-case removed** — `:db/ident` is no longer special-cased in lookup and transaction. (0.6.1588, [#711])
- **`datoms` works with system attribute components** — system attrs (eavt/aevt/avet) accept component args correctly. (0.6.1581, [#704])
- **Ident keywords in tuple add/retract syntax with `:attribute-refs?`** — tuple form now accepts the ident keyword for refs-mode dbs. (0.6.1578, [#698])
- **Query 20× faster via constant substitution** — replaces variables in patterns by known values, picking strategies that fold constants into the scan. (0.6.1556, [#636])
- **Writer latency improvements** — parallel writer operations, batching transactor, exposed buffer sizes with back-pressure warnings, synchronous flushing of pending writes. (0.6.1555, [#618])
- **Global address space and HTTP client/server** — connections are watchable, readable, and track `store-id`; deterministic commit-ids; readers for history dbs; HTTP client/server addressing across stores. (0.6.1550, [#639])
- **GraalVM native image builds** — `datahike` builds as a native binary; CLI tooling lives under `dthk`. (0.6.1546, [#640])
- **Babashka pod** — `datahike` is now usable from Babashka scripts via the bb pod. (0.6.1544, [#630])
- **Schema migration** — `datahike.norm` provides a schema-migration framework. *Experimental.* (0.6.1540, [#598])

### Notable fixes

- **History is distinct** — merges sorted sequences of distinct datoms; `history` no longer surfaces duplicates. ([#706])
- **Attribute-refs bug when transacting tuple value** — fix issue #695. ([#696])
- **`bind-by-fn` requires all attrs to have values** — fix issue #676. ([#677])
- **Correctly pull attributes** — fix issue #680. ([#681])
- **Reflection warnings** in `-ident-for`, `int-obj-to-long`, `alength`, `with-precision`, `abs`. ([#670], [#669], [#671])
- **`pset` comparator optimization**. ([#673])

## 0.5 and earlier

The pre-0.6 era (and a small gap between the last CHANGELOG-touching release and the 0.6 cutover at commit `215fd5e6`, March 2023) covers several notable items that weren't recorded in the historical changelog below:

- **GraalVM native-image build support** — first landing of `native-image` compatibility (matured into the 0.6 native binary builds). ([#337])
- **Versioning and GC for persistent-sorted-set backends** — initial `datahike.experimental.versioning` API plus a garbage collector that walks tracked DB snapshots. *Experimental.* ([#232])
- **Query middleware** — pluggable middleware around `q`. ([#566])
- **Query stats** — `:stats` output from `q` covering per-clause work. ([#601])
- **Specs for `datahike.api`** — Clojure specs for all public API operations (later replaced by malli in 0.7). ([#596])
- **All db-creation operations synchronous**. ([#591])

The historical changelog below is preserved verbatim from the pre-0.5 era when changes were curated by hand. It covers 0.4.0 down to 0.1.0, with a "next minor/major release" buffer of items between 0.4.0 and the start of the continuous-release model.

## next minor/major release

- Improve docs
- Add prep-step to deps
- Refactor test-namespaces
  - move tests to use datahike.api
  - move namespaces to `-test` format
  - use random db-ids during testing
  - move config from string to hash-map
  - move with-fn to api-ns
  - call empty-db from db-ns
- Switch to GitHub Flow and using main branch
- Switch to tools.build for building and deploying
- Persist max-eid
- Allow attribute access to historical db records
- Allow keyword keys for queries
- Fix tx-meta on transact through api-ns
- Improve code samples using transact with arg-map @podgorniy
- Insert into persistent sorted set does not replace existing datom with identical EAV
- Single datom retraction fixed for persistent set index
- Refactor index namespaces
- Make persistent set durable

## 0.4.0

- Add attribute references (#211)
- Fix avet upsert (#308)
- Extend benchmarks
- Add byte array support
- Add search cache
- Fix lookup search (#335)
- Fix comparators (#328)
- Add search cache (#294)
- Allow schema attribute updates (thanks to @MrEbbinghaus)
- Fix hitchhiker-tree handling (#358)
- Improve pagination performance (#294)
- Improve upsert performance 
- Fix history duplicates (#363)
- Fix cardinality many duplicates (#364)
- Fix attribute translations
- Add config for index creation
- Remove uniqueness constraint for :db/txInstant
- Fix scalar binding for function output
- Fix equivalent datom input (#932)
- Fix load-entities bugs (#398, #400)
- Fix LRU cache (#404)
- Clean up code examples (#409)
- Add q as built-in (#412)
- Add meta data (#407)
- Add int? as built-in (#435)

## 0.3.6

- Add a generic remote transactor interface (#281)
- Improve and add more benchmarks (#307)
- Improve query engine performance by optimising hash joins (#306)
- Use the latest version of the hitchhiker tree which fixes an issue with comparators (#258)

## 0.3.5

- Fix a dependency issue with release v0.3.4.

## 0.3.4

- Fix issue with upsert operations not always executed in the right order
- Fix an issue with transactions on import
- Add more tests
- Improve benchmarks

## 0.3.3

- Support for tuples (#104)
- Switch to Clojure CLI tools (#253)
- Adapt API namespace for Datomic compatibility (#196)
- Implement query with string (#196)
- Implement transact with lazy sequence (#196, #78, #151)
- Change upsert implementation to improve transaction performance (#62)
- Improve [cljdoc](https://cljdoc.org/d/io.replikativ/datahike/) (#88)
- Format source code according to [Clojure Style Guide](https://github.com/bbatsov/clojure-style-guide) (#198)
- Improve benchmark tooling
- Improve documentation on the pull-api namespace

The improved api namespace is now the entry point to using Datahike and should be the only namespace that needs to be imported in your projects. However it is still possible to use other namespaces but there will be changes that might break existing behaviour. Please take a look at the [improved cljdoc documentation](https://cljdoc.org/d/io.replikativ/datahike/) for the api namespace.

With the change in the upsert implementation (#62), we expect up to 3x speedup in terms of transaction time. However, it also brings a breaking change to the content of transaction reports. In previous Datahike versions, following an upsert operation (which updates an existing entry), you would see in the :tx-data section of the transaction report both the old retracted datom and its newly added version. E.g.:

```clojure
#datahike.db.TxReport{
...
:tx-data [#datahike/Datom[1 :name "Ivan" 536870914 false]
          #datahike/Datom[1 :name "Petr" 536870914 true]]
...}
```

With this release, you would only see the newly added entry and no information about retraction or addition is shown (it is assumed to be an addition).

```clojure
#datahike.db.TxReport{
...
:tx-data [#datahike/Datom [1 :name "Petr" 536870914]]
...}
```

Thanks to all the contributors and the community for helping on this release. Special thanks go to [clojurists together](https://www.clojuriststogether.org/) for funding large parts of this work.

## 0.3.2

- added entity specs (#197)
- fixed hash computation (#190)
- improved printer (#202)
- fixed history upsert (#219)
- added database name to environ
- added circle ci orbs for ci/cd across all libraries (#167)
- fixed reverse schema update (#199)
- added automatic releases
- added benchmark utility
- extended time variance test
- updated dependencies
- adjusted documentation

## 0.3.1

- support returning maps (#149, #186)
- support on-write schema for empty-db (#178)
- add hashmap for transact! (#173)
- cleanup old benchmarks (#181)
- cleanup leftover code (#172)
- fix index selection (#143)
- fix in-memory database existence check (#180)
- improve API docs
- update dependencies
- use java 1.8 for release build

## 0.3.0

- overhaul configuration while still supporting the old one
- support of environment variables for configuration 
- added better default configuration
- adjust time points in history functions to match Datomic's API
- add load-entities capabilities
- add cas support for nil 
- add support for non-date tx attributes 
- add Java API
- add Java interop in queries
- add basic pagination
- add noHistory support
- multiple bugfixes including downstream dependencies

## 0.2.1

- add numbers type
- re-introduce import/export functionality
- decouple backends from core
- integrate improved hitchhiker tree
- remove full eavt-index from db printing
- fix missing history entities

## 0.2.0

- integrate latest code from `datascript`
- move query parser to separate project: io.lambdaforge/datalog-parser
- add protocols for core indices: persistent set, hitchhiker tree now supported
- add protocols for backend stores: memory, file-based, LevelDB, PostgreSQL now
  supported (thanks to Alejandro Gómez)
- add schema-on-write capabilities
- add time variance capabilities
- add example project
- improve api documentation

## 0.1.3

- fixed null pointer exceptions in the compare relation of the hitchhiker-tree

## 0.1.2

- disk layout change, migration needed
- write root nodes of indices efficiently; reduces garbage by ~40 times and halves transaction times
- support export/import functionality

## 0.1.1

- preliminary support for datascript style schemas through create-database-with-schema
- support storage of BigDecimal and BigInteger values

## 0.1.0

- small, but stable JVM API
- caching for fast query performance in konserve
- reactive reflection warnings?
- schema support
- remove eavt-durable
- remove redundant slicing code
- generalize interface to indices
- integration factui/reactive?

[#232]: https://github.com/replikativ/datahike/pull/232
[#337]: https://github.com/replikativ/datahike/pull/337
[#566]: https://github.com/replikativ/datahike/pull/566
[#591]: https://github.com/replikativ/datahike/pull/591
[#596]: https://github.com/replikativ/datahike/pull/596
[#598]: https://github.com/replikativ/datahike/pull/598
[#601]: https://github.com/replikativ/datahike/pull/601
[#618]: https://github.com/replikativ/datahike/pull/618
[#630]: https://github.com/replikativ/datahike/pull/630
[#636]: https://github.com/replikativ/datahike/pull/636
[#639]: https://github.com/replikativ/datahike/pull/639
[#640]: https://github.com/replikativ/datahike/pull/640
[#669]: https://github.com/replikativ/datahike/pull/669
[#670]: https://github.com/replikativ/datahike/pull/670
[#671]: https://github.com/replikativ/datahike/pull/671
[#673]: https://github.com/replikativ/datahike/pull/673
[#677]: https://github.com/replikativ/datahike/pull/677
[#679]: https://github.com/replikativ/datahike/pull/679
[#681]: https://github.com/replikativ/datahike/pull/681
[#683]: https://github.com/replikativ/datahike/pull/683
[#685]: https://github.com/replikativ/datahike/pull/685
[#687]: https://github.com/replikativ/datahike/pull/687
[#689]: https://github.com/replikativ/datahike/pull/689
[#693]: https://github.com/replikativ/datahike/pull/693
[#696]: https://github.com/replikativ/datahike/pull/696
[#698]: https://github.com/replikativ/datahike/pull/698
[#700]: https://github.com/replikativ/datahike/pull/700
[#702]: https://github.com/replikativ/datahike/pull/702
[#704]: https://github.com/replikativ/datahike/pull/704
[#706]: https://github.com/replikativ/datahike/pull/706
[#711]: https://github.com/replikativ/datahike/pull/711
[#713]: https://github.com/replikativ/datahike/pull/713
[#714]: https://github.com/replikativ/datahike/pull/714
[#716]: https://github.com/replikativ/datahike/pull/716
[#730]: https://github.com/replikativ/datahike/pull/730
[#734]: https://github.com/replikativ/datahike/pull/734
[#740]: https://github.com/replikativ/datahike/pull/740
[#748]: https://github.com/replikativ/datahike/pull/748
[#759]: https://github.com/replikativ/datahike/pull/759
[#763]: https://github.com/replikativ/datahike/pull/763
[#765]: https://github.com/replikativ/datahike/pull/765
[#770]: https://github.com/replikativ/datahike/pull/770
[#773]: https://github.com/replikativ/datahike/pull/773
[#774]: https://github.com/replikativ/datahike/pull/774
[#775]: https://github.com/replikativ/datahike/pull/775
[#778]: https://github.com/replikativ/datahike/pull/778
[#781]: https://github.com/replikativ/datahike/pull/781
[#787]: https://github.com/replikativ/datahike/pull/787
[#788]: https://github.com/replikativ/datahike/pull/788
[#789]: https://github.com/replikativ/datahike/pull/789
[#791]: https://github.com/replikativ/datahike/pull/791
[#792]: https://github.com/replikativ/datahike/pull/792
[#795]: https://github.com/replikativ/datahike/pull/795
[#800]: https://github.com/replikativ/datahike/pull/800
[#803]: https://github.com/replikativ/datahike/pull/803
[#804]: https://github.com/replikativ/datahike/pull/804
[#805]: https://github.com/replikativ/datahike/pull/805
[#806]: https://github.com/replikativ/datahike/pull/806
[#807]: https://github.com/replikativ/datahike/pull/807
[#810]: https://github.com/replikativ/datahike/pull/810
[#813]: https://github.com/replikativ/datahike/pull/813
[#814]: https://github.com/replikativ/datahike/pull/814
[#815]: https://github.com/replikativ/datahike/pull/815
[#816]: https://github.com/replikativ/datahike/pull/816
[#818]: https://github.com/replikativ/datahike/pull/818
[#819]: https://github.com/replikativ/datahike/pull/819
[#821]: https://github.com/replikativ/datahike/pull/821
[#822]: https://github.com/replikativ/datahike/pull/822
[#823]: https://github.com/replikativ/datahike/pull/823
[#824]: https://github.com/replikativ/datahike/pull/824
[#825]: https://github.com/replikativ/datahike/pull/825
[#826]: https://github.com/replikativ/datahike/pull/826
[#827]: https://github.com/replikativ/datahike/pull/827
[#829]: https://github.com/replikativ/datahike/pull/829
[#830]: https://github.com/replikativ/datahike/pull/830
[#831]: https://github.com/replikativ/datahike/pull/831
[#832]: https://github.com/replikativ/datahike/pull/832
[#840]: https://github.com/replikativ/datahike/pull/840
[#844]: https://github.com/replikativ/datahike/pull/844
[#845]: https://github.com/replikativ/datahike/pull/845
[#852]: https://github.com/replikativ/datahike/pull/852
[#861]: https://github.com/replikativ/datahike/pull/861
[#859]: https://github.com/replikativ/datahike/pull/859
[#865]: https://github.com/replikativ/datahike/pull/865
[#867]: https://github.com/replikativ/datahike/pull/867
[#868]: https://github.com/replikativ/datahike/pull/868
[#869]: https://github.com/replikativ/datahike/pull/869
[#873]: https://github.com/replikativ/datahike/pull/873
[#874]: https://github.com/replikativ/datahike/pull/874
[#875]: https://github.com/replikativ/datahike/pull/875
[#879]: https://github.com/replikativ/datahike/pull/879
[#887]: https://github.com/replikativ/datahike/pull/887
[#885]: https://github.com/replikativ/datahike/pull/885
[#883]: https://github.com/replikativ/datahike/pull/883
[#882]: https://github.com/replikativ/datahike/pull/882
[#886]: https://github.com/replikativ/datahike/pull/886
[#881]: https://github.com/replikativ/datahike/pull/881
[#877]: https://github.com/replikativ/datahike/pull/877
