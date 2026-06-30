# Compiled Query Engine

Datahike's query planner plans and executes Datalog queries using a fused scan+merge strategy over B-tree indices. For multi-clause entity joins it can be significantly faster than the relational (base) engine. **It is enabled by default.**

The planner runs for *eligible* queries (a single primary DB or temporal wrapper, non-stats); everything else — multi-source disjoint joins, nested temporal wrappers, stats — falls back to the relational engine. That engine is therefore a permanent fallback, not "legacy". The planner produces identical results to it for all supported query shapes.

## Disabling the planner

The planner is on by default. To run every query through the relational engine, set the environment variable before starting your JVM:

```bash
DATAHIKE_QUERY_PLANNER=false clj -M:dev
```

Or bind the dynamic var at runtime:

```clojure
(require '[datahike.query :as dq])

;; Disable for a specific query
(binding [dq/*disable-planner* true]
  (d/q '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]] @conn))

;; Disable globally (for the current thread)
(alter-var-root #'dq/*disable-planner* (constantly true))
```

## Performance

The compiled planner makes Datahike competitive with — and on most read-heavy
shapes faster than — embedded Datalog/temporal databases. The tables below are from
the in-repo cross-database suite (`benchmark.datascript-bench`) on a 20,000-entity
dataset, all times **wall-clock milliseconds, lower is better**, Datahike running the
compiled planner (the default).

> **Methodology / caveats.** Single machine, single run, query-result cache disabled,
> JIT pre-warmed; Datalevin 0.10.18, Datomic Peer 1.0.7387. Results are bit-for-bit
> identical across engines (verified by row count on every shape). These are
> *directional, reproducible* numbers from the bundled suite, not a controlled
> multi-run study — reproduce them yourself with the commands at the end of this
> section.

### Queries (vs Datalevin and Datomic)

Datahike wins 9 of 15 shapes; Datalevin edges it on simple multi-clause selections
and the non-recursive rule — cases where both are already fast (well under a few ms).

| Query | Datahike | Datalevin | Datomic |
|---|---:|---:|---:|
| simple lookup `[?e :name "Ivan"]` | **0.13** | 0.69 | 5.2 |
| two-clause join (name + age) | 0.62 | **0.56** | 8.8 |
| three clauses (reversed order) | 0.63 | **0.31** | 12.7 |
| value join (shared `?age`) | **7.9** | 231 | 217 |
| predicate `salary > ?min` | **1.5** | 2.5 | 19.6 |
| NOT negation | **3.8** | 148 | 49.6 |
| OR-join (name or name + age) | **5.5** | 228 | 70.7 |
| NOT-join | **6.0** | 146 | 47.8 |
| 5-clause entity merge | **2.4** | 2.9 | 138 |
| non-recursive rule | 3.6 | **2.5** | 8.3 |

### Cross-entity joins (vs Datalevin and Datomic)

Datahike wins every join shape, often decisively.

| Join | Datahike | Datalevin | Datomic |
|---|---:|---:|---:|
| ref join, 1 dept → people | **0.18** | 4.4 | 3.3 |
| ref join, 10 depts → people | **1.2** | 8.9 | 6.0 |
| ref join + predicate | **1.8** | 15.8 | 9.5 |
| 3-hop chain (person → dept → division) | **30.5** | 73.2 | 94.5 |
| selective (salary > 90k → dept) | **4.3** | 20.5 | 22.0 |

### Recursive rules (vs Datalevin and Datomic)

Datahike wins long chains; Datalevin wins the very-wide fan-out trees.

| Rule | Datahike | Datalevin | Datomic |
|---|---:|---:|---:|
| wide tree 3×3 | **0.23** | 1.2 | 5.3 |
| wide tree 7×3 | 23.4 | **13.1** | 163.7 |
| chain 10×3 | **0.34** | 1.5 | 5.7 |
| chain 30×5 | **3.3** | 5.7 | 64.2 |

### Temporal: as-of / history (vs Datomic)

Datahike wins all 9 temporal shapes — including the multi-clause history join, after
the temporal-merge cursor fast path (it cursor-drives the entity-group merge in one
pass instead of one index seek per entity).

| Temporal query | Datahike | Datomic |
|---|---:|---:|
| current: name = Ivan | **0.13** | 5.0 |
| current: name + age | **0.54** | 8.8 |
| as-of: name + age | **4.9** | 9.8 |
| as-of: name + age + sex | **6.4** | 13.5 |
| history: all names | **4.8** | 6.6 |
| history: age + tx | **7.3** | 13.4 |
| history: name + age join | **16.2** | 40.2 |
| history: retracted ages | **1.5** | 5.0 |

### Aggregates

PSS-based aggregates (the default) are mid-pack; with the **stratum** columnar
secondary index they are the fastest measured (10–50× the alternatives) because the
planner pushes the aggregate to native columnar storage.

| Aggregate | Datahike (PSS) | Datahike + stratum | Datalevin | Datomic |
|---|---:|---:|---:|---:|
| avg salary | 2.0 | **0.27** | 11.7 | 22.8 |
| avg + count by sex | 32.8 | **0.54** | 9.5 | 57.6 |
| avg / min / max (filtered) | 3.2 | **0.98** | 5.6 | 48.2 |
| avg salary > 50k by sex | 29.7 | **0.43** | 4.4 | 35.2 |
| avg salary by sex × name | 36.7 | **0.45** | 15.2 | 101.5 |
| variance + stddev + median | 8.1 | **4.9** | 12.0 | 55.7 |

(See [secondary-indices.md](secondary-indices.md) for the stratum index.)

### Writes

The planner is a read-path optimization and does not change write throughput. On bulk
insert (20k entities) Datahike is roughly on par with Datalevin and behind Datomic's
in-memory peer — the write path is not the focus of this suite, so those numbers are
not tracked here.

### Reproducing these numbers

```bash
# Full cross-database suite (Datahike vs Datalevin vs Datomic)
clj -M:bench-compare -m benchmark.datascript-bench all
# Or a single category: queries | writes | rules | aggregates | temporal | joins
clj -M:bench-compare -m benchmark.datascript-bench temporal

# Planner-vs-base-engine regression gate (CI): asserts the planner is never
# more than 2x slower than the relational engine on historically-tricky shapes.
clj -M:bench-compare -m benchmark.planner-regression --assert
```

### Reporting slow queries

**If a query is unexpectedly slow, please [open an issue](https://github.com/replikativ/datahike/issues).**

We hold ourselves to a simple standard: on the same data, a supported query should not
be meaningfully slower in Datahike than in any comparable database — and never slower
than Datahike's own relational fallback. Performance isn't Datahike's headline feature
(durable immutable history, time travel, flexible storage backends and distribution are),
but we take it seriously and treat a significant slowdown against *any* baseline as a bug
to fix, not a limitation to document. If you can show another database — Datalog, SQL,
document, anything — answering the same query faster on the same dataset, that is exactly
the kind of issue we want to see.

Two cases especially worth a report:

- a query **markedly slower with the planner on than off** — compare directly:
  ```clojure
  (require '[datahike.query :as dq])
  (time (d/q my-query @conn))                                       ;; planner (default)
  (time (binding [dq/*disable-planner* true] (d/q my-query @conn))) ;; relational fallback
  ```
  If the fallback is much faster, that is a planner regression.
- a query whose latency is far out of line with a comparable shape in the tables above,
  or with another database on the same data.

Include `(d/explain my-query @conn)` (the plan — see below), the schema for the attributes
involved, rough dataset size, and your timings. A reproducible case with a synthetic
dataset is ideal but not required.

## Architecture

The query planner is a multi-phase compiler that transforms Datalog queries into fused B-tree scan plans:

```
Query Text
    │
    ▼
┌─────────────────────────────────────────┐
│ analyze.cljc: Classify where-clauses    │
│   patterns, predicates, functions,      │
│   OR/NOT branches, rule calls           │
└─────────────────────┬───────────────────┘
                      │ classified clause-info records
                      ▼
┌─────────────────────────────────────────┐
│ logical.cljc: Build logical IR          │
│   Group by entity-var → LEntityJoin     │
│   Fold NOT → anti-scans                 │
│   OR → LUnion, rules → LFixpoint        │
│   Output: unordered bag of IR nodes     │
└─────────────────────┬───────────────────┘
                      │ LogicalPlan
                      ▼
┌─────────────────────────────────────────┐
│ lower.cljc: Lower to physical plan      │
│   Pushdown predicates → index bounds    │
│   Select indices (EAVT/AEVT/AVET)       │
│   DP merge ordering within groups       │
│   Pipeline annotation (fused path)      │
│   Inter-group join detection            │
│   Cost ordering (one cost model)        │
│   Output: ordered ops with pipelines    │
└─────────────────────┬───────────────────┘
                      │ plan map {:ops [...] :pipeline ...}
                      ▼
┌─────────────────────────────────────────┐
│ execute.cljc: Execute plan              │
│   Fused scan+merge (no intermediates)   │
│   Hash-probe value joins (multi-group)  │
│   Adaptive fn placement, post-filter    │
│   Project to find-vars                  │
│   Semi-naive fixpoint (recursive rules) │
└─────────────────────────────────────────┘
```

### Planning Primitives (plan.cljc, estimate.cljc)

The core optimization logic lives in plan.cljc as reusable primitives, called by the lowering pass:

- **Index selection** (`plan-pattern-op`) — Chooses EAVT, AEVT, or AVET per pattern based on which components are ground
- **DP merge ordering** (`dp-order-fuse-ops`) — For N patterns on the same entity, finds the optimal (scan, merge₁, merge₂, ...) ordering using a short-circuit AND cost model: `merge-cost / (1 - pass-rate)`
- **Entity group assembly** (`assemble-entity-group`) — Combines DP ordering, anti-merge selectivity sorting, cardinality estimation, and pipeline annotation into a single entity-group op
- **Inter-group join detection** (`detect-inter-group-joins`) — Finds shared variables between entity groups for hash-probe value joins
- **Inter-group ordering** (`dp-order-groups`) — Bitmask DP over entity-groups/pattern-scans, minimizing total intermediate join cardinality (Σ rows). Falls back to greedy (GOO) above `dp-group-threshold` (16) groups.
- **Cost-based operation ordering** (`order-plan-ops`) — Runs the inter-group DP, then greedily interleaves the non-group ops (functions, predicates, OR/NOT, rules) by **one cost model** — `cost = rows × per-unit` — subject to the hard readiness constraint (a consumer can't precede the producer of its inputs).

Cardinality estimation (estimate.cljc) uses two techniques:

1. **Count-slice** (O(log n)) — Counts datoms in an index range without scanning. Used for pattern cardinality: `[?e :name ?v]` → count all `:name` datoms; `[42 :name ?v]` → 1 for card-one.
2. **Sample-based selectivity** — For predicates, samples 64 datoms from the relevant index slice, applies the predicate, and computes the pass rate. Falls back to operator-based heuristics (`=` → 0.1, `>` → 0.33, etc.) when sampling isn't possible.

These feed a single `{var → cardinality}` map that the orderer threads through the join graph. The map is seeded from **three sources** (`source-cards` in plan.cljc), all reduced to the same shape:

- **Pattern** — attribute stats (above).
- **Function bind** — a function's `:datahike/output-cardinality` (see *Function cost model* below), so a downstream join on the bind's output is bounded by it.
- **`:in` input** — the binding's *shape*: a scalar/tuple seeds card 1, a collection/relation `[?x ...]` / `[[?a ?b]]` seeds a real multi-row default rather than 1.

### Function cost model (plan.cljc)

A function bind `[(f ?g $ ?x) [?n ...]]` can carry metadata the planner reads via `(meta (resolve fn-sym))`. This is the general mechanism graph algorithms use, and **any user function can opt in** by carrying the same keys:

- **`:datahike/output-cardinality`** — how many rows the bind produces (a number, or `(fn [ctx])`). Feeds the `{var → card}` map as the function `source-card`, so a downstream join on the output is bounded (e.g. a reachability result bounds the join by node count, not the whole attribute).
- **`:datahike/cost`** — per-invocation cost (a number, or `(fn [ctx])`). With it, `op-cost = invocations × per-call`; without it a function costs 1 (ordered eagerly).

Both can be *computed from the inputs*: a graph passed as `?g` is resolved back to its underlying edge attribute through **provenance** (`?g → (attr-graph :follows) → :follows`, descending `undirected`/`reverse`/`filtered`/`subgraph` wrappers), so a cost fn can read the graph's `[V E]`. The graph algorithms use this for complexity tiers (linear `V+E`, log-linear `E·log V`, quadratic `V·E`, iterative `k·(V+E)`), memoized per `(attr, db-version)`. When the graph isn't statically resolvable (weighted/multi/query graphs), both fall back to safe defaults.

There is **one cost** for every op: `cost = rows × per-unit` (scans: `rows × 1`; functions: `invocations × per-call`; predicates: 0). The three quantities for a function are distinct: *output cardinality* (a property of the function, feeds downstream), *per-call cost* (a property of the function), and *invocation count* (NOT a property of the function — it's the relation cardinality at the function's position, propagated through the joins). What the static cost cannot know — whether a join will *expand* or *reduce* a function's input — is corrected at execution time (see *Adaptive function placement*).

### Logical IR (ir.cljc, logical.cljc)

The logical IR is an algebraic representation of what to compute, with no physical decisions. Each where-clause maps to a typed record:

| IR Node | Source | Description |
|---------|--------|-------------|
| `LScan` | `[?e :attr ?v]` | Single data pattern with clause, free vars, optional source |
| `LEntityJoin` | Multiple patterns on same `?e` | Groups scans + anti-scans on the same entity variable |
| `LFilter` | `[(pred ?x ?y)]` | Predicate with function symbol and args |
| `LBind` | `[(fn ?x) ?y]` | Function binding producing output variable(s) |
| `LUnion` | `(or ...)` / `(or-join ...)` | OR branches, each a sub-plan |
| `LAntiJoin` | `(not ...)` / `(not-join ...)` | Negation with optional explicit join-vars |
| `LFixpoint` | Recursive rule call | Base cases + delta-clause versions for semi-naive evaluation |
| `LPassthrough` | Unsupported clause | Fallback to legacy engine |

`build-logical-plan` in logical.cljc:
1. Classifies each where-clause into clause-info records (via analyze.cljc)
2. Groups `LScan` nodes by entity variable into `LEntityJoin` records
3. Folds simple NOT clauses into entity groups as anti-scans (when the NOT has a single data pattern whose entity-var already has a scan group)
4. Remaining NOTs become standalone `LAntiJoin` nodes
5. Returns a `LogicalPlan` — an **unordered** bag of IR nodes (ordering is a physical decision)

### Lowering (lower.cljc)

The `lower` function converts a `LogicalPlan` into a physical plan map by calling the planning primitives:

1. **Pushdown detection** — Predicates like `[(> ?age 50)]` are pushed down to narrow the index slice bounds, eliminating rows before they enter the merge pipeline
2. **Per-node lowering** — Each IR node is converted to physical ops via the appropriate plan.cljc primitive
3. **Merge-lost predicate recovery** — Pushdown predicates that land on merge-ops (where they can't be applied) are restored as standalone filter ops
4. **Group-attached predicates** — Predicates whose free-var args are a subset of exactly one entity group's vars are attached to that group (`:attached-preds`). These are evaluated during or immediately after that group's execution, avoiding the global wide-tuple post-filter penalty. Predicates with variable fn-syms, source symbols, or non-scalar bindings are excluded (via `post-op-direct-eligible?`).
5. **Pipeline annotation** — Pre-computes the fused execution path as a `PPipeline` record, eliminating runtime dispatch

The lowering pass exploits the IR's explicit grouping structure: the logical IR groups patterns by entity-var (`LEntityJoin`) and keeps predicates as separate `LFilter` nodes, so lowering can analyze variable dependencies and attach predicates to their owning group — a decision the flat classified-clause list couldn't support.

### Pipeline DSL (ir.cljc)

Each entity group's `PPipeline` is a sequence of typed step records that describe exactly how to execute the fused scan:

```
PIndexScan(:eavt, ground-attr?)
  → PGroundFilter(check-e?, check-v?, pushdown-bounds)
  → PProbeFilter(field=:entity or :value)          [multi-group only]
  → PSortedMerge / PPerCursorMerge / PCardManyMerge
  → PEmitTuple(n-find-vars)
```

Fused execution paths:
- **`:scan-only`** — No merges, single pattern
- **`:sorted-merge`** — All merges card-one, uses B-tree cursors for O(log n) lookupGE
- **`:per-cursor-merge`** — Mixed cardinalities, per-cursor merge strategy
- **`:card-many-merge`** — Hash-based merge for many-cardinality attributes

### IR-Level Optimizations

The logical IR enables optimization passes that reason over the full query structure to make globally better decisions than the flat classified-clause list could support.

**Implemented:**

1. **Group-attached predicates** — Predicates whose input vars belong to a single entity group are attached to that group at lowering time and evaluated during group execution, not as global post-filters. This eliminates the wide-tuple penalty for single-group queries with predicates and reduces probe-set size in multi-group queries by filtering producers before the join.

2. **Multi-group find-var propagation** — The producer builds a `HashMap<probe-value → List<tuple>>` (probe-map) instead of a scalar `HashSet`. The consumer looks up producer values during tuple combination, enabling find-vars from any group. Queries like `[:find ?n1 ?n2 :where [?e :name ?n1] [?e :friend ?f] [?f :name ?n2]]` now use the fused scan path instead of falling back to the Relation engine.

**Planned:**

1. **Cross-group predicate placement** — Predicates on value-join variables (e.g., `[(> ?salary 50000)]` where `?salary` joins two entity groups) can be pushed into the producer group at IR level, reducing the probe-set size before the consumer group ever executes. Currently these are applied as post-filters on the final result list.

2. **3+ group probe-chain planning** — The current probe-map propagation supports 2-group plans. For 3+ groups, an IR-level planner could thread values through intermediate probe-maps, choosing producer/consumer roles by cost.

## How Execution Works

### Fused Scan+Merge (execute.cljc)

For the common case of multiple clauses on the same entity (`[?e :name ?n] [?e :age ?a]`), the engine:

1. **Scans** the lowest-cardinality attribute in the chosen index (e.g., AEVT on `:name`)
2. For each entity found, **merges** via `seekGE` lookups in other attributes' index ranges — no intermediate Relations, no hash-joining
3. **Projects** directly into the output set or QueryResult array

This avoids the dominant cost of the legacy engine: creating and hash-joining intermediate Relation tuples.

### Multi-Group Value Joins

When a query has patterns on different entity variables joined by a shared value (`[?e :friend ?f] [?f :name ?n]`), the engine:

1. Executes the **producer** group (lower cardinality) first
2. If the producer has find-vars not available from the consumer, builds a **probe-map** (`HashMap<join-var → List<producer-tuples>>`) that carries producer values forward. Otherwise, builds a scalar **probe-set** (`HashSet` of join-var values).
3. Executes the **consumer** group, filtering during scan to only entities whose join-var value appears in the probe-set
4. When a probe-map is present, **combines** consumer tuples with producer values: for each consumer tuple, looks up the probe-var in the map and merges producer find-var values into the output tuple. Handles card-many (one probe-value → multiple producer tuples) by emitting multiple combined tuples.

### Post-Processing Pipeline

Group-attached predicates are evaluated during or immediately after their owning group's execution, avoiding the global post-filter path. When a plan has *remaining* predicates or functions (cross-group predicates, variable fn-syms, or functions), the engine emits wide tuples (all group vars), then:

1. **Post-filter** — Applies remaining predicates in-place, compacting the result list
2. **Post-apply** — Evaluates functions, extending tuples with output vars (or filtering on already-bound vars)
3. **Project** — Narrows wide tuples to find-vars only

### Adaptive function placement (probe-and-sink)

Whether a join *expands* or *reduces* a function's input is a matter of data correlation/skew — invisible to per-attribute statistics, so no static cost model can place an expensive function correctly relative to such a join in general. The executor measures it instead, but only for functions that declare `:datahike/cost` (so ordinary queries pay nothing):

- **Sink** (at a function op) — if a row-*reducing* join on the function's input is still ahead in the plan, run it first, so the function then runs on fewer rows.
- **Hoist** (at a group op) — if the group would *expand* a pending downstream function's input, run the function first, so it isn't applied to the blown-up relation.

The decision uses a single index probe to get the *exact* post-join cardinality, then reorders the remaining ops (a one-shot `:probed?` flag prevents re-probing). Together they place each expensive function on its minimal input — after reducing joins, before expanding ones. Worked examples are the cost oracles in `test/datahike/test/experimental/graph_cost_oracle.clj` (FILTER/EXPAND/JOINFN/SINK/MUTUAL), which assert the optimal invocation counts.

### Recursive Rules (Semi-Naive Fixpoint)

Recursive rules use a delta-clause strategy:

1. Compute base cases (non-recursive branches)
2. Loop: evaluate recursive branches using only **new** tuples from the previous iteration (delta)
3. Stop when delta is empty (fixpoint reached)

SCC detection (Tarjan's algorithm) identifies mutually recursive rule groups. Magic-set optimization restricts base-case scans when the caller provides bound variables.

## Query Plan Visualization

Use `d/explain` to see the query plan for any query:

```clojure
(require '[datahike.api :as d])

(println (d/explain '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]] @conn))
;; === Query Plan ===
;; find: [?n ?a]
;; bound: #{}
;; engine: compiled
;; ---
;; ENTITY-GROUP on ?e (est. 1 rows)
;;   scan: [?e :name ?n] [aevt] card-one est=3
;;   merge[0]: [?e :age ?a] [card-one] est=3
```

`d/explain` works with recursive rules too:

```clojure
(println (d/explain '[:find ?n
                      :in $ ?start %
                      :where (follows ?start ?e2) [?e2 :name ?n]]
                    @conn 1
                    '[[(follows ?e1 ?e2) [?e1 :follows ?e2]]
                      [(follows ?e1 ?e2) [?e1 :follows ?t] (follows ?t ?e2)]]))
;; === Query Plan ===
;; find: [?n]
;; rules: [follows]
;; consts: {?start 1}
;; engine: compiled
;; ---
;; SCAN aevt [?e2 :name ?n] [card-one] (est. 3 rows)
;; RECURSIVE-RULE follows ...
;;   base-plans: 1
;;     [0]: SCAN aevt [?e1 :follows ?e2] [card-one] (est. 2 rows)
;;   clause-versions: 1
;;     [0]: SCAN aevt [?e1 :follows ?t] ...
;;          RULE-LOOKUP follows [?t ?e2] mode=delta
```

## Supported Query Features

| Feature | Compiled Engine | Notes |
|---------|----------------|-------|
| Entity joins (`[?e :a ?v] [?e :b ?w]`) | Yes | Fused scan+merge via DP-ordered B-tree lookupGE |
| Value joins (`[?e :a ?x] [?e2 :b ?x]`) | Yes | Hash-probe between fused groups |
| `:in` bindings (scalar, tuple, collection) | Yes | |
| Predicates (`[(> ?a 50)]`) | Yes | Range predicates pushed into index slice |
| Functions (`[(str ?n " " ?a) ?s]`) | Yes | Post-applied on wide tuples |
| OR / OR-JOIN | Yes | Recursive sub-plans |
| NOT / NOT-JOIN | Yes | Anti-merge folding or subtraction |
| Non-recursive rules | Yes | Expanded to OR at plan time |
| Recursive rules | Yes | Semi-naive fixpoint with SCC detection and magic sets |
| Aggregates (`(avg ?x)`, `(count ?e)`) | Yes | Via unified fused-scan path |
| Pull expressions | Yes | |
| ORDER BY | Yes | Independent of engine choice |
| FindScalar / FindColl / FindTuple | Yes | |
| Temporal DBs (`as-of`, `since`, `history`) | Yes | Numeric and Date time-points |
| Nested temporal wrappers (`(history (as-of ...))`) | Fallback to base engine | |

When the query planner encounters an unsupported query shape, it automatically falls back to the relational (base) engine — no error, no configuration needed.

## ORDER BY

Sort query results with optional offset and limit:

```clojure
;; Single key ascending
(d/q {:query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
      :args [@conn]
      :order-by '?a})
;; => [["Bob" 25] ["Alice" 30] ["Charlie" 35]]

;; Multi-key: age descending, then name ascending
(d/q {:query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
      :args [@conn]
      :order-by '[?a :desc ?n :asc]})

;; With offset and limit (pagination)
(d/q {:query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
      :args [@conn]
      :order-by '[?a :asc]
      :offset 1
      :limit 2})
;; => [["Alice" 30] ["Charlie" 35]]

;; Column index instead of variable name
(d/q {:query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
      :args [@conn]
      :order-by [1 :desc]})  ;; sort by second find-var (age) descending
```

Queries with `:order-by` return a vector (ordered). Without `:order-by`, queries return a set.

## Query Result Cache

Query results are cached automatically across transactions. When a transaction modifies attributes `:name` and `:age`, only cached queries that reference those attributes are invalidated — queries on unrelated attributes survive.

### Configuration

```clojure
(require '[datahike.query :as dq])

;; Set cache size via environment variable (before JVM start)
;; DATAHIKE_QUERY_CACHE_SIZE=128

;; Or at runtime (clears existing cache)
(dq/set-query-cache-size! 128)

;; Clear all cached results
(dq/clear-query-cache!)

;; Disable caching (e.g. for benchmarking)
(binding [dq/*query-result-cache?* false]
  (d/q '[:find ?e :where [?e :name "Alice"]] @conn))

;; Inspect the cache
@dq/query-result-cache
```

The cache is:
- **Global** — shared across all connections in the JVM, portable across CLJ and CLJS
- **LRU-bounded** — retains up to `*query-cache-size*` DB snapshots (default 64)
- **Propagated** — surviving entries are carried from parent to child DB on each transaction via structural sharing
- **Attribute-selective** — only invalidates entries whose attribute dependencies overlap with the transaction's modified attributes

### Cache and `d/with`

Speculative queries via `d/with` also benefit from cache propagation:

```clojure
(let [report (d/with @conn [[:db/add 1 :age 31]])
      speculative-db (:db-after report)]
  ;; Cached queries on @conn that don't touch :age are available here
  (d/q '[:find ?n :where [?e :name ?n]] speculative-db))
```

## Cross-Platform Support (CLJ + CLJS)

The compiled engine works on both JVM Clojure and ClojureScript (Node.js / browser). The execution layer uses cross-platform abstractions for:

- **Result lists** — `ArrayList` on JVM, JS arrays on CLJS
- **Tuple arrays** — `object-array` / `aget` / `aset` on both platforms (with `^objects` type hints on JVM for correct reflection-free access)
- **Predicate resolution** — `requiring-resolve` on JVM, atom-based registration on CLJS (breaks circular dependency between query.cljc and execute.cljc)
- **Index comparators** — The persistent-sorted-set library accepts `:comparator` on both platforms for custom datom ordering

## Source Files

| File | Role |
|------|------|
| `src/datahike/query.cljc` | Entry point: parse, plan, execute, cache |
| `src/datahike/query/analyze.cljc` | Clause classification, pushdown detection |
| `src/datahike/query/ir.cljc` | IR record definitions (logical + pipeline DSL) |
| `src/datahike/query/logical.cljc` | Build logical IR from classified clauses |
| `src/datahike/query/lower.cljc` | Lower logical IR to physical plan |
| `src/datahike/query/plan.cljc` | Planning primitives: DP ordering, pipeline, cost model |
| `src/datahike/query/estimate.cljc` | Cardinality estimation (count-slice, sampling) |
| `src/datahike/query/execute.cljc` | Fused scan+merge execution, post-processing |
| `src/datahike/query/relation.cljc` | Legacy Relation data structure (used by fallback) |
