# Compiled Query Engine

Datahike includes an opt-in query planner that plans and executes Datalog queries using a fused scan+merge strategy over B-tree indices. For multi-clause entity joins, it can be significantly faster than the legacy engine.

**Status: Experimental** — The query planner produces identical results to the legacy engine for all supported query shapes. It is opt-in and the legacy engine remains the default.

## Enabling the Compiled Engine

Set the environment variable before starting your JVM:

```bash
DATAHIKE_QUERY_PLANNER=true clj -M:dev
```

Or bind the dynamic var at runtime:

```clojure
(require '[datahike.query :as dq])

;; Enable for a specific query
(binding [dq/*force-legacy* false]
  (d/q '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]] @conn))

;; Enable globally (for the current thread)
(alter-var-root #'dq/*force-legacy* (constantly false))
```

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
│   Cost-based operation ordering         │
│   Output: ordered ops with pipelines    │
└─────────────────────┬───────────────────┘
                      │ plan map {:ops [...] :pipeline ...}
                      ▼
┌─────────────────────────────────────────┐
│ execute.cljc: Execute plan              │
│   Fused scan+merge (no intermediates)   │
│   Hash-probe value joins (multi-group)  │
│   Post-filter preds, apply functions    │
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
- **Cost-based operation ordering** (`order-plan-ops`) — Greedy ordering: lowest-cost executable operation first, respecting data dependencies

Cardinality estimation (estimate.cljc) uses two techniques:

1. **Count-slice** (O(log n)) — Counts datoms in an index range without scanning. Used for pattern cardinality: `[?e :name ?v]` → count all `:name` datoms; `[42 :name ?v]` → 1 for card-one.
2. **Sample-based selectivity** — For predicates, samples 64 datoms from the relevant index slice, applies the predicate, and computes the pass rate. Falls back to operator-based heuristics (`=` → 0.1, `>` → 0.33, etc.) when sampling isn't possible.

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
| Nested temporal wrappers (`(history (as-of ...))`) | Fallback to legacy | |

When the query planner encounters an unsupported query shape, it automatically falls back to the legacy engine — no error, no configuration needed.

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
