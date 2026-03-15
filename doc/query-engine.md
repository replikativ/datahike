# Compiled Query Engine

Datahike includes an opt-in compiled query engine that plans and executes Datalog queries using a fused scan+merge strategy over B-tree indices. For multi-clause entity joins, it can be significantly faster than the legacy engine.

**Status: Experimental** — The compiled engine produces identical results to the legacy engine for all supported query shapes. It is opt-in and the legacy engine remains the default.

## Enabling the Compiled Engine

Set the environment variable before starting your JVM:

```bash
DATAHIKE_COMPILED_QUERY=true clj -M:dev
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

## How It Works

The compiled engine processes queries in four phases:

1. **Analyze** — Classify where-clauses into patterns, predicates, functions, OR/NOT branches, and rule calls
2. **Estimate** — Estimate cardinality of each pattern using index statistics and sample-based predicate selectivity
3. **Plan** — Select optimal scan index, order merge operations by selectivity, group patterns by entity variable
4. **Execute** — Fused scan+merge: iterate the scan index once, performing lookupGE merges inline, projecting results directly into the output set

For queries with multiple clauses on the same entity variable (the common case), this avoids creating intermediate Relations and hash-joining them — the dominant cost in the legacy engine.

## Query Plan Visualization

Use `d/explain` to see the compiled plan for any query:

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
| Entity joins (`[?e :a ?v] [?e :b ?w]`) | Yes | Fused scan+merge |
| Value joins (`[?e :a ?x] [?e2 :b ?x]`) | Yes | Hash-probe between fused groups |
| `:in` bindings (scalar, tuple, collection) | Yes | |
| Predicates (`[(> ?a 50)]`) | Yes | Range predicates pushed into index slice |
| OR / OR-JOIN | Yes | Recursive sub-plans |
| NOT / NOT-JOIN | Yes | Anti-merge or subtraction |
| Non-recursive rules | Yes | Expanded to OR at plan time |
| Recursive rules | Yes | Semi-naive fixpoint with magic sets |
| Aggregates (`(avg ?x)`, `(count ?e)`) | Yes | Via unified fused-scan path |
| Pull expressions | Yes | |
| ORDER BY | Yes | Independent of engine choice |
| FindScalar / FindColl / FindTuple | Yes | |
| Temporal DBs (as-of, since, history) | Fallback to legacy | |

When the compiled engine encounters an unsupported query shape, it automatically falls back to the legacy engine — no error, no configuration needed.

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
