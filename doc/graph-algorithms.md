# Graph Algorithms

> **Experimental.** The `datahike.experimental.graph` and
> `datahike.experimental.graph-spec` namespaces are evolving and their APIs may
> change between releases.

Datahike ships a collection of graph algorithms that run over data already
stored in a database — reachability, shortest paths, centrality, community
detection, spanning trees, flows, random walks and link prediction. They work
on both Clojure (JVM) and ClojureScript.

```clojure
(require '[datahike.api :as d]
         '[datahike.experimental.graph-spec :as gs]
         '[datahike.experimental.graph :as graph])
```

## How it works: a graph is a `GraphSpec`

The algorithms do not read the database directly. Instead they go through a
**`GraphSpec`** — a small protocol that says how to read adjacency, edges, nodes
and weights out of a db. Every algorithm has the shape:

```clojure
(algorithm graph-spec db & args)
```

You build a spec, optionally transform it, and pass it in. This decouples the
algorithms from how your edges happen to be modelled.

### Constructors

| Constructor | Edge model |
|---|---|
| `(gs/attr-graph :follows)` | a ref attribute: each datom `[e :follows v]` is an edge `e → v` |
| `(gs/multi-attr-graph [:follows :friends])` | the union of several ref attributes |
| `(gs/weighted-graph :edge/from :edge/to :edge/weight)` | **reified edges**: an edge is an entity with from/to refs and a weight |
| `(gs/query-graph '[:find ?a ?b :where ...])` | edges are the `[source target]` rows of a query |

Weighted graphs use *reified edges* because that is the only sound way to attach
a weight to a particular edge (a weight on a node would be shared by all of that
node's out-edges). A reified edge looks like:

```clojure
{:edge/from alice :edge/to bob :edge/weight 3.0}
```

### Transformers

Wrap a spec to change how it is traversed:

```clojure
(gs/reverse-graph g)          ; follow edges backwards
(gs/undirected-graph g)       ; treat edges as undirected (both directions)
(gs/filtered-graph g pred)    ; keep only nodes where (pred db node)
(gs/subgraph g #{1 2 3})      ; restrict to a node set
```

Direction is therefore a property of the spec: for "incoming" neighbours pass
`(gs/reverse-graph g)`, for "both" pass `(gs/undirected-graph g)`.

### Access methods: index vs. materialize

There are two ways a spec reads the graph, with different performance:

- **Index-backed** (`attr-graph`): each neighbour lookup is an `O(log n + degree)`
  index probe (forward off EAVT, reverse off AVET when the attribute is
  indexed — refs, `:db/index`, `:db/unique` all are). No up-front cost. Best for
  **local / bounded** traversals (k-hop, near-pair shortest path, ego nets).
- **Materialized** (`gs/materialize`): snapshot the whole graph into in-memory
  adjacency + weight maps once (`O(E)`), then every lookup is `O(1)`. Best for
  **whole-graph** algorithms that visit most nodes repeatedly.

Whole-graph algorithms (page-rank, components, centrality, louvain, …)
materialize internally. If you call several algorithms on the same graph,
materialize once and reuse it:

```clojure
(let [g (gs/materialize (gs/attr-graph :follows) db)]
  [(graph/page-rank g db)
   (graph/connected-components g db)])
```

## Algorithms

In the examples, `g` is a graph spec and `db` a database value; `a`, `b`, etc.
are entity ids.

### Reachability and paths

```clojure
(graph/transitive-closure g db a)   ; #{nodes reachable from a via >=1 hop}
(graph/reachable? g db a b)         ; is there a path a -> ... -> b ?
(graph/all-paths g db a {:target b :max-depth 6})  ; seq of node vectors
(graph/shortest-path g db a b)      ; [a ... b] with fewest edges, or nil
(graph/path-length g db a b)        ; number of edges, or nil
(graph/semi-naive-transitive-closure g db)  ; #{[source target] ...}
```

`transitive-closure` and `reachable?` follow at least one edge, so `a` is in its
own closure exactly when it lies on a cycle.

### Connected components

```clojure
(graph/connected-component g db a)  ; #{nodes} in a's undirected component
(graph/connected-components g db)   ; seq of component sets
(graph/same-component? g db a b)
(graph/strongly-connected-components g db)  ; vec of SCC sets (Tarjan, iterative)
```

### Ordering and cycles

```clojure
(graph/topological-sort g db)   ; vec in topological order, or nil if cyclic
(graph/has-cycle? g db)
(graph/find-cycle g db)         ; one cycle as a node vector, or nil
(graph/bipartite-coloring g db) ; {:bipartite? true :coloring {n 0|1}} | {:bipartite? false}
(graph/find-bridges g db)       ; #{[a b] ...} cut edges (undirected)
```

### Weighted paths

Edge weights come from the spec's `edge-weight` (1 for unweighted graphs), so use
a `weighted-graph`:

```clojure
(def wg (gs/weighted-graph :edge/from :edge/to :edge/weight))

(graph/weighted-shortest-path wg db a b)   ; {:path [...] :cost c}   (Dijkstra)
(graph/bottleneck-path wg db a b)          ; {:path [...] :capacity c} (widest path)
(graph/astar-path wg db a b heuristic-fn)  ; {:path [...] :cost c}   (A*)
```

`heuristic-fn` takes `[node target]` and estimates the remaining cost; use a
constant `0` heuristic to get plain Dijkstra. Weights must be non-negative.

### Centrality

```clojure
(graph/page-rank g db)              ; {node score}, scores sum to ~1
(graph/page-rank g db :damping 0.85 :iterations 20 :tolerance 1e-6)
(graph/closeness-centrality g db)   ; {node score in [0,1]} (undirected)
(graph/betweenness-centrality g db) ; {node score in [0,1]} (Brandes, undirected)
```

PageRank redistributes the mass of dangling (sink) nodes each iteration, so rank
is conserved even when the graph has sinks. Betweenness is normalized to `[0,1]`.

### Community detection

```clojure
(graph/louvain g db)            ; {:communities {node id} :modularity Q :levels n :stats ...}
(graph/louvain g db :resolution 1.0)   ; higher resolution => more, smaller communities
(graph/label-propagation g db) ; {:communities {node label} :converged ? ...}
(graph/community-stats {node->community})  ; sizes / counts / isolation score
```

Louvain is multi-level modularity maximization (local moving + aggregation).

### Spanning tree, flow

```clojure
(graph/prim-mst wg db)              ; {:edges [[a b w] ...] :total-weight n}
(graph/mst-weight wg db)
(graph/max-flow wg db source sink)  ; {:flow n :flow-map {[a b] f}}  (Edmonds-Karp)
(graph/min-cut wg db source sink)   ; the max-flow value
```

Max-flow treats the spec's edge weights as capacities. (Antiparallel real edges
are not supported — model them with an intermediate node.)

### Random walks

Walks are deterministic given a `:seed` (default 42) — no reliance on a global
RNG, so they reproduce on JVM and ClojureScript alike.

```clojure
(graph/random-walk g db start walk-length :seed 1)
(graph/weighted-random-walk wg db start walk-length :seed 1)  ; P(step) ∝ weight
(graph/biased-random-walk g db start walk-length p q :seed 1) ; Node2Vec p,q
(graph/random-walks g db walk-length walks-per-node :seed 1)  ; from every node
(graph/biased-random-walks g db walk-length walks-per-node p q :seed 1)
```

### Neighbourhood and link prediction

Direction is taken from the spec (wrap with `reverse-graph`/`undirected-graph`
as needed):

```clojure
(graph/neighbors g db a)               ; #{out-neighbours}
(graph/degree g db a)
(graph/common-neighbors g db a b)      ; count; -set for the set
(graph/jaccard-index g db a b)         ; |∩| / |∪|
(graph/adamic-adar g db a b)
(graph/resource-allocation g db a b)
(graph/preferential-attachment g db a b)
(graph/total-neighbors g db a b)
(graph/node-similarity g db a)         ; vec of [other score] sorted desc
(graph/same-community communities a b) ; 1.0 / 0.0 over a community map
(graph/link-prediction-candidates g db graph/adamic-adar :limit 50)
```

`node-similarity` returns an **ordered vector** of `[node score]` pairs.

### Semiring path computations

```clojure
(graph/trust-propagation wg db source :discount 0.9)  ; {node trust}; edge weight = per-hop trust
(graph/probability-reachability wg db a b :max-depth 5) ; max product of edge probabilities
```

| Semiring | Combine | Choose | Algorithm |
|---|---|---|---|
| Costs | + | min | `weighted-shortest-path` |
| Capacities | min | max | `bottleneck-path` |
| Probabilities | × | max | `probability-reachability` |
| Trust | × (discounted) | max | `trust-propagation` |

## Using algorithms from Datalog

Algorithms are ordinary functions, so you can call them in a `:where` clause.
Bind the spec, then call the algorithm:

```clojure
(d/q '[:find ?reachable .
       :in $ ?start
       :where
       [(datahike.experimental.graph-spec/attr-graph :follows) ?g]
       [(datahike.experimental.graph/transitive-closure ?g $ ?start) ?reachable]]
     db start)
```

For pure reachability there are also reusable Datalog rules, which the query
engine evaluates directly:

```clojure
(d/q '[:find [?t ...]
       :in $ % ?start
       :where (reachable ?start ?t :follows)]
     db graph/transitive-rules start)
```

### Planner integration

The algorithms carry planner metadata so they compose well inside Datalog:

- **`:datahike/output-cardinality`** lets the planner bound a *downstream* join by the result size. A join on `transitive-closure`/`page-rank`/`betweenness-centrality` output is sized by the graph's node count rather than the whole attribute extent.
- **`:datahike/cost`** is the algorithm's per-invocation complexity (computed from the graph's size). It lets the planner order a cheaper algorithm before a dearer one on a shared input, and — together with an execution-time probe — place an expensive algorithm *after* the joins that shrink its input and *before* any join that would expand it.

You can annotate **your own** functions with the same two keys to get the same treatment. See [the query engine docs](query-engine.md) (*Function cost model*, *Adaptive function placement*) for the mechanics.

## ClojureScript

Everything here is portable: the algorithms, the `GraphSpec` data layer, and the
underlying primitives (a priority queue, a seedable PRNG, a FIFO queue) all run
unchanged under ClojureScript, and the test suite is exercised on Node.

## Further reading

- [NetworkX](https://networkx.org/) — a Python graph library; several algorithms
  here are validated against its results.
- [LDBC Graphalytics](https://ldbcouncil.org/benchmarks/graphalytics/) — graph
  algorithm benchmarks.
