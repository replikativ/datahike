# Graph Algorithms

Datahike provides production-ready graph algorithms through the `datahike.experimental.graph` namespace. These algorithms are optimized for common graph operations and can be significantly faster than equivalent Datalog queries, scaling to 100K+ nodes.

## Why Graph Algorithms in a Datalog Database?

Graph databases like [Neo4j](https://neo4j.com/) (used by 84% of Fortune 100) and [TigerGraph](https://www.tigergraph.com/) have demonstrated that specialized graph algorithms unlock powerful use cases that pure query languages struggle with:

- **Fraud Detection** - Finding fraud rings via connected components and community detection
- **Recommendations** - Computing similarity and influence via PageRank and node similarity
- **Supply Chain** - Optimizing routes via shortest path and bottleneck analysis
- **Social Networks** - Understanding influence via centrality measures
- **Knowledge Graphs** - Building context for GenAI/RAG applications

Datahike brings these capabilities to the Clojure ecosystem with a Datalog-native API.

## Setup

```clojure
(require '[datahike.api :as d]
         '[datahike.experimental.graph :as graph])
```

## Algorithm Reference by Use Case

### Pathfinding & Routing

| Algorithm | Function | Use Case | Complexity |
|-----------|----------|----------|------------|
| Shortest Path | `shortest-path` | GPS routing, network hops | O(V+E) |
| Weighted Shortest | `weighted-shortest-path` | Cost-optimized routing | O((V+E) log V) |
| Bottleneck Path | `bottleneck-path` | Maximum bandwidth path | O((V+E) log V) |
| All Paths | `all-paths` | Route enumeration | O(V!) worst |
| Reachability | `reachable?` | Can A reach B? | O(V+E) |

**Industry examples:**
- Google Maps, Waze - turn-by-turn navigation
- OSPF/IS-IS - Internet routing protocols
- Emergency services - ambulance/fire routing

### Community & Clustering

| Algorithm | Function | Use Case | Complexity |
|-----------|----------|----------|------------|
| Connected Components | `all-connected-components` | Subgraph detection | O(V+E) |
| Strongly Connected | `strongly-connected-components` | Cyclic dependencies | O(V+E) |
| Louvain | `louvain-communities` | Community detection | O(V log V) |
| Label Propagation | `label-propagation` | Fast clustering | O(E) |

**Industry examples:**
- Fraud rings at banks (JP Morgan, UBS)
- Customer segmentation for marketing
- Social network analysis (Twitter, LinkedIn)

### Centrality & Influence

| Algorithm | Function | Use Case | Complexity |
|-----------|----------|----------|------------|
| PageRank | `page-rank` | Node importance | O(iterations × E) |
| Betweenness | `betweenness-centrality` | Information brokers | O(V × E) |
| Closeness | `closeness-centrality` | Broadcast efficiency | O(V × E) |

**Industry examples:**
- Google - web page ranking (original PageRank)
- LinkedIn - company/person importance ("LinkedIn Attractors")
- Telecommunications - network bottleneck identification

### Similarity & Recommendations

| Algorithm | Function | Use Case | Complexity |
|-----------|----------|----------|------------|
| Node Similarity | `node-similarity` | Find similar nodes | O(V²) |
| Jaccard Similarity | `jaccard-similarity` | Overlap coefficient | O(neighbors) |

**Industry examples:**
- E-commerce product recommendations (Amazon)
- Friend suggestions (Facebook, LinkedIn)
- Content recommendations (Netflix)

### Graph Structure

| Algorithm | Function | Use Case | Complexity |
|-----------|----------|----------|------------|
| Topological Sort | `topological-sort` | Task scheduling | O(V+E) |
| Cycle Detection | `has-cycle?`, `find-cycle` | Dependency validation | O(V+E) |
| Bipartite Check | `bipartite-coloring` | 2-partition problems | O(V+E) |
| Bridge Detection | `find-bridges` | Critical edge identification | O(V+E) |

### Trust & Probabilistic

| Algorithm | Function | Use Case | Complexity |
|-----------|----------|----------|------------|
| Trust Propagation | `trust-propagation` | Reputation systems | O(V+E) |
| Probability Reachability | `probability-reachability` | Risk assessment | O(paths) |

**Industry examples:**
- E-commerce trust scores
- Security risk assessment (Cisco ThreatGrid used [Asami](https://github.com/threatgrid/asami) for graph-based threat analysis)
- Nuclear safety system reliability

---

## Pathfinding Algorithms

### Shortest Path (BFS)

Find shortest path using bidirectional BFS - O(b^(d/2)) instead of O(b^d):

```clojure
(graph/shortest-path @conn 1 :person/friend 5)
;; => [1 3 5]

(graph/path-length @conn 1 :person/friend 5)
;; => 2
```

**Performance:** Tested at 100K nodes in ~1.2 seconds (vs 12.8s unoptimized).

### Weighted Shortest Path (Dijkstra)

For cost-optimized routing with edge weights:

```clojure
;; Find cheapest route considering :edge/cost
(graph/weighted-shortest-path @conn 1 :connects-to :edge/cost 10)
;; => {:path [1 3 7 10] :cost 15}
```

**Use case:** GPS navigation with traffic-aware routing, supply chain cost optimization.

### Bottleneck Path (Widest Path)

Find path maximizing minimum edge capacity - critical for bandwidth routing:

```clojure
;; Find highest-bandwidth network path
(graph/bottleneck-path @conn router-a :connects-to :link/bandwidth router-z)
;; => {:path [1 5 10] :capacity 1000}
```

**Use case:** Network bandwidth allocation, maximum flow preprocessing, emergency vehicle routing.

---

## Community Detection

### Connected Components

Find disconnected subgraphs (treats edges as undirected):

```clojure
(graph/all-connected-components @conn [:person/friend] #(pos? %))
;; => [#{1 2 3 4 5} #{10 11}]  ;; Two disconnected groups

;; Check if two nodes are in same component
(graph/same-component? @conn 1 [:person/friend] 5)
;; => true
```

**Use case:** Entity resolution, fraud ring detection, network segmentation.

### Strongly Connected Components (Tarjan)

Find SCCs in directed graphs - iterative algorithm handles 100K+ nodes without stack overflow:

```clojure
(graph/strongly-connected-components @conn :person/follows)
;; => [#{1 2 3} #{4} #{5 6}]  ;; Nodes 1,2,3 form a cycle
```

**Use case:** Dependency cycle detection, compiler optimization, social network clique analysis.

### Louvain Community Detection

Hierarchical community detection optimizing modularity:

```clojure
(graph/louvain-communities @conn :person/interacts-with)
;; => [{:community 0 :members #{1 2 3 7 8}}
;;     {:community 1 :members #{4 5 6}}
;;     {:community 2 :members #{9 10 11 12}}]

;; With options
(graph/louvain-communities @conn :person/interacts-with
  :resolution 1.0      ;; Higher = more communities
  :max-iterations 10)
```

**Use case:** Customer segmentation, fraud ring detection, social community discovery.

**Note:** For highest quality results, consider the Leiden algorithm (improved Louvain).

### Label Propagation

Fast semi-supervised community detection:

```clojure
(graph/label-propagation @conn :interacts-with)
;; => {1 :community-a, 2 :community-a, 3 :community-b, ...}

;; With seed labels
(graph/label-propagation @conn :interacts-with
  :seeds {1 :marketing, 5 :engineering})
```

**Use case:** Fast clustering when speed matters more than precision.

---

## Centrality Measures

### PageRank

Compute node importance scores (Google's original algorithm):

```clojure
(graph/page-rank @conn :links-to)
;; => {1 0.15, 2 0.22, 3 0.31, ...}

;; With options
(graph/page-rank @conn :links-to
  :damping 0.85      ;; Random jump probability
  :iterations 20     ;; Max iterations
  :tolerance 1e-6)   ;; Convergence threshold
```

**Performance:** 200x optimized - 23ms at 1K nodes, 3s at 100K nodes.

**Use case:** Web ranking, influence measurement, fraud detection (suspicious nodes with many connections), GraphRAG for LLMs.

### Betweenness Centrality

Find nodes that act as bridges/brokers in the network:

```clojure
(graph/betweenness-centrality @conn :communicates-with)
;; => {bridge-node 0.45, regular-node 0.02, ...}
```

**Use case:**
- Telecommunications - identifying network bottlenecks
- Social networks - finding "structural holes" that control information flow
- Organizations - identifying key people who bridge departments

### Closeness Centrality

Find nodes that can reach others most efficiently:

```clojure
(graph/closeness-centrality @conn :connected-to)
;; => {central-node 0.78, peripheral-node 0.23, ...}
```

**Use case:**
- Emergency services - optimal station placement
- Information dissemination - finding best "broadcasters"
- Facility location problems

---

## Similarity Algorithms

### Node Similarity (Jaccard)

Find structurally similar nodes based on shared neighbors:

```clojure
;; Find nodes similar to node 1
(graph/node-similarity @conn 1 :likes)
;; => {2 0.75, 5 0.60, 3 0.45, ...}  ;; Jaccard scores

;; Get similarity between specific nodes
(graph/jaccard-similarity @conn 1 2 :likes)
;; => 0.75  ;; 75% neighbor overlap
```

**Use case:**
- Product recommendations ("customers who bought X also bought Y")
- Friend suggestions based on mutual friends
- Duplicate detection

---

## Trust & Reputation

### Trust Propagation

Propagate trust scores through network with multiplicative decay:

```clojure
;; Compute trust from source through network
(graph/trust-propagation @conn trusted-source :trusts :trust/score)
;; => {trusted-source 1.0, friend 0.81, friend-of-friend 0.65, ...}

;; Custom discount factor
(graph/trust-propagation @conn 1 :trusts :trust/score :discount 0.8)
```

**Use case:** Reputation systems, web-of-trust PKI, recommendation quality scores.

### Probability Reachability

Compute probability of reaching target through uncertain edges:

```clojure
(graph/probability-reachability @conn source :edge :probability target)
;; => 0.72  ;; 72% chance of reaching target
```

**Use case:** Security risk assessment, reliability engineering, attack graph analysis.

---

## DAG Operations

### Topological Sort

Order nodes so all edges point forward:

```clojure
(graph/topological-sort @conn :task/depends-on)
;; => [5 4 3 2 1]  ;; Valid execution order

;; Returns nil if graph has cycles
(graph/topological-sort @conn :person/friend)
;; => nil
```

**Use case:** Build systems (Make, Gradle), task scheduling, course prerequisites.

### Cycle Detection

```clojure
(graph/has-cycle? @conn :depends-on)
;; => true

;; Get the actual cycle path
(graph/find-cycle @conn :depends-on)
;; => [1 2 3 1]
```

**Use case:** Validating DAG constraints, debugging circular dependencies.

---

## Reachability & Traversal

### Transitive Closure

Find all reachable nodes:

```clojure
(graph/transitive-closure @conn 1 :person/friend)
;; => #{2 3 4 5}

;; With options
(graph/transitive-closure @conn 1 :person/friend
  {:reflexive? true   ;; Include start node (* semantics)
   :max-depth 3})     ;; Limit traversal depth
```

### Reachability Check

```clojure
(graph/reachable? @conn 1 :person/friend 5)
;; => true
```

**Use in queries:**

```clojure
(d/q '[:find ?name
       :where [?p :person/name ?name]
              [(datahike.experimental.graph/reachable? $ 1 :person/friend ?p)]]
     @conn)
```

---

## Performance & Scalability

### Benchmarks

All algorithms tested on commodity hardware (single thread):

| Algorithm | 1K nodes | 10K nodes | 100K nodes |
|-----------|----------|-----------|------------|
| PageRank | 23ms | 240ms | 3.0s |
| Shortest Path | <1ms | 12ms | 1.2s |
| SCC (Tarjan) | 3ms | 56ms | 1.6s |
| Connected Components | 2ms | 45ms | 890ms |
| Louvain | 15ms | 180ms | 2.1s |

### Optimization Techniques Used

1. **Pre-loaded adjacency maps** - Query database once, not per-node
2. **Bidirectional BFS** - O(b^(d/2)) instead of O(b^d) for shortest path
3. **Reverse adjacency** - O(V+E) instead of O(V×E) for PageRank
4. **Iterative algorithms** - No stack overflow on large graphs

### When to Use Specialized Algorithms vs Datalog

**Use specialized algorithms when:**
- Graph exceeds a few thousand nodes
- Early termination is beneficial (shortest path, reachability)
- Algorithm needs specialized data structures (priority queue)
- Performance is critical

**Use Datalog rules when:**
- Graph is small (<1K nodes)
- Query integrates with other Datalog patterns
- Simplicity is preferred over performance

```clojure
;; Datalog alternative (slower but integrates with queries)
(d/q '[:find ?target
       :in $ %
       :where [?start :person/name "Alice"]
              (reachable ?start ?target)]
     @conn
     '[[(reachable ?x ?y) [?x :person/friend ?y]]
       [(reachable ?x ?y) [?x :person/friend ?z] (reachable ?z ?y)]])
```

---

## Comparison with Other Graph Databases

### vs Neo4j Graph Data Science

Neo4j GDS offers 65+ algorithms. Datahike covers the most commonly used:

| Category | Neo4j GDS | Datahike |
|----------|-----------|----------|
| Pathfinding | 8 algorithms | 5 algorithms |
| Centrality | 6 algorithms | 3 algorithms |
| Community | 7 algorithms | 4 algorithms |
| Similarity | 4 algorithms | 2 algorithms |

Key difference: Datahike integrates natively with Datalog queries and Clojure data structures.

### vs Amazon Neptune

Neptune Analytics offers 25+ algorithms optimized for serverless. Datahike provides similar core algorithms with simpler deployment (embedded, no cloud required).

### Unique Datahike Advantages

1. **Embedded deployment** - No separate database server
2. **Time-travel queries** - Combine graph algorithms with historical data
3. **Clojure-native** - First-class Clojure data structures
4. **Datalog integration** - Mix algorithms with declarative queries

---

## Semiring Framework

Different weight interpretations for path computations:

| Semiring | Combine | Choose | Use Case |
|----------|---------|--------|----------|
| Costs | + (sum) | min | Shortest path |
| Probabilities | × (multiply) | max | Belief propagation |
| Capacities | min | max | Bottleneck/bandwidth |
| Trust | ×d (discount) | max | Reputation |

```clojure
;; Cost-based (sum weights, minimize total)
(graph/weighted-shortest-path @conn src :edge :cost target)

;; Capacity-based (min along path, maximize result)
(graph/bottleneck-path @conn src :edge :bandwidth target)

;; Trust-based (multiply with per-hop discount)
(graph/trust-propagation @conn src :trusts :trust-score)
```

---

## Incremental Maintenance

For future integration with incremental view maintenance (MVM):

| Algorithm | Add Edge | Delete Edge | MVM Ready |
|-----------|----------|-------------|-----------|
| Transitive Closure | Easy | Medium | Partial |
| Connected Components | Easy | Hard | Partial |
| Topological Sort | Easy | Easy | Yes |
| Cycle Detection | Easy | N/A | Yes |
| Bipartite Check | Easy | Easy | Yes |
| PageRank | Easy | Easy | Yes |
| Community Detection | Medium | Hard | No |

---

## Further Reading

- [Neo4j Graph Data Science](https://neo4j.com/docs/graph-data-science/current/) - Comprehensive algorithm documentation
- [LDBC Graphalytics Benchmark](https://ldbcouncil.org/benchmarks/graphalytics/) - Industry standard benchmarks
- [NetworkX](https://networkx.org/) - Python reference implementations
- [Asami](https://github.com/quoll/asami) - Clojure graph database that inspired some of this work
