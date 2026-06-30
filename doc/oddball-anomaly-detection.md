# OddBall: Egonet-Based Anomaly Detection with Datalog

> **This page is a recipe, not a shipped API.** Datahike does not export an
> `oddball` function. It shows how to build egonet-based anomaly detection from
> the [graph algorithms](graph-algorithms.md) and the shipped
> [ECOD detector](anomaly-detection.md); the code blocks below are illustrative
> user code.

OddBall is a graph anomaly detection algorithm that identifies suspicious nodes by analyzing their **ego networks** (the subgraph consisting of a node, its neighbors, and the edges between them). Normal nodes follow predictable power-law patterns; anomalies deviate from these patterns.

## Key Concept: Ego Network Features

For each node, OddBall computes:
- **N_i**: Number of nodes in ego network (node + neighbors)
- **E_i**: Number of edges in ego network
- **W_i**: Total edge weight (for weighted graphs)

Normal patterns follow power laws:
- E ∝ N^α where α ≈ 1.0 to 1.5 for social/web graphs
- Stars have high N, low E (α < 1)
- Cliques have high E, low N (α > 2)

## Datalog Queries for Egonet Analysis

### 1. Basic Ego Network Extraction

```clojure
;; Get all neighbors of a node (1-hop ego network nodes)
(defn egonet-neighbors [db node edge-attr]
  (d/q '[:find [?neighbor ...]
         :in $ ?node ?edge-attr
         :where
         (or [?node ?edge-attr ?neighbor]
             [?neighbor ?edge-attr ?node])]
       db node edge-attr))

;; Count edges within ego network (edges between node's neighbors)
(defn egonet-edge-count [db node edge-attr]
  (let [neighbors (set (egonet-neighbors db node edge-attr))]
    (d/q '[:find (count ?e) .
           :in $ ?neighbors ?edge-attr
           :where
           [?s ?edge-attr ?t]
           [(contains? ?neighbors ?s)]
           [(contains? ?neighbors ?t)]]
         db neighbors edge-attr)))
```

### 2. Complete Egonet Feature Extraction

```clojure
(require '[datahike.api :as d])

(defn egonet-features
  "Extract OddBall features for a node's ego network.
   Returns {:node-count N, :edge-count E, :density D}"
  [db node edge-attr]
  (let [;; Get 1-hop neighbors (undirected)
        neighbors (d/q '[:find [?n ...]
                         :in $ ?node ?attr
                         :where
                         (or [?node ?attr ?n]
                             [?n ?attr ?node])]
                       db node edge-attr)
        ego-nodes (conj (set neighbors) node)
        n (count ego-nodes)

        ;; Count edges within ego network
        edges (d/q '[:find ?s ?t
                     :in $ ?ego ?attr
                     :where
                     [?s ?attr ?t]
                     [(contains? ?ego ?s)]
                     [(contains? ?ego ?t)]]
                   db ego-nodes edge-attr)
        e (count edges)

        ;; Density = E / (N*(N-1)/2) for undirected
        max-edges (/ (* n (dec n)) 2)
        density (if (pos? max-edges) (/ e max-edges) 0)]

    {:node-count n
     :edge-count e
     :density density
     :neighbors neighbors}))
```

### 3. Batch Analysis for Anomaly Scoring

```clojure
(defn all-egonet-features
  "Compute egonet features for all nodes in graph."
  [db edge-attr]
  (let [nodes (d/q '[:find [?n ...]
                     :in $ ?attr
                     :where
                     (or [?n ?attr _]
                         [_ ?attr ?n])]
                   db edge-attr)]
    (into {}
      (for [node nodes]
        [node (egonet-features db node edge-attr)]))))

(defn oddball-score
  "Compute OddBall anomaly scores.
   Nodes with high deviation from E = C * N^alpha are suspicious."
  [db edge-attr & {:keys [alpha] :or {alpha 1.2}}]
  (let [features (all-egonet-features db edge-attr)
        ;; Expected edges for given node count: E = C * N^alpha
        ;; Use log-log regression ideally, or simple formula
        scores (for [[node {:keys [node-count edge-count]}] features
                     :when (> node-count 1)]
                 (let [expected (Math/pow node-count alpha)
                       deviation (Math/abs (Math/log (/ (max edge-count 1) expected)))]
                   [node {:node-count node-count
                          :edge-count edge-count
                          :expected-edges expected
                          :deviation deviation}]))]
    (sort-by #(-> % second :deviation) > scores)))
```

## Anomaly Patterns OddBall Detects

### Star Pattern (Near-Star)
- Many neighbors, few edges between them
- E << N^α
- **Example**: Bot account following many unrelated users

```clojure
;; Detect star-like nodes: high degree, low clustering
(defn find-stars [db edge-attr & {:keys [min-degree max-density]
                                   :or {min-degree 10 max-density 0.1}}]
  (let [features (all-egonet-features db edge-attr)]
    (filter (fn [[node {:keys [node-count density]}]]
              (and (>= (dec node-count) min-degree)  ; degree = N-1
                   (< density max-density)))
            features)))
```

### Clique Pattern (Near-Clique)
- Tightly connected subgraph
- E >> N^α
- **Example**: Fraud ring where all members transact with each other

```clojure
;; Detect clique-like nodes: high density
(defn find-cliques [db edge-attr & {:keys [min-density min-size]
                                     :or {min-density 0.8 min-size 4}}]
  (let [features (all-egonet-features db edge-attr)]
    (filter (fn [[node {:keys [node-count density]}]]
              (and (>= node-count min-size)
                   (>= density min-density)))
            features)))
```

### Heavy Vicinity
- Unusually high total edge weight in ego network
- Requires weighted graph

```clojure
(defn egonet-weight [db node edge-attr weight-attr]
  (let [neighbors (egonet-neighbors db node edge-attr)
        ego-nodes (conj (set neighbors) node)]
    (d/q '[:find (sum ?w) .
           :in $ ?ego ?edge-attr ?weight-attr
           :where
           [?e ?edge-attr ?t]
           [(contains? ?ego ?e)]
           [(contains? ?ego ?t)]
           [?e ?weight-attr ?w]]
         db ego-nodes edge-attr weight-attr)))
```

## Integration with Community Detection

OddBall works well with community detection. Anomalies often:
1. Bridge multiple communities (unusual position)
2. Have different patterns than their community peers
3. Belong to anomalous communities (unusual size, structure)

### Label Propagation for Fast Community Assignment

Label propagation is fast and good for detecting natural clusters:

```clojure
(require '[datahike.experimental.graph :as graph])

(defn community-oddball [db edge-attr]
  (let [;; Get community assignments (new API returns map with :communities key)
        lp-result (graph/label-propagation db edge-attr)
        labels (:communities lp-result)
        ;; Get egonet features
        features (all-egonet-features db edge-attr)
        ;; Group by community
        by-community (group-by (fn [[node _]] (labels node)) features)]
    ;; Find within-community outliers
    (for [[comm members] by-community
          :let [densities (map #(-> % second :density) members)
                avg-density (/ (reduce + densities) (count densities))
                std-density (Math/sqrt (/ (reduce + (map #(Math/pow (- % avg-density) 2) densities))
                                          (count densities)))]]
      {:community comm
       :outliers (filter (fn [[_ {:keys [density]}]]
                          (> (Math/abs (- density avg-density)) (* 2 std-density)))
                        members)})))
```

**Anomaly signals from Label Propagation:**
- `:converged false` - Graph structure is unstable, may indicate unusual topology
- High `:isolation-score` in stats - Many singleton communities, possible outliers

### Louvain for Modularity-Based Anomaly Detection

Louvain provides modularity scores useful for structural anomaly detection:

```clojure
(defn modularity-anomalies [db edge-attr]
  (let [result (graph/louvain db edge-attr)
        communities (:communities result)
        modularity (:modularity result)
        stats (:stats result)]

    ;; Anomaly signals:
    ;; 1. Unusually low modularity indicates random/unusual structure
    ;; 2. High isolation-score means many nodes don't fit any community
    ;; 3. Extreme community sizes (too small or too large)

    {:modularity modularity
     :modularity-anomaly? (< modularity 0.3)  ; Typical graphs have Q > 0.3
     :isolation-score (:isolation-score stats)
     :num-communities (:num-communities stats)
     :size-distribution (:sizes stats)

     ;; Find nodes in unusually small communities (potential outliers)
     :small-community-nodes
     (let [small-comms (set (for [[c size] (:sizes stats) :when (= size 1)] c))]
       (filter (fn [[node comm]] (small-comms comm)) communities))}))
```

### Community Bridge Detection

Nodes that connect multiple communities are often interesting (influencers, brokers, or anomalies):

```clojure
(defn find-bridge-nodes [db edge-attr]
  (let [communities (:communities (graph/louvain db edge-attr))
        ;; For each node, count how many different communities its neighbors belong to
        nodes (keys communities)]
    (for [node nodes
          :let [neighbors (d/q '[:find [?n ...]
                                 :in $ ?node ?attr
                                 :where (or [?node ?attr ?n] [?n ?attr ?node])]
                               db node edge-attr)
                neighbor-comms (set (map communities neighbors))
                bridge-score (count neighbor-comms)]
          :when (> bridge-score 1)]
      {:node node
       :community (communities node)
       :connected-communities neighbor-comms
       :bridge-score bridge-score})))
```

### Combined Anomaly Score

Combine OddBall egonet features with community detection:

```clojure
(defn combined-anomaly-score [db edge-attr]
  (let [;; Community detection
        louvain-result (graph/louvain db edge-attr)
        communities (:communities louvain-result)
        comm-sizes (:sizes (:stats louvain-result))

        ;; Egonet features
        egonet (all-egonet-features db edge-attr)

        ;; Compute per-node anomaly score
        scores
        (for [[node features] egonet
              :let [comm (communities node)
                    comm-size (get comm-sizes comm 1)
                    {:keys [node-count edge-count density]} features

                    ;; OddBall: deviation from expected edges
                    expected-edges (Math/pow node-count 1.2)
                    oddball-score (Math/abs (Math/log (/ (max edge-count 1) expected-edges)))

                    ;; Community: small community = suspicious
                    comm-score (if (< comm-size 3) 1.0 0.0)

                    ;; Density: extreme density = suspicious
                    density-score (if (or (< density 0.1) (> density 0.9)) 0.5 0.0)

                    ;; Combined score
                    total (+ oddball-score comm-score density-score)]]
          [node {:oddball oddball-score
                 :community-size comm-size
                 :density density
                 :total-score total}])]

    (sort-by #(-> % second :total-score) > scores)))
```

## Use Cases

1. **Fraud Detection**: Identify unusual transaction patterns (fraud rings = cliques, money mules = stars)
2. **Bot Detection**: Social media bots often have star-like patterns
3. **Intrusion Detection**: Compromised machines may show unusual connection patterns
4. **Spam Detection**: Spammers connect to many unrelated entities

## Vector-Based Anomaly Detection (for Embedding Vectors)

When nodes have associated embedding vectors (e.g., from Node2Vec, GNNs, or external embeddings), additional anomaly detection methods become available:

### HNSW for kNN-Based Outliers (Global)

Use proximum's HNSW index for fast kNN queries:
- **Anomaly score**: kNN distance in embedding space
- **Catches**: Points far from everything (global outliers)
- **Strength**: O(log n) query time, persistent index

### UMAP/EVoC for Density-Aware Outliers (Local)

UMAP-style density estimation catches local anomalies:
- Computes per-point density via sigma/rho smoothing
- Points with unusual density relative to neighbors are flagged
- **Catches**: Points that don't fit local manifold structure

### Recommended Approach

For graph anomaly detection with embeddings:

1. **Batch analysis**: Use EVoC directly
   - Provides GLOSH scores from hierarchical clustering
   - Multi-granularity cluster assignments
   - No index needed

2. **Streaming/online**: HNSW on UMAP embedding
   - UMAP embeds data (captures manifold structure)
   - HNSW indexes embeddings (fast kNN queries)
   - Anomaly score = kNN distance in embedding space

### Integration with Graph Features

Combine structural (OddBall) and embedding-based scores:

```clojure
(defn hybrid-anomaly-score [db edge-attr embeddings-index]
  (let [;; Structural: OddBall + community detection
        structural-scores (combined-anomaly-score db edge-attr)

        ;; Embedding: kNN distance from HNSW
        ;; (pseudocode - depends on proximum API)
        embedding-scores (for [[node _] structural-scores
                               :let [knn-dist (hnsw-knn-distance embeddings-index node 10)]]
                           [node knn-dist])]

    ;; Combine scores (normalize and weight)
    (for [[node struct-score] structural-scores
          :let [embed-score (get embedding-scores node 0)
                combined (+ (* 0.5 (:total-score struct-score))
                            (* 0.5 (normalize embed-score)))]]
      [node (assoc struct-score :embedding-score embed-score
                                :hybrid-score combined)])))
```

## References

- Akoglu, L., McGlohon, M., & Faloutsos, C. (2010). "OddBall: Spotting Anomalies in Weighted Graphs"
- The algorithm uses power-law deviations: normal graphs follow E ∝ N^α patterns
- Traag, V. A., et al. (2019). "From Louvain to Leiden: guaranteeing well-connected communities"
- McInnes, L., et al. (2017). "UMAP: Uniform Manifold Approximation and Projection"
