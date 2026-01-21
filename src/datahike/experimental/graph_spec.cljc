(ns datahike.experimental.graph-spec
  "Graph specification protocol for abstracting graph access patterns.

   Enables data-driven graph construction in Datalog queries via nested expressions:

     (d/q '[:find ?name ?reachable
            :where
            [?cfg :edge-attr ?attr]           ;; bind attr from DB
            [?p :name ?name]
            [(attr-graph ?attr) ?g]           ;; nested: construct graph from bound var
            [(bfs-reachable ?g $ ?p) ?r]]     ;; use constructed graph
          db)

   Graph specs can be composed:
     [(bfs-reachable (reverse-graph (attr-graph ?attr)) $ ?p) ?r]"
  (:require [datahike.api :as d]))

;; =============================================================================
;; GraphSpec Protocol
;; =============================================================================

(defprotocol GraphSpec
  "Protocol for abstracting graph access patterns.

   Implementations define how to traverse a graph structure stored in Datahike.
   This allows algorithms to work with different edge types, filtered views,
   reversed directions, etc."

  (out-neighbors [this db node]
    "Return sequence of nodes reachable via outgoing edges from node.
     Returns nil or empty seq if no outgoing edges.")

  (in-neighbors [this db node]
    "Return sequence of nodes with incoming edges to node.
     Returns nil or empty seq if no incoming edges.")

  (all-edges [this db]
    "Return sequence of [source target] pairs for all edges in graph.")

  (all-nodes [this db]
    "Return sequence of all node IDs in the graph.")

  (edge-weight [this db source target]
    "Return weight of edge from source to target, or nil if no edge.
     Default weight is 1 for unweighted graphs.")

  (directed? [this]
    "Return true if graph is directed, false if undirected."))

;; =============================================================================
;; Core Graph Spec Implementations
;; =============================================================================

(defn attr-graph
  "Create a graph spec from an edge attribute.

   Each datom [e attr v] becomes an edge from e to v.

   Usage:
     [(attr-graph :follows) ?g]
     [(attr-graph ?edge-attr) ?g]  ;; ?edge-attr bound earlier"
  [edge-attr]
  (reify GraphSpec
    (out-neighbors [_ db node]
      (seq (map :v (d/datoms db :eavt node edge-attr))))

    (in-neighbors [_ db node]
      ;; Use query since AVET index may not be populated for this attr
      (seq (d/q '[:find [?e ...]
                  :in $ ?target ?attr
                  :where [?e ?attr ?target]]
                db node edge-attr)))

    (all-edges [_ db]
      (map (juxt :e :v) (d/datoms db :aevt edge-attr)))

    (all-nodes [_ db]
      (distinct
       (mapcat (fn [datom] [(:e datom) (:v datom)])
               (d/datoms db :aevt edge-attr))))

    (edge-weight [_ _db _source _target]
      1)

    (directed? [_] true)))

(defn multi-attr-graph
  "Create a graph spec from multiple edge attributes (union).

   Edges from any of the given attributes are included.

   Usage:
     [(multi-attr-graph [:follows :friends :colleagues]) ?g]"
  [edge-attrs]
  (reify GraphSpec
    (out-neighbors [_ db node]
      (seq (distinct
            (mapcat (fn [attr]
                      (map :v (d/datoms db :eavt node attr)))
                    edge-attrs))))

    (in-neighbors [_ db node]
      (seq (d/q '[:find [?e ...]
                  :in $ ?target [?attr ...]
                  :where [?e ?attr ?target]]
                db node edge-attrs)))

    (all-edges [_ db]
      (mapcat (fn [attr]
                (map (juxt :e :v) (d/datoms db :aevt attr)))
              edge-attrs))

    (all-nodes [_ db]
      (distinct
       (mapcat (fn [attr]
                 (mapcat (fn [datom] [(:e datom) (:v datom)])
                         (d/datoms db :aevt attr)))
               edge-attrs)))

    (edge-weight [_ _db _source _target]
      1)

    (directed? [_] true)))

(defn weighted-graph
  "Create a weighted graph spec.

   edge-attr defines the graph structure.
   weight-attr provides edge weights (looked up on source entity).

   Usage:
     [(weighted-graph :connects :distance) ?g]"
  [edge-attr weight-attr]
  (reify GraphSpec
    (out-neighbors [_ db node]
      (seq (map :v (d/datoms db :eavt node edge-attr))))

    (in-neighbors [_ db node]
      (seq (d/q '[:find [?e ...]
                  :in $ ?target ?attr
                  :where [?e ?attr ?target]]
                db node edge-attr)))

    (all-edges [_ db]
      (map (juxt :e :v) (d/datoms db :aevt edge-attr)))

    (all-nodes [_ db]
      (distinct
       (mapcat (fn [datom] [(:e datom) (:v datom)])
               (d/datoms db :aevt edge-attr))))

    (edge-weight [_ db source _target]
      (or (ffirst (d/q '[:find ?w
                         :in $ ?e ?wa
                         :where [?e ?wa ?w]]
                       db source weight-attr))
          1))

    (directed? [_] true)))

;; =============================================================================
;; Graph Spec Transformers
;; =============================================================================

(defn reverse-graph
  "Wrap a graph spec to reverse edge directions.

   out-neighbors becomes in-neighbors and vice versa.

   Usage:
     [(reverse-graph (attr-graph :reports-to)) ?g]  ;; who reports TO me"
  [spec]
  (reify GraphSpec
    (out-neighbors [_ db node]
      (in-neighbors spec db node))

    (in-neighbors [_ db node]
      (out-neighbors spec db node))

    (all-edges [_ db]
      (map (fn [[s t]] [t s]) (all-edges spec db)))

    (all-nodes [_ db]
      (all-nodes spec db))

    (edge-weight [_ db source target]
      ;; Reversed: look up weight from target to source
      (edge-weight spec db target source))

    (directed? [_]
      (directed? spec))))

(defn undirected-graph
  "Wrap a graph spec to treat it as undirected.

   Both forward and backward edges are traversable.

   Usage:
     [(undirected-graph (attr-graph :knows)) ?g]"
  [spec]
  (reify GraphSpec
    (out-neighbors [_ db node]
      (distinct (concat (out-neighbors spec db node)
                        (in-neighbors spec db node))))

    (in-neighbors [_ db node]
      (out-neighbors _ db node))  ;; Same as out for undirected

    (all-edges [_ db]
      ;; Include both directions
      (let [edges (all-edges spec db)]
        (distinct (concat edges (map (fn [[s t]] [t s]) edges)))))

    (all-nodes [_ db]
      (all-nodes spec db))

    (edge-weight [_ db source target]
      (or (edge-weight spec db source target)
          (edge-weight spec db target source)))

    (directed? [_] false)))

(defn filtered-graph
  "Wrap a graph spec to filter nodes.

   node-pred is a function (fn [db node] -> boolean).
   Only nodes satisfying the predicate are included.

   Usage:
     [(filtered-graph (attr-graph :follows)
                      (fn [db n] (> (:salary (d/entity db n)) 50000))) ?g]"
  [spec node-pred]
  (reify GraphSpec
    (out-neighbors [_ db node]
      (when (node-pred db node)
        (seq (filter #(node-pred db %) (out-neighbors spec db node)))))

    (in-neighbors [_ db node]
      (when (node-pred db node)
        (seq (filter #(node-pred db %) (in-neighbors spec db node)))))

    (all-edges [_ db]
      (filter (fn [[s t]]
                (and (node-pred db s) (node-pred db t)))
              (all-edges spec db)))

    (all-nodes [_ db]
      (filter #(node-pred db %) (all-nodes spec db)))

    (edge-weight [_ db source target]
      (when (and (node-pred db source) (node-pred db target))
        (edge-weight spec db source target)))

    (directed? [_]
      (directed? spec))))

(defn subgraph
  "Wrap a graph spec to only include specific nodes.

   node-set is the set of node IDs to include.

   Usage:
     [(subgraph (attr-graph :follows) #{1 2 3 4}) ?g]"
  [spec node-set]
  (filtered-graph spec (fn [_db node] (contains? node-set node))))

;; =============================================================================
;; Graph Algorithms
;; =============================================================================

(defn bfs-reachable
  "Find all nodes reachable from start via BFS.

   Returns a set of node IDs including start.

   Usage in query:
     [(bfs-reachable ?graph $ ?start) ?reachable-set]"
  [spec db start]
  (loop [seen #{start}
         frontier #{start}]
    (if (empty? frontier)
      seen
      (let [next-nodes (into #{}
                             (comp
                              (mapcat #(out-neighbors spec db %))
                              (remove seen))
                             frontier)]
        (recur (into seen next-nodes) next-nodes)))))

(defn bfs-shortest-path
  "Find shortest path from source to target via BFS.

   Returns vector of node IDs, or nil if no path.

   Usage in query:
     [(bfs-shortest-path ?graph $ ?source ?target) ?path]"
  [spec db source target]
  (if (= source target)
    [source]
    (loop [frontier [[source]]
           visited #{source}]
      (when (seq frontier)
        (let [path (first frontier)
              node (last path)
              neighbors (out-neighbors spec db node)]
          (if-let [found (some #(when (= % target) %) neighbors)]
            (conj path found)
            (let [new-paths (for [n neighbors
                                  :when (not (visited n))]
                              (conj path n))
                  new-visited (into visited neighbors)]
              (recur (concat (rest frontier) new-paths) new-visited))))))))

(defn dfs-paths
  "Find all paths from start up to max-depth via DFS.

   Returns sequence of paths (vectors of node IDs).

   Usage in query:
     [(dfs-paths ?graph $ ?start 5) ?all-paths]"
  [spec db start max-depth]
  (letfn [(dfs [node path depth]
            (if (>= depth max-depth)
              [path]
              (let [neighbors (out-neighbors spec db node)
                    unvisited (remove (set path) neighbors)]
                (if (empty? unvisited)
                  [path]
                  (mapcat (fn [n]
                            (dfs n (conj path n) (inc depth)))
                          unvisited)))))]
    (dfs start [start] 0)))

(defn connected-components
  "Find all connected components in an undirected view of the graph.

   Returns sequence of sets, each containing node IDs in one component.

   Usage in query:
     [(connected-components ?graph $) ?components]"
  [spec db]
  (let [all (set (all-nodes spec db))
        undirected (undirected-graph spec)]
    (loop [remaining all
           components []]
      (if (empty? remaining)
        components
        (let [start (first remaining)
              component (bfs-reachable undirected db start)]
          (recur (clojure.set/difference remaining component)
                 (conj components component)))))))

(defn topological-sort
  "Topological sort of a directed acyclic graph.

   Returns vector of node IDs in topological order, or nil if cycle detected.

   Usage in query:
     [(topological-sort ?graph $) ?sorted-nodes]"
  [spec db]
  (let [nodes (set (all-nodes spec db))
        ;; Compute in-degrees
        in-degrees (reduce (fn [m [_s t]]
                             (update m t (fnil inc 0)))
                           (zipmap nodes (repeat 0))
                           (all-edges spec db))]
    (loop [result []
           degrees in-degrees
           queue (into clojure.lang.PersistentQueue/EMPTY
                       (filter #(zero? (degrees %)) nodes))]
      (if (empty? queue)
        (when (= (count result) (count nodes))
          result)  ;; nil if cycle (not all nodes processed)
        (let [node (peek queue)
              neighbors (out-neighbors spec db node)
              new-degrees (reduce (fn [m n]
                                    (update m n dec))
                                  degrees
                                  neighbors)
              new-queue (into (pop queue)
                              (filter #(and (pos? (degrees %))
                                            (zero? (new-degrees %)))
                                      neighbors))]
          (recur (conj result node) new-degrees new-queue))))))

;; =============================================================================
;; Convenience Constructors for Query Use
;; =============================================================================

(defn query-graph
  "Create a graph spec from a Datalog query.

   The query must return [source target] pairs.

   Usage:
     [(query-graph '[:find ?a ?b :where [?a :trusts ?b] [?b :trusts ?a]]) ?g]"
  [query & inputs]
  (let [cached-edges (atom nil)]
    (reify GraphSpec
      (out-neighbors [_ db node]
        (let [edges (or @cached-edges
                        (reset! cached-edges
                                (apply d/q query db inputs)))
              adj (reduce (fn [m [s t]] (update m s (fnil conj []) t))
                          {} edges)]
          (get adj node)))

      (in-neighbors [_ db node]
        (let [edges (or @cached-edges
                        (reset! cached-edges
                                (apply d/q query db inputs)))
              rev-adj (reduce (fn [m [s t]] (update m t (fnil conj []) s))
                              {} edges)]
          (get rev-adj node)))

      (all-edges [_ db]
        (or @cached-edges
            (reset! cached-edges
                    (apply d/q query db inputs))))

      (all-nodes [_ db]
        (let [edges (or @cached-edges
                        (reset! cached-edges
                                (apply d/q query db inputs)))]
          (distinct (mapcat identity edges))))

      (edge-weight [_ _db _source _target]
        1)

      (directed? [_] true))))
