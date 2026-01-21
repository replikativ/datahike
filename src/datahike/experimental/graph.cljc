(ns datahike.experimental.graph
  "Graph algorithms for Datahike.

   Provides:
   - Transitive closure (reachability)
   - All paths enumeration
   - Connected components (subgraph detection)
   - Shortest path (BFS)

   These can be used directly or via query function calls."
  (:require [datahike.api :as d]
            [clojure.set :as set]))

;; =============================================================================
;; Transitive Closure
;; =============================================================================

(defn transitive-closure
  "Find all entities reachable from `start-eid` via `attr` in db.
   Returns a set of entity IDs (not including start-eid).

   This implements + semantics (at least one hop).
   For * semantics (zero or more hops), use with :reflexive? true."
  ([db start-eid attr]
   (transitive-closure db start-eid attr {}))
  ([db start-eid attr {:keys [reflexive? max-depth]
                       :or {reflexive? false max-depth nil}}]
   (loop [seen #{start-eid}
          frontier #{start-eid}
          depth 0]
     (if (or (empty? frontier) (and max-depth (>= depth max-depth)))
       (cond-> seen (not reflexive?) (disj start-eid))
       (let [next-eids (into #{}
                             (comp
                              (mapcat (fn [eid]
                                        (d/q '[:find [?target ...]
                                               :in $ ?e ?a
                                               :where [?e ?a ?target]]
                                             db eid attr)))
                              (remove seen))
                             frontier)]
         (recur (into seen next-eids) next-eids (inc depth)))))))

(defn reachable?
  "Check if target is reachable from source via attr.
   Can be used in Datahike query clauses via function calls.

   Example:
     (d/q '[:find ?name
            :in $ reachable?
            :where [?p :person/name ?name]
                   [(reachable? $ ?p :follows 42)]]
          db reachable?)"
  [db source attr target]
  (contains? (transitive-closure db source attr) target))

;; =============================================================================
;; Path Enumeration
;; =============================================================================

(defn all-paths
  "Find all paths from start-eid to any reachable node via attr.
   Returns a sequence of paths, where each path is a vector of entity IDs.

   Options:
     :max-depth - limit traversal depth (default 10)
     :target-eid - only return paths to this target"
  ([db start-eid attr]
   (all-paths db start-eid attr {}))
  ([db start-eid attr {:keys [max-depth target-eid]
                       :or {max-depth 10}}]
   (loop [frontier [[start-eid [start-eid]]]
          depth 0
          paths []]
     (if (or (empty? frontier) (>= depth max-depth))
       (cond->> paths
         target-eid (filter #(= (last %) target-eid)))
       (let [expansions
             (for [[eid path] frontier
                   target (d/q '[:find [?t ...]
                                 :in $ ?e ?a
                                 :where [?e ?a ?t]]
                               db eid attr)
                   :when (not (some #{target} path))]  ;; Avoid cycles
               [target (conj path target)])
             new-paths (map second expansions)]
         (recur expansions (inc depth) (into paths new-paths)))))))

;; =============================================================================
;; Shortest Path
;; =============================================================================

(defn shortest-path
  "Find the shortest path from source to target via attr using BFS.
   Returns nil if no path exists, or a vector of entity IDs.

   Uses bidirectional BFS for better performance on large graphs -
   O(b^(d/2)) instead of O(b^d) where b is branching factor and d is distance."
  [db source attr target]
  (if (= source target)
    [source]
    ;; Pre-load all edges once instead of querying per node
    (let [edges (d/q '[:find ?s ?t
                       :in $ ?attr
                       :where [?s ?attr ?t]]
                     db attr)
          ;; Build forward and reverse adjacency for bidirectional BFS
          fwd-adj (reduce (fn [m [s t]] (update m s (fnil conj #{}) t)) {} edges)
          rev-adj (reduce (fn [m [s t]] (update m t (fnil conj #{}) s)) {} edges)
          get-fwd (fn [n] (fwd-adj n #{}))
          get-rev (fn [n] (rev-adj n #{}))]
      ;; Bidirectional BFS - expand from both ends, meet in middle
      (loop [fwd-frontier #{source}
             rev-frontier #{target}
             fwd-parent {source nil}
             rev-parent {target nil}]
        (when (and (seq fwd-frontier) (seq rev-frontier))
          ;; Check for intersection
          (let [intersection (clojure.set/intersection (set (keys fwd-parent))
                                                       (set (keys rev-parent)))]
            (if (seq intersection)
              ;; Found meeting point - reconstruct path
              (let [meet (first intersection)
                    ;; Build path from source to meet
                    fwd-path (loop [path [] node meet]
                               (if (nil? node)
                                 (vec (reverse path))
                                 (recur (conj path node) (fwd-parent node))))
                    ;; Build path from meet to target (skip meet, already included)
                    rev-path (loop [path [] node (rev-parent meet)]
                               (if (nil? node)
                                 path
                                 (recur (conj path node) (rev-parent node))))]
                (into fwd-path rev-path))
              ;; Expand smaller frontier (optimization)
              (if (<= (count fwd-frontier) (count rev-frontier))
                ;; Expand forward
                (let [new-fwd (into {} (for [n fwd-frontier
                                             neighbor (get-fwd n)
                                             :when (not (contains? fwd-parent neighbor))]
                                         [neighbor n]))]
                  (recur (set (keys new-fwd))
                         rev-frontier
                         (merge fwd-parent new-fwd)
                         rev-parent))
                ;; Expand backward
                (let [new-rev (into {} (for [n rev-frontier
                                             neighbor (get-rev n)
                                             :when (not (contains? rev-parent neighbor))]
                                         [neighbor n]))]
                  (recur fwd-frontier
                         (set (keys new-rev))
                         fwd-parent
                         (merge rev-parent new-rev)))))))))))

(defn path-length
  "Returns the length of the shortest path, or nil if unreachable."
  [db source attr target]
  (when-let [path (shortest-path db source attr target)]
    (dec (count path))))

;; =============================================================================
;; Connected Components
;; =============================================================================

(defn connected-component
  "Find all entities in the same connected component as start-eid.
   Traverses both forward and backward through the given attributes.

   attrs can be a keyword or a collection of keywords."
  [db start-eid attrs]
  (let [attrs (if (keyword? attrs) [attrs] (vec attrs))]
    (loop [seen #{start-eid}
           frontier #{start-eid}]
      (if (empty? frontier)
        seen
        (let [;; Forward edges
              forward (into #{}
                            (for [eid frontier
                                  attr attrs
                                  target (d/q '[:find [?t ...]
                                                :in $ ?e ?a
                                                :where [?e ?a ?t]]
                                              db eid attr)
                                  :when (not (seen target))]
                              target))
              ;; Backward edges
              backward (into #{}
                             (for [eid frontier
                                   attr attrs
                                   source (d/q '[:find [?s ...]
                                                 :in $ ?e ?a
                                                 :where [?s ?a ?e]]
                                               db eid attr)
                                   :when (not (seen source))]
                               source))
              next-frontier (set/union forward backward)]
          (recur (into seen next-frontier) next-frontier))))))

(defn all-connected-components
  "Find all disconnected subgraphs in db.

   Args:
     db - Datahike database value
     attrs - attribute(s) to traverse for connectivity
     entity-pred - predicate to filter which entities to consider
                   (receives entity ID, returns truthy to include)

   Returns a sequence of sets, where each set contains entity IDs
   in the same connected component."
  [db attrs entity-pred]
  (let [all-entities (into #{}
                           (d/q '[:find [?e ...]
                                  :in $ ?pred
                                  :where [?e ?a _]
                                  [(not= ?a :db/txInstant)]
                                  [(?pred ?e)]]
                                db entity-pred))]
    (loop [remaining all-entities
           components []]
      (if (empty? remaining)
        components
        (let [start (first remaining)
              component (connected-component db start attrs)
              valid-component (set/intersection component remaining)]
          (recur (set/difference remaining valid-component)
                 (conj components valid-component)))))))

(defn same-component?
  "Check if two entities are in the same connected component.
   Can be used in query function calls."
  [db eid1 attrs eid2]
  (contains? (connected-component db eid1 attrs) eid2))

;; =============================================================================
;; Weighted/Scored Traversal (for future extension)
;; =============================================================================

(defn weighted-shortest-path
  "Find shortest path considering edge weights.

   weight-attr - attribute containing edge weight (must be a number)

   Returns {:path [...] :cost N} or nil if unreachable.
   Uses Dijkstra's algorithm."
  [db source edge-attr weight-attr target]
  ;; Priority queue simulation using sorted-set
  (loop [frontier (sorted-set-by (fn [[c1 _] [c2 _]] (compare c1 c2))
                                 [0 source [source]])
         visited #{}]
    (when (seq frontier)
      (let [[cost eid path] (first frontier)
            frontier (disj frontier (first frontier))]
        (cond
          (= eid target)
          {:path path :cost cost}

          (visited eid)
          (recur frontier visited)

          :else
          (let [edges (d/q '[:find ?t ?w
                             :in $ ?e ?ea ?wa
                             :where [?e ?ea ?t]
                             [?e ?wa ?w]]
                           db eid edge-attr weight-attr)
                new-frontier (reduce
                              (fn [f [t w]]
                                (if (visited t)
                                  f
                                  (conj f [(+ cost (or w 1)) t (conj path t)])))
                              frontier
                              edges)]
            (recur new-frontier (conj visited eid))))))))

;; =============================================================================
;; Query Helper Functions
;; =============================================================================

(defn make-transitive-pred
  "Create a predicate function for use in queries that checks transitivity.

   Usage:
     (def follows+ (make-transitive-pred :person/follows))
     (d/q '[:find ?name
            :in $ follows+
            :where [?start :person/name \"Alice\"]
                   [(follows+ $ ?start ?target)]
                   [?target :person/name ?name]]
          db follows+)"
  [attr]
  (fn [db source target]
    (reachable? db source attr target)))

;; =============================================================================
;; Semi-Naive Evaluation
;; =============================================================================

(defn semi-naive-transitive-closure
  "Transitive closure using semi-naive evaluation.
   Only computes new derivations each iteration (not re-deriving known facts).

   This is significantly faster than naive iteration for larger graphs.

   Returns a set of [source target] pairs."
  [db edge-attr]
  (let [edges (into #{}
                    (d/q '[:find ?s ?t
                           :in $ ?attr
                           :where [?s ?attr ?t]]
                         db edge-attr))
        ;; Index edges by source for O(1) lookup
        edges-by-source (group-by first edges)]
    (loop [known edges
           delta edges]
      (if (empty? delta)
        known
        ;; Only join delta (new facts) with edges
        (let [new-facts (into #{}
                              (for [[x z] delta
                                    [_ y] (edges-by-source z)
                                    :when (not (known [x y]))]
                                [x y]))]
          (recur (into known new-facts) new-facts))))))

;; =============================================================================
;; Native Datalog Rules
;; =============================================================================

(def ^{:doc "Standard Datalog rules for transitive closure.
             Use with (d/q query db %) where % binds these rules.

             Example:
               (d/q '[:find ?target
                      :in $ %
                      :where [?start :person/name \"Alice\"]
                             (reachable ?start ?target :person/friend)]
                    db transitive-rules)"}
  transitive-rules
  '[;; reachable(?x, ?y, ?attr) - ?y is reachable from ?x via ?attr
    [(reachable ?x ?y ?attr)
     [?x ?attr ?y]]
    [(reachable ?x ?y ?attr)
     [?x ?attr ?z]
     (reachable ?z ?y ?attr)]])

(def ^{:doc "Rules for computing paths with length.
             Returns (path-length ?x ?y ?attr ?len) tuples."}
  path-length-rules
  '[[(path-length ?x ?y ?attr 1)
     [?x ?attr ?y]]
    [(path-length ?x ?y ?attr ?len)
     [?x ?attr ?z]
     (path-length ?z ?y ?attr ?len-1)
     [(inc ?len-1) ?len]]])

;; =============================================================================
;; Design Notes: Future Optimizations
;; =============================================================================

;; Current limitations (as of this implementation):
;; - Datahike doesn't have a query planner yet
;; - No predicate pushdown into range queries
;; - No magic sets transformation
;; - No semi-naive evaluation in core query engine
;;
;; What we provide at the algorithm level:
;; - Semi-naive transitive closure (semi-naive-transitive-closure)
;; - Indexed lookups within our BFS/Dijkstra implementations
;; - Early termination when possible
;;
;; For probabilistic/weighted logic, weight types to consider:
;; - Probabilities: multiply, range [0,1], maximize
;; - Costs: sum, minimize
;; - Capacities: min along path, maximize (max-flow)
;; - Trust/confidence: various semiring operations
;;
;; See Souffle for inspiration on subsumption:
;;   sp(x,y,c1) <= sp(x,y,c2) :- c2 <= c1.
;; This discards dominated tuples (longer paths) during computation.

;; =============================================================================
;; Strongly Connected Components (Tarjan's Algorithm)
;; =============================================================================
;; Incremental potential: Add-only (merge SCCs), delete is hard (may split)

(defn strongly-connected-components
  "Find all strongly connected components using iterative Tarjan's algorithm.
   Returns a vector of sets, where each set is an SCC.

   O(V + E) time complexity. Uses explicit stack instead of recursion
   to handle large graphs without stack overflow.

   Incremental note: Adding edges can merge SCCs (easy to update).
   Deleting edges may split SCCs (requires recomputation)."
  [db edge-attr]
  ;; Pre-load all edges once instead of querying per node
  (let [edges (d/q '[:find ?s ?t
                     :in $ ?attr
                     :where [?s ?attr ?t]]
                   db edge-attr)
        nodes (into #{} (mapcat identity edges))
        ;; Build adjacency map
        adj (reduce (fn [m [s t]] (update m s (fnil conj []) t)) {} edges)
        get-successors (fn [n] (adj n []))]
    (if (empty? nodes)
      []
      ;; Iterative Tarjan's using explicit call stack
      ;; Each stack frame: [node, successor-index, phase]
      ;; phase: :enter (first visit) or :return (returning from successor)
      (loop [work-stack (list)           ;; Stack of [node succ-idx phase return-to]
             nodes-to-visit (seq nodes)
             index 0
             indices {}                   ;; node -> discovery index
             lowlinks {}                  ;; node -> lowlink value
             node-stack []                ;; Tarjan's stack for SCC construction
             on-stack #{}                 ;; Set of nodes on node-stack
             sccs []]                     ;; Result: vector of SCC sets
        (cond
          ;; Process current work item
          (seq work-stack)
          (let [[v succ-idx phase return-to] (first work-stack)
                rest-work (rest work-stack)
                successors (get-successors v)]
            (cond
              ;; First visit to node v
              (= phase :enter)
              (recur (cons [v 0 :process return-to] rest-work)
                     nodes-to-visit
                     (inc index)
                     (assoc indices v index)
                     (assoc lowlinks v index)
                     (conj node-stack v)
                     (conj on-stack v)
                     sccs)

              ;; Processing successors of v
              (= phase :process)
              (if (< succ-idx (count successors))
                (let [w (nth successors succ-idx)]
                  (cond
                    ;; w not yet visited - "recurse" into it
                    (not (contains? indices w))
                    (recur (cons [w 0 :enter v]
                                 (cons [v (inc succ-idx) :process return-to] rest-work))
                           nodes-to-visit index indices lowlinks node-stack on-stack sccs)

                    ;; w is on stack - update lowlink
                    (contains? on-stack w)
                    (recur (cons [v (inc succ-idx) :process return-to] rest-work)
                           nodes-to-visit
                           index
                           indices
                           (update lowlinks v min (indices w))
                           node-stack on-stack sccs)

                    ;; w already processed (cross edge) - continue
                    :else
                    (recur (cons [v (inc succ-idx) :process return-to] rest-work)
                           nodes-to-visit index indices lowlinks node-stack on-stack sccs)))

                ;; Done with all successors - check if v is SCC root
                (if (= (lowlinks v) (indices v))
                  ;; v is root - pop SCC from node-stack
                  (let [[scc new-stack new-on-stack]
                        (loop [scc #{} stk node-stack ons on-stack]
                          (let [w (peek stk)]
                            (if (= w v)
                              [(conj scc w) (pop stk) (disj ons w)]
                              (recur (conj scc w) (pop stk) (disj ons w)))))]
                    ;; Update lowlink of parent if returning
                    (if return-to
                      (recur rest-work nodes-to-visit index indices
                             (update lowlinks return-to min (lowlinks v))
                             new-stack new-on-stack (conj sccs scc))
                      (recur rest-work nodes-to-visit index indices lowlinks
                             new-stack new-on-stack (conj sccs scc))))
                  ;; Not root - just update parent's lowlink if returning
                  (if return-to
                    (recur rest-work nodes-to-visit index indices
                           (update lowlinks return-to min (lowlinks v))
                           node-stack on-stack sccs)
                    (recur rest-work nodes-to-visit index indices lowlinks
                           node-stack on-stack sccs))))

              :else
              (recur rest-work nodes-to-visit index indices lowlinks node-stack on-stack sccs)))

          ;; Start processing next unvisited node
          (seq nodes-to-visit)
          (let [v (first nodes-to-visit)]
            (if (contains? indices v)
              (recur work-stack (rest nodes-to-visit) index indices lowlinks node-stack on-stack sccs)
              (recur (list [v 0 :enter nil])
                     (rest nodes-to-visit)
                     index indices lowlinks node-stack on-stack sccs)))

          ;; Done
          :else sccs)))))

;; =============================================================================
;; Topological Sort
;; =============================================================================
;; Incremental potential: ✅ Can maintain order incrementally for DAGs

(defn topological-sort
  "Topological sort using Kahn's algorithm.
   Returns a vector of entity IDs in topological order, or nil if cycle exists.

   O(V + E) time complexity.

   Incremental note: Order can be maintained incrementally when edges are
   added/removed, as long as the graph remains a DAG."
  [db edge-attr]
  (let [;; Get all edges
        edges (d/q '[:find ?s ?t
                     :in $ ?attr
                     :where [?s ?attr ?t]]
                   db edge-attr)
        ;; Compute in-degrees
        nodes (into #{} (mapcat identity edges))
        in-degree (reduce (fn [m [_ t]] (update m t (fnil inc 0)))
                          (zipmap nodes (repeat 0))
                          edges)
        ;; Build adjacency list
        adj (reduce (fn [m [s t]] (update m s (fnil conj []) t))
                    {}
                    edges)]
    (loop [queue (into clojure.lang.PersistentQueue/EMPTY
                       (filter #(zero? (in-degree %)) nodes))
           in-deg in-degree
           result []]
      (if (empty? queue)
        (when (= (count result) (count nodes))
          result)  ;; nil if cycle (not all nodes processed)
        (let [n (peek queue)
              queue' (pop queue)
              ;; Decrease in-degree of successors
              [queue'' in-deg']
              (reduce (fn [[q d] succ]
                        (let [new-deg (dec (d succ))]
                          [(if (zero? new-deg) (conj q succ) q)
                           (assoc d succ new-deg)]))
                      [queue' in-deg]
                      (adj n []))]
          (recur queue'' in-deg' (conj result n)))))))

(defn has-cycle?
  "Check if the graph has a cycle.

   Incremental note: When adding an edge (u,v), only need to check
   if v can reach u (would create cycle)."
  [db edge-attr]
  (nil? (topological-sort db edge-attr)))

;; =============================================================================
;; Cycle Detection with Path
;; =============================================================================

(defn find-cycle
  "Find a cycle in the graph if one exists.
   Returns a vector of entity IDs forming the cycle, or nil.

   Useful for debugging constraint violations."
  [db edge-attr]
  (let [nodes (vec (d/q '[:find [?n ...]
                          :in $ ?attr
                          :where [?n ?attr _]]
                        db edge-attr))]
    (when (seq nodes)
      (loop [idx 0
             visited #{}
             rec-stack #{}
             parent {}]
        (if (>= idx (count nodes))
          nil
          (let [start (nth nodes idx)]
            (if (visited start)
              (recur (inc idx) visited rec-stack parent)
              ;; DFS from start using a vector stack
              (loop [stack [[start nil]]
                     vis visited
                     rec rec-stack
                     par parent]
                (if (empty? stack)
                  (recur (inc idx) vis #{} par)
                  (let [[node from] (peek stack)
                        stack' (pop stack)]
                    (cond
                      (rec node)
                      ;; Found cycle - reconstruct path back
                      (loop [path [node] curr from]
                        (if (or (nil? curr) (= curr node))
                          (if curr (conj path curr) path)
                          (recur (conj path curr) (par curr))))

                      (vis node)
                      (recur stack' vis rec par)

                      :else
                      (let [succs (d/q '[:find [?t ...]
                                         :in $ ?s ?attr
                                         :where [?s ?attr ?t]]
                                       db node edge-attr)
                            new-stack (into stack' (for [s succs] [s node]))]
                        (recur new-stack
                               (conj vis node)
                               (conj rec node)
                               (assoc par node from))))))))))))))

;; =============================================================================
;; Bipartite Check
;; =============================================================================
;; Incremental potential: ✅ Easy - check new edges don't violate coloring

(defn bipartite-coloring
  "Check if graph is bipartite and return 2-coloring if so.
   Returns {:bipartite? true :coloring {node -> 0|1}} or {:bipartite? false}.

   Treats edges as undirected.

   Incremental note: When adding edge (u,v), just check color[u] != color[v]."
  [db edge-attr]
  (let [edges (d/q '[:find ?s ?t
                     :in $ ?attr
                     :where [?s ?attr ?t]]
                   db edge-attr)
        nodes (into #{} (mapcat identity edges))
        ;; Build undirected adjacency
        adj (reduce (fn [m [s t]]
                      (-> m
                          (update s (fnil conj #{}) t)
                          (update t (fnil conj #{}) s)))
                    {}
                    edges)]
    (loop [remaining (seq nodes)
           coloring {}]
      (if (empty? remaining)
        {:bipartite? true :coloring coloring}
        (let [start (first remaining)]
          (if (coloring start)
            (recur (rest remaining) coloring)
            ;; BFS coloring - returns updated coloring or :not-bipartite
            (let [bfs-result
                  (loop [queue (conj clojure.lang.PersistentQueue/EMPTY start)
                         col (assoc coloring start 0)]
                    (if (empty? queue)
                      col  ;; Return the coloring
                      (let [n (peek queue)
                            queue' (pop queue)
                            n-color (col n)
                            neighbor-color (- 1 n-color)]
                        (let [result
                              (reduce
                               (fn [[q c] neighbor]
                                 (cond
                                   (not (c neighbor))
                                   [(conj q neighbor) (assoc c neighbor neighbor-color)]

                                   (not= (c neighbor) neighbor-color)
                                   (reduced :not-bipartite)

                                   :else [q c]))
                               [queue' col]
                               (adj n #{}))]
                          (if (= result :not-bipartite)
                            :not-bipartite
                            (recur (first result) (second result)))))))]
              (if (= bfs-result :not-bipartite)
                {:bipartite? false}
                (recur (rest remaining) bfs-result)))))))))

;; =============================================================================
;; Bridge Detection (Cut Edges)
;; =============================================================================
;; Incremental potential: Complex - requires maintaining 2-edge-connectivity

(defn find-bridges
  "Find all bridges (cut edges) in an undirected graph.
   A bridge is an edge whose removal disconnects the graph.
   Returns a set of [source target] pairs.

   Uses Tarjan's bridge-finding algorithm. O(V + E)."
  [db edge-attr]
  (let [edges (d/q '[:find ?s ?t
                     :in $ ?attr
                     :where [?s ?attr ?t]]
                   db edge-attr)
        nodes (into #{} (mapcat identity edges))
        ;; Build undirected adjacency
        adj (reduce (fn [m [s t]]
                      (-> m
                          (update s (fnil conj #{}) t)
                          (update t (fnil conj #{}) s)))
                    {}
                    edges)]
    (if (empty? nodes)
      #{}
      (let [state (atom {:time 0
                         :disc {}
                         :low {}
                         :parent {}
                         :bridges #{}})]
        (letfn [(dfs [u]
                  (let [t (:time @state)]
                    (swap! state assoc-in [:disc u] t)
                    (swap! state assoc-in [:low u] t)
                    (swap! state update :time inc)

                    (doseq [v (adj u #{})]
                      (cond
                        (not (contains? (:disc @state) v))
                        (do
                          (swap! state assoc-in [:parent v] u)
                          (dfs v)
                          (swap! state assoc-in [:low u]
                                 (min (get-in @state [:low u])
                                      (get-in @state [:low v])))
                          (when (> (get-in @state [:low v])
                                   (get-in @state [:disc u]))
                            (swap! state update :bridges conj
                                   (if (< u v) [u v] [v u]))))

                        (not= v (get-in @state [:parent u]))
                        (swap! state assoc-in [:low u]
                               (min (get-in @state [:low u])
                                    (get-in @state [:disc v])))))))]
          (doseq [n nodes]
            (when-not (contains? (:disc @state) n)
              (dfs n)))
          (:bridges @state))))))

;; =============================================================================
;; PageRank
;; =============================================================================
;; Incremental potential: ✅ Good - delta propagation

(defn page-rank
  "Compute PageRank scores for all nodes.

   Options:
     :damping - damping factor (default 0.85)
     :iterations - max iterations (default 20)
     :tolerance - convergence threshold (default 1e-6)

   Returns a map of {entity-id -> score}.

   Incremental note: ✅ Good candidate for MVM
   - Adding edge (u,v): propagate fraction of u's rank to v
   - Removing edge: subtract and re-propagate"
  [db edge-attr & {:keys [damping iterations tolerance]
                   :or {damping 0.85 iterations 20 tolerance 1e-6}}]
  (let [edges (d/q '[:find ?s ?t
                     :in $ ?attr
                     :where [?s ?attr ?t]]
                   db edge-attr)
        nodes (into #{} (mapcat identity edges))
        n (count nodes)]
    (when (pos? n)
      (let [;; Build forward adjacency (who does node point to)
            out-links (reduce (fn [m [s t]] (update m s (fnil conj #{}) t)) {} edges)
            out-degree (reduce-kv (fn [m k v] (assoc m k (count v))) {} out-links)
            ;; Build reverse adjacency (who points to node) - O(edges) once instead of O(nodes×edges)
            in-links (reduce (fn [m [s t]] (update m t (fnil conj #{}) s)) {} edges)
            initial-score (/ 1.0 n)
            base-score (/ (- 1.0 damping) n)]
        (loop [scores (zipmap nodes (repeat initial-score))
               iter 0]
          (if (>= iter iterations)
            scores
            (let [new-scores
                  (reduce
                   (fn [acc node]
                     (let [;; Sum contributions from all nodes that point to this node
                           incoming-sum
                           (reduce
                            (fn [sum src]
                              (+ sum (/ (scores src) (out-degree src))))
                            0.0
                            (in-links node #{}))]
                       (assoc acc node (+ base-score (* damping incoming-sum)))))
                   {}
                   nodes)
                  max-diff (apply max (map #(Math/abs (double (- (scores %) (new-scores %)))) nodes))]
              (if (< max-diff tolerance)
                new-scores
                (recur new-scores (inc iter))))))))))

;; =============================================================================
;; Weighted/Semiring Path Computations
;; =============================================================================
;; Different semirings for different use cases:
;; - Probabilities: (×, max, [0,1]) - belief propagation
;; - Costs: (+, min, R+) - shortest path
;; - Capacities: (min, max, R+) - bottleneck/max-flow
;; - Trust: (×d, max, [0,1]) - reputation systems

(defn bottleneck-path
  "Find path with maximum bottleneck (minimum edge along path is maximized).

   Use for: bandwidth, capacity, weakest-link problems.

   Returns {:path [...] :capacity N} or nil."
  [db source edge-attr capacity-attr target]
  (loop [frontier (sorted-set-by (fn [[c1 _ _] [c2 _ _]] (compare c2 c1))
                                 [Double/MAX_VALUE source [source]])
         visited #{}]
    (when (seq frontier)
      (let [[capacity eid path] (first frontier)
            frontier' (disj frontier (first frontier))]
        (cond
          (= eid target)
          {:path path :capacity capacity}

          (visited eid)
          (recur frontier' visited)

          :else
          (let [edges (d/q '[:find ?t ?c
                             :in $ ?e ?ea ?ca
                             :where [?e ?ea ?t] [?e ?ca ?c]]
                           db eid edge-attr capacity-attr)
                new-frontier
                (reduce
                 (fn [f [t c]]
                   (if (visited t)
                     f
                     (conj f [(min capacity (or c Double/MAX_VALUE)) t (conj path t)])))
                 frontier'
                 edges)]
            (recur new-frontier (conj visited eid))))))))

(defn trust-propagation
  "Propagate trust scores from source through the graph.
   Uses multiplicative discount model: trust decreases with each hop.

   Options:
     :discount - per-hop discount factor (default 0.9)

   Returns map of {entity-id -> trust-score}."
  [db source edge-attr trust-attr & {:keys [discount] :or {discount 0.9}}]
  (loop [trust-scores {source 1.0}
         frontier #{source}]
    (if (empty? frontier)
      trust-scores
      (let [expansions
            (for [node frontier
                  [target edge-trust] (d/q '[:find ?t ?tr
                                             :in $ ?e ?ea ?ta
                                             :where [?e ?ea ?t] [?e ?ta ?tr]]
                                           db node edge-attr trust-attr)
                  :let [propagated (* (trust-scores node) discount (or edge-trust 1.0))]
                  :when (> propagated (get trust-scores target 0))]
              [target propagated])
            new-trust (reduce (fn [m [t s]] (assoc m t (max s (get m t 0))))
                              trust-scores
                              expansions)
            new-frontier (into #{} (map first expansions))]
        (recur new-trust new-frontier)))))

(defn probability-reachability
  "Compute probability of reaching target from source.
   Assumes edge weights are independent probabilities.
   Returns max path probability (simplified independence assumption).

   For proper probabilistic inference, consider using a proper
   probabilistic programming framework."
  [db source edge-attr prob-attr target]
  (let [paths (all-paths db source edge-attr {:target-eid target :max-depth 5})]
    (if (empty? paths)
      0.0
      (apply max
             (for [path paths]
               (reduce
                (fn [prob [from _to]]
                  (* prob (or (d/q '[:find ?w . :in $ ?e ?wa :where [?e ?wa ?w]]
                                   db from prob-attr)
                              1.0)))
                1.0
                (partition 2 1 path)))))))

;; =============================================================================
;; Summary: Incremental Maintenance Potential
;; =============================================================================
;;
;; ✅ = Good candidate for differential dataflow / MVM
;; ⚠️ = Asymmetric (add easy, delete hard)
;; ❌ = Likely needs specialized algorithm
;;
;; | Algorithm              | Add-Only | Full MVM | Notes                    |
;; |------------------------|----------|----------|--------------------------|
;; | transitive-closure     | ✅       | ⚠️       | Delete needs path check  |
;; | reachable?             | ✅       | ⚠️       | Same as TC               |
;; | shortest-path          | ⚠️       | ❌       | Shorter path invalidates |
;; | connected-component    | ✅       | ⚠️       | Delete may split         |
;; | all-connected-comps    | ✅       | ⚠️       | Same as CC               |
;; | strongly-connected     | ⚠️       | ❌       | Delete may split SCC     |
;; | topological-sort       | ✅       | ✅       | If remains DAG           |
;; | has-cycle?             | ✅       | ✅       | Just check new edge      |
;; | bipartite-coloring     | ✅       | ✅       | Just check new edge      |
;; | find-bridges           | ⚠️       | ❌       | Complex 2-edge-conn      |
;; | PageRank (future)      | ✅       | ✅       | Delta propagation        |

;; =============================================================================
;; Centrality Measures
;; =============================================================================

(defn closeness-centrality
  "Compute closeness centrality for all nodes.
   Closeness is the reciprocal of the average shortest path distance to all other nodes.

   Higher scores indicate nodes that can reach others more efficiently.

   Uses: Emergency services placement, information dissemination, facility location.

   Returns map of {node -> score} where score is in [0, 1].
   Disconnected nodes get score 0."
  [db edge-attr]
  (let [edges (d/q '[:find ?s ?t
                     :in $ ?attr
                     :where [?s ?attr ?t]]
                   db edge-attr)
        nodes (into #{} (mapcat identity edges))
        n (count nodes)
        ;; Build undirected adjacency for BFS
        adj (reduce (fn [m [s t]]
                      (-> m
                          (update s (fnil conj #{}) t)
                          (update t (fnil conj #{}) s)))
                    {}
                    edges)]
    (when (> n 1)
      (into {}
            (for [source nodes]
          ;; BFS from source to compute distances
              (let [distances
                    (loop [queue (conj clojure.lang.PersistentQueue/EMPTY source)
                           dist {source 0}]
                      (if (empty? queue)
                        dist
                        (let [node (peek queue)
                              d (dist node)
                              neighbors (adj node #{})]
                          (let [[new-queue new-dist]
                                (reduce (fn [[q dmap] neighbor]
                                          (if (contains? dmap neighbor)
                                            [q dmap]
                                            [(conj q neighbor)
                                             (assoc dmap neighbor (inc d))]))
                                        [(pop queue) dist]
                                        neighbors)]
                            (recur new-queue new-dist)))))
                ;; Sum of distances to reachable nodes
                    reachable (dec (count distances))
                    total-dist (reduce + 0 (vals distances))]
            ;; Closeness = (reachable / (n-1)) * (reachable / total-dist)
            ;; This is normalized closeness that handles disconnected graphs
                [source (if (and (pos? reachable) (pos? total-dist))
                          (* (/ reachable (dec n))
                             (/ reachable total-dist))
                          0.0)]))))))

(defn betweenness-centrality
  "Compute betweenness centrality for all nodes.
   Betweenness measures how often a node lies on shortest paths between other nodes.

   Higher scores indicate nodes that act as bridges or brokers in the network.

   Uses: Finding network bottlenecks, identifying key information brokers,
         organizational analysis.

   Returns map of {node -> score} normalized by (n-1)(n-2)/2.
   Uses Brandes' algorithm O(VE)."
  [db edge-attr]
  (let [edges (d/q '[:find ?s ?t
                     :in $ ?attr
                     :where [?s ?attr ?t]]
                   db edge-attr)
        nodes (vec (into #{} (mapcat identity edges)))
        n (count nodes)
        ;; Build undirected adjacency
        adj (reduce (fn [m [s t]]
                      (-> m
                          (update s (fnil conj #{}) t)
                          (update t (fnil conj #{}) s)))
                    {}
                    edges)]
    (when (> n 2)
      (let [;; Initialize betweenness scores
            initial-bc (zipmap nodes (repeat 0.0))
            ;; Run BFS from each source (Brandes' algorithm)
            bc (reduce
                (fn [betweenness source]
                   ;; BFS phase - compute distances and shortest path counts
                  (let [[stack pred sigma dist]
                        (loop [queue (conj clojure.lang.PersistentQueue/EMPTY source)
                               stack []
                               pred (zipmap nodes (repeat []))
                               sigma (assoc (zipmap nodes (repeat 0)) source 1)
                               dist (assoc (zipmap nodes (repeat -1)) source 0)]
                          (if (empty? queue)
                            [stack pred sigma dist]
                            (let [v (peek queue)
                                  queue' (pop queue)
                                  dv (dist v)]
                              (let [[q' p' s' d']
                                    (reduce
                                     (fn [[q p s d] w]
                                       (cond
                                           ;; w found for first time
                                         (neg? (d w))
                                         [(conj q w)
                                          (update p w conj v)
                                          (update s w + (s v))
                                          (assoc d w (inc dv))]

                                           ;; Another shortest path to w through v
                                         (= (d w) (inc dv))
                                         [q
                                          (update p w conj v)
                                          (update s w + (s v))
                                          d]

                                         :else [q p s d]))
                                     [queue' pred sigma dist]
                                     (adj v #{}))]
                                (recur q' (conj stack v) p' s' d')))))
                         ;; Accumulation phase - back-propagate dependencies
                        delta (zipmap nodes (repeat 0.0))
                        delta' (reduce
                                (fn [d w]
                                  (reduce
                                   (fn [d' v]
                                     (let [coeff (/ (sigma v) (sigma w))
                                           contrib (* coeff (+ 1 (d w)))]
                                       (update d' v + contrib)))
                                   d
                                   (pred w)))
                                delta
                                (reverse stack))]
                     ;; Add delta to betweenness (except source)
                    (reduce (fn [bc' v]
                              (if (= v source)
                                bc'
                                (update bc' v + (delta' v))))
                            betweenness
                            nodes)))
                initial-bc
                nodes)
            ;; Normalize by (n-1)(n-2)/2 for undirected graphs
            norm (/ (* (dec n) (- n 2)) 2.0)]
        (reduce-kv (fn [m k v] (assoc m k (/ v norm))) {} bc)))))

;; =============================================================================
;; Similarity Algorithms
;; =============================================================================

(defn jaccard-similarity
  "Compute Jaccard similarity between two nodes based on shared neighbors.

   Jaccard = |intersection(neighbors(a), neighbors(b))| / |union(neighbors(a), neighbors(b))|

   Returns a value in [0, 1] where 1 means identical neighbor sets."
  [db node-a node-b edge-attr]
  (let [neighbors-a (into #{} (d/q '[:find [?t ...]
                                     :in $ ?s ?attr
                                     :where [?s ?attr ?t]]
                                   db node-a edge-attr))
        neighbors-b (into #{} (d/q '[:find [?t ...]
                                     :in $ ?s ?attr
                                     :where [?s ?attr ?t]]
                                   db node-b edge-attr))
        intersection (count (set/intersection neighbors-a neighbors-b))
        union (count (set/union neighbors-a neighbors-b))]
    (if (zero? union)
      0.0
      (/ intersection union))))

(defn node-similarity
  "Find nodes similar to the given node based on shared neighbors (Jaccard).

   Returns map of {other-node -> similarity-score} sorted by score descending.

   Options:
     :min-similarity - minimum score threshold (default 0.0)
     :top-k - return only top K results (default: all)"
  [db node edge-attr & {:keys [min-similarity top-k]
                        :or {min-similarity 0.0}}]
  (let [;; Get all nodes
        edges (d/q '[:find ?s ?t
                     :in $ ?attr
                     :where [?s ?attr ?t]]
                   db edge-attr)
        all-nodes (disj (into #{} (mapcat identity edges)) node)
        ;; Compute neighbors once
        node-neighbors (into #{} (d/q '[:find [?t ...]
                                        :in $ ?s ?attr
                                        :where [?s ?attr ?t]]
                                      db node edge-attr))
        ;; Build adjacency for efficiency
        adj (reduce (fn [m [s t]] (update m s (fnil conj #{}) t)) {} edges)
        ;; Compute similarities
        similarities
        (for [other all-nodes
              :let [other-neighbors (adj other #{})
                    intersection (count (set/intersection node-neighbors other-neighbors))
                    union (count (set/union node-neighbors other-neighbors))
                    sim (if (zero? union) 0.0 (double (/ intersection union)))]
              :when (>= sim min-similarity)]
          [other sim])
        sorted (sort-by second > similarities)]
    (into {} (if top-k (take top-k sorted) sorted))))

;; =============================================================================
;; Community Detection
;; =============================================================================

(defn community-stats
  "Compute statistics for a community assignment map.

   Input: {node -> community-label} map

   Returns:
     :num-communities - total number of distinct communities
     :sizes - {community -> member-count}
     :largest - [community size] of largest community
     :smallest - [community size] of smallest community
     :avg-size - average community size
     :isolation-score - fraction of singleton communities (potential anomalies)"
  [communities]
  (let [by-comm (group-by val communities)
        sizes (reduce-kv (fn [m comm members]
                           (assoc m comm (count members)))
                         {} by-comm)
        sorted-sizes (sort-by val > sizes)
        n (count sizes)
        singletons (count (filter #(= 1 (val %)) sizes))]
    {:num-communities n
     :sizes sizes
     :largest (first sorted-sizes)
     :smallest (last sorted-sizes)
     :avg-size (if (pos? n) (/ (count communities) n) 0)
     :isolation-score (if (pos? n) (double (/ singletons n)) 0.0)}))

(defn label-propagation
  "Fast community detection using label propagation.
   Each node adopts the most frequent label among its neighbors.

   Uses: Fast clustering, semi-supervised learning with seed labels,
         anomaly detection (non-convergence indicates unstable structure).

   Options:
     :max-iterations - stop after N iterations (default 10)
     :seeds - initial labels for specific nodes {node -> label}

   Returns:
     :communities - {node -> community-label}
     :converged - true if stabilized before max-iterations
     :iterations - number of iterations run
     :stats - community statistics (sizes, counts, isolation score)"
  [db edge-attr & {:keys [max-iterations seeds]
                   :or {max-iterations 10 seeds {}}}]
  (let [edges (d/q '[:find ?s ?t
                     :in $ ?attr
                     :where [?s ?attr ?t]]
                   db edge-attr)
        nodes (vec (into #{} (mapcat identity edges)))
        ;; Build undirected adjacency
        adj (reduce (fn [m [s t]]
                      (-> m
                          (update s (fnil conj #{}) t)
                          (update t (fnil conj #{}) s)))
                    {}
                    edges)
        ;; Initialize labels - seeds take priority, else unique per node
        initial-labels (merge (zipmap nodes nodes) seeds)
        ;; Seed nodes cannot change
        seed-nodes (set (keys seeds))]
    (loop [labels initial-labels
           iter 0]
      (if (>= iter max-iterations)
        {:communities labels
         :converged false
         :iterations iter
         :stats (community-stats labels)}
        ;; Process nodes in random order (shuffle for randomization)
        (let [shuffled (shuffle nodes)
              [new-labels changed?]
              (reduce
               (fn [[lbls changed] node]
                 (if (seed-nodes node)
                   [lbls changed]  ;; Don't change seed nodes
                   (let [neighbor-labels (map lbls (adj node #{}))
                          ;; Most frequent label among neighbors
                         freqs (frequencies neighbor-labels)
                         max-freq (when (seq freqs) (apply max (vals freqs)))
                         best-labels (when max-freq
                                       (for [[l f] freqs :when (= f max-freq)] l))
                          ;; Pick one (randomly if tie)
                         new-label (when (seq best-labels)
                                     (rand-nth (vec best-labels)))
                         current-label (lbls node)]
                     (if (and new-label (not= new-label current-label))
                       [(assoc lbls node new-label) true]
                       [lbls changed]))))
               [labels false]
               shuffled)]
          (if changed?
            (recur new-labels (inc iter))
            {:communities new-labels
             :converged true
             :iterations (inc iter)
             :stats (community-stats new-labels)}))))))

(defn louvain
  "Fast hierarchical Louvain community detection with O(m) per pass.

   Uses delta-modularity formula with hash tables for O(1) community lookups,
   plus hierarchical graph contraction for multi-level optimization.

   Options:
     :resolution - higher values produce more communities (default 1.0)
     :max-iterations - max passes per level (default 10)
     :max-levels - max hierarchy levels (default 10)
     :min-modularity-gain - stop if improvement < this (default 0.0001)

   Returns:
     :communities - {original-node -> community-id}
     :modularity - partition quality score Q
     :iterations - total optimization passes across all levels
     :levels - number of hierarchy levels used
     :stats - community statistics"
  [db edge-attr & {:keys [resolution max-iterations max-levels min-modularity-gain]
                   :or {resolution 1.0 max-iterations 10 max-levels 10 min-modularity-gain 0.0001}}]
  (let [;; Load edges from database
        db-edges (d/q '[:find ?s ?t
                        :in $ ?attr
                        :where [?s ?attr ?t]]
                      db edge-attr)

        ;; Build weighted adjacency: {node -> {neighbor -> weight}}
        ;; For unweighted graphs, all weights are 1
        build-weighted-adj
        (fn [edges]
          (reduce (fn [m [s t w]]
                    (let [weight (or w 1)]
                      (-> m
                          (update-in [s t] (fnil + 0) weight)
                          (update-in [t s] (fnil + 0) weight))))
                  {}
                  edges))

        initial-adj (build-weighted-adj (map #(conj % 1) db-edges))
        all-nodes (vec (keys initial-adj))

        ;; Total edge weight (2m in modularity formula, since undirected)
        total-weight (reduce + (for [[_ neighbors] initial-adj
                                     [_ w] neighbors]
                                 w))
        m2 (double total-weight)]

    (if (or (empty? all-nodes) (zero? total-weight))
      {:communities {}
       :modularity 0.0
       :iterations 0
       :levels 0
       :stats {:num-communities 0 :sizes {} :largest nil :smallest nil :avg-size 0 :isolation-score 0.0}}

      (let [;; Compute node degree (sum of edge weights)
            node-degree (fn [adj node]
                          (reduce + (vals (adj node {}))))

            ;; One level of Louvain optimization
            ;; Returns [new-community-map improved? iterations]
            optimize-level
            (fn [adj nodes]
              (let [;; Initial: each node in own community
                    init-comm (zipmap nodes nodes)

                    ;; Community stats: {comm -> {:sum-degree D, :internal-weight W}}
                    init-stats (into {}
                                     (for [n nodes]
                                       [n {:sum-degree (node-degree adj n)
                                           :internal-weight 0.0}]))

                    ;; Delta modularity for moving node to target community
                    ;; ΔQ = [k_i,in / m] - [2 * Σ_tot * k_i * resolution / (2m)²]
                    delta-q
                    (fn [node target-comm comm-map comm-stats adj]
                      (let [ki (node-degree adj node)
                            ;; Sum of edge weights from node to target community
                            ki-in (reduce + (for [[neighbor weight] (adj node {})
                                                  :when (= target-comm (comm-map neighbor))]
                                              weight))
                            ;; Total degree of target community
                            sigma-tot (:sum-degree (comm-stats target-comm) 0)]
                        (- (/ ki-in m2)
                           (/ (* 2.0 sigma-tot ki resolution) (* m2 m2)))))

                    ;; Move node: update community map and stats
                    move-node
                    (fn [node old-comm new-comm comm-map comm-stats adj]
                      (let [ki (node-degree adj node)
                            ;; Edges to old community (will become external)
                            ki-old (reduce + (for [[neighbor weight] (adj node {})
                                                   :when (and (= old-comm (comm-map neighbor))
                                                              (not= neighbor node))]
                                               weight))
                            ;; Edges to new community (will become internal)
                            ki-new (reduce + (for [[neighbor weight] (adj node {})
                                                   :when (= new-comm (comm-map neighbor))]
                                               weight))
                            ;; Update stats
                            new-stats (-> comm-stats
                                          (update-in [old-comm :sum-degree] - ki)
                                          (update-in [old-comm :internal-weight] - ki-old)
                                          (update-in [new-comm :sum-degree] (fnil + 0) ki)
                                          (update-in [new-comm :internal-weight] (fnil + 0) ki-new))]
                        [(assoc comm-map node new-comm) new-stats]))

                    ;; One pass over all nodes
                    one-pass
                    (fn [[comm-map comm-stats moved-count]]
                      (reduce
                       (fn [[cm cs mc] node]
                         (let [current-comm (cm node)
                                ;; Find neighboring communities and their delta-Q
                               neighbor-comms (distinct
                                               (for [[neighbor _] (adj node {})
                                                     :let [nc (cm neighbor)]
                                                     :when (not= nc current-comm)]
                                                 nc))
                                ;; Find best community to move to
                               [best-comm best-gain]
                               (reduce
                                (fn [[bc bg] nc]
                                  (let [gain (delta-q node nc cm cs adj)]
                                    (if (> gain bg) [nc gain] [bc bg])))
                                [current-comm 0.0]
                                neighbor-comms)]
                           (if (> best-gain 0.0)
                             (let [[new-cm new-cs] (move-node node current-comm best-comm cm cs adj)]
                               [new-cm new-cs (inc mc)])
                             [cm cs mc])))
                       [comm-map comm-stats moved-count]
                       (shuffle nodes)))]

                ;; Iterate passes until convergence
                (loop [comm-map init-comm
                       comm-stats init-stats
                       iter 0]
                  (if (>= iter max-iterations)
                    [comm-map iter]
                    (let [[new-cm new-cs moved] (one-pass [comm-map comm-stats 0])]
                      (if (zero? moved)
                        [comm-map (inc iter)]
                        (recur new-cm new-cs (inc iter))))))))

            ;; Contract graph: merge communities into super-nodes
            contract-graph
            (fn [adj comm-map]
              (let [;; Group nodes by community
                    by-comm (group-by comm-map (keys adj))
                    ;; Build super-node adjacency
                    super-adj
                    (reduce-kv
                     (fn [sa comm members]
                       (let [;; Collect edges from this community to others
                             external-edges
                             (for [node members
                                   [neighbor weight] (adj node {})
                                   :let [neighbor-comm (comm-map neighbor)]
                                   :when (not= comm neighbor-comm)]
                               [neighbor-comm weight])
                              ;; Sum weights by target community
                             edge-weights (reduce (fn [m [c w]]
                                                    (update m c (fnil + 0) w))
                                                  {}
                                                  external-edges)]
                         (if (empty? edge-weights)
                           sa
                           (assoc sa comm edge-weights))))
                     {}
                     by-comm)]
                [super-adj (vec (keys by-comm))]))

            ;; Compute modularity for final partition
            compute-modularity
            (fn [adj comm-map]
              (let [nodes (keys adj)]
                (reduce
                 (fn [q node]
                   (let [ki (node-degree adj node)
                         comm (comm-map node)]
                     (reduce
                      (fn [q2 [neighbor weight]]
                        (if (= comm (comm-map neighbor))
                          (let [kj (node-degree adj neighbor)
                                expected (/ (* ki kj) m2)]
                            (+ q2 (/ (- weight expected) m2)))
                          q2))
                      q
                      (adj node {}))))
                 0.0
                 nodes)))

            ;; Run hierarchical optimization
            [final-comm total-iters levels]
            (loop [adj initial-adj
                   nodes all-nodes
                   ;; Maps super-node back to original nodes
                   node-mapping (zipmap all-nodes (map vector all-nodes))
                   level 0
                   total-iters 0
                   prev-modularity -1.0]
              (if (or (>= level max-levels)
                      (<= (count nodes) 1))
                ;; Done: build final community map
                (let [super-comm (zipmap nodes (range))
                      final-map (into {}
                                      (for [[super originals] node-mapping
                                            orig originals]
                                        [orig (super-comm super)]))]
                  [final-map total-iters level])

                ;; Optimize this level
                (let [[level-comm iters] (optimize-level adj nodes)
                      ;; Contract to super-graph
                      [super-adj super-nodes] (contract-graph adj level-comm)
                      ;; Update node mapping
                      new-mapping (reduce-kv
                                   (fn [m node comm]
                                     (update m comm (fnil into [])
                                             (node-mapping node [])))
                                   {}
                                   level-comm)
                      ;; Check modularity improvement
                      current-mod (compute-modularity adj level-comm)
                      improvement (- current-mod prev-modularity)]
                  (if (and (> level 0)
                           (< improvement min-modularity-gain))
                    ;; No significant improvement, stop
                    (let [final-map (into {}
                                          (for [[super originals] node-mapping
                                                orig originals]
                                            [orig (level-comm super super)]))]
                      [final-map total-iters level])
                    ;; Continue to next level
                    (recur super-adj
                           super-nodes
                           new-mapping
                           (inc level)
                           (+ total-iters iters)
                           current-mod)))))

            ;; Renumber communities to be contiguous 0, 1, 2, ...
            comm-ids (distinct (vals final-comm))
            comm-renumber (zipmap comm-ids (range))
            final-communities (into {} (for [[n c] final-comm]
                                         [n (comm-renumber c)]))]

        {:communities final-communities
         :modularity (compute-modularity initial-adj final-comm)
         :iterations total-iters
         :levels levels
         :stats (community-stats final-communities)}))))

;; =============================================================================
;; A* Search Algorithm
;; =============================================================================

(defn astar-path
  "Find shortest path from source to target using A* algorithm with heuristic.

   The heuristic function (heuristic-fn node target) should return an estimate
   of the distance from node to target. For optimal results, it should be
   admissible (never overestimate).

   Parameters:
     db - Datahike database
     source - Starting node entity ID
     edge-attr - Attribute for edges (e.g., :connects-to)
     target - Target node entity ID
     heuristic-fn - Function (fn [node target] estimated-cost)

   Options:
     :weight-attr - Attribute for edge weights (default: all edges weight 1)

   Returns {:path [...] :cost N} or nil if no path exists.

   Use cases: Geographic routing, game pathfinding, puzzle solving."
  [db source edge-attr target heuristic-fn & {:keys [weight-attr]}]
  (if (= source target)
    {:path [source] :cost 0}
    (let [;; Pre-load all edges
          edges (if weight-attr
                  (d/q '[:find ?s ?t ?w
                         :in $ ?ea ?wa
                         :where [?s ?ea ?t] [?s ?wa ?w]]
                       db edge-attr weight-attr)
                  (d/q '[:find ?s ?t
                         :in $ ?ea
                         :where [?s ?ea ?t]]
                       db edge-attr))
          ;; Build weighted adjacency {node -> {neighbor -> weight}}
          adj (if weight-attr
                (reduce (fn [m [s t w]]
                          (update m s (fnil assoc {}) t (or w 1)))
                        {}
                        edges)
                (reduce (fn [m [s t]]
                          (update m s (fnil assoc {}) t 1))
                        {}
                        edges))]
      ;; A* search with priority queue
      ;; Queue entries: [f-score g-score node path]
      ;; f = g + h, where g = actual cost, h = heuristic
      (loop [;; Priority queue sorted by f-score
             open-set (sorted-set-by
                       (fn [[f1 _ n1 _] [f2 _ n2 _]]
                         (let [c (compare f1 f2)]
                           (if (zero? c) (compare n1 n2) c)))
                       [(heuristic-fn source target) 0 source [source]])
             ;; Best g-score seen for each node
             g-scores {source 0}
             ;; Closed set
             closed #{}]
        (when (seq open-set)
          (let [[f-score g-score node path] (first open-set)
                open-set' (disj open-set (first open-set))]
            (cond
              ;; Found target
              (= node target)
              {:path path :cost g-score}

              ;; Already processed with better score
              (closed node)
              (recur open-set' g-scores closed)

              ;; Expand node
              :else
              (let [neighbors (adj node {})
                    [new-open new-g]
                    (reduce
                     (fn [[open g-map] [neighbor edge-weight]]
                       (let [tentative-g (+ g-score edge-weight)]
                         (if (and (not (closed neighbor))
                                  (< tentative-g (get g-map neighbor Double/MAX_VALUE)))
                           (let [h (heuristic-fn neighbor target)
                                 f (+ tentative-g h)]
                             [(conj open [f tentative-g neighbor (conj path neighbor)])
                              (assoc g-map neighbor tentative-g)])
                           [open g-map])))
                     [open-set' g-scores]
                     neighbors)]
                (recur new-open new-g (conj closed node))))))))))

;; =============================================================================
;; Minimum Spanning Tree (Prim's Algorithm)
;; =============================================================================

(defn prim-mst
  "Find minimum spanning tree using Prim's algorithm.

   For undirected weighted graphs. Edges are treated as undirected
   (both directions considered).

   Parameters:
     db - Datahike database
     from-attr - Attribute for source node on edge entity
     to-attr - Attribute for target node on edge entity
     weight-attr - Attribute for edge weights

   Options:
     :start - Starting node (default: arbitrary node)

   Returns {:edges [[n1 n2 weight] ...] :total-weight N}
           or nil if graph is empty.

   Use cases: Network design, clustering, approximation algorithms."
  [db from-attr to-attr weight-attr & {:keys [start]}]
  (let [;; Pre-load all edges with weights (reified edge entities)
        edges (d/q '[:find ?s ?t ?w
                     :in $ ?fa ?ta ?wa
                     :where [?e ?fa ?s] [?e ?ta ?t] [?e ?wa ?w]]
                   db from-attr to-attr weight-attr)
        ;; Build undirected weighted adjacency
        adj (reduce (fn [m [s t w]]
                      (-> m
                          (update s (fnil assoc {}) t w)
                          (update t (fnil assoc {}) s w)))
                    {}
                    edges)
        all-nodes (set (keys adj))]
    (when (seq all-nodes)
      (let [start-node (or start (first all-nodes))]
        ;; Prim's algorithm with priority queue
        ;; Queue entries: [weight from-node to-node]
        (loop [;; MST edges collected
               mst-edges []
               total-weight 0
               ;; Nodes in MST
               in-mst #{start-node}
               ;; Priority queue of candidate edges
               candidates (into (sorted-set)
                                (for [[neighbor weight] (adj start-node {})]
                                  [weight start-node neighbor]))]
          (if (or (empty? candidates) (= (count in-mst) (count all-nodes)))
            {:edges mst-edges :total-weight total-weight}
            (let [[weight from to] (first candidates)
                  candidates' (disj candidates (first candidates))]
              (if (in-mst to)
                ;; Skip - already in MST
                (recur mst-edges total-weight in-mst candidates')
                ;; Add edge to MST
                (let [new-candidates
                      (into candidates'
                            (for [[neighbor w] (adj to {})
                                  :when (not (in-mst neighbor))]
                              [w to neighbor]))]
                  (recur (conj mst-edges [from to weight])
                         (+ total-weight weight)
                         (conj in-mst to)
                         new-candidates))))))))))

(defn mst-weight
  "Returns just the total weight of the minimum spanning tree."
  [db from-attr to-attr weight-attr & opts]
  (:total-weight (apply prim-mst db from-attr to-attr weight-attr opts)))

;; =============================================================================
;; Maximum Flow (Edmonds-Karp Algorithm)
;; =============================================================================

(defn max-flow
  "Find maximum flow from source to sink using Edmonds-Karp algorithm.

   This is the BFS-based Ford-Fulkerson method, which runs in O(VE²).

   Parameters:
     db - Datahike database
     source - Source node entity ID
     from-attr - Attribute for source node on edge entity
     to-attr - Attribute for target node on edge entity
     capacity-attr - Attribute for edge capacities
     sink - Sink node entity ID

   Returns {:flow N :flow-map {[from to] flow-value ...}}
           where flow-map shows flow on each edge.

   Use cases: Network capacity, bipartite matching, assignment problems."
  [db source from-attr to-attr capacity-attr sink]
  (let [;; Pre-load all edges with capacities (reified edge entities)
        edges (d/q '[:find ?s ?t ?c
                     :in $ ?fa ?ta ?ca
                     :where [?e ?fa ?s] [?e ?ta ?t] [?e ?ca ?c]]
                   db from-attr to-attr capacity-attr)
        ;; Build capacity map {[from to] -> capacity}
        capacity (reduce (fn [m [s t c]]
                           (assoc m [s t] (or c 0)))
                         {}
                         edges)
        ;; Get all nodes
        all-nodes (into #{} (mapcat (fn [[s t _]] [s t]) edges))
        ;; Build adjacency for BFS (including reverse edges for residual)
        adj (reduce (fn [m [s t _]]
                      (-> m
                          (update s (fnil conj #{}) t)
                          (update t (fnil conj #{}) s)))  ; reverse edge
                    {}
                    edges)

        ;; BFS to find augmenting path in residual graph
        find-path (fn [flow-map]
                    (loop [queue (conj clojure.lang.PersistentQueue/EMPTY source)
                           parent {source nil}]
                      (if (empty? queue)
                        nil  ; No path found
                        (let [node (peek queue)]
                          (if (= node sink)
                            ;; Reconstruct path
                            (loop [path [] n sink]
                              (if (nil? (parent n))
                                (vec (reverse path))
                                (recur (conj path [(parent n) n]) (parent n))))
                            ;; Explore neighbors
                            (let [neighbors (adj node #{})
                                  [queue' parent']
                                  (reduce
                                   (fn [[q p] neighbor]
                                     (let [cap (get capacity [node neighbor] 0)
                                           rev-flow (get flow-map [neighbor node] 0)
                                           fwd-flow (get flow-map [node neighbor] 0)
                                           residual (+ (- cap fwd-flow) rev-flow)]
                                       (if (and (not (contains? p neighbor))
                                                (pos? residual))
                                         [(conj q neighbor) (assoc p neighbor node)]
                                         [q p])))
                                   [(pop queue) parent]
                                   neighbors)]
                              (recur queue' parent')))))))

        ;; Find bottleneck capacity along path
        path-capacity (fn [path flow-map]
                        (reduce
                         (fn [min-cap [from to]]
                           (let [cap (get capacity [from to] 0)
                                 rev-flow (get flow-map [to from] 0)
                                 fwd-flow (get flow-map [from to] 0)
                                 residual (+ (- cap fwd-flow) rev-flow)]
                             (min min-cap residual)))
                         Double/MAX_VALUE
                         path))]

    ;; Main loop: find augmenting paths until none exist
    (loop [flow-map {}
           total-flow 0]
      (if-let [path (find-path flow-map)]
        (let [path-cap (path-capacity path flow-map)
              ;; Update flow along path
              new-flow-map
              (reduce
               (fn [fm [from to]]
                 (let [fwd-cap (get capacity [from to] 0)]
                   (if (pos? fwd-cap)
                      ;; Forward edge - add flow
                     (update fm [from to] (fnil + 0) path-cap)
                      ;; This is a reverse edge - subtract from reverse
                     (update fm [to from] (fnil - 0) path-cap))))
               flow-map
               path)]
          (recur new-flow-map (+ total-flow path-cap)))
        ;; No more augmenting paths
        {:flow total-flow
         :flow-map (into {} (filter (fn [[_ v]] (pos? v)) flow-map))}))))

(defn min-cut
  "Find minimum cut (maximum flow) between source and sink.
   Returns just the max-flow value."
  [db source from-attr to-attr capacity-attr sink]
  (:flow (max-flow db source from-attr to-attr capacity-attr sink)))

;; =============================================================================
;; Link Prediction Algorithms
;; =============================================================================
;;
;; These algorithms predict the likelihood of a link forming between two nodes
;; based on the graph structure. Useful for:
;; - Recommendation systems
;; - Knowledge graph completion
;; - Social network friend suggestions
;; - Biological interaction prediction

(defn neighbors
  "Returns the set of neighbors of a node via the given edge attribute.

   Parameters:
     db - Datahike database
     node - Node entity ID
     edge-attr - Attribute representing edges

   Options:
     :direction - :outgoing (default), :incoming, or :both"
  [db node edge-attr & {:keys [direction] :or {direction :outgoing}}]
  (case direction
    :outgoing (set (d/q '[:find [?t ...]
                          :in $ ?n ?ea
                          :where [?n ?ea ?t]]
                        db node edge-attr))
    :incoming (set (d/q '[:find [?s ...]
                          :in $ ?n ?ea
                          :where [?s ?ea ?n]]
                        db node edge-attr))
    :both (clojure.set/union
           (neighbors db node edge-attr :direction :outgoing)
           (neighbors db node edge-attr :direction :incoming))))

(defn degree
  "Returns the degree of a node (number of edges).

   Options:
     :direction - :outgoing (default), :incoming, or :both"
  [db node edge-attr & {:keys [direction] :or {direction :outgoing}}]
  (count (neighbors db node edge-attr :direction direction)))

(defn common-neighbors-set
  "Returns the set of common neighbors between two nodes.

   This is the foundation for most link prediction algorithms."
  [db a b edge-attr & {:keys [direction] :or {direction :outgoing}}]
  (clojure.set/intersection
   (neighbors db a edge-attr :direction direction)
   (neighbors db b edge-attr :direction direction)))

(defn common-neighbors
  "Returns the count of common neighbors between two nodes.

   CN(a,b) = |Γ(a) ∩ Γ(b)|

   Higher values indicate stronger likelihood of connection.
   Simple but effective baseline for link prediction."
  [db a b edge-attr & {:keys [direction] :or {direction :outgoing}}]
  (count (common-neighbors-set db a b edge-attr :direction direction)))

(defn adamic-adar
  "Adamic-Adar index for link prediction.

   AA(a,b) = Σ 1/log(|Γ(z)|) for z ∈ Γ(a) ∩ Γ(b)

   Weights common neighbors by the inverse log of their degree.
   Intuition: A shared friend with few connections is a stronger
   signal than a shared friend who knows everyone (celebrity effect).

   Returns 0.0 if no common neighbors exist."
  [db a b edge-attr & {:keys [direction] :or {direction :outgoing}}]
  (let [common (common-neighbors-set db a b edge-attr :direction direction)]
    (if (empty? common)
      0.0
      (reduce + (for [z common
                      :let [deg (degree db z edge-attr :direction direction)]
                      :when (> deg 1)]  ; log(1)=0 would cause division issues
                  (/ 1.0 (Math/log deg)))))))

(defn resource-allocation
  "Resource Allocation index for link prediction.

   RA(a,b) = Σ 1/|Γ(z)| for z ∈ Γ(a) ∩ Γ(b)

   Similar to Adamic-Adar but uses reciprocal of degree directly.
   Models 'resource flow' through common neighbors - a neighbor with
   degree 2 transmits 1/2 of resources, degree 100 transmits 1/100.

   Generally performs slightly better than Adamic-Adar in practice."
  [db a b edge-attr & {:keys [direction] :or {direction :outgoing}}]
  (let [common (common-neighbors-set db a b edge-attr :direction direction)]
    (if (empty? common)
      0.0
      (reduce + (for [z common
                      :let [deg (degree db z edge-attr :direction direction)]
                      :when (pos? deg)]
                  (/ 1.0 deg))))))

(defn preferential-attachment
  "Preferential Attachment score for link prediction.

   PA(a,b) = |Γ(a)| × |Γ(b)|

   Based on the 'rich get richer' phenomenon - high-degree nodes
   are more likely to attract new connections. Simple but effective
   for scale-free networks (social networks, web graphs)."
  [db a b edge-attr & {:keys [direction] :or {direction :outgoing}}]
  (* (degree db a edge-attr :direction direction)
     (degree db b edge-attr :direction direction)))

(defn total-neighbors
  "Total neighbors (union) between two nodes.

   TN(a,b) = |Γ(a) ∪ Γ(b)|

   Measures the combined 'social reach' of a potential connection."
  [db a b edge-attr & {:keys [direction] :or {direction :outgoing}}]
  (count (clojure.set/union
          (neighbors db a edge-attr :direction direction)
          (neighbors db b edge-attr :direction direction))))

(defn jaccard-index
  "Jaccard index for link prediction.

   JC(a,b) = |Γ(a) ∩ Γ(b)| / |Γ(a) ∪ Γ(b)|

   Normalized version of common neighbors. Returns value in [0,1].
   Useful when comparing nodes with very different degrees."
  [db a b edge-attr & {:keys [direction] :or {direction :outgoing}}]
  (let [na (neighbors db a edge-attr :direction direction)
        nb (neighbors db b edge-attr :direction direction)
        union-size (count (clojure.set/union na nb))]
    (if (zero? union-size)
      0.0
      (/ (double (count (clojure.set/intersection na nb))) union-size))))

(defn same-community
  "Check if two nodes belong to the same community.

   Returns 1.0 if same community, 0.0 otherwise.
   Requires nodes to have the community attribute set
   (typically from prior community detection)."
  [db a b community-attr]
  (let [comm-a (d/q '[:find ?c .
                      :in $ ?n ?ca
                      :where [?n ?ca ?c]]
                    db a community-attr)
        comm-b (d/q '[:find ?c .
                      :in $ ?n ?ca
                      :where [?n ?ca ?c]]
                    db b community-attr)]
    (if (and comm-a comm-b (= comm-a comm-b))
      1.0
      0.0)))

(defn link-prediction-candidates
  "Find candidate pairs for link prediction and score them.

   Only considers node pairs within 2 hops (sharing at least one neighbor),
   which is O(Σ degree²) rather than O(n²) for all pairs.

   Parameters:
     db - Datahike database
     edge-attr - Attribute representing edges
     score-fn - Scoring function (e.g., adamic-adar, common-neighbors)

   Options:
     :limit - Maximum number of results (default 100)
     :source-nodes - Only consider pairs involving these nodes
     :min-score - Minimum score threshold (default 0)

   Returns sequence of {:source :target :score} maps, sorted by score desc."
  [db edge-attr score-fn & {:keys [limit source-nodes min-score direction]
                            :or {limit 100 min-score 0 direction :outgoing}}]
  (let [;; Find all 2-hop pairs (candidates with at least 1 common neighbor)
        base-query '[:find ?a ?b
                     :in $ ?ea
                     :where
                     [?a ?ea ?z]
                     [?z ?ea ?b]
                     [(< ?a ?b)]      ; avoid duplicates (a,b) and (b,a)
                     (not [?a ?ea ?b])] ; no direct edge exists

        candidates (if source-nodes
                     ;; Filter to only include specified source nodes
                     (let [source-set (set source-nodes)]
                       (filter (fn [[a b]] (or (source-set a) (source-set b)))
                               (d/q base-query db edge-attr)))
                     (d/q base-query db edge-attr))]
    (->> candidates
         (pmap (fn [[a b]]
                 {:source a
                  :target b
                  :score (score-fn db a b edge-attr :direction direction)}))
         (filter #(>= (:score %) min-score))
         (sort-by :score >)
         (take limit)
         vec)))

;; =============================================================================
;; Random Walk Algorithms
;; =============================================================================
;;
;; Random walks are fundamental graph sampling techniques used for:
;; - Node embeddings (Node2Vec, DeepWalk)
;; - Graph sampling and approximation
;; - PageRank-style computations
;; - Community detection

(defn random-walk
  "Perform a single random walk starting from a node.

   Parameters:
     db - Datahike database
     start - Starting node entity ID
     edge-attr - Attribute representing edges
     walk-length - Maximum number of steps

   Options:
     :seed - Random seed for reproducibility
     :direction - :outgoing (default), :incoming, or :both

   Returns vector of node IDs representing the walk path.
   Walk terminates early if a dead-end (no neighbors) is reached."
  [db start edge-attr walk-length & {:keys [seed direction]
                                     :or {direction :outgoing}}]
  (let [rng (if seed (java.util.Random. (long seed)) (java.util.Random.))
        ;; Pre-load adjacency for efficiency
        adj-cache (atom {})
        get-neighbors (fn [node]
                        (if-let [cached (@adj-cache node)]
                          cached
                          (let [nbrs (vec (neighbors db node edge-attr :direction direction))]
                            (swap! adj-cache assoc node nbrs)
                            nbrs)))]
    (loop [path [start]
           current start
           steps 0]
      (if (>= steps walk-length)
        path
        (let [nbrs (get-neighbors current)]
          (if (empty? nbrs)
            path  ; dead end - terminate early
            (let [next-node (nth nbrs (.nextInt rng (count nbrs)))]
              (recur (conj path next-node) next-node (inc steps)))))))))

(defn weighted-random-walk
  "Perform a weighted random walk where edge weights affect transition probabilities.

   Higher weight edges are more likely to be traversed.

   Parameters:
     db - Datahike database
     start - Starting node entity ID
     from-attr - Attribute for edge source (on edge entity)
     to-attr - Attribute for edge target (on edge entity)
     weight-attr - Attribute for edge weight
     walk-length - Maximum number of steps

   Options:
     :seed - Random seed for reproducibility

   Returns vector of node IDs representing the walk path."
  [db start from-attr to-attr weight-attr walk-length & {:keys [seed]}]
  (let [rng (if seed (java.util.Random. (long seed)) (java.util.Random.))
        ;; Pre-load weighted edges
        edges (d/q '[:find ?s ?t ?w
                     :in $ ?fa ?ta ?wa
                     :where [?e ?fa ?s] [?e ?ta ?t] [?e ?wa ?w]]
                   db from-attr to-attr weight-attr)
        ;; Build adjacency with cumulative weights for sampling
        adj (reduce (fn [m [s t w]]
                      (update m s (fnil conj []) {:target t :weight (max 0 w)}))
                    {} edges)

        sample-neighbor (fn [node]
                          (let [neighbors (adj node)]
                            (when (seq neighbors)
                              (let [total-weight (reduce + (map :weight neighbors))
                                    r (* (.nextDouble rng) total-weight)]
                                (loop [remaining neighbors
                                       cumulative 0.0]
                                  (when (seq remaining)
                                    (let [{:keys [target weight]} (first remaining)
                                          new-cumulative (+ cumulative weight)]
                                      (if (< r new-cumulative)
                                        target
                                        (recur (rest remaining) new-cumulative)))))))))]
    (loop [path [start]
           current start
           steps 0]
      (if (>= steps walk-length)
        path
        (if-let [next-node (sample-neighbor current)]
          (recur (conj path next-node) next-node (inc steps))
          path)))))  ; dead end

(defn biased-random-walk
  "Perform a biased random walk with return parameter p and in-out parameter q.

   This is the Node2Vec-style walk that interpolates between BFS and DFS:
   - p controls likelihood of returning to previous node (low p = more backtracking)
   - q controls exploration vs exploitation (low q = DFS-like, high q = BFS-like)

   Transition probabilities from node t to v, considering move to x:
   - 1/p if x = t (return to previous node)
   - 1   if x is also neighbor of t (stay local)
   - 1/q if x is not neighbor of t (explore outward)

   Parameters:
     db - Datahike database
     start - Starting node entity ID
     edge-attr - Attribute representing edges
     walk-length - Maximum number of steps
     p - Return parameter (default 1.0)
     q - In-out parameter (default 1.0)

   Options:
     :seed - Random seed for reproducibility
     :direction - :outgoing (default), :incoming, or :both"
  [db start edge-attr walk-length p q & {:keys [seed direction]
                                         :or {direction :outgoing}}]
  (let [rng (if seed (java.util.Random. (long seed)) (java.util.Random.))
        ;; Pre-load all adjacencies for efficiency
        all-edges (case direction
                    :outgoing (d/q '[:find ?s ?t :in $ ?ea :where [?s ?ea ?t]] db edge-attr)
                    :incoming (d/q '[:find ?t ?s :in $ ?ea :where [?s ?ea ?t]] db edge-attr)
                    :both (into (d/q '[:find ?s ?t :in $ ?ea :where [?s ?ea ?t]] db edge-attr)
                                (d/q '[:find ?t ?s :in $ ?ea :where [?s ?ea ?t]] db edge-attr)))
        adj (reduce (fn [m [s t]]
                      (update m s (fnil conj #{}) t))
                    {} all-edges)

        sample-biased (fn [prev current]
                        (let [neighbors (vec (adj current))]
                          (when (seq neighbors)
                            (let [prev-neighbors (if prev (adj prev) #{})
                                  ;; Calculate unnormalized probabilities
                                  weights (mapv (fn [x]
                                                  (cond
                                                    (= x prev) (/ 1.0 p)        ; return
                                                    (prev-neighbors x) 1.0      ; common neighbor
                                                    :else (/ 1.0 q)))           ; explore
                                                neighbors)
                                  total (reduce + weights)
                                  r (* (.nextDouble rng) total)]
                              ;; Sample based on weights
                              (loop [i 0
                                     cumulative 0.0]
                                (when (< i (count neighbors))
                                  (let [new-cumulative (+ cumulative (nth weights i))]
                                    (if (< r new-cumulative)
                                      (nth neighbors i)
                                      (recur (inc i) new-cumulative)))))))))]
    (loop [path [start]
           prev nil
           current start
           steps 0]
      (if (>= steps walk-length)
        path
        (if-let [next-node (sample-biased prev current)]
          (recur (conj path next-node) current next-node (inc steps))
          path)))))

(defn- uniform-walk-with-adj
  "Internal: perform uniform random walk using pre-computed adjacency map."
  [adj start walk-length rng]
  (loop [path [start]
         current start
         steps 0]
    (if (>= steps walk-length)
      path
      (let [nbrs (adj current)]
        (if (or (nil? nbrs) (empty? nbrs))
          path
          (let [nbrs-vec (if (vector? nbrs) nbrs (vec nbrs))
                next-node (nth nbrs-vec (.nextInt rng (count nbrs-vec)))]
            (recur (conj path next-node) next-node (inc steps))))))))

(defn random-walks
  "Generate multiple random walks from multiple starting nodes.
   Optimized: loads adjacency structure once and reuses for all walks.

   Parameters:
     db - Datahike database
     edge-attr - Attribute representing edges
     walk-length - Maximum steps per walk
     walks-per-node - Number of walks to start from each node

   Options:
     :seed - Random seed for reproducibility
     :source-nodes - Specific nodes to start from (default: all nodes with edges)
     :direction - :outgoing (default), :incoming, or :both
     :parallel? - Use parallel processing (default true)

   Returns lazy sequence of walks (vectors of node IDs)."
  [db edge-attr walk-length walks-per-node & {:keys [seed source-nodes direction parallel?]
                                              :or {direction :outgoing parallel? true}}]
  (let [;; Pre-load all adjacencies ONCE for efficiency
        all-edges (case direction
                    :outgoing (d/q '[:find ?s ?t :in $ ?ea :where [?s ?ea ?t]] db edge-attr)
                    :incoming (d/q '[:find ?t ?s :in $ ?ea :where [?s ?ea ?t]] db edge-attr)
                    :both (into (d/q '[:find ?s ?t :in $ ?ea :where [?s ?ea ?t]] db edge-attr)
                                (d/q '[:find ?t ?s :in $ ?ea :where [?s ?ea ?t]] db edge-attr)))
        adj (reduce (fn [m [s t]]
                      (update m s (fnil conj []) t))
                    {} all-edges)
        sources (or source-nodes (set (keys adj)))
        tasks (for [node sources
                    walk-idx (range walks-per-node)]
                [node walk-idx])
        generate-walk (fn [[node walk-idx]]
                        (let [walk-seed (if seed (+ seed (* node 1000) walk-idx) (System/nanoTime))
                              rng (java.util.Random. (long walk-seed))]
                          (uniform-walk-with-adj adj node walk-length rng)))]
    (if parallel?
      (pmap generate-walk tasks)
      (map generate-walk tasks))))

(defn- biased-walk-with-adj
  "Internal: perform biased walk using pre-computed adjacency map."
  [adj start walk-length p q rng]
  (let [sample-biased (fn [prev current]
                        (let [neighbors (vec (adj current))]
                          (when (seq neighbors)
                            (let [prev-neighbors (if prev (adj prev) #{})
                                  weights (mapv (fn [x]
                                                  (cond
                                                    (= x prev) (/ 1.0 p)
                                                    (prev-neighbors x) 1.0
                                                    :else (/ 1.0 q)))
                                                neighbors)
                                  total (reduce + weights)
                                  r (* (.nextDouble rng) total)]
                              (loop [i 0
                                     cumulative 0.0]
                                (when (< i (count neighbors))
                                  (let [new-cumulative (+ cumulative (nth weights i))]
                                    (if (< r new-cumulative)
                                      (nth neighbors i)
                                      (recur (inc i) new-cumulative)))))))))]
    (loop [path [start]
           prev nil
           current start
           steps 0]
      (if (>= steps walk-length)
        path
        (if-let [next-node (sample-biased prev current)]
          (recur (conj path next-node) current next-node (inc steps))
          path)))))

(defn biased-random-walks
  "Generate Node2Vec-style biased random walks from multiple nodes.

   Same as random-walks but uses biased sampling with p and q parameters.
   Optimized: loads adjacency structure once and reuses for all walks.

   Parameters:
     db - Datahike database
     edge-attr - Attribute representing edges
     walk-length - Maximum steps per walk
     walks-per-node - Number of walks per node
     p - Return parameter
     q - In-out parameter

   Options:
     :seed - Random seed
     :source-nodes - Specific starting nodes
     :direction - Edge direction
     :parallel? - Use parallel processing (default true)"
  [db edge-attr walk-length walks-per-node p q & {:keys [seed source-nodes direction parallel?]
                                                  :or {direction :outgoing parallel? true}}]
  (let [;; Pre-load all adjacencies ONCE for efficiency
        all-edges (case direction
                    :outgoing (d/q '[:find ?s ?t :in $ ?ea :where [?s ?ea ?t]] db edge-attr)
                    :incoming (d/q '[:find ?t ?s :in $ ?ea :where [?s ?ea ?t]] db edge-attr)
                    :both (into (d/q '[:find ?s ?t :in $ ?ea :where [?s ?ea ?t]] db edge-attr)
                                (d/q '[:find ?t ?s :in $ ?ea :where [?s ?ea ?t]] db edge-attr)))
        adj (reduce (fn [m [s t]]
                      (update m s (fnil conj #{}) t))
                    {} all-edges)
        sources (or source-nodes (set (keys adj)))
        tasks (for [node sources
                    walk-idx (range walks-per-node)]
                [node walk-idx])
        generate-walk (fn [[node walk-idx]]
                        (let [walk-seed (if seed (+ seed (* node 1000) walk-idx) (System/nanoTime))
                              rng (java.util.Random. (long walk-seed))]
                          (biased-walk-with-adj adj node walk-length p q rng)))]
    (if parallel?
      (pmap generate-walk tasks)
      (map generate-walk tasks))))

