(ns datahike.experimental.graph
  "Graph algorithms over a GraphSpec (see datahike.experimental.graph-spec).

   Every algorithm takes a graph spec and a db: (algorithm graph-spec db ...).
   Build a spec with a graph-spec constructor (attr-graph, weighted-graph, …),
   optionally transform it (reverse-graph, undirected-graph, …), then call an
   algorithm:

     (let [g (attr-graph :follows)]
       (shortest-path g db alice carol))

   Whole-graph algorithms materialize the spec internally; if you call several
   on the same graph, materialize once (graph-spec/materialize) and reuse it.

   Portable across Clojure and ClojureScript."
  (:require [datahike.api :as d]
            [datahike.experimental.graph-spec :as gs]
            [datahike.experimental.graph-util :as gu]
            [datahike.lru :as lru]
            [clojure.set :as set]))

;; ===========================================================================
;; Planner cost models (:datahike/output-cardinality)
;; ===========================================================================
;; When an algorithm is called inside a Datalog query and its result is
;; destructured (e.g. [(transitive-closure ?g $ ?s) [?n ...]]), these tell the
;; planner how many rows the bind produces, so a downstream join on the output
;; is ordered/costed well. The cost fn receives
;; {:db :fn-sym :args :binding :provenance}; :provenance maps a graph var to the
;; form that built it (e.g. ?g -> (attr-graph :follows)), so we can introspect a
;; graph passed by variable. Users attach the same metadata to their own algos.

(defn- graph-edge-attr
  "Resolve a graph argument to its single underlying edge attribute, following
   :provenance for vars and descending through transformer/constructor forms.
   Returns a keyword, or nil if not statically resolvable (e.g. weighted/
   multi-attr/query graphs, or a runtime-only spec)."
  [arg provenance]
  (let [form (if (symbol? arg) (get provenance arg) arg)]
    (cond
      (keyword? form) form
      (not (seq? form)) nil
      :else
      (let [head (first form)
            head-name (when (symbol? head) (name head))]
        (case head-name
          "attr-graph" (let [a (second form)] (when (keyword? a) a))
          ("undirected-graph" "reverse-graph" "filtered-graph" "subgraph"
                              "materialize" "ensure-materialized")
          (recur (second form) provenance)
          nil)))))

(defn- graph-node-count
  "Number of distinct nodes touched by the graph's edge attribute, or nil."
  [db arg provenance]
  (when-let [attr (graph-edge-attr arg provenance)]
    (count (into #{} (mapcat (fn [d] [(:e d) (:v d)])) (d/datoms db :aevt attr)))))

(defn- node-set-card
  "Cost model for node-set-returning algorithms (reachability, components,
   topo-sort): the graph's node count, an upper bound on the result size. The
   graph is the first argument."
  [{:keys [db args provenance]}]
  (graph-node-count db (first args) provenance))

(defn- path-card
  "Cost model for single-path algorithms: a small constant — shortest paths are
   short (≈ graph diameter), so destructuring one yields few rows."
  [_ctx]
  16)

;; --- execution-cost (:datahike/cost) ---------------------------------------
;; The planner orders a function by per-call-cost × input-rows (the latter from
;; bound-var cardinalities). The per-call factor is the algorithm's COMPLEXITY in
;; the graph size, so the planner can (a) order a cheap algo before an expensive
;; one when both feed selective filters on a shared input, and (b) defer an
;; expensive algo behind row-reducing joins. The execution-time probe-and-sink
;; (execute.cljc) handles the row-reducing-vs-expanding-join ambiguity that the
;; static per-call factor cannot — so unlike before we can carry a TRUE
;; complexity factor here without over-deferring past input-expanding joins.
;;
;; Graph size [V E] is read from the edge attribute via :provenance (so a graph
;; passed as ?g resolves back through transformer wrappers). When the graph is
;; not statically resolvable (weighted/multi/query graphs), per-call falls back
;; to 1 (cost = input-rows), the prior behaviour.

(def ^:private graph-size-cache
  "LRU cache of [V E] keyed by [edge-attr db-content-hash max-eid]. graph-size
   scans :aevt (O(E)) and op-cost calls it many times per plan, but the size is
   fixed for a given (attr, db). The key is scoped on the db's CONTENT identity
   (`:hash` — datahike's own db-equality basis) rather than max-tx/max-eid, which
   can collide across independently-built dbs of equal size but different content."
  (atom (lru/lru 256)))

(defn- graph-size
  "[node-count edge-count] for the graph `arg`, or nil when the edge attribute
   isn't statically resolvable. Memoized per (attr, db-content)."
  [db arg provenance]
  (when-let [attr (graph-edge-attr arg provenance)]
    (let [k [attr (:hash db) (:max-eid db)]]
      (or (get @graph-size-cache k)
          (let [edges (d/datoms db :aevt attr)
                e (count edges)
                v (count (into #{} (mapcat (fn [d] [(:e d) (:v d)])) edges))
                sz [v e]]
            (swap! graph-size-cache assoc k sz)
            sz)))))

(defn- exec-cost
  "Build a :datahike/cost fn: total = input-rows × `complexity`(V, E). Falls back
   to input-rows when the graph size isn't statically resolvable."
  [complexity]
  (fn [{:keys [db args provenance input-rows]}]
    (let [rows (max 1 (long (or input-rows 1)))]
      (if-let [[v e] (graph-size db (first args) provenance)]
        (* rows (max 1 (long (complexity v e))))
        rows))))

(defn- log2 [n] (/ (Math/log (inc (max 0 (double n)))) (Math/log 2.0)))

;; Complexity tiers (per call), separated enough to order reliably on any graph:
(def ^:private linear-exec-cost    (exec-cost (fn [v e] (+ (long v) (long e)))))           ; BFS/DFS: V+E
(def ^:private loglinear-exec-cost (exec-cost (fn [v e] (long (* (long e) (inc (log2 v))))))) ; heap SSSP: E·log V
(def ^:private quadratic-exec-cost (exec-cost (fn [v e] (* (long v) (long (max 1 e))))))   ; all-source BFS: V·E
(def ^:private iterative-exec-cost (exec-cost (fn [v e] (* 20 (+ (long v) (long e))))))    ; ~20 passes ·(V+E)

;; ===========================================================================
;; Reachability
;; ===========================================================================

(defn ^{:datahike/output-cardinality node-set-card
        :datahike/cost linear-exec-cost} transitive-closure
  "Set of nodes reachable from `start` via one or more out-edges. `start` is
   included iff it lies on a cycle (i.e. is reachable from itself)."
  [g db start]
  (loop [seen #{}
         q (into gu/empty-queue (gs/out-neighbors g db start))]
    (if (empty? q)
      seen
      (let [n (peek q)]
        (if (contains? seen n)
          (recur seen (pop q))
          (recur (conj seen n) (into (pop q) (gs/out-neighbors g db n))))))))

(defn ^{:datahike/cost linear-exec-cost} reachable?
  "True iff there is a path of length >= 1 from `source` to `target`."
  [g db source target]
  (loop [seen #{}
         q (into gu/empty-queue (gs/out-neighbors g db source))]
    (if (empty? q)
      false
      (let [n (peek q)]
        (cond
          (= n target) true
          (contains? seen n) (recur seen (pop q))
          :else (recur (conj seen n) (into (pop q) (gs/out-neighbors g db n))))))))

;; ===========================================================================
;; Path enumeration
;; ===========================================================================

(defn all-paths
  "All simple paths from `start` following out-edges, up to `:max-depth`
   (default 10). With `:target`, keep only paths ending at target. Returns a
   seq of node vectors."
  ([g db start] (all-paths g db start {}))
  ([g db start {:keys [max-depth target] :or {max-depth 10}}]
   (loop [frontier [[start]]
          depth 0
          paths []]
     (if (or (empty? frontier) (>= depth max-depth))
       (cond->> paths
         target (filter #(= (peek %) target)))
       (let [expansions (for [path frontier
                              nb (gs/out-neighbors g db (peek path))
                              :when (not (some #{nb} path))]
                          (conj path nb))]
         (recur expansions (inc depth) (into paths expansions)))))))

;; ===========================================================================
;; Shortest path (unweighted, BFS)
;; ===========================================================================

(defn ^{:datahike/output-cardinality path-card
        :datahike/cost linear-exec-cost} shortest-path
  "Shortest path (fewest edges) from `source` to `target` as a vector of nodes,
   or nil if unreachable. Plain BFS — the minimum hop count is guaranteed."
  [g db source target]
  (if (= source target)
    [source]
    (loop [q (conj gu/empty-queue source)
           parent {source nil}]
      (if (empty? q)
        nil
        (let [n (peek q)]
          (if (= n target)
            (loop [path () node target]
              (if (nil? node) (vec path) (recur (conj path node) (parent node))))
            (let [nbrs (remove #(contains? parent %) (gs/out-neighbors g db n))]
              (recur (into (pop q) nbrs)
                     (reduce (fn [p nb] (assoc p nb n)) parent nbrs)))))))))

(defn path-length
  "Number of edges on the shortest path, or nil if unreachable."
  [g db source target]
  (when-let [p (shortest-path g db source target)]
    (dec (count p))))

;; ===========================================================================
;; Connected components (undirected)
;; ===========================================================================

(defn ^{:datahike/output-cardinality node-set-card
        :datahike/cost linear-exec-cost} connected-component
  "Set of nodes in the same undirected connected component as `start`
   (including `start`)."
  [g db start]
  (loop [seen #{start}
         q (into gu/empty-queue (concat (gs/out-neighbors g db start)
                                        (gs/in-neighbors g db start)))]
    (if (empty? q)
      seen
      (let [n (peek q)]
        (if (contains? seen n)
          (recur seen (pop q))
          (recur (conj seen n)
                 (into (pop q) (concat (gs/out-neighbors g db n)
                                       (gs/in-neighbors g db n)))))))))

(defn ^{:datahike/cost linear-exec-cost} connected-components
  "All undirected connected components as a seq of node sets."
  [g db]
  (let [mg (gs/materialize g db)
        nodes (set (gs/all-nodes mg db))]
    (loop [remaining nodes
           comps []]
      (if (empty? remaining)
        comps
        (let [c (connected-component mg db (first remaining))]
          (recur (set/difference remaining c) (conj comps c)))))))

(defn same-component?
  "True iff `a` and `b` are in the same undirected connected component."
  [g db a b]
  (contains? (connected-component g db a) b))

;; ===========================================================================
;; Strongly connected components (iterative Tarjan)
;; ===========================================================================

(defn ^{:datahike/cost linear-exec-cost} strongly-connected-components
  "All strongly connected components as a vector of node sets. O(V+E),
   iterative (no recursion depth limit)."
  [g db]
  (let [mg (gs/materialize g db)
        nodes (set (gs/all-nodes mg db))
        get-successors (fn [n] (gs/out-neighbors mg db n))]
    (if (empty? nodes)
      []
      (loop [work-stack (list)
             nodes-to-visit (seq nodes)
             index 0
             indices {}
             lowlinks {}
             node-stack []
             on-stack #{}
             sccs []]
        (cond
          (seq work-stack)
          (let [[v succ-idx phase return-to] (first work-stack)
                rest-work (rest work-stack)
                successors (vec (get-successors v))]
            (cond
              (= phase :enter)
              (recur (cons [v 0 :process return-to] rest-work)
                     nodes-to-visit (inc index)
                     (assoc indices v index)
                     (assoc lowlinks v index)
                     (conj node-stack v)
                     (conj on-stack v)
                     sccs)

              (= phase :process)
              (if (< succ-idx (count successors))
                (let [w (nth successors succ-idx)]
                  (cond
                    (not (contains? indices w))
                    (recur (cons [w 0 :enter v]
                                 (cons [v (inc succ-idx) :process return-to] rest-work))
                           nodes-to-visit index indices lowlinks node-stack on-stack sccs)

                    (contains? on-stack w)
                    (recur (cons [v (inc succ-idx) :process return-to] rest-work)
                           nodes-to-visit index indices
                           (update lowlinks v min (indices w))
                           node-stack on-stack sccs)

                    :else
                    (recur (cons [v (inc succ-idx) :process return-to] rest-work)
                           nodes-to-visit index indices lowlinks node-stack on-stack sccs)))

                (if (= (lowlinks v) (indices v))
                  (let [[scc new-stack new-on-stack]
                        (loop [scc #{} stk node-stack ons on-stack]
                          (let [w (peek stk)]
                            (if (= w v)
                              [(conj scc w) (pop stk) (disj ons w)]
                              (recur (conj scc w) (pop stk) (disj ons w)))))]
                    (if return-to
                      (recur rest-work nodes-to-visit index indices
                             (update lowlinks return-to min (lowlinks v))
                             new-stack new-on-stack (conj sccs scc))
                      (recur rest-work nodes-to-visit index indices lowlinks
                             new-stack new-on-stack (conj sccs scc))))
                  (if return-to
                    (recur rest-work nodes-to-visit index indices
                           (update lowlinks return-to min (lowlinks v))
                           node-stack on-stack sccs)
                    (recur rest-work nodes-to-visit index indices lowlinks
                           node-stack on-stack sccs))))

              :else
              (recur rest-work nodes-to-visit index indices lowlinks node-stack on-stack sccs)))

          (seq nodes-to-visit)
          (let [v (first nodes-to-visit)]
            (if (contains? indices v)
              (recur work-stack (rest nodes-to-visit) index indices lowlinks node-stack on-stack sccs)
              (recur (list [v 0 :enter nil])
                     (rest nodes-to-visit)
                     index indices lowlinks node-stack on-stack sccs)))

          :else sccs)))))

;; ===========================================================================
;; Topological sort (Kahn)
;; ===========================================================================

(defn ^{:datahike/output-cardinality node-set-card
        :datahike/cost linear-exec-cost} topological-sort
  "Topological order of a DAG as a vector, or nil if the graph has a cycle.
   O(V+E)."
  [g db]
  (let [mg (gs/materialize g db)
        edges (gs/all-edges mg db)
        nodes (set (gs/all-nodes mg db))
        in-degree (reduce (fn [m [_ t]] (update m t (fnil inc 0)))
                          (zipmap nodes (repeat 0))
                          edges)]
    (loop [queue (into gu/empty-queue (filter #(zero? (in-degree %)) nodes))
           in-deg in-degree
           result []]
      (if (empty? queue)
        (when (= (count result) (count nodes)) result)
        (let [n (peek queue)
              queue' (pop queue)
              [queue'' in-deg']
              (reduce (fn [[q d] succ]
                        (let [nd (dec (d succ))]
                          [(if (zero? nd) (conj q succ) q) (assoc d succ nd)]))
                      [queue' in-deg]
                      (gs/out-neighbors mg db n))]
          (recur queue'' in-deg' (conj result n)))))))

(defn has-cycle?
  "True iff the (directed) graph contains a cycle."
  [g db]
  (nil? (topological-sort g db)))

;; ===========================================================================
;; Cycle extraction
;; ===========================================================================

(defn find-cycle
  "Return one directed cycle as a vector of nodes, or nil if the graph is
   acyclic. Iterative.

   Method: peel sink nodes (out-degree 0) repeatedly; every surviving node has
   an out-edge to another survivor, so following out-edges from any survivor is
   guaranteed to revisit a node — that repeat delimits a cycle."
  [g db]
  (let [mg (gs/materialize g db)
        nodes (set (gs/all-nodes mg db))
        out-deg (persistent!
                 (reduce (fn [m n] (assoc! m n (count (gs/out-neighbors mg db n))))
                         (transient {}) nodes))
        remaining (loop [od out-deg]
                    (let [sinks (filter #(zero? (get od %)) (keys od))]
                      (if (empty? sinks)
                        (set (keys od))
                        (recur (reduce (fn [d s]
                                         (reduce (fn [d2 p]
                                                   (if (contains? d2 p) (update d2 p dec) d2))
                                                 (dissoc d s)
                                                 (gs/in-neighbors mg db s)))
                                       od sinks)))))]
    (when (seq remaining)
      (loop [node (first remaining)
             path []
             seen {}]
        (if-let [i (get seen node)]
          (subvec path i)
          (recur (first (filter remaining (gs/out-neighbors mg db node)))
                 (conj path node)
                 (assoc seen node (count path))))))))

;; ===========================================================================
;; Bipartite coloring (undirected)
;; ===========================================================================

(defn ^{:datahike/cost linear-exec-cost} bipartite-coloring
  "Check 2-colorability of the undirected view. Returns
   {:bipartite? true :coloring {node 0|1}} or {:bipartite? false}."
  [g db]
  (let [ug (gs/materialize (gs/undirected-graph g) db)
        nodes (gs/all-nodes ug db)
        nbrs (fn [n] (gs/out-neighbors ug db n))]
    (loop [remaining (seq nodes)
           coloring {}]
      (if (empty? remaining)
        {:bipartite? true :coloring coloring}
        (let [start (first remaining)]
          (if (contains? coloring start)
            (recur (rest remaining) coloring)
            (let [result
                  (loop [queue (conj gu/empty-queue start)
                         col (assoc coloring start 0)]
                    (if (empty? queue)
                      col
                      (let [n (peek queue)
                            q' (pop queue)
                            nc (- 1 (col n))
                            step (reduce (fn [[q c] nb]
                                           (cond
                                             (not (contains? c nb)) [(conj q nb) (assoc c nb nc)]
                                             (not= (c nb) nc) (reduced :not-bipartite)
                                             :else [q c]))
                                         [q' col]
                                         (nbrs n))]
                        (if (= step :not-bipartite)
                          :not-bipartite
                          (recur (first step) (second step))))))]
              (if (= result :not-bipartite)
                {:bipartite? false}
                (recur (rest remaining) result)))))))))

;; ===========================================================================
;; Bridges / cut edges (iterative Tarjan, undirected)
;; ===========================================================================

(defn ^{:datahike/cost linear-exec-cost} find-bridges
  "Set of bridges (edges whose removal disconnects the graph) in the undirected
   view, as a set of [a b] pairs with a < b. O(V+E), iterative."
  [g db]
  (let [ug (gs/materialize (gs/undirected-graph g) db)
        nodes (gs/all-nodes ug db)
        nbrs (fn [n] (gs/out-neighbors ug db n))]
    (loop [roots (seq nodes)
           disc {}
           low {}
           timer 0
           bridges #{}]
      (cond
        (empty? roots) bridges
        (contains? disc (first roots)) (recur (rest roots) disc low timer bridges)
        :else
        (let [r (first roots)
              [disc low timer bridges]
              (loop [stack [[r -1 (seq (nbrs r))]]
                     disc (assoc disc r timer)
                     low (assoc low r timer)
                     timer (inc timer)
                     bridges bridges]
                (if (empty? stack)
                  [disc low timer bridges]
                  (let [[u p remaining] (peek stack)]
                    (if (seq remaining)
                      (let [v (first remaining)
                            stack' (conj (pop stack) [u p (next remaining)])]
                        (cond
                          (= v p)
                          (recur stack' disc low timer bridges)

                          (contains? disc v)
                          (recur stack' disc (update low u min (disc v)) timer bridges)

                          :else
                          (recur (conj stack' [v u (seq (nbrs v))])
                                 (assoc disc v timer) (assoc low v timer)
                                 (inc timer) bridges)))
                      (let [stack' (pop stack)
                            low' (if (not= p -1) (update low p min (low u)) low)
                            bridges' (if (and (not= p -1) (> (low u) (disc p)))
                                       (conj bridges (if (< p u) [p u] [u p]))
                                       bridges)]
                        (recur stack' disc low' timer bridges'))))))]
          (recur (rest roots) disc low timer bridges))))))

;; ===========================================================================
;; Semi-naive transitive closure + reusable Datalog rules
;; ===========================================================================

(defn ^{:datahike/cost quadratic-exec-cost} semi-naive-transitive-closure
  "All reachable [source target] pairs, computed by semi-naive evaluation
   (only new derivations are joined each round). Returns a set of pairs."
  [g db]
  (let [edges (into #{} (gs/all-edges (gs/materialize g db) db))
        by-src (group-by first edges)]
    (loop [known edges
           delta edges]
      (if (empty? delta)
        known
        (let [new-facts (into #{}
                              (for [[x z] delta
                                    [_ y] (by-src z)
                                    :when (not (known [x y]))]
                                [x y]))]
          (recur (into known new-facts) new-facts))))))

(def ^{:doc "Datalog rules for transitive reachability over an attribute. Use
             with (d/q query db transitive-rules):

               (d/q '[:find ?t :in $ % ?start
                      :where (reachable ?start ?t :follows)]
                    db transitive-rules start)"}
  transitive-rules
  '[[(reachable ?x ?y ?attr)
     [?x ?attr ?y]]
    [(reachable ?x ?y ?attr)
     [?x ?attr ?z]
     (reachable ?z ?y ?attr)]])

(def ^{:doc "Datalog rules producing (path-length ?x ?y ?attr ?len) tuples."}
  path-length-rules
  '[[(path-length ?x ?y ?attr 1)
     [?x ?attr ?y]]
    [(path-length ?x ?y ?attr ?len)
     [?x ?attr ?z]
     (path-length ?z ?y ?attr ?len-1)
     [(inc ?len-1) ?len]]])

(defn make-transitive-pred
  "Return a predicate (fn [db source target]) testing reachability over `g`,
   for use in query function clauses."
  [g]
  (fn [db source target] (reachable? g db source target)))

;; ===========================================================================
;; Weighted shortest path (Dijkstra)
;; ===========================================================================

(defn ^{:datahike/cost loglinear-exec-cost} weighted-shortest-path
  "Dijkstra shortest path by edge weight. Returns {:path [...] :cost c} or nil
   if unreachable. Edge weights come from the spec's edge-weight (1 when
   unweighted). Non-negative weights only.

   Uses a total-order priority queue, so equal-cost frontier entries are never
   dropped, and per-edge weights (not per-node)."
  [g db source target]
  (let [mg (gs/materialize g db)]
    (loop [pq (gu/pq-push (gu/pq) 0 [source [source]])
           best {source 0}
           visited #{}]
      (if (gu/pq-empty? pq)
        nil
        (let [[cost [node path]] (gu/pq-peek pq)
              pq (gu/pq-pop pq)]
          (cond
            (= node target) {:path path :cost cost}
            (contains? visited node) (recur pq best visited)
            :else
            (let [visited (conj visited node)
                  [pq' best']
                  (reduce (fn [[q b] nb]
                            (if (contains? visited nb)
                              [q b]
                              (let [nc (+ cost (gs/edge-weight mg db node nb))]
                                (if (< nc (get b nb gu/infinity))
                                  [(gu/pq-push q nc [nb (conj path nb)]) (assoc b nb nc)]
                                  [q b]))))
                          [pq best]
                          (gs/out-neighbors mg db node))]
              (recur pq' best' visited))))))))

(defn ^{:datahike/cost loglinear-exec-cost} bottleneck-path
  "Widest path: maximize the minimum edge weight (capacity) along the path.
   Returns {:path [...] :capacity c} or nil. Capacity from edge-weight."
  [g db source target]
  (let [mg (gs/materialize g db)]
    (loop [pq (gu/pq-push (gu/pq) (- gu/infinity) [source [source]])
           best {source gu/infinity}
           visited #{}]
      (if (gu/pq-empty? pq)
        nil
        (let [[negcap [node path]] (gu/pq-peek pq)
              cap (- negcap)
              pq (gu/pq-pop pq)]
          (cond
            (= node target) {:path path :capacity cap}
            (contains? visited node) (recur pq best visited)
            :else
            (let [visited (conj visited node)
                  [pq' best']
                  (reduce (fn [[q b] nb]
                            (if (contains? visited nb)
                              [q b]
                              (let [nc (min cap (gs/edge-weight mg db node nb))]
                                (if (> nc (get b nb (- gu/infinity)))
                                  [(gu/pq-push q (- nc) [nb (conj path nb)]) (assoc b nb nc)]
                                  [q b]))))
                          [pq best]
                          (gs/out-neighbors mg db node))]
              (recur pq' best' visited))))))))

(defn ^{:datahike/cost loglinear-exec-cost} astar-path
  "A* shortest path with an admissible heuristic. `heuristic-fn` takes
   [node target] and estimates remaining cost (never overestimating for an
   optimal result). Returns {:path [...] :cost c} or nil. Weights from
   edge-weight."
  [g db source target heuristic-fn]
  (if (= source target)
    {:path [source] :cost 0}
    (let [mg (gs/materialize g db)]
      (loop [pq (gu/pq-push (gu/pq) (heuristic-fn source target) [0 source [source]])
             gscore {source 0}
             closed #{}]
        (if (gu/pq-empty? pq)
          nil
          (let [[_f [g-score node path]] (gu/pq-peek pq)
                pq (gu/pq-pop pq)]
            (cond
              (= node target) {:path path :cost g-score}
              (contains? closed node) (recur pq gscore closed)
              :else
              (let [closed (conj closed node)
                    [pq' gscore']
                    (reduce (fn [[q gm] nb]
                              (let [tg (+ g-score (gs/edge-weight mg db node nb))]
                                (if (and (not (contains? closed nb))
                                         (< tg (get gm nb gu/infinity)))
                                  [(gu/pq-push q (+ tg (heuristic-fn nb target))
                                               [tg nb (conj path nb)])
                                   (assoc gm nb tg)]
                                  [q gm])))
                            [pq gscore]
                            (gs/out-neighbors mg db node))]
                (recur pq' gscore' closed)))))))))

;; ===========================================================================
;; Semiring path computations (trust, probability)
;; ===========================================================================

(defn trust-propagation
  "Propagate trust from `source`. Multiplicative max-semiring: each hop
   multiplies by `:discount` (default 0.9) and the edge weight (a per-edge
   trust multiplier, 1 when unweighted). Returns {node trust-score}."
  [g db source & {:keys [discount] :or {discount 0.9}}]
  (let [mg (gs/materialize g db)]
    (loop [trust {source 1.0}
           frontier #{source}]
      (if (empty? frontier)
        trust
        (let [expansions (for [node frontier
                               t (gs/out-neighbors mg db node)
                               :let [prop (* (trust node) discount (gs/edge-weight mg db node t))]
                               :when (> prop (get trust t 0))]
                           [t prop])
              new-trust (reduce (fn [m [t s]] (assoc m t (max s (get m t 0))))
                                trust expansions)
              new-frontier (into #{} (map first) expansions)]
          (recur new-trust new-frontier))))))

(defn probability-reachability
  "Max probability of reaching `target` from `source`, treating edge weights as
   independent probabilities — the maximum over simple paths (up to
   `:max-depth`, default 5) of the product of edge weights. Returns 0.0 if
   unreachable."
  [g db source target & {:keys [max-depth] :or {max-depth 5}}]
  (let [mg (gs/materialize g db)
        paths (all-paths mg db source {:target target :max-depth max-depth})]
    (if (empty? paths)
      0.0
      (apply max
             (for [path paths]
               (reduce (fn [p [a b]] (* p (gs/edge-weight mg db a b)))
                       1.0
                       (partition 2 1 path)))))))

;; ===========================================================================
;; Centrality
;; ===========================================================================

(defn ^{:datahike/output-cardinality node-set-card
        :datahike/cost iterative-exec-cost} page-rank
  "PageRank over the directed graph. Returns {node score}; scores sum to ~1.

   Options: :damping (0.85), :iterations (20), :tolerance (1e-6). Dangling nodes
   (out-degree 0) have their mass redistributed to all nodes each iteration, so
   rank is conserved even when the graph has sinks."
  [g db & {:keys [damping iterations tolerance]
           :or {damping 0.85 iterations 20 tolerance 1e-6}}]
  (let [mg (gs/materialize g db)
        nodes (set (gs/all-nodes mg db))
        n (count nodes)]
    (when (pos? n)
      (let [out-deg (into {} (map (fn [v] [v (count (gs/out-neighbors mg db v))])) nodes)
            in-links (reduce (fn [m [s t]] (update m t (fnil conj []) s))
                             {} (gs/all-edges mg db))
            init (/ 1.0 n)
            base (/ (- 1.0 damping) n)]
        (loop [scores (zipmap nodes (repeat init))
               iter 0]
          (if (>= iter iterations)
            scores
            (let [dangling-mass (reduce (fn [s v] (if (zero? (out-deg v)) (+ s (scores v)) s))
                                        0.0 nodes)
                  dangling (* damping (/ dangling-mass n))
                  new-scores
                  (reduce (fn [acc node]
                            (let [incoming (reduce (fn [sum src]
                                                     (+ sum (/ (scores src) (out-deg src))))
                                                   0.0
                                                   (get in-links node []))]
                              (assoc acc node (+ base dangling (* damping incoming)))))
                          {} nodes)
                  max-diff (apply max (map #(gu/abs (- (scores %) (new-scores %))) nodes))]
              (if (< max-diff tolerance)
                new-scores
                (recur new-scores (inc iter))))))))))

(defn ^{:datahike/output-cardinality node-set-card
        :datahike/cost quadratic-exec-cost} closeness-centrality
  "Closeness centrality over the undirected view: reciprocal of average distance
   to reachable nodes, scaled by the fraction reachable (so it is well-defined on
   disconnected graphs). Returns {node score in [0,1]}; isolated nodes score 0."
  [g db]
  (let [ug (gs/materialize (gs/undirected-graph g) db)
        nodes (set (gs/all-nodes ug db))
        n (count nodes)]
    (when (> n 1)
      (into {}
            (for [source nodes]
              (let [distances
                    (loop [queue (conj gu/empty-queue source)
                           dist {source 0}]
                      (if (empty? queue)
                        dist
                        (let [node (peek queue)
                              d (dist node)
                              [nq nd] (reduce (fn [[q dm] nb]
                                                (if (contains? dm nb)
                                                  [q dm]
                                                  [(conj q nb) (assoc dm nb (inc d))]))
                                              [(pop queue) dist]
                                              (gs/out-neighbors ug db node))]
                          (recur nq nd))))
                    reachable (dec (count distances))
                    total (reduce + 0 (vals distances))]
                [source (if (and (pos? reachable) (pos? total))
                          (* (/ reachable (dec n)) (/ reachable total))
                          0.0)]))))))

(defn ^{:datahike/output-cardinality node-set-card
        :datahike/cost quadratic-exec-cost} betweenness-centrality
  "Betweenness centrality over the undirected view (Brandes' algorithm, O(VE)).
   Returns {node score in [0,1]}, normalized by (n-1)(n-2) — which folds in the
   undirected pair double-counting so values match the standard definition."
  [g db]
  (let [ug (gs/materialize (gs/undirected-graph g) db)
        nodes (vec (gs/all-nodes ug db))
        n (count nodes)]
    (when (> n 2)
      (let [bc (reduce
                (fn [betweenness source]
                  (let [[stack pred sigma dist]
                        (loop [queue (conj gu/empty-queue source)
                               stack []
                               pred (zipmap nodes (repeat []))
                               sigma (assoc (zipmap nodes (repeat 0)) source 1)
                               dist (assoc (zipmap nodes (repeat -1)) source 0)]
                          (if (empty? queue)
                            [stack pred sigma dist]
                            (let [v (peek queue)
                                  queue' (pop queue)
                                  dv (dist v)
                                  [q' p' s' d']
                                  (reduce
                                   (fn [[q p s d] w]
                                     (cond
                                       (neg? (d w))
                                       [(conj q w) (update p w conj v)
                                        (update s w + (s v)) (assoc d w (inc dv))]
                                       (= (d w) (inc dv))
                                       [q (update p w conj v) (update s w + (s v)) d]
                                       :else [q p s d]))
                                   [queue' pred sigma dist]
                                   (gs/out-neighbors ug db v))]
                              (recur q' (conj stack v) p' s' d'))))
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
                    (reduce (fn [bc' v]
                              (if (= v source) bc' (update bc' v + (delta' v))))
                            betweenness
                            nodes)))
                (zipmap nodes (repeat 0.0))
                nodes)
            norm (* (dec n) (- n 2) 1.0)]
        (reduce-kv (fn [m k v] (assoc m k (/ v norm))) {} bc)))))

;; ===========================================================================
;; Community detection
;; ===========================================================================

(defn community-stats
  "Statistics for a {node -> community} map: :num-communities, :sizes,
   :largest, :smallest, :avg-size, :isolation-score (fraction of singletons)."
  [communities]
  (let [by-comm (group-by val communities)
        sizes (reduce-kv (fn [m comm members] (assoc m comm (count members))) {} by-comm)
        sorted (sort-by val > sizes)
        n (count sizes)
        singletons (count (filter #(= 1 (val %)) sizes))]
    {:num-communities n
     :sizes sizes
     :largest (first sorted)
     :smallest (last sorted)
     :avg-size (if (pos? n) (/ (count communities) n) 0)
     :isolation-score (if (pos? n) (double (/ singletons n)) 0.0)}))

(defn ^{:datahike/output-cardinality node-set-card
        :datahike/cost iterative-exec-cost} label-propagation
  "Label-propagation community detection over the undirected view: each node
   adopts the most frequent label among its neighbors (ties broken via the
   seeded PRNG, so results are deterministic and reproducible across JVM and JS).
   Returns {:communities {node label} :converged bool :iterations n :stats ...}.

   Options: :max-iterations (10), :seeds {node label} (fixed labels),
            :rng-seed (1) — PRNG seed for the node visit order and tie-breaking."
  [g db & {:keys [max-iterations seeds rng-seed] :or {max-iterations 10 seeds {} rng-seed 1}}]
  (let [ug (gs/materialize (gs/undirected-graph g) db)
        nodes (vec (gs/all-nodes ug db))
        initial (merge (zipmap nodes nodes) seeds)
        seed-nodes (set (keys seeds))]
    (loop [labels initial
           iter 0
           rng (gu/rng rng-seed)]
      (if (>= iter max-iterations)
        {:communities labels :converged false :iterations iter
         :stats (community-stats labels)}
        (let [[rng order] (gu/rng-shuffle rng nodes)
              [new-labels changed? rng]
              (reduce
               (fn [[lbls changed r] node]
                 (if (seed-nodes node)
                   [lbls changed r]
                   (let [freqs (frequencies (map lbls (gs/out-neighbors ug db node)))
                         max-freq (when (seq freqs) (apply max (vals freqs)))
                         best (when max-freq (vec (for [[l f] freqs :when (= f max-freq)] l)))]
                     (if (seq best)
                       (let [[r' new-label] (gu/rng-nth r best)]
                         (if (not= new-label (lbls node))
                           [(assoc lbls node new-label) true r']
                           [lbls changed r']))
                       [lbls changed r]))))
               [labels false rng]
               order)]
          (if changed?
            (recur new-labels (inc iter) rng)
            {:communities new-labels :converged true :iterations (inc iter)
             :stats (community-stats new-labels)}))))))

;; --- Louvain (multi-level modularity optimization) -------------------------

(defn- lv-degrees [adj]
  (persistent!
   (reduce-kv (fn [m n nbrs] (assoc! m n (reduce + 0.0 (vals nbrs)))) (transient {}) adj)))

(defn- lv-modularity
  "Modularity of `comm` (node -> community) over weighted adjacency `adj`,
   m2 = total weight (sum of all adjacency entries = 2m)."
  [adj comm m2]
  (if (zero? m2)
    0.0
    (let [deg (lv-degrees adj)
          in-c (reduce-kv (fn [m i nbrs]
                            (reduce-kv (fn [m j w]
                                         (if (= (comm i) (comm j))
                                           (update m (comm i) (fnil + 0.0) w)
                                           m))
                                       m nbrs))
                          {} adj)
          tot-c (reduce-kv (fn [m i d] (update m (comm i) (fnil + 0.0) d)) {} deg)]
      (reduce (fn [q c]
                (+ q (- (/ (get in-c c 0.0) m2)
                        (let [t (/ (get tot-c c 0.0) m2)] (* t t)))))
              0.0
              (set (vals comm))))))

(defn- lv-one-level
  "One pass of local moving. Returns {node community}. Each node starts in its
   own community and repeatedly moves to the neighbor community with the highest
   positive modularity gain until stable."
  [adj m2 resolution]
  (let [deg (lv-degrees adj)
        m (/ m2 2.0)
        nodes (vec (keys adj))]
    (loop [comm (into {} (map (fn [n] [n n])) nodes)
           tot (into {} (map (fn [n] [n (deg n)])) nodes)]
      (let [[comm' tot' moved]
            (reduce
             (fn [[comm tot _moved :as acc] node]
               (let [ki (deg node)
                     ci (comm node)
                     tot-wo (update tot ci - ki)
                     nbr-comm-w (reduce-kv (fn [mm nbr w]
                                             (if (= nbr node)
                                               mm
                                               (update mm (comm nbr) (fnil + 0.0) w)))
                                           {} (adj node {}))
                     gain (fn [c]
                            (- (/ (get nbr-comm-w c 0.0) m)
                               (/ (* resolution (get tot-wo c 0.0) ki) (* 2.0 m m))))
                     candidates (conj (set (keys nbr-comm-w)) ci)
                     [bc bg] (reduce (fn [[bc bg] c]
                                       (let [g (gain c)]
                                         (if (> g bg) [c g] [bc bg])))
                                     [ci (gain ci)]
                                     candidates)]
                 (if (= bc ci)
                   acc
                   [(assoc comm node bc)
                    (update tot-wo bc (fnil + 0.0) ki)
                    true])))
             [comm tot false]
             nodes)]
        (if moved
          (recur comm' tot')
          comm)))))

(defn- lv-aggregate
  "Contract communities into super-nodes; inter/intra weights summed (intra
   become self-loops)."
  [adj comm]
  (reduce-kv (fn [m i nbrs]
               (reduce-kv (fn [m j w]
                            (update-in m [(comm i) (comm j)] (fnil + 0.0) w))
                          m nbrs))
             {} adj))

(defn ^{:datahike/cost iterative-exec-cost} louvain
  "Louvain community detection (multi-level modularity maximization) over the
   undirected, weighted view of `g`. Returns
     {:communities {node community-id} :modularity Q :levels n :stats ...}.

   Options: :resolution (1.0; higher ⇒ more, smaller communities),
            :max-levels (10)."
  [g db & {:keys [resolution max-levels] :or {resolution 1.0 max-levels 10}}]
  (let [mg (gs/materialize g db)
        wedges (gs/weighted-edges mg db)
        adj0 (reduce (fn [m [s t w]]
                       (-> m
                           (update-in [s t] (fnil + 0.0) (double w))
                           (update-in [t s] (fnil + 0.0) (double w))))
                     {} wedges)
        m2 (reduce-kv (fn [s _ nbrs] (+ s (reduce + 0.0 (vals nbrs)))) 0.0 adj0)]
    (if (or (empty? adj0) (zero? m2))
      {:communities {} :modularity 0.0 :levels 0 :stats (community-stats {})}
      (loop [adj adj0
             node->super (into {} (map (fn [n] [n n])) (keys adj0))
             level 0]
        (let [comm (lv-one-level adj m2 resolution)
              n-comms (count (set (vals comm)))
              node->super' (into {} (map (fn [[orig super]] [orig (comm super)])) node->super)]
          (if (or (>= (inc level) max-levels) (= n-comms (count adj)))
            (let [final node->super']
              {:communities final
               :modularity (lv-modularity adj0 final m2)
               :levels (inc level)
               :stats (community-stats final)})
            (recur (lv-aggregate adj comm) node->super' (inc level))))))))

;; ===========================================================================
;; Minimum spanning tree (Prim) and max-flow / min-cut (Edmonds-Karp)
;; ===========================================================================

(defn ^{:datahike/cost loglinear-exec-cost} prim-mst
  "Minimum spanning tree (Prim) over the undirected weighted view of `g`.
   Returns {:edges [[a b weight] ...] :total-weight n} or nil if empty.
   Spans the component of `:start` (default an arbitrary node)."
  [g db & {:keys [start]}]
  (let [mg (gs/materialize g db)
        adj (reduce (fn [m [s t w]]
                      (-> m
                          (update s (fnil assoc {}) t w)
                          (update t (fnil assoc {}) s w)))
                    {} (gs/weighted-edges mg db))
        all-nodes (set (keys adj))]
    (when (seq all-nodes)
      (let [start-node (or start (first all-nodes))]
        (loop [mst-edges []
               total-weight 0
               in-mst #{start-node}
               ;; sorted-set of [weight from to] — fully comparable ⇒ total order
               candidates (into (sorted-set)
                                (for [[nb w] (adj start-node {})] [w start-node nb]))]
          (if (or (empty? candidates) (= (count in-mst) (count all-nodes)))
            {:edges mst-edges :total-weight total-weight}
            (let [[w from to] (first candidates)
                  candidates' (disj candidates (first candidates))]
              (if (in-mst to)
                (recur mst-edges total-weight in-mst candidates')
                (recur (conj mst-edges [from to w])
                       (+ total-weight w)
                       (conj in-mst to)
                       (into candidates'
                             (for [[nb w2] (adj to {}) :when (not (in-mst nb))]
                               [w2 to nb])))))))))))

(defn mst-weight
  "Total weight of the minimum spanning tree."
  [g db & opts]
  (:total-weight (apply prim-mst g db opts)))

(defn ^{:datahike/cost quadratic-exec-cost} max-flow
  "Maximum flow from `source` to `sink` (Edmonds-Karp, O(VE^2)). Edge capacities
   come from the spec's edge-weight. Returns {:flow n :flow-map {[a b] f}}.
   (Antiparallel real edges are not supported — model them via an intermediate
   node.)"
  [g db source sink]
  (let [mg (gs/materialize g db)
        wedges (gs/weighted-edges mg db)
        capacity (reduce (fn [m [s t c]] (assoc m [s t] (or c 0))) {} wedges)
        adj (reduce (fn [m [s t _]]
                      (-> m
                          (update s (fnil conj #{}) t)
                          (update t (fnil conj #{}) s)))
                    {} wedges)
        residual (fn [flow-map node nbr]
                   (+ (- (get capacity [node nbr] 0) (get flow-map [node nbr] 0))
                      (get flow-map [nbr node] 0)))
        find-path (fn [flow-map]
                    (loop [queue (conj gu/empty-queue source)
                           parent {source nil}]
                      (if (empty? queue)
                        nil
                        (let [node (peek queue)]
                          (if (= node sink)
                            (loop [path [] n sink]
                              (if (nil? (parent n))
                                (vec (reverse path))
                                (recur (conj path [(parent n) n]) (parent n))))
                            (let [[q' p']
                                  (reduce (fn [[q p] nbr]
                                            (if (and (not (contains? p nbr))
                                                     (pos? (residual flow-map node nbr)))
                                              [(conj q nbr) (assoc p nbr node)]
                                              [q p]))
                                          [(pop queue) parent]
                                          (adj node #{}))]
                              (recur q' p')))))))]
    (loop [flow-map {}
           total 0]
      (if-let [path (find-path flow-map)]
        (let [bottleneck (reduce (fn [mn [from to]] (min mn (residual flow-map from to)))
                                 gu/infinity path)
              flow-map' (reduce (fn [fm [from to]]
                                  (if (pos? (get capacity [from to] 0))
                                    (update fm [from to] (fnil + 0) bottleneck)
                                    (update fm [to from] (fnil - 0) bottleneck)))
                                flow-map path)]
          (recur flow-map' (+ total bottleneck)))
        {:flow total
         :flow-map (into {} (filter (fn [[_ v]] (pos? v))) flow-map)}))))

(defn ^{:datahike/cost quadratic-exec-cost} min-cut
  "Minimum s-t cut value (= max flow)."
  [g db source sink]
  (:flow (max-flow g db source sink)))

;; ===========================================================================
;; Random walks (seeded, deterministic, portable)
;; ===========================================================================

(defn- weighted-pick
  "Pick an index from `weights` proportional to weight, using rng state. Returns
   [rng' index]. Falls back to uniform when all weights are zero."
  [rng weights]
  (let [total (reduce + 0.0 weights)
        n (count weights)]
    (if (<= total 0.0)
      (gu/rng-int rng n)
      (let [[rng' u] (gu/rng-next rng)
            r (* u total)]
        (loop [i 0 cum 0.0]
          (let [cum' (+ cum (nth weights i))]
            (if (or (< r cum') (= i (dec n)))
              [rng' i]
              (recur (inc i) cum'))))))))

(defn- do-uniform-walk [mg db start walk-length rng]
  (loop [path [start] current start steps 0 rng rng]
    (if (>= steps walk-length)
      path
      (let [nbrs (vec (gs/out-neighbors mg db current))]
        (if (empty? nbrs)
          path
          (let [[rng' nxt] (gu/rng-nth rng nbrs)]
            (recur (conj path nxt) nxt (inc steps) rng')))))))

(defn- do-weighted-walk [mg db start walk-length rng]
  (loop [path [start] current start steps 0 rng rng]
    (if (>= steps walk-length)
      path
      (let [nbrs (vec (gs/out-neighbors mg db current))]
        (if (empty? nbrs)
          path
          (let [ws (mapv #(max 0.0 (double (gs/edge-weight mg db current %))) nbrs)
                [rng' i] (weighted-pick rng ws)]
            (recur (conj path (nth nbrs i)) (nth nbrs i) (inc steps) rng')))))))

(defn- do-biased-walk [mg db start walk-length p q rng]
  (loop [path [start] prev nil current start steps 0 rng rng]
    (if (>= steps walk-length)
      path
      (let [nbrs (vec (gs/out-neighbors mg db current))]
        (if (empty? nbrs)
          path
          (let [prev-nbrs (if prev (set (gs/out-neighbors mg db prev)) #{})
                ws (mapv (fn [x] (cond (= x prev) (/ 1.0 p)
                                       (prev-nbrs x) 1.0
                                       :else (/ 1.0 q)))
                         nbrs)
                [rng' i] (weighted-pick rng ws)]
            (recur (conj path (nth nbrs i)) current (nth nbrs i) (inc steps) rng')))))))

(defn random-walk
  "Single uniform random walk of up to `walk-length` steps from `start`,
   following out-edges. Returns a vector of nodes (terminates early at a dead
   end). Deterministic given `:seed` (default 42)."
  [g db start walk-length & {:keys [seed] :or {seed 42}}]
  (do-uniform-walk (gs/materialize g db) db start walk-length (gu/rng seed)))

(defn weighted-random-walk
  "Random walk where the next node is chosen with probability proportional to
   the out-edge weight. Deterministic given `:seed` (default 42)."
  [g db start walk-length & {:keys [seed] :or {seed 42}}]
  (do-weighted-walk (gs/materialize g db) db start walk-length (gu/rng seed)))

(defn biased-random-walk
  "Node2Vec-style biased walk with return parameter `p` and in-out parameter
   `q`. Deterministic given `:seed` (default 42)."
  [g db start walk-length p q & {:keys [seed] :or {seed 42}}]
  (do-biased-walk (gs/materialize g db) db start walk-length p q (gu/rng seed)))

(defn random-walks
  "Generate `walks-per-node` uniform walks from each source node (default all
   nodes). Materializes the graph once. Returns a seq of walks. Each walk gets a
   distinct deterministic seed derived from `:seed` (default 42)."
  [g db walk-length walks-per-node & {:keys [seed source-nodes] :or {seed 42}}]
  (let [mg (gs/materialize g db)
        sources (or source-nodes (gs/all-nodes mg db))]
    (for [node sources
          wi (range walks-per-node)]
      (do-uniform-walk mg db node walk-length
                       (gu/rng (+ seed (* node 1000003) wi))))))

(defn biased-random-walks
  "Generate `walks-per-node` Node2Vec biased walks (params `p`,`q`) from each
   source node (default all nodes). Materializes once. Deterministic seeds."
  [g db walk-length walks-per-node p q & {:keys [seed source-nodes] :or {seed 42}}]
  (let [mg (gs/materialize g db)
        sources (or source-nodes (gs/all-nodes mg db))]
    (for [node sources
          wi (range walks-per-node)]
      (do-biased-walk mg db node walk-length p q
                      (gu/rng (+ seed (* node 1000003) wi))))))

;; ===========================================================================
;; Neighborhood / link prediction
;; ===========================================================================
;; Direction is encoded by the spec: pass (reverse-graph g) for incoming or
;; (undirected-graph g) for both.

(defn neighbors
  "Set of out-neighbors of `node`."
  [g db node]
  (set (gs/out-neighbors g db node)))

(defn degree
  "Number of out-neighbors of `node`."
  [g db node]
  (count (gs/out-neighbors g db node)))

(defn common-neighbors-set
  "Set of nodes that are out-neighbors of both `a` and `b`."
  [g db a b]
  (set/intersection (neighbors g db a) (neighbors g db b)))

(defn common-neighbors
  "Count of common neighbors |Γ(a) ∩ Γ(b)|."
  [g db a b]
  (count (common-neighbors-set g db a b)))

(defn adamic-adar
  "Adamic–Adar index: Σ 1/log(deg z) over common neighbors z (deg z > 1)."
  [g db a b]
  (let [common (common-neighbors-set g db a b)]
    (reduce + 0.0
            (for [z common
                  :let [d (degree g db z)]
                  :when (> d 1)]
              (/ 1.0 (Math/log d))))))

(defn resource-allocation
  "Resource-allocation index: Σ 1/deg z over common neighbors z (deg z > 0)."
  [g db a b]
  (let [common (common-neighbors-set g db a b)]
    (reduce + 0.0
            (for [z common
                  :let [d (degree g db z)]
                  :when (pos? d)]
              (/ 1.0 d)))))

(defn preferential-attachment
  "Preferential attachment score |Γ(a)| × |Γ(b)|."
  [g db a b]
  (* (degree g db a) (degree g db b)))

(defn total-neighbors
  "Size of the combined neighborhood |Γ(a) ∪ Γ(b)|."
  [g db a b]
  (count (set/union (neighbors g db a) (neighbors g db b))))

(defn jaccard-index
  "Jaccard index |Γ(a) ∩ Γ(b)| / |Γ(a) ∪ Γ(b)| in [0,1]."
  [g db a b]
  (let [na (neighbors g db a)
        nb (neighbors g db b)
        uni (count (set/union na nb))]
    (if (zero? uni)
      0.0
      (/ (double (count (set/intersection na nb))) uni))))

(defn node-similarity
  "Nodes most similar to `node` by neighborhood Jaccard. Returns a vector of
   [other score] pairs sorted by score descending (a real ordering — not a map).

   Options: :min-similarity (0.0), :top-k."
  [g db node & {:keys [min-similarity top-k] :or {min-similarity 0.0}}]
  (let [mg (gs/materialize g db)
        nn (neighbors mg db node)
        others (disj (set (gs/all-nodes mg db)) node)
        sims (for [other others
                   :let [on (neighbors mg db other)
                         uni (count (set/union nn on))
                         sim (if (zero? uni)
                               0.0
                               (double (/ (count (set/intersection nn on)) uni)))]
                   :when (>= sim min-similarity)]
               [other sim])
        sorted (sort-by second > sims)]
    (vec (if top-k (take top-k sorted) sorted))))

(defn same-community
  "1.0 if `a` and `b` share a community in the `communities` map (e.g. from
   louvain), else 0.0."
  [communities a b]
  (if (and (contains? communities a)
           (= (communities a) (communities b)))
    1.0
    0.0))

(defn link-prediction-candidates
  "Score candidate (non-edge) pairs that share at least one common neighbor.
   `score-fn` is called as (score-fn g db a b) — e.g. common-neighbors,
   adamic-adar, resource-allocation. Returns a vector of
   {:source :target :score} sorted by score descending.

   Options: :limit (100), :min-score (0). O(Σ deg²), not O(n²)."
  [g db score-fn & {:keys [limit min-score] :or {limit 100 min-score 0}}]
  (let [mg (gs/materialize g db)
        nodes (gs/all-nodes mg db)
        pairs (into #{}
                    (for [z nodes
                          :let [ins (vec (gs/in-neighbors mg db z))]
                          a ins
                          b ins
                          :when (< a b)
                          :when (not (contains? (set (gs/out-neighbors mg db a)) b))]
                      [a b]))]
    (->> pairs
         (map (fn [[a b]] {:source a :target b :score (score-fn mg db a b)}))
         (filter #(>= (:score %) min-score))
         (sort-by :score >)
         (take limit)
         vec)))
