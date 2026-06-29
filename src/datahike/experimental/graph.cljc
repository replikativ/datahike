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
  (:require [datahike.experimental.graph-spec :as gs]
            [datahike.experimental.graph-util :as gu]
            [clojure.set :as set]))

;; ===========================================================================
;; Reachability
;; ===========================================================================

(defn transitive-closure
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

(defn reachable?
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

(defn shortest-path
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

(defn connected-component
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

(defn connected-components
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

(defn strongly-connected-components
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

(defn topological-sort
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

(defn bipartite-coloring
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

(defn find-bridges
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
