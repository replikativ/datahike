(ns datahike.experimental.graph-spec
  "Graph specification protocol — the data-access layer for graph algorithms.

   A GraphSpec abstracts *how* a graph is read out of a Datahike database, so
   the algorithms in `datahike.experimental.graph` work uniformly over:
     - simple ref edges          (attr-graph)
     - a union of edge attrs     (multi-attr-graph)
     - reified weighted edges    (weighted-graph)
     - transformed views         (reverse-graph, undirected-graph,
                                   filtered-graph, subgraph)
     - an in-memory snapshot      (materialize)
     - an arbitrary query         (query-graph)

   Two access methods matter for performance:
     - index-backed (attr-graph): O(log n + degree) per neighbor lookup off
       EAVT/AVET, zero up-front cost — best for local/bounded traversals.
     - materialized (materialize): O(E) up-front build, then O(1) lookups —
       best for whole-graph algorithms that visit most nodes repeatedly.

   Algorithms take `(graph-spec db ...)`. Construct a spec, then call an
   algorithm:

     (let [g (attr-graph :follows)]
       (reachable g db alice))"
  (:require [datahike.api :as d]
            [datahike.db.utils :as dbu]))

;; =============================================================================
;; GraphSpec protocol
;; =============================================================================

(defprotocol GraphSpec
  "Abstraction over graph access patterns. Implementations define how to read
   adjacency, edges, nodes and weights from a Datahike database."

  (out-neighbors [this db node]
    "Seq of nodes reachable via outgoing edges from `node` (nil/empty if none).")

  (in-neighbors [this db node]
    "Seq of nodes with edges into `node` (nil/empty if none).")

  (all-edges [this db]
    "Seq of [source target] pairs for every edge in the graph.")

  (all-nodes [this db]
    "Seq of all node ids in the graph.")

  (edge-weight [this db source target]
    "Weight of the edge source->target. 1 for unweighted graphs.")

  (directed? [this]
    "True if the graph is directed."))

;; =============================================================================
;; Constructors
;; =============================================================================

(defn attr-graph
  "Unweighted graph from a single ref edge attribute: each datom [e attr v]
   is an edge e->v.

     (attr-graph :follows)"
  [edge-attr]
  (reify GraphSpec
    (out-neighbors [_ db node]
      ;; Forward adjacency off EAVT — always present, O(log n + degree).
      (seq (map :v (d/datoms db :eavt node edge-attr))))

    (in-neighbors [_ db node]
      ;; Reverse adjacency off AVET when the attr is indexed (refs, :db/index,
      ;; :db/unique all live in AVET); fall back to a query otherwise, where
      ;; AVET would be empty and silently wrong.
      (if (dbu/indexing? db edge-attr)
        (seq (map :e (d/datoms db :avet edge-attr node)))
        (seq (d/q '[:find [?e ...]
                    :in $ ?target ?attr
                    :where [?e ?attr ?target]]
                  db node edge-attr))))

    (all-edges [_ db]
      (map (juxt :e :v) (d/datoms db :aevt edge-attr)))

    (all-nodes [_ db]
      (distinct
       (mapcat (fn [datom] [(:e datom) (:v datom)])
               (d/datoms db :aevt edge-attr))))

    (edge-weight [_ _db _source _target] 1)

    (directed? [_] true)))

(defn multi-attr-graph
  "Unweighted graph from the union of several ref edge attributes.

     (multi-attr-graph [:follows :friends :colleagues])"
  [edge-attrs]
  (reify GraphSpec
    (out-neighbors [_ db node]
      (seq (distinct
            (mapcat (fn [attr] (map :v (d/datoms db :eavt node attr)))
                    edge-attrs))))

    (in-neighbors [_ db node]
      (seq (distinct
            (mapcat (fn [attr]
                      (if (dbu/indexing? db attr)
                        (map :e (d/datoms db :avet attr node))
                        (d/q '[:find [?e ...]
                               :in $ ?target ?attr
                               :where [?e ?attr ?target]]
                             db node attr)))
                    edge-attrs))))

    (all-edges [_ db]
      (mapcat (fn [attr] (map (juxt :e :v) (d/datoms db :aevt attr)))
              edge-attrs))

    (all-nodes [_ db]
      (distinct
       (mapcat (fn [attr]
                 (mapcat (fn [datom] [(:e datom) (:v datom)])
                         (d/datoms db :aevt attr)))
               edge-attrs)))

    (edge-weight [_ _db _source _target] 1)

    (directed? [_] true)))

(defn weighted-graph
  "Weighted graph from REIFIED edges: each edge is an entity carrying a source
   ref, a target ref and a weight, e.g.

     {:edge/from a :edge/to b :edge/weight 3.0}

   Construct with the three attributes:

     (weighted-graph :edge/from :edge/to :edge/weight)

   This is the only sound model for true per-edge weights (a weight attached to
   a node would be shared by all of that node's out-edges)."
  [from-attr to-attr weight-attr]
  (reify GraphSpec
    (out-neighbors [_ db node]
      (seq (d/q '[:find [?t ...]
                  :in $ ?n ?fa ?ta
                  :where [?e ?fa ?n] [?e ?ta ?t]]
                db node from-attr to-attr)))

    (in-neighbors [_ db node]
      (seq (d/q '[:find [?s ...]
                  :in $ ?n ?fa ?ta
                  :where [?e ?ta ?n] [?e ?fa ?s]]
                db node from-attr to-attr)))

    (all-edges [_ db]
      (d/q '[:find ?s ?t
             :in $ ?fa ?ta
             :where [?e ?fa ?s] [?e ?ta ?t]]
           db from-attr to-attr))

    (all-nodes [_ db]
      (distinct
       (mapcat identity
               (d/q '[:find ?s ?t
                      :in $ ?fa ?ta
                      :where [?e ?fa ?s] [?e ?ta ?t]]
                    db from-attr to-attr))))

    (edge-weight [_ db source target]
      (or (ffirst (d/q '[:find ?w
                         :in $ ?s ?t ?fa ?ta ?wa
                         :where [?e ?fa ?s] [?e ?ta ?t] [?e ?wa ?w]]
                       db source target from-attr to-attr weight-attr))
          1))

    (directed? [_] true)))

(defn query-graph
  "Graph from a Datalog query returning [source target] pairs. Edges are
   loaded once and cached on first access.

     (query-graph '[:find ?a ?b :where [?a :trusts ?b] [?b :trusts ?a]])"
  [query & inputs]
  (let [edges-cache (atom nil)
        edges (fn [db] (or @edges-cache
                           (reset! edges-cache (vec (apply d/q query db inputs)))))]
    (reify GraphSpec
      (out-neighbors [_ db node]
        (seq (for [[s t] (edges db) :when (= s node)] t)))
      (in-neighbors [_ db node]
        (seq (for [[s t] (edges db) :when (= t node)] s)))
      (all-edges [_ db] (edges db))
      (all-nodes [_ db] (distinct (mapcat (fn [[s t]] [s t]) (edges db))))
      (edge-weight [_ _db _source _target] 1)
      (directed? [_] true))))

;; =============================================================================
;; Transformers
;; =============================================================================

(defn reverse-graph
  "View `spec` with edge directions reversed."
  [spec]
  (reify GraphSpec
    (out-neighbors [_ db node] (in-neighbors spec db node))
    (in-neighbors [_ db node] (out-neighbors spec db node))
    (all-edges [_ db] (map (fn [[s t]] [t s]) (all-edges spec db)))
    (all-nodes [_ db] (all-nodes spec db))
    (edge-weight [_ db source target] (edge-weight spec db target source))
    (directed? [_] (directed? spec))))

(defn undirected-graph
  "View `spec` as undirected — both directions are traversable."
  [spec]
  (reify GraphSpec
    (out-neighbors [_ db node]
      (seq (distinct (concat (out-neighbors spec db node)
                             (in-neighbors spec db node)))))
    (in-neighbors [this db node] (out-neighbors this db node))
    (all-edges [_ db]
      (let [edges (all-edges spec db)]
        (distinct (concat edges (map (fn [[s t]] [t s]) edges)))))
    (all-nodes [_ db] (all-nodes spec db))
    (edge-weight [_ db source target]
      (or (edge-weight spec db source target)
          (edge-weight spec db target source)))
    (directed? [_] false)))

(defn filtered-graph
  "View `spec` keeping only nodes satisfying `(node-pred db node)`."
  [spec node-pred]
  (reify GraphSpec
    (out-neighbors [_ db node]
      (when (node-pred db node)
        (seq (filter #(node-pred db %) (out-neighbors spec db node)))))
    (in-neighbors [_ db node]
      (when (node-pred db node)
        (seq (filter #(node-pred db %) (in-neighbors spec db node)))))
    (all-edges [_ db]
      (filter (fn [[s t]] (and (node-pred db s) (node-pred db t)))
              (all-edges spec db)))
    (all-nodes [_ db] (filter #(node-pred db %) (all-nodes spec db)))
    (edge-weight [_ db source target]
      (when (and (node-pred db source) (node-pred db target))
        (edge-weight spec db source target)))
    (directed? [_] (directed? spec))))

(defn subgraph
  "View `spec` restricted to `node-set`."
  [spec node-set]
  (filtered-graph spec (fn [_db node] (contains? node-set node))))

(defn materialize
  "Snapshot `spec` against `db` into in-memory adjacency + weight maps, returning
   a GraphSpec with O(1) lookups. Pay O(E) once; best for whole-graph algorithms
   that visit most nodes. The result ignores the `db` passed to its methods.

   For local/bounded traversals do NOT materialize — an index-backed attr-graph
   only touches the nodes it visits."
  [spec db]
  (let [edges (vec (all-edges spec db))
        fwd (persistent!
             (reduce (fn [m [s t]] (assoc! m s (conj (get m s []) t)))
                     (transient {}) edges))
        rev (persistent!
             (reduce (fn [m [s t]] (assoc! m t (conj (get m t []) s)))
                     (transient {}) edges))
        wmap (persistent!
              (reduce (fn [m [s t]] (assoc! m [s t] (edge-weight spec db s t)))
                      (transient {}) edges))
        nodes (vec (distinct (mapcat (fn [[s t]] [s t]) edges)))
        directed (directed? spec)]
    (reify GraphSpec
      (out-neighbors [_ _db node] (seq (get fwd node)))
      (in-neighbors  [_ _db node] (seq (get rev node)))
      (all-edges     [_ _db] edges)
      (all-nodes     [_ _db] nodes)
      (edge-weight   [_ _db source target] (get wmap [source target] 1))
      (directed?     [_] directed))))

;; =============================================================================
;; Helpers
;; =============================================================================

(defn weighted-edges
  "Seq of [source target weight] triples for every edge. On a materialized
   spec the weight lookup is O(1)."
  [spec db]
  (map (fn [[s t]] [s t (edge-weight spec db s t)]) (all-edges spec db)))

(defn ensure-materialized
  "Return `spec` if it is already a cheap in-memory snapshot, else materialize
   it. Algorithms that need repeated whole-graph access call this so callers can
   pass either an index-backed spec or a pre-built one."
  [spec db]
  (materialize spec db))
