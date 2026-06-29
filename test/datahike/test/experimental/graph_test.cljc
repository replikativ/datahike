(ns datahike.test.experimental.graph-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing]])
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.experimental.graph-spec :as gs]
   [datahike.experimental.graph :as g]))

;; ---------------------------------------------------------------------------
;; Fixtures — build a directed graph from [from to] name pairs, synchronously
;; (db-with works identically on JVM and ClojureScript).
;; ---------------------------------------------------------------------------

(def ^:private schema
  {:node/name {:db/unique :db.unique/identity}
   :e/to {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}})

(defn build
  "Build a db from [from to] name pairs. Returns {:db :id :name} where :id maps
   a name to its entity id and :name the inverse."
  [name-edges]
  (let [names (vec (distinct (mapcat identity name-edges)))
        db0 (-> (db/empty-db schema)
                (d/db-with (mapv (fn [n] {:node/name n}) names)))
        dbf (d/db-with db0 (mapv (fn [[a b]] [:db/add [:node/name a] :e/to [:node/name b]])
                                 name-edges))
        id (into {} (for [n names]
                      [n (ffirst (d/q '[:find ?e :in $ ?n :where [?e :node/name ?n]] dbf n))]))]
    {:db dbf :id id :name (into {} (map (fn [[k v]] [v k])) id)}))

(defn- named-set [fixture eids]
  (set (map (:name fixture) eids)))

;; DAG with two components: A→B→C, A→C, C→D ; and E→F
(def dag (build [["A" "B"] ["B" "C"] ["A" "C"] ["C" "D"] ["E" "F"]]))
;; Cyclic: X→Y→Z→X, plus W→X (W leads into the cycle, not on it)
(def cyc (build [["X" "Y"] ["Y" "Z"] ["Z" "X"] ["W" "X"]]))

(defn g-of [fixture] [(gs/attr-graph :e/to) (:db fixture)])

;; ---------------------------------------------------------------------------

(deftest test-transitive-closure
  (let [[gr db] (g-of dag)
        id (:id dag)]
    (is (= #{"B" "C" "D"} (named-set dag (g/transitive-closure gr db (id "A"))))
        "reachable from A excludes A (not on a cycle)"))
  (let [[gr db] (g-of cyc)
        id (:id cyc)]
    (is (= #{"X" "Y" "Z"} (named-set cyc (g/transitive-closure gr db (id "X"))))
        "X is included because it lies on a cycle")))

(deftest test-reachable?
  (let [[gr db] (g-of dag) id (:id dag)]
    (is (g/reachable? gr db (id "A") (id "D")))
    (is (not (g/reachable? gr db (id "D") (id "A"))))
    (is (not (g/reachable? gr db (id "A") (id "A"))) "no self-cycle in a DAG"))
  (let [[gr db] (g-of cyc) id (:id cyc)]
    (is (g/reachable? gr db (id "X") (id "X")) "X reachable from itself via the cycle")
    (is (g/reachable? gr db (id "W") (id "Z")))
    (is (not (g/reachable? gr db (id "X") (id "W"))) "W is upstream only")))

(deftest test-shortest-path
  (let [[gr db] (g-of dag) id (:id dag) nm (:name dag)]
    (is (= ["A" "C" "D"] (mapv nm (g/shortest-path gr db (id "A") (id "D"))))
        "prefers the 2-hop A→C→D over A→B→C→D")
    (is (= 2 (g/path-length gr db (id "A") (id "D"))))
    (is (= ["A"] (mapv nm (g/shortest-path gr db (id "A") (id "A")))))
    (is (nil? (g/shortest-path gr db (id "A") (id "E"))) "different component")
    (is (nil? (g/path-length gr db (id "A") (id "E"))))))

(deftest test-all-paths
  (let [[gr db] (g-of dag) id (:id dag) nm (:name dag)
        paths (->> (g/all-paths gr db (id "A") {:target (id "D")})
                   (map #(mapv nm %))
                   set)]
    (is (= #{["A" "C" "D"] ["A" "B" "C" "D"]} paths)
        "both A→D paths enumerated")))

(deftest test-connected-components
  (let [[gr db] (g-of dag) id (:id dag)]
    (is (= #{"A" "B" "C" "D"} (named-set dag (g/connected-component gr db (id "A")))))
    (is (= #{"E" "F"} (named-set dag (g/connected-component gr db (id "E")))))
    (is (g/same-component? gr db (id "A") (id "D")))
    (is (not (g/same-component? gr db (id "A") (id "E"))))
    (let [comps (set (map #(named-set dag %) (g/connected-components gr db)))]
      (is (= #{#{"A" "B" "C" "D"} #{"E" "F"}} comps)))))

(deftest test-strongly-connected-components
  (let [[gr db] (g-of cyc)
        sccs (set (map #(named-set cyc %) (g/strongly-connected-components gr db)))]
    (is (contains? sccs #{"X" "Y" "Z"}) "the cycle is one SCC")
    (is (contains? sccs #{"W"}) "W is its own SCC")
    (is (= 2 (count sccs))))
  (let [[gr db] (g-of dag)
        sccs (g/strongly-connected-components gr db)]
    (is (= 6 (count sccs)) "a DAG has all-singleton SCCs")
    (is (every? #(= 1 (count %)) sccs))))

(deftest test-topological-sort
  (let [[gr db] (g-of dag) nm (:name dag)
        order (g/topological-sort gr db)
        pos (into {} (map-indexed (fn [i n] [n i]) order))]
    (is (= 6 (count order)))
    (is (= #{"A" "B" "C" "D" "E" "F"} (set (map nm order))))
    (doseq [[s t] (gs/all-edges (gs/attr-graph :e/to) db)]
      (is (< (pos s) (pos t)) "every edge points forward in the order"))
    (is (false? (g/has-cycle? gr db))))
  (let [[gr db] (g-of cyc)]
    (is (nil? (g/topological-sort gr db)) "cyclic graph has no topo order")
    (is (true? (g/has-cycle? gr db)))))

(deftest test-find-cycle
  (let [[gr db] (g-of dag)]
    (is (nil? (g/find-cycle gr db)) "no cycle in a DAG"))
  (let [[gr db] (g-of cyc) nm (:name cyc)
        cycle (g/find-cycle gr db)
        names (mapv nm cycle)]
    (is (= #{"X" "Y" "Z"} (set names)) "finds exactly the cycle nodes (not W)")
    ;; consecutive nodes (and wrap-around) must be real edges
    (let [edges (set (gs/all-edges (gs/attr-graph :e/to) db))
          n (count cycle)]
      (is (every? (fn [i] (contains? edges [(nth cycle i) (nth cycle (mod (inc i) n))]))
                  (range n))
          "every step of the returned cycle is an edge"))))

(deftest test-bipartite-coloring
  ;; even 4-cycle A-B-C-D-A is bipartite
  (let [bip (build [["A" "B"] ["B" "C"] ["C" "D"] ["D" "A"]])
        [gr db] (g-of bip) id (:id bip)
        {:keys [bipartite? coloring]} (g/bipartite-coloring gr db)]
    (is bipartite?)
    (is (not= (coloring (id "A")) (coloring (id "B"))))
    (is (= (coloring (id "A")) (coloring (id "C"))) "opposite corners share a color"))
  ;; triangle is not bipartite
  (let [tri (build [["A" "B"] ["B" "C"] ["C" "A"]])
        [gr db] (g-of tri)]
    (is (false? (:bipartite? (g/bipartite-coloring gr db))))))

(deftest test-find-bridges
  ;; triangle A-B-C (no bridges) + tail C-D + isolated edge E-F
  (let [[gr db] (g-of dag) nm (:name dag)
        bridges (g/find-bridges gr db)
        named (set (map (fn [[a b]] (set [(nm a) (nm b)])) bridges))]
    (is (= #{#{"C" "D"} #{"E" "F"}} named)
        "C-D and E-F are bridges; the A-B-C triangle has none")))

;; ---------------------------------------------------------------------------
;; Batch 2 — weighted paths (reified-edge fixtures)
;; ---------------------------------------------------------------------------

(def ^:private wschema
  {:node/name {:db/unique :db.unique/identity}
   :edge/from {:db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   :edge/to {:db/valueType :db.type/ref :db/cardinality :db.cardinality/one}})

(defn build-weighted
  "Build a reified-edge weighted graph from [from to weight] triples."
  [triples]
  (let [names (vec (distinct (mapcat (fn [[a b _]] [a b]) triples)))
        db0 (-> (db/empty-db wschema)
                (d/db-with (mapv (fn [n] {:node/name n}) names)))
        dbf (d/db-with db0 (mapv (fn [[a b w]]
                                   {:edge/from [:node/name a]
                                    :edge/to [:node/name b]
                                    :edge/weight (double w)})
                                 triples))
        id (into {} (for [n names]
                      [n (ffirst (d/q '[:find ?e :in $ ?n :where [?e :node/name ?n]] dbf n))]))]
    {:db dbf :id id :name (into {} (map (fn [[k v]] [v k])) id)}))

(defn wg-of [fixture]
  [(gs/weighted-graph :edge/from :edge/to :edge/weight) (:db fixture)])

(deftest test-weighted-shortest-path
  (testing "cheaper multi-hop beats expensive direct edge"
    (let [f (build-weighted [["A" "B" 1] ["B" "C" 1] ["A" "C" 5]])
          [gr db] (wg-of f) id (:id f) nm (:name f)
          {:keys [path cost]} (g/weighted-shortest-path gr db (id "A") (id "C"))]
      (is (= ["A" "B" "C"] (mapv nm path)))
      (is (= 2.0 cost))))
  (testing "equal-cost frontier entries are not dropped (dedup-bug regression)"
    ;; S→A and S→B both cost 1 (collide under a cost-only comparator); A is a
    ;; dead end, the real path is S→B→T. The old sorted-set-by code returned nil.
    (let [f (build-weighted [["S" "A" 1] ["S" "B" 1] ["B" "T" 1]])
          [gr db] (wg-of f) id (:id f) nm (:name f)
          {:keys [path cost]} (g/weighted-shortest-path gr db (id "S") (id "T"))]
      (is (= ["S" "B" "T"] (mapv nm path)))
      (is (= 2.0 cost)))))

(deftest test-bottleneck-path
  ;; widest path A→C: via B has bottleneck min(3,2)=2, beating direct A→C cap 1
  (let [f (build-weighted [["A" "B" 3] ["B" "C" 2] ["A" "C" 1]])
        [gr db] (wg-of f) id (:id f) nm (:name f)
        {:keys [path capacity]} (g/bottleneck-path gr db (id "A") (id "C"))]
    (is (= ["A" "B" "C"] (mapv nm path)))
    (is (= 2.0 capacity))))

(deftest test-astar-path
  ;; zero heuristic ⇒ behaves like Dijkstra
  (let [f (build-weighted [["A" "B" 1] ["B" "C" 1] ["A" "C" 5]])
        [gr db] (wg-of f) id (:id f) nm (:name f)
        {:keys [path cost]} (g/astar-path gr db (id "A") (id "C") (fn [_ _] 0))]
    (is (= ["A" "B" "C"] (mapv nm path)))
    (is (= 2.0 cost))))

(deftest test-trust-propagation
  (let [f (build-weighted [["A" "B" 0.5] ["B" "C" 0.5]])
        [gr db] (wg-of f) id (:id f) nm (:name f)
        trust (g/trust-propagation gr db (id "A") :discount 1.0)
        by-name (into {} (map (fn [[k v]] [(nm k) v])) trust)]
    (is (= 1.0 (by-name "A")))
    (is (= 0.5 (by-name "B")))
    (is (= 0.25 (by-name "C")))))

(deftest test-probability-reachability
  (let [f (build-weighted [["A" "B" 0.5] ["B" "C" 0.5] ["A" "C" 0.2]])
        [gr db] (wg-of f) id (:id f)
        p (g/probability-reachability gr db (id "A") (id "C"))]
    (is (< 0.249 p 0.251) "max over paths: 0.5*0.5=0.25 beats direct 0.2")))

(deftest test-semi-naive-transitive-closure
  (let [[gr db] (g-of dag) id (:id dag) nm (:name dag)
        pairs (set (map (fn [[a b]] [(nm a) (nm b)])
                        (g/semi-naive-transitive-closure gr db)))]
    (is (= #{["A" "B"] ["B" "C"] ["A" "C"] ["C" "D"] ["A" "D"] ["B" "D"] ["E" "F"]}
           pairs)
        "all transitively reachable pairs")))

;; ---------------------------------------------------------------------------
;; Batch 3 — centrality (baselines from NetworkX on the path P5)
;; ---------------------------------------------------------------------------

(defn- approx [a b] (< (#?(:clj Math/abs :cljs js/Math.abs) (- a b)) 1e-3))

;; Path: A-B-C-D-E (stored directed; centrality uses the undirected view)
(def path5 (build [["A" "B"] ["B" "C"] ["C" "D"] ["D" "E"]]))

(deftest test-betweenness-centrality
  (let [[gr db] (g-of path5) id (:id path5)
        bc (g/betweenness-centrality gr db)]
    ;; NetworkX betweenness_centrality(P5): ends 0, B/D 0.5, C 0.667
    (is (approx 0.0   (bc (id "A"))))
    (is (approx 0.5   (bc (id "B"))))
    (is (approx 0.667 (bc (id "C"))))
    (is (approx 0.5   (bc (id "D"))))
    (is (approx 0.0   (bc (id "E"))))
    (is (every? #(<= 0.0 % 1.0) (vals bc)) "all within [0,1]")))

(deftest test-closeness-centrality
  (let [[gr db] (g-of path5) id (:id path5)
        cc (g/closeness-centrality gr db)]
    ;; NetworkX closeness_centrality(P5): A/E 0.4, B/D 0.571, C 0.667
    (is (approx 0.4   (cc (id "A"))))
    (is (approx 0.571 (cc (id "B"))))
    (is (approx 0.667 (cc (id "C"))))
    (is (approx 0.571 (cc (id "D"))))
    (is (approx 0.4   (cc (id "E"))))))

(deftest test-page-rank
  (testing "symmetric cycle ⇒ equal scores"
    (let [f (build [["A" "B"] ["B" "C"] ["C" "A"]])
          [gr db] (g-of f) id (:id f)
          pr (g/page-rank gr db)]
      (is (approx (/ 1.0 3) (pr (id "A"))))
      (is (approx (/ 1.0 3) (pr (id "B"))))
      (is (approx (/ 1.0 3) (pr (id "C"))))))
  (testing "dangling node (sink) ⇒ rank still conserved (sums to 1)"
    (let [f (build [["A" "B"] ["B" "C"]]) ;; C is a sink
          [gr db] (g-of f)
          pr (g/page-rank gr db)]
      (is (approx 1.0 (reduce + (vals pr))) "no rank leaks out of the sink")
      (is (every? pos? (vals pr))))))

;; ---------------------------------------------------------------------------
;; Batch 4 — community detection
;; ---------------------------------------------------------------------------

(defn- comm-groups
  "Set of communities (as name-sets) from a {node -> community} map."
  [fixture communities]
  (->> communities
       (group-by val)
       vals
       (map (fn [members] (set (map (comp (:name fixture) first) members))))
       set))

(def ^:private two-triangles
  ;; A-B-C clique and D-E-F clique
  [["A" "B"] ["B" "C"] ["A" "C"] ["D" "E"] ["E" "F"] ["D" "F"]])

(deftest test-louvain
  (testing "two disconnected triangles ⇒ two communities"
    (let [f (build two-triangles) [gr db] (g-of f)
          {:keys [communities modularity]} (g/louvain gr db)]
      (is (= #{#{"A" "B" "C"} #{"D" "E" "F"}} (comm-groups f communities)))
      (is (> modularity 0.3) "clear community structure has positive modularity")))
  (testing "two triangles joined by a bridge ⇒ STILL two communities (not collapsed)"
    (let [f (build (conj two-triangles ["C" "D"])) [gr db] (g-of f)
          {:keys [communities]} (g/louvain gr db)]
      (is (= #{#{"A" "B" "C"} #{"D" "E" "F"}} (comm-groups f communities))
          "the bridge must not merge the two triangles")))
  (testing "a single clique ⇒ one community"
    (let [f (build [["A" "B"] ["A" "C"] ["A" "D"] ["B" "C"] ["B" "D"] ["C" "D"]])
          [gr db] (g-of f)
          {:keys [communities]} (g/louvain gr db)]
      (is (= 1 (count (comm-groups f communities)))))))

(deftest test-label-propagation
  (let [f (build two-triangles) [gr db] (g-of f)
        {:keys [communities converged]} (g/label-propagation gr db)]
    (is converged)
    (is (= #{#{"A" "B" "C"} #{"D" "E" "F"}} (comm-groups f communities))
        "each disconnected triangle homogenizes to one label")))

(deftest test-community-stats
  (let [stats (g/community-stats {1 :a 2 :a 3 :b 4 :c})]
    (is (= 3 (:num-communities stats)))
    (is (= {:a 2 :b 1 :c 1} (:sizes stats)))
    (is (= [:a 2] (:largest stats)))
    (is (approx (/ 2.0 3) (:isolation-score stats)) "two of three communities are singletons")))
