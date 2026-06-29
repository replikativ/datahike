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
