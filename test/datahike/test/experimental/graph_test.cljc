(ns datahike.test.experimental.graph-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj [clojure.test :as t :refer [is deftest testing]])
   [datahike.api :as d]
   [datahike.experimental.graph :as graph]))

;; =============================================================================
;; Test Fixtures - Graph Database Setup
;; =============================================================================

(def test-schema
  [{:db/ident :person/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :person/follows
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many}
   {:db/ident :person/friend
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many}
   {:db/ident :edge/from
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :edge/to
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :edge/weight
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :edge/capacity
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :edge/trust
    :db/valueType :db.type/double
    :db/cardinality :db.cardinality/one}
   {:db/ident :edge/prob
    :db/valueType :db.type/double
    :db/cardinality :db.cardinality/one}])

(defn create-test-db
  "Create an in-memory test database with schema."
  []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write
             :keep-history? false}]
    (d/delete-database cfg)
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn test-schema)
      conn)))

;; =============================================================================
;; Simple Graph Fixture
;; =============================================================================
;;
;;  Alice(1) --> Bob(2) --> Carol(3)
;;     |                       |
;;     v                       v
;;  Dave(4) ----------------> Eve(5)
;;
;; This creates a DAG with multiple paths from Alice to Eve.

(defn setup-simple-graph [conn]
  (d/transact conn
              [{:db/id -1 :person/name "Alice"}
               {:db/id -2 :person/name "Bob"}
               {:db/id -3 :person/name "Carol"}
               {:db/id -4 :person/name "Dave"}
               {:db/id -5 :person/name "Eve"}
     ;; Edges
               {:db/id -1 :person/follows [-2 -4]}   ; Alice -> Bob, Dave
               {:db/id -2 :person/follows [-3]}       ; Bob -> Carol
               {:db/id -3 :person/follows [-5]}       ; Carol -> Eve
               {:db/id -4 :person/follows [-5]}])     ; Dave -> Eve
  ;; Return entity IDs
  (let [db @conn
        get-id (fn [name]
                 (d/q '[:find ?e . :in $ ?n :where [?e :person/name ?n]] db name))]
    {:alice (get-id "Alice")
     :bob (get-id "Bob")
     :carol (get-id "Carol")
     :dave (get-id "Dave")
     :eve (get-id "Eve")}))

;; =============================================================================
;; Cyclic Graph Fixture
;; =============================================================================
;;
;;  A(1) --> B(2) --> C(3)
;;    ^               |
;;    +---------------+
;;
;; Simple cycle for testing cycle detection.

(defn setup-cyclic-graph [conn]
  (d/transact conn
              [{:db/id -1 :person/name "A"}
               {:db/id -2 :person/name "B"}
               {:db/id -3 :person/name "C"}
               {:db/id -1 :person/follows [-2]}
               {:db/id -2 :person/follows [-3]}
               {:db/id -3 :person/follows [-1]}])  ; Creates cycle
  (let [db @conn
        get-id (fn [name]
                 (d/q '[:find ?e . :in $ ?n :where [?e :person/name ?n]] db name))]
    {:a (get-id "A") :b (get-id "B") :c (get-id "C")}))

;; =============================================================================
;; Disconnected Graph Fixture
;; =============================================================================
;;
;;  Component 1: A -- B -- C
;;  Component 2: X -- Y
;;

(defn setup-disconnected-graph [conn]
  (d/transact conn
              [{:db/id -1 :person/name "A"}
               {:db/id -2 :person/name "B"}
               {:db/id -3 :person/name "C"}
               {:db/id -4 :person/name "X"}
               {:db/id -5 :person/name "Y"}
     ;; Component 1 (using friend for undirected)
               {:db/id -1 :person/friend [-2]}
               {:db/id -2 :person/friend [-1 -3]}
               {:db/id -3 :person/friend [-2]}
     ;; Component 2
               {:db/id -4 :person/friend [-5]}
               {:db/id -5 :person/friend [-4]}])
  (let [db @conn
        get-id (fn [name]
                 (d/q '[:find ?e . :in $ ?n :where [?e :person/name ?n]] db name))]
    {:a (get-id "A") :b (get-id "B") :c (get-id "C")
     :x (get-id "X") :y (get-id "Y")}))

;; =============================================================================
;; Weighted Graph Fixture
;; =============================================================================
;;
;;  A --5--> B --3--> D
;;  |        |
;;  2        1
;;  v        v
;;  C --1--> D
;;
;; Shortest path A->D: A->C->D (cost 3) not A->B->D (cost 8)

(defn setup-weighted-graph [conn]
  (d/transact conn
              [{:db/id -1 :person/name "A"}
               {:db/id -2 :person/name "B"}
               {:db/id -3 :person/name "C"}
               {:db/id -4 :person/name "D"}
     ;; Edges with weights (edge entities)
               {:db/id -10 :edge/to -2 :edge/weight 5}   ; A->B (5)
               {:db/id -11 :edge/to -3 :edge/weight 2}   ; A->C (2)
               {:db/id -12 :edge/to -4 :edge/weight 3}   ; B->D (3)
               {:db/id -13 :edge/to -4 :edge/weight 1}   ; C->D (1)
               {:db/id -14 :edge/to -4 :edge/weight 1}   ; B->D (via C) (1)
     ;; Link persons to their outgoing edges
               {:db/id -1 :person/follows [-10 -11]}     ; A's edges
               {:db/id -2 :person/follows [-12 -14]}     ; B's edges
               {:db/id -3 :person/follows [-13]}])       ; C's edges
  (let [db @conn
        get-id (fn [name]
                 (d/q '[:find ?e . :in $ ?n :where [?e :person/name ?n]] db name))]
    {:a (get-id "A") :b (get-id "B") :c (get-id "C") :d (get-id "D")}))

;; =============================================================================
;; Bipartite Graph Fixture
;; =============================================================================
;;
;;  Left: A, B, C
;;  Right: X, Y
;;
;;  A -- X
;;  A -- Y
;;  B -- X
;;  C -- Y
;;

(defn setup-bipartite-graph [conn]
  (d/transact conn
              [{:db/id -1 :person/name "A"}
               {:db/id -2 :person/name "B"}
               {:db/id -3 :person/name "C"}
               {:db/id -4 :person/name "X"}
               {:db/id -5 :person/name "Y"}
     ;; Bipartite edges only (left to right)
               {:db/id -1 :person/friend [-4 -5]}
               {:db/id -2 :person/friend [-4]}
               {:db/id -3 :person/friend [-5]}])
  (let [db @conn
        get-id (fn [name]
                 (d/q '[:find ?e . :in $ ?n :where [?e :person/name ?n]] db name))]
    {:a (get-id "A") :b (get-id "B") :c (get-id "C")
     :x (get-id "X") :y (get-id "Y")}))

;; =============================================================================
;; Tests: Transitive Closure
;; =============================================================================

(deftest test-transitive-closure
  (let [conn (create-test-db)
        ids (setup-simple-graph conn)
        db @conn]
    (testing "Basic transitive closure from Alice"
      (let [reachable (graph/transitive-closure db (:alice ids) :person/follows)]
        (is (contains? reachable (:bob ids)) "Alice can reach Bob")
        (is (contains? reachable (:carol ids)) "Alice can reach Carol")
        (is (contains? reachable (:dave ids)) "Alice can reach Dave")
        (is (contains? reachable (:eve ids)) "Alice can reach Eve")
        (is (not (contains? reachable (:alice ids))) "Alice not in own closure (non-reflexive)")))

    (testing "Reflexive transitive closure"
      (let [reachable (graph/transitive-closure db (:alice ids) :person/follows {:reflexive? true})]
        (is (contains? reachable (:alice ids)) "Alice in own reflexive closure")))

    (testing "Max depth limiting"
      (let [reachable (graph/transitive-closure db (:alice ids) :person/follows {:max-depth 1})]
        (is (contains? reachable (:bob ids)) "Bob reachable in 1 hop")
        (is (contains? reachable (:dave ids)) "Dave reachable in 1 hop")
        (is (not (contains? reachable (:eve ids))) "Eve not reachable in 1 hop")))

    (testing "Transitive closure from leaf node"
      (let [reachable (graph/transitive-closure db (:eve ids) :person/follows)]
        (is (empty? reachable) "Eve has no outgoing edges")))))

(deftest test-reachable?
  (let [conn (create-test-db)
        ids (setup-simple-graph conn)
        db @conn]
    (testing "Reachability check"
      (is (graph/reachable? db (:alice ids) :person/follows (:eve ids))
          "Alice can reach Eve")
      (is (not (graph/reachable? db (:eve ids) :person/follows (:alice ids)))
          "Eve cannot reach Alice (DAG)")
      (is (not (graph/reachable? db (:bob ids) :person/follows (:dave ids)))
          "Bob cannot reach Dave (different branch)"))))

;; =============================================================================
;; Tests: Path Enumeration
;; =============================================================================

(deftest test-all-paths
  (let [conn (create-test-db)
        ids (setup-simple-graph conn)
        db @conn]
    (testing "All paths from Alice"
      (let [paths (graph/all-paths db (:alice ids) :person/follows)]
        (is (seq paths) "Should find paths")
        ;; Check that multiple paths to Eve exist
        (let [paths-to-eve (filter #(= (last %) (:eve ids)) paths)]
          (is (= 2 (count paths-to-eve)) "Two paths to Eve (via Bob/Carol and via Dave)"))))

    (testing "Paths with target"
      (let [paths (graph/all-paths db (:alice ids) :person/follows {:target-eid (:eve ids)})]
        (is (= 2 (count paths)) "Two paths specifically to Eve")))

    (testing "Max depth limiting"
      (let [paths (graph/all-paths db (:alice ids) :person/follows {:max-depth 1})]
        (is (every? #(<= (count %) 2) paths) "All paths have at most 2 nodes")))))

;; =============================================================================
;; Tests: Shortest Path
;; =============================================================================

(deftest test-shortest-path
  (let [conn (create-test-db)
        ids (setup-simple-graph conn)
        db @conn]
    (testing "Shortest path exists"
      (let [path (graph/shortest-path db (:alice ids) :person/follows (:eve ids))]
        (is (= (:alice ids) (first path)) "Path starts at Alice")
        (is (= (:eve ids) (last path)) "Path ends at Eve")
        ;; Shortest is Alice -> Dave -> Eve (length 2 hops)
        (is (= 3 (count path)) "Shortest path has 3 nodes")))

    (testing "Same node path"
      (let [path (graph/shortest-path db (:alice ids) :person/follows (:alice ids))]
        (is (= [(:alice ids)] path) "Path to self is just the node")))

    (testing "No path exists"
      (let [path (graph/shortest-path db (:eve ids) :person/follows (:alice ids))]
        (is (nil? path) "No path from Eve back to Alice")))))

(deftest test-path-length
  (let [conn (create-test-db)
        ids (setup-simple-graph conn)
        db @conn]
    (testing "Path length calculation"
      (is (= 2 (graph/path-length db (:alice ids) :person/follows (:eve ids)))
          "Alice to Eve is 2 hops")
      (is (= 0 (graph/path-length db (:alice ids) :person/follows (:alice ids)))
          "Path to self is 0")
      (is (nil? (graph/path-length db (:eve ids) :person/follows (:alice ids)))
          "No path returns nil"))))

;; =============================================================================
;; Tests: Connected Components
;; =============================================================================

(deftest test-connected-component
  (let [conn (create-test-db)
        ids (setup-disconnected-graph conn)
        db @conn]
    (testing "Single component from A"
      (let [component (graph/connected-component db (:a ids) [:person/friend])]
        (is (= 3 (count component)) "Component has A, B, C")
        (is (contains? component (:a ids)))
        (is (contains? component (:b ids)))
        (is (contains? component (:c ids)))
        (is (not (contains? component (:x ids))) "X not in same component")))

    (testing "Other component from X"
      (let [component (graph/connected-component db (:x ids) [:person/friend])]
        (is (= 2 (count component)) "Component has X, Y")
        (is (not (contains? component (:a ids))))))))

(deftest test-all-connected-components
  (let [conn (create-test-db)
        ids (setup-disconnected-graph conn)
        db @conn
        ;; Only consider person entities (those with :person/name)
        person-ids (set (vals ids))
        person? (fn [eid] (contains? person-ids eid))]
    (testing "Find all components"
      (let [components (graph/all-connected-components db [:person/friend] person?)]
        (is (= 2 (count components)) "Two disconnected components")
        (let [sizes (set (map count components))]
          (is (= #{2 3} sizes) "Components have sizes 2 and 3"))))))

(deftest test-same-component?
  (let [conn (create-test-db)
        ids (setup-disconnected-graph conn)
        db @conn]
    (testing "Same component check"
      (is (graph/same-component? db (:a ids) [:person/friend] (:c ids))
          "A and C are in same component")
      (is (not (graph/same-component? db (:a ids) [:person/friend] (:x ids)))
          "A and X are in different components"))))

;; =============================================================================
;; Tests: Strongly Connected Components
;; =============================================================================

(deftest test-strongly-connected-components
  (let [conn (create-test-db)
        ids (setup-cyclic-graph conn)
        db @conn]
    (testing "SCC in cyclic graph"
      (let [sccs (graph/strongly-connected-components db :person/follows)]
        (is (= 1 (count sccs)) "One SCC containing all nodes")
        (is (= 3 (count (first sccs))) "SCC has 3 nodes")))))

(deftest test-strongly-connected-components-dag
  (let [conn (create-test-db)
        ids (setup-simple-graph conn)
        db @conn]
    (testing "SCC in DAG (no cycles)"
      (let [sccs (graph/strongly-connected-components db :person/follows)]
        ;; Each node is its own SCC in a DAG
        (is (= 5 (count sccs)) "5 SCCs (one per node)")
        (is (every? #(= 1 (count %)) sccs) "Each SCC has 1 node")))))

;; =============================================================================
;; Tests: Topological Sort & Cycle Detection
;; =============================================================================

(deftest test-topological-sort
  (let [conn (create-test-db)
        ids (setup-simple-graph conn)
        db @conn]
    (testing "Topological sort of DAG"
      (let [sorted (graph/topological-sort db :person/follows)]
        (is (some? sorted) "DAG has valid topological order")
        (is (= 5 (count sorted)) "All nodes included")
        ;; Alice must come before all others
        (is (< (.indexOf sorted (:alice ids)) (.indexOf sorted (:bob ids)))
            "Alice before Bob")
        (is (< (.indexOf sorted (:alice ids)) (.indexOf sorted (:eve ids)))
            "Alice before Eve")))))

(deftest test-topological-sort-cyclic
  (let [conn (create-test-db)
        _ (setup-cyclic-graph conn)
        db @conn]
    (testing "Topological sort fails for cyclic graph"
      (is (nil? (graph/topological-sort db :person/follows))
          "Returns nil for cyclic graph"))))

(deftest test-has-cycle?
  (let [conn-dag (create-test-db)
        _ (setup-simple-graph conn-dag)
        conn-cyclic (create-test-db)
        _ (setup-cyclic-graph conn-cyclic)]
    (testing "Cycle detection"
      (is (not (graph/has-cycle? @conn-dag :person/follows))
          "DAG has no cycle")
      (is (graph/has-cycle? @conn-cyclic :person/follows)
          "Cyclic graph has cycle"))))

(deftest test-find-cycle
  (let [conn (create-test-db)
        ids (setup-cyclic-graph conn)
        db @conn]
    (testing "Find cycle returns actual cycle"
      (let [cycle (graph/find-cycle db :person/follows)]
        (is (some? cycle) "Cycle found")
        (is (>= (count cycle) 2) "Cycle has at least 2 nodes")))))

;; =============================================================================
;; Tests: Bipartite Check
;; =============================================================================

(deftest test-bipartite-coloring
  (let [conn (create-test-db)
        ids (setup-bipartite-graph conn)
        db @conn]
    (testing "Bipartite graph detection"
      (let [result (graph/bipartite-coloring db :person/friend)]
        (is (:bipartite? result) "Graph is bipartite")
        (let [coloring (:coloring result)]
          ;; All left nodes should have same color
          (is (= (coloring (:a ids)) (coloring (:b ids)) (coloring (:c ids)))
              "Left partition has same color")
          ;; Right nodes have different color from left
          (is (not= (coloring (:a ids)) (coloring (:x ids)))
              "Different partitions have different colors"))))))

(deftest test-bipartite-coloring-non-bipartite
  (let [conn (create-test-db)
        _ (setup-cyclic-graph conn)  ; Triangle is not bipartite
        db @conn]
    (testing "Non-bipartite (odd cycle) detection"
      (let [result (graph/bipartite-coloring db :person/follows)]
        (is (not (:bipartite? result)) "Triangle is not bipartite")))))

;; =============================================================================
;; Tests: Bridge Detection
;; =============================================================================

(deftest test-find-bridges
  (let [conn (create-test-db)]
    ;; Create a graph with a bridge: A--B--C--D where B--C is a bridge
    (d/transact conn test-schema)
    (d/transact conn
                [{:db/id -1 :person/name "A"}
                 {:db/id -2 :person/name "B"}
                 {:db/id -3 :person/name "C"}
                 {:db/id -4 :person/name "D"}
                 {:db/id -1 :person/friend [-2]}
                 {:db/id -2 :person/friend [-1 -3]}
                 {:db/id -3 :person/friend [-2 -4]}
                 {:db/id -4 :person/friend [-3]}])
    (let [db @conn
          bridges (graph/find-bridges db :person/friend)]
      (testing "Bridge detection"
        ;; All edges in a line graph are bridges
        (is (= 3 (count bridges)) "Line graph A-B-C-D has 3 bridges")))))

;; =============================================================================
;; Tests: PageRank
;; =============================================================================

(deftest test-page-rank
  (let [conn (create-test-db)
        ids (setup-simple-graph conn)
        db @conn]
    (testing "PageRank computation"
      (let [ranks (graph/page-rank db :person/follows)]
        (is (some? ranks) "Ranks computed")
        (is (= 5 (count ranks)) "All nodes have ranks")
        ;; Eve should have highest rank (most incoming edges)
        (is (> (ranks (:eve ids)) (ranks (:alice ids)))
            "Eve ranks higher than Alice")
        ;; All ranks should be positive
        (is (every? pos? (vals ranks)) "All ranks are positive")))))

;; =============================================================================
;; Tests: Semi-Naive Transitive Closure
;; =============================================================================

(deftest test-semi-naive-transitive-closure
  (let [conn (create-test-db)
        ids (setup-simple-graph conn)
        db @conn]
    (testing "Semi-naive TC matches regular TC"
      (let [sn-tc (graph/semi-naive-transitive-closure db :person/follows)
            ;; Convert to comparable format
            alice-reachable (into #{} (map second (filter #(= (first %) (:alice ids)) sn-tc)))]
        (is (contains? alice-reachable (:bob ids)))
        (is (contains? alice-reachable (:eve ids)))))))

;; =============================================================================
;; Tests: Weighted Shortest Path
;; =============================================================================

(deftest test-weighted-shortest-path
  (let [conn (create-test-db)]
    ;; Simple weighted graph: A --(1)--> B --(1)--> C
    ;;                        A --(10)--------------> C
    (d/transact conn test-schema)
    (d/transact conn
                [{:db/id -1 :person/name "A" :edge/to -2 :edge/weight 1}
                 {:db/id -2 :person/name "B" :edge/to -3 :edge/weight 1}
                 {:db/id -3 :person/name "C"}
       ;; Direct expensive edge
                 {:db/id -4 :edge/to -3 :edge/weight 10}
                 {:db/id -1 :person/follows [-4]}])  ; A also has direct path to C
    (let [db @conn
          a (d/q '[:find ?e . :in $ :where [?e :person/name "A"]] db)
          c (d/q '[:find ?e . :in $ :where [?e :person/name "C"]] db)
          result (graph/weighted-shortest-path db a :edge/to :edge/weight c)]
      (testing "Weighted shortest path finds cheaper route"
        (is (some? result) "Path found")
        ;; Cost should be 2 (via B) not 10 (direct)
        (is (= 2 (:cost result)) "Cheaper path chosen")))))

;; =============================================================================
;; Tests: Bottleneck Path
;; =============================================================================

(deftest test-bottleneck-path
  (let [conn (create-test-db)]
    ;; Graph: A --(cap:10)--> B --(cap:5)--> C
    ;;        A --(cap:3)----------------> C
    ;; Bottleneck A->C via B is min(10,5)=5, direct is 3
    ;; So A->B->C should be chosen (capacity 5 > 3)
    (d/transact conn test-schema)
    (d/transact conn
                [{:db/id -1 :person/name "A" :edge/to -2 :edge/capacity 10}
                 {:db/id -2 :person/name "B" :edge/to -3 :edge/capacity 5}
                 {:db/id -3 :person/name "C"}
                 {:db/id -4 :edge/to -3 :edge/capacity 3}
                 {:db/id -1 :person/follows [-4]}])
    (let [db @conn
          a (d/q '[:find ?e . :in $ :where [?e :person/name "A"]] db)
          c (d/q '[:find ?e . :in $ :where [?e :person/name "C"]] db)
          result (graph/bottleneck-path db a :edge/to :edge/capacity c)]
      (testing "Bottleneck path maximizes minimum capacity"
        (is (some? result) "Path found")
        (is (= 5 (:capacity result)) "Bottleneck capacity is 5 (via B)")))))

;; =============================================================================
;; Tests: Trust Propagation
;; =============================================================================

(deftest test-trust-propagation
  (let [conn (create-test-db)]
    ;; A trusts B (0.9), B trusts C (0.8)
    (d/transact conn test-schema)
    (d/transact conn
                [{:db/id -1 :person/name "A" :edge/to -2 :edge/trust 0.9}
                 {:db/id -2 :person/name "B" :edge/to -3 :edge/trust 0.8}
                 {:db/id -3 :person/name "C"}])
    (let [db @conn
          a (d/q '[:find ?e . :in $ :where [?e :person/name "A"]] db)
          b (d/q '[:find ?e . :in $ :where [?e :person/name "B"]] db)
          c (d/q '[:find ?e . :in $ :where [?e :person/name "C"]] db)
          trust (graph/trust-propagation db a :edge/to :edge/trust :discount 1.0)]
      (testing "Trust propagation"
        (is (= 1.0 (trust a)) "Source has trust 1.0")
        (is (= 0.9 (trust b)) "Direct trust to B")
        ;; C's trust = 0.9 * 0.8 = 0.72
        (is (< (Math/abs (- 0.72 (trust c))) 0.01) "Propagated trust to C")))))

;; =============================================================================
;; Tests: Centrality Measures
;; =============================================================================

(deftest test-closeness-centrality
  (let [conn (create-test-db)]
    ;; Star graph: center connected to 4 periphery nodes
    ;; Center should have highest closeness
    (d/transact conn
                [{:db/id -1 :person/name "Center"}
                 {:db/id -2 :person/name "P1"}
                 {:db/id -3 :person/name "P2"}
                 {:db/id -4 :person/name "P3"}
                 {:db/id -5 :person/name "P4"}
                 {:db/id -1 :person/friend [-2 -3 -4 -5]}])
    (let [db @conn
          center (d/q '[:find ?e . :in $ :where [?e :person/name "Center"]] db)
          p1 (d/q '[:find ?e . :in $ :where [?e :person/name "P1"]] db)
          closeness (graph/closeness-centrality db :person/friend)]
      (testing "Closeness centrality"
        (is (some? closeness) "Closeness computed")
        (is (= 5 (count closeness)) "All 5 nodes have scores")
        ;; Center should have highest closeness (distance 1 to all)
        (is (> (closeness center) (closeness p1))
            "Center has higher closeness than periphery")
        ;; All scores should be positive
        (is (every? #(>= % 0) (vals closeness)) "All scores non-negative")))))

(deftest test-betweenness-centrality
  (let [conn (create-test-db)]
    ;; Line graph: A -- B -- C -- D
    ;; B and C should have highest betweenness (they're on all paths)
    (d/transact conn
                [{:db/id -1 :person/name "A"}
                 {:db/id -2 :person/name "B"}
                 {:db/id -3 :person/name "C"}
                 {:db/id -4 :person/name "D"}
                 {:db/id -1 :person/friend [-2]}
                 {:db/id -2 :person/friend [-3]}
                 {:db/id -3 :person/friend [-4]}])
    (let [db @conn
          a (d/q '[:find ?e . :in $ :where [?e :person/name "A"]] db)
          b (d/q '[:find ?e . :in $ :where [?e :person/name "B"]] db)
          c (d/q '[:find ?e . :in $ :where [?e :person/name "C"]] db)
          d (d/q '[:find ?e . :in $ :where [?e :person/name "D"]] db)
          betweenness (graph/betweenness-centrality db :person/friend)]
      (testing "Betweenness centrality"
        (is (some? betweenness) "Betweenness computed")
        (is (= 4 (count betweenness)) "All 4 nodes have scores")
        ;; B and C are on more shortest paths than A and D
        (is (> (betweenness b) (betweenness a))
            "B has higher betweenness than A")
        (is (> (betweenness c) (betweenness d))
            "C has higher betweenness than D")))))

;; =============================================================================
;; Tests: Similarity Algorithms
;; =============================================================================

(deftest test-jaccard-similarity
  (let [conn (create-test-db)]
    ;; A likes [X, Y, Z], B likes [X, Y], C likes [W]
    ;; Jaccard(A,B) = 2/3, Jaccard(A,C) = 0
    (d/transact conn
                [{:db/id -1 :person/name "A"}
                 {:db/id -2 :person/name "B"}
                 {:db/id -3 :person/name "C"}
                 {:db/id -4 :person/name "X"}
                 {:db/id -5 :person/name "Y"}
                 {:db/id -6 :person/name "Z"}
                 {:db/id -7 :person/name "W"}
                 {:db/id -1 :person/follows [-4 -5 -6]}  ; A -> X, Y, Z
                 {:db/id -2 :person/follows [-4 -5]}     ; B -> X, Y
                 {:db/id -3 :person/follows [-7]}])      ; C -> W
    (let [db @conn
          a (d/q '[:find ?e . :in $ :where [?e :person/name "A"]] db)
          b (d/q '[:find ?e . :in $ :where [?e :person/name "B"]] db)
          c (d/q '[:find ?e . :in $ :where [?e :person/name "C"]] db)]
      (testing "Jaccard similarity"
        ;; A and B share X,Y out of X,Y,Z -> 2/3
        (let [sim-ab (graph/jaccard-similarity db a b :person/follows)]
          (is (< (Math/abs (- sim-ab (/ 2.0 3))) 0.01)
              "Jaccard(A,B) = 2/3"))
        ;; A and C share nothing
        (let [sim-ac (graph/jaccard-similarity db a c :person/follows)]
          (is (zero? sim-ac) "Jaccard(A,C) = 0"))))))

(deftest test-node-similarity
  (let [conn (create-test-db)]
    ;; Same setup as jaccard test
    (d/transact conn
                [{:db/id -1 :person/name "A"}
                 {:db/id -2 :person/name "B"}
                 {:db/id -3 :person/name "C"}
                 {:db/id -4 :person/name "X"}
                 {:db/id -5 :person/name "Y"}
                 {:db/id -6 :person/name "Z"}
                 {:db/id -7 :person/name "W"}
                 {:db/id -1 :person/follows [-4 -5 -6]}
                 {:db/id -2 :person/follows [-4 -5]}
                 {:db/id -3 :person/follows [-7]}])
    (let [db @conn
          a (d/q '[:find ?e . :in $ :where [?e :person/name "A"]] db)
          b (d/q '[:find ?e . :in $ :where [?e :person/name "B"]] db)]
      (testing "Node similarity"
        (let [similar (graph/node-similarity db a :person/follows)]
          (is (some? similar) "Similarity computed")
          ;; B should be most similar to A
          (is (contains? similar b) "B is in similarity results")
          (is (> (similar b) 0.5) "B has high similarity to A"))
        ;; Test top-k option
        (let [top1 (graph/node-similarity db a :person/follows :top-k 1)]
          (is (= 1 (count top1)) "Top-k limits results"))))))

;; =============================================================================
;; Tests: Community Detection
;; =============================================================================

(deftest test-label-propagation
  (let [conn (create-test-db)]
    ;; Two disconnected cliques
    (d/transact conn
                [{:db/id -1 :person/name "A1"}
                 {:db/id -2 :person/name "A2"}
                 {:db/id -3 :person/name "A3"}
                 {:db/id -4 :person/name "B1"}
                 {:db/id -5 :person/name "B2"}
                 {:db/id -6 :person/name "B3"}
       ;; Clique A: fully connected
                 {:db/id -1 :person/friend [-2 -3]}
                 {:db/id -2 :person/friend [-1 -3]}
                 {:db/id -3 :person/friend [-1 -2]}
       ;; Clique B: fully connected
                 {:db/id -4 :person/friend [-5 -6]}
                 {:db/id -5 :person/friend [-4 -6]}
                 {:db/id -6 :person/friend [-4 -5]}])
    (let [db @conn
          a1 (d/q '[:find ?e . :in $ :where [?e :person/name "A1"]] db)
          a2 (d/q '[:find ?e . :in $ :where [?e :person/name "A2"]] db)
          b1 (d/q '[:find ?e . :in $ :where [?e :person/name "B1"]] db)
          result (graph/label-propagation db :person/friend)
          labels (:communities result)]
      (testing "Label propagation"
        (is (some? result) "Result computed")
        (is (:converged result) "Algorithm converged")
        (is (pos? (:iterations result)) "Iterations tracked")
        (is (= 6 (count labels)) "All nodes labeled")
        ;; Stats should be computed
        (is (= 2 (-> result :stats :num-communities)) "Two communities found")
        ;; Nodes in same clique should have same label
        (is (= (labels a1) (labels a2))
            "Nodes in clique A have same label")
        ;; Different cliques should (likely) have different labels
        (let [clique-a-label (labels a1)
              clique-b-label (labels b1)]
          (is (not= clique-a-label clique-b-label)
              "Different cliques have different labels"))))))

(deftest test-label-propagation-with-seeds
  (let [conn (create-test-db)]
    ;; Star with A at center: A -- B, A -- C, A -- D (A seeded)
    ;; All neighbors see only A, so they all adopt A's label
    (d/transact conn
                [{:db/id -1 :person/name "A"}
                 {:db/id -2 :person/name "B"}
                 {:db/id -3 :person/name "C"}
                 {:db/id -4 :person/name "D"}
                 {:db/id -1 :person/friend [-2 -3 -4]}])
    (let [db @conn
          a (d/q '[:find ?e . :in $ :where [?e :person/name "A"]] db)
          b (d/q '[:find ?e . :in $ :where [?e :person/name "B"]] db)
          c (d/q '[:find ?e . :in $ :where [?e :person/name "C"]] db)
          result (graph/label-propagation db :person/friend :seeds {a :marketing})
          labels (:communities result)]
      (testing "Label propagation with seeds"
        (is (:converged result) "Should converge with seeds")
        (is (= :marketing (labels a)) "Seed label preserved")
        ;; B only sees A, so it adopts A's label
        (is (= :marketing (labels b)) "B adopts A's label")))))

(deftest test-louvain
  (let [conn (create-test-db)]
    ;; Two dense clusters with one weak link
    ;;  [A1-A2-A3] --- [B1-B2-B3]
    ;;     dense   weak   dense
    (d/transact conn
                [{:db/id -1 :person/name "A1"}
                 {:db/id -2 :person/name "A2"}
                 {:db/id -3 :person/name "A3"}
                 {:db/id -4 :person/name "B1"}
                 {:db/id -5 :person/name "B2"}
                 {:db/id -6 :person/name "B3"}
       ;; Dense cluster A
                 {:db/id -1 :person/friend [-2 -3]}
                 {:db/id -2 :person/friend [-1 -3]}
                 {:db/id -3 :person/friend [-1 -2 -4]}  ; A3 connects to B1
       ;; Dense cluster B
                 {:db/id -4 :person/friend [-3 -5 -6]}  ; B1 connects to A3
                 {:db/id -5 :person/friend [-4 -6]}
                 {:db/id -6 :person/friend [-4 -5]}])
    (let [db @conn
          a1 (d/q '[:find ?e . :in $ :where [?e :person/name "A1"]] db)
          a2 (d/q '[:find ?e . :in $ :where [?e :person/name "A2"]] db)
          a3 (d/q '[:find ?e . :in $ :where [?e :person/name "A3"]] db)
          b1 (d/q '[:find ?e . :in $ :where [?e :person/name "B1"]] db)
          b2 (d/q '[:find ?e . :in $ :where [?e :person/name "B2"]] db)
          b3 (d/q '[:find ?e . :in $ :where [?e :person/name "B3"]] db)
          result (graph/louvain db :person/friend)
          communities (:communities result)]
      (testing "Louvain community detection"
        (is (some? result) "Result computed")
        (is (map? communities) "Communities is a map")
        (is (number? (:modularity result)) "Modularity score computed")
        (is (>= (:modularity result) -0.5) "Modularity in valid range (lower)")
        (is (<= (:modularity result) 1.0) "Modularity in valid range (upper)")
        (is (pos? (:iterations result)) "Iterations tracked")
        (is (number? (:levels result)) "Hierarchy levels tracked")
        ;; Stats should be computed
        (is (pos? (-> result :stats :num-communities)) "Communities found")
        ;; All nodes should be assigned
        (is (= 6 (count communities)) "All nodes assigned to communities")
        ;; Community IDs should be contiguous integers starting from 0
        (let [comm-ids (set (vals communities))]
          (is (= comm-ids (set (range (count comm-ids))))
              "Community IDs are contiguous integers"))
        ;; Nodes in same dense cluster should be in same community
        (is (= (communities a1) (communities a2))
            "A1 and A2 in same community")
        (is (= (communities a1) (communities a3))
            "A1 and A3 in same community")
        (is (= (communities b1) (communities b2))
            "B1 and B2 in same community")
        (is (= (communities b1) (communities b3))
            "B1 and B3 in same community")))))

(deftest test-louvain-disconnected
  (let [conn (create-test-db)]
    ;; Two completely disconnected triangles
    (d/transact conn
                [{:db/id -1 :person/name "A1"}
                 {:db/id -2 :person/name "A2"}
                 {:db/id -3 :person/name "A3"}
                 {:db/id -4 :person/name "B1"}
                 {:db/id -5 :person/name "B2"}
                 {:db/id -6 :person/name "B3"}
       ;; Triangle A (no connection to B)
                 {:db/id -1 :person/friend [-2 -3]}
                 {:db/id -2 :person/friend [-1 -3]}
                 {:db/id -3 :person/friend [-1 -2]}
       ;; Triangle B (no connection to A)
                 {:db/id -4 :person/friend [-5 -6]}
                 {:db/id -5 :person/friend [-4 -6]}
                 {:db/id -6 :person/friend [-4 -5]}])
    (let [db @conn
          a1 (d/q '[:find ?e . :in $ :where [?e :person/name "A1"]] db)
          b1 (d/q '[:find ?e . :in $ :where [?e :person/name "B1"]] db)
          result (graph/louvain db :person/friend)
          communities (:communities result)]
      (testing "Louvain on disconnected components"
        (is (= 6 (count communities)) "All nodes assigned")
        ;; Disconnected components should be in different communities
        (is (not= (communities a1) (communities b1))
            "Disconnected components in different communities")
        ;; High modularity expected for well-separated components
        (is (> (:modularity result) 0.3)
            "Good modularity for separated components")))))

(deftest test-louvain-single-clique
  (let [conn (create-test-db)]
    ;; Fully connected 4-clique
    ;; Note: For a complete graph, modularity is 0 for any partition since
    ;; actual edges = expected edges. Louvain may not merge nodes as there's
    ;; no modularity gain. This is correct behavior.
    (d/transact conn
                [{:db/id -1 :person/name "A" :person/friend [-2 -3 -4]}
                 {:db/id -2 :person/name "B" :person/friend [-1 -3 -4]}
                 {:db/id -3 :person/name "C" :person/friend [-1 -2 -4]}
                 {:db/id -4 :person/name "D" :person/friend [-1 -2 -3]}])
    (let [db @conn
          result (graph/louvain db :person/friend)
          communities (:communities result)]
      (testing "Louvain on single clique"
        (is (= 4 (count communities)) "All nodes assigned")
        ;; Modularity should be near 0 for complete graph
        (is (<= (Math/abs (:modularity result)) 0.1)
            "Modularity near 0 for complete graph")
        ;; All nodes should be assigned valid community IDs
        (is (every? number? (vals communities))
            "All community IDs are numbers")))))

;; =============================================================================
;; Tests: Datalog Rules
;; =============================================================================

(deftest test-transitive-rules
  (let [conn (create-test-db)
        ids (setup-simple-graph conn)
        db @conn]
    (testing "Transitive rules in query"
      (let [reachable (d/q '[:find [?target ...]
                             :in $ % ?start
                             :where (reachable ?start ?target :person/follows)]
                           db
                           graph/transitive-rules
                           (:alice ids))]
        (is (contains? (set reachable) (:eve ids))
            "Eve reachable via Datalog rules")))))

;; =============================================================================
;; Tests: A* Search
;; =============================================================================

(deftest test-astar-path
  (let [conn (create-test-db)]
    ;; Grid-like graph with coordinates stored for heuristic
    ;; A(0,0) -- B(1,0) -- C(2,0)
    ;;   |         |         |
    ;; D(0,1) -- E(1,1) -- F(2,1)
    (d/transact conn
                [{:db/ident :node/x :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                 {:db/ident :node/y :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])
    (d/transact conn
                [{:db/id -1 :person/name "A" :node/x 0 :node/y 0}
                 {:db/id -2 :person/name "B" :node/x 1 :node/y 0}
                 {:db/id -3 :person/name "C" :node/x 2 :node/y 0}
                 {:db/id -4 :person/name "D" :node/x 0 :node/y 1}
                 {:db/id -5 :person/name "E" :node/x 1 :node/y 1}
                 {:db/id -6 :person/name "F" :node/x 2 :node/y 1}
       ;; Horizontal edges
                 {:db/id -1 :person/friend [-2]}
                 {:db/id -2 :person/friend [-1 -3 -5]}
                 {:db/id -3 :person/friend [-2 -6]}
       ;; Vertical edges
                 {:db/id -1 :person/friend [-4]}
                 {:db/id -4 :person/friend [-1 -5]}
                 {:db/id -5 :person/friend [-2 -4 -6]}
                 {:db/id -6 :person/friend [-3 -5]}])
    (let [db @conn
          a (d/q '[:find ?e . :in $ :where [?e :person/name "A"]] db)
          f (d/q '[:find ?e . :in $ :where [?e :person/name "F"]] db)
          ;; Manhattan distance heuristic
          coords-raw (d/q '[:find ?e ?x ?y :where [?e :node/x ?x] [?e :node/y ?y]] db)
          coords (reduce (fn [m [e x y]] (assoc m e [x y])) {} coords-raw)
          heuristic (fn [node target]
                      (let [[nx ny] (get coords node)
                            [tx ty] (get coords target)]
                        (if (and nx ny tx ty)
                          (+ (Math/abs (- tx nx)) (Math/abs (- ty ny)))
                          0)))]
      (testing "A* finds path"
        (let [result (graph/astar-path db a :person/friend f heuristic)]
          (is (some? result) "Path found")
          (is (= a (first (:path result))) "Path starts at A")
          (is (= f (last (:path result))) "Path ends at F")
          (is (= 3 (:cost result)) "Optimal path cost is 3"))))))

(deftest test-astar-path-weighted
  (let [conn (create-test-db)]
    ;; A --(1)--> B --(1)--> C
    ;; A --(5)-------------> C
    (d/transact conn
                [{:db/id -1 :person/name "A" :edge/to -2 :edge/weight 1}
                 {:db/id -2 :person/name "B" :edge/to -3 :edge/weight 1}
                 {:db/id -3 :person/name "C"}
                 {:db/id -4 :edge/to -3 :edge/weight 5}
                 {:db/id -1 :person/follows [-4]}])
    (let [db @conn
          a (d/q '[:find ?e . :in $ :where [?e :person/name "A"]] db)
          c (d/q '[:find ?e . :in $ :where [?e :person/name "C"]] db)
          ;; Zero heuristic (falls back to Dijkstra)
          heuristic (fn [_ _] 0)]
      (testing "A* with weights"
        (let [result (graph/astar-path db a :edge/to c heuristic :weight-attr :edge/weight)]
          (is (some? result) "Path found")
          (is (= 2 (:cost result)) "Cheaper path via B chosen"))))))

;; =============================================================================
;; Tests: Minimum Spanning Tree
;; =============================================================================

(deftest test-prim-mst
  (let [conn (create-test-db)]
    ;; Triangle with edge entities: A --1-- B --2-- C --3-- A
    ;; Each edge is a separate entity with :edge/from, :edge/to, :edge/weight
    ;; MST should pick edges 1 and 2, total = 3
    (d/transact conn
                [{:db/id -1 :person/name "A"}
                 {:db/id -2 :person/name "B"}
                 {:db/id -3 :person/name "C"}
       ;; Edge A-B weight 1
                 {:db/id -10 :edge/from -1 :edge/to -2 :edge/weight 1}
       ;; Edge B-C weight 2
                 {:db/id -11 :edge/from -2 :edge/to -3 :edge/weight 2}
       ;; Edge C-A weight 3
                 {:db/id -12 :edge/from -3 :edge/to -1 :edge/weight 3}])
    (let [db @conn
          result (graph/prim-mst db :edge/from :edge/to :edge/weight)]
      (testing "Prim's MST"
        (is (some? result) "MST computed")
        (is (= 2 (count (:edges result))) "MST has n-1 edges for 3 nodes")
        ;; Total weight should be 1+2=3 (not including heaviest edge)
        (is (= 3 (:total-weight result)) "MST weight is 3")))))

(deftest test-mst-weight
  (let [conn (create-test-db)]
    ;; Simple line: A --1-- B --2-- C (reified edges)
    (d/transact conn
                [{:db/id -1 :person/name "A"}
                 {:db/id -2 :person/name "B"}
                 {:db/id -3 :person/name "C"}
                 {:db/id -10 :edge/from -1 :edge/to -2 :edge/weight 1}
                 {:db/id -11 :edge/from -2 :edge/to -3 :edge/weight 2}])
    (let [db @conn]
      (testing "MST weight helper"
        (is (= 3 (graph/mst-weight db :edge/from :edge/to :edge/weight))
            "Total MST weight is 3")))))

;; =============================================================================
;; Tests: Maximum Flow
;; =============================================================================

(deftest test-max-flow
  (let [conn (create-test-db)]
    ;; Simple two-path flow: S -> A -> T (cap 5) and S -> B -> T (cap 3)
    ;; Max flow = 5 + 3 = 8
    ;; Using reified edge entities
    (d/transact conn
                [{:db/id -1 :person/name "S"}
                 {:db/id -2 :person/name "A"}
                 {:db/id -3 :person/name "B"}
                 {:db/id -4 :person/name "T"}
       ;; S -> A (cap 5)
                 {:db/id -10 :edge/from -1 :edge/to -2 :edge/capacity 5}
       ;; S -> B (cap 3)
                 {:db/id -11 :edge/from -1 :edge/to -3 :edge/capacity 3}
       ;; A -> T (cap 10)
                 {:db/id -12 :edge/from -2 :edge/to -4 :edge/capacity 10}
       ;; B -> T (cap 10)
                 {:db/id -13 :edge/from -3 :edge/to -4 :edge/capacity 10}])
    (let [db @conn
          s (d/q '[:find ?e . :in $ :where [?e :person/name "S"]] db)
          t (d/q '[:find ?e . :in $ :where [?e :person/name "T"]] db)
          result (graph/max-flow db s :edge/from :edge/to :edge/capacity t)]
      (testing "Max flow"
        (is (some? result) "Flow computed")
        (is (= 8 (:flow result)) "Max flow is 8")
        (is (map? (:flow-map result)) "Flow map returned")))))

(deftest test-max-flow-bottleneck
  (let [conn (create-test-db)]
    ;; Linear with bottleneck: S --100--> A --1--> T
    ;; Max flow limited by bottleneck = 1
    ;; Using reified edge entities
    (d/transact conn
                [{:db/id -1 :person/name "S"}
                 {:db/id -2 :person/name "A"}
                 {:db/id -3 :person/name "T"}
                 {:db/id -10 :edge/from -1 :edge/to -2 :edge/capacity 100}
                 {:db/id -11 :edge/from -2 :edge/to -3 :edge/capacity 1}])
    (let [db @conn
          s (d/q '[:find ?e . :in $ :where [?e :person/name "S"]] db)
          t (d/q '[:find ?e . :in $ :where [?e :person/name "T"]] db)]
      (testing "Bottleneck limits flow"
        (is (= 1 (graph/min-cut db s :edge/from :edge/to :edge/capacity t))
            "Flow limited by bottleneck")))))

;; =============================================================================
;; Tests: Link Prediction - Neighbors and Degree
;; =============================================================================

(deftest test-neighbors
  (let [conn (create-test-db)]
    ;; Graph: A -> B -> C, A -> D
    (d/transact conn
                [{:db/id -1 :person/name "A"}
                 {:db/id -2 :person/name "B"}
                 {:db/id -3 :person/name "C"}
                 {:db/id -4 :person/name "D"}
                 {:db/id -1 :person/follows [-2 -4]}   ; A -> B, A -> D
                 {:db/id -2 :person/follows [-3]}])    ; B -> C
    (let [db @conn
          a (d/q '[:find ?e . :in $ :where [?e :person/name "A"]] db)
          b (d/q '[:find ?e . :in $ :where [?e :person/name "B"]] db)
          c (d/q '[:find ?e . :in $ :where [?e :person/name "C"]] db)]
      (testing "Outgoing neighbors"
        (is (= 2 (count (graph/neighbors db a :person/follows))) "A has 2 outgoing")
        (is (= 1 (count (graph/neighbors db b :person/follows))) "B has 1 outgoing")
        (is (= 0 (count (graph/neighbors db c :person/follows))) "C has 0 outgoing"))
      (testing "Incoming neighbors"
        (is (= 0 (count (graph/neighbors db a :person/follows :direction :incoming))) "A has 0 incoming")
        (is (= 1 (count (graph/neighbors db b :person/follows :direction :incoming))) "B has 1 incoming")
        (is (= 1 (count (graph/neighbors db c :person/follows :direction :incoming))) "C has 1 incoming"))
      (testing "Degree helper"
        (is (= 2 (graph/degree db a :person/follows)) "A degree is 2")
        (is (= 1 (graph/degree db b :person/follows)) "B degree is 1")))))

;; =============================================================================
;; Tests: Link Prediction - Common Neighbors
;; =============================================================================

(deftest test-common-neighbors
  (let [conn (create-test-db)]
    ;; Triangle with shared neighbors:
    ;;     B
    ;;    / \
    ;;   A   C
    ;;    \ /
    ;;     D
    ;; A->B, A->D, B->C, D->C (so B and D are common neighbors of A and C)
    (d/transact conn
                [{:db/id -1 :person/name "A"}
                 {:db/id -2 :person/name "B"}
                 {:db/id -3 :person/name "C"}
                 {:db/id -4 :person/name "D"}
                 {:db/id -1 :person/follows [-2 -4]}   ; A -> B, D
                 {:db/id -2 :person/follows [-3]}       ; B -> C
                 {:db/id -4 :person/follows [-3]}])     ; D -> C
    (let [db @conn
          a (d/q '[:find ?e . :in $ :where [?e :person/name "A"]] db)
          b (d/q '[:find ?e . :in $ :where [?e :person/name "B"]] db)
          c (d/q '[:find ?e . :in $ :where [?e :person/name "C"]] db)
          d (d/q '[:find ?e . :in $ :where [?e :person/name "D"]] db)]
      (testing "Common neighbors count"
        ;; A's outgoing neighbors: {B, D}
        ;; C's incoming neighbors would need :direction :incoming
        ;; With outgoing direction: A->{B,D}, C->{} -> intersection = 0
        (is (= 0 (graph/common-neighbors db a c :person/follows))
            "A and C have no common outgoing neighbors")
        ;; B's outgoing: {C}, D's outgoing: {C} -> {C}
        (is (= 1 (graph/common-neighbors db b d :person/follows))
            "B and D both point to C")))))

(deftest test-link-prediction-metrics
  (let [conn (create-test-db)]
    ;; Star graph: Center -> A, B, C, D, E
    ;; A and B share the center as a common "incoming" neighbor
    (d/transact conn
                [{:db/id -1 :person/name "Center"}
                 {:db/id -2 :person/name "A"}
                 {:db/id -3 :person/name "B"}
                 {:db/id -4 :person/name "C"}
                 {:db/id -5 :person/name "D"}
                 {:db/id -6 :person/name "E"}
                 {:db/id -1 :person/follows [-2 -3 -4 -5 -6]}])  ; Center -> all
    (let [db @conn
          center (d/q '[:find ?e . :in $ :where [?e :person/name "Center"]] db)
          a (d/q '[:find ?e . :in $ :where [?e :person/name "A"]] db)
          b (d/q '[:find ?e . :in $ :where [?e :person/name "B"]] db)]
      (testing "Preferential Attachment"
        ;; PA = degree(A) * degree(B)
        ;; Outgoing degree: A=0, B=0, so PA = 0
        (is (= 0 (graph/preferential-attachment db a b :person/follows))
            "PA is 0 when nodes have no outgoing edges")
        ;; Center has 5 outgoing
        (is (= 0 (graph/preferential-attachment db center a :person/follows))
            "PA with a leaf is 0"))
      (testing "Total Neighbors"
        ;; Union of A's and B's outgoing neighbors (both empty)
        (is (= 0 (graph/total-neighbors db a b :person/follows))
            "Total neighbors is 0 for leaf nodes")))))

(deftest test-adamic-adar-resource-allocation
  (let [conn (create-test-db)]
    ;; Graph where A and B share neighbors with varying degrees
    ;;   A -> X -> B
    ;;   A -> Y -> B
    ;;   X has degree 1 (just B)
    ;;   Y has degree 2 (B and Z)
    (d/transact conn
                [{:db/id -1 :person/name "A"}
                 {:db/id -2 :person/name "B"}
                 {:db/id -3 :person/name "X"}
                 {:db/id -4 :person/name "Y"}
                 {:db/id -5 :person/name "Z"}
                 {:db/id -1 :person/follows [-3 -4]}   ; A -> X, Y
                 {:db/id -3 :person/follows [-2]}       ; X -> B (degree 1)
                 {:db/id -4 :person/follows [-2 -5]}])  ; Y -> B, Z (degree 2)
    (let [db @conn
          a (d/q '[:find ?e . :in $ :where [?e :person/name "A"]] db)
          x (d/q '[:find ?e . :in $ :where [?e :person/name "X"]] db)
          y (d/q '[:find ?e . :in $ :where [?e :person/name "Y"]] db)]
      (testing "Common neighbors of A and B (via outgoing from A)"
        ;; A's neighbors: {X, Y}
        ;; X's neighbors: {B}, Y's neighbors: {B, Z}
        ;; Common(A, X) outgoing = {} (no shared targets)
        (is (= 0 (graph/common-neighbors db a x :person/follows))))
      (testing "Resource Allocation"
        ;; RA sums 1/degree for common neighbors
        ;; If X and Y share target B, and both have degree 1 and 2:
        ;; RA(X, Y) where common = {B}, but B has degree 0 outgoing
        ;; Actually let's test directly with known structure
        (is (>= (graph/resource-allocation db x y :person/follows) 0.0)
            "RA is non-negative"))
      (testing "Adamic-Adar"
        ;; AA sums 1/log(degree) for common neighbors
        (is (>= (graph/adamic-adar db x y :person/follows) 0.0)
            "AA is non-negative")))))

(deftest test-jaccard-index
  (let [conn (create-test-db)]
    ;; A -> {X, Y, Z}, B -> {Y, Z, W}
    ;; Common = {Y, Z}, Union = {X, Y, Z, W}
    ;; Jaccard = 2/4 = 0.5
    (d/transact conn
                [{:db/id -1 :person/name "A"}
                 {:db/id -2 :person/name "B"}
                 {:db/id -3 :person/name "X"}
                 {:db/id -4 :person/name "Y"}
                 {:db/id -5 :person/name "Z"}
                 {:db/id -6 :person/name "W"}
                 {:db/id -1 :person/follows [-3 -4 -5]}   ; A -> X, Y, Z
                 {:db/id -2 :person/follows [-4 -5 -6]}]) ; B -> Y, Z, W
    (let [db @conn
          a (d/q '[:find ?e . :in $ :where [?e :person/name "A"]] db)
          b (d/q '[:find ?e . :in $ :where [?e :person/name "B"]] db)]
      (testing "Jaccard index"
        (is (== 0.5 (graph/jaccard-index db a b :person/follows))
            "Jaccard = |{Y,Z}|/|{X,Y,Z,W}| = 2/4 = 0.5")))))

(deftest test-same-community
  (let [conn (create-test-db)
        _ (d/transact conn
                      [{:db/ident :person/community
                        :db/valueType :db.type/long
                        :db/cardinality :db.cardinality/one}])]
    (d/transact conn
                [{:db/id -1 :person/name "A" :person/community 1}
                 {:db/id -2 :person/name "B" :person/community 1}
                 {:db/id -3 :person/name "C" :person/community 2}])
    (let [db @conn
          a (d/q '[:find ?e . :in $ :where [?e :person/name "A"]] db)
          b (d/q '[:find ?e . :in $ :where [?e :person/name "B"]] db)
          c (d/q '[:find ?e . :in $ :where [?e :person/name "C"]] db)]
      (testing "Same community check"
        (is (== 1.0 (graph/same-community db a b :person/community))
            "A and B in same community")
        (is (== 0.0 (graph/same-community db a c :person/community))
            "A and C in different communities")))))

;; =============================================================================
;; Tests: Link Prediction Candidates
;; =============================================================================

(deftest test-link-prediction-candidates
  (let [conn (create-test-db)]
    ;; Chain: A -> B -> C -> D
    ;; 2-hop pairs without direct edge: (A,C), (B,D)
    (d/transact conn
                [{:db/id -1 :person/name "A"}
                 {:db/id -2 :person/name "B"}
                 {:db/id -3 :person/name "C"}
                 {:db/id -4 :person/name "D"}
                 {:db/id -1 :person/follows [-2]}   ; A -> B
                 {:db/id -2 :person/follows [-3]}   ; B -> C
                 {:db/id -3 :person/follows [-4]}]) ; C -> D
    (let [db @conn
          candidates (graph/link-prediction-candidates
                      db :person/follows graph/common-neighbors :limit 10)]
      (testing "Find 2-hop candidates"
        ;; Should find pairs that share a neighbor but aren't directly connected
        (is (vector? candidates) "Returns vector")
        (is (every? #(contains? % :score) candidates) "Each has score")))))

;; =============================================================================
;; Tests: Random Walk
;; =============================================================================

(deftest test-random-walk
  (let [conn (create-test-db)]
    ;; Simple chain: A -> B -> C -> D -> E
    (d/transact conn
                [{:db/id -1 :person/name "A"}
                 {:db/id -2 :person/name "B"}
                 {:db/id -3 :person/name "C"}
                 {:db/id -4 :person/name "D"}
                 {:db/id -5 :person/name "E"}
                 {:db/id -1 :person/follows [-2]}
                 {:db/id -2 :person/follows [-3]}
                 {:db/id -3 :person/follows [-4]}
                 {:db/id -4 :person/follows [-5]}])
    (let [db @conn
          a (d/q '[:find ?e . :in $ :where [?e :person/name "A"]] db)
          walk (graph/random-walk db a :person/follows 4 :seed 42)]
      (testing "Random walk basics"
        (is (vector? walk) "Walk is a vector")
        (is (= a (first walk)) "Walk starts at source")
        (is (<= (count walk) 5) "Walk length bounded by walk-length + 1")
        (is (pos? (count walk)) "Walk is non-empty")))))

(deftest test-random-walk-deterministic
  (let [conn (create-test-db)]
    ;; Graph with choices: A -> B, A -> C
    (d/transact conn
                [{:db/id -1 :person/name "A"}
                 {:db/id -2 :person/name "B"}
                 {:db/id -3 :person/name "C"}
                 {:db/id -1 :person/follows [-2 -3]}])
    (let [db @conn
          a (d/q '[:find ?e . :in $ :where [?e :person/name "A"]] db)
          walk1 (graph/random-walk db a :person/follows 5 :seed 12345)
          walk2 (graph/random-walk db a :person/follows 5 :seed 12345)]
      (testing "Deterministic with seed"
        (is (= walk1 walk2) "Same seed produces same walk")))))

(deftest test-random-walk-dead-end
  (let [conn (create-test-db)]
    ;; A -> B (B is dead end)
    (d/transact conn
                [{:db/id -1 :person/name "A"}
                 {:db/id -2 :person/name "B"}
                 {:db/id -1 :person/follows [-2]}])
    (let [db @conn
          a (d/q '[:find ?e . :in $ :where [?e :person/name "A"]] db)
          walk (graph/random-walk db a :person/follows 10 :seed 1)]
      (testing "Walk terminates at dead end"
        (is (= 2 (count walk)) "Walk stops at dead end (A -> B)")))))

;; =============================================================================
;; Tests: Biased Random Walk (Node2Vec style)
;; =============================================================================

(deftest test-biased-random-walk
  (let [conn (create-test-db)]
    ;; Triangle: A <-> B <-> C <-> A (undirected via both directions)
    (d/transact conn
                [{:db/id -1 :person/name "A"}
                 {:db/id -2 :person/name "B"}
                 {:db/id -3 :person/name "C"}
                 {:db/id -1 :person/follows [-2]}
                 {:db/id -2 :person/follows [-1 -3]}
                 {:db/id -3 :person/follows [-2]}])
    (let [db @conn
          a (d/q '[:find ?e . :in $ :where [?e :person/name "A"]] db)
          ;; p=1, q=1 should be uniform
          walk (graph/biased-random-walk db a :person/follows 5 1.0 1.0 :seed 42)]
      (testing "Biased walk basics"
        (is (vector? walk) "Walk is a vector")
        (is (= a (first walk)) "Walk starts at source")
        (is (pos? (count walk)) "Walk is non-empty")))))

(deftest test-biased-walk-return-bias
  (let [conn (create-test-db)]
    ;; A -> B -> C, B -> A (so B can return to A or go to C)
    (d/transact conn
                [{:db/id -1 :person/name "A"}
                 {:db/id -2 :person/name "B"}
                 {:db/id -3 :person/name "C"}
                 {:db/id -1 :person/follows [-2]}
                 {:db/id -2 :person/follows [-1 -3]}])
    (let [db @conn
          a (d/q '[:find ?e . :in $ :where [?e :person/name "A"]] db)
          b (d/q '[:find ?e . :in $ :where [?e :person/name "B"]] db)
          ;; Low p (0.1) = high return probability
          ;; Generate many walks and count returns
          walks (repeatedly 100
                            #(graph/biased-random-walk db a :person/follows 2 0.1 1.0))
          ;; Count walks that return to A after going A->B
          returns (count (filter #(and (= 3 (count %))
                                       (= (nth % 2) a))
                                 walks))]
      (testing "Low p increases return probability"
        ;; With p=0.1, return should be favored (1/0.1 = 10x more likely than explore)
        (is (> returns 30) "Should return frequently with low p")))))

;; =============================================================================
;; Tests: Multiple Random Walks
;; =============================================================================

(deftest test-random-walks-batch
  (let [conn (create-test-db)]
    (d/transact conn
                [{:db/id -1 :person/name "A"}
                 {:db/id -2 :person/name "B"}
                 {:db/id -3 :person/name "C"}
                 {:db/id -1 :person/follows [-2]}
                 {:db/id -2 :person/follows [-3]}
                 {:db/id -3 :person/follows [-1]}])  ; cycle
    (let [db @conn
          walks (vec (graph/random-walks db :person/follows 5 2
                                         :seed 42 :parallel? false))]
      (testing "Batch random walks"
        (is (= 6 (count walks)) "3 nodes × 2 walks = 6 walks")
        (is (every? vector? walks) "All walks are vectors")
        (is (every? #(pos? (count %)) walks) "All walks non-empty")))))

(deftest test-weighted-random-walk
  (let [conn (create-test-db)]
    ;; A with two outgoing edges of different weights
    (d/transact conn
                [{:db/id -1 :person/name "A"}
                 {:db/id -2 :person/name "B"}
                 {:db/id -3 :person/name "C"}
       ;; Reified edges with weights
                 {:db/id -10 :edge/from -1 :edge/to -2 :edge/weight 100}  ; A->B, high weight
                 {:db/id -11 :edge/from -1 :edge/to -3 :edge/weight 1}])  ; A->C, low weight
    (let [db @conn
          a (d/q '[:find ?e . :in $ :where [?e :person/name "A"]] db)
          b (d/q '[:find ?e . :in $ :where [?e :person/name "B"]] db)
          ;; Generate many walks and count which way they go
          walks (repeatedly 100
                            #(graph/weighted-random-walk
                              db a :edge/from :edge/to :edge/weight 1))
          to-b (count (filter #(and (= 2 (count %))
                                    (= (second %) b))
                              walks))]
      (testing "Weighted walk favors high-weight edges"
        ;; With 100:1 weight ratio, should almost always go to B
        (is (> to-b 80) "Should strongly favor high-weight edge")))))

;; =============================================================================
;; Run all tests
;; =============================================================================

#?(:clj
   (defn run-tests []
     (t/run-tests 'datahike.test.experimental.graph-test)))

(comment
  (run-tests))
