(ns datahike.test.experimental.graph-planner-test
  "Planner integration for graph algorithms: a graph algorithm's
   :datahike/output-cardinality metadata should flow (via bind provenance) into
   the cardinality the planner assigns to a downstream join on its output. CLJ
   only — it inspects planner internals (lower/logical)."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.db :as db]
            [datahike.query.logical :as logical]
            [datahike.query.lower :as lower]
            ;; ensure the algorithm vars (and their cost metadata) are loaded
            [datahike.experimental.graph]
            [datahike.experimental.graph-spec]))

;; :follows graph spans only 20 nodes (eids 100..119); :node/name spans 200.
(def ^:private test-db
  (let [schema {:node/name {:db/unique :db.unique/identity}
                :follows {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}}
        d0 (-> (db/empty-db schema)
               (d/db-with (mapv (fn [i] {:db/id (+ i 100) :node/name (str i)}) (range 200))))]
    (d/db-with d0 (vec (for [i (range 20)]
                         [:db/add (+ i 100) :follows (+ (mod (inc i) 20) 100)])))))

(defn- scan-card-of
  "scan-card the planner assigns to the pattern op matching `attr` in `where`."
  [where attr]
  (let [lp (logical/build-logical-plan test-db (vec where) #{} nil)
        plan (lower/lower lp test-db nil)]
    (some (fn [op]
            (when (and (= :pattern-scan (:op op))
                       (= attr (second (:clause op))))
              (:scan-card op)))
          (:ops plan))))

(deftest reachability-cardinality-flows-to-downstream-join
  (testing "transitive-closure over a 20-node graph bounds the :node/name join"
    ;; graph bound to a var, then traversed — provenance must resolve ?g back to
    ;; (attr-graph :follows) so the cost model can count its 20 nodes.
    (is (= 20 (scan-card-of
               '[[(datahike.experimental.graph-spec/attr-graph :follows) ?g]
                 [(datahike.experimental.graph/transitive-closure ?g $ 100) [?n ...]]
                 [?n :node/name ?nm]]
               :node/name))
        "downstream join is bounded by graph node count, not the 200-row extent"))
  (testing "control: same join without the graph algorithm is not bounded"
    (is (< 20 (scan-card-of '[[?n :node/name ?nm]] :node/name))
        "without an upstream cardinality the scan sees the full extent")))

(deftest centrality-cardinality-flows-to-downstream-join
  (testing "a per-node-score algorithm (page-rank/betweenness) destructured as
            [[?node ?score]] bounds a downstream join by the graph's node count"
    (is (= 20 (scan-card-of
               '[[(datahike.experimental.graph-spec/attr-graph :follows) ?g]
                 [(datahike.experimental.graph/page-rank ?g $) [[?node ?score]]]
                 [?node :node/name ?nm]]
               :node/name))
        "page-rank output is bounded by the 20-node graph, not the 200-row extent")
    (is (= 20 (scan-card-of
               '[[(datahike.experimental.graph-spec/attr-graph :follows) ?g]
                 [(datahike.experimental.graph/betweenness-centrality ?g $) [[?node ?score]]]
                 [?node :node/name ?nm]]
               :node/name))
        "betweenness output is bounded the same way")))

(deftest transformer-preserves-cardinality
  (testing "provenance chains through undirected-graph"
    (is (= 20 (scan-card-of
               '[[(datahike.experimental.graph-spec/attr-graph :follows) ?g0]
                 [(datahike.experimental.graph-spec/undirected-graph ?g0) ?g]
                 [(datahike.experimental.graph/transitive-closure ?g $ 100) [?n ...]]
                 [?n :node/name ?nm]]
               :node/name)))))

(deftest path-cardinality-is-small
  (testing "shortest-path bound as a collection yields a small constant card"
    (is (= 16 (scan-card-of
               '[[(datahike.experimental.graph-spec/attr-graph :follows) ?g]
                 [(datahike.experimental.graph/shortest-path ?g $ 100 110) [?n ...]]
                 [?n :node/name ?nm]]
               :node/name)))))

(deftest scalar-binding-not-bounded
  (testing "a scalar binding holds the whole result ⇒ metadata does not apply"
    (is (< 16 (scan-card-of
               '[[(datahike.experimental.graph-spec/attr-graph :follows) ?g]
                 [(datahike.experimental.graph/transitive-closure ?g $ 100) ?whole]
                 [?whole :node/name ?nm]]
               :node/name))
        "scalar ?whole is not a destructured collection")))
