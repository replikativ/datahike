(ns datahike.planner-bugs.cross-product-test
  "Reproducers for two related query-planner correctness bugs surfaced
   by gatlab-clj's ACSet homomorphism search.

   Both bugs are present on main as of 2026-06-02. Legacy engine is
   correct in every case.

   BUG A — Disjoint groups silently dropped
   ----------------------------------------
   A query whose where-clauses form TWO scan groups that share no
   variables should produce the Cartesian product of the two groups.
   The planner returns 0 results instead of |A| × |B|.

   Affects both `execute-plan-direct` (the fast HashSet path) and
   `execute-planned-relation` (the fallback Relation path). Even
   monkey-patching the direct path to always return nil leaves the
   bug in place — the planner's relation pipeline does not build a
   cross-product join either.

   BUG B — Same-shape consumer probes against shared producer
   ----------------------------------------------------------
   When TWO consumer scans of identical shape (same attribute + same
   constant value, different entity vars) both probe a SINGLE producer
   group via different ref attributes, the planner conflates them: it
   binds both consumers to the same producer-attribute value.

   Reproducer: a directed graph schema where Edge has src and tgt
   refs to Vertex, plus the obvious homomorphism query. The planner
   returns tuples where ?v1 = ?v2 (self-joined), which violates the
   constraint that source's edges have distinct endpoints.

   Both bugs together cause graph homomorphism queries to be wrong by
   construction — the principal motivating use case for the ACSet
   backend in gatlab-clj.

   Test scaffolding kept self-contained so this file can be moved
   verbatim into datahike's main test tree."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.query :as dq]))

(defn- fresh-conn [tx]
  (let [cfg {:store              {:backend :memory :id (random-uuid)}
             :keep-history?      false
             :schema-flexibility :write
             :initial-tx         tx}]
    (d/create-database cfg)
    (d/connect cfg)))

(defn- legacy [q & dbs]
  (binding [dq/*force-legacy* true]  (apply d/q q dbs)))

(defn- planner [q & dbs]
  (binding [dq/*force-legacy* false] (apply d/q q dbs)))

;; ============================================================================
;; Bug A — disjoint groups give Cartesian product (legacy) but nothing (planner)
;; ============================================================================

(deftest bug-a-disjoint-groups-same-marker
  (let [conn (fresh-conn [{:db/ident :marker  :db/valueType :db.type/keyword
                           :db/cardinality :db.cardinality/one :db/index true}])
        _    (d/transact conn {:tx-data [{:marker :V} {:marker :V} {:marker :V}]})
        db   (d/db conn)
        q    '[:find ?a ?b :where [?a :marker :V] [?b :marker :V]]]
    (testing "Legacy returns the Cartesian product (3 × 3 = 9 tuples)"
      (is (= 9 (count (legacy q db)))))
    (testing "Planner currently returns 0 tuples — wrong"
      (is (= 9 (count (planner q db)))
          "planner should return the same 9 tuples as legacy"))))

(deftest bug-a-disjoint-groups-different-attributes
  (let [conn (fresh-conn [{:db/ident :a :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one :db/index true}
                          {:db/ident :b :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one :db/index true}])
        _    (d/transact conn {:tx-data [{:a 1} {:a 2} {:b 10} {:b 20} {:b 30}]})
        db   (d/db conn)
        q    '[:find ?ea ?eb :where [?ea :a _] [?eb :b _]]]
    (testing "Disjoint groups on different attributes: legacy = 2 × 3 = 6"
      (is (= 6 (count (legacy q db)))))
    (testing "Planner: 6"
      (is (= 6 (count (planner q db)))))))

;; ============================================================================
;; Bug B — consumer-probe conflation: two consumers, same scan shape, one producer
;; ============================================================================

(deftest bug-b-edge-with-distinct-vertex-endpoints
  (let [conn (fresh-conn
              [{:db/ident :marker  :db/valueType :db.type/keyword
                :db/cardinality :db.cardinality/one :db/index true}
               {:db/ident :src     :db/valueType :db.type/ref
                :db/cardinality :db.cardinality/one :db/index true}
               {:db/ident :tgt     :db/valueType :db.type/ref
                :db/cardinality :db.cardinality/one :db/index true}])
        ;; Triangle 1→2, 2→3, 3→1.
        _ (d/transact conn {:tx-data [{:db/id "v1" :marker :V}
                                      {:db/id "v2" :marker :V}
                                      {:db/id "v3" :marker :V}
                                      {:db/id "e1" :marker :E :src "v1" :tgt "v2"}
                                      {:db/id "e2" :marker :E :src "v2" :tgt "v3"}
                                      {:db/id "e3" :marker :E :src "v3" :tgt "v1"}]})
        db (d/db conn)
        q '[:find ?v1 ?v2 ?e
            :where
            [?v1 :marker :V]
            [?v2 :marker :V]
            [?e  :marker :E]
            [?e  :src ?v1]
            [?e  :tgt ?v2]]]
    (testing "Legacy returns one tuple per edge (3) with distinct v1, v2"
      (let [r (legacy q db)]
        (is (= 3 (count r)))
        (is (every? (fn [[v1 v2 _]] (not= v1 v2)) r)
            "every result must have v1 ≠ v2 (triangle has no self-loops)")))
    (testing "Planner returns 6 tuples, 3 of which have v1 = v2 — wrong on both axes"
      (let [r (planner q db)]
        (is (= 3 (count r)) "planner returns wrong count (currently 6)")
        (is (every? (fn [[v1 v2 _]] (not= v1 v2)) r)
            "planner returns spurious self-joined tuples")))))

;; ============================================================================
;; Observation: the bug disappears with a [(!= ?a ?b)] predicate or with
;; a free var in the v position. So users can sometimes work around it,
;; but the planner result is just not safe to trust on these shapes.
;; ============================================================================

(deftest bug-a-vanishes-with-distinctness-predicate
  (let [conn (fresh-conn [{:db/ident :marker :db/valueType :db.type/keyword
                           :db/cardinality :db.cardinality/one :db/index true}])
        _    (d/transact conn {:tx-data [{:marker :V} {:marker :V} {:marker :V}]})
        db   (d/db conn)
        q    '[:find ?a ?b
               :where [?a :marker :V] [?b :marker :V] [(!= ?a ?b)]]]
    ;; Adding the predicate happens to fix the result count (legacy = 6).
    ;; This shows the bug is in cross-product handling, not in scan results.
    (is (= 6 (count (legacy q db))))
    (is (= 6 (count (planner q db)))
        "the predicate avoids the buggy path")))

(deftest bug-a-vanishes-with-free-var-in-v
  (let [conn (fresh-conn [{:db/ident :marker :db/valueType :db.type/keyword
                           :db/cardinality :db.cardinality/one :db/index true}])
        _    (d/transact conn {:tx-data [{:marker :V} {:marker :V} {:marker :V}]})
        db   (d/db conn)
        q    '[:find ?a ?b :where [?a :marker ?x] [?b :marker ?x]]]
    ;; Replacing the constant :V with a shared free var ?x makes the
    ;; planner do the right thing. Smoking-gun: the bug is in how the
    ;; planner handles two scans whose v position is the SAME ground
    ;; value but whose e positions are distinct.
    (is (= 9 (count (legacy q db))))
    (is (= 9 (count (planner q db))))))
