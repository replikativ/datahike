(ns datahike.test.query-cartesian-test
  "Regression tests for the Cartesian-product / disconnected-component
   query handling.

   A query whose where-clauses split into two-or-more sub-sets sharing
   no free variables is semantically a Cartesian product. Both engines
   must produce the same result count and the same tuples (modulo
   ordering).

   Tests cover:
   - Two disjoint patterns with the same constant in v position (the
     pattern that surfaced the bug in the first place — a self-join
     shape that the planner mishandled).
   - Disjoint patterns with different attributes.
   - A canonical graph homomorphism query: two consumers probing a
     single edge producer via distinct ref attributes, exercising the
     multi-consumer code path in execute.cljc.
   - Cross-component predicates (which must apply as post-Cartesian
     filters).
   - Free var in v position (works because the variable connects
     the groups syntactically).

   Each test runs under both `*disable-planner* true` and `false` to lock
   the planner against regression."
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
  (binding [dq/*disable-planner* true]  (apply d/q q dbs)))

(defn- planner [q & dbs]
  (binding [dq/*disable-planner* false] (apply d/q q dbs)))

;; ============================================================================
;; Disjoint groups — same-constant patterns
;; ============================================================================

(deftest test-disjoint-same-marker
  (let [conn (fresh-conn [{:db/ident :marker :db/valueType :db.type/keyword
                           :db/cardinality :db.cardinality/one :db/index true}])
        _    (d/transact conn {:tx-data [{:marker :V} {:marker :V} {:marker :V}]})
        db   (d/db conn)
        q    '[:find ?a ?b :where [?a :marker :V] [?b :marker :V]]]
    (testing "Cartesian product of two disjoint same-attribute patterns"
      (is (= 9 (count (legacy q db))))
      (is (= 9 (count (planner q db))))
      (is (= (legacy q db) (planner q db))
          "legacy and planner must produce the same tuples"))))

(deftest test-disjoint-different-attributes
  (let [conn (fresh-conn [{:db/ident :a :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one :db/index true}
                          {:db/ident :b :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one :db/index true}])
        _    (d/transact conn {:tx-data [{:a 1} {:a 2} {:b 10} {:b 20} {:b 30}]})
        db   (d/db conn)
        q    '[:find ?ea ?eb :where [?ea :a _] [?eb :b _]]]
    (testing "Disjoint patterns on different attributes: 2 × 3 = 6"
      (is (= 6 (count (legacy q db))))
      (is (= 6 (count (planner q db)))))))

;; ============================================================================
;; Edge case: when a free variable connects what would otherwise be disjoint
;; ============================================================================

(deftest test-free-var-in-v-position
  (let [conn (fresh-conn [{:db/ident :marker :db/valueType :db.type/keyword
                           :db/cardinality :db.cardinality/one :db/index true}])
        _    (d/transact conn {:tx-data [{:marker :V} {:marker :V} {:marker :V}]})
        db   (d/db conn)
        q    '[:find ?a ?b :where [?a :marker ?x] [?b :marker ?x]]]
    (testing "Shared free var ?x makes this a single component (3 × 3 = 9)"
      (is (= 9 (count (legacy q db))))
      (is (= 9 (count (planner q db)))))))

;; ============================================================================
;; Cross-component predicates — applied as post-Cartesian filter
;; ============================================================================

(deftest test-cross-component-predicate
  (let [conn (fresh-conn [{:db/ident :marker :db/valueType :db.type/keyword
                           :db/cardinality :db.cardinality/one :db/index true}])
        _    (d/transact conn {:tx-data [{:marker :V} {:marker :V} {:marker :V}]})
        db   (d/db conn)
        q    '[:find ?a ?b
               :where [?a :marker :V] [?b :marker :V] [(!= ?a ?b)]]]
    (testing "Predicate spanning components becomes a post-Cartesian filter (9 - 3 = 6)"
      (is (= 6 (count (legacy q db))))
      (is (= 6 (count (planner q db)))))))

(deftest test-cross-component-predicate-strict
  (let [conn (fresh-conn [{:db/ident :n :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one :db/index true}])
        _    (d/transact conn {:tx-data [{:n 1} {:n 2} {:n 3}]})
        db   (d/db conn)
        q    '[:find ?ea ?eb
               :where [?ea :n ?x] [?eb :n ?y] [(< ?x ?y)]]]
    (testing "Strict-inequality predicate across disjoint same-attribute groups: 3 strict pairs"
      (is (= 3 (count (legacy q db))))
      (is (= 3 (count (planner q db))))
      (is (= (legacy q db) (planner q db))))))

;; ============================================================================
;; Multi-consumer producer — distinct-ref shape
;; ============================================================================

(deftest test-edge-with-distinct-vertex-endpoints
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
    (testing "One result per edge (3), v1 ≠ v2 in each (triangle has no self-loops)"
      (let [legacy-r  (legacy q db)
            planner-r (planner q db)]
        (is (= 3 (count legacy-r)))
        (is (= 3 (count planner-r)))
        (is (= legacy-r planner-r))
        (is (every? (fn [[v1 v2 _]] (not= v1 v2)) planner-r)
            "every result must have v1 ≠ v2")))))

;; ============================================================================
;; :in bindings span components — bound vars are constants, not joiners
;; ============================================================================

(deftest test-in-binding-shared-across-components
  (let [conn (fresh-conn [{:db/ident :marker :db/valueType :db.type/keyword
                           :db/cardinality :db.cardinality/one :db/index true}])
        _    (d/transact conn {:tx-data [{:marker :V} {:marker :V} {:marker :V}]})
        db   (d/db conn)
        q    '[:find ?a ?b
               :in $ ?marker
               :where [?a :marker ?marker] [?b :marker ?marker]]]
    (testing ":in-bound var doesn't connect the two patterns — Cartesian still applies"
      (is (= 9 (count (legacy q db :V))))
      (is (= 9 (count (planner q db :V)))))))
