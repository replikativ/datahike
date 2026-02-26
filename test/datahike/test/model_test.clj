(ns datahike.test.model-test
  "Model-based integration tests for Datahike.
   
   This test compares a pure Clojure model of Datahike semantics
   against the actual Datahike implementation. The model tracks
   expected state, and we verify that the real system matches."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [datahike.api :as d]
            [datahike.test.model.rng :as rng]
            [datahike.test.model.core :as model]
            [datahike.test.model.invariant :as inv])
  (:import [java.util UUID]))

;; =============================================================================
;; Test Schema
;; =============================================================================

(def test-schema
  [{:db/ident :name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :label
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/many
    :db/index true}
   {:db/ident :age
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :friend
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many}])

(def schema-map
  (into {} (map (juxt :db/ident identity)) test-schema))

(def user-attrs #{:name :label :age :friend})

(def entity-range (vec (range 100 200)))

;; =============================================================================
;; Test Helpers
;; =============================================================================

(defn create-test-db
  "Create a fresh in-memory database with schema.
   Returns map with :conn, :db, :cfg, and :tx-offset.
   
   The tx-offset maps model tx-ids to actual tx-ids:
   - Model starts with next-tx = 536870912 (first user tx would be recorded at 536870912)
   - After schema, actual max-tx = 536870913 (schema tx)
   - First user tx will be at 536870914
   - So offset = 536870914 - 536870912 = 2
   
   This means: model-tx-id + tx-offset = actual-tx-id"
  []
  (let [cfg {:store {:backend :memory :id (UUID/randomUUID)}
             :keep-history? true
             :schema-flexibility :write}]
    (d/delete-database cfg)
    (d/create-database cfg)
    (let [conn (d/connect cfg)
          _ (d/transact conn test-schema)
          db-after-schema (d/db conn)
          ;; Schema tx is at max-tx, first user tx will be at max-tx + 1
          ;; Model first tx is at 536870912
          ;; So offset = (max-tx + 1) - 536870912 = max-tx - 536870911
          tx-offset (- (:max-tx db-after-schema) 536870911)]
      {:conn conn :db (d/db conn) :cfg cfg :tx-offset tx-offset})))

(defn apply-tx-to-conn
  "Apply transaction to connection, returning updated db."
  [conn tx-ops]
  (try
    (d/transact conn {:tx-data (mapv vec tx-ops)})
    (d/db conn)
    (catch Exception _
      (d/db conn))))

;; =============================================================================
;; Properties (test.check)
;; =============================================================================

(def seed-gen (gen/choose 0 9223372036854775807))

(def index-integrity-prop
  (prop/for-all [seed seed-gen]
                (let [rng (rng/create seed)
                      model-state (model/create-model schema-map)
                      {:keys [conn]} (create-test-db)]
                  (loop [i 0
                         model-state model-state]
                    (if (>= i 50)
                      true
                      (let [tx-ops (model/generate-transaction rng schema-map entity-range model-state 10)
                            model-state' (model/apply-tx model-state {:tx-data tx-ops})
                            db (apply-tx-to-conn conn tx-ops)
                            result (inv/check-all
                                    (inv/all-index-invariants user-attrs)
                                    model-state'
                                    db)]
                        (if (:valid? result)
                          (recur (inc i) model-state')
                          false)))))))

(def historical-consistency-prop
  (prop/for-all [seed seed-gen]
                (let [rng (rng/create seed)
                      model-state (model/create-model schema-map)
                      {:keys [conn tx-offset]} (create-test-db)]
                  (loop [i 0
                         model-state model-state]
                    (if (>= i 20)
                      true
                      (let [tx-ops (model/generate-transaction rng schema-map entity-range model-state 3)
                            model-state' (model/apply-tx model-state {:tx-data tx-ops})
                            db (apply-tx-to-conn conn tx-ops)
                            model-tx-ids (model/get-transaction-ids model-state')
                            actual-tx-ids (map #(+ % tx-offset) (take 3 model-tx-ids))
                            hist-result (inv/check-all
                                         [(inv/historical-consistency
                                           actual-tx-ids
                                           tx-offset
                                           user-attrs)]
                                         model-state'
                                         db)]
                        (if (:valid? hist-result)
                          (recur (inc i) model-state')
                          false)))))))

(defspec index-integrity 100
  index-integrity-prop)

(defspec historical-queries-match-model 30
  historical-consistency-prop)

;; =============================================================================
;; Unit Tests
;; =============================================================================

(deftest test-model-accumulate-datom
  (testing "cardinality one replaces"
    (let [s1 (model/accumulate-datom schema-map #{} [100 :name "Alice" true])
          s2 (model/accumulate-datom schema-map s1 [100 :name "Bob" true])]
      (is (= #{[100 :name "Bob"]} s2))))

  (testing "cardinality many accumulates"
    (let [s1 (model/accumulate-datom schema-map #{} [100 :label "a" true])
          s2 (model/accumulate-datom schema-map s1 [100 :label "b" true])]
      (is (= #{[100 :label "a"] [100 :label "b"]} s2))))

  (testing "retract removes"
    (let [s1 (model/accumulate-datom schema-map #{} [100 :name "Alice" true])
          s2 (model/accumulate-datom schema-map s1 [100 :name "Alice" false])]
      (is (= #{} s2)))))

(deftest test-model-index-computation
  (let [model-state (model/apply-txs
                     (model/create-model schema-map)
                     [{:tx-data [[:db/add 100 :name "Alice"]
                                 [:db/add 100 :age 25]]}
                      {:tx-data [[:db/add 101 :name "Bob"]
                                 [:db/add 101 :age 30]]}])]
    (testing "EAVT sorted by [e a v]"
      (let [eavt (model/compute-eavt model-state)]
        (is (= [100 :age 25] (first eavt)))
        (is (= [101 :name "Bob"] (last eavt)))))

    (testing "AVET contains indexed attrs"
      (let [avet (model/compute-avet model-state)]
        (is (some #{[100 :age 25]} avet))
        (is (some #{[100 :name "Alice"]} avet))))

    (testing "AEVT sorted by [a e v]"
      (let [aevt (model/compute-aevt model-state)]
        (is (= :age (second (first aevt))))))))

(deftest test-rng-reproducibility
  (testing "same seed produces same sequence"
    (let [rng1 (rng/create 12345)
          rng2 (rng/create 12345)
          seq1 (repeatedly 10 #(rng/next-int rng1 100))
          seq2 (repeatedly 10 #(rng/next-int rng2 100))]
      (is (= seq1 seq2))))

  (testing "different seeds produce different sequences"
    (let [rng1 (rng/create 12345)
          rng2 (rng/create 54321)
          seq1 (repeatedly 10 #(rng/next-int rng1 100))
          seq2 (repeatedly 10 #(rng/next-int rng2 100))]
      (is (not= seq1 seq2))))

  (testing "fork creates independent stream"
    (let [rng (rng/create 12345)
          child (rng/fork rng)
          parent-val (rng/next-int rng 100)
          child-val (rng/next-int child 100)]
      (is (number? parent-val))
      (is (number? child-val)))))