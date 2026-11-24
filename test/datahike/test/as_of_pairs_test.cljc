(ns datahike.test.as-of-pairs-test
  "`as-of` over a history that holds one value both asserted and retracted
   under one transaction id.

   Databases written before a transaction recorded its net effect hold such
   pairs: every step of a transaction was written, so `retract x, add x`, or a
   cardinality-one attribute set to `40`, `35` and `40` again, left the value
   with an assertion and a retraction of the same transaction. The fixtures
   here are loaded record by record, which stores them the way those
   transactions were written, whatever the transactor does today."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]))

(def ^:private schema
  [{:db/ident :many :db/valueType :db.type/keyword :db/cardinality :db.cardinality/many}
   {:db/ident :one :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])

(defn- history-of
  "A database holding `transactions`, a vector of vectors of `[a v added]` on
   one entity, written step by step. Returns `[db entity tx-ids]`."
  [transactions]
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :keep-history? true :schema-flexibility :write}
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (d/transact conn schema)
    @(d/load-entities conn (vec (for [[i ops] (map-indexed vector transactions)
                                      :let [t (+ 5001 i)]
                                      rec (cons [t :db/txInstant
                                                 #?(:clj (java.util.Date. (long (* 1000 (inc i))))
                                                    :cljs (js/Date. (* 1000 (inc i))))
                                                 t true]
                                                (for [[a v added] ops] [1001 a v t added]))]
                                  rec)))
    (let [db @conn
          e (:e (first (filter #(#{:many :one} (:a %)) (d/datoms (d/history db) :eavt))))
          txs (vec (sort (distinct (map :tx (filter #(= e (:e %)) (d/datoms (d/history db) :eavt))))))]
      (d/release conn)
      [db e txs])))

(defn- states
  "The values of `a` as of each of the fixture's transactions, by `datoms` and
   by query."
  [[db e txs] a]
  (vec (for [t txs]
         (let [view (d/as-of db t)
               by-datoms (set (map :v (d/datoms view :eavt e a)))
               by-query (set (map first (d/q '[:find ?v :in $ ?e ?a :where [?e ?a ?v]] view e a)))]
           (if (= by-datoms by-query) by-datoms {:datoms by-datoms :query by-query})))))

(deftest cardinality-many-pairs
  (testing "retracted and asserted again, then retracted for good"
    (is (= [#{:x} #{:x} #{}]
           (states (history-of [[[:many :x true]]
                                [[:many :x false] [:many :x true]]
                                [[:many :x false]]])
                   :many))))
  (testing "asserted and retracted, then asserted"
    (is (= [#{} #{:x}]
           (states (history-of [[[:many :x true] [:many :x false]]
                                [[:many :x true]]])
                   :many))))
  (testing "asserted and retracted, never touched again"
    (is (= [#{}]
           (states (history-of [[[:many :x true] [:many :x false]]])
                   :many))))
  (testing "asserted, retracted, asserted; retracted later"
    (is (= [#{:x} #{}]
           (states (history-of [[[:many :x true] [:many :x false] [:many :x true]]
                                [[:many :x false]]])
                   :many))))
  (testing "retracted, asserted, retracted; asserted later"
    (is (= [#{:x} #{} #{:x}]
           (states (history-of [[[:many :x true]]
                                [[:many :x false] [:many :x true] [:many :x false]]
                                [[:many :x true]]])
                   :many))))
  (testing "retracted and asserted again, still there"
    (is (= [#{:x} #{:x}]
           (states (history-of [[[:many :x true]]
                                [[:many :x false] [:many :x true]]])
                   :many))))
  (testing "other values are not disturbed"
    (is (= [#{:x :y} #{:x :y} #{:y}]
           (states (history-of [[[:many :x true] [:many :y true]]
                                [[:many :x false] [:many :x true]]
                                [[:many :x false]]])
                   :many)))))

(deftest cardinality-one-pairs
  (testing "a passing value revisited, then overwritten"
    (is (= [#{30} #{40} #{50}]
           (states (history-of [[[:one 30 true]]
                                [[:one 40 true] [:one 35 true] [:one 40 true]]
                                [[:one 50 true]]])
                   :one))))
  (testing "set twice"
    (is (= [#{24} #{21} #{50}]
           (states (history-of [[[:one 24 true]]
                                [[:one 25 true] [:one 21 true]]
                                [[:one 50 true]]])
                   :one))))
  (testing "away and back, then overwritten"
    (is (= [#{24} #{24} #{50}]
           (states (history-of [[[:one 24 true]]
                                [[:one 25 true] [:one 24 true]]
                                [[:one 50 true]]])
                   :one))))
  (testing "set and retracted"
    (is (= [#{24} #{} #{50}]
           (states (history-of [[[:one 24 true]]
                                [[:one 25 true] [:one 25 false]]
                                [[:one 50 true]]])
                   :one))))
  (testing "a passing value revisited, still there"
    (is (= [#{30} #{40}]
           (states (history-of [[[:one 30 true]]
                                [[:one 40 true] [:one 35 true] [:one 40 true]]])
                   :one)))))
