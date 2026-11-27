(ns datahike.test.migrate-export-pairs-test
  "Exporting a history that holds one value both asserted and retracted under
   one transaction id.

   Databases written before a transaction recorded its net effect hold such
   pairs, and an import cannot tell how one ended. The export can, and writes
   the pair's net effect instead. The sources here are loaded record by record,
   which stores them the way those transactions were written."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.migrate :as m]
            [datahike.test.utils :as utils]))

(def ^:private schema
  [{:db/ident :many :db/valueType :db.type/keyword :db/cardinality :db.cardinality/many}
   {:db/ident :one :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])

(defn- mem-cfg []
  {:store {:backend :memory :id (random-uuid)}
   :keep-history? true :schema-flexibility :write})

(defn- source
  "A connection holding `transactions`, a vector of vectors of `[a v added]`
   on one entity, stored record by record."
  [transactions]
  (let [conn (utils/setup-db (mem-cfg))]
    (d/transact conn schema)
    @(d/load-entities conn (vec (for [[i ops] (map-indexed vector transactions)
                                      :let [t (+ 5001 i)]
                                      rec (cons [t :db/txInstant (java.util.Date. (long (* 1000 (inc i)))) t true]
                                                (for [[a v added] ops] [1001 a v t added]))]
                                  rec)))
    conn))

(defn- picture
  "What `db` says about attribute `a`: the current values, and the history as
   `[value transaction-rank added]`, transactions ranked by their instant."
  [db a]
  (let [rank (into {} (map-indexed (fn [i [t]] [t i]))
                   (sort-by second (d/q '[:find ?t ?i :where [?t :db/txInstant ?i]
                                          [(.getTime ^java.util.Date ?i) ?ms] [(< ?ms 1000000)]]
                                        db)))]
    {:current (set (d/q '[:find [?v ...] :in $ ?a :where [_ ?a ?v]] db a))
     :history (set (map (fn [[v t added]] [v (rank t) added])
                        (d/q '[:find ?v ?t ?added :in $ ?a :where [_ ?a ?v ?t ?added]]
                             (d/history db) a)))}))

(defn- roundtrip
  "Export `conn` with history and import it; returns `[report target-conn]`."
  [conn export-opts import-opts]
  (let [path (str (System/getProperty "java.io.tmpdir") "/dh-pairs-" (random-uuid))
        _ (m/export-db @conn path (merge {:history? true} export-opts))
        tgt (if (:build-indexes? import-opts)
              (let [cfg (mem-cfg)] (d/create-database cfg) (d/connect cfg))
              (utils/setup-db (mem-cfg)))]
    [(m/import-db tgt path import-opts) tgt]))

(def ^:private cases
  "name, attribute, transactions as written, and what they amount to."
  [["retracted and asserted again, then retracted for good" :many
    [[[:many :x true]] [[:many :x false] [:many :x true]] [[:many :x false]]]
    {:current #{} :history #{[:x 0 true] [:x 2 false]}}]
   ["asserted and retracted, then asserted" :many
    [[[:many :x true] [:many :x false]] [[:many :x true]]]
    {:current #{:x} :history #{[:x 1 true]}}]
   ["asserted and retracted, never touched again" :many
    [[[:many :y true]] [[:many :x true] [:many :x false]]]
    {:current #{:y} :history #{[:y 0 true]}}]
   ["retracted and asserted again, still there" :many
    [[[:many :x true]] [[:many :x false] [:many :x true]]]
    {:current #{:x} :history #{[:x 0 true]}}]
   ["a pair followed by a pair, then retracted" :many
    [[[:many :x true] [:many :x false]]
     [[:many :x true] [:many :x false] [:many :x true]]
     [[:many :x false]]]
    {:current #{} :history #{[:x 1 true] [:x 2 false]}}]
   ["a passing value revisited, then overwritten" :one
    [[[:one 30 true]]
     [[:one 30 false] [:one 40 true] [:one 40 false] [:one 35 true] [:one 35 false] [:one 40 true]]
     [[:one 40 false] [:one 50 true]]]
    {:current #{50} :history #{[30 0 true] [30 1 false] [40 1 true] [40 2 false] [50 2 true]}}]
   ["set twice in one transaction" :one
    [[[:one 24 true]]
     [[:one 24 false] [:one 25 true] [:one 25 false] [:one 21 true]]]
    {:current #{21} :history #{[24 0 true] [24 1 false] [21 1 true]}}]
   ["away and back" :one
    [[[:one 24 true]]
     [[:one 24 false] [:one 25 true] [:one 25 false] [:one 24 true]]
     [[:one 24 false] [:one 50 true]]]
    {:current #{50} :history #{[24 0 true] [24 2 false] [50 2 true]}}]])

(deftest a-dump-carries-the-net-effect-of-every-pair
  (doseq [[export-name export-opts] [["sorted export" {}] ["streaming export" {:sort? false}]]
          [import-name import-opts] [["replayed" {}] ["index build" {:build-indexes? true}]]
          [case-name a transactions expected] cases]
    (testing (str export-name ", " import-name ": " case-name)
      (let [src (source transactions)
            [report tgt] (roundtrip src export-opts import-opts)]
        (is (= expected (picture @tgt a)))
        (is (true? (:verified? report)))
        (d/release src)
        (d/release tgt)))))

(deftest a-history-without-pairs-is-exported-as-it-is
  (let [src (utils/setup-db (mem-cfg))]
    (d/transact src schema)
    (d/transact src [{:db/id 1001 :many [:x :y] :one 1}])
    (d/transact src [[:db/retract 1001 :many :x] {:db/id 1001 :one 2}])
    (d/transact src [[:db/add 1001 :many :x]])
    (let [[report tgt] (roundtrip src {} {})
          history (fn [db] (set (d/q '[:find ?a ?v ?added :where [_ ?a ?v _ ?added] [(#{:many :one} ?a)]]
                                     (d/history db))))]
      (is (= (history @src) (history @tgt)))
      (is (true? (:verified? report)))
      (d/release src)
      (d/release tgt))))
