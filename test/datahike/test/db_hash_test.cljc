(ns datahike.test.db-hash-test
  "What `:hash` is, and the one case where it named something that does not exist.

   `:hash` is an incrementally maintained additive sum over datom hashes, and
   `datahike.datom/hash-datom` covers `[e a v]` only — not `tx`, not `added`. It
   is deliberately NOT a hash of the current index alone: with history kept, a
   superseded or retracted value moves into the temporal index and stays
   counted, so the sum covers everything the database knows about. That makes it
   roll monotonically forward as datoms are inserted, and — because `+` is
   commutative and `tx` is not hashed — independent of the order they arrive in.

   That reading is also what makes an export/import round trip reproduce the
   value: the dump carries the full history, the import replays the same
   assertions, and both sides accumulate the same quantity.

   ONE case named a value that exists nowhere. Under `:keep-history? false` a
   cardinality-one overwrite left the superseded value counted, although there
   is no temporal index for it to move into — it is absent from every index and
   from any dump, so the source and a re-import of it disagreed:

     keep-history? false   src :hash 3737939373   tgt :hash 5253422115  DIFFER
     keep-history? true    src :hash 9996251644   tgt :hash 9996251644  EQUAL

   `with-datom-upsert` added the new value's term and never subtracted the old
   one, which is right when history keeps it and wrong when nothing does. The
   equal-value case is subtracted under both settings: `-upsert` replaces a
   datom with the same `[e a v]` and `temporal-upsert` stores nothing at all, so
   nothing was added and the addition has to be cancelled.

   These tests state the two properties that hold rather than a single formula:
   without history the sum is over the current index, with history it is not.
   The invariant had no test at all before, which is how the divergence went
   unnoticed since 2020 (`e9d55972`)."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.test.utils :as utils]))

(def ^:private schema
  [{:db/ident :name :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :score :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :tag :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/many}
   {:db/ident :note :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one :db/noHistory true}])

(defn- sum-over-current [db]
  (reduce #(+ %1 (hash %2)) 0 (d/datoms db :eavt)))

(defn- with-conn [keep-history? f]
  (let [conn (utils/setup-db {:store {:backend :memory :id (java.util.UUID/randomUUID)}
                              :keep-history? keep-history? :schema-flexibility :write})]
    (try (d/transact conn schema) (f conn)
         (finally (d/release conn)))))

(defn- hash-after [keep-history? txs]
  (with-conn keep-history?
    (fn [conn]
      (doseq [tx txs] (d/transact conn tx))
      [(:hash @conn) (sum-over-current @conn)])))

;; ---------------------------------------------------------------------------

(deftest without-history-the-sum-is-over-the-current-index
  (testing "nothing survives a retraction or an overwrite, so nothing may stay
            counted. Every shape, because the failing one was a single branch."
    (doseq [[label txs]
            [["plain assertions"       [[{:db/id -1 :name "a" :score 1 :tag :x}]]]
             ["card-one overwrite"     [[{:db/id -1 :name "a" :score 1}]
                                        [{:db/id [:name "a"] :score 2}]]]
             ["repeated overwrite"     [[{:db/id -1 :name "a" :score 1}]
                                        [{:db/id [:name "a"] :score 2}]
                                        [{:db/id [:name "a"] :score 3}]]]
             ["overwrite, same value"  [[{:db/id -1 :name "a" :score 1}]
                                        [{:db/id [:name "a"] :score 1}]]]
             ["revert to an old value" [[{:db/id -1 :name "a" :score 1}]
                                        [{:db/id [:name "a"] :score 2}]
                                        [{:db/id [:name "a"] :score 1}]]]
             ["explicit retraction"    [[{:db/id -1 :name "a" :tag :x}]
                                        [[:db/retract [:name "a"] :tag :x]]]]
             ["retract then re-assert" [[{:db/id -1 :name "a" :tag :x}]
                                        [[:db/retract [:name "a"] :tag :x]]
                                        [{:db/id [:name "a"] :tag :x}]]]
             ["retractEntity"          [[{:db/id -1 :name "a" :score 1 :tag :x}]
                                        [[:db/retractEntity [:name "a"]]]]]
             [":db/noHistory overwrite" [[{:db/id -1 :name "a" :note "first"}]
                                         [{:db/id [:name "a"] :note "second"}]]]
             ["unique-identity upsert" [[{:db/id -1 :name "a" :score 1}]
                                        [{:name "a" :score 2}]]]
             ["schema attribute change" [[{:db/id -1 :name "a"}]
                                         [{:db/ident :score :db/doc "x"}]
                                         [{:db/ident :score :db/doc "y"}]]]]]
      (let [[h s] (hash-after false txs)]
        (is (= s h) (str label ": :hash must equal the sum over :eavt"))))))

(deftest with-history-the-superseded-value-stays-counted
  "Pinned deliberately: the sum is NOT over the current index when history is
   kept, and a change that made it so would be a semantic change rather than a
   fix. It would also stop an export/import round trip reproducing the value."
  (testing "a card-one overwrite keeps the old value's term, because the value
            itself is still in the temporal index"
    (let [[h s] (hash-after true [[{:db/id -1 :name "a" :score 1}]
                                  [{:db/id [:name "a"] :score 2}]])]
      (is (not= s h)
          ":hash covers what history retains, not only the current index")))
  (testing "and a retraction likewise leaves the fact counted"
    (let [[h s] (hash-after true [[{:db/id -1 :name "a" :tag :x}]
                                  [[:db/retract [:name "a"] :tag :x]]])]
      (is (not= s h)))))

(deftest an-overwrite-with-the-same-value-changes-nothing
  (testing "`-upsert` replaces a datom with the same [e a v] and
            `temporal-upsert` stores nothing, so no index changed and the sum
            must not move either.

            `:keep-history? false` only: with history on, the transaction's own
            tx entity puts a `:db/txInstant` datom into the CURRENT index, which
            moves the sum for reasons that have nothing to do with the overwrite."
    (doseq [keep-history? [false]]
      (with-conn keep-history?
        (fn [conn]
          (d/transact conn [{:db/id -1 :name "a" :score 1}])
          (let [before (:hash @conn)]
            (d/transact conn [{:db/id [:name "a"] :score 1}])
            (is (= before (:hash @conn))
                (str "keep-history? " keep-history?
                     ": re-asserting the same value moved the sum"))))))))

(deftest the-sum-does-not-depend-on-insertion-order
  (testing "`hash-datom` ignores tx and `+` is commutative, so the same datoms
            inserted in a different order give the same value.

            Entity ids are explicit: allocating them through tempids would make
            the two runs insert genuinely DIFFERENT datoms, since `e` is hashed.
            Restricted to `:keep-history? false` because with history the tx
            entities' `:db/txInstant` datoms live in the current index and carry
            wall-clock values, which differ between two runs for reasons that
            have nothing to do with ordering."
    (let [order-a (first (hash-after false [[{:db/id 100 :name "a" :tag :x}]
                                            [{:db/id 200 :name "b" :tag :y}]
                                            [{:db/id 100 :tag :z}]]))
          order-b (first (hash-after false [[{:db/id 200 :name "b" :tag :y}]
                                            [{:db/id 100 :tag :z}]
                                            [{:db/id 100 :name "a" :tag :x}]]))]
      (is (= order-a order-b)))))

;; ---------------------------------------------------------------------------
;; The DB views — FilteredDB / HistoricalDB / AsOfDB / SinceDB — carry no
;; precomputed `:hash`. They hash by `db-view-hash`, deliberately the SAME
;; additive [e a v] sum DB maintains as `:hash`, computed over their own datoms.
;; Two properties that had no test: the sum IS the hash, and a FilteredDB that
;; mirrors a DB hashes equal to it (equiv-db reports them equal, so it must).

(deftest views-over-the-same-content-hash-equal
  (testing "the view hash is a function of content, not instance identity: two
            separately constructed views over the same database hash equal (as a
            java.util.HashSet needs, and as a per-instance cache must preserve)."
    (with-conn true
      (fn [conn]
        (d/transact conn [{:db/id -1 :name "a" :score 1}])
        (d/transact conn [{:db/id [:name "a"] :score 2}])
        (let [db @conn]
          (doseq [[label mk] [["FilteredDB" #(d/filter db (fn [_ _] true))]
                              ["HistoricalDB" #(d/history db)]
                              ["AsOfDB" #(d/as-of db (:max-tx db))]
                              ["SinceDB" #(d/since db 0)]]]
            (is (= (hash (mk)) (hash (mk)))
                (str label ": two views over the same content must hash equal"))))))))

(deftest a-filtered-view-mirroring-a-db-hashes-equal-to-it
  (testing "a FilteredDB with an always-true predicate has the same schema and
            `:eavt` datoms as the DB, so `equiv-db` reports them equal — and
            equal values must hash equal, otherwise they take two slots in a
            java.util.HashSet. `:keep-history? false` so the DB's `:hash` is the
            sum over the current index the FilteredDB sees."
    (with-conn false
      (fn [conn]
        (d/transact conn [{:db/id -1 :name "a" :score 1}
                          {:db/id -2 :name "b" :score 2}])
        (let [db @conn
              mirror (d/filter db (fn [_ _] true))]
          (is (= db mirror) "a FilteredDB mirroring the DB must be equal to it")
          (is (= (hash db) (hash mirror)) "and therefore must hash equal"))))))
