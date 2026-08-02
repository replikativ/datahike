(ns datahike.test.migrate-fidelity-test
  "How closely does a restored database equal the one it came from?

   Round-trip tests usually compare query results, which is much weaker than it
   sounds: a database whose *history* is wrong answers every present-tense query
   correctly. So this compares the DB RECORD field by field, and replays `as-of`
   at every transaction rather than only at the end.

   The generator deliberately produces the shapes where history goes wrong —
   card-one overwrites, card-many add/retract, retract-then-reassert, fully
   retracted entities, schema added mid-history, `:db/noHistory` attributes. A
   uniform random database exercises almost none of them.

   This is also the oracle a bulk-build import has to satisfy: it must produce a
   DB record equal to the one the transact path produces. Written first, so the
   fast path is measured against it rather than against itself."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.migrate :as m]
            [datahike.test.utils :as utils]))

(defn- teardown [conn]
  (let [cfg (:config @conn)] (d/release conn) (d/delete-database cfg)))

(defn- mem-cfg []
  {:store {:backend :memory :id (java.util.UUID/randomUUID)}
   :keep-history? true :schema-flexibility :write})

;; ---------------------------------------------------------------------------
;; the generator — shapes chosen because they break history, not at random

(def ^:private base-schema
  [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :score :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :tag :db/valueType :db.type/keyword :db/cardinality :db.cardinality/many}
   {:db/ident :pal :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/ident :note :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/noHistory true}])

(defn- build-adversarial-db!
  "Transact a history containing every shape that makes currentness non-obvious.
   Returns the conn. Each step is its own transaction so `as-of` has many points."
  [conn]
  (d/transact conn (vec (take 4 base-schema)))
  (d/transact conn [{:db/id -1 :name "a" :score 1 :tag :x}
                    {:db/id -2 :name "b" :score 2 :tag :y}
                    {:db/id -3 :name "c" :score 3}])
  ;; card-one overwrite — the classic currentness case
  (d/transact conn [{:db/id [:name "a"] :score 10}])
  (d/transact conn [{:db/id [:name "a"] :score 100}])
  ;; card-many add and retract
  (d/transact conn [{:db/id [:name "b"] :tag :z}])
  (d/transact conn [[:db/retract [:name "b"] :tag :y]])
  ;; retract then RE-ASSERT the same value — the datom returns after being gone
  (d/transact conn [[:db/retract [:name "c"] :score 3]])
  (d/transact conn [{:db/id [:name "c"] :score 3}])
  ;; a ref, then retract the entity it points AT
  (d/transact conn [{:db/id [:name "a"] :pal [:name "c"]}])
  (d/transact conn [[:db/retractEntity [:name "c"]]])
  ;; schema added MID-history, then used
  (d/transact conn [(nth base-schema 4)])
  (d/transact conn [{:db/id [:name "a"] :note "no history kept for this"}])
  ;; and overwrite the :db/noHistory attribute, which must NOT accumulate history
  (d/transact conn [{:db/id [:name "a"] :note "second value"}])
  conn)

;; ---------------------------------------------------------------------------
;; the comparison

(def ^:private index-fields
  [:eavt :aevt :avet :temporal-eavt :temporal-aevt :temporal-avet])

(def ^:private derived-fields
  ;; :op-count is deliberately EXCLUDED — see `op-count-diverges-but-is-inert`.
  [:schema :rschema :max-eid :system-entities :ident-ref-map :ref-ident-map
   :secondary-indices])

(defn- compare-records
  "Field-by-field differences between two DB records. Returns a map of
   field -> [a b] for everything that differs, so a failure names the field
   rather than dumping two databases."
  [a b]
  (reduce (fn [acc k]
            (let [x (get a k) y (get b k)]
              (if (contains? (set index-fields) k)
                (if (= (vec x) (vec y)) acc (assoc acc k [(count x) (count y)]))
                (if (= x y) acc (assoc acc k [x y])))))
          {}
          (concat index-fields derived-fields)))

(defn- roundtrip!
  "Export `conn` and import into a fresh database. Returns the target conn."
  [conn]
  (let [path (str (System/getProperty "java.io.tmpdir") "/dh-fid-" (utils/get-time))
        _ (m/export-db conn path {:format :flat :history? true})
        tgt (utils/setup-db (mem-cfg))]
    (m/import-db tgt path {})
    tgt))

;; ---------------------------------------------------------------------------

(deftest all-indexes-and-derived-fields-match
  (testing "every index and every schema-derived field survives a round trip.

            Indexes are compared by CONTENTS across all six — including the three
            temporal ones, which a present-tense query never touches and which are
            exactly where a wrong currentness rule would show."
    (let [src (build-adversarial-db! (utils/setup-db (mem-cfg)))
          tgt (roundtrip! src)
          diffs (compare-records @src @tgt)]
      (is (empty? diffs)
          (str "fields differ: " (pr-str (keys diffs)) " -> " (pr-str diffs)))
      (teardown src) (teardown tgt))))

(deftest as-of-matches-at-every-transaction
  (testing "the property final-state equality cannot see.

            A restored database with a wrong history answers every present-tense
            query correctly and diverges only when you look backwards. So every
            transaction point is replayed, not just the last one."
    (let [src (build-adversarial-db! (utils/setup-db (mem-cfg)))
          tgt (roundtrip! src)
          txs (sort (distinct (map :tx (d/datoms @src :eavt))))
          triples (fn [db] (set (map (juxt :e :a :v :added) (d/datoms db :eavt))))]
      (is (seq txs) "precondition: there are transactions to replay")
      (doseq [t txs]
        (is (= (triples (d/as-of @src t)) (triples (d/as-of @tgt t)))
            (str "as-of " t " differs")))
      (teardown src) (teardown tgt))))

(deftest history-matches
  (testing "the full temporal set, including retracted datoms"
    (let [src (build-adversarial-db! (utils/setup-db (mem-cfg)))
          tgt (roundtrip! src)
          hist (fn [db] (set (map (juxt :e :a :v :added) (d/datoms (d/history db) :eavt))))]
      (is (= (hist @src) (hist @tgt)))
      (teardown src) (teardown tgt))))

(deftest no-history-attribute-accumulates-nothing
  (testing ":db/noHistory means the temporal indexes must not collect it — on
            BOTH sides. A restore that quietly starts keeping history for such an
            attribute grows without bound and leaks values the user asked to
            forget."
    (let [src (build-adversarial-db! (utils/setup-db (mem-cfg)))
          tgt (roundtrip! src)
          notes (fn [db] (filter #(= :note (:a %)) (d/datoms (d/history db) :eavt)))]
      (is (= (count (notes @src)) (count (notes @tgt)))
          "same number of :note datoms in history")
      (teardown src) (teardown tgt))))

(deftest max-tx-drift-is-plus-one-reported-and-does-not-compound
  (testing "the restored database's max-tx is one HIGHER than its source's.

            The import ends via `transact-entities-directly`, which bumps max-tx
            once more, so one transaction id is skipped. Datom content is
            unaffected — every index matches — but the restored database numbers
            its next transaction differently from the one it replaced.

            Pinned at exactly +1 rather than asserted away: if it ever becomes +2,
            or starts compounding across round trips, that is a different and much
            worse bug, and this is what would catch it. `import-db` also REPORTS
            the drift and warns, so an operator is told rather than left to find
            out."
    (let [src (build-adversarial-db! (utils/setup-db (mem-cfg)))
          path (str (System/getProperty "java.io.tmpdir") "/dh-fid-mt-" (utils/get-time))
          _ (m/export-db src path {:format :flat :history? true})
          gen1 (utils/setup-db (mem-cfg))
          rep1 (m/import-db gen1 path {})]
      (is (= 1 (- (:max-tx @gen1) (:max-tx @src))) "exactly +1, not more")
      (is (= 1 (:max-tx-drift rep1)) "and the report says so")
      (testing "a second round trip is stable — the drift does not compound"
        (let [p2 (str (System/getProperty "java.io.tmpdir") "/dh-fid-mt2-" (utils/get-time))
              _ (m/export-db gen1 p2 {:format :flat :history? true})
              gen2 (utils/setup-db (mem-cfg))
              rep2 (m/import-db gen2 p2 {})]
          (is (= (:max-tx @gen1) (:max-tx @gen2)) "gen2 max-tx equals gen1's")
          (is (zero? (:max-tx-drift rep2)) "and the report shows no further drift")
          (teardown gen2)))
      (teardown src) (teardown gen1))))

(deftest op-count-diverges-but-is-inert
  (testing ":op-count does NOT round-trip, and that is acceptable — but only
            because nothing supported reads it.

            `persistent-set` ignores the argument entirely (`_op-count` in every
            index op). `hitchhiker-tree` DOES use it, as the sequence number for
            its message buffers — but that index is deprecated.

            Recorded rather than fixed, so the reasoning is attached to the fact.
            If persistent-set ever starts using op-count, this test is where the
            assumption is written down."
    (let [src (build-adversarial-db! (utils/setup-db (mem-cfg)))
          tgt (roundtrip! src)]
      (is (not= (:op-count @src) (:op-count @tgt))
          "if these now MATCH, the import changed — revisit whether the exclusion
           from `derived-fields` is still warranted")
      (testing "and every index still agrees, which is what actually matters"
        (is (= (vec (:eavt @src)) (vec (:eavt @tgt))))
        (is (= (vec (:temporal-eavt @src)) (vec (:temporal-eavt @tgt)))))
      (teardown src) (teardown tgt))))
