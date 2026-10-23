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
  [:hash :schema :rschema :max-eid :system-entities :ident-ref-map :ref-ident-map
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
        _ (m/export-db @conn path {:history? true})
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

(deftest a-value-asserted-and-retracted-in-one-transaction-round-trips
  (testing "an [e a v] asserted AND retracted in one transaction is restored
            faithfully, on both import paths, whenever the history determines the
            outcome.

            datahike writes such pairs itself — a cardinality-one attribute set
            twice in one transaction records the first value's assertion and
            retraction — and the history does not say which of the two came
            first. The streaming path replayed them retraction-first: the
            retraction of a not-yet-present value was dropped and the value then
            upserted over the one that survived, so the restore answered `25`
            where the source answers `21` and lost a datom (95 of 3,000,032 on a
            synthetic history). The index-build path kept the passing value
            current alongside the survivor."
    (let [src (build-adversarial-db! (utils/setup-db (mem-cfg)))]
      (d/transact src [{:db/ident :blob :db/valueType :db.type/bytes
                        :db/cardinality :db.cardinality/many}])
      ;; set twice: 25 passes through, 21 survives
      (d/transact src [[:db/add [:name "a"] :score 25] [:db/add [:name "a"] :score 21]])
      ;; away and back: 26 passes through, 21 is re-asserted
      (d/transact src [[:db/add [:name "a"] :score 26] [:db/add [:name "a"] :score 21]])
      ;; revisit then move on: 21 -> 25 -> 21 -> 30
      (d/transact src [[:db/add [:name "a"] :score 25] [:db/add [:name "a"] :score 21]
                       [:db/add [:name "a"] :score 30]])
      ;; revisit a passing value: 30 -> 40 -> 35 -> 40
      (d/transact src [[:db/add [:name "a"] :score 40] [:db/add [:name "a"] :score 35]
                       [:db/add [:name "a"] :score 40]])
      ;; card-many: retracted and re-asserted; added, retracted, added
      (d/transact src [[:db/retract [:name "b"] :tag :z] [:db/add [:name "b"] :tag :z]])
      (d/transact src [[:db/add [:name "b"] :tag :q] [:db/retract [:name "b"] :tag :q]
                       [:db/add [:name "b"] :tag :q]])
      ;; bytes: a decoded byte array compares by identity, so its pair must be
      ;; keyed by contents to be recognised at all
      (d/transact src [[:db/add [:name "b"] :blob (byte-array [1 2 3])]])
      (d/transact src [[:db/retract [:name "b"] :blob (byte-array [1 2 3])]
                       [:db/add [:name "b"] :blob (byte-array [1 2 3])]])
      (let [path (str (System/getProperty "java.io.tmpdir") "/dh-fid-pair-" (utils/get-time))
            _ (m/export-db @src path {:history? true})
            bkey (fn [v] (if (bytes? v) (vec v) v))
            hist (fn [db] (set (map (juxt :e :a (comp bkey :v) :added) (d/datoms (d/history db) :eavt))))
            cur (fn [db] (set (map (juxt :e :a (comp bkey :v)) (d/datoms db :eavt))))]
        (doseq [[label opts] [["streaming" {}]
                              ["streaming, one record per batch" {:batch-size 1}]
                              ["streaming, :on-error :collect" {:on-error :collect}]
                              ["index build" {:build-indexes? true}]]]
          (testing label
            (let [tgt (utils/setup-db (mem-cfg))
                  rep (m/import-db tgt path opts)]
              (is (:verified? rep))
              (is (= #{[40]} (d/q '[:find ?s :where [?e :name "a"] [?e :score ?s]] @tgt))
                  "the value that survived, and only it")
              (is (= #{[:z] [:q]} (d/q '[:find ?t :where [?e :name "b"] [?e :tag ?t]] @tgt)))
              (is (= (cur @src) (cur @tgt)) "current datoms")
              (is (= (hist @src) (hist @tgt)) "history")
              (is (empty? (select-keys (compare-records @src @tgt) index-fields))
                  "all six indexes")
              (teardown tgt)))))
      (teardown src))))

(deftest an-ambiguous-same-transaction-history-restores-the-documented-reading
  (testing "`add, retract` and `add, retract, add` record the SAME history, so a
            restore must pick a reading. It is the one the import made before
            same-transaction pairs were handled (see `datahike.migrate.history`):
            the value is current. Pinned here on both paths, with the history
            itself restored exactly."
    (let [src (build-adversarial-db! (utils/setup-db (mem-cfg)))]
      (d/transact src [[:db/add [:name "b"] :tag :q] [:db/retract [:name "b"] :tag :q]])
      (d/transact src [[:db/add [:name "a"] :score 25] [:db/retract [:name "a"] :score 25]])
      (let [path (str (System/getProperty "java.io.tmpdir") "/dh-fid-amb-" (utils/get-time))
            _ (m/export-db @src path {:history? true})
            hist (fn [db] (set (map (juxt :e :a :v :added) (d/datoms (d/history db) :eavt))))]
        ;; `d/datoms`, not `d/q`: the restore carries the source's `:hash` (the
        ;; histories are equal), and a query result cached for the source would
        ;; be served for the target
        (is (empty? (d/datoms @src :eavt (:db/id (d/entity @src [:name "a"])) :score)) "precondition")
        (doseq [[label opts] [["streaming" {}] ["index build" {:build-indexes? true}]]]
          (testing label
            (let [tgt (utils/setup-db (mem-cfg))
                  rep (m/import-db tgt path opts)]
              (is (:verified? rep))
              (is (= (hist @src) (hist @tgt)) "the history is exact")
              (is (= #{[:z] [:q]} (d/q '[:find ?t :where [?e :name "b"] [?e :tag ?t]] @tgt))
                  "card-many: read as add, retract, add")
              (is (= #{[25]} (d/q '[:find ?s :where [?e :name "a"] [?e :score ?s]] @tgt))
                  "card-one: read as 25, retract 25, 25 — and only one value")
              (teardown tgt)))))
      (teardown src))))

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

(deftest a-single-transaction-dump-still-flushes-in-batches
  (testing "the memory bound must not depend on the source having many
            transactions.

            The batcher prefers to flush at a TRANSACTION boundary, so its
            condition was `(and (>= n batch-size) (not= t last-t))`. A dump whose
            records all carry one `t` never satisfies that — and such a dump is
            ordinary, not exotic: any database built by a single large `transact`
            or by `load-entities` has every datom at one transaction. Without a
            backstop nothing flushed until the very end and the whole dump sat in
            memory, which is precisely what `:chunk-size` and `:batch-size` exist
            to prevent.

            Counted through `:progress-fn`, which reports `{:phase :batch}` once
            per flush. The assertion contrasts the backstop against ITSELF
            disabled — `:max-pending` set absurdly high reproduces the old
            behaviour exactly — so it cannot pass vacuously: measured at 809
            datoms in one transaction, 1 flush without the backstop and 11 with
            it."
    (let [conn (utils/setup-db (mem-cfg))
          _    (d/transact conn base-schema)
          ;; ONE transaction, many datoms — every record shares a `t`
          _    (d/transact conn (vec (for [i (range 400)]
                                       {:name (str "e" i) :score i})))
          path (str (System/getProperty "java.io.tmpdir") "/dh-fid-1tx-" (utils/get-time))
          _    (m/export-db @conn path {:history? true})
          run  (fn [opts]
                 (let [c (utils/setup-db (mem-cfg))
                       n (atom 0)
                       _ (m/import-db c path (assoc opts :progress-fn
                                                    (fn [ev] (when (= :batch (:phase ev))
                                                               (swap! n inc)))))
                       datoms (set (map (juxt :e :a :v :tx :added) (d/datoms @c :eavt)))]
                   (teardown c)
                   {:flushes @n :datoms datoms}))
          off  (run {:batch-size 20 :max-pending 100000000})
          on   (run {:batch-size 20})
          tight (run {:batch-size 20 :max-pending 50})]
      (is (= 1 (:flushes off))
          "without the backstop the whole single-transaction dump buffers — the bug")
      (is (> (:flushes on) 5)
          (str "with it the stream drains incrementally, got " (:flushes on) " flushes"))
      (is (> (:flushes tight) (:flushes on))
          ":max-pending tightens the ceiling")
      (is (= (:datoms off) (:datoms on) (:datoms tight))
          "and splitting one transaction across flushes changes nothing")
      (teardown conn))))

(deftest a-restore-is-faithful-and-batch-size-does-not-show
  (testing "the restored database equals its source, and `:batch-size` — a
            tuning knob — leaves no trace in it.

            `:batch-size` decides how many records go into one `load-entities`
            call. Nothing about the RESULT may depend on it, or two restores of
            one dump cannot be compared and every temporal query shifts with a
            performance setting.

            This once failed. `transact-entities-directly` bumped `max-tx` once
            per CALL on top of the correct bump per source transaction, so each
            batch boundary skipped a transaction id and stretched the numbering:
            at `:batch-size 5` the six transactions of a small fixture landed on
            ids stepping by two, at one batch they stepped by one, and 240 of 253
            datoms differed in their `tx` component. The old test pinned the
            resulting drift at `+1` and asserted 'datom content is unaffected' —
            both true only because its fixture fitted in a single batch.

            So the property is stated over batch sizes, and includes `max-tx`:
            the restored database is now identical to its source, holes and all
            absent."
    (let [src  (build-adversarial-db! (utils/setup-db (mem-cfg)))
          path (str (System/getProperty "java.io.tmpdir") "/dh-fid-mt-" (utils/get-time))
          _    (m/export-db @src path {:history? true})
          tuples (fn [db] (set (map (juxt :e :a :v :tx :added) (d/datoms db :eavt))))
          restore (fn [bs] (let [c (utils/setup-db (mem-cfg))
                                 rep (m/import-db c path {:batch-size bs})
                                 r {:max-tx (:max-tx @c) :datoms (tuples @c)
                                    :drift (:max-tx-drift rep)}]
                             (teardown c) r))
          tiny  (restore 3)
          small (restore 17)
          huge  (restore 1000000)]
      (testing "identical across batch sizes — datoms AND max-tx"
        (is (= (:datoms tiny) (:datoms small) (:datoms huge)))
        (is (= (:max-tx tiny) (:max-tx small) (:max-tx huge))))
      (testing "and faithful to the source: no drift at any batch size"
        (is (= (:max-tx @src) (:max-tx huge)) "max-tx equals the source's")
        (is (= (tuples @src) (:datoms huge)) "every datom, tx component included")
        (is (= 0 (:drift tiny) (:drift small) (:drift huge))
            "and the report agrees"))
      (testing "a second round trip changes nothing"
        (let [gen1 (utils/setup-db (mem-cfg))
              _    (m/import-db gen1 path {})
              p2   (str (System/getProperty "java.io.tmpdir") "/dh-fid-mt2-" (utils/get-time))
              _    (m/export-db @gen1 p2 {:history? true})
              gen2 (utils/setup-db (mem-cfg))
              rep2 (m/import-db gen2 p2 {})]
          (is (= (:max-tx @gen1) (:max-tx @gen2)))
          (is (= (tuples @gen1) (tuples @gen2)))
          (is (zero? (:max-tx-drift rep2)))
          (teardown gen1) (teardown gen2)))
      (teardown src))))

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
