(ns datahike.test.migrate-source-test
  "Importing from a source that is NOT a datahike dump.

   `run-import` used to take the dump's manifest and read five keys from it.
   Those five were the whole of its dependency on the dump FORMAT, so it takes
   them directly now as `source-meta` — and that is what makes CSV files, triple
   dumps and pre-sorted synthetic data possible without a second importer.

   These tests drive the importer with `{:chunks … :read …}` and a hand-built
   `source-meta`, with no manifest, no chunk files, no digest and no
   `:source-config`. If they pass, the seam is real; if the importer ever reaches
   back into a manifest again, they are what notices.

   `run-import` is private, so these use `#'` to reach it — the same thing
   `migrate_ids_test` does for `open-dump`/`reduce-dump-records`."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.migrate :as m]
            [datahike.test.utils :as utils]
            [clojure.set]))

(def ^:private run-import #'m/run-import)

(defn- fresh-conn []
  (utils/setup-db {:store {:backend :memory :id (java.util.UUID/randomUUID)}
                   :keep-history? false
                   :schema-flexibility :read}))

(defn- teardown [conn]
  (let [cfg (:config @conn)] (d/release conn) (d/delete-database cfg)))

(defn- records
  "`n` records over `n` entities, one transaction each, in tx order. `t` values
   start above `tx0` because the importer only counts user-transaction datoms."
  [n]
  (vec (for [i (range n)]
         [(+ 100 i) :name (str "n" i) (+ 536870913 i) true])))

(defn- chunks-of
  "A record source over `records`, `per` records to a chunk. Chunk descriptors
   are whatever the source wants — the importer never inspects them."
  [records per]
  (let [parts (vec (partition-all per records))]
    {:chunks (vec (range (count parts)))
     :read (fn [i _opts] (nth parts i))}))

;; ---------------------------------------------------------------------------

(deftest imports-from-a-source-with-no-manifest
  (testing "the whole point. No manifest, no dump directory, no digest — just
            chunks of [e a v t op] and the three things source-meta carries."
    (let [conn (fresh-conn)
          rs (records 300)
          rep (run-import conn
                          {:history? false :expected-count (count rs)}
                          {}
                          (chunks-of rs 50)
                          {:sync? true})]
      (is (= 300 (:datom-count rep)) "every record landed")
      (is (true? (:verified? rep)) "and the count check passed against
                                    source-meta's :expected-count")
      (is (= #{"n0" "n299"}
             (set (map first (d/q '[:find ?v :where [?e :name ?v]
                                    [(contains? #{"n0" "n299"} ?v)]]
                                  @conn))))
          "and the data is queryable")
      (teardown conn))))

(deftest a-source-that-knows-no-count-can-still-import
  (testing "`:expected-count` is optional. A CSV reader does not know how many
            datoms it will produce until it has produced them, so verification
            has to be something the caller can decline rather than a
            precondition of importing at all."
    (let [conn (fresh-conn)
          rs (records 120)
          rep (run-import conn {} {} (chunks-of rs 40) {:sync? true :verify? false})]
      (is (= 120 (:datom-count rep)))
      (is (nil? (:verified? rep)) "verification was declined, not failed")
      (is (nil? (:max-tx-drift rep)) "and with no :max-tx there is no drift claim")
      (teardown conn))))

(deftest a-wrong-count-is-still-caught
  (testing "declining verification must be explicit — a source that STATES a
            count is held to it, or the check would be decoration."
    (let [conn (fresh-conn)
          rs (records 50)]
      (is (thrown-with-msg?
           Exception #"verification failed"
           (run-import conn {:expected-count 999} {} (chunks-of rs 10) {:sync? true})))
      (teardown conn))))

(deftest the-translate-hook-works-on-a-non-dump-source
  (testing "filtering is the same operation for any source. Dropping an
            attribute is `:translate` returning nil, and the dropped records are
            subtracted from the expected count so the drop is not reported as
            corruption."
    (let [conn (fresh-conn)
          rs (into (records 100)
                   (for [i (range 100)]
                     [(+ 100 i) :secret (str "s" i) (+ 536870913 i) true]))
          rep (run-import conn
                          {:expected-count (count rs)}
                          {}
                          (chunks-of rs 25)
                          {:sync? true
                           :translate (fn [[_ a _ _ _ :as r]]
                                        (when-not (= a :secret) r))})]
      (is (true? (:translated? rep)))
      (is (= 100 (:dropped rep)) "every :secret record was dropped")
      (is (= 100 (:datom-count rep)) "and only the kept ones landed")
      (is (true? (:verified? rep))
          "verification accounted for the drop rather than reporting corruption")
      (is (empty? (d/q '[:find ?v :where [?e :secret ?v]] @conn))
          "nothing secret reached the database")
      (teardown conn))))

(deftest one-transaction-for-every-record-does-not-defeat-the-batcher
  (testing "the batcher flushes on `(and (>= n batch-size) (not= t last-t))` —
            it will not split a transaction. A source that gives every record a
            DISTINCT t therefore flushes freely."
    (let [conn (fresh-conn)
          rs (records 500)
          rep (run-import conn {:expected-count 500} {} (chunks-of rs 100)
                          {:sync? true :batch-size 10})]
      (is (= 500 (:datom-count rep)))
      (is (= 500 (:tx-count rep)) "one source transaction per record")
      (teardown conn))))

(deftest every-record-in-one-transaction-is-the-memory-hazard
  (testing "the other end of that rule, and the trap for a synthetic source:
            when every record shares one `t`, the batcher can NEVER flush early
            — `(not= t last-t)` is false forever — so the whole import
            accumulates in one batch regardless of `:batch-size`.

            That is correct (a transaction must not be split) and it is a real
            memory ceiling a CSV or synthetic source has to know about: give
            rows a `t` that advances, or accept holding the import in memory.
            Pinned here so the behaviour is documented rather than discovered."
    (let [conn (fresh-conn)
          one-t 536870913
          rs (vec (for [i (range 400)] [(+ 100 i) :name (str "n" i) one-t true]))
          rep (run-import conn {:expected-count 400} {} (chunks-of rs 50)
                          {:sync? true :batch-size 10})]
      (is (= 400 (:datom-count rep)) "it still imports correctly")
      (is (= 1 (:tx-count rep)) "as a single source transaction")
      (teardown conn))))

(deftest chunk-descriptors-are-opaque
  (testing "`:read` is handed back whatever `:chunks` holds. The importer never
            inspects a descriptor, which is why a file, a store key, a row range
            or an index all work without it knowing the difference."
    (let [conn (fresh-conn)
          rs (records 60)
          seen (atom [])
          src {:chunks [{:tag :first} {:tag :second}]
               :read (fn [d _] (swap! seen conj (:tag d))
                       (case (:tag d) :first (subvec rs 0 30) :second (subvec rs 30)))}
          rep (run-import conn {:expected-count 60} {} src {:sync? true})]
      (is (= [:first :second] @seen) "descriptors reached :read untouched")
      (is (= 60 (:datom-count rep)))
      (teardown conn))))

;; ---------------------------------------------------------------------------
;; :eids — how source entity ids bind to target ones

(deftest eids-preserve-keeps-source-ids
  (testing "the dump's ids are already correct. `transact-entities-directly`
            normally allocates fresh ones and the source ids vanish; `:preserve`
            seeds `:migration` with identity, and a seeded answer wins over
            allocation."
    (let [conn (fresh-conn)
          rs [[100 :name "a" 536870913 true]
              [200 :name "b" 536870913 true]
              [300 :name "c" 536870914 true]]
          rep (run-import conn {:expected-count 3} {} (chunks-of rs 3)
                          {:sync? true :eids :preserve})]
      (is (= 3 (:datom-count rep)))
      (is (= #{100 200 300} (set (map :e (d/datoms @conn :eavt))))
          "the source eids came through unchanged")
      (teardown conn))))

(deftest eids-allocate-is-still-the-default
  (testing "nothing changes for a caller that says nothing. Fresh ids, source
            ids irrelevant."
    (let [conn (fresh-conn)
          rs [[100 :name "a" 536870913 true]]
          _ (run-import conn {:expected-count 1} {} (chunks-of rs 1) {:sync? true})]
      (is (not= #{100} (set (map :e (d/datoms @conn :eavt))))
          "the source eid was NOT preserved")
      (teardown conn))))

(deftest eids-offset-cannot-collide
  (testing "`:offset` shifts every source id above the target's max-eid, which
            is what makes it safe for a database the caller knows to be
            disjoint from the dump."
    (let [conn (fresh-conn)
          ;; give the target some entities of its own first
          _ (d/transact conn (vec (for [i (range 20)] {:name (str "existing" i)})))
          before (set (map :e (d/datoms @conn :eavt)))
          rs [[1 :name "imported-a" 536870913 true]
              [2 :name "imported-b" 536870913 true]]
          _ (run-import conn {:expected-count 2} {} (chunks-of rs 2)
                        {:sync? true :eids :offset :verify? false})
          after (set (map :e (d/datoms @conn :eavt)))
          added (clojure.set/difference after before)]
      (is (= 2 (count added)) "two new entities")
      (is (every? #(> % (apply max before)) added)
          "every imported eid sits above everything that was already there")
      (teardown conn))))

(deftest a-caller-supplied-function-is-honoured
  (testing "the general case the three named strategies are shorthands for."
    (let [conn (fresh-conn)
          rs [[7 :name "seven" 536870913 true]]
          _ (run-import conn {:expected-count 1} {} (chunks-of rs 1)
                        {:sync? true :eids (fn [e] (* 1000 e))})]
      (is (= #{7000} (set (map :e (d/datoms @conn :eavt))))
          "the function decided the id")
      (teardown conn))))

(deftest a-function-policy-holds-no-id-map
  (testing "the reason a function exists at all. `:allocate` accumulates an
            entry per entity in `:migration` — O(entities), and what the heap
            warning is about. A function answers without remembering, so the
            map stays empty however many entities are imported."
    (let [conn (fresh-conn)
          rs (vec (for [i (range 500)]
                    [(+ 1000 i) :name (str "n" i) (+ 536870913 i) true]))]
      (run-import conn {:expected-count 500} {} (chunks-of rs 100)
                  {:sync? true :eids :preserve :finalize? false})
      (is (fn? (:eids (:migration @conn)))
          ":eids is still the policy function — no map was accumulated at all")

      (testing "whereas the default remembers every one"
        (let [c2 (fresh-conn)]
          (run-import c2 {:expected-count 500} {} (chunks-of rs 100)
                      {:sync? true :finalize? false})
          (is (= 500 (count (:eids (:migration @c2))))
              "one entry per source entity")
          (teardown c2)))
      (teardown conn))))

(deftest refs-follow-the-policy
  (testing "a ref VALUE is remapped through the same policy as an entity id, or
            an imported reference would point at the wrong entity."
    (let [conn (utils/setup-db {:store {:backend :memory :id (java.util.UUID/randomUUID)}
                                :keep-history? false
                                :schema-flexibility :write})]
      (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one}
                        {:db/ident :pal :db/valueType :db.type/ref
                         :db/cardinality :db.cardinality/one}])
      (let [rs [[500 :name "a" 536870914 true]
                [600 :name "b" 536870914 true]
                [600 :pal 500 536870914 true]]
            _ (run-import conn {:expected-count 3} {} (chunks-of rs 3)
                          {:sync? true :eids :preserve :verify? false})]
        (is (= #{[600 500]}
               (set (d/q '[:find ?e ?v :where [?e :pal ?v]] @conn)))
            "the ref points at the preserved id, not a freshly allocated one"))
      (teardown conn))))

(deftest preserve-into-an-occupied-target-warns-rather-than-refusing
  (testing "`:preserve` is the caller ASSERTING that the source ids are right
            for this target, and only they can know that. Refusing whenever
            `max-eid > e0` would reject every `:schema-flexibility :write`
            database — schema attributes are entities and occupy ids — while
            proving nothing about whether the ranges actually overlap. So it
            warns and proceeds."
    (let [conn (fresh-conn)
          _ (d/transact conn [{:name "existing"}])
          ;; `:verify? false` because count verification still compares against
          ;; the WHOLE database rather than the delta — correct for an empty
          ;; target, wrong for this one. That is the merge work, not this test's
          ;; subject.
          rep (run-import conn {:expected-count 1} {}
                          (chunks-of [[9999 :name "x" 536870913 true]] 1)
                          {:sync? true :eids :preserve :verify? false})]
      (is (= 2 (:datom-count rep)) "it proceeded (count is whole-db, not a delta)")
      (is (contains? (set (map :e (d/datoms @conn :eavt))) 9999)
          "and used the source id as given")
      (teardown conn))))

(deftest an-unknown-strategy-is-refused
  (let [conn (fresh-conn)]
    (is (thrown-with-msg?
         Exception #"Unknown :eids strategy"
         (run-import conn {} {} (chunks-of (records 1) 1)
                     {:sync? true :eids :nonsense})))
    (teardown conn)))
