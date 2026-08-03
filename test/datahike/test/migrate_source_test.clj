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
            [datahike.test.utils :as utils]))

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
