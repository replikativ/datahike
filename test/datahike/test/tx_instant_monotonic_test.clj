(ns datahike.test.tx-instant-monotonic-test
  "Tests for the strict-monotonic `:db/txInstant` allocator
   (`datahike.db.transaction/next-tx-instant`). Matches Datomic's
   contract: auto-stamped tx-instants are strictly increasing
   across writes, even when several writes land in the same
   wall-clock millisecond. Removes the d/as-of <Date> tied-instant
   ambiguity at the source.

   See ADR for design rationale + the original flake reproducer."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [datahike.api :as d]
            [datahike.tools :as dt]
            [datahike.db.transaction :as tx])
  (:import [java.util Date]))

(defn- fresh-conn []
  (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
             :schema-flexibility :write
             :keep-history? true}]
    (d/create-database cfg)
    (d/connect cfg)))

(defn- tx-instants [conn]
  (->> (d/q '[:find ?tx ?t :where [?tx :db/txInstant ?t]]
            (d/history (d/db conn)))
       (sort-by first)
       (mapv second)))

(deftest auto-stamp-strictly-monotonic-under-burst
  (testing "back-to-back writes within a single wall-clock ms still get distinct, strictly-ordered :db/txInstant"
    (let [conn (fresh-conn)]
      (d/transact conn [{:db/ident :x :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/one}])
      (dotimes [i 50] (d/transact conn [{:x (long i)}]))
      (let [insts (tx-instants conn)
            ms (mapv #(.getTime ^Date %) insts)]
        (is (= 51 (count insts)) "51 txes (1 schema + 50 data)")
        (is (= (count insts) (count (distinct insts)))
            "no two :db/txInstant values tie")
        (is (apply < ms)
            "values are strictly monotonically increasing")))))

(deftest pinned-clock-becomes-logical-clock
  (testing "with `get-date` pinned to a constant, the allocator advances 1ms per tx — fully deterministic"
    (let [pinned-date #inst "2024-01-01T00:00:00.000-00:00"
          pinned-ms (.getTime ^Date pinned-date)
          orig dt/get-date]
      (alter-var-root #'dt/get-date (constantly (constantly pinned-date)))
      (try
        (let [conn (fresh-conn)]
          (d/transact conn [{:db/ident :x :db/valueType :db.type/long
                             :db/cardinality :db.cardinality/one}])
          (dotimes [i 5] (d/transact conn [{:x (long i)}]))
          (let [ms (mapv #(.getTime ^Date %) (tx-instants conn))]
            (is (= 6 (count ms)))
            (is (= (range pinned-ms (+ pinned-ms 6)) ms)
                "stamps are pinned-ms, pinned-ms+1, pinned-ms+2 …")))
        (finally
          (alter-var-root #'dt/get-date (constantly orig)))))))

(deftest user-provided-txInstant-still-wins
  (testing ":tx-meta {:db/txInstant <date>} still overrides the allocator default, even with a back-dated value"
    (let [conn (fresh-conn)
          past #inst "2024-06-01T00:00:00.000-00:00"]
      (d/transact conn [{:db/ident :x :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/one}])
      (d/transact conn {:tx-meta {:db/txInstant past}
                        :tx-data [{:x 1}]})
      (let [last-data-tx-inst (last (tx-instants conn))]
        (is (= past last-data-tx-inst)
            "user override flows through unchanged — historical-import / SCD2 test patterns preserved")))))

(deftest next-tx-instant-is-dynamic-overridable
  (testing "next-tx-instant is ^:dynamic — a future caller can swap in an entirely custom allocator (e.g., HLC) without changing the call site"
    (let [orig tx/next-tx-instant
          pinned #inst "2030-12-31T23:59:59.999-00:00"]
      (alter-var-root #'tx/next-tx-instant (constantly (fn [_db-before] pinned)))
      (try
        (let [conn (fresh-conn)]
          (d/transact conn [{:db/ident :x :db/valueType :db.type/long
                             :db/cardinality :db.cardinality/one}])
          (is (= pinned (last (tx-instants conn)))
              "custom allocator's value lands on the stamped tx"))
        (finally
          (alter-var-root #'tx/next-tx-instant (constantly orig)))))))

(deftest valid-at-composes-with-as-of-no-flake-under-load
  (testing "previously-flaky scenario (valid-at-composes-with-as-of) — runs N times under concurrent JVM load, zero flakes"
    (let [setup! (fn [c]
                   (d/transact c [{:db/ident :emp/name :db/valueType :db.type/string
                                   :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                                  {:db/ident :emp/salary :db/valueType :db.type/long
                                   :db/cardinality :db.cardinality/one}])
                   (d/transact c [{:db/id "datomic.tx" :db.valid/from #inst "2024-01-01"
                                   :db.valid/to #inst "2024-07-01"}
                                  {:emp/name "Bob" :emp/salary 100000}])
                   (d/transact c [{:db/id "datomic.tx" :db.valid/from #inst "2024-07-01"}
                                  {:emp/name "Bob" :emp/salary 110000}])
                   (d/transact c [{:db/id "datomic.tx" :db.valid/from #inst "2024-01-01"}
                                  {:emp/name "Alice" :emp/salary 80000}]))
          flakes (atom [])
          n 200
          threads (doall
                   (for [_ (range 8)]
                     (future
                       (dotimes [_ (/ n 8)]
                         (let [c (fresh-conn)
                               _ (setup! c)
                               db (d/db c)
                               t2 (d/q '[:find ?t .
                                         :where [?tx :db.valid/from #inst "2024-07-01"]
                                         [?tx :db/txInstant ?t]]
                                       (d/history db))
                               composed (-> db
                                            (d/history)
                                            (d/as-of t2)
                                            (d/valid-at #inst "2024-04-15"))
                               res (d/q '[:find ?s
                                          :where [?e :emp/salary ?s ?tx true]]
                                        composed)]
                           (when (not= #{[100000]} res)
                             (swap! flakes conj res)))))))]
      (doseq [f threads] @f)
      (is (zero? (count @flakes))
          (str "expected 0 flakes, got " (count @flakes)
               ", sample: " (first @flakes))))))
