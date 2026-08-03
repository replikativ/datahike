(ns datahike.test.as-of-resolution-test
  "`as-of` must reconstruct the database as it stood after a transaction, and
   agree with the current indices when asked about the latest one.

   The hard case is a value asserted and retracted within ONE transaction. Both
   history entries then carry the same `tx`, and `assemble-datoms-xform` sorted
   by `tx` alone — a stable sort preserves input order on ties, and that order
   comes from the temporal index, where `added` sorts false before true. So the
   assertion always won and `as-of` reported a fact the current indices said was
   retracted.

   Two ingredients fix it, and neither is sufficient alone:

   1. Assertions fold before retractions within a transaction, so the retraction
      wins on a tie. A temporal assertion and retraction sharing a `tx` can only
      mean the datom was created and removed inside that transaction.
   2. That would be wrong for the opposite order — retract-then-assert, where
      the re-assertion is live and must survive. It is not in the temporal index
      at all; it arrives from the current indices through `distinct-datoms`, and
      only a current-index probe tells the two apart.

   The probe is confined to the ambiguous case: the fold reports which values it
   dropped because of a retraction at the SAME tx as their assertion, and only
   those are looked up. An ordinary retraction — assertion and retraction at
   different transactions — is already resolved by the fold and costs nothing.

   The cardinality-one branch needs the same rule PER VALUE rather than per
   attribute: a transaction that retracts one value and asserts another has both
   an assertion and a retraction at the latest tx, and only the retracted value
   may be suppressed. `time-variance-test/test-filter-current-values-of-same-transaction`
   covers that shape and caught exactly this error."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.test.utils :as utils]))

(defn- conn-with [schema]
  (let [conn (utils/setup-db {:store {:backend :memory :id (java.util.UUID/randomUUID)}
                              :keep-history? true :schema-flexibility :write})]
    (d/transact conn (into [{:db/ident :name :db/valueType :db.type/string
                             :db/cardinality :db.cardinality/one
                             :db/unique :db.unique/identity}]
                           schema))
    (d/transact conn [{:db/id -1 :name "a"}])
    [conn (:e (first (d/datoms @conn :avet :name "a")))]))

(def ^:private card-many
  [{:db/ident :tag :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/many}])

(def ^:private card-one
  [{:db/ident :score :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}])

(defn- present? [db e a v] (boolean (seq (d/datoms db :eavt e a v))))

;; ---------------------------------------------------------------------------

(deftest a-value-asserted-and-retracted-in-one-transaction-is-absent
  (testing "cardinality many"
    (let [[conn e] (conn-with card-many)]
      (try
        (d/transact conn [[:db/add e :tag :x] [:db/retract e :tag :x]])
        (let [db @conn]
          (is (false? (present? db e :tag :x)) "current")
          (is (false? (present? (d/as-of db (:max-tx db)) e :tag :x)) "as-of"))
        (finally (d/release conn)))))
  (testing "cardinality one"
    (let [[conn e] (conn-with card-one)]
      (try
        (d/transact conn [[:db/add e :score 1] [:db/retract e :score 1]])
        (let [db @conn]
          (is (false? (present? db e :score 1)) "current")
          (is (false? (present? (d/as-of db (:max-tx db)) e :score 1)) "as-of"))
        (finally (d/release conn))))))

(deftest the-opposite-order-keeps-the-re-assertion
  (testing "retract-then-assert leaves the value present — the case that a
            retraction-always-wins rule would break. The re-assertion is not in
            the temporal index; it is the live datom."
    (let [[conn e] (conn-with card-many)]
      (try
        (d/transact conn [[:db/add e :tag :x]])
        (d/transact conn [[:db/retract e :tag :x] [:db/add e :tag :x]])
        (let [db @conn]
          (is (true? (present? db e :tag :x)) "current")
          (is (true? (present? (d/as-of db (:max-tx db)) e :tag :x)) "as-of"))
        (finally (d/release conn))))))

(deftest a-retraction-of-one-value-does-not-suppress-another
  (testing "cardinality one, a transaction that retracts one value and asserts
            another. Both an assertion and a retraction sit at the latest tx, so
            an attribute-level rule would wrongly drop the new value — the rule
            has to be per VALUE."
    (let [[conn e] (conn-with card-one)]
      (try
        (d/transact conn [[:db/add e :score 1]])
        (d/transact conn [[:db/retract e :score 1] [:db/add e :score 2]])
        (let [db @conn]
          (is (true? (present? db e :score 2)) "the new value is current")
          (is (true? (present? (d/as-of db (:max-tx db)) e :score 2))
              "and as-of sees it")
          (is (false? (present? (d/as-of db (:max-tx db)) e :score 1))
              "while the retracted one is gone"))
        (finally (d/release conn))))))

(deftest history-is-still-readable-at-every-point
  (testing "assert, retract and re-assert across three transactions — the
            ordinary path, which must be unchanged"
    (let [[conn e] (conn-with card-many)
          txs (atom [])]
      (try
        (doseq [ops [[[:db/add e :tag :x]]
                     [[:db/retract e :tag :x]]
                     [[:db/add e :tag :x]]]]
          (d/transact conn ops)
          (swap! txs conj (:max-tx @conn)))
        (let [db @conn [t1 t2 t3] @txs]
          (is (true? (present? (d/as-of db t1) e :tag :x)) "asserted at t1")
          (is (false? (present? (d/as-of db t2) e :tag :x)) "retracted at t2")
          (is (true? (present? (d/as-of db t3) e :tag :x)) "re-asserted at t3")
          (is (true? (present? db e :tag :x)) "and currently present"))
        (finally (d/release conn))))))

(deftest a-cardinality-one-overwrite-chain-reads-back
  (testing "each value is visible as-of its own transaction and not later ones"
    (let [[conn e] (conn-with card-one)
          txs (atom [])]
      (try
        (doseq [v [1 2 3]]
          (d/transact conn [[:db/add e :score v]])
          (swap! txs conj (:max-tx @conn)))
        (let [db @conn [t1 t2 t3] @txs]
          (is (true? (present? (d/as-of db t1) e :score 1)))
          (is (true? (present? (d/as-of db t2) e :score 2)))
          (is (false? (present? (d/as-of db t2) e :score 1))
              "the superseded value is not current as of t2")
          (is (true? (present? (d/as-of db t3) e :score 3)))
          (is (true? (present? db e :score 3))))
        (finally (d/release conn))))))

(deftest churn-after-a-prior-life
  (testing "a value that lived, was retracted, and is then asserted+retracted
            again inside one transaction — the fold must not resurrect it"
    (let [[conn e] (conn-with card-many)]
      (try
        (d/transact conn [[:db/add e :tag :x]])
        (d/transact conn [[:db/retract e :tag :x]])
        (d/transact conn [[:db/add e :tag :x] [:db/retract e :tag :x]])
        (let [db @conn]
          (is (false? (present? db e :tag :x)) "current")
          (is (false? (present? (d/as-of db (:max-tx db)) e :tag :x)) "as-of"))
        (finally (d/release conn))))))
