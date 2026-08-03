(ns datahike.test.db-hash-test
  "`:hash` must equal the additive datom-sum over `:eavt`.

   That is not an incidental property, it is the field's definition. `init-db`
   computes `(reduce #(+ %1 (hash %2)) 0 datoms)` directly, and `db-view-hash`
   computes the same sum for the DB VIEWS, whose docstring says it is
   \"deliberately the SAME additive datom-sum DB maintains incrementally ...
   `equiv-db` reports those equal, so their hashes must agree\". `DB` itself
   returns the stored field from `-hash`, `hashCode` and `hasheq`, and
   `equiv-db` uses it as a conjunct — so a DB whose running sum has drifted
   compares UNEQUAL to a DB holding exactly the same datoms.

   The invariant had no test, which is how it drifted for five years:

     2019-06-28  03d45475  introduced the rolling sum, correctly: `+` on assert
                           and `-` on retract, nothing else.
     2020-06-23  4a5f062e  added `keep-history? (update :hash + (hash prim))` to
                           the retract branch, where `prim` is the RETRACTION
                           datom — which goes to the temporal trees, not `:eavt`.
     2020-06-24  e9d55972  created `with-datom-upsert` by copying the assert
                           branch and swapping `-insert` for `-upsert`. `-upsert`
                           REPLACES rather than adds, so the copied `+` needed a
                           matching `-`. The commit even carries the old-datom
                           lookup, commented out, beside a TODO saying the old
                           datom has to be retracted first.

   Those two are fixed here. A THIRD source is older than both, is not fixed
   here, and has its own test — see `re-asserting-a-present-datom-double-counts`.

   Note the subtraction in the retract branch uses `removing`, the datom found in
   the index, and nothing adds the caller's datom back. That matters on the
   raw-Datom path (`d/transact` with a Datom, `d/load-entities`), which carries
   the CALLER's value object: `:db.type/bytes` and the array types compare by
   content but hash by identity, so an added term would not have cancelled the
   subtracted one. `a-raw-datom-retraction-of-an-identity-hashed-value` pins it.

   The one existing test that touches this (`db-test/test-equiv-db-hash`) asserts
   that assert-then-`retractEntity` returns to the empty hash — the round trip,
   which is the case that never broke. `api-test/test-database-hash` passes both
   before and after: its history arm survives only because every transaction adds
   a `:db/txInstant` datom to the CURRENT tree, which moves the sum whichever
   definition is in force. The two consumers that would otherwise have caught the
   drift are immune: `db-snapshot-key` and `execute`'s cache key both include
   `:max-tx`, which strictly increases per transaction, so cache identity never
   depended on `:hash` moving at all.

   Everything here is expressed against the DEFINITION rather than against
   recorded values, so it stays meaningful if the datom hash ever changes."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.datom :as dd]
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

(defn- sum-over-eavt
  "The definition: `init-db` and `db-view-hash` both compute exactly this."
  [db]
  (reduce #(+ %1 (hash %2)) 0 (d/datoms db :eavt)))

(defn- with-conn
  "Run `f` on a connection carrying `schema`, then release it."
  [keep-history? f]
  (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
             :keep-history? keep-history? :schema-flexibility :write}
        conn (utils/setup-db cfg)]
    (try
      (d/transact conn schema)
      (f conn)
      (finally (d/release conn)))))

(defn- check-invariant!
  [label keep-history? txs]
  (with-conn keep-history?
    (fn [conn]
      (doseq [tx txs] (d/transact conn tx))
      (let [db @conn]
        (is (= (sum-over-eavt db) (:hash db))
            (str label " (keep-history? " keep-history? "): :hash must equal the "
                 "additive datom-sum over :eavt"))))))

;; ---------------------------------------------------------------------------

(deftest hash-equals-the-datom-sum-over-eavt
  (testing "for every transaction shape, under both history settings"
    (doseq [keep-history? [false true]]
      (check-invariant! "plain assertions" keep-history?
                        [[{:db/id -1 :name "a" :score 1 :tag :x}]])

      ;; regression 2: -upsert replaces, so the running sum must lose the old term
      (check-invariant! "card-one overwrite" keep-history?
                        [[{:db/id -1 :name "a" :score 1}]
                         [{:db/id [:name "a"] :score 2}]])

      (check-invariant! "repeated card-one overwrite" keep-history?
                        [[{:db/id -1 :name "a" :score 1}]
                         [{:db/id [:name "a"] :score 2}]
                         [{:db/id [:name "a"] :score 3}]
                         [{:db/id [:name "a"] :score 4}]])

      (check-invariant! "overwrite with the SAME value" keep-history?
                        [[{:db/id -1 :name "a" :score 1}]
                         [{:db/id [:name "a"] :score 1}]])

      ;; regression 1: the retraction datom goes to temporal, never to :eavt
      (check-invariant! "explicit retraction" keep-history?
                        [[{:db/id -1 :name "a" :tag :x}]
                         [[:db/retract [:name "a"] :tag :x]]])

      (check-invariant! "retract then re-assert" keep-history?
                        [[{:db/id -1 :name "a" :tag :x}]
                         [[:db/retract [:name "a"] :tag :x]]
                         [{:db/id [:name "a"] :tag :x}]])

      (check-invariant! "card-many add and retract" keep-history?
                        [[{:db/id -1 :name "a" :tag :x}]
                         [{:db/id [:name "a"] :tag :y}]
                         [[:db/retract [:name "a"] :tag :x]]])

      (check-invariant! "retractEntity" keep-history?
                        [[{:db/id -1 :name "a" :score 1 :tag :x}]
                         [[:db/retractEntity [:name "a"]]]])

      (check-invariant! ":db/noHistory overwrite" keep-history?
                        [[{:db/id -1 :name "a" :note "first"}]
                         [{:db/id [:name "a"] :note "second"}]])

      ;; shapes that separate this rule from the plausible alternatives — each
      ;; one distinguished two candidate definitions during review.
      ;; NOTE: re-asserting an ALREADY PRESENT card-many datom is deliberately
      ;; absent here — see `re-asserting-a-present-datom-double-counts`.
      (check-invariant! "revert to an older value" keep-history?
                        [[{:db/id -1 :name "a" :score 1}]
                         [{:db/id [:name "a"] :score 2}]
                         [{:db/id [:name "a"] :score 1}]])

      (check-invariant! "schema attribute overwritten" keep-history?
                        [[{:db/id -1 :name "a"}]
                         [{:db/ident :score :db/doc "first"}]
                         [{:db/ident :score :db/doc "second"}]])

      (check-invariant! "unique-identity attribute re-upserted" keep-history?
                        [[{:db/id -1 :name "a" :score 1}]
                         [{:name "a" :score 2}]])

      (check-invariant! "everything at once" keep-history?
                        [[{:db/id -1 :name "a" :score 1 :tag :x}
                          {:db/id -2 :name "b" :score 2 :tag :y}]
                         [{:db/id [:name "a"] :score 10}]
                         [[:db/retract [:name "b"] :tag :y]]
                         [{:db/id [:name "a"] :note "n"}]
                         [{:db/id [:name "a"] :note "n2"}]
                         [[:db/retractEntity [:name "b"]]]]))))

(deftest equal-content-implies-equal-hash
  (testing "two databases holding the same datoms must hash alike, because
            `equiv-db` uses `(= (hash db) (hash other))` as a conjunct — so a
            drifted sum makes equal databases compare UNEQUAL.

            The two are reached differently on purpose: one by overwriting a
            card-one value, one by asserting the final value directly. Before the
            fix these agreed on datoms and disagreed on hash.

            `:keep-history? false` only, and that is a property of the question
            rather than a gap in the test. With history on, the tx entities stay
            in `:eavt`, so the two sequences differ by a `:db/txInstant` datom per
            extra transaction — and those carry wall-clock values. Two histories
            of different lengths are then genuinely different databases, and
            asking them to hash alike would be asking for the wrong answer."
    (let [final-of (fn [txs]
                     (with-conn false
                       (fn [conn]
                         (doseq [tx txs] (d/transact conn tx))
                         (let [db @conn]
                           {:hash (:hash db)
                            :datoms (set (map (juxt :e :a :v) (d/datoms db :eavt)))}))))
          overwritten (final-of [[{:db/id -1 :name "a" :score 1}]
                                 [{:db/id [:name "a"] :score 2}]])
          direct (final-of [[{:db/id -1 :name "a" :score 2}]])]
      (is (= (:datoms overwritten) (:datoms direct))
          "precondition: the same datoms either way")
      (is (= (:hash overwritten) (:hash direct))
          "and therefore the same hash"))))

(deftest re-asserting-a-present-datom-double-counts
  "KNOWN GAP, not fixed here — asserts the CURRENT behaviour so that fixing it
   trips this test rather than passing silently.

   `with-datom`'s assert branch adds `(hash prim)` unconditionally, including
   when `di/-insert` changed nothing because the datom was already there.
   Measured: after asserting the same card-many datom twice, `:eavt` holds ONE
   entry (with the FIRST transaction's tx, so the insert really was a no-op)
   while `:hash` has gained the term twice.

   This is a third drift source, independent of the two this commit fixes and
   older than both — it is in the original 2019 assert branch. It is left alone
   because the fix belongs on the assert hot path: the cheap form compares the
   index count before and after the insert, which is O(1) for
   persistent-sorted-set but not obviously so for every index implementation.
   That deserves its own change and its own benchmark."
  (testing "the sum over :eavt and the running :hash disagree by exactly one term"
    (doseq [keep-history? [false true]]
      (with-conn keep-history?
        (fn [conn]
          (d/transact conn [{:db/id -1 :name "a" :tag :x}])
          (let [before (:hash @conn)
                sum-before (sum-over-eavt @conn)]
            (is (= before sum-before) "consistent before the duplicate assertion")
            (d/transact conn [{:db/id [:name "a"] :tag :x}])
            (let [db @conn
                  entries (filter #(and (= :tag (:a %)) (= :x (:v %)))
                                  (d/datoms db :eavt))]
              (is (= 1 (count entries))
                  "the index holds the datom once — the insert was a no-op")
              (is (not= (sum-over-eavt db) (:hash db))
                  "but :hash counted it twice — REMOVE THIS ASSERTION WHEN FIXED"))))))))

(deftest purge-keeps-the-sum-consistent
  (testing "`with-temporal-datom` subtracts only under `current?`, which is
            exactly right here: purging removes the datom from the current tree
            when it is there, and a purge of a temporal-only datom must not move
            a sum that is taken over the current tree.

            Worth pinning because an earlier proposal added a second subtraction
            for the temporal-only case. Under a union-based definition that
            would have been needed — and would still have been wrong, because a
            superseded triple has TWO temporal entries and the purge visits each."
    (with-conn true
      (fn [conn]
        (d/transact conn [{:db/id -1 :name "a" :score 1 :tag :x}])
        (d/transact conn [{:db/id [:name "a"] :score 2}])
        (d/transact conn [[:db/retract [:name "a"] :tag :x]])
        (let [e (:e (first (d/datoms @conn :avet :name "a")))]
          (testing "purging a value that is in BOTH trees"
            (d/transact conn [[:db/purge e :score 2]])
            (let [db @conn] (is (= (sum-over-eavt db) (:hash db)))))
          (testing "purging a value that is ONLY in the temporal tree"
            (d/transact conn [[:db/purge e :score 1]])
            (let [db @conn] (is (= (sum-over-eavt db) (:hash db)))))
          (testing "purging a retracted card-many value (temporal only)"
            (d/transact conn [[:db/purge e :tag :x]])
            (let [db @conn] (is (= (sum-over-eavt db) (:hash db))))))))))

(deftest a-raw-datom-retraction-of-an-identity-hashed-value
  (testing "`transact-tx-data` accepts a Datom directly and retracts using the
            CALLER's value object, not the stored one. `:db.type/bytes` (and the
            array types) compare by content but hash by identity, so the caller's
            object and the stored one can hash differently.

            This is safe here only because the sum is maintained by subtracting
            `removing` — the datom found in the index — and nothing adds the
            caller's datom. An earlier proposal kept a `+ (hash prim)` term in
            this branch on the theory that it cancelled the subtraction; on this
            path it does not, and the sum would drift by the difference."
    (with-conn true
      (fn [conn]
        (d/transact conn [{:db/ident :blob :db/valueType :db.type/bytes
                           :db/cardinality :db.cardinality/one}])
        (d/transact conn [{:db/id -1 :name "a" :blob (byte-array [1 2 3])}])
        (let [db @conn
              stored (first (filter #(= :blob (:a %)) (d/datoms db :eavt)))]
          (is (some? stored) "precondition: the blob datom is present")
          (is (= (sum-over-eavt db) (:hash db)) "consistent before the retraction")
          ;; a Datom carrying an EQUAL-BUT-NOT-IDENTICAL value
          (d/transact conn [(dd/datom (:e stored) :blob (byte-array [1 2 3])
                                      (:tx stored) false)])
          (let [db' @conn]
            (is (= (sum-over-eavt db') (:hash db'))
                "and consistent after retracting through the raw-Datom path")))))))

(deftest a-view-and-a-db-holding-the-same-datoms-hash-alike
  (testing "`db-view-hash` computes the sum on demand for FilteredDB and friends,
            and its docstring makes agreement with DB's stored field a
            requirement. A filter that removes nothing must therefore hash like
            the database itself."
    (doseq [keep-history? [false true]]
      (with-conn keep-history?
        (fn [conn]
          (d/transact conn [{:db/id -1 :name "a" :score 1 :tag :x}])
          (d/transact conn [{:db/id [:name "a"] :score 2}])
          (d/transact conn [[:db/retract [:name "a"] :tag :x]])
          (let [db @conn
                keep-all (d/filter db (fn [_ _] true))]
            (is (= (hash db) (hash keep-all))
                (str "a no-op filter must hash like the db (keep-history? "
                     keep-history? ")"))))))))
