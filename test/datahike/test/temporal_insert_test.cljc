(ns datahike.test.temporal-insert-test
  "A datom asserted and retracted in the SAME transaction must leave both
   entries in the temporal trees.

   The temporal trees are ordered by `cmp-temporal-datoms-eavt-quick`
   — `[e a v tx added]` — because an assertion and its retraction are distinct
   history entries that agree on the first four components when they share a
   transaction. `-temporal-insert` was passing the CURRENT comparator instead,
   `[e a v tx]`, under which those two datoms compare equal; `PersistentSortedSet`
   returns the receiver unchanged for a key it already holds, so the second
   insert was a silent no-op and the retraction marker was dropped.

   The current tree stayed correct, so present-tense queries were unaffected and
   nothing ever raised. `d/history` showed only the assertion — and because the
   temporal tree is persisted, the loss is durable and cannot be recovered from
   the tree. That also means an export inherited it: a dump is taken from the
   history view.

   Restoring the entry does NOT make `as-of` agree with the current tree; that
   is a separate defect in the query assembly, pinned by
   `as-of-still-resolves-a-same-tx-pair-to-present`. This commit makes the
   stored history complete; it does not change how `as-of` folds it.

   Only same-transaction assert+retract is affected: across transactions the two
   entries differ in `tx`, so the current comparator separates them anyway. Nor
   is ordering ever wrong — the current comparator is a strict PREFIX of the
   temporal one, so the two agree on every pair that is not a `[e a v tx]` tie.
   This was a lost entry, never a misplaced one.

   Both spellings have been present since the durable persistent-set landed
   (`6c1468fd`, 2022-11): the standalone `temporal-insert` helper had the right
   comparator and no callers, while the protocol implementation had the wrong one
   and was the only live path. The fix is to delegate, as `-insert`, `-upsert`
   and `-temporal-upsert` already do."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.test.utils :as utils]))

(defn- with-conn [f]
  (let [conn (utils/setup-db {:store {:backend :memory :id (java.util.UUID/randomUUID)}
                              :keep-history? true :schema-flexibility :write})]
    (try
      (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one
                         :db/unique :db.unique/identity}
                        {:db/ident :tag :db/valueType :db.type/keyword
                         :db/cardinality :db.cardinality/many}])
      (d/transact conn [{:db/id -1 :name "a"}])
      (f conn (:e (first (d/datoms @conn :avet :name "a"))))
      (finally (d/release conn)))))

(defn- temporal-entries
  "Entries for one `[a v]` in a temporal tree, read from the TREE rather than
   through `d/history` — the history view unions in current datoms for card-many
   attributes, which would mask a missing temporal entry."
  [db tree-key a v]
  (vec (filter #(and (= a (:a %)) (= v (:v %))) (seq (get db tree-key)))))

(defn- added-flags [entries] (set (map :added entries)))

;; ---------------------------------------------------------------------------

(deftest assert-and-retract-in-one-transaction-keeps-both-entries
  (testing "the assertion and the retraction share a tx, so they differ only in
            `added` — exactly the pair the current comparator cannot separate"
    (with-conn
      (fn [conn e]
        (d/transact conn [[:db/add e :tag :x] [:db/retract e :tag :x]])
        (let [db @conn]
          (doseq [tree [:temporal-eavt :temporal-aevt]]
            (testing (name tree)
              (let [entries (temporal-entries db tree :tag :x)]
                (is (= 2 (count entries))
                    (str "both history entries must survive, got " (pr-str entries)))
                (is (= #{true false} (added-flags entries))
                    "one assertion and one retraction")))))))))

(deftest the-current-tree-is-unaffected
  (testing "the datom was retracted, so it must be gone from the current tree —
            this is what stayed correct throughout and hid the bug"
    (with-conn
      (fn [conn e]
        (d/transact conn [[:db/add e :tag :x] [:db/retract e :tag :x]])
        (is (empty? (d/datoms @conn :eavt e :tag :x)))))))

(deftest the-retraction-marker-reaches-the-history-view
  (testing "`d/history` must show the retraction, not just the assertion. This
            is the query-visible part of the fix, and it is what an export
            reads — a dump taken from an affected database inherited the loss."
    (with-conn
      (fn [conn e]
        (d/transact conn [[:db/add e :tag :x] [:db/retract e :tag :x]])
        (let [entries (vec (filter #(and (= :tag (:a %)) (= :x (:v %)))
                                   (d/datoms (d/history @conn) :eavt)))]
          (is (= #{true false} (added-flags entries))
              (str "history must carry both, got " (pr-str entries))))))))

(deftest as-of-still-resolves-a-same-tx-pair-to-present
  "KNOWN GAP, not fixed here — asserts the CURRENT behaviour so that fixing it
   trips this test rather than passing silently.

   Restoring the retraction marker does not by itself make `as-of` agree with
   the current tree. `db.cljc`'s `assemble-datoms-xform` sorts by `tx` ALONE,
   and a stable sort on equal keys preserves input order — which comes from the
   tree, where `added` sorts false before true. So within one transaction the
   assertion always wins, whichever order the caller wrote the ops in. The
   cardinality-one branch has the same bias, taking the first ADDED datom at the
   maximum tx.

   That bias is right for retract-then-add and wrong for add-then-retract, and
   it is a defect in the query assembly rather than in the index — which is why
   it is left to a separate change."
  (testing "the current tree says absent; as-of still says present"
    (with-conn
      (fn [conn e]
        (d/transact conn [[:db/add e :tag :x] [:db/retract e :tag :x]])
        (let [db @conn]
          (is (empty? (d/datoms db :eavt e :tag :x))
              "the current tree is correct")
          (is (seq (d/datoms (d/as-of db (:max-tx db)) :eavt e :tag :x))
              "as-of disagrees with it — REMOVE THIS ASSERTION WHEN FIXED"))))))

(deftest a-retract-entity-alongside-a-fresh-assertion
  (testing "the reachable shape: assert an attribute and retract the whole
            entity in one transaction. The freshly asserted datom shares its tx
            with its own retraction; an attribute asserted EARLIER does not, and
            was never affected."
    (with-conn
      (fn [conn e]
        (d/transact conn [{:db/id e :tag :x} [:db/retractEntity e]])
        (let [db @conn]
          (is (= #{true false} (added-flags (temporal-entries db :temporal-eavt :tag :x)))
              "the same-transaction datom keeps both entries")
          (is (= #{true false} (added-flags (temporal-entries db :temporal-eavt :name "a")))
              "and so does the one asserted in an earlier transaction"))))))

;; --- controls: shapes that were already correct, and must stay correct -------

(deftest separate-transactions-were-never-affected
  (testing "different tx ids, so the current comparator separated them anyway"
    (with-conn
      (fn [conn e]
        (d/transact conn [[:db/add e :tag :y]])
        (d/transact conn [[:db/retract e :tag :y]])
        (is (= #{true false}
               (added-flags (temporal-entries @conn :temporal-eavt :tag :y))))))))

(deftest retract-then-assert-in-one-transaction
  (testing "the other order: the retraction targets a datom from an EARLIER
            transaction, so the two entries differ in tx. Correct before and
            after; `datahike.migrate.history` documents datahike as producing
            both records for this shape."
    (with-conn
      (fn [conn e]
        (d/transact conn [[:db/add e :tag :z]])
        (d/transact conn [[:db/retract e :tag :z] [:db/add e :tag :z]])
        (let [db @conn]
          (is (= #{true false} (added-flags (temporal-entries db :temporal-eavt :tag :z))))
          (is (seq (d/datoms db :eavt e :tag :z))
              "and the datom ends up present, since the assertion wins"))))))

(deftest a-card-one-overwrite-is-unaffected
  (testing "card-one goes through `-temporal-upsert`, which always used the
            temporal comparator — included so a future change to that path is
            covered here too"
    (with-conn
      (fn [conn e]
        (d/transact conn [{:db/id e :name "a"}])
        (d/transact conn [{:db/id e :name "b"}])
        (let [db @conn]
          (is (seq (temporal-entries db :temporal-eavt :name "a"))
              "the superseded value is in history")
          (is (= #{["b"]} (d/q '[:find ?n :in $ ?e :where [?e :name ?n]] db e))
              "and the current value is the new one"))))))
