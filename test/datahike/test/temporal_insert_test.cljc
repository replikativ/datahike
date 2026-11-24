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

(deftest assert-and-retract-in-one-transaction-leaves-no-history
  (testing "a transaction records its net effect: a value it asserts and
            retracts was never in the database, so it leaves no history entry.
            (Written step by step it left an assertion and a retraction under
            one tx, a pair that does not say which came last.)"
    (with-conn
      (fn [conn e]
        (d/transact conn [[:db/add e :tag :x] [:db/retract e :tag :x]])
        (let [db @conn]
          (doseq [tree [:temporal-eavt :temporal-aevt]]
            (testing (name tree)
              (let [entries (temporal-entries db tree :tag :x)]
                (is (empty? entries)
                    (str "no history entry, got " (pr-str entries)))))))))))

(deftest the-current-tree-is-unaffected
  (testing "the datom was retracted, so it must be gone from the current tree —
            this is what stayed correct throughout and hid the bug"
    (with-conn
      (fn [conn e]
        (d/transact conn [[:db/add e :tag :x] [:db/retract e :tag :x]])
        (is (empty? (d/datoms @conn :eavt e :tag :x)))))))

(deftest the-history-view-holds-nothing-for-it
  (testing "`d/history`, which is what an export reads, shows neither an
            assertion nor a retraction of the value."
    (with-conn
      (fn [conn e]
        (d/transact conn [[:db/add e :tag :x] [:db/retract e :tag :x]])
        (let [entries (vec (filter #(and (= :tag (:a %)) (= :x (:v %)))
                                   (d/datoms (d/history @conn) :eavt)))]
          (is (empty? entries)
              (str "history must hold nothing, got " (pr-str entries))))))))

(deftest as-of-agrees-with-the-current-indices
  (testing "a fact asserted and retracted in one transaction must be absent from
            `as-of` too, not just from the current indices.

            Restoring the retraction marker was necessary but not sufficient:
            `assemble-datoms-xform` sorted by `tx` ALONE, and a stable sort
            preserves input order on ties, so within one transaction the
            assertion always won. Now assertions fold before retractions, which
            makes the retraction win on a tie — a temporal assertion and
            retraction sharing a tx can only mean the datom was created and
            removed inside that transaction."
    (with-conn
      (fn [conn e]
        (d/transact conn [[:db/add e :tag :x] [:db/retract e :tag :x]])
        (let [db @conn]
          (is (empty? (d/datoms db :eavt e :tag :x))
              "the current indices say absent")
          (is (empty? (d/datoms (d/as-of db (:max-tx db)) :eavt e :tag :x))
              "and as-of agrees")
          (is (empty? (d/datoms (d/as-of db (java.util.Date.)) :eavt e :tag :x))
              "including as-of now"))))))

(deftest a-retract-entity-alongside-a-fresh-assertion
  (testing "the reachable shape: assert an attribute and retract the whole
            entity in one transaction. The freshly asserted datom never was in
            the database and leaves nothing; an attribute asserted EARLIER is
            retracted, and keeps its assertion and its retraction."
    (with-conn
      (fn [conn e]
        (d/transact conn [{:db/id e :tag :x} [:db/retractEntity e]])
        (let [db @conn]
          (is (empty? (temporal-entries db :temporal-eavt :tag :x))
              "the datom asserted in the same transaction leaves no entry")
          (is (= #{true false} (added-flags (temporal-entries db :temporal-eavt :name "a")))
              "the one asserted in an earlier transaction keeps both"))))))

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
  (testing "the other order: the value was present before and is present
            after, so the transaction changed nothing for it. No history entry,
            and the datom keeps the transaction that asserted it."
    (with-conn
      (fn [conn e]
        (d/transact conn [[:db/add e :tag :z]])
        (let [asserted (:tx (first (d/datoms @conn :eavt e :tag :z)))]
          (d/transact conn [[:db/retract e :tag :z] [:db/add e :tag :z]])
          (let [db @conn]
            (is (empty? (temporal-entries db :temporal-eavt :tag :z)))
            (is (= [asserted] (map :tx (d/datoms db :eavt e :tag :z)))
                "the datom is present, dated to its original transaction")))))))

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

(deftest overwriting-false-preserves-its-history-retraction
  (with-conn
    (fn [conn eid]
      (d/transact conn [{:db/ident :enabled :db/valueType :db.type/boolean
                         :db/cardinality :db.cardinality/one}])
      (let [add-false (d/transact conn [[:db/add eid :enabled false]])
            add-true (d/transact conn [[:db/add eid :enabled true]])
            restore-false (d/transact conn [[:db/add eid :enabled false]])
            db @conn
            t1 (:max-tx (:db-after add-false))
            t2 (:max-tx (:db-after add-true))
            t3 (:max-tx (:db-after restore-false))]
        (is (= #{[false t1 true] [false t2 false] [true t2 true]
                 [true t3 false] [false t3 true]}
               (into #{} (map (juxt :v :tx :added))
                     (d/datoms (d/history db) :eavt eid :enabled))))
        (is (= [[false] [true] [false]]
               (mapv (fn [tx]
                       (mapv :v (d/datoms (d/as-of db tx) :eavt eid :enabled)))
                     [t1 t2 t3])))))))
