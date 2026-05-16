(ns datahike.test.valid-at-test
  "Tests for the `d/valid-at` filter wrapper. Verifies that pure-datalog
   queries (no vt-aware secondary index in the query path) correctly
   filter to entities whose asserting tx's valid-time window contains
   the given `at` instant.

   `d/valid-at` returns `(d/filter db vt-pred)` where the pred reads
   the tx-entity's `:db.valid/from` / `:db.valid/to` and admits a
   datom iff `vf <= at < vt`. The planner's regular-DB hot scan and
   the temporal merge-slice both call `maybe-post-process` so the
   FilteredDB's xform-after fires on every read path."
  (:require [clojure.test :as t :refer [is deftest testing]]
            [datahike.api :as d]))

(defn- fresh-conn []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write
             :keep-history? true}]
    (d/create-database cfg)
    (d/connect cfg)))

(defn- setup-vt-data!
  "Two employees, each with two vt-windowed salary updates.
   Bob: 100k (2024-Q1-Q2) → 110k (2024-Q3 onwards, open).
   Alice: 80k (2024-Q1 onwards, open)."
  [conn]
  (d/transact conn [{:db/ident :emp/name
                     :db/valueType :db.type/string
                     :db/cardinality :db.cardinality/one
                     :db/unique :db.unique/identity}
                    {:db/ident :emp/salary
                     :db/valueType :db.type/long
                     :db/cardinality :db.cardinality/one}])
  (d/transact conn [{:db/id "datomic.tx"
                     :db.valid/from #inst "2024-01-01"
                     :db.valid/to   #inst "2024-07-01"}
                    {:emp/name "Bob" :emp/salary 100000}])
  (d/transact conn [{:db/id "datomic.tx"
                     :db.valid/from #inst "2024-07-01"}
                    {:emp/name "Bob" :emp/salary 110000}])
  (d/transact conn [{:db/id "datomic.tx"
                     :db.valid/from #inst "2024-01-01"}
                    {:emp/name "Alice" :emp/salary 80000}]))

(deftest valid-at-filters-pure-datalog-query
  ;; History queries combined with valid-at use the 5-tuple
  ;; `[e a v t true]` pattern to filter to additions only — otherwise
  ;; both the addition AND the retraction datom match the
  ;; 3-tuple `[?e :emp/salary ?s]` shape (the retraction's value field
  ;; is the OLD value being retracted), and the per-datom vt-pred can
  ;; admit a retraction whose retracting-tx vt-window contains `at`.
  ;; This 5-tuple discipline is the standard datalog history pattern.
  (let [conn (fresh-conn)]
    (setup-vt-data! conn)
    (let [hist-db (d/history (d/db conn))]
      (testing "history view, additions only — all 3 asserted salary rows"
        (is (= #{[100000] [110000] [80000]}
               (d/q '[:find ?s :where [?e :emp/salary ?s ?tx true]] hist-db))))
      (testing "valid-at mid-Q2 → Bob's 100k row + Alice's 80k"
        (is (= #{[100000] [80000]}
               (d/q '[:find ?s :where [?e :emp/salary ?s ?tx true]]
                    (d/valid-at hist-db #inst "2024-04-15")))))
      (testing "valid-at mid-Q3 → Bob's 110k + Alice's 80k"
        (is (= #{[110000] [80000]}
               (d/q '[:find ?s :where [?e :emp/salary ?s ?tx true]]
                    (d/valid-at hist-db #inst "2024-08-15")))))
      (testing "valid-at before-data returns empty"
        (is (= #{}
               (d/q '[:find ?s :where [?e :emp/salary ?s ?tx true]]
                    (d/valid-at hist-db #inst "2020-01-01"))))))))

(deftest valid-at-nil-clears-marker
  (let [conn (fresh-conn)
        db (d/db conn)
        marked (d/valid-at db #inst "2024-04-15")
        cleared (d/valid-at marked nil)]
    (testing "marked db has the meta"
      (is (= #inst "2024-04-15" (:datahike/valid-at (meta marked)))))
    (testing "nil clears the meta marker"
      (is (nil? (:datahike/valid-at (meta cleared)))))))

(deftest valid-at-composes-with-as-of
  ;; Tx1 (vt 2024-Q1) created Bob with salary 100k.
  ;; Tx2 (vt 2024-Q3) updated Bob's salary to 110k.
  ;; Tx3 (vt 2024-Q1) created Alice with salary 80k.
  ;;
  ;; as-of at tx2-instant => sees tx1+tx2 (not tx3): so Bob 100k + 110k.
  ;; valid-at "mid-Q2" on top: vt-window for Bob's 100k is Q1..Q3 — contains mid-Q2 → match.
  ;; Bob's 110k vt-window is Q3..MAX — doesn't contain mid-Q2 → no match.
  ;; Result: just 100000.
  (let [conn (fresh-conn)
        _ (setup-vt-data! conn)
        tx2-instant (d/q '[:find ?t .
                           :where [?tx :db.valid/from #inst "2024-07-01"]
                           [?tx :db/txInstant ?t]]
                         (d/history (d/db conn)))
        composed (-> (d/db conn)
                     (d/history)
                     (d/as-of tx2-instant)
                     (d/valid-at #inst "2024-04-15"))]
    (testing "as-of tx2 then valid-at mid-Q2 → only Bob's 100k row"
      (is (= #{[100000]}
             (d/q '[:find ?s :where [?e :emp/salary ?s ?tx true]] composed))))))

(deftest valid-at-non-vt-tx-passes-through
  ;; A tx with no :db.valid/from is treated as "always valid" — the
  ;; vt-pred admits any datom whose asserting tx has no vt-window.
  ;; This keeps non-vt data working under d/valid-at.
  (let [conn (fresh-conn)]
    (d/transact conn [{:db/ident :foo/x
                       :db/valueType :db.type/long
                       :db/cardinality :db.cardinality/one}])
    (d/transact conn [{:foo/x 42}])
    (let [db (d/valid-at (d/db conn) #inst "2024-04-15")]
      (testing "datom from non-vt tx survives the filter"
        (is (= 42 (d/q '[:find ?x . :where [_ :foo/x ?x]] db)))))))
