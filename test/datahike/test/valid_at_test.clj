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

;; ============================================================================
;; vf < vt validation at the transactor (DH-1)
;; ============================================================================

(deftest transact-rejects-reverse-valid-window
  (testing "Tx with :db.valid/from > :db.valid/to throws at transact"
    (let [conn (fresh-conn)]
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo #"Invalid valid-time window"
            (d/transact conn
                        {:tx-meta {:db.valid/from #inst "2024-07-01"
                                   :db.valid/to   #inst "2024-01-01"}
                         :tx-data [{:db/ident :foo
                                    :db/valueType :db.type/long
                                    :db/cardinality :db.cardinality/one}]}))))))

(deftest transact-rejects-zero-width-valid-window
  (testing "Tx with :db.valid/from == :db.valid/to throws at transact"
    (let [conn (fresh-conn)]
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo #"Invalid valid-time window"
            (d/transact conn
                        {:tx-meta {:db.valid/from #inst "2024-04-01"
                                   :db.valid/to   #inst "2024-04-01"}
                         :tx-data [{:db/ident :foo
                                    :db/valueType :db.type/long
                                    :db/cardinality :db.cardinality/one}]}))))))

(deftest transact-accepts-open-ended-valid-window
  (testing "Tx with :db.valid/from but no :db.valid/to succeeds"
    (let [conn (fresh-conn)]
      (is (some? (d/transact conn
                              {:tx-meta {:db.valid/from #inst "2024-01-01"}
                               :tx-data [{:db/ident :foo
                                          :db/valueType :db.type/long
                                          :db/cardinality :db.cardinality/one}]}))))))

;; ============================================================================
;; valid-between / valid-during / valid-all wrappers (DH-2)
;; ============================================================================

(deftest valid-between-filters-overlapping-tx-windows
  ;; Setup: Bob 100k [Jan-Jul), 110k [Jul-MAX); Alice 80k [Jan-MAX).
  ;; Query: valid-between [Apr, Sep) should keep all three (Bob's two
  ;; windows both overlap [Apr, Sep): [Jan, Jul) overlaps on [Apr, Jul);
  ;; [Jul, MAX) overlaps on [Jul, Sep). Alice's [Jan, MAX) overlaps too.
  (let [conn (fresh-conn)]
    (setup-vt-data! conn)
    (let [hist (d/history (d/db conn))
          db (d/valid-between hist #inst "2024-04-01" #inst "2024-09-01")]
      (testing "all three windows overlap [Apr, Sep)"
        (is (= #{[100000] [110000] [80000]}
               (d/q '[:find ?s :where [?e :emp/salary ?s ?tx true]] db))))
      (testing "marker is set"
        (is (= [#inst "2024-04-01" #inst "2024-09-01"]
               (:datahike/valid-between (meta db))))))))

(deftest valid-between-narrow-window-excludes-non-overlapping
  ;; Bob 100k [Jan-Jul), 110k [Jul-MAX). Query [Aug, Oct) overlaps only
  ;; the 110k window. Alice's [Jan-MAX) also overlaps.
  (let [conn (fresh-conn)]
    (setup-vt-data! conn)
    (let [hist (d/history (d/db conn))
          db (d/valid-between hist #inst "2024-08-01" #inst "2024-10-01")]
      (is (= #{[110000] [80000]}
             (d/q '[:find ?s :where [?e :emp/salary ?s ?tx true]] db))))))

(deftest valid-during-strict-containment
  ;; Bob 100k [Jan, Jul) — fully contained in [Jan, Sep). vt-to=Jul <= Sep.
  ;; Bob 110k [Jul, MAX) — vt-to=MAX is open, NOT contained in any bounded window.
  ;; Alice 80k [Jan, MAX) — same, not contained.
  ;; Query valid-during [Jan, Sep): only Bob's 100k row passes.
  (let [conn (fresh-conn)]
    (setup-vt-data! conn)
    (let [hist (d/history (d/db conn))
          db (d/valid-during hist #inst "2024-01-01" #inst "2024-09-01")]
      (is (= #{[100000]}
             (d/q '[:find ?s :where [?e :emp/salary ?s ?tx true]] db))))))

(deftest valid-all-clears-markers
  (let [conn (fresh-conn)
        ;; Chaining valid-at then valid-between nests FilteredDBs;
        ;; the outer wrapper's meta doesn't carry the inner's marker.
        ;; Stack both markers on a single FilteredDB via vary-meta
        ;; to exercise the multi-marker case.
        marked (-> (d/valid-at (d/db conn) #inst "2024-04-15")
                   (vary-meta assoc :datahike/valid-between
                              [#inst "2024-01-01" #inst "2024-07-01"]))]
    (testing "both markers present"
      (is (some? (:datahike/valid-at (meta marked))))
      (is (some? (:datahike/valid-between (meta marked)))))
    (let [cleared (d/valid-all marked)]
      (testing "valid-all strips both markers"
        (is (nil? (:datahike/valid-at (meta cleared))))
        (is (nil? (:datahike/valid-between (meta cleared))))))))

(deftest valid-between-nil-endpoint-clears-marker
  (let [conn (fresh-conn)
        marked (d/valid-between (d/db conn) #inst "2024-01-01" #inst "2024-07-01")
        cleared (d/valid-between marked nil #inst "2024-07-01")]
    (testing "nil endpoint clears the between marker"
      (is (nil? (:datahike/valid-between (meta cleared)))))))

;; ============================================================================
;; Allen interval predicates as built-in datalog rules (DH-3)
;;
;; 4-arg interval-* rules take (a-from, a-to, b-from, b-to). Generic
;; over any orderable type — works for the bitemporal axis, but also
;; for application-domain date ranges (lease vs contract, etc.).
;; ============================================================================

(defn- intervals-conn []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write
             :keep-history? false}]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn [{:db/ident :iv/name
                         :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one}
                        {:db/ident :iv/from
                         :db/valueType :db.type/instant
                         :db/cardinality :db.cardinality/one}
                        {:db/ident :iv/to
                         :db/valueType :db.type/instant
                         :db/cardinality :db.cardinality/one}])
      ;; Two intervals: A=[Jan, Jul), B=[Apr, Sep)
      (d/transact conn [{:iv/name "A" :iv/from #inst "2024-01-01" :iv/to #inst "2024-07-01"}
                        {:iv/name "B" :iv/from #inst "2024-04-01" :iv/to #inst "2024-09-01"}])
      conn)))

(deftest interval-overlaps?-detects-overlap
  (let [conn (intervals-conn)]
    (testing "A and B overlap"
      (is (= #{["A" "B"]}
             (d/q '[:find ?an ?bn
                    :where
                    [?a :iv/name ?an] [?a :iv/from ?af] [?a :iv/to ?at]
                    [?b :iv/name ?bn] [?b :iv/from ?bf] [?b :iv/to ?bt]
                    [(not= ?a ?b)]
                    [(< ?an ?bn)]  ;; one direction only
                    (interval-overlaps? ?af ?at ?bf ?bt)]
                  (d/db conn)))))))

(deftest interval-contains?-checks-containment
  ;; Setup: A=[Jan, Sep) contains B=[Apr, Jul) but not C=[Jun, Oct).
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write :keep-history? false}
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (d/transact conn [{:db/ident :iv/name :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one}
                      {:db/ident :iv/from :db/valueType :db.type/instant
                       :db/cardinality :db.cardinality/one}
                      {:db/ident :iv/to :db/valueType :db.type/instant
                       :db/cardinality :db.cardinality/one}])
    (d/transact conn [{:iv/name "A" :iv/from #inst "2024-01-01" :iv/to #inst "2024-09-01"}
                      {:iv/name "B" :iv/from #inst "2024-04-01" :iv/to #inst "2024-07-01"}
                      {:iv/name "C" :iv/from #inst "2024-06-01" :iv/to #inst "2024-10-01"}])
    (testing "A contains B but not C"
      (is (= #{["A" "B"]}
             (d/q '[:find ?an ?bn
                    :where
                    [?a :iv/name ?an] [?a :iv/from ?af] [?a :iv/to ?at]
                    [?b :iv/name ?bn] [?b :iv/from ?bf] [?b :iv/to ?bt]
                    [(not= ?a ?b)]
                    (interval-contains? ?af ?at ?bf ?bt)]
                  (d/db conn)))))))

(deftest interval-precedes?-touching-vs-strict
  ;; A=[Jan, Apr), B=[Apr, Jul). A.to == B.from — touching.
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write :keep-history? false}
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (d/transact conn [{:db/ident :iv/name :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one}
                      {:db/ident :iv/from :db/valueType :db.type/instant
                       :db/cardinality :db.cardinality/one}
                      {:db/ident :iv/to :db/valueType :db.type/instant
                       :db/cardinality :db.cardinality/one}])
    (d/transact conn [{:iv/name "A" :iv/from #inst "2024-01-01" :iv/to #inst "2024-04-01"}
                      {:iv/name "B" :iv/from #inst "2024-04-01" :iv/to #inst "2024-07-01"}])
    (testing "A precedes B (touching counts)"
      (is (= #{["A" "B"]}
             (d/q '[:find ?an ?bn
                    :where
                    [?a :iv/name ?an] [?a :iv/from ?af] [?a :iv/to ?at]
                    [?b :iv/name ?bn] [?b :iv/from ?bf] [?b :iv/to ?bt]
                    [(not= ?a ?b)] [(< ?an ?bn)]
                    (interval-precedes? ?af ?at ?bf ?bt)]
                  (d/db conn)))))
    (testing "A strictly-precedes B fails on touching"
      (is (empty?
            (d/q '[:find ?an ?bn
                   :where
                   [?a :iv/name ?an] [?a :iv/from ?af] [?a :iv/to ?at]
                   [?b :iv/name ?bn] [?b :iv/from ?bf] [?b :iv/to ?bt]
                   [(not= ?a ?b)] [(< ?an ?bn)]
                   (interval-strictly-precedes? ?af ?at ?bf ?bt)]
                 (d/db conn)))))
    (testing "A meets B (alias for immediately-precedes)"
      (is (= #{["A" "B"]}
             (d/q '[:find ?an ?bn
                    :where
                    [?a :iv/name ?an] [?a :iv/from ?af] [?a :iv/to ?at]
                    [?b :iv/name ?bn] [?b :iv/from ?bf] [?b :iv/to ?bt]
                    [(not= ?a ?b)] [(< ?an ?bn)]
                    (interval-meets? ?af ?at ?bf ?bt)]
                  (d/db conn)))))))
