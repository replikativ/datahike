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
  (d/transact conn {:tx-data [{:emp/name "Bob" :emp/salary 100000}]
                    :tx-meta {:db.valid/from #inst "2024-01-01"
                              :db.valid/to   #inst "2024-07-01"}})
  (d/transact conn {:tx-data [{:emp/name "Bob" :emp/salary 110000}]
                    :tx-meta {:db.valid/from #inst "2024-07-01"}})
  (d/transact conn {:tx-data [{:emp/name "Alice" :emp/salary 80000}]
                    :tx-meta {:db.valid/from #inst "2024-01-01"}}))

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

(deftest as-of-rejects-vt-marked-input
  ;; Wrapping order matters: d/as-of must be wrapped FIRST, then
  ;; d/valid-at outermost — otherwise the supersession check inside
  ;; valid-at's predicate captures an unbounded db and reads future
  ;; txes. d/as-of throws if it sees a vt-marked db to surface the
  ;; mistake at call-site rather than silently returning wrong
  ;; answers.
  (let [conn (fresh-conn)
        _ (setup-vt-data! conn)
        vt-marked (d/valid-at (d/db conn) #inst "2024-04-15")]
    (testing "wrong order throws :temporal/wrap-order"
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Cannot wrap d/as-of around a db already filtered by d/valid-at"
           (d/as-of vt-marked #inst "2024-08-01"))))
    (testing "correct order works"
      (is (some? (-> (d/db conn)
                     (d/as-of #inst "2024-08-01")
                     (d/valid-at #inst "2024-04-15")))))))

(deftest valid-at-composes-with-as-of
  ;; Tx1 (vt 2024-Q1) created Bob with salary 100k.
  ;; Tx2 (vt 2024-Q3) updated Bob's salary to 110k.
  ;; Tx3 (vt 2024-Q1) created Alice with salary 80k.
  ;;
  ;; as-of at tx2-id => sees tx1+tx2 (not tx3): so Bob 100k + 110k.
  ;; valid-at "mid-Q2" on top: vt-window for Bob's 100k is Q1..Q3 — contains mid-Q2 → match.
  ;; Bob's 110k vt-window is Q3..MAX — doesn't contain mid-Q2 → no match.
  ;; Result: just 100000.
  ;;
  ;; Cuts by tx-id rather than tx-instant: tx-ids are strictly
  ;; monotonic by definition, while :db/txInstant could tie when
  ;; multiple writes land in the same wall-clock ms. The Datomic
  ;; idiom for "exact tx as the cut point" is `as-of <tx-id>`.
  ;; (Today next-tx-instant also enforces strict monotonicity on
  ;; auto-stamped :db/txInstant, but tx-id is the correct primitive
  ;; for an unambiguous snapshot cut.)
  (let [conn (fresh-conn)
        _ (setup-vt-data! conn)
        tx2-id (d/q '[:find ?tx .
                      :where [?tx :db.valid/from #inst "2024-07-01"]]
                    (d/history (d/db conn)))
        composed (-> (d/db conn)
                     (d/history)
                     (d/as-of tx2-id)
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
;; Allen interval predicates as built-in datalog rules (DH-3 + note 179 Gap 2)
;;
;; 4-arg interval-* rules take (a-from, a-to, b-from, b-to). Generic
;; over any orderable type — works for the bitemporal axis, but also
;; for application-domain date ranges (lease vs contract, etc.).
;;
;; The canonical 13 Allen relations (Allen 1983) are: equals,
;; before/after, meets/met-by, overlaps/overlapped-by, during/contains,
;; starts/started-by, finishes/finished-by. The library implements 11
;; names + 1 alias (meets? → immediately-precedes?). The library splits
;; "before" into precedes? (touching counts) / strictly-precedes? (no
;; touch) and "after" into succeeds? / strictly-succeeds?, which is
;; more granular than canonical Allen. Each named relation gets its own
;; focused test below — each interval setup is a hand-computed oracle.
;; ============================================================================

(defn- install-interval-schema! [conn]
  (d/transact conn [{:db/ident :iv/name :db/valueType :db.type/string
                     :db/cardinality :db.cardinality/one}
                    {:db/ident :iv/from :db/valueType :db.type/instant
                     :db/cardinality :db.cardinality/one}
                    {:db/ident :iv/to :db/valueType :db.type/instant
                     :db/cardinality :db.cardinality/one}]))

(defn- intervals-conn
  "Fresh in-memory conn with `[:iv/name :iv/from :iv/to]` schema and the
   given intervals already transacted. Each map in `intervals` has shape
   `{:name <s> :from <inst> :to <inst>}`."
  [intervals]
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write
             :keep-history? false}]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (install-interval-schema! conn)
      (d/transact conn (mapv (fn [{:keys [name from to]}]
                               {:iv/name name :iv/from from :iv/to to})
                             intervals))
      conn)))

(defmacro ^:private fires-on
  "Returns the set of `[?an ?bn]` pairs from `conn` where the named
   Allen rule body fires. The rule is given as a literal sexp like
   `(interval-starts? ?af ?at ?bf ?bt)` — the macro splices it into a
   standard 2-interval datalog query. Per-relation tests use this to
   keep the assertion line readable while reusing the shared pattern."
  [conn rule-call]
  `(d/q '~(into '[:find ?an ?bn :where
                  [?a :iv/name ?an] [?a :iv/from ?af] [?a :iv/to ?at]
                  [?b :iv/name ?bn] [?b :iv/from ?bf] [?b :iv/to ?bt]
                  [(not= ?a ?b)]]
                [rule-call])
        (d/db ~conn)))

;; --- Symmetric: equals --------------------------------------------------

(deftest interval-equals?-only-self-shaped-pair-fires
  ;; A=[Jan,Jul), B=[Jan,Jul) — identical windows; C=[Jan,Aug) — same
  ;; from but different to. equals? must pair (A,B) and (B,A); never
  ;; (A,C) / (C,A).
  (let [conn (intervals-conn
              [{:name "A" :from #inst "2024-01-01" :to #inst "2024-07-01"}
               {:name "B" :from #inst "2024-01-01" :to #inst "2024-07-01"}
               {:name "C" :from #inst "2024-01-01" :to #inst "2024-08-01"}])]
    (is (= #{["A" "B"] ["B" "A"]}
           (fires-on conn (interval-equals? ?af ?at ?bf ?bt)))
        "equal-shaped windows fire symmetrically; differing-to ones don't")))

;; --- overlaps? --------------------------------------------------------------

(deftest interval-overlaps?-detects-partial-overlap
  ;; A=[Jan,Jul), B=[Apr,Sep). A.from < B.from < A.to < B.to → overlap.
  ;; The rule is symmetric (both [(< ?af ?bt)] and [(< ?bf ?at)] hold
  ;; under swap), so both (A,B) and (B,A) fire.
  (let [conn (intervals-conn
              [{:name "A" :from #inst "2024-01-01" :to #inst "2024-07-01"}
               {:name "B" :from #inst "2024-04-01" :to #inst "2024-09-01"}])]
    (is (= #{["A" "B"] ["B" "A"]}
           (fires-on conn (interval-overlaps? ?af ?at ?bf ?bt)))
        "partially-overlapping windows fire in both orientations")))

(deftest interval-overlaps?-fails-on-disjoint
  ;; A=[Jan,Apr), B=[Jul,Sep). A.to <= B.from → no overlap, even with
  ;; the symmetric rule body.
  (let [conn (intervals-conn
              [{:name "A" :from #inst "2024-01-01" :to #inst "2024-04-01"}
               {:name "B" :from #inst "2024-07-01" :to #inst "2024-09-01"}])]
    (is (empty? (fires-on conn (interval-overlaps? ?af ?at ?bf ?bt)))
        "disjoint windows never overlap")))

;; --- contains? / strictly-contains? -----------------------------------------

(deftest interval-contains?-allows-shared-boundaries
  ;; A=[Jan,Sep) contains B=[Apr,Jul). C=[Jun,Oct) is NOT contained by A
  ;; (C.to > A.to). A also contains itself trivially — the [(not= ?a ?b)]
  ;; guard suppresses that, leaving only (A,B).
  (let [conn (intervals-conn
              [{:name "A" :from #inst "2024-01-01" :to #inst "2024-09-01"}
               {:name "B" :from #inst "2024-04-01" :to #inst "2024-07-01"}
               {:name "C" :from #inst "2024-06-01" :to #inst "2024-10-01"}])]
    (is (= #{["A" "B"]}
           (fires-on conn (interval-contains? ?af ?at ?bf ?bt)))
        "non-strict contains: A contains B but not C")))

(deftest interval-strictly-contains?-rejects-shared-boundaries
  ;; A=[Jan,Sep), B=[Jan,Jul) — B shares A's from, so A does NOT strictly
  ;; contain B (the rule demands A.from < B.from AND A.to > B.to).
  ;; A=[Jan,Sep) DOES strictly contain D=[Apr,Jul).
  (let [conn (intervals-conn
              [{:name "A" :from #inst "2024-01-01" :to #inst "2024-09-01"}
               {:name "B" :from #inst "2024-01-01" :to #inst "2024-07-01"}
               {:name "D" :from #inst "2024-04-01" :to #inst "2024-07-01"}])]
    (is (= #{["A" "D"]}
           (fires-on conn (interval-strictly-contains? ?af ?at ?bf ?bt)))
        "strict contains: only A wholly inside, no shared boundary")))

;; --- precedes? / strictly-precedes? -----------------------------------------

(deftest interval-precedes?-touching-fires
  ;; A=[Jan,Apr), B=[Apr,Jul) — A.to == B.from. Touching counts for
  ;; precedes? (rule body is [(<= ?at ?bf)]).
  (let [conn (intervals-conn
              [{:name "A" :from #inst "2024-01-01" :to #inst "2024-04-01"}
               {:name "B" :from #inst "2024-04-01" :to #inst "2024-07-01"}])]
    (is (= #{["A" "B"]}
           (fires-on conn (interval-precedes? ?af ?at ?bf ?bt)))
        "A precedes B with touching boundary; the reverse does not")))

(deftest interval-strictly-precedes?-rejects-touching
  ;; A=[Jan,Apr), B=[Apr,Jul) — A.to == B.from. strictly-precedes?
  ;; demands [(< ?at ?bf)] so touching is rejected.
  (let [conn (intervals-conn
              [{:name "A" :from #inst "2024-01-01" :to #inst "2024-04-01"}
               {:name "B" :from #inst "2024-04-01" :to #inst "2024-07-01"}])]
    (is (empty?
         (fires-on conn (interval-strictly-precedes? ?af ?at ?bf ?bt)))
        "touching is the canonical edge case strict ordering rejects"))
  ;; But with a real gap (A=[Jan,Mar), C=[May,Jul)) strict-precedes
  ;; fires.
  (let [conn (intervals-conn
              [{:name "A" :from #inst "2024-01-01" :to #inst "2024-03-01"}
               {:name "C" :from #inst "2024-05-01" :to #inst "2024-07-01"}])]
    (is (= #{["A" "C"]}
           (fires-on conn (interval-strictly-precedes? ?af ?at ?bf ?bt)))
        "with a real gap, strict-precedes fires forward only")))

;; --- immediately-precedes? = meets? -----------------------------------------

(deftest interval-immediately-precedes?-fires-only-on-touch
  ;; meets? is an alias — same rule, both must fire on the same data.
  ;; Touching case (A.to == B.from): both fire (A,B). Gapped case
  ;; (A=[Jan,Mar), C=[May,Jul)): neither fires.
  (let [touching (intervals-conn
                  [{:name "A" :from #inst "2024-01-01" :to #inst "2024-04-01"}
                   {:name "B" :from #inst "2024-04-01" :to #inst "2024-07-01"}])
        gapped (intervals-conn
                [{:name "A" :from #inst "2024-01-01" :to #inst "2024-03-01"}
                 {:name "C" :from #inst "2024-05-01" :to #inst "2024-07-01"}])]
    (is (= #{["A" "B"]}
           (fires-on touching (interval-immediately-precedes? ?af ?at ?bf ?bt)))
        "immediately-precedes: A.to == B.from")
    (is (= #{["A" "B"]}
           (fires-on touching (interval-meets? ?af ?at ?bf ?bt)))
        "meets? is the canonical alias — same fact pattern fires")
    (is (empty? (fires-on gapped (interval-immediately-precedes? ?af ?at ?bf ?bt)))
        "a real gap → no immediate-precede; meets? agrees")
    (is (empty? (fires-on gapped (interval-meets? ?af ?at ?bf ?bt))))))

;; --- succeeds? / strictly-succeeds? / immediately-succeeds? ----------------

(deftest interval-succeeds?-touching-fires
  ;; A=[Apr,Jul) succeeds B=[Jan,Apr) (A.from == B.to → touching counts).
  ;; Symmetric to precedes?, opposite direction.
  (let [conn (intervals-conn
              [{:name "A" :from #inst "2024-04-01" :to #inst "2024-07-01"}
               {:name "B" :from #inst "2024-01-01" :to #inst "2024-04-01"}])]
    (is (= #{["A" "B"]}
           (fires-on conn (interval-succeeds? ?af ?at ?bf ?bt)))
        "A succeeds B with touching boundary")))

(deftest interval-strictly-succeeds?-rejects-touching
  ;; Touching: A.from == B.to → strict succeed rejects (needs >, not >=).
  (let [conn (intervals-conn
              [{:name "A" :from #inst "2024-04-01" :to #inst "2024-07-01"}
               {:name "B" :from #inst "2024-01-01" :to #inst "2024-04-01"}])]
    (is (empty?
         (fires-on conn (interval-strictly-succeeds? ?af ?at ?bf ?bt)))
        "touching boundary is rejected by strict-succeeds?"))
  (let [conn (intervals-conn
              [{:name "A" :from #inst "2024-05-01" :to #inst "2024-07-01"}
               {:name "B" :from #inst "2024-01-01" :to #inst "2024-03-01"}])]
    (is (= #{["A" "B"]}
           (fires-on conn (interval-strictly-succeeds? ?af ?at ?bf ?bt)))
        "with a real gap, strict-succeeds fires")))

(deftest interval-immediately-succeeds?-fires-only-on-touch
  ;; A=[Apr,Jul), B=[Jan,Apr). A.from == B.to — fires.
  ;; A=[May,Jul), C=[Jan,Mar) — gap. Doesn't fire.
  (let [touching (intervals-conn
                  [{:name "A" :from #inst "2024-04-01" :to #inst "2024-07-01"}
                   {:name "B" :from #inst "2024-01-01" :to #inst "2024-04-01"}])
        gapped (intervals-conn
                [{:name "A" :from #inst "2024-05-01" :to #inst "2024-07-01"}
                 {:name "C" :from #inst "2024-01-01" :to #inst "2024-03-01"}])]
    (is (= #{["A" "B"]}
           (fires-on touching (interval-immediately-succeeds? ?af ?at ?bf ?bt)))
        "immediately-succeeds: A.from == B.to")
    (is (empty?
         (fires-on gapped (interval-immediately-succeeds? ?af ?at ?bf ?bt)))
        "a real gap → no immediate-succeed")))

;; --- starts? / started-by? (note 179 Gap 2 — newly added) -------------------

(deftest interval-starts?-shared-from-shorter-to
  ;; A=[Jan,Apr), B=[Jan,Jul) — same from, A.to < B.to. A "starts" B in
  ;; Allen's sense: they begin together and A finishes first.
  ;; Reverse direction (B starts A) does NOT fire — B.to > A.to.
  (let [conn (intervals-conn
              [{:name "A" :from #inst "2024-01-01" :to #inst "2024-04-01"}
               {:name "B" :from #inst "2024-01-01" :to #inst "2024-07-01"}])]
    (is (= #{["A" "B"]}
           (fires-on conn (interval-starts? ?af ?at ?bf ?bt)))
        "A starts B: shared from, A.to strictly less than B.to")))

(deftest interval-starts?-rejects-equal-windows
  ;; A=[Jan,Apr), B=[Jan,Apr) — identical. starts? demands STRICT
  ;; A.to < B.to, so identical windows give equals?, not starts?.
  (let [conn (intervals-conn
              [{:name "A" :from #inst "2024-01-01" :to #inst "2024-04-01"}
               {:name "B" :from #inst "2024-01-01" :to #inst "2024-04-01"}])]
    (is (empty? (fires-on conn (interval-starts? ?af ?at ?bf ?bt)))
        "identical windows are equals?, not starts?")
    (is (= #{["A" "B"] ["B" "A"]}
           (fires-on conn (interval-equals? ?af ?at ?bf ?bt)))
        "and equals? confirms the relation is the right one")))

(deftest interval-started-by?-is-the-inverse-of-starts?
  ;; A=[Jan,Jul), B=[Jan,Apr). A.from == B.from, A.to > B.to → A
  ;; "started-by" B (B starts A in the Allen sense, viewed from A).
  (let [conn (intervals-conn
              [{:name "A" :from #inst "2024-01-01" :to #inst "2024-07-01"}
               {:name "B" :from #inst "2024-01-01" :to #inst "2024-04-01"}])]
    (is (= #{["A" "B"]}
           (fires-on conn (interval-started-by? ?af ?at ?bf ?bt)))
        "A started-by B: shared from, A.to strictly greater than B.to")
    ;; Cross-check: starts? fires in the opposite direction on the same
    ;; data. The pair (B,A) for starts? must equal the pair (A,B) for
    ;; started-by? — that's the inversion contract.
    (is (= #{["B" "A"]}
           (fires-on conn (interval-starts? ?af ?at ?bf ?bt)))
        "starts? is the inverse — fires on the swapped pair")))

;; --- finishes? / finished-by? (note 179 Gap 2 — newly added) ----------------

(deftest interval-finishes?-shared-to-later-from
  ;; A=[Apr,Jul), B=[Jan,Jul) — shared to, A.from > B.from. A "finishes"
  ;; B in Allen's sense: they end together but A starts later.
  (let [conn (intervals-conn
              [{:name "A" :from #inst "2024-04-01" :to #inst "2024-07-01"}
               {:name "B" :from #inst "2024-01-01" :to #inst "2024-07-01"}])]
    (is (= #{["A" "B"]}
           (fires-on conn (interval-finishes? ?af ?at ?bf ?bt)))
        "A finishes B: shared to, A.from strictly greater than B.from")))

(deftest interval-finishes?-rejects-equal-windows
  ;; Identical windows go to equals?, not finishes?.
  (let [conn (intervals-conn
              [{:name "A" :from #inst "2024-04-01" :to #inst "2024-07-01"}
               {:name "B" :from #inst "2024-04-01" :to #inst "2024-07-01"}])]
    (is (empty? (fires-on conn (interval-finishes? ?af ?at ?bf ?bt)))
        "identical windows are equals?, not finishes?")))

(deftest interval-finished-by?-is-the-inverse-of-finishes?
  ;; A=[Jan,Jul), B=[Apr,Jul). Shared to, A.from < B.from → A
  ;; finished-by B.
  (let [conn (intervals-conn
              [{:name "A" :from #inst "2024-01-01" :to #inst "2024-07-01"}
               {:name "B" :from #inst "2024-04-01" :to #inst "2024-07-01"}])]
    (is (= #{["A" "B"]}
           (fires-on conn (interval-finished-by? ?af ?at ?bf ?bt)))
        "A finished-by B: shared to, A.from strictly less than B.from")
    (is (= #{["B" "A"]}
           (fires-on conn (interval-finishes? ?af ?at ?bf ?bt)))
        "finishes? is the inverse — fires on the swapped pair")))

;; ============================================================================
;; Half-open boundary semantics on valid-at (note 179 §4 Gap 2)
;;
;; The substrate's valid-at semantics is **half-open `[vf, vt)`** —
;; documented at `doc/valid_time.md:75`, encoded in two places:
;;   * the supersession predicate in `api/impl.cljc:179-198`
;;     (`tx-covers-at?`): `(not (.after vf at)) AND (vt.after at)`
;;     — i.e. `vf <= at < vt`.
;;   * the auto-injected `valid-at` rule at `query.cljc:640-641`:
;;     `[(<= ?vf ?at)] [(> ?vt ?at)]`.
;;
;; A query at `t == vf` MUST match; a query at `t == vt` MUST NOT.
;; Without these boundary tests, a future refactor that flips the `<=`
;; to `<` (or the `>` to `>=`) — say while porting to a different time
;; type — would silently regress: every existing test queries strictly
;; inside or strictly outside the window.
;; ============================================================================

(defn- vt-bounded-conn
  "Tx a single tx with vt window [from, to). Returns the conn."
  [from to]
  (let [conn (fresh-conn)]
    (d/transact conn [{:db/ident :emp/name :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one}
                      {:db/ident :emp/salary :db/valueType :db.type/long
                       :db/cardinality :db.cardinality/one}])
    (d/transact conn {:tx-data [{:emp/name "Bob" :emp/salary 100000}]
                      :tx-meta {:db.valid/from from :db.valid/to to}})
    conn))

(defn- vt-open-ended-conn
  "Tx a single tx with vt window [from, +∞). Returns the conn."
  [from]
  (let [conn (fresh-conn)]
    (d/transact conn [{:db/ident :emp/name :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one}
                      {:db/ident :emp/salary :db/valueType :db.type/long
                       :db/cardinality :db.cardinality/one}])
    (d/transact conn {:tx-data [{:emp/name "Bob" :emp/salary 100000}]
                      :tx-meta {:db.valid/from from}})
    conn))

(defn- salaries-at [db at]
  (d/q '[:find ?s :where [_ :emp/salary ?s]] (d/valid-at db at)))

(deftest valid-at-boundary-lower-vf-inclusive
  ;; Half-open `[vf, vt)` — `t == vf` is INSIDE the window.
  ;; Setup: window = [2024-01-01, 2024-07-01).
  (let [conn (vt-bounded-conn #inst "2024-01-01" #inst "2024-07-01")
        db (d/db conn)]
    (testing "at exactly vf (2024-01-01) the fact IS visible (inclusive lower)"
      (is (= #{[100000]} (salaries-at db #inst "2024-01-01"))
          "vf <= at must admit at == vf — the half-open [vf, vt) contract"))))

(deftest valid-at-boundary-just-before-vt-inclusive
  ;; t = vt - 1ms must STILL be inside — the upper bound is exclusive
  ;; but everything below it is in.
  (let [conn (vt-bounded-conn #inst "2024-01-01" #inst "2024-07-01")
        db (d/db conn)]
    (testing "at 2024-06-30T23:59:59.999 (vt minus 1 ms) the fact IS visible"
      (is (= #{[100000]} (salaries-at db #inst "2024-06-30T23:59:59.999"))
          "any instant strictly < vt must admit the fact"))))

(deftest valid-at-boundary-upper-vt-exclusive
  ;; The defining test: `t == vt` MUST NOT be inside. This is what
  ;; distinguishes half-open `[vf, vt)` from closed `[vf, vt]`.
  (let [conn (vt-bounded-conn #inst "2024-01-01" #inst "2024-07-01")
        db (d/db conn)]
    (testing "at exactly vt (2024-07-01) the fact is NOT visible (exclusive upper)"
      (is (= #{} (salaries-at db #inst "2024-07-01"))
          "vt > at must reject at == vt — the half-open contract"))))

(deftest valid-at-boundary-just-after-vt-exclusive
  ;; And of course `t > vt` is also out.
  (let [conn (vt-bounded-conn #inst "2024-01-01" #inst "2024-07-01")
        db (d/db conn)]
    (testing "at 2024-07-01T00:00:00.001 (vt plus 1 ms) the fact is NOT visible"
      (is (= #{} (salaries-at db #inst "2024-07-01T00:00:00.001"))
          "any instant > vt is strictly outside the window"))))

(deftest valid-at-boundary-just-before-vf-exclusive
  ;; Strictly-before-vf: the fact is NOT visible. Pairs with the
  ;; lower-inclusive test above — together they nail down the lower
  ;; boundary.
  (let [conn (vt-bounded-conn #inst "2024-01-01" #inst "2024-07-01")
        db (d/db conn)]
    (testing "at 2023-12-31T23:59:59.999 (vf minus 1 ms) the fact is NOT visible"
      (is (= #{} (salaries-at db #inst "2023-12-31T23:59:59.999"))
          "any instant strictly < vf is before the window"))))

(deftest valid-at-open-ended-far-future-visible
  ;; Open-ended `[vf, ∞)` — the substrate substitutes `+∞` for missing
  ;; `:db.valid/to`. A query at any instant after vf must match.
  ;; `query.cljc:639` uses `#inst "9999-12-31T23:59:59.999"` as the
  ;; sentinel; the impl.cljc predicate treats nil vt as no upper bound.
  (let [conn (vt-open-ended-conn #inst "2024-01-01")
        db (d/db conn)]
    (testing "at far future (2999-12-31) the fact IS visible for an open vt"
      (is (= #{[100000]} (salaries-at db #inst "2999-12-31"))
          "open-ended vt treats every future instant as inside"))))

(deftest valid-at-open-ended-before-vf-not-visible
  ;; Open-ended on the upper end doesn't help below the lower bound.
  (let [conn (vt-open-ended-conn #inst "2024-01-01")
        db (d/db conn)]
    (testing "at 2020-01-01 (before vf) the fact is NOT visible"
      (is (= #{} (salaries-at db #inst "2020-01-01"))
          "lower bound stays in effect even for open-ended windows"))))

(deftest valid-at-open-ended-at-vf-visible
  ;; Lower-bound inclusivity holds for open-ended windows too.
  (let [conn (vt-open-ended-conn #inst "2024-01-01")
        db (d/db conn)]
    (testing "at exactly vf (2024-01-01) the fact IS visible for an open vt"
      (is (= #{[100000]} (salaries-at db #inst "2024-01-01"))
          "vf inclusive boundary applies regardless of vt being open"))))

;; ============================================================================
;; Clock pinning for repeatable tests (DH-4)
;;
;; Per-call dynamic bindings (`(binding [tools/get-date ...] ...)`)
;; don't reach the writer thread, which runs transactions on a
;; background go-loop. Two patterns work across the thread hop:
;; (a) tx-meta `:db/txInstant` override (recommended), or
;; (b) `alter-var-root` on `get-date` (whole-suite fixture).
;; ============================================================================

(deftest tx-meta-txInstant-override-pins-time
  (testing "tx-meta :db/txInstant overrides the default :db/txInstant"
    (let [pinned-date #inst "2024-01-01T00:00:00.000-00:00"
          conn (fresh-conn)]
      (d/transact conn {:tx-meta {:db/txInstant pinned-date}
                        :tx-data [{:db/ident :foo/x
                                   :db/valueType :db.type/long
                                   :db/cardinality :db.cardinality/one}]})
      (let [tx-instant (d/q '[:find ?t .
                              :where [_ :db/ident :foo/x] [_ :db/txInstant ?t]]
                            (d/db conn))]
        (is (= pinned-date tx-instant)
            "tx-meta :db/txInstant should pin the tx-instant deterministically")))))

(deftest default-tx-instant-is-wall-clock
  (testing "Without tx-meta override, :db/txInstant is wall-clock now"
    (let [conn (fresh-conn)
          before (System/currentTimeMillis)]
      (d/transact conn [{:db/ident :foo/x
                         :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/one}])
      (let [after (System/currentTimeMillis)
            tx-instant (d/q '[:find ?t .
                              :where [_ :db/ident :foo/x] [_ :db/txInstant ?t]]
                            (d/db conn))]
        (testing "tx-instant falls in [before, after] window"
          (is (<= before (.getTime ^java.util.Date tx-instant) after)))))))

(deftest valid-at-filters-all-read-apis
  ;; Locks in the invariant: the FilteredDB returned by `d/valid-at`
  ;; carries its predicate via `-search-context`'s xform-after, so every
  ;; read path that flows through `dbi/datoms` / `dbi/search` honours it
  ;; — not just `d/q`. Setup: one entity with three attributes asserted
  ;; in a single tx whose vt-window is [Jan, Jul). A `valid-at` inside
  ;; the window admits all three datoms; one before the window admits
  ;; none.
  (let [conn (fresh-conn)
        _ (d/transact conn [{:db/ident :emp/name
                             :db/valueType :db.type/string
                             :db/cardinality :db.cardinality/one
                             :db/unique :db.unique/identity}
                            {:db/ident :emp/department
                             :db/valueType :db.type/string
                             :db/cardinality :db.cardinality/one
                             :db/index true}
                            {:db/ident :emp/salary
                             :db/valueType :db.type/long
                             :db/cardinality :db.cardinality/one
                             :db/index true}])
        _ (d/transact conn {:tx-data [{:emp/name "Bob"
                                       :emp/department "eng"
                                       :emp/salary 100000}]
                            :tx-meta {:db.valid/from #inst "2024-01-01"
                                      :db.valid/to   #inst "2024-07-01"}})
        db        (d/db conn)
        ;; resolve Bob's numeric eid on the *unfiltered* db once; we use
        ;; the numeric form below so the lookup-ref doesn't bottom out on
        ;; the filtered view (which would correctly throw — that's a
        ;; separate, already-tested property).
        bob       (d/q '[:find ?e . :where [?e :emp/name "Bob"]] db)
        in-window  (d/valid-at db #inst "2024-04-15")
        out-window (d/valid-at db #inst "2023-06-01")]
    (testing "d/q on plain db sees Bob"
      (is (= #{["Bob"]} (d/q '[:find ?n :where [_ :emp/name ?n]] db))))
    (testing "d/q honours valid-at"
      (is (= #{["Bob"]} (d/q '[:find ?n :where [_ :emp/name ?n]] in-window))
          "in-window: Bob visible")
      (is (= #{}        (d/q '[:find ?n :where [_ :emp/name ?n]] out-window))
          "out-window: Bob filtered out"))
    (testing "d/datoms honours valid-at"
      (is (seq  (d/datoms in-window  :avet :emp/name))
          "in-window: AVET scan returns Bob's name datom")
      (is (empty? (d/datoms out-window :avet :emp/name))
          "out-window: AVET scan returns nothing"))
    (testing "d/pull honours valid-at"
      (is (= {:db/id bob :emp/name "Bob" :emp/salary 100000}
             (d/pull in-window  [:db/id :emp/name :emp/salary] bob))
          "in-window: pull resolves both attrs by numeric eid")
      (is (nil? (d/pull out-window [:db/id :emp/name :emp/salary] bob))
          "out-window: pull returns nil because no datoms survive the filter"))
    (testing "d/entity honours valid-at"
      (is (= "Bob"   (:emp/name   (d/entity in-window  bob))))
      (is (= 100000  (:emp/salary (d/entity in-window  bob))))
      (is (nil? (:emp/name   (d/entity out-window bob)))
          "out-window: entity lookup returns nil for filtered attrs")
      (is (nil? (:emp/salary (d/entity out-window bob)))))
    (testing "d/seek-datoms and d/index-range honour valid-at"
      (is (seq (d/seek-datoms in-window :avet :emp/salary)))
      (is (empty?
           (filter #(= :emp/salary (:a %))
                   (d/seek-datoms out-window :avet :emp/salary))))
      (is (seq (d/index-range in-window {:attrid :emp/salary :start 0 :end 200000})))
      (is (empty?
           (d/index-range out-window {:attrid :emp/salary :start 0 :end 200000}))))))
