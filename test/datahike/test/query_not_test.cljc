(ns datahike.test.query-not-test
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [are deftest is testing]]
      :clj  [clojure.test :as t :refer        [are deftest is testing]])
   [datahike.api :as d]
   #?(:cljs [datahike.cljs :refer [Throwable]])
   [datahike.db :as db]
   [datahike.test.core-test]))

(def test-db
  (delay
    (d/db-with (db/empty-db)
               [{:db/id 1 :name "Ivan" :age 10}
                {:db/id 2 :name "Ivan" :age 20}
                {:db/id 3 :name "Oleg" :age 10}
                {:db/id 4 :name "Oleg" :age 20}
                {:db/id 5 :name "Ivan" :age 10}
                {:db/id 6 :name "Ivan" :age 20}])))

(deftest test-not
  (are [q res] (= (set (d/q (into '[:find [?e ...] :where] (quote q)) @test-db))
                  res)
    [[?e :name]
     (not [?e :name "Ivan"])]
    #{3 4}

    [[?e :name]
     (not
      [?e :name "Ivan"]
      [?e :age  10])]
    #{2 3 4 6}

    [[?e :name]
     (not [?e :name "Ivan"])
     (not [?e :age 10])]
    #{4}

    ;; full exclude
    [[?e :name]
     (not [?e :age])]
    #{}

    ;; not-intersecting rels
    [[?e :name "Ivan"]
     (not [?e :name "Oleg"])]
    #{1 2 5 6}

    ;; exclude empty set
    [[?e :name]
     (not [?e :name "Ivan"]
          [?e :name "Oleg"])]
    #{1 2 3 4 5 6}

    ;; nested excludes
    [[?e :name]
     (not [?e :name "Ivan"]
          (not [?e :age 10]))]
    #{1 3 4 5}

    ;; extra binding in not
    [[?e :name ?a]
     (not [?e :age ?f]
          [?e :age 10])]
    #{2 4 6}))

(deftest test-not-join
  (are [q res] (= (d/q (into '[:find ?e ?a :where] (quote q)) @test-db)
                  res)
    [[?e :name]
     [?e :age  ?a]
     (not-join [?e]
               [?e :name "Oleg"]
               [?e :age ?a])]
    #{[1 10] [2 20] [5 10] [6 20]}

    [[?e :age  ?a]
     [?e :age  10]
     (not-join [?e]
               [?e :name "Oleg"]
               [?e :age  ?a]
               [?e :age  10])]
    #{[1 10] [5 10]}))

(deftest test-not-join-in-bound-join-vars
  ;; Regression for #901: a not-join whose join vars include one supplied
  ;; through :in. Scalar :in bindings are constants, not columns of the
  ;; fused wide tuple, so the planner's anti-join post-filter has to take
  ;; their value from the const map — it used to read a missing column and
  ;; throw an NPE.
  (let [db (d/db-with (db/empty-db)
                      [{:db/id 1 :name "Ivan" :age 10}
                       {:db/id 2 :name "Ivan" :age 20}
                       {:db/id 3 :name "Oleg" :age 20}
                       {:db/id 4 :name "Petr" :age 30}])]
    (testing "one :in-bound join var next to a where-bound one"
      (are [age res] (= res
                        (d/q '[:find ?n
                               :in $ ?age
                               :where
                               [?e :name ?n]
                               (not-join [?n ?age]
                                         [?e2 :name ?n]
                                         [?e2 :age ?age])]
                             db age))
        10 #{["Oleg"] ["Petr"]}
        20 #{["Petr"]}
        40 #{["Ivan"] ["Oleg"] ["Petr"]}))

    (testing "several :in-bound join vars"
      (are [name age res] (= res
                             (d/q '[:find ?n
                                    :in $ ?name ?age
                                    :where
                                    [?e :name ?n]
                                    (not-join [?n ?name ?age]
                                              [?e2 :name ?name]
                                              [?e2 :age ?age]
                                              [?e2 :name ?n])]
                                  db name age))
        "Ivan" 10 #{["Oleg"] ["Petr"]}
        "Ivan" 30 #{["Ivan"] ["Oleg"] ["Petr"]}))))

(deftest test-negation-over-only-constants
  ;; When every var a negation mentions is supplied through :in, the fold turns
  ;; its body fully ground and the clause becomes a BOOLEAN over every row.
  ;; Three separate places lost that: the base engine reduced hash-join over
  ;; zero relations (arity exception) when such a negation came first; the
  ;; temporal fused scan emitted nothing for a column-less group, so the
  ;; negation could not tell "exists" from "doesn't" under as-of/history; and
  ;; not-join limited the negation to its join vars, which discarded a
  ;; column-less relation entirely and left the clause constraining nothing.
  (let [db (d/db-with (db/empty-db {:tag {:db/cardinality :db.cardinality/many}})
                      [{:db/id 100 :name "alice" :nick "al" :tag [:red]}
                       {:db/id 101 :name "bob" :tag [:blue]}])]
    (testing "negation first, all vars from :in"
      (are [eid res] (= res (d/q '[:find ?e ?n
                                   :in $ ?e
                                   :where (not [?e :tag :red]) [?e :name ?n]]
                                 db eid))
        100 #{}
        101 #{[101 "bob"]}))

    (testing "not-join whose join var is a constant"
      (are [eid res] (= res (d/q '[:find ?n
                                   :in $ ?e
                                   :where [?e :name ?n] (not-join [?e] [?e :nick _])]
                                 db eid))
        100 #{}
        101 #{["bob"]}))

    (testing "with an aggregate in :find"
      (is (= [] (d/q '[:find ?n (count ?e)
                       :in $ ?e
                       :where [?e :name ?n] (not-join [?e] [?e :nick _])]
                     db 100)))
      (is (= [["bob" 1]]
             (d/q '[:find ?n (count ?e)
                    :in $ ?e
                    :where [?e :name ?n] (not-join [?e] [?e :nick _])]
                  db 101))))))

;; The as-of / history side of this lives in query-planner-temporal-test, which
;; has the history-enabled connection fixtures.

(deftest test-not-value-var-bound-outside
  ;; A single-pattern `not` folds into an entity group as an anti-merge, which
  ;; drops everything but the entity: `(not [?e :tag ?t])` becomes "?e has no
  ;; :tag datom". That is the clause's meaning only while ?t is LOCAL to the
  ;; negation. Bind ?t outside and the clause means "?e has no :tag valued ?t"
  ;; — a per-binding test the anti-merge cannot express. It also contributes
  ;; no column for ?t, so the group advertised one it never wrote and the
  ;; all-nil column annihilated the join against the outer binding: every
  ;; shape below returned #{}.
  (let [db (d/db-with (db/empty-db {:tag {:db/cardinality :db.cardinality/many}})
                      [{:db/id 1 :name "a" :tag [:red :green]}
                       {:db/id 2 :name "b" :tag [:blue]}
                       {:db/id 3 :name "c"}])]
    (testing "value var bound by :in"
      (are [binding-form arg res]
           (= res (d/q {:query {:find '[?e]
                                :in ['$ binding-form]
                                :where '[[?e :name _] (not [?e :tag ?T])]}
                        :args [db arg]}))
        '[?T ...] [:red]   #{[2] [3]}
        '[?T ?U]  [:red :x] #{[2] [3]}
        '?T       :red      #{[2] [3]}))

    (testing "value var bound by another clause"
      (is (= #{[1 :blue] [2 :red] [2 :green] [3 :red] [3 :green] [3 :blue]}
             (d/q '[:find ?e ?T
                    :where [?x :tag ?T] [?e :name _] (not [?e :tag ?T])]
                  db))))

    (testing "value var bound by a function"
      (is (= #{[2] [3]}
             (d/q '[:find ?e
                    :where [(ground :red) ?T] [?e :name _] (not [?e :tag ?T])]
                  db))))

    (testing "value var supplied as a rule argument"
      (is (= #{[2] [3]}
             (d/q '[:find ?e :in $ % :where (untagged ?e :red)]
                  db '[[(untagged ?e ?t) [?e :name _] (not [?e :tag ?t])]]))))

    (testing "a genuinely local value var still folds to the anti-merge"
      (is (= #{[3]}
             (d/q '[:find ?e :where [?e :name _] (not [?e :tag ?t])] db))
          "means: ?e has no :tag at all")
      (is (= #{[2] [3]}
             (d/q '[:find ?e :where [?e :name _] (not [?e :tag :red])] db))))))

(deftest test-not-in-bound-var-only
  ;; A plain `not` whose only bound var is a scalar :in binding. The planner
  ;; folds const VALUES into clause bodies, and `not` — unlike `not-join` —
  ;; has no declared var vector to keep the var alive, so by planning time
  ;; the clause read as a negation with nothing bound and was rejected. The
  ;; binding check now runs before the fold, on the clauses as written.
  (let [db (d/db-with (db/empty-db)
                      [{:db/id 1 :name "Ivan" :age 10}
                       {:db/id 2 :name "Oleg" :age 20}])]
    (testing "the negation's body has a solution — the gate empties the result"
      (is (= #{}
             (d/q '[:find ?n
                    :in $ ?age
                    :where [?e :name ?n] (not [?e2 :age ?age])]
                  db 10))))

    (testing "the negation's body has no solution — the gate is a no-op"
      (is (= #{["Ivan"] ["Oleg"]}
             (d/q '[:find ?n
                    :in $ ?age
                    :where [?e :name ?n] (not [?e2 :age ?age])]
                  db 99))))))

(deftest test-not-join-join-var-not-bound-by-body
  ;; The fused anti-join runs the negation's sub-plan with NO outer bindings,
  ;; so the sub-plan has to bind every join var itself. Here it only READS
  ;; ?a, in a body predicate: the sub-plan came back empty, the anti-join
  ;; excluded nothing, and the query silently returned every row. Such plans
  ;; belong to the Relation engine, which threads the outer context in.
  (let [db (d/db-with (db/empty-db)
                      [{:db/id 1 :name "a" :age 10 :color "red"}
                       {:db/id 2 :name "b" :age 20 :color "blue"}
                       {:db/id 3 :name "c" :age 10 :color "red"}
                       {:db/id 4 :name "d" :age 30}])]
    (testing "join var used only by a body predicate"
      (is (= #{["d" 30]}
             (d/q '[:find ?n ?a
                    :where
                    [?e :name ?n] [?e :age ?a]
                    (not-join [?e ?a]
                              [?e :color ?c]
                              [(< ?a 25)])]
                  db))))

    (testing "join var used only as a body function argument"
      (is (= #{["d" 30]}
             (d/q '[:find ?n ?a
                    :where
                    [?e :name ?n] [?e :age ?a]
                    (not-join [?e ?a]
                              [?e :color ?c]
                              [(str ?a) ?s])]
                  db))))))

(deftest test-not-join-disconnected-gate
  ;; A not-join whose declared vars all come from :in shares no free var
  ;; with the rest of the query, which makes it a global gate: its body
  ;; either has a solution — and the whole result is empty — or it has
  ;; none and the clause is a no-op. The planner used to treat it as its
  ;; own Cartesian component, hand the resulting sub-query a :find var
  ;; only the negation mentions, and raise "Query for unknown vars".
  (let [db (d/db-with (db/empty-db)
                      [{:db/id 1 :name "Ivan" :age 10}
                       {:db/id 2 :name "Oleg" :age 20}])]
    (testing "gate with a solution empties the result"
      (is (= #{}
             (d/q '[:find ?n
                    :in $ ?age
                    :where
                    [?e :name ?n]
                    (not-join [?age] [?e2 :age ?age])]
                  db 10))))

    (testing "gate without a solution is a no-op"
      (is (= #{["Ivan"] ["Oleg"]}
             (d/q '[:find ?n
                    :in $ ?age
                    :where
                    [?e :name ?n]
                    (not-join [?age] [?e2 :age ?age])]
                  db 99))))

    (testing "genuinely disjoint patterns still split and Cartesian-merge"
      (is (= #{["Ivan" 10] ["Ivan" 20] ["Oleg" 10] ["Oleg" 20]}
             (d/q '[:find ?n ?a
                    :where
                    [?e :name ?n]
                    [?x :age ?a]]
                  db))))))

(deftest test-default-source
  (let [db1 (d/db-with (db/empty-db)
                       [[:db/add 1 :name "Ivan"]
                        [:db/add 2 :name "Oleg"]])
        db2 (d/db-with (db/empty-db)
                       [[:db/add 1 :age 10]
                        [:db/add 2 :age 20]])]
    (are [q res] (= (set (d/q (into '[:find [?e ...]
                                      :in   $ $2
                                      :where]
                                    (quote q))
                              db1 db2))
                    res)
      ;; NOT inherits default source
      [[?e :name]
       (not [?e :name "Ivan"])]
      #{2}

      ;; NOT can reference any source
      [[?e :name]
       (not [$2 ?e :age 10])]
      #{2}

      ;; NOT can change default source
      [[?e :name]
       ($2 not [?e :age 10])]
      #{2}

      ;; even with another default source, it can reference any other source explicitly
      [[?e :name]
       ($2 not [$ ?e :name "Ivan"])]
      #{2}

      ;; nested NOT keeps the default source
      [[?e :name]
       ($2 not (not [?e :age 10]))]
      #{1}

      ;; can override nested NOT source
      [[?e :name]
       ($2 not ($ not [?e :name "Ivan"]))]
      #{1})))

(deftest test-impl-edge-cases
  (are [q res] (= (d/q (quote q) @test-db)
                  res)
    ;; const \ empty
    [:find ?e
     :where [?e :name "Oleg"]
     [?e :age  10]
     (not [?e :age 20])]
    #{[3]}

    ;; const \ const
    [:find ?e
     :where [?e :name "Oleg"]
     [?e :age  10]
     (not [?e :age 10])]
    #{}

    ;; rel \ const
    [:find ?e
     :where [?e :name "Oleg"]
     (not [?e :age 10])]
    #{[4]}

    ;; 2 rels \ 2 rels
    [:find ?e ?e2
     :where [?e  :name "Ivan"]
     [?e2 :name "Ivan"]
     (not [?e :age 10]
          [?e2 :age 20])]
    #{[2 1] [6 5] [1 1] [2 2] [5 5] [6 6] [2 5] [1 5] [2 6] [6 1] [5 1] [6 2]}

    ;; 2 rels \ rel + const
    [:find ?e ?e2
     :where [?e  :name "Ivan"]
     [?e2 :name "Oleg"]
     (not [?e :age 10]
          [?e2 :age 20])]
    #{[2 3] [1 3] [2 4] [6 3] [5 3] [6 4]}

    ;; 2 rels \ 2 consts
    [:find ?e ?e2
     :where [?e  :name "Oleg"]
     [?e2 :name "Oleg"]
     (not [?e :age 10]
          [?e2 :age 20])]
    #{[4 3] [3 3] [4 4]}))

(deftest test-insufficient-bindings
  ;; Both engines now accept NOT before its binder — the legacy engine's
  ;; iterative resolver defers an unresolvable NOT and retries it after
  ;; binders fire (datahike/tools.cljc:resolve-clauses), matching the
  ;; compiled engine's plan-time topological reordering. Previously the
  ;; legacy engine raised "Insufficient bindings" eagerly.
  (testing "reorderable NOT — both engines handle correctly"
    (is (= #{[3] [4]}
           (d/q '[:find ?e :where (not [?e :name "Ivan"]) [?e :name]] @test-db))))
  (testing "NOT-JOIN with inner vars bound within body"
    (is (= #{[1] [3] [5]}
           (d/q '[:find ?e :where [?e :name]
                  (not-join [?e] (not [1 :age ?a]) [?e :age ?a])]
                @test-db))))

  ;; Truly unbound vars must still error — the iterative resolver gives
  ;; up after a fixed-point pass with no progress, raising
  ;; "Cannot resolve any more clauses" with the failed-clauses list.
  (testing "truly unbound vars throw"
    (is (thrown-with-msg? Throwable #"Cannot resolve any more clauses|Insufficient bindings"
                          (d/q '[:find ?e :where [?e :name] (not [?a :name "Ivan"])]
                               @test-db)))))

(deftest test-deferred-clause-binding
  ;; Regression test for a planner gap: clauses that gate on bound vars
  ;; (predicates, NOT, NOT-JOIN, OR-JOIN-with-required-vars) used to
  ;; raise "Insufficient bindings" on the first pass instead of deferring
  ;; like bind-by-fn does. When their inputs traced back to a deferred
  ;; binder (e.g. a get-else whose entity var was itself bound by a
  ;; later pattern), the eager raise masked perfectly resolvable queries.
  ;;
  ;; The user-facing repro lives in pgwire-datahike: a SQL `WHERE
  ;; format_type(a.atttypid, a.atttypmod) NOT IN (…)` translated to:
  ;;   [?e :pg_attribute/db-row-exists true]      ; row marker — last
  ;;   [(get-else $ ?e :…/atttypid :__null__) ?a] ; deferred until ?e bound
  ;;   [(get-else $ ?e :…/atttypmod :__null__) ?b]
  ;;   [(?fmt ?a ?b) ?v1]                          ; deferred until ?a ?b bound
  ;;   (not [(contains? #{…} ?v1)])               ; deferred until ?v1 bound
  ;; Old planner: raised on the (not …) before the chain could fire.
  ;; New: each clause defers, the resolver iterates, all clauses resolve.
  (let [db (d/db-with (db/empty-db)
                      [{:db/id 1 :name "Ivan"}
                       {:db/id 2 :name "Oleg"}
                       {:db/id 3 :name "Ivan"}])]
    (testing "predicate before its binder defers"
      (is (= #{[1] [3]}
             (d/q '[:find ?e
                    :where [(= ?n "Ivan")]
                    [?e :name ?n]]
                  db))))

    (testing "NOT before its binder defers"
      (is (= #{[2]}
             (d/q '[:find ?e
                    :where (not [?e :name "Ivan"])
                    [?e :name]]
                  db))))

    (testing "fn-call deferred chain feeding (not [pred])"
      ;; ?upper resolves only after both the binder and the deferred
      ;; fn-call run; the NOT clause must wait through the cascade.
      ;; Tests both engines:
      ;;  - Legacy: iterative resolver retries the deferred clauses.
      ;;  - New planner: relies on lower.cljc's NOT validation reading
      ;;    :function ops' :binding (was :bind-vars — wrong key, never set).
      (is (= #{[2]}
             (d/q '[:find ?e
                    :in $ ?up
                    :where (not [(contains? #{"IVAN"} ?upper)])
                    [(?up ?n) ?upper]
                    [?e :name ?n]]
                  db (fn [^String s] (.toUpperCase s))))))))
