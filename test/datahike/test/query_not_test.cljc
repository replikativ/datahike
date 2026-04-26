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
