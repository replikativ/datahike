(ns datahike.test.query-or-test
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest]]
      :clj  [clojure.test :as t :refer        [is are deftest]])
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

(deftest test-or
  (are [q res] (= (d/q (into '[:find ?e :where] (quote q)) @test-db)
                  (into #{} (map vector) res))

    ;; intersecting results
    [(or [?e :name "Oleg"]
         [?e :age 10])]
    #{1 3 4 5}

    ;; one branch empty
    [(or [?e :name "Oleg"]
         [?e :age 30])]
    #{3 4}

    ;; both empty
    [(or [?e :name "Petr"]
         [?e :age 30])]
    #{}

    ;; join with 1 var
    [[?e :name "Ivan"]
     (or [?e :name "Oleg"]
         [?e :age 10])]
    #{1 5}

    ;; join with 2 vars
    [[?e :age ?a]
     (or (and [?e :name "Ivan"]
              [1  :age  ?a])
         (and [?e :name "Oleg"]
              [2  :age  ?a]))]
    #{1 5 4}

    ;; OR introduces vars
    [(or (and [?e :name "Ivan"]
              [1  :age  ?a])
         (and [?e :name "Oleg"]
              [2  :age  ?a]))
     [?e :age ?a]]
    #{1 5 4}

    ;; OR introduces vars in different order
    [(or (and [?e :name "Ivan"]
              [1  :age  ?a])
         (and [2  :age  ?a]
              [?e :name "Oleg"]))
     [?e :age ?a]]
    #{1 5 4}))

(deftest test-or-join
  (are [q res] (= (d/q (into '[:find ?e :where] (quote q)) @test-db)
                  (into #{} (map vector) res))
    [(or-join [?e]
              [?e :name ?n]
              (and [?e :age ?a]
                   [?e :name ?n]))]
    #{1 2 3 4 5 6}

    [[?e  :name ?a]
     [?e2 :name ?a]
     (or-join [?e]
              (and [?e  :age ?a]
                   [?e2 :age ?a]))]
    #{1 2 3 4 5 6})

  (is (= #{[:a1 :b1 :c1]
           [:a2 :b2 :c2]}
         (d/q '[:find ?a ?b ?c
                :in $xs $ys
                :where [$xs ?a ?b ?c] ;; check join by ?a, ignoring ?b, dropping ?c ?d
                (or-join [?a]
                         [$ys ?a ?b ?d])]
              [[:a1 :b1 :c1]
               [:a2 :b2 :c2]
               [:a3 :b3 :c3]]
              [[:a1 :b1  :d1] ;; same ?a, same ?b
               [:a2 :b2* :d2] ;; same ?a, different ?b. Should still be joined
               [:a4 :b4 :c4]]))) ;; different ?a, should be dropped

  (is (= #{[:a1 :c1] [:a2 :c2]}
         (d/q '[:find ?a ?c
                :in $xs $ys
                :where (or-join [?a ?c]
                                [$xs ?a ?b ?c] ; rel with hole (?b gets dropped, leaving {?a 0 ?c 2} and 3-element tuples)
                                [$ys ?a ?c])]
              [[:a1 :b1 :c1]]
              [[:a2 :c2]]))))

(deftest test-or-join-predicate-branches
  ;; or-join / or where branches contain only predicates (no data patterns).
  ;; Regression: these were misclassified as patterns in the query planner
  ;; because (= ?a 10) is a list that classify-clause treated as [entity attr val].
  (is (= #{[10]}
         (d/q '[:find ?a
                :where [?e :age ?a]
                (or-join [?a] [(= ?a 10)])]
              @test-db))
      "or-join with single predicate branch")
  (is (= #{[10] [20]}
         (d/q '[:find ?a
                :where [?e :age ?a]
                (or-join [?a] [(= ?a 10)] [(= ?a 20)])]
              @test-db))
      "or-join with two predicate branches")
  (is (= #{[10]}
         (d/q '[:find ?a
                :where [?e :age ?a]
                (or [(= ?a 10)])]
              @test-db))
      "or with single predicate branch")
  ;; NOT wrapping predicate-only or-join
  (is (= #{[20]}
         (d/q '[:find ?a
                :where [?e :age ?a]
                (not (or-join [?a] [(= ?a 10)]))]
              @test-db))
      "not wrapping predicate-only or-join"))

(deftest test-or-join-output-var-predicate
  ;; Predicate on a variable produced by or-join branches (not pre-bound).
  ;; Regression: or-join required ALL join-vars bound, so when a join-var
  ;; was an output (produced by branches, not the outer context), the or-join
  ;; was deferred to force-emit and scheduled after the predicate.
  (let [db (d/db-with (db/empty-db)
                      [{:db/ident :j/i :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                       {:db/ident :j/k :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                       {:db/ident :t/i :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                       {:t/i 1} {:t/i 2} {:t/i 4}
                       {:j/i 1 :j/k 10} {:j/i 2 :j/k 20}])]
    (is (= #{[2 20]}
           (d/q '[:find ?i ?k
                  :where [?e :t/i ?i]
                  (or-join [?i ?k]
                           (and [?e2 :j/i ?i] [?e2 :j/k ?k])
                           (and (not-join [?i] [?e2 :j/i ?i])
                                [(ground -1) ?k]))
                  [(= ?k 20)]]
                db))
        "predicate on or-join output var filters correctly")))

(deftest test-default-source
  (let [db1 (d/db-with (db/empty-db)
                       [[:db/add 1 :name "Ivan"]
                        [:db/add 2 :name "Oleg"]])
        db2 (d/db-with (db/empty-db)
                       [[:db/add 1 :age 10]
                        [:db/add 2 :age 20]])]
    (are [q res] (= (d/q (into '[:find ?e :in $ $2 :where] (quote q)) db1 db2)
                    (into #{} (map vector) res))
      ;; OR inherits default source
      [[?e :name]
       (or [?e :name "Ivan"])]
      #{1}

      ;; OR can reference any source
      [[?e :name]
       (or [$2 ?e :age 10])]
      #{1}

      ;; OR can change default source
      [[?e :name]
       ($2 or [?e :age 10])]
      #{1}

      ;; even with another default source, it can reference any other source explicitly
      [[?e :name]
       ($2 or [$ ?e :name "Ivan"])]
      #{1}

      ;; nested OR keeps the default source
      [[?e :name]
       ($2 or (or [?e :age 10]))]
      #{1}

      ;; can override nested OR source
      [[?e :name]
       ($2 or ($ or [?e :name "Ivan"]))]
      #{1})))

(deftest test-errors
  (is (thrown-with-msg? Throwable #"Join variable not declared inside clauses"
                        (d/q '[:find ?e
                               :where (or [?e :name _]
                                          [?e :age ?a])]
                             @test-db)))

  (is (thrown-with-msg? Throwable #"Insufficient bindings: #\{\?e\} not bound"
                        (d/q '[:find ?e
                               :where (or-join [[?e]]
                                               [?e :name "Ivan"])]
                             @test-db))))
