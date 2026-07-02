(ns datahike.test.query-fns-test
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [datahike.api :as d]
   [datahike.db :as db])
  (:import [java.util UUID Date])
  #?(:clj
     (:import [clojure.lang ExceptionInfo])))

(deftest test-query-fns
  (testing "predicate without free variables"
    (is (= (d/q '[:find ?x
                  :in [?x ...]
                  :where [(> 2 1)]] [:a :b :c])
           #{[:a] [:b] [:c]})))

  (let [db (-> (db/empty-db {:parent {:db/valueType :db.type/ref}})
               (d/db-with [{:db/id 1, :name  "Ivan",  :age   15}
                           {:db/id 2, :name  "Petr",  :age   22, :height 240, :parent 1}
                           {:db/id 3, :name  "Slava", :age   37, :parent 2}
                           {:db/id 4, :name  "Ivan",  :age   22}]))]

    (testing "ground"
      (is (= (d/q '[:find ?vowel
                    :where [(ground [:a :e :i :o :u]) [?vowel ...]]])
             #{[:a] [:e] [:i] [:o] [:u]})))

    (testing "get-else"
      (is (= #{[1 15 300] [2 22 240] [3 37 300] [4 22 300]}
             (d/q '[:find ?e ?age ?height
                    :where [?e :age ?age]
                    [(get-else $ ?e :height 300) ?height]] db)))
      (is (= #{[1 15 300] [2 22 240] [3 37 300] [4 22 300]}
             (d/q '[:find ?e ?age ?height
                    :where
                    [(get-else $ ?e :height 300) ?height]
                    [?e :age ?age]] db)))

      (is (thrown-with-msg? ExceptionInfo #"get-else: nil default value is not supported"
                            (d/q '[:find ?e ?height
                                   :where [?e :age]
                                   [(get-else $ ?e :height nil) ?height]] db))))

    (testing "get-some"
      (is (= #{[1 :age 15]
               [2 :height 240]
               [3 :age 37]
               [4 :age 22]}
             (d/q '[:find ?e ?a ?v
                    :where [?e :name _]
                    [(get-some $ ?e :height :age) [?a ?v]]] db))))

    (testing "q"
      (is (= #{[1 "Ivan"]}
             (d/q '[:find ?e ?n
                    :where
                    [(q '[:find (min ?age) :where [_ :age ?age]] $) [[?a]]]
                    [?e :age ?a]
                    [?e :name ?n]]
                  db))))

    (testing "q with rules"
      (is (thrown-with-msg? ExceptionInfo #"Missing rules var '%' in :in"
                            (d/q '[:find ?n ?a
                                   :in $ %
                                   :where
                                   [(q [:find ?name (min ?age)
                                        :where (has-age ?name ?age)])
                                    [[?n ?a]]]
                                   (has-age ?n ?a)]
                                 db
                                 '[[(has-age ?n ?a) [?e :age ?a] [?e :name ?n]]])))
      (is (= #{}
             (d/q '[:find ?n ?a
                    :in $ %
                    :where
                    [(q [:find ?name (min ?age)
                         :in $ %
                         :where (has-age ?name ?age)])
                     [[?n ?a]]]
                    (has-age ?n ?a)]
                  db
                  '[[(has-age ?n ?a) [?e :age ?a] [?e :name ?n]]])))
      (is (= #{["Petr" 22] ["Ivan" 15] ["Slava" 37]}
             (d/q '[:find ?n ?a
                    :in $ %
                    :where
                    [(q [:find ?name (min ?age)
                         :in $ %
                         :where (has-age ?name ?age)]
                        $
                        [[(has-age ?n ?a) [?e :age ?a] [?e :name ?n]]])
                     [[?n ?a]]]
                    (has-age ?n ?a)]
                  db
                  '[[(has-age ?n ?a) [?e :age ?a] [?e :name ?n]]]))))

    (testing "q without quotes"
      (is (= #{[1 "Ivan"]}
             (d/q '[:find ?e ?n
                    :where
                    [(q [:find (min ?age) :where [_ :age ?age]] $) [[?a]]]
                    [?e :age ?a]
                    [?e :name ?n]]
                  db))))

    (testing "missing?"
      (is (= #{[1 15] [3 37] [4 22]}
             (d/q '[:find ?e ?age
                    :in $
                    :where [?e :age ?age]
                    [(missing? $ ?e :height)]] db))))

    (testing "missing? back-ref"
      (is (= #{[3] [4]}
             (d/q '[:find ?e
                    :in $
                    :where [?e :age ?age]
                    [(missing? $ ?e :_parent)]] db))))

    (testing "Built-ins"
      (is (= #{[1 2] [1 3] [1 4]}
             (d/q '[:find  ?e1 ?e2
                    :where [?e1 :age ?a1]
                    [?e2 :age ?a2]
                    [(< ?a1 18 ?a2)]] db)))

      (is (= (d/q '[:find  ?x ?c
                    :in    [?x ...]
                    :where [(count ?x) ?c]]
                  ["a" "abc"])
             #{["a" 1] ["abc" 3]})))

    (testing "Clojure core built-ins"
      (is (= #{[:age] [:parent] [:height]}
             (d/q '[:find ?attr
                    :where
                    [_ ?attr ?a]
                    [(int? ?a)]]
                  db)))
      (is (= #{[:Ivan] [:Petr] [:Slava]}
             (d/q '[:find ?k
                    :where
                    [_ :name ?n]
                    [(keyword ?n) ?k]]
                  db)))

      (is (= #{[16] [23] [38]}
             (d/q '[:find ?na
                    :where
                    [_ :age ?a]
                    [(inc ?a) ?na]]
                  db))))

    (testing "Function binding filtered by input argument"
      (is (= (d/q '[:find  ?x
                    :in    [?x ...] ?c
                    :where [(count ?x) ?c]]
                  ["a" "abc"]
                  3)
             #{["abc"]})))

    (testing "Built-in vector, hashmap"
      (is (= (d/q '[:find [?tx-data ...]
                    :where
                    [(ground :db/add) ?op]
                    [(vector ?op -1 :attr 12) ?tx-data]])
             [[:db/add -1 :attr 12]]))

      (is (= (d/q '[:find [?tx-data ...]
                    :where
                    [(hash-map :db/id -1 :age 92 :name "Aaron") ?tx-data]])
             [{:db/id -1 :age 92 :name "Aaron"}])))

    (testing "Passing predicate as source"
      (is (= #{[2] [3] [4]}
             (d/q '[:find  ?e
                    :in    $ ?adult
                    :where [?e :age ?a]
                    [(?adult ?a)]]
                  db
                  #(> % 18)))))

    (testing "Calling a function"
      (is (= #{[1 2 3] [2 1 3] [1 4 3] [4 1 3]}
             (d/q '[:find  ?e1 ?e2 ?e3
                    :where [?e1 :age ?a1]
                    [?e2 :age ?a2]
                    [?e3 :age ?a3]
                    [(+ ?a1 ?a2) ?a12]
                    [(= ?a12 ?a3)]]
                  db))))

    (testing "Two conflicting function values for one binding."
      (is (= (d/q '[:find  ?n
                    :where [(identity 1) ?n]
                    [(identity 2) ?n]]
                  db)
             #{})))

    (testing "Destructured conflicting function values for two bindings."
      (is (= (d/q '[:find  ?n ?x
                    :where [(identity [3 4]) [?n ?x]]
                    [(identity [1 2]) [?n ?x]]]
                  db)
             #{})))

    (testing "Rule bindings interacting with function binding. (fn, rule)"
      (is (= (d/q '[:find  ?n
                    :in $ %
                    :where [(identity 2) ?n]
                    (my-vals ?n)]
                  db
                  '[[(my-vals ?x)
                     [(identity 1) ?x]]
                    [(my-vals ?x)
                     [(identity 2) ?x]]
                    [(my-vals ?x)
                     [(identity 3) ?x]]])
             #{[2]})))

    (testing "Rule bindings interacting with function binding. (rule, fn)"
      (is (= (d/q '[:find  ?n
                    :in $ %
                    :where (my-vals ?n)
                    [(identity 2) ?n]]
                  db
                  '[[(my-vals ?x)
                     [(identity 1) ?x]]
                    [(my-vals ?x)
                     [(identity 2) ?x]]
                    [(my-vals ?x)
                     [(identity 3) ?x]]])
             #{[2]})))

    (testing "Conflicting relational bindings with function binding. (rel, fn)"
      (is (= (d/q '[:find  ?age
                    :where [_ :age ?age]
                    [(identity 100) ?age]]
                  db)
             #{})))

    (testing "Conflicting relational bindings with function binding. (fn, rel)"
      (is (= (d/q '[:find  ?age
                    :where [(identity 100) ?age]
                    [_ :age ?age]]
                  db)
             #{})))

    (testing "Function on empty rel"
      (is (= (d/q '[:find  ?e ?y
                    :where [?e :salary ?x]
                    [(+ ?x 100) ?y]]
                  [[0 :age 15] [1 :age 35]])
             #{})))

    (testing "Returning nil from function filters out tuple from result"
      (is (= (d/q '[:find ?x
                    :in    [?in ...] ?f
                    :where [(?f ?in) ?x]]
                  [1 2 3 4]
                  #(when (even? %) %))
             #{[2] [4]})))

    (testing "Result bindings"
      (is (= (d/q '[:find ?a ?c
                    :in ?in
                    :where [(ground ?in) [?a _ ?c]]]
                  [:a :b :c])
             #{[:a :c]}))

      (is (= (d/q '[:find ?in
                    :in ?in
                    :where [(ground ?in) _]]
                  :a)
             #{[:a]}))

      (is (= (d/q '[:find ?x ?z
                    :in ?in
                    :where [(ground ?in) [[?x _ ?z] ...]]]
                  [[:a :b :c] [:d :e :f]])
             #{[:a :c] [:d :f]}))

      (is (= (d/q '[:find ?in
                    :in [?in ...]
                    :where [(ground ?in) _]]
                  [])
             #{})))))

(deftest test-predicate-over-bound-vars
  ;; Issue #848: a predicate/function clause whose vars are all bound by `:in`
  ;; constants (no relation in context) was never allowed to eliminate those
  ;; bindings — the const passed through unfiltered. Root cause was in -collect:
  ;; a zero-tuple eliminating relation with no find-var attrs was skipped instead
  ;; of annihilating the const-seeded accumulator. Datomic semantics: a predicate
  ;; that returns false/nil removes the binding (present->drop, absent->keep,
  ;; false->drop), including when every input is an `:in` const. (CI runs the
  ;; whole suite under both the compiled planner and the legacy engine, so these
  ;; plain assertions cover both.)
  (let [db (-> (db/empty-db) (d/db-with [{:db/id 100 :a "v" :b "v"}]))]

    (testing "const-only predicate over :in binding filters that binding"
      ;; present attribute -> missing? false -> binding dropped
      (is (= nil (d/q '[:find ?e . :in $ ?e
                        :where [(missing? $ ?e :a)]] db 100)))
      ;; absent attribute -> missing? true -> binding kept
      (is (= 100 (d/q '[:find ?e . :in $ ?e
                        :where [(missing? $ ?e :c)]] db 100)))
      ;; plain predicate, false / true over a const
      (is (= nil (d/q '[:find ?e . :in $ ?e :where [(= ?e 999)]] db 100)))
      (is (= 100 (d/q '[:find ?e . :in $ ?e :where [(= ?e 100)]] db 100))))

    (testing "const-only predicate with no var args"
      (is (= 100 (d/q '[:find ?e . :in $ ?e :where [(= 1 1)]] db 100)))
      (is (= nil (d/q '[:find ?e . :in $ ?e :where [(= 1 2)]] db 100))))

    (testing "multiple const-only predicates compose"
      (is (= 100 (d/q '[:find ?e . :in $ ?e
                        :where [(= ?e 100)] [(missing? $ ?e :c)]] db 100)))
      (is (= nil (d/q '[:find ?e . :in $ ?e
                        :where [(= ?e 100)] [(missing? $ ?e :a)]] db 100))))

    (testing "a where-clause with no matches and a const find-var annihilates"
      (is (= nil (d/q '[:find ?e . :in $ ?e
                        :where [?x :nope ?y]] db 100))))

    (testing "pattern-bound predicates are unaffected (no regression)"
      ;; present -> missing? false -> []
      (is (= [] (d/q '[:find [?e ...] :where
                       [?e :b ?v] [(missing? $ ?e :a)]] db)))
      ;; absent -> missing? true -> [100]  (the case a stale REPL mis-reported)
      (is (= [100] (d/q '[:find [?e ...] :where
                          [?e :a ?v] [(missing? $ ?e :c)]] db)))
      (is (= [] (d/q '[:find [?e ...] :where
                       [?e :a ?v] [(= ?e 999)]] db)))
      (is (= [100] (d/q '[:find [?e ...] :where
                          [?e :a ?v] [(> ?e 0)]] db))))

    (testing "mixed const+pattern predicate"
      (is (= [] (d/q '[:find [?x ...] :in $ ?e
                       :where [?e :a ?x] [(= ?e 999)]] db 100)))
      (is (= ["v"] (d/q '[:find [?x ...] :in $ ?e
                          :where [?e :a ?x] [(= ?e 100)]] db 100))))

    (testing "const-only predicate alongside a db-bound pattern"
      ;; The empty eliminating relation produced by a false const-only predicate
      ;; must annihilate even when a separate, non-empty pattern relation is also
      ;; in context (it is conj'd onto :rels and -collect honors it). A true one
      ;; is a no-op. Before the fix this diverged: the legacy engine kept all
      ;; tuples for the false case while the planner correctly dropped them.
      (let [ndb (-> (db/empty-db) (d/db-with [{:db/id 1 :v 10}
                                              {:db/id 2 :v 20}]))]
        (is (= [] (d/q '[:find [?v ...] :in $ ?lim
                         :where [?e :v ?v] [(< 1 ?lim)]] ndb 0)))
        (is (= #{10 20} (set (d/q '[:find [?v ...] :in $ ?lim
                                    :where [?e :v ?v] [(< 1 ?lim)]] ndb 99))))
        ;; mixed within a single predicate: db var + :in const, all/some filtered
        (is (= [] (d/q '[:find [?v ...] :in $ ?lim
                         :where [?e :v ?v] [(< ?v ?lim)]] ndb 5)))
        (is (= #{10} (set (d/q '[:find [?v ...] :in $ ?lim
                                 :where [?e :v ?v] [(< ?v ?lim)]] ndb 15))))))

    (testing "binding fns and negation unaffected"
      (is (= ["def"] (d/q '[:find [?x ...] :where
                            [?e :a ?v] [(get-else $ ?e :c "def") ?x]] db)))
      (is (= [100] (d/q '[:find [?e ...] :where
                          [?e :a ?v] (not [?e :c ?z])] db))))

    (testing "empty-result aggregates agree across const and pattern paths"
      ;; const-false path and empty-pattern path now both yield nil (they
      ;; disagreed before the fix: const-false wrongly counted 1)
      (is (= nil (d/q '[:find (count ?e) . :in $ ?e
                        :where [(= ?e 999)]] db 100)))
      (is (= nil (d/q '[:find (count ?e) . :where [?e :nope ?v]] db)))
      (is (= 1 (d/q '[:find (count ?e) . :where [?e :a ?v]] db))))))

(deftest test-predicates
  (let [entities [{:db/id 1 :name "Ivan" :age 10}
                  {:db/id 2 :name "Ivan" :age 20}
                  {:db/id 3 :name "Oleg" :age 10}
                  {:db/id 4 :name "Oleg" :age 20}]
        db (d/db-with (db/empty-db) entities)]
    (are [q res] (= (d/q (quote q) db) res)
      ;; plain predicate
      [:find  ?e ?a
       :where [?e :age ?a]
       [(> ?a 10)]]
      #{[2 20] [4 20]}

      ;; join in predicate
      [:find  ?e ?e2
       :where [?e  :name]
       [?e2 :name]
       [(< ?e ?e2)]]
      #{[1 2] [1 3] [1 4] [2 3] [2 4] [3 4]}

      ;; join with extra symbols
      [:find  ?e ?e2
       :where [?e  :age ?a]
       [?e2 :age ?a2]
       [(< ?e ?e2)]]
      #{[1 2] [1 3] [1 4] [2 3] [2 4] [3 4]}

      ;; empty result
      [:find  ?e ?e2
       :where [?e  :name "Ivan"]
       [?e2 :name "Oleg"]
       [(= ?e ?e2)]]
      #{}

      ;; pred over const, true
      [:find  ?e
       :where [?e :name "Ivan"]
       [?e :age 20]
       [(= ?e 2)]]
      #{[2]}

      ;; pred over const, false
      [:find  ?e
       :where [?e :name "Ivan"]
       [?e :age 20]
       [(= ?e 1)]]
      #{})
    (let [pred (fn [db e a]
                 (= a (:age (d/entity db e))))]
      (is (= (d/q '[:find ?e
                    :in $ ?pred
                    :where [?e :age ?a]
                    [(?pred $ ?e 10)]]
                  db pred)
             #{[1] [3]})))))

(deftest test-exceptions
  (is (thrown-with-msg? ExceptionInfo #"Unknown predicate 'fun in \[\(fun \?e\)\]"
                        (d/q '[:find ?e
                               :in   [?e ...]
                               :where [(fun ?e)]]
                             [1])))

  (is (thrown-with-msg? ExceptionInfo #"Unknown function 'fun in \[\(fun \?e\) \?x\]"
                        (d/q '[:find ?e ?x
                               :in   [?e ...]
                               :where [(fun ?e) ?x]]
                             [1]))))

(deftest test-issue-180
  (is (= #{}
         (d/q '[:find ?e ?a
                :where [_ :pred ?pred]
                [?e :age ?a]
                [(?pred ?a)]]
              (d/db-with (db/empty-db) [[:db/add 1 :age 20]])))))

(defn sample-query-fn [] 42)

#?(:clj
   (deftest test-symbol-resolution
     (is (= 42 (d/q '[:find ?x .
                      :where [(datahike.test.query-fns-test/sample-query-fn) ?x]])))))

(deftest test-built-in-predicates-types
  (let [uuids (sort (repeatedly 3 #(UUID/randomUUID)))
        dates (map #(-> (Date.) .getTime (+ (* % 86400 1000)) Date.) (range 3))
        entities [{:db/id 1 :uuid (nth uuids 0) :birthday (nth dates 0) :symbol 'a :name "Alek" :age 20
                   :group :a :collegues [1 2] :married? false}
                  {:db/id 2 :uuid (nth uuids 1) :birthday (nth dates 1) :symbol 'i :name "Ivan" :age 30
                   :group :b :collegues [2 3] :married? true}
                  {:db/id 3 :uuid (nth uuids 2) :birthday (nth dates 2) :symbol 'o :name "Oleg" :age 40
                   :group :c :collegues [3 4] :married? true}
                  {:db/id 4 :uuid (nth uuids 2) :birthday (nth dates 2) :symbol 'o :name "Oleg" :age 40
                   :group :c :collegues [3 4] :married? true}]
        db (d/db-with (db/empty-db) entities)]

    (testing "lesser"
      (are [attr] (=  #{[1 2] [1 3] [1 4] [2 3] [2 4]}
                      (d/q (into '[:find  ?e1 ?e2 :where]
                                 [['?e1 attr '?a1]
                                  ['?e2 attr '?a2]
                                  ['(< ?a1 ?a2)]])
                           db))
        :uuid :birthday :symbol :name :age :group :collegues)
      (is (=  #{[1 2] [1 3] [1 4]}
              (d/q (into '[:find  ?e1 ?e2 :where]
                         [['?e1 :married? '?a1]
                          ['?e2 :married? '?a2]
                          ['(< ?a1 ?a2)]])
                   db)))
      (are [attr] (=  #{[1 2 3] [1 2 4]}
                      (d/q (into '[:find  ?e1 ?e2 ?e3 :where]
                                 [['?e1 attr '?a1]
                                  ['?e2 attr '?a2]
                                  ['?e3 attr '?a3]
                                  ['(< ?a1 ?a2 ?a3)]])

                           db))
        :uuid :birthday :symbol :name :age :group :collegues)
      (is (=  #{}
              (d/q (into '[:find  ?e1 ?e2 ?e3 :where]
                         [['?e1 :married? '?a1]
                          ['?e2 :married? '?a2]
                          ['?e3 :married? '?a3]
                          ['(< ?a1 ?a2 ?a3)]])

                   db)))

      (testing "greater"
        (are [attr] (=  #{[4 1] [3 1] [2 1] [4 2] [3 2]}
                        (d/q (into '[:find  ?e1 ?e2 :where]
                                   [['?e1 attr '?a1]
                                    ['?e2 attr '?a2]
                                    ['(> ?a1 ?a2)]])
                             db))
          :uuid :birthday :symbol :name :age :group :collegues)
        (is (=  #{[4 1] [3 1] [2 1]}
                (d/q (into '[:find  ?e1 ?e2 :where]
                           [['?e1 :married? '?a1]
                            ['?e2 :married? '?a2]
                            ['(> ?a1 ?a2)]])
                     db)))
        (are [attr] (=  #{[3 2 1] [4 2 1]}
                        (d/q (into '[:find  ?e1 ?e2 ?e3 :where]
                                   [['?e1 attr '?a1]
                                    ['?e2 attr '?a2]
                                    ['?e3 attr '?a3]
                                    ['(> ?a1 ?a2 ?a3)]])
                             db))
          :uuid :birthday :symbol :name :age :group :collegues)
        (is (=  #{}
                (d/q (into '[:find  ?e1 ?e2 ?e3 :where]
                           [['?e1 :married? '?a1]
                            ['?e2 :married? '?a2]
                            ['?e3 :married? '?a3]
                            ['(> ?a1 ?a2 ?a3)]])
                     db))))

      (testing "lesser-equal"
        (are [attr] (=  #{[1 2] [1 3] [1 4] [2 3] [2 4] [1 1] [2 2] [3 3] [3 4] [4 3] [4 4]}
                        (d/q (into '[:find  ?e1 ?e2 :where]
                                   [['?e1 attr '?a1]
                                    ['?e2 attr '?a2]
                                    ['(<= ?a1 ?a2)]])
                             db))
          :uuid :birthday :symbol :name :age :group :collegues)
        (is (=  #{[4 3] [2 2] [2 3] [3 3] [1 1] [3 4] [4 2] [1 4] [1 3] [2 4] [4 4] [1 2] [3 2]}
                (d/q (into '[:find  ?e1 ?e2 :where]
                           [['?e1 :married? '?a1]
                            ['?e2 :married? '?a2]
                            ['(<= ?a1 ?a2)]])
                     db)))
        (are [attr] (=  #{[1 1 1] [1 1 2] [1 1 3] [1 1 4] [1 2 2] [1 2 3] [1 2 4] [1 3 3]
                          [1 3 4] [1 4 3] [1 4 4] [2 2 2] [2 2 3] [2 2 4] [2 3 3] [2 3 4]
                          [2 4 3] [2 4 4] [3 3 3] [3 3 4] [3 4 3] [3 4 4] [4 3 3] [4 3 4]
                          [4 4 3] [4 4 4]}
                        (d/q (into '[:find  ?e1 ?e2 ?e3 :where]
                                   [['?e1 attr '?a1]
                                    ['?e2 attr '?a2]
                                    ['?e3 attr '?a3]
                                    ['(<= ?a1 ?a2 ?a3)]])
                             db))
          :uuid :birthday :symbol :name :age :group :collegues)
        (is (=  #{[2 4 4] [3 2 4] [4 4 2] [1 4 3] [2 4 2] [2 2 4] [1 3 2] [4 3 2]
                  [3 4 3] [4 4 3] [4 2 4] [3 4 2] [1 1 3] [1 3 4] [1 4 4] [2 3 2]
                  [1 1 1] [2 2 2] [3 2 2] [3 3 3] [1 3 3] [3 2 3] [2 3 4] [4 3 4]
                  [2 2 3] [4 3 3] [4 4 4] [1 2 3] [1 1 4] [2 3 3] [3 4 4] [1 1 2]
                  [2 4 3] [3 3 4] [1 2 2] [1 2 4] [4 2 2] [4 2 3] [1 4 2] [3 3 2]}
                (d/q (into '[:find  ?e1 ?e2 ?e3 :where]
                           [['?e1 :married? '?a1]
                            ['?e2 :married? '?a2]
                            ['?e3 :married? '?a3]
                            ['(<= ?a1 ?a2 ?a3)]])
                     db))))

      (testing "greater-equal"
        (are [attr] (=  #{[4 1] [3 1] [2 1] [4 2] [3 2] [1 1] [2 2] [3 3] [3 4] [4 3] [4 4]}
                        (d/q (into '[:find  ?e1 ?e2 :where]
                                   [['?e1 attr '?a1]
                                    ['?e2 attr '?a2]
                                    ['(>= ?a1 ?a2)]])
                             db))
          :uuid :birthday :symbol :name :age :group :collegues)
        (is (=  #{[4 3] [2 2] [2 3] [3 3] [1 1] [3 4] [4 2] [4 1] [2 4] [3 1] [2 1] [4 4] [3 2]}
                (d/q (into '[:find  ?e1 ?e2 :where]
                           [['?e1 :married? '?a1]
                            ['?e2 :married? '?a2]
                            ['(>= ?a1 ?a2)]])
                     db)))
        (are [attr] (=  #{[3 2 1] [4 2 1] [4 3 1] [4 3 2] [1 1 1] [2 1 1] [2 2 1] [2 2 2]
                          [3 1 1] [3 2 2] [3 3 1] [3 3 2] [3 3 3] [3 3 4] [3 4 1] [3 4 2]
                          [3 4 3] [3 4 4] [4 1 1] [4 2 2] [4 3 3] [4 3 4] [4 4 1] [4 4 2]
                          [4 4 3] [4 4 4]}
                        (d/q (into '[:find  ?e1 ?e2 ?e3 :where]
                                   [['?e1 attr '?a1]
                                    ['?e2 attr '?a2]
                                    ['?e3 attr '?a3]
                                    ['(>= ?a1 ?a2 ?a3)]])
                             db))
          :uuid :birthday :symbol :name :age :group :collegues)
        (is (=  #{[2 4 4] [2 3 1] [2 4 1] [3 2 4] [4 4 2] [3 4 1] [2 4 2] [2 2 4]
                  [3 1 1] [4 3 2] [3 4 3] [4 4 3] [4 2 4] [3 4 2] [2 3 2] [4 4 1]
                  [1 1 1] [2 2 2] [3 2 2] [3 3 3] [3 2 3] [4 2 1] [2 2 1] [4 3 1]
                  [2 3 4] [4 3 4] [2 2 3] [3 3 1] [4 3 3] [4 4 4] [2 1 1] [4 1 1]
                  [2 3 3] [3 4 4] [3 2 1] [2 4 3] [3 3 4] [4 2 2] [4 2 3] [3 3 2]}
                (d/q (into '[:find  ?e1 ?e2 ?e3 :where]
                           [['?e1 :married? '?a1]
                            ['?e2 :married? '?a2]
                            ['?e3 :married? '?a3]
                            ['(>= ?a1 ?a2 ?a3)]])
                     db)))))))

