(ns datahike.test.query-fns
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [datahike.core :as d]
   [datahike.test.core :as tdc])
  #?(:clj
     (:import [clojure.lang ExceptionInfo])))

(deftest test-query-fns
  (testing "predicate without free variables"
    (is (= (d/q '[:find ?x
                  :in [?x ...]
                  :where [(> 2 1)]] [:a :b :c])
           #{[:a] [:b] [:c]})))

  (let [db (-> (d/empty-db {:parent {:db/valueType :db.type/ref}})
               (d/db-with [{:db/id tdc/e1, :name  "Ivan",  :age   15}
                           {:db/id tdc/e2, :name  "Petr",  :age   22, :height 240, :parent tdc/e1}
                           {:db/id tdc/e3, :name  "Slava", :age   37, :parent tdc/e2}]))]

    (testing "ground"
      (is (= (d/q '[:find ?vowel
                    :where [(ground [:a :e :i :o :u]) [?vowel ...]]])
             #{[:a] [:e] [:i] [:o] [:u]})))

    (testing "get-else"
      (is (= (d/q '[:find ?age ?height
                    :where [?e :age ?age]
                    [(get-else $ ?e :height 300) ?height]] db)
             #{[15 300] [22 240] [37 300]}))

      (is (thrown-with-msg? ExceptionInfo #"get-else: nil default value is not supported"
                            (d/q '[:find ?e ?height
                                   :where [?e :age]
                                   [(get-else $ ?e :height nil) ?height]] db))))

    (testing "get-some"
      (is (= (d/q '[:find ?a ?v
                    :where [?e :name _]
                    [(get-some $ ?e :height :age) [?a ?v]]] db)
             #{[:age 15]
               [:height 240]
               [:age 37]})))

    (testing "missing?"
      (is (= (d/q '[:find ?age
                    :in $
                    :where [?e :age ?age]
                    [(missing? $ ?e :height)]] db)
             #{[15] [37]})))

    (testing "missing? back-ref"
      (is (= (d/q '[:find ?age
                    :in $
                    :where [?e :age ?age]
                    [(missing? $ ?e :_parent)]] db)
             #{[37]})))

    (testing "Built-ins"
      (is (= (d/q '[:find  ?a1 ?a2
                    :where [?e1 :age ?a1]
                    [?e2 :age ?a2]
                    [(< ?a1 18 ?a2)]] db)
             #{[15 22] [15 37]}))

      (is (= (d/q '[:find  ?x ?c
                    :in    [?x ...]
                    :where [(count ?x) ?c]]
                  ["a" "abc"])
             #{["a" 1] ["abc" 3]})))

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
      (is (= (d/q '[:find  ?a
                    :in    $ ?adult
                    :where [?e :age ?a]
                    [(?adult ?a)]]
                  db
                  #(> % 18))
             #{[22] [37]})))

    (testing "Calling a function"
      (is (= (d/q '[:find  ?a1 ?a2 ?a3
                    :where [?e1 :age ?a1]
                    [?e2 :age ?a2]
                    [?e3 :age ?a3]
                    [(+ ?a1 ?a2) ?a12]
                    [(= ?a12 ?a3)]]
                  db)
             #{[15 22 37] [22 15 37]})))

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

(deftest test-predicates
  (let [entities [{:db/id tdc/e1 :name "Ivan" :age 10}
                  {:db/id tdc/e2 :name "Ivan" :age 20}
                  {:db/id tdc/e3 :name "Oleg" :age 30}
                  {:db/id tdc/e4 :name "Oleg" :age 40}]
        db (d/db-with (d/empty-db) entities)]
    (are [q res] (= (d/q q db) res)
      ;; plain predicate
      '[:find  ?a
        :where [?e :age ?a]
        [(> ?a 20)]]
      #{[30] [40]}

      ;; join in predicate
      '[:find  ?e ?e2
        :where [?e  :name]
        [?e2 :name]
        [(< ?e ?e2)]]
      #{[tdc/e1 tdc/e2] [tdc/e1 tdc/e3] [tdc/e1 tdc/e4] [tdc/e2 tdc/e3] [tdc/e2 tdc/e4] [tdc/e3 tdc/e4]}

      ;; join with extra symbols
      '[:find  ?a ?a2
        :where [?e  :age ?a]
        [?e2 :age ?a2]
        [(< ?e ?e2)]]
      #{[10 20] [20 30] [30 40] [10 30] [10 40] [20 40]}

      ;; empty result
      '[:find  ?e ?e2
        :where [?e  :name "Ivan"]
        [?e2 :name "Oleg"]
        [(= ?e ?e2)]]
      #{}

      ;; pred over const, true
      '[:find  ?a
        :where [?e :name "Ivan"]
        [?e :age ?a]
        [(= ?a 20)]]
      #{[20]}

      ;; pred over const, false
      '[:find  ?e
        :where [?e :name "Ivan"]
        [?e :age ?a]
        [(= ?a 21)]]
      #{})
    (let [pred (fn [db e a]
                 (= a (:age (d/entity db e))))]
      (is (= (d/q '[:find ?e
                    :in $ ?pred
                    :where [?e :age ?a]
                    [(?pred $ ?e 10)]]
                  db pred)
             #{[tdc/e1]})))))

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
              (d/db-with (d/empty-db) [[:db/add tdc/e1 :age 20]])))))

(defn sample-query-fn [] 42)

#?(:clj
   (deftest test-symbol-resolution
     (is (= 42 (d/q '[:find ?x .
                      :where [(datahike.test.query-fns/sample-query-fn) ?x]])))))
