(ns datahike.test.attribute_refs.query-fns-test
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [datahike.core :as d]
   [datahike.test.attribute-refs.utils :refer [ref-db ref-e0 shift-entities]])
  #?(:clj
     (:import [clojure.lang ExceptionInfo])))

(deftest test-query-fns
  (let [entities [{:db/id 1, :name  "Ivan",  :age   15}
                  {:db/id 2, :name  "Petr",  :age   22, :height 240, :parent (+ ref-e0 1)}
                  {:db/id 3, :name  "Slava", :age   37, :parent (+ ref-e0 2)}]
        db (d/db-with ref-db (shift-entities ref-e0 entities))]

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
      (is (= #{[:age 15]
               [:height 240]
               [:age 37]}
             (d/q '[:find ?a ?v
                    :where [?e :name _]
                    [(get-some $ ?e :height :age) [?a ?v]]] db))))

    (testing "missing?"
      (is (= #{[15] [37]}
             (d/q '[:find ?age
                    :in $
                    :where [?e :age ?age]
                    [(missing? $ ?e :height)]] db))))

    (testing "missing? back-ref"
      (is (= #{[(+ ref-e0 3)]}
             (d/q '[:find ?e
                    :in $
                    :where [?e :age ?age]
                    [(missing? $ ?e :_parent)]] db))))

    (testing "Built-ins"
      (is (= #{[(+ ref-e0 1) (+ ref-e0 2)] [(+ ref-e0 1) (+ ref-e0 3)]}
             (d/q '[:find  ?e1 ?e2
                    :where [?e1 :age ?a1]
                    [?e2 :age ?a2]
                    [(< ?a1 18 ?a2)]] db))))

    (testing "Passing predicate as source"
      (is (=  #{[(+ ref-e0 2)] [(+ ref-e0 3)]}
              (d/q '[:find  ?e
                     :in    $ ?adult
                     :where [?e :age ?a]
                     [(?adult ?a)]]
                   db
                   #(> % 18)))))

    (testing "Calling a function"
      (is (=  #{[(+ ref-e0 1) (+ ref-e0 2) (+ ref-e0 3)] [(+ ref-e0 2) (+ ref-e0 1) (+ ref-e0 3)]}
              (d/q '[:find  ?e1 ?e2 ?e3
                     :where [?e1 :age ?a1]
                     [?e2 :age ?a2]
                     [?e3 :age ?a3]
                     [(+ ?a1 ?a2) ?a12]
                     [(= ?a12 ?a3)]]
                   db))))

    (testing "Two conflicting function values for one binding."
      (is (= #{}
             (d/q '[:find  ?n
                    :where [(identity 1) ?n]
                    [(identity 2) ?n]]
                  db))))

    (testing "Destructured conflicting function values for two bindings."
      (is (= #{}
             (d/q '[:find  ?n ?x
                    :where [(identity [3 4]) [?n ?x]]
                    [(identity [1 2]) [?n ?x]]]
                  db))))

    (testing "Rule bindings interacting with function binding. (fn, rule)"
      (is (= #{[2]}
             (d/q '[:find  ?n
                    :in $ %
                    :where [(identity 2) ?n]
                    (my-vals ?n)]
                  db
                  '[[(my-vals ?x)
                     [(identity 1) ?x]]
                    [(my-vals ?x)
                     [(identity 2) ?x]]
                    [(my-vals ?x)
                     [(identity 3) ?x]]]))))

    (testing "Rule bindings interacting with function binding. (rule, fn)"
      (is (= #{[2]}
             (d/q '[:find  ?n
                    :in $ %
                    :where (my-vals ?n)
                    [(identity 2) ?n]]
                  db
                  '[[(my-vals ?x)
                     [(identity 1) ?x]]
                    [(my-vals ?x)
                     [(identity 2) ?x]]
                    [(my-vals ?x)
                     [(identity 3) ?x]]]))))

    (testing "Conflicting relational bindings with function binding. (rel, fn)"
      (is (= #{}
             (d/q '[:find  ?age
                    :where [_ :age ?age]
                    [(identity 100) ?age]]
                  db))))

    (testing "Conflicting relational bindings with function binding. (fn, rel)"
      (is (= #{}
             (d/q '[:find  ?age
                    :where [(identity 100) ?age]
                    [_ :age ?age]]
                  db))))))

