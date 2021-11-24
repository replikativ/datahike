(ns datahike.test.attribute_refs.query-rules
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj [clojure.test :as t :refer [is deftest testing]])
   [datahike.test.attribute-refs.util :refer [ref-db ref-e0
                                              wrap-direct-datoms
                                              wrap-ref-datoms
                                              shift-in]]
   [datahike.api :as d]))

(deftest test-rules
  (let [datoms [[1 :follow 2]
                [2 :follow 3]
                [2 :follow 4]
                [3 :follow 4]
                [4 :follow 6]
                [5 :follow 3]]
        db (d/db-with ref-db (wrap-ref-datoms ref-db ref-e0 :db/add datoms))
        all-pairs (shift-in #{[1 2] [2 3] [2 4] [3 4] [4 6] [5 3]} [0 1] ref-e0)
        all-pairs-reversed (shift-in #{[2 1] [3 2] [4 2] [4 3] [6 4] [3 5]} [0 1] ref-e0)]
    (is (= all-pairs
           (d/q '[:find  ?e1 ?e2
                  :in    $ %
                  :where (follow ?e1 ?e2)]
                db
                '[[(follow ?x ?y)
                   [?x :follow ?y]]])))

    (testing "Joining regular clauses with rule"
      (is (= (set (filter (fn [[_ x]] (and (even? x) ((set (map first all-pairs-reversed)) x)))
                          all-pairs-reversed))
             (d/q '[:find ?y ?x
                    :in $ %
                    :where [_ _ ?x]
                    (rule ?x ?y)
                    [(even? ?x)]]
                  db
                  '[[(rule ?a ?b)
                     [?a :follow ?b]]]))))

    (testing "Rule context is isolated from outer context"
      (is (= (shift-in #{[2] [3] [4] [6]} [0] ref-e0)
             (d/q '[:find ?x
                    :in $ %
                    :where [?e _ _]
                    (rule ?x)]
                  db
                  '[[(rule ?e)
                     [_ :follow ?e]]]))))

    (testing "Rule with branches"
      (is (= (shift-in #{[2] [3] [4]} [0] ref-e0)
             (d/q '[:find  ?e2
                    :in    $ ?e1 %
                    :where (follow ?e1 ?e2)]
                  db
                  (+ 1 ref-e0)
                  '[[(follow ?e2 ?e1)
                     [?e2 :follow ?e1]]
                    [(follow ?e2 ?e1)
                     [?e2 :follow ?t]
                     [?t  :follow ?e1]]]))))

    (testing "Recursive rules"
      (is (= (shift-in #{[2] [3] [4] [6]} [0] ref-e0)
             (d/q '[:find  ?e2
                    :in    $ ?e1 %
                    :where (follow ?e1 ?e2)]
                  db
                  (+ 1 ref-e0)
                  '[[(follow ?e1 ?e2)
                     [?e1 :follow ?e2]]
                    [(follow ?e1 ?e2)
                     [?e1 :follow ?t]
                     (follow ?t ?e2)]])))

      (is (= (shift-in #{[1 2] [2 3] [2 1] [3 2]} [0 1] ref-e0)
             (d/q '[:find ?e1 ?e2
                    :in $ %
                    :where (follow ?e1 ?e2)]
                  (d/db-with ref-db (wrap-ref-datoms ref-db ref-e0 :db/add
                                                     [[1 :follow 2] [2 :follow 3]]))
                  '[[(follow ?e1 ?e2)
                     [?e1 :follow ?e2]]
                    [(follow ?e1 ?e2)
                     (follow ?e2 ?e1)]])))

      (is (= (shift-in #{[1 2] [2 3] [3 1] [2 1] [3 2] [1 3]} [0 1] ref-e0)
             (d/q '[:find ?e1 ?e2
                    :in $ %
                    :where (follow ?e1 ?e2)]
                  (d/db-with ref-db (wrap-ref-datoms ref-db ref-e0 :db/add
                                                     [[1 :follow 2] [2 :follow 3] [3 :follow 1]]))
                  '[[(follow ?e1 ?e2)
                     [?e1 :follow ?e2]]
                    [(follow ?e1 ?e2)
                     (follow ?e2 ?e1)]]))))

    (testing "Mutually recursive rules"
      (let [datoms [[0 :f1 1]
                    [1 :f2 2]
                    [2 :f1 3]
                    [3 :f2 4]
                    [4 :f1 5]
                    [5 :f2 6]]
            db2 (d/db-with ref-db (wrap-ref-datoms ref-db ref-e0 :db/add datoms))]
        (is (= (shift-in #{[0 1] [0 3] [0 5]
                           [1 3] [1 5]
                           [2 3] [2 5]
                           [3 5]
                           [4 5]} [0 1] ref-e0)
               (d/q '[:find  ?e1 ?e2
                      :in    $ %
                      :where (f1 ?e1 ?e2)]
                    db2
                    '[[(f1 ?e1 ?e2)
                       [?e1 :f1 ?e2]]
                      [(f1 ?e1 ?e2)
                       [?t :f1 ?e2]
                       (f2 ?e1 ?t)]
                      [(f2 ?e1 ?e2)
                       [?e1 :f2 ?e2]]
                      [(f2 ?e1 ?e2)
                       [?t :f2 ?e2]
                       (f1 ?e1 ?t)]])))))

    (testing "Passing ins to rule"
      (is (= (set (filter (fn [[x y]] (and (even? x) (even? y)))
                          all-pairs))
             (d/q '[:find ?x ?y
                    :in $ % ?even
                    :where
                    (match ?even ?x ?y)]
                  db
                  '[[(match ?pred ?e ?e2)
                     [?e :follow ?e2]
                     [(?pred ?e)]
                     [(?pred ?e2)]]]
                  even?))))

    (testing "Using built-ins inside rule"
      (is (= (set (filter (fn [[x y]] (and (even? x) (even? y)))
                          all-pairs))
             (d/q '[:find ?x ?y
                    :in $ %
                    :where (match ?x ?y)]
                  db
                  '[[(match ?e ?e2)
                     [?e :follow ?e2]
                     [(even? ?e)]
                     [(even? ?e2)]]]))))
    (testing "Calling rule twice (#44)"
      (d/q '[:find ?p
             :in $ % ?fn
             :where (rule ?p ?fn :a)
             (rule ?p ?fn :b)]
           (d/db-with ref-db (wrap-direct-datoms ref-db ref-e0 :db/add [[1 :attr :a]]))
           '[[(rule ?p ?fn ?x)
              [?p :attr ?x]
              [(?fn ?x)]]]
           (constantly true)))))

(deftest test-false-arguments
  (let [db    (d/db-with ref-db
                         (wrap-direct-datoms ref-db ref-e0 :db/add [[1 :huh? true]
                                                                    [2 :huh? false]]))
        rules '[[(is ?id ?val)
                 [?id :huh? ?val]]]]
    (is (= (shift-in #{[1]} [0] ref-e0)
           (d/q '[:find ?id :in $ %
                  :where (is ?id true)]
                db rules)))
    (is (= (shift-in #{[2]} [0] ref-e0)
           (d/q '[:find ?id :in $ %
                  :where (is ?id false)]
                db rules)))))
