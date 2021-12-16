(ns datahike.test.query-rules-test
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer        [is deftest testing]])
   [datahike.api :as d]
   [datahike.db :as db]))

(deftest test-rules
  (let [db [[5 :follow 3]
            [1 :follow 2] [2 :follow 3] [3 :follow 4] [4 :follow 6]
            [2         :follow           4]]]
    (is (= (d/q '[:find  ?e1 ?e2
                  :in    $ %
                  :where (follow ?e1 ?e2)]
                db
                '[[(follow ?x ?y)
                   [?x :follow ?y]]])
           #{[1 2] [2 3] [3 4] [2 4] [5 3] [4 6]}))

    (testing "Joining regular clauses with rule"
      (is (= (d/q '[:find ?y ?x
                    :in $ %
                    :where [_ _ ?x]
                    (rule ?x ?y)
                    [(even? ?x)]]
                  db
                  '[[(rule ?a ?b)
                     [?a :follow ?b]]])
             #{[3 2] [6 4] [4 2]})))

    (testing "Rule context is isolated from outer context"
      (is (= (d/q '[:find ?x
                    :in $ %
                    :where [?e _ _]
                    (rule ?x)]
                  db
                  '[[(rule ?e)
                     [_ ?e _]]])
             #{[:follow]})))

    (testing "Rule with branches"
      (is (= (d/q '[:find  ?e2
                    :in    $ ?e1 %
                    :where (follow ?e1 ?e2)]
                  db
                  1
                  '[[(follow ?e2 ?e1)
                     [?e2 :follow ?e1]]
                    [(follow ?e2 ?e1)
                     [?e2 :follow ?t]
                     [?t  :follow ?e1]]])
             #{[2] [3] [4]})))

    (testing "Recursive rules"
      (is (= (d/q '[:find  ?e2
                    :in    $ ?e1 %
                    :where (follow ?e1 ?e2)]
                  db
                  1
                  '[[(follow ?e1 ?e2)
                     [?e1 :follow ?e2]]
                    [(follow ?e1 ?e2)
                     [?e1 :follow ?t]
                     (follow ?t ?e2)]])
             #{[2] [3] [4] [6]}))

      (is (= (d/q '[:find ?e1 ?e2
                    :in $ %
                    :where (follow ?e1 ?e2)]
                  [[1 :follow 2] [2 :follow 3]]
                  '[[(follow ?e1 ?e2)
                     [?e1 :follow ?e2]]
                    [(follow ?e1 ?e2)
                     (follow ?e2 ?e1)]])
             #{[1 2] [2 3] [2 1] [3 2]}))

      (is (= (d/q '[:find ?e1 ?e2
                    :in $ %
                    :where (follow ?e1 ?e2)]
                  [[1 :follow 2] [2 :follow 3] [3 :follow 1]]
                  '[[(follow ?e1 ?e2)
                     [?e1 :follow ?e2]]
                    [(follow ?e1 ?e2)
                     (follow ?e2 ?e1)]])
             #{[1 2] [2 3] [3 1] [2 1] [3 2] [1 3]})))

    (testing "Mutually recursive rules"
      (is (= (d/q '[:find  ?e1 ?e2
                    :in    $ %
                    :where (f1 ?e1 ?e2)]
                  [[0 :f1 1]
                   [1 :f2 2]
                   [2 :f1 3]
                   [3 :f2 4]
                   [4 :f1 5]
                   [5 :f2 6]]
                  '[[(f1 ?e1 ?e2)
                     [?e1 :f1 ?e2]]
                    [(f1 ?e1 ?e2)
                     [?t :f1 ?e2]
                     (f2 ?e1 ?t)]
                    [(f2 ?e1 ?e2)
                     [?e1 :f2 ?e2]]
                    [(f2 ?e1 ?e2)
                     [?t :f2 ?e2]
                     (f1 ?e1 ?t)]])
             #{[0 1] [0 3] [0 5]
               [1 3] [1 5]
               [2 3] [2 5]
               [3 5]
               [4 5]})))

    (testing "Passing ins to rule"
      (is (= (d/q '[:find ?x ?y
                    :in $ % ?even
                    :where
                    (match ?even ?x ?y)]
                  db
                  '[[(match ?pred ?e ?e2)
                     [?e :follow ?e2]
                     [(?pred ?e)]
                     [(?pred ?e2)]]]
                  even?)
             #{[4 6] [2 4]})))

    (testing "Using built-ins inside rule"
      (is (= (d/q '[:find ?x ?y
                    :in $ %
                    :where (match ?x ?y)]
                  db
                  '[[(match ?e ?e2)
                     [?e :follow ?e2]
                     [(even? ?e)]
                     [(even? ?e2)]]])
             #{[4 6] [2 4]})))
    (testing "Calling rule twice (#44)"
      (d/q '[:find ?p
             :in $ % ?fn
             :where (rule ?p ?fn "a")
             (rule ?p ?fn "b")]
           [[1 :attr "a"]]
           '[[(rule ?p ?fn ?x)
              [?p :attr ?x]
              [(?fn ?x)]]]
           (constantly true))))

  (testing "Specifying db to rule"
    (is (= (d/q '[:find ?n
                  :in   $sexes $ages %
                  :where ($sexes male ?n)
                  ($ages adult ?n)]
                [["Ivan" :male] ["Darya" :female] ["Oleg" :male] ["Igor" :male]]
                [["Ivan" 15] ["Oleg" 66] ["Darya" 32]]
                '[[(male ?x)
                   [?x :male]]
                  [(adult ?y)
                   [?y ?a]
                   [(>= ?a 18)]]])
           #{["Oleg"]}))))

;; https://github.com/tonsky/datahike/issues/218

(deftest test-false-arguments
  (let [db    (d/db-with (db/empty-db)
                         [[:db/add 1 :attr true]
                          [:db/add 2 :attr false]])
        rules '[[(is ?id ?val)
                 [?id :attr ?val]]]]
    (is (= (d/q '[:find ?id :in $ %
                  :where (is ?id true)]
                db rules)
           #{[1]}))
    (is (= (d/q '[:find ?id :in $ %
                  :where (is ?id false)] db rules)
           #{[2]}))))

(deftest test-rule-arguments
  (let [cfg {:store {:backend :mem
                     :id "rule-test"}
             :name "rule-test"
             :keep-history? true
             :schema-flexibility :write
             :attribute-refs? true}
        schema [{:db/ident       :name
                 :db/cardinality :db.cardinality/one
                 :db/index       true
                 :db/unique      :db.unique/identity
                 :db/valueType   :db.type/string}
                {:db/ident       :parents
                 :db/cardinality :db.cardinality/many
                 :db/valueType   :db.type/ref}
                {:db/ident       :age
                 :db/cardinality :db.cardinality/one
                 :db/valueType   :db.type/long}]
        rules '[[(parent-info ?child ?name ?age)
                 [?child :parents ?p]
                 [(ground ["Alice" "Bob"]) [?name ...]]
                 [?p :name ?name]
                 [?p :age ?age]]]
        _ (d/delete-database cfg)
        _ (d/create-database cfg)
        conn (d/connect cfg)]

    (d/transact conn {:tx-data schema})
    (d/transact conn {:tx-data [{:name "Alice"
                                 :age  25}
                                {:name "Bob"
                                 :age 30}]})
    (d/transact conn {:tx-data [{:name    "Charlie"
                                 :age     5
                                 :parents [[:name "Alice"]
                                           [:name "Bob"]]}]})

    (is (= #{[25]}
           (d/q {:query '{:find [?age]
                          :in [$ ?n ?pn %]
                          :where
                          [[?child :name ?n]
                           (parent-info ?child ?pn ?age)]}
                 :args [@conn "Charlie" "Alice" rules]})))

    (is (= #{[25]}
           (d/q {:query '{:find [?age]
                          :in [$ ?n [?pn ...] %]
                          :where
                          [[?child :name ?n]
                           (parent-info ?child ?pn ?age)]}
                 :args [@conn "Charlie" ["Alice"] rules]})))

    (is (= #{[25]}
           (d/q {:query '{:find [?age]
                          :in [$ ?n %]
                          :where
                          [[?child :name ?n]
                           (parent-info ?child "Alice" ?age)]}
                 :args [@conn "Charlie" rules]})))

    (is (thrown-msg? "Bad format for value in pattern, must be a scalar, nil or a vector of two elements."
                     (d/q {:query '{:find [?age]
                                    :in [$ ?n %]
                                    :where
                                    [[?child :name ?n]
                                     (parent-info ?child ["Alice"] ?age)]}
                           :args [@conn "Charlie" rules]})))))
