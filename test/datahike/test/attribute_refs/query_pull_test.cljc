(ns datahike.test.attribute-refs.query-pull-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing are]]
      :clj [clojure.test :as t :refer [is deftest testing are]])
   [datahike.test.attribute-refs.utils :refer [ref-db ref-e0
                                               shift-entities
                                               shift-in]]
   [datahike.api :as d]))

(def test-db (d/db-with ref-db
                        (shift-entities ref-e0 [{:db/id 1 :name "Petr" :age 44}
                                                {:db/id 2 :name "Ivan" :age 25}
                                                {:db/id 3 :name "Oleg" :age 11}])))

(deftest test-basics
  (are [find res] (= res
                     (set (d/q {:find find
                                :where '[[?e :age ?a]
                                         [(>= ?a 18)]]}
                               test-db)))
    '[(pull ?e [:name])]
    #{[{:name "Ivan"}] [{:name "Petr"}]}

    '[(pull ?e [*])]
    #{(shift-entities ref-e0 [{:db/id 2 :age 25 :name "Ivan"}])
      (shift-entities ref-e0 [{:db/id 1 :age 44 :name "Petr"}])}

    '[?e (pull ?e [:name])]
    (shift-in #{[2 {:name "Ivan"}] [1 {:name "Petr"}]} [0] ref-e0)

    '[?e ?a (pull ?e [:name])]
    (shift-in #{[2 25 {:name "Ivan"}] [1 44 {:name "Petr"}]} [0] ref-e0)

    '[?e (pull ?e [:name]) ?a]
    (shift-in #{[2 {:name "Ivan"} 25] [1 {:name "Petr"} 44]} [0] ref-e0)))

(deftest test-var-pattern
  (are [find pattern res] (= res
                             (set (d/q {:find find
                                        :in   '[$ ?pattern]
                                        :where '[[?e :age ?a]
                                                 [(>= ?a 18)]]}
                                       test-db pattern)))
    '[(pull ?e ?pattern)] [:name]
    #{[{:name "Ivan"}] [{:name "Petr"}]}

    '[?e ?a ?pattern (pull ?e ?pattern)] [:name]
    (shift-in #{[2 25 [:name] {:name "Ivan"}] [1 44 [:name] {:name "Petr"}]} [0] ref-e0)))

;; not supported
#_(deftest test-multi-pattern
    (is (= (set (d/q '[:find ?e ?p (pull ?e ?p)
                       :in $ [?p ...]
                       :where [?e :age ?a]
                       [>= ?a 18]]
                     test-db [[:name] [:age]]))
           #{[2 [:name] {:name "Ivan"}]
             [2 [:age]  {:age 25}]
             [1 [:name] {:name "Petr"}]
             [1 [:age]  {:age 44}]})))

(deftest test-multiple-sources
  (let [db1 (d/db-with ref-db (shift-entities ref-e0 [{:db/id 1 :name "Ivan" :age 25}]))
        db2 (d/db-with ref-db (shift-entities ref-e0 [{:db/id 1 :name "Petr" :age 25}]))]
    (is (= (shift-in #{[1 {:name "Ivan"}]} [0] ref-e0)
           (set (d/q '[:find ?e (pull $1 ?e [:name])
                       :in $1 $2
                       :where [$1 ?e :age 25]]
                     db1 db2))))

    (is (=  (shift-in #{[1 {:name "Petr"}]} [0] ref-e0)
            (set (d/q '[:find ?e (pull $2 ?e [:name])
                        :in $1 $2
                        :where [$2 ?e :age 25]]
                      db1 db2))))

    (testing "$ is default source"
      (is (= (shift-in #{[1 {:name "Petr"}]} [0] ref-e0)
             (set (d/q '[:find ?e (pull ?e [:name])
                         :in $1 $
                         :where [$ ?e :age 25]]
                       db1 db2)))))))

(deftest test-find-spec
  (is (= {:name "Ivan"}
         (d/q '[:find (pull ?e [:name]) .
                :where [?e :age 25]]
              test-db)))

  (is (= #{{:name "Ivan"} {:name "Petr"} {:name "Oleg"}}
         (set (d/q '[:find [(pull ?e [:name]) ...]
                     :where [?e :age ?a]]
                   test-db))))

  (is (= [(+ ref-e0 2) {:name "Ivan"}]
         (d/q '[:find [?e (pull ?e [:name])]
                :where [?e :age 25]]
              test-db))))

(deftest test-find-spec-input
  (let [pattern '[*]]
    (is (= (d/pull test-db pattern 1)
           (d/q '[:find (pull ?e ?p) .
                  :in $ ?p
                  :where [(ground 1) ?e]]
                test-db pattern)))))

(deftest test-aggregates
  (let [db (d/db-with ref-db
                      (shift-entities ref-e0 [{:db/id 1 :name "Petr" :value [10 20 30 40]}
                                              {:db/id 2 :name "Ivan" :value [14 16]}
                                              {:db/id 3 :name "Oleg" :value 1}]))]
    (is (= (shift-in #{[1 {:name "Petr"} 10 40]
                       [2 {:name "Ivan"} 14 16]
                       [3 {:name "Oleg"} 1 1]} [0] ref-e0)
           (set (d/q '[:find ?e (pull ?e [:name]) (min ?v) (max ?v)
                       :where [?e :value ?v]]
                     db))))))

(deftest test-lookup-refs
  (let [db (d/db-with ref-db
                      (shift-entities ref-e0 [{:db/id 1 :name "Petr" :age 44}
                                              {:db/id 2 :name "Ivan" :age 25}
                                              {:db/id 3 :name "Oleg" :age 11}]))]
    (is (=  #{[[:name "Petr"] 44 {:db/id (+ ref-e0 1) :name "Petr"}]
              [[:name "Ivan"] 25 {:db/id (+ ref-e0 2) :name "Ivan"}]}
            (set (d/q '[:find ?ref ?a (pull ?ref [:db/id :name])
                        :in   $ [?ref ...]
                        :where [?ref :age ?a]
                        [(>= ?a 18)]]
                      db [[:name "Ivan"] [:name "Oleg"] [:name "Petr"]]))))))
