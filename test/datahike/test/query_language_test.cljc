(ns datahike.test.query-language-test
  (:require
   #?(:clj [clojure.test :refer [deftest is testing]]
      :cljs [cljs.test :refer-macros [deftest is testing]])
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.query :as q]
   [datahike.query.resolve :as qr]))

(defn- query-db []
  (d/db-with (db/empty-db)
             [{:db/id 1 :id "a4" :flag false}
              {:db/id 2 :id "b5" :flag true}
              {:db/id 3 :id "c4" :flag false}]))

(deftest blank-rule-arguments
  (let [db (d/db-with (db/empty-db {:from {:db/valueType :db.type/ref}
                                    :to {:db/valueType :db.type/ref}}
                                   {:keep-history? true})
                      [{:db/id 10 :id "a"} {:db/id 11 :id "b"} {:db/id 12 :id "c"}
                       {:db/id 20 :from 10 :to 11 :type "related"}
                       {:db/id 21 :from 11 :to 12 :type "broader"}])
        rules '[[(forward ?f ?type ?to ?r)
                 [(ground ["broader" "related"]) [?type ...]]
                 [?r :from ?f] [?r :type ?type] [?r :to ?to]]
                [(reverse-edge ?f ?type ?to ?r)
                 [(ground {"narrower" "broader" "related" "related"}) [[?type ?rev]]]
                 [?r :to ?f] [?r :type ?rev] [?r :from ?to]]
                [(edge ?f ?type ?to ?r)
                 (or (forward ?f ?type ?to ?r) (reverse-edge ?f ?type ?to ?r))]
                [(any-edge ?f ?r) (edge ?f _ _ ?r)]
                [(reach ?f ?to) [?r :from ?f] [?r :to ?to]]
                [(reach ?f ?to) [?r :from ?f] [?r :to ?mid] (reach ?mid ?to)]
                [(same ?x ?x) [?x :id _]]]]
    (doseq [disable-planner? [true false]
            source [db (d/history db)]]
      (binding [q/*disable-planner* disable-planner?
                q/*query-result-cache?* false]
        (testing "#1024: a rule containing or, with relation :in and a blank argument"
          (doseq [[input expected] [[[[10 20]] #{[10 #{"related"}]}]
                                    [[[11 21]] #{[11 #{"broader"}]}]
                                    [[[1 2]] #{}]
                                    [[] #{}]]]
            (is (= expected
                   (set (d/q '[:find ?c (distinct ?type) :in $ % [[?c ?r]]
                               :where (edge ?c ?type _ ?r)] source rules input))))))
        (testing "multiple blanks in a nested rule call are independent"
          (is (= #{[10 20] [11 20] [11 21] [12 21]}
                 (d/q '[:find ?c ?r :in $ % :where (any-edge ?c ?r)] source rules))))
        (testing "blank arguments work inside outer disjunctions and recursive rules"
          (is (= #{[10] [11]}
                 (d/q '[:find ?c :in $ % :where
                        (or (reach ?c _) (forward ?c "related" _ _))] source rules)))
          (is (= #{[10] [11] [12]}
                 (d/q '[:find ?c :in $ % :where (same _ ?c)] source rules))))
        (testing "a blank in a negated rule remains local"
          (is (= #{[12]}
                 (d/q '[:find ?c :in $ % :where [?c :id _]
                        (not (reach ?c _))] source rules))))))))

(deftest constant-function-outputs
  (doseq [disable-planner? [true false]]
    (binding [q/*disable-planner* disable-planner?
              q/*query-result-cache?* false]
      (let [db (query-db)]
        (is (= #{[1] [3]}
               (d/q '[:find ?e :where [?e :id ?id] [(subs ?id 1 2) "4"]] db)))
        (is (= #{[1] [3]}
               (d/q '[:find ?e :where [?e :flag ?f] [(identity ?f) false]] db)))
        (testing "fresh variables do not collide with user variables"
          (is (= #{[1] [3]}
                 (d/q '[:find ?__fn_result__1 :where
                        [?__fn_result__1 :id ?id] [(subs ?id 1 2) "4"]] db))))
        (testing "constants preserve disjunction and negation scopes"
          (is (= #{[1] [3]}
                 (d/q '[:find ?e :where [?e :id ?id]
                        (or [(subs ?id 0 1) "a"] [(subs ?id 0 1) "c"])] db)))
          (is (= #{[2]}
                 (d/q '[:find ?e :where [?e :id ?id]
                        (not [(subs ?id 1 2) "4"])] db)))
          (is (= #{[2]}
                 (d/q '[:find ?e :where [?e :id ?id]
                        (not-join [?id] [(subs ?id 1 2) "4"])] db)))
          (is (= #{[1] [3]}
                 (d/q '[:find ?e :where [?e :id ?id]
                        (or-join [?id] [(subs ?id 0 1) "a"] [(subs ?id 0 1) "c"])] db))))
        (testing "rule bodies and subqueries use the same normalization"
          (is (= #{[1] [3]}
                 (d/q '[:find ?e :in $ % :where (four ?e)] db
                      '[[(four ?e) [?e :id ?id] [(subs ?id 1 2) "4"]]])))
          (is (= #{[2]}
                 (d/q '[:find ?n :where
                        [(q [:find (count ?e) :where
                             [?e :id ?id] [(subs ?id 1 2) "4"]] $) [[?n]]]] db))))
        (testing "nil function returns still discard rows"
          (is (= #{}
                 (d/q '[:find ?e :where [?e :id ?id] [(identity nil) nil]] db))))
        (testing "quoted collections are constants; vectors stay bindings"
          (is (= #{[1] [2] [3]}
                 (d/q '[:find ?e :where [?e :id _]
                        [(vector 1 2) (quote [1 2])]] db)))
          (is (= #{[1 2]}
                 (d/q '[:find ?a ?b :where [(tuple 1 2) [?a ?b]]]))))))))

(deftest eager-if-values
  (doseq [disable-planner? [true false]]
    (binding [q/*disable-planner* disable-planner?
              q/*query-result-cache?* false
              qr/*symbol-resolver* qr/safe-symbol-resolver]
      (is (= #{[1 0] [2 1] [3 0]}
             (d/q '[:find ?e ?v :where [?e :flag ?f] [(if ?f 1 0) ?v]] (query-db))))
      (is (= #{[0 :then] [false :else] [nil :else]}
             (d/q '[:find ?test ?v :in [?test ...] :where [(if ?test :then :else) ?v]]
                  [0 false nil])))
      (is (= #{[true 1]}
             (d/q '[:find ?test ?v :in [?test ...] :where [(if ?test 1) ?v]] [true false])))
      (is (= #{[false]}
             (d/q '[:find ?v :where [(if true false true) ?v]]))))))

#?(:clj
   (deftest safe-math-and-function-exceptions
     (doseq [disable-planner? [true false]]
       (binding [q/*disable-planner* disable-planner?
                 q/*query-result-cache?* false
                 qr/*symbol-resolver* qr/safe-symbol-resolver]
         (let [db (query-db)]
           (is (= #{[2 1.0 2.0 8.0 3.0]}
                  (d/q '[:find ?r ?f ?c ?p ?s :in $ :where
                         [(clojure.math/round 1.6) ?r]
                         [(clojure.math/floor 1.6) ?f]
                         [(clojure.math/ceil 1.6) ?c]
                         [(clojure.math/pow 2 3) ?p]
                         [(clojure.math/sqrt 9) ?s]] db)))
           (is (thrown? StringIndexOutOfBoundsException
                        (d/q '[:find ?v :where [?e :id ?id] [(subs ?id 7 8) ?v]] db)))
           (is (thrown-with-msg? clojure.lang.ExceptionInfo #"may not receive a database"
                                 (d/q '[:find ?v :where [(clojure.math/sqrt $) ?v]] db)))
           (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Nested expression"
                                 (d/q '[:find ?e :where [?e :id ?id]
                                        [(= (subs ?id 1 2) "4")]] db))))))))
