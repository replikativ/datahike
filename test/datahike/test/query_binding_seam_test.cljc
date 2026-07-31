(ns datahike.test.query-binding-seam-test
  "ONE law, asserted at every site that binds a variable:

     Binding a variable that is already bound is UNIFICATION, not assignment.

   An occurrence of a bound variable is an equality obligation. This is
   Datomic's semantics, and datahike already implements it for a variable
   repeated *inside* one clause (#912/#913) and for ordinary data patterns —
   `[?e :name ?v] [?e :nick ?v]` correctly selects the entities whose nick
   equals their name. It was the clause forms that BIND a value which each
   invented their own rule:

     get-else          planner ignored the obligation, base engine overwrote
     tuple binding     both overwrote
     repeated head var both produced nil
     :in constant      planner ignored the obligation

   Four bugs, one law. The tests are grouped by site rather than by engine so
   that a future site is obviously missing from the list, and each asserts an
   ABSOLUTE expected value as well as engine agreement — two engines agreeing
   on a wrong answer is exactly how these survived."
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing]])
   [datahike.api :as d]
   [datahike.query :as q]))

(defn- seam-db []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write}]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one}
                        {:db/ident :nick :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one}
                        {:db/ident :score :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/one}])
      ;; carol's nick EQUALS her name: the one row a unifying engine keeps
      (d/transact conn [{:db/id 1 :name "alice" :nick "al"    :score 20}
                        {:db/id 2 :name "carol" :nick "carol" :score 30}
                        {:db/id 3 :name "dave"                :score 10}])
      (d/db conn))))

(defn- planner [query db & args]
  (binding [q/*disable-planner* false q/*query-result-cache?* false]
    (set (apply d/q query db args))))

(defn- base [query db & args]
  (binding [q/*disable-planner* true q/*query-result-cache?* false]
    (set (apply d/q query db args))))

(defn- both [expected query db & args]
  (is (= expected (apply base query db args)) "base engine")
  (is (= expected (apply planner query db args)) "planner"))

(deftest pattern-occurrence-is-an-obligation
  (testing "the case that was ALREADY correct — the reference for the rest"
    (let [db (seam-db)]
      (both #{[2 "carol"]}
            '[:find ?e ?v :where [?e :name ?v] [?e :nick ?v]] db))))

(deftest get-else-output-unifies
  (testing "a get-else writing into a bound var constrains it"
    (let [db (seam-db)]
      ;; only carol's nick equals her name; dave has no nick, and the default
      ;; "zzz" is not his name either
      (both #{[2 "carol"]}
            '[:find ?e ?v :where [?e :name ?v] [(get-else $ ?e :nick "zzz") ?v]] db)
      ;; …and the default DOES satisfy the obligation when it matches: dave
      ;; has no nick so the default "dave" is compared, and carol still
      ;; qualifies on her real nick
      (both #{[2 "carol"] [3 "dave"]}
            '[:find ?e ?v :where [?e :name ?v] [(get-else $ ?e :nick "dave") ?v]] db))))

(deftest get-else-output-unifies-with-an-in-constant
  (testing "the bound value may come from :in rather than a pattern"
    (let [db (seam-db)]
      (both #{[2 "carol"]}
            '[:find ?e ?v :in $ ?v :where [?e :name _] [(get-else $ ?e :nick "zzz") ?v]]
            db "carol"))))

(deftest function-output-unifies
  (testing "a plain function output writing into a bound var constrains it"
    (let [db (seam-db)]
      (both #{[3 10]}
            '[:find ?e ?s :where [?e :score ?s] [(+ 5 5) ?s]] db))))

(deftest tuple-binding-unifies
  (testing "each slot of a tuple binding is its own obligation"
    (let [db (seam-db)]
      ;; [?n 10] against a bound ?n and a bound ?s: only the entity whose
      ;; score is 10 survives, and ?n keeps the value the database holds
      (both #{[3 "dave" 10]}
            '[:find ?e ?n ?s :where
              [?e :name ?n] [?e :score ?s] [(vector ?n 10) [?n ?s]]] db))))

(deftest collection-binding-unifies
  (testing "a collection binding filters rather than overwrites"
    (let [db (seam-db)]
      (both #{[1 20] [3 10]}
            '[:find ?e ?s :where
              [?e :score ?s] [(identity [10 20]) [?s ...]]] db))))

(deftest repeated-rule-head-var-unifies
  (testing "a variable repeated in a rule HEAD binds one value, not nil"
    (let [db (seam-db)]
      (both #{[20 20] [30 30] [10 10]}
            '[:find ?x ?y :in $ % :where (same ?x ?y)]
            db '[[(same ?a ?a) [?e :score ?a]]]))))
