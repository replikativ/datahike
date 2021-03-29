(ns datahike.test.attribute-refs.query
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj [clojure.test :as t :refer [is deftest testing]])
   [datahike.test.attribute-refs.util :refer [ref-db ref-e0
                                              shift-entities
                                              wrap-direct-datoms]]
   [datahike.api :as d])
  #?(:clj
     (:import [clojure.lang ExceptionInfo])))

(deftest test-joins
  (let [entities [{:db/id 1, :name "Ivan", :age 15}
                  {:db/id 2, :name "Petr", :age 37}
                  {:db/id 4, :age 15}]
        db (d/db-with ref-db (shift-entities ref-e0 entities))]
    (is (= (d/q '[:find ?e
                  :where [?e :name]] db)
           #{[(+ ref-e0 1)] [(+ ref-e0 2)]}))
    (is (= (d/q '[:find ?v
                  :where [?e :name "Ivan"]
                  [?e :age ?v]] db)
           #{[15]}))
    (is (= (d/q '[:find ?e1 ?e2
                  :where [?e1 :name ?n]
                  [?e2 :name ?n]] db)
           #{[(+ ref-e0 1) (+ ref-e0 1)] [(+ ref-e0 2) (+ ref-e0 2)]}))
    (is (= (d/q '[:find ?e ?e2 ?n
                  :where [?e :name "Ivan"]
                  [?e :age ?a]
                  [?e2 :age ?a]
                  [?e2 :name ?n]] db)
           #{[(+ ref-e0 1) (+ ref-e0 1) "Ivan"]}))))

(deftest test-q-many
  (let [datoms [[1 :name "Ivan"]
                [1 :aka "ivolga"]
                [1 :aka "pi"]
                [2 :name "Petr"]
                [2 :aka "porosenok"]
                [2 :aka "pi"]]
        db (d/db-with ref-db (wrap-direct-datoms ref-db ref-e0 :db/add datoms))]
    (is (= (d/q '[:find ?n1 ?n2
                  :where [?e1 :aka ?x]
                  [?e2 :aka ?x]
                  [?e1 :name ?n1]
                  [?e2 :name ?n2]] db)
           #{["Ivan" "Ivan"]
             ["Petr" "Petr"]
             ["Ivan" "Petr"]
             ["Petr" "Ivan"]}))))

(deftest test-q-in
  (let [entities [{:db/id 1, :name "Ivan", :age 15}
                  {:db/id 2, :name "Petr", :age 37}]
        db (d/db-with ref-db (shift-entities ref-e0 entities))
        query '{:find [?e]
                :in [$ ?attr ?value]
                :where [[?e ?attr ?value]]}
        ref (fn [key] (get-in db [:ident-ref-map key]))]
    (is (= (d/q query db (ref :name) "Ivan")
           #{[(+ ref-e0 1)]}))
    (is (= (d/q query db (ref :age) 37)
           #{[(+ ref-e0 2)]}))

    (testing "DB join with collection"
      (is (= (d/q '[:find ?e ?email
                    :in $ $b
                    :where [?e :name ?n]
                    [$b ?n ?email]]
                  db
                  [["Ivan" "ivan@mail.ru"]
                   ["Petr" "petr@gmail.com"]])
             #{[(+ ref-e0 1) "ivan@mail.ru"]
               [(+ ref-e0 2) "petr@gmail.com"]})))

    (testing "Query without DB"
      (is (= (d/q '[:find ?a ?b
                    :in ?a ?b]
                  10 20)
             #{[10 20]})))))

(deftest test-bindings
  (let [entities [{:db/id 1, :name "Ivan", :age 15}
                  {:db/id 2, :name "Petr", :age 37}]
        db (d/db-with ref-db (shift-entities ref-e0 entities))]
    (testing "Relation binding"
      (is (= (d/q '[:find ?e ?email
                    :in $ [[?n ?email]]
                    :where [?e :name ?n]]
                  db
                  [["Ivan" "ivan@mail.ru"]
                   ["Petr" "petr@gmail.com"]])
             #{[(+ ref-e0 1) "ivan@mail.ru"]
               [(+ ref-e0 2) "petr@gmail.com"]})))

    (testing "Tuple binding"
      (is (= (d/q '[:find ?e
                    :in $ [?name ?age]
                    :where [?e :name ?name]
                    [?e :age ?age]]
                  db ["Petr" 37])
             #{[(+ ref-e0 2)]})))

    (testing "Collection binding"
      (is (= (d/q '[:find ?attr ?value
                    :in $ ?e [?attr ...]
                    :where [?e ?r ?value]
                    [?r :db/ident ?attr]]
                  db (+ ref-e0 1) [:name :age])
             #{[:name "Ivan"] [:age 15]})))

    #_(testing "Collection binding with direct attribute"   ;; TODO: make work in next query engine version
        (is (= (d/q '[:find ?attr ?value
                      :in $ ?e [?attr ...]
                      :where [?e ?attr ?value]]
                    db (+ ref-e0 1) [:name :age])
               #{[:name "Ivan"] [:age 15]}))))

  (testing "Placeholders"
    (is (= (d/q '[:find ?x ?z
                  :in [?x _ ?z]]
                [:x :y :z])
           #{[:x :z]}))
    (is (= (d/q '[:find ?x ?z
                  :in [[?x _ ?z]]]
                [[:x :y :z] [:a :b :c]])
           #{[:x :z] [:a :c]})))

  (testing "Error reporting"
    (is (thrown-with-msg? ExceptionInfo #"Cannot bind value :a to tuple \[\?a \?b\]"
                          (d/q '[:find ?a ?b :in [?a ?b]] :a)))
    (is (thrown-with-msg? ExceptionInfo #"Cannot bind value :a to collection \[\?a \.\.\.\]"
                          (d/q '[:find ?a :in [?a ...]] :a)))
    (is (thrown-with-msg? ExceptionInfo #"Not enough elements in a collection \[:a\] to bind tuple \[\?a \?b\]"
                          (d/q '[:find ?a ?b :in [?a ?b]] [:a])))))

(deftest test-nested-bindings
  (is (= (d/q '[:find ?k ?v
                :in [[?k ?v] ...]
                :where [(> ?v 1)]]
              {:a 1, :b 2, :c 3})
         #{[:b 2] [:c 3]}))

  (is (= (d/q '[:find ?k ?min ?max
                :in [[?k ?v] ...] ?minmax
                :where [(?minmax ?v) [?min ?max]]
                [(> ?max ?min)]]
              {:a [1 2 3 4]
               :b [5 6 7]
               :c [3]}
              #(vector (reduce min %) (reduce max %)))
         #{[:a 1 4] [:b 5 7]}))

  (is (= (d/q '[:find ?k ?x
                :in [[?k [?min ?max]] ...] ?range
                :where [(?range ?min ?max) [?x ...]]
                [(even? ?x)]]
              {:a [1 7]
               :b [2 4]}
              range)
         #{[:a 2] [:a 4] [:a 6]
           [:b 2]})))

(deftest test-offset
  (let [entities [{:db/id 1, :name "Alice", :age 15}
                  {:db/id 2, :name "Bob", :age 37}
                  {:db/id 3, :name "Charlie", :age 37}]
        db (d/db-with ref-db (shift-entities ref-e0 entities))]
    (is (= (count (d/q {:query '[:find ?e :where [?e :name _]]
                        :args [db]}))
           3))
    (is (= (count (d/q {:query '[:find ?e :where [?e :name _]]
                        :args [db]
                        :offset 1
                        :limit 1}))
           1))
    (is (= (count (d/q {:query '[:find ?e :where [?e :name _]]
                        :args [db]
                        :limit 2}))
           2))
    (is (= (count (d/q {:query '[:find ?e :where [?e :name _]]
                        :args [db]
                        :offset 1
                        :limit 2}))
           2))
    (is (= (count (d/q {:query '[:find ?e :where [?e :name _]]
                        :args [db]
                        :offset 2
                        :limit 2}))
           1))
    (is (not (= (d/q {:query '[:find ?e :where [?e :name _]]
                      :args [db]
                      :limit 2})
                (d/q {:query '[:find ?e :where [?e :name _]]
                      :args [db]
                      :offset 1
                      :limit 2}))))
    (is (= (d/q {:query '[:find ?e :where [?e :name _]]
                 :args [db]
                 :offset 4})
           #{}))
    (is (= (d/q {:query '[:find ?e :where [?e :name _]]
                 :args [db]
                 :offset 10
                 :limit 5})
           #{}))
    (is (= (d/q {:query '[:find ?e :where [?e :name _]]
                 :args [db]
                 :offset 1
                 :limit 0})
           #{}))))

(deftest test-return-maps
  (let [entities [{:db/id 1, :name "Alice", :age 15}
                  {:db/id 2, :name "Bob", :age 37}
                  {:db/id 3, :name "Charlie", :age 37}]
        db (d/db-with ref-db (shift-entities ref-e0 entities))]
    (testing "returns map"
      (is (map? (first (d/q {:query '[:find ?e :keys name :where [?e :name _]]
                             :args [db]})))))
    (testing "returns set without return-map"
      (is (= #{["Charlie"] ["Alice"] ["Bob"]}
             (d/q {:query '[:find ?name :where [_ :name ?name]]
                   :args [db]}))))
    (testing "returns map with key return-map"
      (is (= #{{:foo (+ ref-e0 2)} {:foo (+ ref-e0 3)} {:foo (+ ref-e0 1)}}
             (set (d/q {:query '[:find ?e :keys foo :where [?e :name _]]
                        :args [db]})))))
    (testing "returns map with string return-map"
      (is (= [{"foo" "Charlie"} {"foo" "Alice"} {"foo" "Bob"}]
             (d/q {:query '[:find ?name :strs foo :where [?e :name ?name]]
                   :args [db]}))))
    (testing "return map with keys using multiple find vars"
      (is (= #{["Bob" {:age 37 :db/id  (+ ref-e0 2)}]
               ["Charlie" {:age 37 :db/id  (+ ref-e0 3)}]
               ["Alice" {:age 15 :db/id  (+ ref-e0 1)}]}
             (into #{} (d/q {:find '[?name (pull ?e ?p)]
                             :args [db '[:age :db/id]]
                             :in '[$ ?p]
                             :where '[[?e :name ?name]]})))))))
