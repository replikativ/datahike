(ns datahike.test.query
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [datahike.core :as d]
   [datahike.query :as dq]
   [datahike.db :as db]
   [datahike.test.core :as tdc])
  #?(:clj
     (:import [clojure.lang ExceptionInfo])))

(deftest test-joins
  (let [db (-> (d/empty-db)
               (d/db-with [{:db/id 1, :name  "Ivan", :age   15}
                           {:db/id 2, :name  "Petr", :age   37}
                           {:db/id 3, :name  "Ivan", :age   37}
                           {:db/id 4, :age 15}]))]
    (is (= (d/q '[:find ?e
                  :where [?e :name]] db)
           #{[1] [2] [3]}))
    (is (= (d/q '[:find  ?e ?v
                  :where [?e :name "Ivan"]
                  [?e :age ?v]] db)
           #{[1 15] [3 37]}))
    (is (= (d/q '[:find  ?e1 ?e2
                  :where [?e1 :name ?n]
                  [?e2 :name ?n]] db)
           #{[1 1] [2 2] [3 3] [1 3] [3 1]}))
    (is (= (d/q '[:find  ?e ?e2 ?n
                  :where [?e :name "Ivan"]
                  [?e :age ?a]
                  [?e2 :age ?a]
                  [?e2 :name ?n]] db)
           #{[1 1 "Ivan"]
             [3 3 "Ivan"]
             [3 2 "Petr"]}))))

(deftest test-q-many
  (let [db (-> (d/empty-db {:aka {:db/cardinality :db.cardinality/many}})
               (d/db-with [[:db/add 1 :name "Ivan"]
                           [:db/add 1 :aka  "ivolga"]
                           [:db/add 1 :aka  "pi"]
                           [:db/add 2 :name "Petr"]
                           [:db/add 2 :aka  "porosenok"]
                           [:db/add 2 :aka  "pi"]]))]
    (is (= (d/q '[:find  ?n1 ?n2
                  :where [?e1 :aka ?x]
                  [?e2 :aka ?x]
                  [?e1 :name ?n1]
                  [?e2 :name ?n2]] db)
           #{["Ivan" "Ivan"]
             ["Petr" "Petr"]
             ["Ivan" "Petr"]
             ["Petr" "Ivan"]}))))

(deftest test-q-coll
  (let [db [[1 :name "Ivan"]
            [1 :age  19]
            [1 :aka  "dragon_killer_94"]
            [1 :aka  "-=autobot=-"]]]
    (is (= (d/q '[:find  ?n ?a
                  :where [?e :aka "dragon_killer_94"]
                  [?e :name ?n]
                  [?e :age  ?a]] db)
           #{["Ivan" 19]})))

  (testing "Query over long tuples"
    (let [db [[1 :name "Ivan" 945 :db/add]
              [1 :age  39     999 :db/retract]]]
      (is (= (d/q '[:find  ?e ?v
                    :where [?e :name ?v]] db)
             #{[1 "Ivan"]}))
      (is (= (d/q '[:find  ?e ?a ?v ?t
                    :where [?e ?a ?v ?t :db/retract]] db)
             #{[1 :age 39 999]})))))

(deftest test-q-in
  (let [db (-> (d/empty-db)
               (d/db-with [{:db/id 1, :name "Ivan", :age 15}
                           {:db/id 2, :name "Petr", :age 37}
                           {:db/id 3, :name "Ivan", :age 37}]))
        query '{:find  [?e]
                :in    [$ ?attr ?value]
                :where [[?e ?attr ?value]]}]
    (is (= (d/q query db :name "Ivan")
           #{[1] [3]}))
    (is (= (d/q query db :age 37)
           #{[2] [3]}))

    (testing "Named DB"
      (is (= (d/q '[:find  ?a ?v
                    :in    $db ?e
                    :where [$db ?e ?a ?v]] db 1)
             #{[:name "Ivan"]
               [:age 15]})))

    (testing "DB join with collection"
      (is (= (d/q '[:find  ?e ?email
                    :in    $ $b
                    :where [?e :name ?n]
                    [$b ?n ?email]]
                  db
                  [["Ivan" "ivan@mail.ru"]
                   ["Petr" "petr@gmail.com"]])
             #{[1 "ivan@mail.ru"]
               [2 "petr@gmail.com"]
               [3 "ivan@mail.ru"]})))

    (testing "Query without DB"
      (is (= (d/q '[:find ?a ?b
                    :in   ?a ?b]
                  10 20)
             #{[10 20]})))))

(deftest test-bindings
  (let [db (-> (d/empty-db)
               (d/db-with [{:db/id 1, :name "Ivan", :age 15}
                           {:db/id 2, :name "Petr", :age 37}
                           {:db/id 3, :name "Ivan", :age 37}]))]
    (testing "Relation binding"
      (is (= (d/q '[:find  ?e ?email
                    :in    $ [[?n ?email]]
                    :where [?e :name ?n]]
                  db
                  [["Ivan" "ivan@mail.ru"]
                   ["Petr" "petr@gmail.com"]])
             #{[1 "ivan@mail.ru"]
               [2 "petr@gmail.com"]
               [3 "ivan@mail.ru"]})))

    (testing "Tuple binding"
      (is (= (d/q '[:find  ?e
                    :in    $ [?name ?age]
                    :where [?e :name ?name]
                    [?e :age ?age]]
                  db ["Ivan" 37])
             #{[3]})))

    (testing "Collection binding"
      (is (= (d/q '[:find  ?attr ?value
                    :in    $ ?e [?attr ...]
                    :where [?e ?attr ?value]]
                  db 1 [:name :age])
             #{[:name "Ivan"] [:age 15]})))

    (testing "Empty coll handling"
      (is (= (d/q '[:find ?id
                    :in $ [?id ...]
                    :where [?id :age _]]
                  [[1 :name "Ivan"]
                   [2 :name "Petr"]]
                  [])
             #{}))
      (is (= (d/q '[:find ?id
                    :in $ [[?id]]
                    :where [?id :age _]]
                  [[1 :name "Ivan"]
                   [2 :name "Petr"]]
                  [])
             #{})))

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
                            (d/q '[:find ?a ?b :in [?a ?b]] [:a]))))))

(deftest test-nested-bindings
  (is (= (d/q '[:find  ?k ?v
                :in    [[?k ?v] ...]
                :where [(> ?v 1)]]
              {:a 1, :b 2, :c 3})
         #{[:b 2] [:c 3]}))

  (is (= (d/q '[:find  ?k ?min ?max
                :in    [[?k ?v] ...] ?minmax
                :where [(?minmax ?v) [?min ?max]]
                [(> ?max ?min)]]
              {:a [1 2 3 4]
               :b [5 6 7]
               :c [3]}
              #(vector (reduce min %) (reduce max %)))
         #{[:a 1 4] [:b 5 7]}))

  (is (= (d/q '[:find  ?k ?x
                :in    [[?k [?min ?max]] ...] ?range
                :where [(?range ?min ?max) [?x ...]]
                [(even? ?x)]]
              {:a [1 7]
               :b [2 4]}
              range)
         #{[:a 2] [:a 4] [:a 6]
           [:b 2]})))

(deftest test-offset
  (let [db (-> (d/empty-db)
               (d/db-with [{:db/id 1, :name  "Alice", :age   15}
                           {:db/id 2, :name  "Bob", :age   37}
                           {:db/id 3, :name  "Charlie", :age   37}]))]
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
    (is (= (count  (d/q {:query '[:find ?e :where [?e :name _]]
                         :args [db]
                         :offset 1
                         :limit 2}))
           2))
    (is (= (count  (d/q {:query '[:find ?e :where [?e :name _]]
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
  (let [db (-> (d/empty-db)
               (d/db-with [{:db/id 1, :name  "Alice", :age   15}
                           {:db/id 2, :name  "Bob", :age   37}
                           {:db/id 3, :name  "Charlie", :age   37}]))]
    (testing "returns map"
      (is (map? (first (d/q {:query '[:find ?e :keys name :where [?e :name _]]
                             :args [db]})))))
    (testing "returns set without return-map"
      (is (= #{["Charlie"] ["Alice"] ["Bob"]}
             (d/q {:query '[:find ?name :where [_ :name ?name]]
                   :args [db]}))))
    (testing "returns map with key return-map"
      (is (= [{:foo 3} {:foo 2} {:foo 1}]
             (d/q {:query '[:find ?e :keys foo :where [?e :name _]]
                   :args [db]}))))
    (testing "returns map with string return-map"
      (is (= [{"foo" "Charlie"} {"foo" "Alice"} {"foo" "Bob"}]
             (d/q {:query '[:find ?name :strs foo :where [?e :name ?name]]
                   :args [db]}))))
    (testing "return map with keys using multiple find vars"
      (is (= #{["Bob" {:age 37 :db/id 2}]
               ["Charlie" {:age 37 :db/id 3}]
               ["Alice" {:age 15 :db/id 1}]}
             (into #{} (d/q {:find '[?name (pull ?e ?p)]
                             :args [db '[:age :db/id]]
                             :in '[$ ?p]
                             :where '[[?e :name ?name]]})))))))

(deftest test-memoized-parse-query
  (testing "no map return"
    (is (= nil
           (:qreturnmap (dq/memoized-parse-query '[:find ?e :where [?e :name]])))))
  (testing "key map return"
    (is (= '#datalog.parser.type.ReturnMaps{:mapping-type :keys, :mapping-keys (#datalog.parser.type.MappingKey{:mapping-key foo})}
           (:qreturnmaps (dq/memoized-parse-query '[:find ?e :keys foo :where [?e :name]])))))
  (testing "key map return multiple"
    (is (= '#datalog.parser.type.ReturnMaps{:mapping-type :keys, :mapping-keys (#datalog.parser.type.MappingKey{:mapping-key foo}, #datalog.parser.type.MappingKey{:mapping-key bar})}
           (:qreturnmaps (dq/memoized-parse-query '[:find ?e ?f :keys foo bar :where [?e :name ?f]])))))
  (testing "string map return multiple"
    (is (= '#datalog.parser.type.ReturnMaps{:mapping-type :strs, :mapping-keys (#datalog.parser.type.MappingKey{:mapping-key foo}, #datalog.parser.type.MappingKey{:mapping-key bar})}
           (:qreturnmaps (dq/memoized-parse-query '[:find ?e ?f :strs foo bar :where [?e :name ?f]])))))
  (testing "symbol map return multiple"
    (is (= '#datalog.parser.type.ReturnMaps{:mapping-type :syms, :mapping-keys (#datalog.parser.type.MappingKey{:mapping-key foo}, #datalog.parser.type.MappingKey{:mapping-key bar})}
           (:qreturnmaps (dq/memoized-parse-query '[:find ?e ?f :syms foo bar :where [?e :name ?f]]))))))

(deftest test-convert-to-return-maps
  (testing "converting keys"
    (is (= [{:foo 3} {:foo 2} {:foo 1}]
           (dq/convert-to-return-maps '#datalog.parser.type.ReturnMaps{:mapping-type :keys,
                                                                       :mapping-keys (#datalog.parser.type.MappingKey{:mapping-key foo})}
                                      #{[1] [2] [3]}))))
  (testing "converting strs"
    (is (= [{"foo" 3} {"foo" 2} {"foo" 1}]
           (dq/convert-to-return-maps '#datalog.parser.type.ReturnMaps{:mapping-type :strs,
                                                                       :mapping-keys (#datalog.parser.type.MappingKey{:mapping-key foo})}
                                      #{[1] [2] [3]}))))
  (testing "converting syms"
    (is (= [{'foo 3} {'foo 2} {'foo 1}]
           (dq/convert-to-return-maps '#datalog.parser.type.ReturnMaps{:mapping-type :syms,
                                                                       :mapping-keys (#datalog.parser.type.MappingKey{:mapping-key foo})}
                                      #{[1] [2] [3]}))))
  (testing "converting keys"
    (is (= '[{:foo 1, :bar 11, :baz "Ivan"} {:foo 3, :bar 21, :baz "Petr"} {:foo 3, :bar 31, :baz "Ivan"}]
           (dq/convert-to-return-maps '#datalog.parser.type.ReturnMaps{:mapping-type :keys,
                                                                       :mapping-keys (#datalog.parser.type.MappingKey{:mapping-key foo},
                                                                                      #datalog.parser.type.MappingKey{:mapping-key bar},
                                                                                      #datalog.parser.type.MappingKey{:mapping-key baz})}
                                      #{[1 11 "Ivan"]
                                        [3 31 "Ivan"]
                                        [3 21 "Petr"]}))))
  (testing "converting strs"
    (is (= '[{"foo" 1, "bar" 11, "baz" "Ivan"} {"foo" 3, "bar" 21, "baz" "Petr"} {"foo" 3, "bar" 31, "baz" "Ivan"}]
           (dq/convert-to-return-maps '#datalog.parser.type.ReturnMaps{:mapping-type :strs,
                                                                       :mapping-keys (#datalog.parser.type.MappingKey{:mapping-key foo},
                                                                                      #datalog.parser.type.MappingKey{:mapping-key bar},
                                                                                      #datalog.parser.type.MappingKey{:mapping-key baz})}
                                      #{[1 11 "Ivan"]
                                        [3 31 "Ivan"]
                                        [3 21 "Petr"]}))))
  (testing "converting syms"
    (is (= '[{foo 1, bar 11, baz "Ivan"} {foo 3, bar 21, baz "Petr"} {foo 3, bar 31, baz "Ivan"}]
           (dq/convert-to-return-maps '#datalog.parser.type.ReturnMaps{:mapping-type :syms,
                                                                       :mapping-keys (#datalog.parser.type.MappingKey{:mapping-key foo},
                                                                                      #datalog.parser.type.MappingKey{:mapping-key bar},
                                                                                      #datalog.parser.type.MappingKey{:mapping-key baz})}
                                      #{[1 11 "Ivan"]
                                        [3 31 "Ivan"]
                                        [3 21 "Petr"]})))))

#_(require 'datahike.test.query :reload)
#_(clojure.test/test-ns 'datahike.test.query)
