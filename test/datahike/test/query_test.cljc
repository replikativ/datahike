(ns datahike.test.query-test
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer        [is deftest testing]])
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.test.utils :as utils]
   [datahike.query :as dq]
   [taoensso.timbre :as log])
  #?(:clj
     (:import [clojure.lang ExceptionInfo])))

#?(:cljs (def Throwable js/Error))

(deftest test-joins
  (let [db (-> (db/empty-db)
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

(deftest test-mixed-age-types
  (let [db (-> (db/empty-db)
               (d/db-with [{:db/id 1, :name  "Ivan", :age   15}
                           {:db/id 2, :name  "Petr", :age   "37"}
                           {:db/id 3, :name  "Ivan", :age   :thirtyseven}
                           {:db/id 4, :age 15}]))]
    (is (= (d/q '[:find  ?e ?name
                  :where
                  [?e :name ?name]
                  [?e :age "37"]] db)
           #{[2 "Petr"]}))))

(deftest test-q-many
  (let [db (-> (db/empty-db {:aka {:db/cardinality :db.cardinality/many}})
               (d/db-with [[:db/add 1 :name "Ivan"]
                           [:db/add 1 :aka  "ivolga"]
                           [:db/add 1 :aka  "pi"]
                           [:db/add 2 :name "Petr"]
                           [:db/add 2 :aka  "porosenok"]
                           [:db/add 2 :aka  "pi"]]))]
    (is (= (d/q '[:find  ?n1 ?n2
                  :where
                  [?e1 :aka ?x]
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
  (let [db (-> (db/empty-db)
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
  (let [db (-> (db/empty-db)
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
  (let [db (-> (db/empty-db)
               (d/db-with [{:db/id 1, :name  "Alice", :age   15}
                           {:db/id 2, :name  "Bob", :age   37}
                           {:db/id 3, :name  "Charlie", :age   37}]))]
    (is (= 3 (count (d/q {:query '[:find ?e :where [?e :name _]]
                          :args [db]
                          :limit -1}))))
    (is (= 3 (count (d/q {:query '[:find ?e :where [?e :name _]]
                          :args [db]}))))
    (is (= 3 (count (d/q {:query '[:find ?e :where [?e :name _]]
                          :args [db]
                          :limit nil}))))
    (is (= 3 (count (d/q {:query '[:find ?e :where [?e :name _]]
                          :args [db]
                          :offset -1}))))
    (is (= 3 (count (d/q {:query '[:find ?e :where [?e :name _]]
                          :args [db]
                          :offset nil}))))
    (is (= 1 (count (d/q {:query '[:find ?e :where [?e :name _]]
                          :args [db]
                          :offset 1
                          :limit 1}))))
    (is (= 2 (count (d/q {:query '[:find ?e :where [?e :name _]]
                          :args [db]
                          :limit 2}))))
    (is (= 2 (count  (d/q {:query '[:find ?e :where [?e :name _]]
                           :args [db]
                           :offset 1
                           :limit 2}))))
    (is (= 1 (count  (d/q {:query '[:find ?e :where [?e :name _]]
                           :args [db]
                           :offset 2
                           :limit 2}))))
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
  (let [db (-> (db/empty-db)
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

#_(deftest test-clause-order-invariance                     ;; TODO: this is what should happen after rewirite of query engine
    (let [db (-> (db/empty-db)
                 (d/db-with [{:db/id 1, :name  "Ivan", :age   15}
                             {:db/id 2, :name  "Petr", :age   37}
                             {:db/id 3, :name  "Ivan", :age   37}
                             {:db/id 4, :age 15}]))]
      (testing "Clause order does not matter for predicates"
        (is (= (d/q {:query '{:find [?e]
                              :where [[?e :age ?age]
                                      [(= ?age 37)]]}
                     :args [db]})
               #{[2] [3]}))
        (is (= (d/q {:query '{:find [?e]
                              :where [[(= ?age 37)]
                                      [?e :age ?age]]}
                     :args [db]})
               #{[2] [3]})))))

(deftest test-clause-order
  (let [db (-> (db/empty-db)
               (d/db-with [{:db/id 1, :name  "Ivan", :age   15}
                           {:db/id 2, :name  "Petr", :age   37}
                           {:db/id 3, :name  "Ivan", :age   37}
                           {:db/id 4, :age 15}]))]
    (testing "Predicate clause before variable binding throws exception"
      (is (= (d/q {:query '{:find [?e]
                            :where [[?e :age ?age]
                                    [(= ?age 37)]]}
                   :args [db]})
             #{[2] [3]}))
      (is (thrown-with-msg? Throwable #"Insufficient bindings: #\{\?age\} not bound"
                            (d/q {:query '{:find [?e]
                                           :where [[(= ?age 37)]
                                                   [?e :age ?age]]}
                                  :args [db]}))))))

(deftest test-zeros-in-pattern
  (let [cfg {:store {:backend :mem
                     :id "sandbox"}
             :schema-flexibility :write
             :attribute-refs? false}
        conn (do
               (d/delete-database cfg)
               (d/create-database cfg)
               (d/connect cfg))]
    (d/transact conn [{:db/ident :version/id
                       :db/valueType :db.type/long
                       :db/cardinality :db.cardinality/one
                       :db/unique :db.unique/identity}
                      {:version/id 0}
                      {:version/id 1}])
    (is (= 1
           (count (d/q '[:find ?t :in $ :where
                         [?t :version/id 0]]
                       @conn))))
    (d/release conn)))

;; https://github.com/replikativ/datahike/issues/471
(deftest keyword-keys-test
  (let [schema [{:db/ident       :name
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
        cfg    {:store              {:backend :mem
                                     :id      "DEV"}
                :schema-flexibility :write
                :attribute-refs?    true}
        conn   (utils/setup-db cfg)]
    (d/transact conn schema)
    (d/transact conn [{:name "Alice"
                       :age  25}
                      {:name "Bob"
                       :age  35}])
    (d/transact conn [{:name    "Charlie"
                       :age     5
                       :parents [[:name "Alice"] [:name "Bob"]]}])
    (let [db             @conn
          keyword-result (into #{} (d/q '[:find ?n ?a
                                          :keys :name :age
                                          :where
                                          [?e :name ?n]
                                          [?e :age ?a]]
                                        db))
          symbol-result  (into #{} (d/q '[:find ?n ?a
                                          :keys name age
                                          :where
                                          [?e :name ?n]
                                          [?e :age ?a]]
                                        db))]
      (testing "keyword result keys"
        (is (= #{{:name "Alice" :age 25}
                 {:name "Charlie" :age 5}
                 {:name "Bob" :age 35}}
               keyword-result)))
      (testing "keyword equals symbol keys"
        (is (= symbol-result
               keyword-result))))
    (d/release conn)))

(deftest test-normalize-q-input
  (testing "query as vector"
    (is (= {:query {:find '[?n]
                    :where '[[?e :name ?n]]}
            :args :db}
           (dq/normalize-q-input '[:find ?n
                                   :where [?e :name ?n]]
                                 :db))))

  (testing "query in :query field"
    (is (= {:query {:find '[?n]
                    :where '[[?e :name ?n]]}
            :args [:db]}
           (dq/normalize-q-input {:query '{:find [?n]
                                           :where [[?e :name ?n]]}
                                  :args [:db]}
                                 [])))
    (is (= {:query {:find '[?n], :where '[[?e :name ?n]]}
            :args [:db]}
           (dq/normalize-q-input {:query '{:find [?n]
                                           :where [[?e :name ?n]]}}
                                 [:db])))
    (is (= {:query {:find '[?n], :where '[[?e :name ?n]]}
            :args [:db]}
           (dq/normalize-q-input {:query '{:find [?n]
                                           :where [[?e :name ?n]]}
                                  :args [:db]}
                                 [:db2])))
    (is (= {:query {:find '[?n]
                    :where '[[?e :name ?n]]}
            :args [:db]
            :limit 100
            :offset 0}
           (dq/normalize-q-input {:query '[:find ?n
                                           :where [?e :name ?n]]
                                  :offset 0
                                  :limit 100
                                  :args [:db]}
                                 []))))

  (testing "query in top-level map"
    (is (= {:query {:find '[?e]
                    :where '[[?e :name ?value]]}
            :args []
            :limit 100
            :offset 0}
           (dq/normalize-q-input {:find '[?e]
                                  :where '[[?e :name ?value]]
                                  :offset 0
                                  :limit 100} [])))))

(deftest test-distinct-tuples
  (is (= [[3 4]] (dq/distinct-tuples [[3 4]])))
  (let [arrays [(object-array [:a]) (object-array [:a])]
        result (dq/distinct-tuples arrays)
        object-array-type (type (object-array []))]
    (is (every? #(= object-array-type (type %)) result))
    (is (= [[:a]] (map vec result)))

    ;; This is just to highlight the difference w.r.t. `distinct`:
    (is (= [[:a] [:a]] (map vec (distinct arrays)))))
  (is (= [[3 4]] (dq/distinct-tuples [[3 4] [3 4]])))
  (is (= [[3 4]] (dq/distinct-tuples [[3 4]
                                      (long-array [3 4])])))
  (is (= [[3 4] [9 7]] (dq/distinct-tuples [[3 4] [9 7] [3 4]]))))

;; A good one
(def ex1 '{:source {:max-tx 536926163},
           :pattern1 [?r1 79 ?oc],
           :context
           {:rels
            [{:attrs {?oc 0},
              :tuples
              [[5289]
               [5294]
               [5299]
               [5304]
               [5307]
               [5310]
               [5313]
               [5317]
               [5322]
               [5325]],
              :tuple-count 3654}
             {:attrs {?__auto__1 0}, :tuples [], :tuple-count 0}],
            :consts {?__auto__1 "narrow-match"}},
           :clause [?r1 :relation/concept-1 ?oc],
           :constrained-patterns
           [[?r1 79 5289]
            [?r1 79 5294]
            [?r1 79 5299]
            [?r1 79 5304]
            [?r1 79 5307]
            [?r1 79 5310]
            [?r1 79 5313]
            [?r1 79 5317]
            [?r1 79 5322]
            [?r1 79 5325]],
           :constrained-pattern-count 3654})

(deftest test-new-search-strategy
  (let [;; pattern1 = [?r1 79 ?oc]
        {:keys [context pattern1]} ex1
        rels (vec (:rels context))
        bsm (dq/bound-symbol-map rels)

        clean-pattern (dq/replace-unbound-symbols-by-nil bsm pattern1)

        strategy0 [nil :substitute :substitute nil]
        strategy1 [nil :substitute :filter nil]

        subst-inds0 (dq/substitution-relation-indices
                     {:bsm bsm
                      :clean-pattern clean-pattern
                      :strategy-vec  strategy0})
        subst-inds1 (dq/substitution-relation-indices
                     {:bsm bsm
                      :clean-pattern clean-pattern
                      :strategy-vec  strategy1})
        filt-inds0 (dq/filtering-relation-indices
                    {:bsm bsm
                     :clean-pattern clean-pattern
                     :strategy-vec  strategy0}
                    subst-inds0)
        filt-inds1 (dq/filtering-relation-indices
                    {:bsm bsm
                     :clean-pattern clean-pattern
                     :strategy-vec  strategy1}
                    subst-inds1)]
    (is (seq rels))
    (is (= '{?oc {:relation-index 0, :tuple-element-index 0},
             ?__auto__1 {:relation-index 1, :tuple-element-index 0}}
           bsm))
    (is (= #{0} subst-inds0))
    (is (= #{} subst-inds1))
    (is (= #{} filt-inds0))
    (is (= #{0} filt-inds1))))

(defn pack6 [step]
  (fn
    ([] (step))
    ([dst] (step dst))
    ([dst e a v tx added? filt]
     (step dst [[e a v tx added?] filt]))))

(deftest test-substitution-plan
  (let [-pattern1 '[?w ?x ?y]
        context '{:rels [{:attrs {?x 0
                                  ?y 1}
                          :tuples [[1 2]
                                   [3 4]
                                   [3 5]
                                   [5 6]]}
                         {:attrs {?z 0}
                          :tuples [[9] [10] [11]]}]}
        rels (vec (:rels context))
        bsm (dq/bound-symbol-map rels)
        clean-pattern (dq/replace-unbound-symbols-by-nil bsm -pattern1)
        strategy [nil :substitute :filter nil]
        subst-inds (dq/substitution-relation-indices
                    {:bsm bsm
                     :clean-pattern clean-pattern
                     :strategy-vec strategy
                     :rels rels})
        filt-inds (dq/filtering-relation-indices
                   {:bsm bsm
                    :clean-pattern clean-pattern
                    :strategy-vec strategy
                    :rels rels}
                   subst-inds)
        [init-coll subst-xform] (dq/initialization-and-substitution-xform
                                 {:bsm bsm
                                  :clean-pattern clean-pattern
                                  :strategy-vec strategy
                                  :rels rels}
                                 subst-inds)

        result (into []
                     (comp dq/unpack6
                           subst-xform
                           pack6)
                     init-coll)
        [[_ p0] [_ p1] [_ p2] [_ p3]] result]
    (is (= #{0} subst-inds))
    (is (= #{} filt-inds))
    (is (= {'?x {:relation-index 0 :tuple-element-index 0}
            '?y {:relation-index 0 :tuple-element-index 1}
            '?z {:relation-index 1 :tuple-element-index 0}}
           bsm))
    (is (= [[nil 1 nil nil nil]
            [nil 3 nil nil nil]
            [nil 3 nil nil nil]
            [nil 5 nil nil nil]] (map first result)))
    (is (p0 [1 2 2]))
    (is (not (p0 [1 2 3])))
    (is (p1 [1 2 4]))
    (is (p2 [1 2 5]))
    (is (not (p1 [1 2 6])))
    (is (p3 [1 2 6]))
    (is (not (p3 [1 2 5])))))

(deftest test-index-feature-extractor
  (let [e (dq/index-feature-extractor [1] true)]
    (is (= 3 (e [119 3])))
    (is (= 4 (e [120 4 9 3]))))
  (let [e (dq/index-feature-extractor [1 0] true)]
    (is (= [3 119] (e [119 3])))
    (is (= [4 120] (e [120 4 9 3]))))
  (let [e (dq/index-feature-extractor [] true)]
    (is (nil? (e [119 3])))
    (is (nil? (e [120 4 9 3]))))
  (is (nil? (dq/index-feature-extractor [] false))))

(deftest test-filtering-plan
  (let [pattern1 '[?w ?x ?y]
        context '{:rels [{:attrs {?x 0}
                          :tuples [[1]
                                   [3]
                                   [5]]}
                         {:attrs {?y 0}
                          :tuples [[2] [4] [6]]}
                         {:attrs {?z 0}
                          :tuples [[9] [10] [11]]}]}

        rels (vec (:rels context))
        bsm (dq/bound-symbol-map rels)
        clean-pattern (dq/replace-unbound-symbols-by-nil bsm pattern1)
        strategy [nil :substitute :filter nil]
        subst-inds (dq/substitution-relation-indices
                    {:bsm bsm
                     :clean-pattern pattern1
                     :strategy-vec strategy})
        filt-inds (dq/filtering-relation-indices
                   {:bsm bsm
                    :clean-pattern clean-pattern
                    :strategy-vec strategy}
                   subst-inds)
        [init-coll subst-xform] (dq/initialization-and-substitution-xform
                                 {:bsm bsm
                                  :clean-pattern clean-pattern
                                  :strategy-vec strategy
                                  :rels rels}
                                 subst-inds)

        subst-result (into []
                           (comp dq/unpack6
                                 subst-xform
                                 pack6)
                           init-coll)
        [[_ p0]] subst-result]
    (is (nil? p0))
    (is (= '[nil ?x ?y nil nil] clean-pattern))
    (is (= #{0} subst-inds))
    (is (= #{1} filt-inds))
    (is (= {'?x {:relation-index 0 :tuple-element-index 0}
            '?y {:relation-index 1 :tuple-element-index 0}
            '?z {:relation-index 2 :tuple-element-index 0}}
           bsm))
    (is (= '([nil 1 nil nil nil]
             [nil 3 nil nil nil]
             [nil 5 nil nil nil])
           (map first subst-result)))))

(defn pcmp [x y]
  (or (nil? x) (= x y)))

(defn mock-backend-fn [datoms]
  (fn [e0 a0 v0 t0 added0]
    (filter (fn [[e1 a1 v1 t1 added1]]
              (and (pcmp e0 e1)
                   (pcmp a0 a1)
                   (pcmp v0 v1)
                   (pcmp t0 t1)
                   (pcmp added0 added1)))
            datoms)))

(deftest test-full-lookup-pipeline
  (let [pattern1 '[?x ?w ?y]
        context '{:rels [{:attrs {?x 0}
                          :tuples [[1] [3] [5]]}
                         {:attrs {?y 0}
                          :tuples [[4] [5] [6]]}]}
        strategy-vec [:substitute nil :filter nil]
        rels (vec (:rels context))
        bsm (dq/bound-symbol-map rels)
        clean-pattern (dq/replace-unbound-symbols-by-nil bsm pattern1)
        sfn (dq/search-batch-fn {:bsm bsm
                                 :clean-pattern clean-pattern
                                 :rels rels})
        result (sfn strategy-vec
                    (mock-backend-fn [[0 :abc 5]
                                      [5 :xyz 6]
                                      [1 :k 4]
                                      [5 :p 7]])
                    identity)]
    (is (= #{[1 :k 4] [5 :xyz 6]} (set result)))))

(defn concept-id [index]
  (let [s (format "%010d" index)]
    (str (subs s 0 4) "_" (subs s 4 7) "_" (subs s 7 10))))

(defn temp-id [x]
  (str "tmp-" x))

(defn make-forest
  "This function constructs tx-data for a forest of concepts in a terminology, 
  for example a labor market terminology of occupations. The edges in the
  forest point toward the root and are labeled `:concept/broader` because the
  closer to the root we get, the broader the concept is. For instance, we could
  have a concept for the occupation name 'Software Engineer' and an edge from 
  that concept pointing at a broader concept 'Occuptions in Computer Science'.

  This function takes as input a concatenated list of pairs of `m` and `t`
  on the form `[m1 t1 m2 t2 ... mN tN]` that specifies how many sub nodes
  should be generated at each level from the root and the type. For example,
  `[5 'ssyk-level-1' 3 'ssyk-level-2']` means that we will construct 5 trees
  of root node type 'ssyk-level-1' and each one of them will have 3 children
  of node type 'ssyk-level-2'."
  ([tree-spec] (make-forest tree-spec 0))
  ([[root-count & tree-spec] init-counter]
   {:pre [(number? root-count)]}
   (loop [[tos & stack] (repeat root-count [nil tree-spec])
          tx-data []
          counter init-counter
          concept-map {}]
     (if (nil? tos)
       {:tx-data tx-data
        :concept-map concept-map}
       (let [[parent-id [this-type child-count & remaining-pairs]] tos
             concept-id (concept-id counter)
             tid (temp-id concept-id)
             parent-tid (temp-id parent-id)]
         (assert (or (nil? parent-id) (string? parent-id)))
         (recur (into stack
                      (when (seq remaining-pairs)
                        (repeat child-count [concept-id remaining-pairs])))
                (into tx-data
                      cat
                      [[[:db/add tid :concept/id concept-id]
                        [:db/add tid :concept/type this-type]]
                       (when parent-id
                         [[:db/add tid :concept/broader parent-tid]])])
                (inc counter)
                (cond-> concept-map
                  true (update concept-id
                               merge {:parent-id parent-id
                                      :type this-type
                                      :id concept-id})
                  parent-id (update-in [parent-id :child-ids] #(conj (or % []) concept-id)))))))))

(def schema [#:db{:ident :concept/id,
                  :valueType :db.type/string,
                  :cardinality :db.cardinality/one,
                  :doc "Unique identifier for concepts",
                  :unique :db.unique/identity}
             #:db{:ident :concept/type,
                  :valueType :db.type/string,
                  :cardinality :db.cardinality/one,
                  :doc "The concepts main type"}
             #:db {:ident :concept/broader
                   :valueType :db.type/ref
                   :cardinality :db.cardinality/one
                   :doc "A broader concept. NOTE: This the JobTech Taxonomy, every relation between two concepts has an entity with attributes :relation/concept-1, :relation-concept-2 and :relation/type."}])

(defn initialize-test-db0 []
  (let [conn (utils/setup-db {:store {:backend :mem}
                              :schema-flexibility :write
                              :attribute-refs? false
                              :keep-history? true})]
    (d/transact conn {:tx-data schema})
    conn))

(defn group-concepts-by-type [concept-map]
  (let [groups (update-vals (group-by (comp :type val)
                                      concept-map)
                            (fn [kv-pairs]
                              (mapv first kv-pairs)))]
    (doseq [[k v] (sort-by val (update-vals groups count))]
      (log/info k v))
    (log/info "Total count:" (count concept-map))
    groups))

(deftest synthetic-ssyk-tree-test

  "In this test we construct a labor market taxonomy of occupations. Given
some concept ids, we look up broader concepts. The queries in this test will
include clauses with up to two unknown variables.

We perform two queries. In the first query, we only provide one input id and
look up concepts broader than that id. 

In the second query, we provide two input ids."

  (testing "Given some concepts, query concepts that are broader."
    (let [conn (initialize-test-db0)
          ssyk-data (make-forest
                     [3 "ssyk-level-1"
                      5 "ssyk-level-2"
                      30 "ssyk-level-3"
                      5 "ssyk-level-4"
                      2 "occupation-name"])
          ssyk-concept-map (:concept-map ssyk-data)
          _ (d/transact conn {:tx-data (:tx-data ssyk-data)})
          concepts-per-type (group-concepts-by-type ssyk-concept-map)
          ssyk-level-3-ids (concepts-per-type "ssyk-level-3")
          expected-result-fn (fn [concept-ids]
                               (into #{}
                                     (map (fn [cid]
                                            {:from_id cid
                                             :id (get-in ssyk-concept-map
                                                         [cid :parent-id])}))
                                     concept-ids))

          related-query '{:find [?from-id ?id],
                          :keys [from_id id],
                          :in [$ [?from-id ...]],
                          :where
                          [[?c :concept/id ?from-id]
                           [?c :concept/broader ?related-c]
                           [?related-c :concept/id ?id]]}]
      (testing "Query for 1 input concept id."
        (let [input-concept-id (first ssyk-level-3-ids)
              _ (is (string? input-concept-id))
              result (d/q {:query related-query
                           :args [(d/db conn) #{input-concept-id}]})]
          (is (= (expected-result-fn [input-concept-id])
                 (set result)))))
      (testing "Query for 2 input ids."
        (let [input-ids (set (take 2 ssyk-level-3-ids))
              result (d/q {:query related-query
                           :args [(d/db conn) input-ids]})]
          (is (= (expected-result-fn input-ids)
                 (set result))))))))

(deftest synthetic-ssyk-tree-test2

  "We construct a forest of four trees with each tree having 2000 subnodes each. We then pick the ids
of two of the root nodes of the trees and the ids from three of the children of one 
trees. Then we we query for all (parent,child) pairs."

  (let [conn (initialize-test-db0)
        ssyk-data (make-forest [4 "ssyk-level-1" 2000 "ssyk-level-2"])
        ssyk-concept-map (:concept-map ssyk-data)
        _ (d/transact conn {:tx-data (:tx-data ssyk-data)})
        concepts-per-type (group-concepts-by-type ssyk-concept-map)]
    (testing "Query (parent,child) pairs from a *small* set of possible combinations in a labour market taxonomy."
      (let [parent-ids (take 2 (concepts-per-type "ssyk-level-1"))
            parent-id (first parent-ids)
            child-ids (take 3 (get-in ssyk-concept-map [parent-id :child-ids]))
            _ (is (= 2 (count parent-ids)))
            _ (is (= 3 (count child-ids)))
            result (d/q {:query '{:find [?parent-id ?child-id]
                                  :keys [parent_id child_id]
                                  :in [$
                                       [?parent-id ...]
                                       [?child-id ...]],
                                  :where
                                  [[?pc :concept/id ?parent-id]
                                   [?cc :concept/id ?child-id]
                                   [?cc :concept/broader ?pc]]}
                         :args [(d/db conn)
                                parent-ids
                                child-ids]})
            expected-result (into #{}
                                  (map (fn [child-id] {:parent_id parent-id :child_id child-id}))
                                  child-ids)]
        (is (= 3 (count expected-result)))
        (is (= (set result)
               expected-result))))))

(deftest synthetic-ssyk-tree-test3
  "We construct a labor market taxonomy of 200 trees where each root node has one child. Then
we query all (parent, child) pairs."

  (let [conn (initialize-test-db0)
        ssyk-data (make-forest [200 "ssyk-level-1" 1 "ssyk-level-2"])
        ssyk-concept-map (:concept-map ssyk-data)
        _ (d/transact conn {:tx-data (:tx-data ssyk-data)})

        ;; Adding some extra data here also makes `expand-once` perform better than
        ;; `identity` and `select-simple`. If we remove these two lines, then
        ;; `expand-once`, `identity` and `select-simple` will perform roughly the same.
        extra-data (make-forest [100 "skill-headline" 100 "skill"] (count ssyk-concept-map))
        _ (d/transact conn {:tx-data (:tx-data extra-data)})

        _concepts-per-type (group-concepts-by-type ssyk-concept-map)]
    (testing "Query (parent,child) pairs from a *large* set of possible combinations in a labour market taxonomy."
      (let [result (d/q {:query '{:find [?parent-id ?child-id]
                                  :keys [parent_id child_id]
                                  :in [$ %],
                                  :where
                                  [[?pc :concept/type "ssyk-level-1"]
                                   [?cc :concept/type "ssyk-level-2"]
                                   [?cc :concept/broader ?pc]
                                   [?pc :concept/id ?parent-id]
                                   [?cc :concept/id ?child-id]]}
                         :args [(d/db conn)]})
            expected-result (into #{}
                                  (keep (fn [[child-id {:keys [parent-id]}]]
                                          (when parent-id
                                            {:child_id child-id :parent_id parent-id})))
                                  ssyk-concept-map)]
        (is (= 200 (count expected-result)))
        (is (= expected-result
               (set result)))))))

(deftest basic-index-selector-test
  (let [f (dq/basic-index-selector 5)]
    (is (= [10 7] ((f [1 3]) [9 10 4 7 1234])))
    (is (= [7 10] ((f [3 1]) [9 10 4 7 1234])))))
