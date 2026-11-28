(ns datahike.test.contract-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [datahike.query :as q]
            [datahike.experimental.contract :as contract]))

(def window
  '[:find ?id :in $ $delta
    :where [$delta ?lo ?hi ?target]
    [?entity :sample/n ?n] [(>= ?n ?lo)] [(<= ?n ?hi)]
    [?entity :sample/id ?id] [(= ?n ?target)]])

(def attrs #{:sample/id :sample/n})

(deftest indexed-window-and-projection
  (let [cfg {:store {:backend :memory :id (random-uuid)} :schema-flexibility :write}]
    (d/create-database cfg)
    (let [c (d/connect cfg)]
      (try
        (d/transact c [{:db/ident :sample/n :db/valueType :db.type/long :db/index true
                        :db/cardinality :db.cardinality/one}
                       {:db/ident :sample/id :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])
        (d/transact c (mapv #(hash-map :sample/n % :sample/id %) (range 150)))
        (let [db @c
              rows [[0 149 149]]
              run #(contract/execute %1 attrs 3 db %2 %3)
              result (run window rows 4096)
              project-all (vec (butlast window))
              expected (set (d/q window db rows))]
          (is (= #{[149]} expected (:rows result)))
          (is (= 150 (get-in result [:events :predicate])))
          (is (= :relation-limit (:status (run project-all rows 4096))))
          (doseq [limit [0 1 2 3 150 300 (dec (:used result))]]
            (let [r (run window rows limit)]
              (is (= [:out-of-fuel limit nil] ((juxt :status :used :rows) r)))
              (is (= limit (reduce + (vals (:events r)))))))
          (is (= #{[149]} (:rows (run window [[149 149 149]] 4096))))
          (is (= #{} (:rows (run window [[149 0 149]] 4096))))
          (doseq [form ['[:find ?sku (sum ?qty) (count ?qty) :with ?leg :in $ $delta
                          :where [$delta ?leg ?sku ?qty]]
                        '[:find ?sku (sum ?qty) :with ?leg :in $ $delta :where [$delta ?leg ?sku ?qty]]
                        '[:find ?sku (sum ?qty) :in $ $delta :where [$delta ?leg ?sku ?qty]]
                        '[:find ?leg :in $ $delta :where [$delta ?leg ?sku ?qty] [(< ?qty 0)]]]]
            (let [rows [[0 :a 2] [1 :a 2] [1 :a 2] [2 :b -1]]]
              (is (= (set (d/q form db rows)) (:rows (run form rows 4096))))))
          (let [sum-form '[:find (sum ?x) :with ?id :in $ $delta :where [$delta ?id ?x ?unused]]
                numeric '[:find ?id :in $ $delta :where [$delta ?id ?x ?unused] [(< ?x 4)]]]
            (doseq [[query input status] [[sum-form [[0 Long/MAX_VALUE 0] [1 1 0]] :numeric-overflow]
                                          [numeric [[0 :keyword 0]] :type-error]
                                          [window [["a" 3 3]] :type-error]]]
              (let [r (run query input 4096)]
                (is (= [status nil] ((juxt :status :rows) r))))))
          (let [form '[:find ?sku (sum ?qty) :with ?leg :in $ $delta :where [$delta ?leg ?sku ?qty]]
                data [[0 :nuts 2] [1 :nuts 2] [2 :bolts -3] [3 :bolts 3]]]
            (doseq [rows [data (vec (reverse data))]]
              (is (= #{[:nuts 4] [:bolts 0]} (:rows (run form rows 12))))
              (doseq [limit (range 12)]
                (let [r (run form rows limit)]
                  (is (= [:out-of-fuel limit nil] ((juxt :status :used :rows) r)))))))
          ;; Deterministic differential corpus: overlapping windows, misses,
          ;; duplicate input rows, and equality joins with local negation.
          (let [rng (java.util.Random. 127)
                absent '[:find ?x :in $ $delta :where [$delta ?lo ?hi ?x]
                         (not-join [?x] [_ :sample/n ?x])]
                grouped '[:find ?x (count ?n) :with ?entity :in $ $delta
                          :where [$delta ?lo ?hi ?x] [?entity :sample/n ?n]
                          [(>= ?n ?lo)] [(<= ?n ?hi)]]]
            (dotimes [_ 32]
              (let [rows (vec (for [_ (range 3)
                                    :let [start (.nextInt rng 155)]]
                                [start (+ start (.nextInt rng 8)) (.nextInt rng 155)]))
                    rows (conj rows (first rows))
                    wire (mapv #(mapv long %) rows)]
                (doseq [form [window absent grouped]]
                  (let [ordinary (fn [legacy? data]
                                   (binding [q/*query-result-cache?* false q/*disable-planner* legacy?]
                                     (set (q/q form db data))))]
                    (is (= (ordinary true rows) (ordinary false rows) (ordinary false wire)
                           (:rows (run form rows 4096)) (:rows (run form wire 4096)))
                        (pr-str [form rows])))))))
          (doseq [clause ['[(slurp ?target)] '[?e ?attribute ?target] '[?e :secret/id ?target]
                          '[?e :sample/n ?new] '(external ?target) '[(identity ?target) ?x]
                          '(not-join [?new] [?e :sample/n ?new])]]
            (let [form (conj '[:find ?target :in $ $delta :where [$delta ?lo ?hi ?target]] clause)]
              (is (thrown? Exception (contract/admit form attrs 3 (:schema db))))))
          (is (thrown? Exception
                       (contract/admit '[:find ?e :in $ $delta :where [$delta ?x] [?e :sample/id ?x]]
                                       attrs 1 (:schema db)))))
        (finally (d/release c) (d/delete-database cfg))))))

(deftest clause-and-fuel-conformance
  (let [cfg {:store {:backend :memory :id (random-uuid)} :schema-flexibility :write}]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :item/n :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one :db/index true}])
        (d/transact conn [{:item/n 1} {:item/n 2} {:item/n 2}])
        (let [db @conn
              attrs #{:item/n}
              head '[:find ?x :in $ $delta :where [$delta ?x]]
              ;; Explicit event counts include misses, duplicate results, terminal
              ;; cursor probes and successful existential short-circuiting.
              cases [[head [[1] [1]] {:input 2 :project 2}]
                     [(conj head '[_ :item/n ?x]) [[2] [3]]
                      {:input 2 :cursor 2 :advance 4 :project 2}]
                     [(conj head '(not-join [?x] [_ :item/n ?x])) [[2] [3]]
                      {:input 2 :branch 2 :cursor 2 :advance 2 :project 1}]
                     [(conj head '(or-join [?x] [(= ?x 1)] (and [(>= ?x 2)] [(< ?x 3)])))
                      [[1] [2] [3]] {:input 3 :branch 5 :predicate 7 :project 2}]
                     [(into head '[[?e :item/n ?v] [(>= ?v ?x)] [(<= ?v 2)]
                                   [?e :item/n _] [(> ?v 2)]])
                      [[1] [3]] {:input 2 :cursor 5 :advance 11 :predicate 3}]
                     ['[:find ?x (count ?v) (sum ?v) :with ?e :in $ $delta
                        :where [$delta ?x] [?e :item/n ?x] [?e :item/n ?v]]
                      [[2]] {:input 1 :cursor 3 :advance 7 :project 2 :aggregate 2}]
                     ['[:find ?x :in $ $delta :where [$delta ?x ?x _ :ok] [(not= ?x 2)]]
                      [[1 1 9 :ok] [1 2 9 :ok] [2 2 9 :ok] [3 3 9 :miss]]
                      {:input 4 :predicate 2 :project 1}]]]
          (doseq [[form rows events] cases]
            (let [width (count (first rows))
                  limit (reduce + (vals events))
                  run #(contract/execute form attrs width db rows %)
                  receipt (run limit)
                  ordinary (fn [legacy?]
                             (binding [q/*query-result-cache?* false q/*disable-planner* legacy?]
                               (set (q/q form db rows))))]
              (is (= :ok (:status receipt)) (pr-str form))
              (is (= (ordinary true) (ordinary false) (:rows receipt)) (pr-str form))
              (is (= events (into {} (filter (comp pos? val)) (:events receipt))) (pr-str form))
              (doseq [budget (range limit)]
                (let [r (run budget)]
                  (is (= [:out-of-fuel budget budget false]
                         [(:status r) (:used r) (reduce + (vals (:events r))) (contains? r :rows)])
                      (pr-str [form budget])))))))
        (finally (d/release conn) (d/delete-database cfg))))))

(deftest ordinary-forms-outside-the-profile-fail-admission
  (let [head '[:find ?x :in $ $delta :where [$delta ?x]]
        attrs #{:sample/n}
        schema {:sample/n {:db/valueType :db.type/long :db/index true}}]
    (doseq [form (concat
                  '[[:find ?x . :in $ $delta :where [$delta ?x]]
                    [:find [?x ...] :in $ $delta :where [$delta ?x]]
                    [:find (pull ?x [*]) :in $ $delta :where [$delta ?x]]
                    [:find (min ?x) :in $ $delta :where [$delta ?x]]
                    [:find ?x :in $ $delta % :where [$delta ?x]]
                    [:find ?x :with ?missing :in $ $delta :where [$delta ?x]]]
                  (map #(conj head %)
                       '[(not [_ :sample/n ?x]) (or [(= ?x 1)] [(= ?x 2)])
                         (reachable ?x) [(inc ?x) ?y] [?entity ?attr ?x]
                         [?entity :sample/n ?n] [?entity :sample/n ?x ?tx]
                         (or-join [?free] [_ :sample/n ?free])]))]
      (is (thrown? Exception (contract/admit form attrs 1 schema)) (pr-str form)))))

(deftest versioned-functions-and-binding-disjunction
  (let [cfg {:store {:backend :memory :id (random-uuid)} :schema-flexibility :write}]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :item/bucket :db/valueType :db.type/long
                           :db/index true :db/cardinality :db.cardinality/one}])
        (d/transact conn (vec (repeat 129 {:item/bucket 1})))
        (let [db @conn
              form '[:find ?x ?out :in $ $delta :where [$delta ?x]
                     (or-join [?x ?out] [(inc ?x) ?out] [(dec ?x) ?out]
                              (and [(inc ?x) ?local] [(inc ?local) ?tmp] [(dec ?tmp) ?out]))]
              run (fn [query rows budget] (contract/execute contract/version-2 query #{} 1 db rows budget))
              rows [[2] [4]]
              receipt (run form rows 28)]
          (doseq [legacy? [false true]]
            (binding [q/*query-result-cache?* false q/*disable-planner* legacy?]
              (is (= #{[2 3] [2 1] [4 5] [4 3]} (set (d/q form db rows)) (:rows receipt)))))
          (is (= {:input 2 :branch 6 :function 10 :union 6 :project 4}
                 (into {} (filter (comp pos? val)) (:events receipt))))
          (doseq [budget (range 28)]
            (let [r (run form rows budget)]
              (is (= [:out-of-fuel budget budget false]
                     [(:status r) (:used r) (reduce + (vals (:events r))) (contains? r :rows)]))))
          (doseq [[op x reason] [['inc Long/MAX_VALUE :numeric-overflow] ['dec Long/MIN_VALUE :numeric-overflow]
                                 ['inc :keyword :type-error]]]
            (let [f (conj '[:find ?out :in $ $delta :where [$delta ?x]] [(list op '?x) '?out])]
              (is (= [reason 2 nil] ((juxt :status :used :rows) (run f [[x]] 2))))))
          (let [union '(or-join [?x ?e] [?e :item/bucket ?x] [?e :item/bucket ?x])
                query (conj '[:find ?x :in $ $delta :where [$delta ?x]] union '[(< ?x 0)])
                r (contract/execute contract/version-2 query #{:item/bucket} 1 db [[1]] 4096)]
            (is (= [:relation-limit 129 0 false]
                   [(:status r) (get-in r [:events :union]) (get-in r [:events :project]) (contains? r :rows)])))
          (let [nested '[:find ?x :in $ $delta :where [$delta ?x]
                         (not-join [?x] (or-join [?x ?y] [(inc ?x) ?y] [(dec ?x) ?y]) [(> ?y 0)])]
                r (run nested [[0] [-2]] 64)]
            (is (= #{[-2]} (set (d/q nested db [[0] [-2]])) (:rows r)))
            (is (= 3 (get-in r [:events :union]))))
          (is (thrown? Exception (contract/admit form #{} 1 (:schema db))))
          (doseq [clause ['(or-join [?x ?out] [(inc ?x) ?out] [(= ?x 1)])
                          '(or-join [?x ?out] [?out :secret/n ?x])
                          '[(inc ?missing) ?out] '[(inc ?x) ?x] '[(identity ?x) ?out]]]
            (is (thrown? Exception (contract/admit contract/version-2
                                                   (conj '[:find ?x :in $ $delta :where [$delta ?x]] clause)
                                                   #{} 1 (:schema db))))))
        (finally (d/release conn) (d/delete-database cfg))))))

(deftest transition-snapshot-joins-and-fuel
  (let [cfg {:store {:backend :memory :id (random-uuid)} :schema-flexibility :write}
        _ (d/create-database cfg)
        c (d/connect cfg)]
    (try
      (d/transact c [{:db/ident :show/price :db/valueType :db.type/long
                      :db/index true :db/cardinality :db.cardinality/one}])
      (let [id (get (:tempids (d/transact c [{:db/id "show" :show/price 10}])) "show")
            before @c
            after (:db-after (d/with before [[:db/add id :show/price 20]]))
            sources {'$before before '$after after}
            schemas (update-vals sources :schema)
            rows [[id 10 12]]
            head '[:find ?e :in $before $after $delta :where [$delta ?e ?lo ?hi]]
            run (fn [q budget] (contract/execute contract/transition-profile q #{:show/price} 3 sources rows budget))]
        (doseq [q [(conj head '[$before ?e :show/price 10] '[$after ?e :show/price 20])
                   (conj head '[$before ?old :show/price 10] '[(= ?old ?e)])
                   (conj head '[$before ?old :show/price ?p] '[(>= ?p ?lo)] '[(<= ?p ?hi)]
                         '(not-join [?old] [$after ?old :show/price 10]))
                   (conj head '[$after ?e :show/price 10])]]
          (let [r (run q 128)]
            (is (= :ok (:status r)))
            (doseq [disable-planner? [false true]]
              (binding [q/*disable-planner* disable-planner? q/*query-result-cache?* false]
                (is (= (set (d/q q before after rows)) (:rows r)))))
            (is (= (assoc r :limit (:used r)) (run q (:used r))))
            (is (= :out-of-fuel (:status (run q (dec (:used r))))))))
        (doseq [clause ['[?e :show/price 10] '[$ ?e :show/price 10]
                        '[$other ?e :show/price 10] '[$before ?e :show/secret 10]]]
          (is (thrown? clojure.lang.ExceptionInfo (run (conj head clause) 128))))
        (let [q (conj head '[$before ?other :show/price 10])]
          (is (thrown? clojure.lang.ExceptionInfo
                       (contract/admit contract/transition-profile q #{:show/price} 3
                                       (update-in schemas ['$before :show/price] dissoc :db/index))))))
      (finally (d/release c) (d/delete-database cfg)))))
