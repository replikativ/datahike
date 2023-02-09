(ns datahike.test.query-stats-test
  (:require #?(:cljs [cljs.test :as t :refer-macros [is deftest use-fixtures]]
               :clj [clojure.test :as t :refer [is deftest use-fixtures]])
            [clojure.string :as str]
            [clojure.walk :as cw]
            [datahike.api :as d]
            [datahike.query :refer [query-stats]]
            [datahike.test.utils :refer [with-db]]))

(def config (d/load-config))

(def test-schema [{:db/ident :name
                   :db/valueType :db.type/string
                   :db/cardinality :db.cardinality/one}
                  {:db/ident :age
                   :db/valueType :db.type/long
                   :db/cardinality :db.cardinality/one}])

(def test-data [{:db/id 1 :name "Ivan" :age 10}
                {:db/id 2 :name "Ivan" :age 20}
                {:db/id 3 :name "Oleg" :age 30}
                {:db/id 4 :name "Oleg" :age 40}
                {:db/id 5 :name "Ivan" :age 50}
                {:db/id 6 :name "Michal" :age 60}])

(use-fixtures :once (partial with-db config (into test-schema test-data)))

(defn unify-stats [stats]
  (cw/postwalk
   #(cond
      (and (map? %) (contains? % :t))                  (assoc % :t :measurement)
      (and (symbol? %) (re-find #"__auto__" (name %))) (symbol (str/replace (name %) #"__auto__\d*" "_tmp"))
      :else                                            %)
   stats))

(deftest test-not
  (is (= {:consts {}
          :query  '{:find  [[?a ...]]
                    :where [[?e :age ?a]
                            (not [?e :age 60])]}
          :ret    [10 20 40 30 50]
          :rules  {}
          :stats  [{:clause '[?e :age ?a]
                    :rels   [{:bound #{'?a '?e} :rows  6}]
                    :t      :measurement
                    :type   :lookup}
                   {:branches [{:clause '[?e :age 60]
                                :rels   [{:bound #{'?a '?e} :rows 1}]
                                :t      :measurement
                                :type   :lookup}]
                    :clause   '(not [?e :age 60])
                    :rels     [{:bound #{'?a '?e} :rows 5}]
                    :t        :measurement
                    :type     :not}]}
         (unify-stats (query-stats '[:find [?a ...] :where
                                     [?e :age ?a]
                                     (not [?e :age 60])]
                                   @(d/connect config))))))

(deftest test-not-join
  (is (= {:consts {}
          :query  '{:find  [?a]
                    :where [[?e :name]
                            [?e :age ?a]
                            (not-join [?e]
                                      [?e :name "Oleg"]
                                      [?e :age ?a])]}
          :ret    #{[10] [20] [50] [60]}
          :rules  {}
          :stats  [{:clause '[?e :name]
                    :rels   [{:bound #{'?e} :rows  6}]
                    :t      :measurement
                    :type   :lookup}
                   {:clause '[?e :age ?a]
                    :rels   [{:bound #{'?a '?e} :rows  6}]
                    :t      :measurement
                    :type   :lookup}
                   {:branches [{:clause '[?e :name "Oleg"]
                                :rels   [{:bound #{'?e} :rows  2}]
                                :t      :measurement
                                :type   :lookup}
                               {:clause '[?e :age ?a]
                                :rels   [{:bound #{'?a '?e} :rows  2}]
                                :t      :measurement
                                :type   :lookup}]
                    :clause   '(not-join [?e]
                                         [?e :name "Oleg"]
                                         [?e :age ?a])
                    :rels     [{:bound #{'?a '?e} :rows  4}]
                    :t        :measurement
                    :type     :not}]}
         (unify-stats (query-stats '[:find ?a :where
                                     [?e :name]
                                     [?e :age ?a]
                                     (not-join [?e]
                                               [?e :name "Oleg"]
                                               [?e :age ?a])]
                                   @(d/connect config))))))

(deftest test-or
  (is (= {:consts {}
          :query  '{:find  [?a]
                    :where [[?e :age ?a]
                            (or [?e :name "Ivan"]
                                [?e :name "Oleg"])]}
          :ret    #{[10] [20] [30] [40] [50]}
          :rules  {}
          :stats  [{:clause '[?e :age ?a]
                    :rels   [{:bound #{'?a '?e}
                              :rows  6}]
                    :t      :measurement
                    :type   :lookup}
                   {:branches [[{:clause '[?e :name "Ivan"]
                                 :rels   [{:bound #{'?a '?e} :rows  3}]
                                 :t      :measurement
                                 :type   :lookup}]
                               [{:clause '[?e :name "Oleg"]
                                 :rels   [{:bound #{'?a '?e} :rows  2}]
                                 :t      :measurement
                                 :type   :lookup}]]
                    :clause   '(or [?e :name "Ivan"]
                                   [?e :name "Oleg"])
                    :rels     [{:bound #{'?a '?e} :rows  5}]
                    :t        :measurement
                    :type     :or}]}
         (unify-stats (query-stats '[:find ?a :where
                                     [?e :age ?a]
                                     (or [?e :name "Ivan"]
                                         [?e :name "Oleg"])]
                                   @(d/connect config))))))

(deftest test-rules
  (let [db [[1 :follow 2]
            [2 :follow 3]
            [2 :follow 4]
            [3 :follow 4]
            [4 :follow 6]
            [5 :follow 3]]]

    (is (= {:consts {},
            :query '{:find [?y ?x], :in [$ %], :where [[_ _ ?x] (rule ?x ?y) [(even? ?x)]]},
            :ret #{[3 2] [4 2] [6 4]},
            :rules {'rule '[[(rule ?a ?b) [?a :follow ?b]]]},
            :stats [{:clause '[_ _ ?x], :rels [{:bound #{'?x}, :rows 6}], :t :measurement, :type :lookup}
                    {:branches [{:branches [],
                                 :clause '(rule ?x ?y),
                                 :clauses (),
                                 :rels [{:bound #{'?x}, :rows 6}],
                                 :t :measurement,
                                 :type :solve}
                                {:branches [{:clause '[?x :follow ?y],
                                             :rels [{:bound #{'?x '?y}, :rows 6}],
                                             :t :measurement,
                                             :type :lookup}],
                                 :clause '([?x :follow ?y]),
                                 :clauses '([?x :follow ?y]),
                                 :rels [{:bound #{'?x '?y}, :rows 6}],
                                 :t :measurement,
                                 :type :solve}],
                     :clause '(rule ?x ?y),
                     :rels [{:bound #{'?x '?y}, :rows 10}],
                     :t :measurement,
                     :type :rule}
                    {:clause '[(even? ?x)], :rels [{:bound #{'?x '?y}, :rows 6}], :t :measurement}]}
           (unify-stats (query-stats '[:find ?y ?x
                                       :in $ %
                                       :where [_ _ ?x]
                                       (rule ?x ?y)
                                       [(even? ?x)]]
                                     db
                                     '[[(rule ?a ?b)
                                        [?a :follow ?b]]]))))

    (is (= {:consts '{?e1 1},
            :query {:find '[?e2], :in '[$ ?e1 %], :where '[(follow ?e1 ?e2)]},
            :ret #{[2] [3] [4]},
            :rules {'follow '[[(follow ?e2 ?e1) [?e2 :follow ?e1]]
                              [(follow ?e2 ?e1) [?e2 :follow ?t] [?t :follow ?e1]]]},
            :stats [{:branches [{:branches [],
                                 :clause '(follow ?e1 ?e2),
                                 :clauses (),
                                 :rels [],
                                 :t :measurement,
                                 :type :solve}
                                {:branches [{:clause '[?e1 :follow ?e2],
                                             :rels [{:bound #{'?e1 '?e2}, :rows 1}],
                                             :t :measurement,
                                             :type :lookup}],
                                 :clause '([?e1 :follow ?e2]),
                                 :clauses '([?e1 :follow ?e2]),
                                 :rels [{:bound #{'?e1 '?e2}, :rows 1}],
                                 :t :measurement,
                                 :type :solve}
                                {:branches '[{:clause [?e1 :follow ?t_tmp],
                                              :rels [{:bound #{?e1 ?t_tmp}, :rows 1}],
                                              :t :measurement,
                                              :type :lookup}
                                             {:clause [?t_tmp :follow ?e2],
                                              :rels [{:bound #{?e1 ?e2 ?t_tmp}, :rows 2}],
                                              :t :measurement,
                                              :type :lookup}],
                                 :clause '([?e1 :follow ?t_tmp] [?t_tmp :follow ?e2]),
                                 :clauses '([?e1 :follow ?t_tmp] [?t_tmp :follow ?e2]),
                                 :rels [{:bound #{'?e1 '?e2 '?t_tmp}, :rows 2}],
                                 :t :measurement,
                                 :type :solve}],
                     :clause '(follow ?e1 ?e2),
                     :rels [{:bound #{'?e1 '?e2}, :rows 3}],
                     :t :measurement,
                     :type :rule}]}
           (unify-stats (query-stats '[:find  ?e2
                                       :in    $ ?e1 %
                                       :where (follow ?e1 ?e2)]
                                     db
                                     1
                                     '[[(follow ?e2 ?e1)
                                        [?e2 :follow ?e1]]
                                       [(follow ?e2 ?e1)
                                        [?e2 :follow ?t]
                                        [?t  :follow ?e1]]]))))))
