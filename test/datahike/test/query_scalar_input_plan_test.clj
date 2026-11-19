(ns datahike.test.query-scalar-input-plan-test
  "A query that runs with many distinct scalar :in values must not plan
   again for each value (#1107). It plans a bounded number of times, then
   keeps the scalar as an input relation so one plan serves every value."
  (:require
   [clojure.test :refer [deftest is testing]]
   [datahike.api :as d]
   [datahike.query :as dq]))

(def ^:private cfg
  {:store {:backend :memory :id (random-uuid)}
   :schema-flexibility :write
   :keep-history? false})

(defn- test-db []
  (d/delete-database cfg)
  (d/create-database cfg)
  (let [conn (d/connect cfg)]
    (d/transact conn [{:db/ident :s/name :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one :db/index true}
                      {:db/ident :s/group :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one :db/index true}
                      {:db/ident :s/kind :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one :db/index true}])
    (d/transact conn (mapv (fn [i] {:s/name (str "n" (mod i 50))
                                    :s/group (str "g" (mod i 7))
                                    :s/kind (str "k" (mod i 3))})
                           (range 500)))
    (d/db conn)))

(defn- count-plans
  "Run `f`, return the number of plans that the planner built."
  [f]
  (let [n (atom 0)
        orig @#'dq/create-plan-via-ir]
    (with-redefs [dq/create-plan-via-ir (fn [& args] (swap! n inc) (apply orig args))]
      (f))
    @n))

(deftest many-scalar-values-plan-a-bounded-number-of-times
  (let [db (test-db)
        q '[:find [?e ...]
            :in $ ?name ?group
            :where [?e :s/name ?name] [?e :s/group ?group] [?e :s/kind ?kind]]
        args (for [n (range 50) g (range 7)] [(str "n" n) (str "g" g)])
        plans (count-plans #(doseq [a args] (apply d/q q db a)))]
    (is (<= plans 10) (str plans " plans for " (count args) " distinct input tuples"))
    (testing "results do not change when the plan stops folding the values"
      (doseq [a args]
        (is (= (set (binding [dq/*disable-planner* true] (apply d/q q db a)))
               (set (apply d/q q db a))))))))

(deftest few-scalar-values-keep-value-specific-plans
  (let [db (test-db)
        q '[:find [?e ...]
            :in $ ?name
            :where [?e :s/name ?name] [?e :s/kind "k1"]]
        args (for [n (range 4)] [(str "n" n)])
        plans (count-plans #(dotimes [_ 5] (doseq [a args] (apply d/q q db a))))]
    (is (= 4 plans) "one plan per value: values stay folded below the limit")))

(deftest explicit-fold-setting-wins
  (let [db (test-db)
        q '[:find [?e ...]
            :in $ ?name
            :where [?e :s/name ?name] [?e :s/group "g1"] [?e :s/kind "k2"]]
        args (for [n (range 30)] [(str "n" n)])]
    (is (= 30 (count-plans #(binding [dq/*fold-scalar-ins* true]
                              (doseq [a args] (apply d/q q db a)))))
        "true always folds")
    (is (<= (count-plans #(binding [dq/*fold-scalar-ins* false]
                            (doseq [a (for [n (range 30 50)] [(str "n" n)])]
                              (apply d/q q db a))))
            1)
        "false never folds")))

(deftest unsafe-shapes-keep-folding
  (let [db (test-db)
        pred-q '[:find [?e ...]
                 :in $ ?name
                 :where [?e :s/name ?n] [(= ?n ?name)]]
        ref-q '[:find [?e ...]
                :in $ ?kind
                :where [?e :s/kind ?kind]]
        names (for [n (range 20)] [(str "n" n)])
        idents (for [n (range 20)] [(keyword (str "k" n))])]
    (testing "a scalar used in a predicate is folded for every value"
      (is (= 20 (count-plans #(doseq [a names] (apply d/q pred-q db a)))))
      (doseq [a names]
        (is (= (set (binding [dq/*disable-planner* true] (apply d/q pred-q db a)))
               (set (apply d/q pred-q db a))))))
    (testing "a keyword value is folded for every value"
      (is (= 20 (count-plans #(doseq [a idents] (apply d/q ref-q db a))))))))
