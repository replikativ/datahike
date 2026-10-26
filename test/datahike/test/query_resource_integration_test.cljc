(ns datahike.test.query-resource-integration-test
  (:require
   #?(:clj [clojure.test :refer [deftest is testing]]
      :cljs [cljs.test :refer-macros [deftest is testing]])
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.query :as q]
   [datahike.query.execute :as execute]
   [datahike.query.resource :as resource]))

(defn- failure-data [f]
  (try (f) nil
       (catch #?(:clj Exception :cljs :default) e
         (ex-data e))))

(defn- fixture []
  (d/db-with (db/empty-db {:left {:db/cardinality :db.cardinality/many}
                           :right {:db/cardinality :db.cardinality/many}})
             (concat (for [i (range 20)]
                       {:db/id (inc i) :n i :bucket :same})
                     [{:db/id 100 :left (vec (range 10)) :right (vec (range 10))}])))

(deftest disconnected-products-stop-at-the-retained-sink
  (let [db (fixture)]
    (doseq [legacy? [false true]
            query ['[:find ?a ?b :where [?a :n] [?b :n]]
                   '[:find (count ?a) . :with ?b :where [?a :n] [?b :n]]
                   '[:find ?a ?b :keys a b :where [?a :n] [?b :n]]]]
      (binding [q/*disable-planner* legacy?]
        ;; Prime the ordinary result cache, then prove a capped execution
        ;; cannot reuse that answer or leave a failed builder for the next call.
        (is (some? (d/q query db)))
        (let [failure (binding [resource/*max-intermediate-slots* 200]
                        (failure-data #(d/q query db)))]
          (is (= :query/intermediate-limit (:error failure)))
          (is (#{:cartesian-output :distinct-output} (:operator failure)))
          (is (<= (:retained failure) 200)))
        (is (some? (binding [resource/*max-intermediate-slots* 2000]
                     (d/q query db))))))))

(deftest planned-and-reference-joins-share-the-guard
  (let [db (fixture)]
    (doseq [legacy? [false true]
            query ['[:find ?a ?b :where [?a :bucket ?k] [?b :bucket ?k]]
                   '[:find ?l ?r :where [?e :left ?l] [?e :right ?r]]]]
      (binding [q/*disable-planner* legacy?]
        (let [expected (d/q query db)]
          (is (> (count expected) 50))
          (is (= :query/intermediate-limit
                 (:error (binding [resource/*max-intermediate-slots* 150]
                           (failure-data #(d/q query db))))))
          (is (= expected (binding [resource/*max-intermediate-slots* 5000]
                            (d/q query db)))))))))

(deftest materialized-scan-is-bounded-before-distinct-projection
  (let [db (fixture)]
    (doseq [legacy? [false true]]
      (binding [q/*disable-planner* legacy?]
        ;; DISTINCT keeps one result, but this executor first retains the scan.
        (is (= :query/intermediate-limit
               (:error (binding [resource/*max-intermediate-slots* 4]
                         (failure-data #(d/q '[:find ?k :where [?e :bucket ?k]] db))))))
        (is (= #{[:same]}
               (binding [resource/*max-intermediate-slots* 200]
                 (d/q '[:find ?k :where [?e :bucket ?k]] db))))))))

(deftest prepared-reexecution-uses-fresh-builders
  (let [db (fixture)
        query '[:find ?a ?b :in $ ?k :where [?a :bucket ?k] [?b :bucket ?k]]]
    (binding [q/*fold-scalar-ins* false execute/*prepared-execution* true]
      (is (= 400 (count (d/q query db :same))))
      (dotimes [_ 2]
        (is (= :query/intermediate-limit
               (:error (binding [resource/*max-intermediate-slots* 200]
                         (failure-data #(d/q query db :same)))))))
      (is (= #{} (binding [resource/*max-intermediate-slots* 200]
                   (d/q query db :absent))))
      (is (= 400 (count (binding [resource/*max-intermediate-slots* 5000]
                          (d/q query db :same))))))))

(deftest nested-predicate-resource-errors-escape
  (let [db (fixture)
        inner '[:find ?a ?b :where [?a :n] [?b :n]]
        pred (fn [_] (d/q inner db) true)
        outer '[:find ?e :in $ ?pred :where [?e :n] [(?pred ?e)]]]
    (doseq [legacy? [false true]]
      (binding [q/*disable-planner* legacy?
                resource/*max-intermediate-slots* 200]
        (is (= :query/intermediate-limit
               (:error (failure-data #(d/q outer db pred)))))))))

(deftest function-products-share-the-retained-production-account
  (let [query '[:find ?a ?b :in [?a ...] ?f :where [(?f ?a) [?b ...]]]
        allocations (atom 0)
        original q/join-tuples]
    (with-redefs [q/join-tuples (fn [& args]
                                  (swap! allocations inc)
                                  (apply original args))]
      (binding [resource/*max-intermediate-slots* 9]
        (let [failure (failure-data #(d/q query [1 2 3] (fn [_] [10 11 12])))]
          (is (= :query/intermediate-limit (:error failure)))
          (is (= :function-production (:operator failure)))
          (is (= 3 @allocations)
              "the second product is rejected before its joined tuple is allocated"))))))

(deftest filtered-sources-preserve-results-and-policy
  (let [db (d/filter (fixture) (fn [_ datom] (not= :right (:a datom))))
        query '[:find ?a ?b :where [?a :n] [?b :n]]]
    (doseq [legacy? [false true]]
      (binding [q/*disable-planner* legacy?]
        (is (= 400 (count (binding [resource/*max-intermediate-slots* 2000]
                            (d/q query db)))))
        (is (= :query/intermediate-limit
               (:error (binding [resource/*max-intermediate-slots* 200]
                         (failure-data #(d/q query db))))))))))

(deftest rule-branch-products-are-checked-before-union-growth
  (let [db (d/db-with (db/empty-db)
                      (concat (for [i (range 20)] {:db/id (inc i) :a :same})
                              (for [i (range 20)] {:db/id (+ 100 i) :b :same})))
        rules '[[(r ?x ?y) [?a :a ?x] [?b :b ?y]]]
        query '[:find ?x ?y :in $ % :where (r ?x ?y)]]
    (binding [q/*disable-planner* true]
      (is (= #{[:same :same]} (d/q query db rules)))
      (is (= :query/intermediate-limit
             (:error (binding [resource/*max-intermediate-slots* 4]
                       (failure-data #(d/q query db rules)))))))))
