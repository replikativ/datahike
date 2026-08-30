(ns datahike.test.query-limit-demand-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.query :as q]
   [datahike.query.execute :as execute]))

(defn- fixture-db []
  (d/db-with
   (db/empty-db nil {:schema-flexibility :write
                     :keep-history? true})
   (into
    [{:db/ident :probe/n
      :db/valueType :db.type/long
      :db/cardinality :db.cardinality/one
      :db/index true}
     {:db/ident :probe/group
      :db/valueType :db.type/keyword
      :db/cardinality :db.cardinality/one
      :db/index true}]
    (map (fn [i]
           {:db/id (+ 1000 i)
            :probe/n i
            :probe/group (if (< i 50) :x :y)})
         (range 100)))))

(defn- counting-cancel []
  (let [derefs (atom 0)]
    [derefs
     (reify clojure.lang.IDeref
       (deref [_]
         (swap! derefs inc)
         false))]))

(defn- uncached-q [query-map]
  (binding [q/*disable-planner* false
            q/*query-result-cache?* false]
    (d/q query-map)))

(defn strict-less-than?
  [left right]
  (< left right))

(deftest direct-limit-demand
  (let [db (fixture-db)]
    (testing "an unordered distinct scan stops after offset + limit tuples"
      (let [[derefs cancel] (counting-cancel)
            result (uncached-q
                    {:query '[:find ?e :where [?e :probe/n ?n]]
                     :args [db]
                     :offset 7
                     :limit 5
                     :cancel cancel})]
        (is (= 5 (count result)))
        (is (= 12 @derefs)
            "the direct scan must not traverse the remainder of the attribute")))

    (testing "prepared direct execution receives the same demand"
      (let [[derefs cancel] (counting-cancel)
            result (binding [q/*disable-planner* false
                             execute/*prepared-execution* true
                             q/*fold-scalar-ins* false
                             q/*query-result-cache?* false]
                     (d/q {:query '[:find ?e
                                    :in $ ?group
                                    :where [?e :probe/group ?group]]
                           :args [db :x]
                           :offset 7
                           :limit 5
                           :cancel cancel}))]
        (is (= 5 (count result)))
        (is (= 12 @derefs))))))

(deftest prepared-scalar-range-remains-an-index-bound
  (let [db (fixture-db)
        [derefs cancel] (counting-cancel)
        result (binding [q/*disable-planner* false
                         execute/*prepared-execution* true
                         q/*fold-scalar-ins* false
                         q/*query-result-cache?* false]
                 (d/q {:query '[:find ?e
                                :in $ ?upper
                                :where
                                [?e :probe/n ?n]
                                [(< ?n ?upper)]]
                       :args [db 10]
                       :cancel cancel}))]
    (is (= 10 (count result)))
    (is (< @derefs 20)
        "the value-free scalar parameter must bound AVET before scanning")
    (testing "the rebound survives a namespaced-predicate relation fallback"
      (let [[fallback-derefs fallback-cancel] (counting-cancel)
            fallback-result
            (binding [q/*disable-planner* false
                      execute/*prepared-execution* true
                      q/*fold-scalar-ins* false
                      q/*query-result-cache?* false]
              (d/q {:query '[:find ?e
                             :in $ ?upper
                             :where
                             [?e :probe/n ?n]
                             [(datahike.test.query-limit-demand-test/strict-less-than?
                               ?n ?upper)]
                             [(< ?n ?upper)]]
                    :args [db 10]
                    :cancel fallback-cancel}))]
        (is (= result fallback-result))
        (is (< @fallback-derefs 20))))
    (testing "a separate candidate collection does not hide the scalar bound"
      (let [candidates (mapv first
                             (d/q '[:find ?e :where [?e :probe/n ?n]] db))
            [mixed-derefs mixed-cancel] (counting-cancel)
            mixed-result
            (binding [q/*disable-planner* false
                      execute/*prepared-execution* true
                      q/*fold-scalar-ins* false
                      q/*query-result-cache?* false]
              (d/q {:query '[:find ?candidate
                             :in $ ?upper [?candidate ...]
                             :where
                             [?candidate :probe/n ?n]
                             [(datahike.test.query-limit-demand-test/strict-less-than?
                               ?n ?upper)]
                             [(< ?n ?upper)]]
                    :args [db 10 candidates]
                    :cancel mixed-cancel}))]
        (is (= result mixed-result))
        (is (< @mixed-derefs 20))))))

(deftest unsafe-demand-remains-unbounded
  (let [db (fixture-db)]
    (testing "a post-filter cannot under-fill a requested page"
      (let [[derefs cancel] (counting-cancel)
            result (uncached-q
                    {:query '[:find ?e
                              :where
                              [?e :probe/n ?n]
                              [(even? ?n)]]
                     :args [db]
                     :offset 7
                     :limit 5
                     :cancel cancel})]
        (is (= 5 (count result)))
        (is (= 100 @derefs)
            "scan candidates are filtered only after direct collection")))

    (testing "projection duplicates cannot consume the result demand"
      (let [[derefs cancel] (counting-cancel)
            result (uncached-q
                    {:query '[:find ?group
                              :where [?e :probe/group ?group]]
                     :args [db]
                     :limit 2
                     :cancel cancel})]
        (is (= #{[:x] [:y]} result))
        (is (= 100 @derefs)
            "both distinct values must survive post-scan hash deduplication")))

    (testing "ORDER BY still sees the complete input before sorting"
      (let [[derefs cancel] (counting-cancel)
            result (uncached-q
                    {:query '[:find ?e ?n :where [?e :probe/n ?n]]
                     :args [db]
                     :order-by '[?n :desc]
                     :limit 5
                     :cancel cancel})]
        (is (= [99 98 97 96 95] (mapv second result)))
        (is (= 100 @derefs))))

    (testing "offset + limit cannot overflow the executor's int counter"
      (let [[derefs cancel] (counting-cancel)
            result (uncached-q
                    {:query '[:find ?e :where [?e :probe/n ?n]]
                     :args [db]
                     :offset Integer/MAX_VALUE
                     :limit 1
                     :cancel cancel})]
        (is (empty? result))
        (is (= 100 @derefs))))

    (testing "historical scans retain their adjacent-dedup pass"
      (let [[derefs cancel] (counting-cancel)
            result (uncached-q
                    {:query '[:find ?e :where [?e :probe/n ?n]]
                     :args [(d/history db)]
                     :limit 1
                     :cancel cancel})]
        (is (= 1 (count result)))
        (is (= 100 @derefs))))))
