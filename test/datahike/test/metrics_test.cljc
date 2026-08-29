(ns datahike.test.metrics-test
  (:require #?(:clj [clojure.test :refer [deftest is testing]]
               :cljs [cljs.test :refer [deftest is testing]])
            [datahike.metrics :as dhm]
            [datahike.query :as query]
            [datahike.lru :as lru]
            [replikativ.metrics :as metrics]
            #?(:clj [datahike.api :as d])))

(def test-id #uuid "1065f77a-f8e7-4fd8-91d8-d60fc51ca441")

(def test-config
  {:store {:backend :memory :id test-id}
   :branch :audit
   :schema-flexibility :read})

(def labels {:database (str test-id) :branch "audit"})

(defn- fresh-registry! []
  (metrics/reset!)
  (dhm/describe!))

(deftest labels-preserve-the-full-branch-identity
  (is (= {:database (str test-id) :branch "tenant/audit"}
         (dhm/db-labels (assoc test-config :branch :tenant/audit)))))

(deftest descriptions-can-be-restored-after-a-registry-reset
  (fresh-registry!)
  (let [snapshot (metrics/snapshot)]
    (is (= :histogram (get-in snapshot [:datahike_commit_seconds :type])))
    (is (= :counter (get-in snapshot [:datahike_transactions_total :type])))
    (is (= :counter (get-in snapshot [:datahike_transacted_datoms_total :type])))
    (is (= :counter (get-in snapshot [:datahike_head_conflicts_total :type])))
    (is (= :histogram (get-in snapshot [:datahike_query_seconds :type])))
    (is (= :histogram (get-in snapshot [:datahike_query_planning_seconds :type])))
    (is (= :counter (get-in snapshot [:datahike_query_engine_total :type])))
    (is (= :counter (get-in snapshot [:datahike_query_errors_total :type])))
    (is (= :counter (get-in snapshot [:datahike_query_planning_errors_total :type])))
    (is (= 256 (get-in snapshot [:datahike_query_sample_every :series {}])))
    (is (every? seq (map :help (vals snapshot)))
        "every exported Datahike metric explains what it measures")
    (is (every? empty? (map :series (vals (dissoc snapshot :datahike_query_sample_every))))
        "only the sampling configuration gauge has a startup sample")))

(deftest one-durable-batch-counts-every-transaction
  (fresh-registry!)
  (dhm/commit! test-config 125 3 7)
  (let [snapshot (metrics/snapshot)
        duration (get-in snapshot [:datahike_commit_seconds :series labels])]
    (testing "one batch has one duration but can make several transactions durable"
      (is (= 1 (:count duration)))
      (is (= 0.125 (:sum duration)))
      (is (= 3 (get-in snapshot [:datahike_transactions_total :series labels])))
      (is (= 7 (get-in snapshot [:datahike_transacted_datoms_total :series labels]))))))

(deftest conflicts-are-labelled-by-caller-outcome
  (fresh-registry!)
  (dhm/head-conflict! test-config :retried)
  (dhm/head-conflict! test-config :retried)
  (dhm/head-conflict! test-config :failed)
  (let [series (get-in (metrics/snapshot) [:datahike_head_conflicts_total :series])]
    (is (= 2 (get series (assoc labels :outcome "retried"))))
    (is (= 1 (get series (assoc labels :outcome "failed"))))))

(deftest connection-samples-omit-reservations-and-preserve-branches
  (let [samples (dhm/connection-samples
                 (atom {[test-id :db]           {:conn ::db :count 3}
                        [test-id :tenant/audit] {:conn ::audit :count 1}
                        [(random-uuid) :db]      {:conn nil :count 0}}))]
    (is (= #{{:name   :datahike_connections
              :type   :gauge
              :help   "Connection leases held in this process, by database and branch."
              :labels {:database (str test-id) :branch "db"}
              :value  3}
             {:name   :datahike_connections
              :type   :gauge
              :help   "Connection leases held in this process, by database and branch."
              :labels {:database (str test-id) :branch "tenant/audit"}
              :value  1}}
           (set samples)))))

#?(:clj
   (deftest runtime-snapshot-uses-only-live-state-and-the-metrics-snapshot
     (let [connections (atom {[test-id :db]
                              {:conn {:wrapped-atom (atom {:max-tx 42})}
                               :count 2}})
           snapshot {:datahike_query_sample_every
                     {:series {{} 256}}
                     :datahike_query_seconds
                     {:series {{:outcome "success" :result_cache "hit"}
                               {:count 3 :sum 0.03}
                               {:outcome "success" :result_cache "miss"}
                               {:count 1 :sum 0.02}}}
                     :datahike_query_errors_total
                     {:series {{:result_cache "miss"} 2}}
                     :datahike_query_planning_seconds
                     {:series {{:outcome "success" :plan_cache "hit"}
                               {:count 2 :sum 0.004}}}
                     :datahike_transactions_total
                     {:series {labels 3}}
                     :datahike_transacted_datoms_total
                     {:series {labels 7}}
                     :datahike_commit_seconds
                     {:series {labels {:count 2 :sum 0.125}}}
                     :datahike_head_conflicts_total
                     {:series {(assoc labels :outcome "retried") 1}}}
           status (dhm/runtime-snapshot connections snapshot)]
       (is (= {:sampled-queries 4
               :average-query-ms 12.5
               :result-cache {:hit 3 :miss 1}
               :query-errors 2
               :sampled-plans 2
               :average-planning-ms 2.0
               :plan-cache {:hit 2}
               :planning-errors 0
               :query-sample-every 256}
              (:node status)))
       (is (= {:loaded? true
               :leases 2
               :branches ["db"]
               :basis-t 42
               :transactions 3
               :transacted-datoms 7
               :commits 2
               :average-commit-ms 62.5
               :head-conflicts 1}
              (get-in status [:databases (str test-id)]))))))

#?(:clj
   (deftest a-real-transaction-records-the-durable-result
     (let [config (assoc test-config :branch :db)]
       (when (d/database-exists? config)
         (d/delete-database config))
       (d/create-database config)
       (let [conn (d/connect config)]
         (try
           ;; Database creation is itself durable but is not a user transaction.
           ;; Isolate the transaction below after the connection is established.
           (fresh-registry!)
           (let [report   (d/transact conn [{:name "Ada"} {:name "Grace"}])
                 expected (count (:tx-data report))
                 labels   {:database (str test-id) :branch "db"}
                 snapshot (metrics/snapshot)]
             (is (= 1 (get-in snapshot [:datahike_commit_seconds :series labels :count])))
             (is (= 1 (get-in snapshot [:datahike_transactions_total :series labels])))
             (is (= expected
                    (get-in snapshot [:datahike_transacted_datoms_total :series labels]))))
           (finally
             (d/release conn)
             (d/delete-database config)))))))

#?(:clj
   (deftest queries-report-result-and-plan-cache-dispositions
     (let [config (-> test-config
                      (assoc :branch :db)
                      (assoc-in [:store :id] (random-uuid)))]
       (when (d/database-exists? config)
         (d/delete-database config))
       (d/create-database config)
       (let [conn (d/connect config)
             qform '[:find ?name :where [?e :name ?name]]]
         (try
           (d/transact conn [{:name "Ada"}])
           (query/clear-query-cache!)
           (vreset! @#'query/plan-cache (lru/lru query/lru-cache-size))
           (fresh-registry!)

           ;; This assertion is specifically about the planner cache. CI also
           ;; runs the whole suite with the planner disabled globally to cover
           ;; the base engine, so pin the subject of this test explicitly.
           (binding [query/*disable-planner* false
                     dhm/*query-metrics-sample-every* 1]
             (is (= #{["Ada"]} (d/q qform @conn)))
             (is (= #{["Ada"]} (d/q qform @conn)))

             ;; Force another execution of the same form without throwing away
             ;; its plan, so both plan-cache dispositions are observable.
             (query/clear-query-cache!)
             (is (= #{["Ada"]} (d/q qform @conn))))

           (let [snapshot (metrics/snapshot)
                 query-series (get-in snapshot [:datahike_query_seconds :series])
                 planning-series (get-in snapshot [:datahike_query_planning_seconds :series])
                 engine-series (get-in snapshot [:datahike_query_engine_total :series])]
             (is (= 2 (get-in query-series [{:outcome "success" :result_cache "miss"} :count])))
             (is (= 1 (get-in query-series [{:outcome "success" :result_cache "hit"} :count])))
             (is (= 1 (get-in planning-series [{:outcome "success" :plan_cache "miss"} :count])))
             (is (= 1 (get-in planning-series [{:outcome "success" :plan_cache "hit"} :count])))
             (is (= 2 (reduce + 0 (vals engine-series)))
                 "a result-cache hit does not execute an engine"))

           (finally
             (d/release conn)
             (d/delete-database config)))))))

(deftest query-metrics-can-be-disabled-for-overhead-measurement
  (fresh-registry!)
  (binding [dhm/*query-metrics?* false]
    (let [started (dhm/query-timer)]
      (is (nil? started))
      (dhm/query! started :bypass :success false)
      (dhm/query-planning! started :hit :success)
      (dhm/query-engine! :planner :direct)))
  (let [snapshot (metrics/snapshot)]
    (is (empty? (get-in snapshot [:datahike_query_seconds :series])))
    (is (empty? (get-in snapshot [:datahike_query_planning_seconds :series])))
    (is (empty? (get-in snapshot [:datahike_query_engine_total :series])))))

(deftest unsampled-query-errors-are-still-counted
  (fresh-registry!)
  (dhm/query! nil :miss :error false)
  (binding [dhm/*record-query-metrics?* false]
    (dhm/query-planning! nil :miss :error))
  (let [snapshot (metrics/snapshot)]
    (is (= 1 (get-in snapshot [:datahike_query_errors_total
                               :series {:result_cache "miss"}])))
    (is (= 1 (get-in snapshot [:datahike_query_planning_errors_total
                               :series {:plan_cache "miss"}])))
    (is (empty? (get-in snapshot [:datahike_query_seconds :series])))
    (is (empty? (get-in snapshot [:datahike_query_planning_seconds :series])))))
