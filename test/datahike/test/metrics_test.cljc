(ns datahike.test.metrics-test
  (:require #?(:clj [clojure.test :refer [deftest is testing]]
               :cljs [cljs.test :refer [deftest is testing]])
            [datahike.metrics :as dhm]
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
    (is (every? seq (map :help (vals snapshot)))
        "every exported Datahike metric explains what it measures")
    (is (every? empty? (map :series (vals snapshot)))
        "describing instruments does not manufacture samples")))

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
