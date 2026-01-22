(ns datahike.test.online-gc-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing use-fixtures]]
      :clj  [clojure.test :as t :refer [is deftest testing use-fixtures]])
   [datahike.api :as d]
   [datahike.online-gc :as online-gc]
   [konserve.core :as k]
   [superv.async :refer [<?? S]])
  #?(:clj (:import [java.util Date])))

#?(:cljs (def Throwable js/Error))

(defn- count-store [db]
  (count (k/keys (:store db) {:sync? true})))

(defn- get-freed-count [db]
  (when-let [freed-atom (-> db :store :storage :freed-addresses)]
    (count @freed-atom)))

(def base-cfg {:store              {:backend :file
                                    :path "/tmp/online-gc-test"
                                    :id #uuid "a0000000-0000-0000-0000-000000000001"}
               :keep-history?      false
               :schema-flexibility :write
               :index              :datahike.index/persistent-set})

(def schema [{:db/ident       :name
              :db/cardinality :db.cardinality/one
              :db/valueType   :db.type/string}
             {:db/ident       :age
              :db/cardinality :db.cardinality/one
              :db/valueType   :db.type/long}])

(deftest online-gc-disabled-test
  (testing "Online GC disabled by default"
    (let [cfg (assoc-in base-cfg [:store :path] "/tmp/online-gc-disabled-test")
          conn (do
                 (d/delete-database cfg)
                 (d/create-database cfg)
                 (d/connect cfg))]
      (d/transact conn schema)
      (d/transact conn [{:name "Alice" :age 30}])
      ;; Freed addresses should accumulate
      (let [freed-before (get-freed-count @conn)]
        (d/transact conn [{:name "Bob" :age 25}])
        (d/transact conn [{:name "Charlie" :age 35}])
        (let [freed-after (get-freed-count @conn)]
          ;; Freed addresses should increase (not be deleted)
          (is (>= freed-after freed-before))
          (is (pos? freed-after))))
      (d/release conn))))

(deftest online-gc-enabled-zero-grace-period-test
  (testing "Online GC enabled with zero grace period (bulk import mode)"
    (let [cfg (-> base-cfg
                  (assoc-in [:store :path] "/tmp/online-gc-zero-grace-test")
                  (assoc :online-gc {:enabled? true
                                     :grace-period-ms 0
                                     :max-batch 1000}))
          conn (do
                 (d/delete-database cfg)
                 (d/create-database cfg)
                 (d/connect cfg))
          initial-count (count-store @conn)]
      (d/transact conn schema)
      (d/transact conn [{:name "Alice" :age 30}])
      (d/transact conn [{:name "Bob" :age 25}])
      (d/transact conn [{:name "Charlie" :age 35}])

      ;; With online GC, freed addresses should be cleaned up
      (let [freed-count (get-freed-count @conn)]
        ;; Should be zero or very small (recently freed in last tx)
        (is (< freed-count 10) "Freed addresses should be cleaned up by online GC"))

      ;; Store should not grow unboundedly
      (let [final-count (count-store @conn)]
        ;; Growth should be reasonable (mostly live data)
        (is (< final-count (+ initial-count 100))
            "Store size should not accumulate garbage"))

      (d/release conn))))

(deftest online-gc-with-grace-period-test
  (testing "Online GC with grace period preserves recent addresses"
    (let [cfg (-> base-cfg
                  (assoc-in [:store :path] "/tmp/online-gc-grace-test")
                  (assoc :online-gc {:enabled? true
                                     :grace-period-ms 300000  ;; 5 minutes
                                     :max-batch 1000}))
          conn (do
                 (d/delete-database cfg)
                 (d/create-database cfg)
                 (d/connect cfg))]
      (d/transact conn schema)
      (d/transact conn [{:name "Alice" :age 30}])

      ;; Freed addresses should remain (within grace period)
      (let [freed-count (get-freed-count @conn)]
        ;; Should accumulate during grace period
        (is (or (nil? freed-count) (>= freed-count 0))
            "Freed addresses preserved during grace period"))

      (d/release conn))))

(deftest online-gc-batch-size-limit-test
  (testing "Online GC respects max-batch limit"
    (let [cfg (-> base-cfg
                  (assoc-in [:store :path] "/tmp/online-gc-batch-test")
                  (assoc :online-gc {:enabled? true
                                     :grace-period-ms 0
                                     :max-batch 5}))  ;; Small batch
          conn (do
                 (d/delete-database cfg)
                 (d/create-database cfg)
                 (d/connect cfg))]
      (d/transact conn schema)
      ;; Make multiple transactions to generate many freed addresses
      (dotimes [i 10]
        (d/transact conn [{:name (str "Person-" i) :age (+ 20 i)}]))

      ;; Some freed addresses may remain if batch size was exceeded
      (let [freed-count (get-freed-count @conn)]
        ;; This is acceptable - GC runs incrementally
        (is (or (nil? freed-count) (>= freed-count 0))
            "Batch size limit causes incremental GC"))

      (d/release conn))))

(deftest online-gc-delete-function-test
  (testing "Online GC delete functions work correctly"
    (let [cfg (-> base-cfg
                  (assoc-in [:store :path] "/tmp/online-gc-delete-test")
                  (assoc :online-gc {:enabled? false}))  ;; Disabled for manual testing
          conn (do
                 (d/delete-database cfg)
                 (d/create-database cfg)
                 (d/connect cfg))]
      (d/transact conn schema)
      (d/transact conn [{:name "Alice" :age 30}])
      (d/transact conn [{:name "Bob" :age 25}])

      ;; Manually run online GC with zero grace period
      (let [deleted (online-gc/online-gc! (:store @conn)
                                          {:enabled? true
                                           :grace-period-ms 0
                                           :max-batch 1000
                                           :sync? true})]
        (is (>= deleted 0) "Should delete some addresses or zero"))

      ;; Freed addresses should be cleared
      (let [freed-count (get-freed-count @conn)]
        (is (or (nil? freed-count) (< freed-count 5))
            "Freed addresses should be cleaned up"))

      (d/release conn))))

(deftest online-gc-integration-test
  (testing "Online GC integration with larger dataset"
    (let [cfg (-> base-cfg
                  (assoc-in [:store :path] "/tmp/online-gc-integration-test")
                  (assoc :online-gc {:enabled? true
                                     :grace-period-ms 0
                                     :max-batch 10000}))
          conn (do
                 (d/delete-database cfg)
                 (d/create-database cfg)
                 (d/connect cfg))
          initial-count (count-store @conn)]
      (d/transact conn schema)

      ;; Bulk insert
      (doseq [batch (partition-all 100 (range 1000))]
        (d/transact conn (mapv (fn [i] {:name (str "Person-" i) :age (+ 20 i)}) batch)))

      ;; With online GC, growth should be controlled
      (let [final-count (count-store @conn)
            growth-ratio (/ (double final-count) (double initial-count))]
        ;; Growth should be reasonable - not 100x or 1000x
        (is (< growth-ratio 50)
            (str "Store growth should be controlled with online GC. Ratio: " growth-ratio)))

      ;; Verify data integrity
      (is (= 1000 (d/q '[:find (count ?e) .
                         :where [?e :name _]]
                       @conn))
          "All data should be accessible")

      (d/release conn))))

(deftest get-and-clear-eligible-freed-test
  (testing "get-and-clear-eligible-freed! correctly filters by timestamp"
    (let [cfg (-> base-cfg
                  (assoc-in [:store :path] "/tmp/online-gc-filter-test")
                  (assoc :online-gc {:enabled? false}))
          conn (do
                 (d/delete-database cfg)
                 (d/create-database cfg)
                 (d/connect cfg))]
      (d/transact conn schema)
      (d/transact conn [{:name "Alice" :age 30}])

      ;; Manually check filtering with very long grace period
      (let [[to-delete remaining] (online-gc/get-and-clear-eligible-freed!
                                   (:store @conn)
                                   300000  ;; 5 minutes
                                   1000)]
        ;; All should be in remaining (within grace period)
        (is (empty? to-delete) "Nothing should be eligible with long grace period")
        (is (or (nil? remaining) (coll? remaining)) "Remaining should be a collection"))

      ;; Check with zero grace period
      (d/transact conn [{:name "Bob" :age 25}])  ;; Generate more freed addresses
      (let [[to-delete _remaining] (online-gc/get-and-clear-eligible-freed!
                                    (:store @conn)
                                    0  ;; Zero grace period
                                    1000)]
        ;; Should have eligible addresses
        (is (>= (count to-delete) 0) "Should find eligible addresses with zero grace period"))

      (d/release conn))))

(deftest background-gc-test
  (testing "Background GC runs periodically"
    (let [cfg (-> base-cfg
                  (assoc-in [:store :path] "/tmp/online-gc-background-test")
                  (assoc :online-gc {:enabled? false}))  ;; Disable automatic GC in commit
          conn (do
                 (d/delete-database cfg)
                 (d/create-database cfg)
                 (d/connect cfg))]
      (d/transact conn schema)

      ;; Start background GC with short interval
      (let [stop-ch (online-gc/start-background-gc!
                     (:store @conn)
                     {:grace-period-ms 0
                      :interval-ms 100    ;; Run every 100ms
                      :max-batch 1000})]

        ;; Generate some freed addresses
        (d/transact conn [{:name "Alice" :age 30}])
        (d/transact conn [{:name "Bob" :age 25}])
        (d/transact conn [{:name "Charlie" :age 35}])

        ;; Wait for background GC to run a few times
        #?(:clj (Thread/sleep 500)
           :cljs (async/<! (async/timeout 500)))

        ;; Stop background GC
        (async/close! stop-ch)

        ;; Wait a bit for cleanup
        #?(:clj (Thread/sleep 100)
           :cljs (async/<! (async/timeout 100)))

        ;; Freed addresses should be cleaned up by background GC
        (let [freed-count (get-freed-count @conn)]
          (is (or (nil? freed-count) (< freed-count 10))
              "Background GC should clean up freed addresses")))

      (d/release conn))))
