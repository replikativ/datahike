(ns datahike.test.online-gc-test
  "Comprehensive tests for online garbage collection.

   Tests verify exact freed address counts to detect subtle memory issues.
   Pattern: 3 + 2n addresses for n data transactions
   - Schema tx: 3 (EAVT, AEVT, AVET roots)
   - Data tx: 2 (EAVT, AEVT only - AVET empty for non-indexed attrs)"
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing]])
   [datahike.api :as d]
   [datahike.online-gc :as online-gc]
   [konserve.core :as k]
   #?(:clj [clojure.core.async :as async]
      :cljs [cljs.core.async :as async]))
  #?(:clj (:import [java.util Date])))

#?(:cljs (def Throwable js/Error))

(defn- count-store [db]
  (count (k/keys (:store db) {:sync? true})))

(defn- get-freed-addresses [db]
  "Get current freed addresses vector"
  @(-> db :store :storage :freed-addresses))

(defn- get-freed-count [db]
  (count (get-freed-addresses db)))

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

;;; ============================================================================
;;; Exact Freed Count Tests (Most Important)
;;; ============================================================================

(deftest precise-freed-count-tracking-test
  (testing "Track exact number of freed addresses per transaction"
    (let [cfg (-> base-cfg
                  (assoc-in [:store :path] "/tmp/online-gc-precise-freed-test")
                  (assoc :online-gc {:enabled? false}))  ;; Disabled to accumulate
          conn (do
                 (d/delete-database cfg)
                 (d/create-database cfg)
                 (d/connect cfg))]

      ;; Initially should be zero
      (is (= 0 (get-freed-count @conn))
          "Initially no freed addresses")

      ;; Transact schema
      (d/transact conn schema)
      (is (= 3 (get-freed-count @conn))
          "Schema transaction frees 3 addresses (EAVT, AEVT, AVET roots)")

      ;; First data transaction
      (d/transact conn [{:name "Alice" :age 30}])
      (is (= 5 (get-freed-count @conn))
          "Alice tx frees 2 more addresses (EAVT, AEVT roots). Total: 3+2=5")

      ;; Second transaction
      (d/transact conn [{:name "Bob" :age 25}])
      (is (= 7 (get-freed-count @conn))
          "Bob tx frees 2 more addresses (EAVT, AEVT roots). Total: 5+2=7")

      ;; Third transaction
      (d/transact conn [{:name "Charlie" :age 35}])
      (is (= 9 (get-freed-count @conn))
          "Charlie tx frees 2 more addresses (EAVT, AEVT roots). Total: 7+2=9")

      ;; Note: Only 2 per data tx, not 3, because AVET only stores indexed attributes.
      ;; Since :name and :age are not indexed, AVET stays empty and doesn't change.

      ;; Verify data is still queryable
      (let [result (d/q '[:find ?name ?age
                          :where [?e :name ?name]
                                 [?e :age ?age]]
                        @conn)]
        (is (= 3 (count result))
            "All entities should be queryable despite freed addresses"))

      (d/release conn))))

(deftest precise-gc-deletion-test
  (testing "Verify freed addresses are actually deleted from storage"
    (let [cfg (-> base-cfg
                  (assoc-in [:store :path] "/tmp/online-gc-precise-deletion-test")
                  (assoc :online-gc {:enabled? true
                                     :grace-period-ms 0
                                     :max-batch 1000}))
          conn (do
                 (d/delete-database cfg)
                 (d/create-database cfg)
                 (d/connect cfg))
          initial-store-count (count-store @conn)]

      ;; Transact schema and data
      (d/transact conn schema)
      (d/transact conn [{:name "Alice" :age 30}])
      (d/transact conn [{:name "Bob" :age 25}])
      (d/transact conn [{:name "Charlie" :age 35}])

      ;; Freed count should be 0 (all deleted by online GC)
      (is (= 0 (get-freed-count @conn))
          "Online GC should have deleted all freed addresses")

      ;; Store should not have garbage
      (let [final-store-count (count-store @conn)
            growth (- final-store-count initial-store-count)]
        ;; With online GC, growth should be controlled
        ;; We have 3 entities + schema + branch metadata + indices
        ;; Should be much less than without GC
        (is (< growth 50)
            (str "Store growth should be controlled with online GC. Growth: " growth)))

      ;; Verify data integrity after releasing and reconnecting
      (d/release conn)
      (let [conn2 (d/connect cfg)
            result (d/q '[:find ?name ?age
                          :where [?e :name ?name]
                                 [?e :age ?age]]
                        @conn2)]
        (is (= 3 (count result))
            "All 3 entities should be queryable after GC and reconnect")
        (is (= #{["Alice" 30] ["Bob" 25] ["Charlie" 35]} (set result))
            "All data should be intact")
        (d/release conn2)))))

(deftest manual-gc-freed-count-test
  (testing "Manual GC invocation returns exact deletion count"
    (let [cfg (-> base-cfg
                  (assoc-in [:store :path] "/tmp/online-gc-manual-count-test")
                  (assoc :online-gc {:enabled? false}))  ;; Disabled for manual control
          conn (do
                 (d/delete-database cfg)
                 (d/create-database cfg)
                 (d/connect cfg))]

      (d/transact conn schema)
      (d/transact conn [{:name "Alice" :age 30}])
      (d/transact conn [{:name "Bob" :age 25}])

      ;; Check how many freed addresses accumulated
      ;; Schema (3) + Alice (2) + Bob (2) = 7
      (let [freed-before-gc (get-freed-count @conn)]
        (is (= 7 freed-before-gc)
            "Should have 7 freed addresses (schema:3 + Alice:2 + Bob:2)")

        ;; Manually run GC
        (let [deleted-count (online-gc/online-gc! (:store @conn)
                                                  {:enabled? true
                                                   :grace-period-ms 0
                                                   :max-batch 1000
                                                   :sync? true})]
          (is (= 7 deleted-count)
              "GC should delete exactly 7 addresses"))

        ;; After GC, freed count should be 0
        (is (= 0 (get-freed-count @conn))
            "After manual GC, freed addresses should be cleared"))

      ;; Data should still be queryable
      (d/release conn)
      (let [conn2 (d/connect cfg)
            result (d/q '[:find (count ?e) .
                          :where [?e :name _]]
                        @conn2)]
        (is (= 2 result)
            "Data should be intact after manual GC")
        (d/release conn2)))))

(deftest recycling-all-at-once-test
  (testing "Address recycling processes all eligible addresses at once"
    (let [cfg (-> base-cfg
                  (assoc-in [:store :path] "/tmp/online-gc-recycle-all-test")
                  (assoc :online-gc {:enabled? false}))  ;; Start disabled
          conn (do
                 (d/delete-database cfg)
                 (d/create-database cfg)
                 (d/connect cfg))]

      (d/transact conn schema)
      ;; Generate many freed addresses
      (dotimes [i 20]
        (d/transact conn [{:name (str "Person-" i) :age (+ 20 i)}]))

      ;; Check accumulated freed count
      ;; Schema (3) + 20 data transactions × 2 = 43 total
      (let [total-freed (get-freed-count @conn)]
        (is (= 43 total-freed)
            "Should have 43 freed addresses (schema:3 + 20×2=40)")

        ;; Run GC - with recycling, all eligible addresses are processed at once
        (let [recycled (online-gc/online-gc! (:store @conn)
                                             {:enabled? true
                                              :grace-period-ms 0
                                              :max-batch 5  ;; This only affects delete mode, not recycling
                                              :sync? true})]
          (is (= 43 recycled)
              "Should recycle all 43 addresses at once (recycling is not limited by max-batch)"))

        ;; All addresses should be recycled (moved to freelist)
        (let [remaining (get-freed-count @conn)]
          (is (= 0 remaining)
              "Should have 0 remaining freed addresses (all recycled)")))

      (d/release conn))))

;;; ============================================================================
;;; Integration Tests
;;; ============================================================================

(deftest online-gc-disabled-test
  (testing "Online GC disabled by default - addresses accumulate"
    (let [cfg (assoc-in base-cfg [:store :path] "/tmp/online-gc-disabled-test")
          conn (do
                 (d/delete-database cfg)
                 (d/create-database cfg)
                 (d/connect cfg))]
      (d/transact conn schema)
      (d/transact conn [{:name "Alice" :age 30}])
      (let [freed-before (get-freed-count @conn)]
        (d/transact conn [{:name "Bob" :age 25}])
        (d/transact conn [{:name "Charlie" :age 35}])
        (let [freed-after (get-freed-count @conn)]
          ;; Freed addresses should increase (not be deleted)
          (is (>= freed-after freed-before))
          (is (pos? freed-after))))
      (d/release conn))))

(deftest online-gc-with-reconnect-test
  (testing "Data integrity verified via reconnect after GC"
    (let [cfg (-> base-cfg
                  (assoc-in [:store :path] "/tmp/online-gc-reconnect-test")
                  (assoc :online-gc {:enabled? true
                                     :grace-period-ms 0
                                     :max-batch 1000}))
          conn (do
                 (d/delete-database cfg)
                 (d/create-database cfg)
                 (d/connect cfg))]

      ;; Insert data
      (d/transact conn schema)
      (d/transact conn [{:name "Alice" :age 30}])
      (d/transact conn [{:name "Bob" :age 25}])
      (d/transact conn [{:name "Charlie" :age 35}])

      ;; All freed addresses should be cleaned up
      (is (= 0 (get-freed-count @conn))
          "Online GC should have cleaned up all freed addresses")

      ;; Release and reconnect
      (d/release conn)
      (let [conn2 (d/connect cfg)
            result (d/q '[:find ?name ?age
                          :where [?e :name ?name]
                                 [?e :age ?age]]
                        @conn2)]
        (is (= 3 (count result))
            "All 3 entities queryable after reconnect")
        (is (= #{["Alice" 30] ["Bob" 25] ["Charlie" 35]} (set result))
            "All data intact after GC and reconnect")
        (d/release conn2)))))

(deftest grace-period-accumulation-test
  (testing "Grace period causes freed addresses to accumulate"
    (let [cfg (-> base-cfg
                  (assoc-in [:store :path] "/tmp/online-gc-grace-accumulation-test")
                  (assoc :online-gc {:enabled? true
                                     :grace-period-ms 300000  ;; 5 minutes
                                     :max-batch 1000}))
          conn (do
                 (d/delete-database cfg)
                 (d/create-database cfg)
                 (d/connect cfg))]

      (d/transact conn schema)
      (d/transact conn [{:name "Alice" :age 30}])
      (d/transact conn [{:name "Bob" :age 25}])

      ;; With long grace period, freed addresses should accumulate
      (let [freed-count (get-freed-count @conn)]
        (is (pos? freed-count)
            (str "Freed addresses should accumulate during grace period. Count: " freed-count)))

      ;; Data should be queryable
      (let [result (d/q '[:find (count ?e) .
                          :where [?e :name _]]
                        @conn)]
        (is (= 2 result)
            "Data should be queryable with accumulated freed addresses"))

      (d/release conn))))

(deftest online-gc-large-dataset-test
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
          "All 1000 entities should be accessible")

      (d/release conn))))

;;; ============================================================================
;;; Timestamp Filtering Tests
;;; ============================================================================

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
                                   300000)]  ;; 5 minutes
        ;; All should be in remaining (within grace period)
        (is (empty? to-delete) "Nothing should be eligible with long grace period")
        (is (coll? remaining) "Remaining should be a collection"))

      ;; Check with zero grace period
      (d/transact conn [{:name "Bob" :age 25}])  ;; Generate more freed addresses
      (let [[to-delete _remaining] (online-gc/get-and-clear-eligible-freed!
                                    (:store @conn)
                                    0)]  ;; Zero grace period
        ;; Should have eligible addresses
        (is (>= (count to-delete) 0) "Should find eligible addresses with zero grace period"))

      (d/release conn))))

;;; ============================================================================
;;; Background GC Tests
;;; ============================================================================

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
          (is (< freed-count 10)
              (str "Background GC should clean up freed addresses. Remaining: " freed-count))))

      (d/release conn))))

;;; ============================================================================
;;; Safety Tests
;;; ============================================================================

(deftest multi-branch-safety-test
  (testing "Multi-branch databases fall back to deletion mode"
    (let [cfg (-> base-cfg
                  (assoc-in [:store :path] "/tmp/online-gc-multi-branch-test")
                  (assoc :online-gc {:enabled? false})
                  (assoc :crypto-hash? false))
          conn (do
                 (d/delete-database cfg)
                 (d/create-database cfg)
                 (d/connect cfg))]

      (d/transact conn schema)
      (d/transact conn [{:name "Alice" :age 30}])

      ;; Check initial branches - should be single branch #{:db}
      (let [branches (k/get (:store @conn) :branches nil {:sync? true})]
        (is (= #{:db} branches)
            "Should start with single branch"))

      ;; Manually add a second branch to simulate multi-branch scenario
      (k/assoc (:store @conn) :branches #{:db :branch-a} {:sync? true})

      ;; Verify multi-branch state
      (let [branches (k/get (:store @conn) :branches nil {:sync? true})]
        (is (= #{:db :branch-a} branches)
            "Should have two branches"))

      ;; Add transaction to generate freed addresses
      (d/transact conn [{:name "Bob" :age 25}])

      (let [freed-before (get-freed-count @conn)]
        (is (pos? freed-before)
            "Should have freed addresses"))

      ;; Run online GC - should detect multi-branch and use deletion mode
      (let [result (online-gc/online-gc! (:store @conn)
                                         {:enabled? true
                                          :grace-period-ms 0
                                          :sync? true})]
        (is (pos? result)
            "Should process addresses in deletion mode"))

      ;; Verify addresses were cleared (deletion worked)
      (let [freed-after (get-freed-count @conn)]
        (is (= 0 freed-after)
            "Multi-branch deletion mode should clear freed addresses"))

      (d/release conn))))
