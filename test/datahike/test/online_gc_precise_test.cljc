(ns datahike.test.online-gc-precise-test
  "Precise tests for online GC that track exact freed address counts and verify deletion."
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing]])
   [datahike.api :as d]
   [datahike.online-gc :as online-gc]
   [konserve.core :as k])
  #?(:clj (:import [java.util Date])))

(defn- count-store [db]
  (count (k/keys (:store db) {:sync? true})))

(defn- get-freed-addresses [db]
  "Get current freed addresses vector"
  @(-> db :store :storage :freed-addresses))

(defn- get-freed-count [db]
  (count (get-freed-addresses db)))

(def base-cfg {:store              {:backend :file
                                    :path "/tmp/online-gc-precise-test"
                                    :id #uuid "b0000000-0000-0000-0000-000000000001"}
               :keep-history?      false
               :schema-flexibility :write
               :index              :datahike.index/persistent-set})

(def schema [{:db/ident       :name
              :db/cardinality :db.cardinality/one
              :db/valueType   :db.type/string}
             {:db/ident       :age
              :db/cardinality :db.cardinality/one
              :db/valueType   :db.type/long}])

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
      (is (= 0 (get-freed-count @conn))
          "Schema transaction shouldn't free addresses (no prior index)")

      ;; First data transaction - no freed addresses yet
      (d/transact conn [{:name "Alice" :age 30}])
      (let [freed-tx1 (get-freed-count @conn)]
        (is (>= freed-tx1 0)
            "First data tx may free some addresses from schema changes"))

      ;; Second transaction - should free addresses from previous index version
      (d/transact conn [{:name "Bob" :age 25}])
      (let [freed-tx2 (get-freed-count @conn)
            freed-tx1 (get-freed-count @conn)]
        (is (> freed-tx2 freed-tx1)
            (str "Second tx should accumulate more freed addresses. "
                 "Freed after tx2: " freed-tx2 ", after tx1: " freed-tx1)))

      ;; Third transaction - more freed addresses
      (d/transact conn [{:name "Charlie" :age 35}])
      (let [freed-tx3 (get-freed-count @conn)]
        (is (pos? freed-tx3)
            (str "Should have accumulated freed addresses. Count: " freed-tx3))
        ;; Each transaction modifies 3 indices (EAVT, AEVT, AVET)
        ;; Each index is a PSS tree - root + possibly branch nodes
        ;; Minimum: 3 freed root addresses per transaction after the first
        (is (>= freed-tx3 3)
            (str "Should have at least 3 freed addresses (3 index roots). Got: " freed-tx3)))

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
      (let [freed-before-gc (get-freed-count @conn)]
        (is (pos? freed-before-gc)
            (str "Should have freed addresses accumulated. Count: " freed-before-gc))

        ;; Manually run GC
        (let [deleted-count (online-gc/online-gc! (:store @conn)
                                                  {:enabled? true
                                                   :grace-period-ms 0
                                                   :max-batch 1000
                                                   :sync? true})]
          (is (= freed-before-gc deleted-count)
              (str "GC should delete exactly the freed count. "
                   "Freed: " freed-before-gc ", Deleted: " deleted-count)))

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

(deftest batch-size-limit-precise-test
  (testing "Batch size limit causes incremental deletion"
    (let [cfg (-> base-cfg
                  (assoc-in [:store :path] "/tmp/online-gc-batch-precise-test")
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
      (let [total-freed (get-freed-count @conn)]
        (is (pos? total-freed)
            (str "Should have accumulated freed addresses. Count: " total-freed))

        ;; Run GC with small batch size
        (let [batch-size 5
              deleted (online-gc/online-gc! (:store @conn)
                                            {:enabled? true
                                             :grace-period-ms 0
                                             :max-batch batch-size
                                             :sync? true})]
          (is (= batch-size deleted)
              (str "Should delete exactly batch-size addresses. "
                   "Expected: " batch-size ", Got: " deleted)))

        ;; Remaining freed addresses
        (let [remaining (get-freed-count @conn)]
          (is (= (- total-freed batch-size) remaining)
              (str "Should have remaining freed addresses. "
                   "Total: " total-freed ", Deleted: " batch-size ", Remaining: " remaining))))

      (d/release conn))))
