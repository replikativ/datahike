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
      ;; Schema (3) + 20 data transactions × 2 = 43 total
      (let [total-freed (get-freed-count @conn)
            batch-size 5]
        (is (= 43 total-freed)
            "Should have 43 freed addresses (schema:3 + 20×2=40)")

        ;; Run GC with small batch size
        (let [deleted (online-gc/online-gc! (:store @conn)
                                            {:enabled? true
                                             :grace-period-ms 0
                                             :max-batch batch-size
                                             :sync? true})]
          (is (= 5 deleted)
              "Should delete exactly 5 addresses (batch size limit)"))

        ;; Remaining freed addresses
        (let [remaining (get-freed-count @conn)]
          (is (= 38 remaining)
              "Should have 38 remaining addresses (43 - 5)")))

      (d/release conn))))
