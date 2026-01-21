(ns datahike.test.pod-test
  "Integration tests for datahike.pod functions.

   These tests verify the pod API works correctly without going through
   the native image + babashka pod protocol. This allows faster CI feedback."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [datahike.pod :as pod]
            [datahike.api :as d])
  (:import [java.util Date]))

(def test-config
  {:keep-history? true
   :search-cache-size 10000
   :index :datahike.index/persistent-set
   :store {:id #uuid "550e8400-e29b-41d4-a716-446655440001"
           :backend :memory
           :scope "pod-test"}
   :store-cache-size 1000
   :attribute-refs? false
   :writer {:backend :self}
   :crypto-hash? false
   :schema-flexibility :read
   :branch :db})

(defn reset-db-fixture [f]
  ;; Clean up before test
  (pod/delete-database test-config)
  (reset! pod/conns {})
  (reset! pod/dbs {})
  (f)
  ;; Clean up after test
  (reset! pod/conns {})
  (reset! pod/dbs {}))

(use-fixtures :each reset-db-fixture)

;; =============================================================================
;; Database Lifecycle Tests
;; =============================================================================

(deftest database-lifecycle-test
  (testing "create-database returns config"
    (let [result (pod/create-database test-config)]
      (is (map? result))
      (is (= :memory (get-in result [:store :backend])))))

  (testing "database-exists? returns true after create"
    (is (= true (pod/database-exists? test-config))))

  (testing "delete-database returns nil"
    (is (nil? (pod/delete-database test-config))))

  (testing "database-exists? returns false after delete"
    (is (= false (pod/database-exists? test-config)))))

;; =============================================================================
;; Connection Tests
;; =============================================================================

(deftest connection-test
  (pod/create-database test-config)

  (testing "connect returns conn-id string"
    (let [conn-id (pod/connect test-config)]
      (is (string? conn-id))
      (is (clojure.string/starts-with? conn-id "conn:"))))

  (testing "connect with same config returns same conn-id"
    (let [conn-id1 (pod/connect test-config)
          conn-id2 (pod/connect test-config)]
      (is (= conn-id1 conn-id2))))

  (testing "release removes connection"
    (let [conn-id (pod/connect test-config)]
      (is (contains? @pod/conns conn-id))
      (pod/release conn-id)
      (is (not (contains? @pod/conns conn-id))))))

;; =============================================================================
;; Database Access Tests
;; =============================================================================

(deftest db-access-test
  (pod/create-database test-config)
  (let [conn-id (pod/connect test-config)]

    (testing "db returns db-id string (UUID format)"
      (let [db-id (pod/db conn-id)]
        (is (string? db-id))
        ;; Should be a UUID string (commit-id)
        (is (re-matches #"[0-9a-f-]+" db-id))))

    (testing "db caches database with conn-id reference"
      (let [db-id (pod/db conn-id)
            entry (get @pod/dbs db-id)]
        (is (some? entry))
        (is (= conn-id (:conn-id entry)))
        (is (some? (:db entry)))))

    (testing "release-db removes from cache"
      (let [db-id (pod/db conn-id)]
        (is (contains? @pod/dbs db-id))
        (is (= {} (pod/release-db db-id)))
        (is (not (contains? @pod/dbs db-id)))))

    (pod/release conn-id)))

;; =============================================================================
;; Transaction Tests
;; =============================================================================

(deftest transact-test
  (pod/create-database test-config)
  (let [conn-id (pod/connect test-config)]

    (testing "transact returns summarized tx-report"
      (let [result (pod/transact conn-id [{:name "Alice" :age 30}])]
        (is (map? result))
        (is (contains? result :tempids))
        (is (contains? result :db-before))
        (is (contains? result :db-after))
        (is (contains? result :tx-meta))
        (is (contains? result :tx-data))
        ;; db-before/after should be summarized (only :max-tx :max-eid)
        (is (= #{:max-tx :max-eid} (set (keys (:db-before result)))))
        (is (= #{:max-tx :max-eid} (set (keys (:db-after result)))))))

    (testing "transact with map syntax"
      (let [result (pod/transact conn-id {:tx-data [{:name "Bob" :age 25}]})]
        (is (map? result))))

    (pod/release conn-id)))

;; =============================================================================
;; Query Tests
;; =============================================================================

(deftest query-test
  (pod/create-database test-config)
  (let [conn-id (pod/connect test-config)]
    (pod/transact conn-id [{:name "Alice" :age 20}
                           {:name "Bob" :age 30}
                           {:name "Charlie" :age 40}])

    (testing "q with map syntax"
      (let [db-id (pod/db conn-id)
            result (pod/q {:query '{:find [?e ?n]
                                    :where [[?e :name ?n]]}
                           :args [db-id]})]
        (is (set? result))
        (is (= 3 (count result)))
        (pod/release-db db-id)))

    (testing "q with vector syntax"
      (let [db-id (pod/db conn-id)
            result (pod/q '[:find ?e ?n :where [?e :name ?n]] db-id)]
        (is (set? result))
        (is (= 3 (count result)))
        (pod/release-db db-id)))

    (pod/release conn-id)))

;; =============================================================================
;; Pull Tests
;; =============================================================================

(deftest pull-test
  (pod/create-database test-config)
  (let [conn-id (pod/connect test-config)]
    (pod/transact conn-id [{:name "Alice" :age 20}])

    (testing "pull with selector and eid"
      (let [db-id (pod/db conn-id)
            result (pod/pull db-id '[*] 1)]
        (is (map? result))
        (is (= "Alice" (:name result)))
        (is (= 20 (:age result)))
        (pod/release-db db-id)))

    (testing "pull-many"
      (pod/transact conn-id [{:name "Bob" :age 30}])
      (let [db-id (pod/db conn-id)
            result (pod/pull-many db-id '[*] [1 2])]
        (is (sequential? result))
        (is (= 2 (count result)))
        (pod/release-db db-id)))

    (pod/release conn-id)))

;; =============================================================================
;; Entity Tests
;; =============================================================================

(deftest entity-test
  (pod/create-database test-config)
  (let [conn-id (pod/connect test-config)]
    (pod/transact conn-id [{:name "Alice" :age 20}])

    (testing "entity returns plain map"
      (let [db-id (pod/db conn-id)
            result (pod/entity db-id 1)]
        (is (map? result))
        (is (= "Alice" (:name result)))
        (is (= 20 (:age result)))
        ;; Should NOT have :db/id (entity transform strips it)
        (is (not (contains? result :db/id)))
        (pod/release-db db-id)))

    (pod/release conn-id)))

;; =============================================================================
;; Temporal Tests
;; =============================================================================

(deftest temporal-test
  (pod/create-database test-config)
  (let [conn-id (pod/connect test-config)]
    (pod/transact conn-id [{:name "Alice" :age 20}])
    (let [timestamp (Date.)]
      (Thread/sleep 10)
      (pod/transact conn-id [{:db/id 1 :age 21}])

      (testing "as-of returns db-id"
        (let [db-id (pod/db conn-id)
              asof-id (pod/as-of db-id timestamp)]
          (is (string? asof-id))
          ;; Query as-of should see old age
          (let [result (pod/q '[:find ?a . :where [1 :age ?a]] asof-id)]
            (is (= 20 result)))
          (pod/release-db db-id)
          (pod/release-db asof-id)))

      (testing "since returns db-id"
        (let [db-id (pod/db conn-id)
              since-id (pod/since db-id timestamp)]
          (is (string? since-id))
          (pod/release-db db-id)
          (pod/release-db since-id)))

      (testing "history returns db-id"
        (let [db-id (pod/db conn-id)
              hist-id (pod/history db-id)]
          (is (string? hist-id))
          ;; History should include both old and new age
          (let [result (pod/q '[:find ?a :where [1 :age ?a]] hist-id)]
            (is (= #{[20] [21]} result)))
          (pod/release-db db-id)
          (pod/release-db hist-id))))

    (pod/release conn-id)))

;; =============================================================================
;; db-with Tests
;; =============================================================================

(deftest db-with-test
  (pod/create-database test-config)
  (let [conn-id (pod/connect test-config)]
    (pod/transact conn-id [{:name "Alice" :age 20}])

    (testing "db-with returns speculative db-id"
      (let [db-id (pod/db conn-id)
            with-id (pod/db-with db-id [{:name "Bob" :age 30}])]
        (is (string? with-id))
        ;; Speculative db should have Bob
        (let [result (pod/q '[:find ?n :where [_ :name ?n]] with-id)]
          (is (contains? result ["Bob"])))
        ;; Original db should NOT have Bob
        (let [result (pod/q '[:find ?n :where [_ :name ?n]] db-id)]
          (is (not (contains? result ["Bob"]))))
        (pod/release-db db-id)
        (pod/release-db with-id)))

    (pod/release conn-id)))

;; =============================================================================
;; Index Operations Tests
;; =============================================================================

(deftest datoms-test
  (pod/create-database test-config)
  (let [conn-id (pod/connect test-config)]
    (pod/transact conn-id [{:name "Alice" :age 20}])

    (testing "datoms with map syntax"
      (let [db-id (pod/db conn-id)
            result (pod/datoms db-id {:index :eavt :components [1]})]
        (is (sequential? result))
        (is (every? sequential? result))
        (pod/release-db db-id)))

    (testing "datoms with variadic positional syntax"
      (let [db-id (pod/db conn-id)
            result (pod/datoms db-id :eavt 1 :age)]
        (is (sequential? result))
        (pod/release-db db-id)))

    (pod/release conn-id)))

;; =============================================================================
;; Schema Tests
;; =============================================================================

(deftest schema-test
  (pod/create-database test-config)
  (let [conn-id (pod/connect test-config)]
    ;; Add schema
    (pod/transact conn-id [{:db/ident :name
                            :db/valueType :db.type/string
                            :db/cardinality :db.cardinality/one}])

    (testing "schema returns schema map"
      (let [db-id (pod/db conn-id)
            result (pod/schema db-id)]
        (is (map? result))
        (is (contains? result :name))
        (pod/release-db db-id)))

    (testing "reverse-schema returns reverse schema"
      (let [db-id (pod/db conn-id)
            result (pod/reverse-schema db-id)]
        (is (map? result))
        (pod/release-db db-id)))

    (pod/release conn-id)))

;; =============================================================================
;; Metrics Tests
;; =============================================================================

(deftest metrics-test
  (pod/create-database test-config)
  (let [conn-id (pod/connect test-config)]
    (pod/transact conn-id [{:name "Alice" :age 20}])

    (testing "metrics returns metrics map"
      (let [db-id (pod/db conn-id)
            result (pod/metrics db-id)]
        (is (map? result))
        (is (contains? result :count))
        (is (contains? result :per-attr-counts))
        (pod/release-db db-id)))

    (pod/release conn-id)))

;; =============================================================================
;; Connection Tracking Tests
;; =============================================================================

(deftest connection-tracking-test
  (pod/create-database test-config)
  (let [conn-id (pod/connect test-config)]
    (pod/transact conn-id [{:name "Alice"}])

    (testing "db entries track parent conn-id"
      (let [db-id (pod/db conn-id)
            entry (get @pod/dbs db-id)]
        (is (= conn-id (:conn-id entry)))
        (pod/release-db db-id)))

    (testing "temporal dbs inherit conn-id"
      (let [db-id (pod/db conn-id)
            hist-id (pod/history db-id)
            hist-entry (get @pod/dbs hist-id)]
        (is (= conn-id (:conn-id hist-entry)))
        (pod/release-db db-id)
        (pod/release-db hist-id)))

    (pod/release conn-id)))
