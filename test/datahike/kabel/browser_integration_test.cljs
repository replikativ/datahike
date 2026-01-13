(ns datahike.kabel.browser-integration-test
  "Browser-side integration tests for Datahike with Kabel writer.
   
   Tests the full distributed transaction flow:
   1. Connect to JVM server via WebSocket
   2. Create database remotely
   3. Connect with TieredStore (memory + IndexedDB) and KabelWriter
   4. Perform transactions (schema, data)
   5. Query data (q, pull, entity)
   6. Disconnect and reconnect to verify persistence
   7. Clean up (delete database and IndexedDB)
   
   Prerequisites:
   - JVM test server must be running on ws://localhost:47296
   - Start with: clj -M:test -m datahike.kabel.browser-test-server/start-test-server!"
  (:require [cljs.test :refer [deftest is testing async use-fixtures]]
            [clojure.core.async :refer [<! timeout alts!] :refer-macros [go]]
            [datahike.api :as d]
            [datahike.kabel.connector]  ;; registers -connect* :kabel multimethod
            [datahike.kabel.writer]     ;; registers create-database/delete-database :kabel multimethods
            [datahike.kabel.fressian-handlers :refer [datahike-fressian-middleware]]
            [is.simm.distributed-scope :as ds]
            [kabel.peer :as peer]
            [konserve-sync.core :as sync]
            [konserve.indexeddb :as idb]
            [superv.async :refer [S] :refer-macros [go-try <?]]))

;; =============================================================================
;; Configuration
;; =============================================================================

(def test-server-url "ws://localhost:47296")
(def test-server-id #uuid "aaaaaaaa-0000-0000-0000-000000000001")
(def test-client-id #uuid "bbbbbbbb-0000-0000-0000-000000000002")

;; =============================================================================
;; Test Fixtures
;; =============================================================================

(defonce client-peer (atom nil))

(defn setup-client-peer!
  "Create and start Kabel client peer for tests."
  []
  (when-not @client-peer
    (js/console.log "[SETUP] Initializing Kabel client peer...")
    (let [peer-atom (peer/client-peer
                     S
                     test-client-id
                      ;; Middleware composition (innermost runs first):
                      ;; 1. remote-middleware - distributed-scope function invocation
                      ;; 2. sync/client-middleware - konserve-sync for store replication
                     (comp ds/remote-middleware
                           (sync/client-middleware))
                      ;; Fressian serialization with Datahike type handlers
                     datahike-fressian-middleware)
          ;; Start the invocation loop for handling remote calls
          _ (ds/invoke-on-peer peer-atom)]
      (reset! client-peer peer-atom)
      (js/console.log "[SETUP] Client peer ready"))))

(defn teardown-client-peer!
  "Stop client peer after tests."
  []
  (when @client-peer
    (js/console.log "[TEARDOWN] Stopping client peer...")
    (reset! client-peer nil)))

(use-fixtures :once
  {:before setup-client-peer!
   :after teardown-client-peer!})

;; =============================================================================
;; Helper Functions
;; =============================================================================

(defonce connection-established (atom false))

(defn ensure-peer-ready!
  "Ensure client peer is ready and connected to server via distributed-scope.
   Returns channel that yields true on success."
  []
  (go-try S
          (when-not @client-peer
            (throw (ex-info "Client peer not initialized" {})))

    ;; Establish connection if not already done
          (when-not @connection-established
            (let [connect-ch (ds/connect-distributed-scope S @client-peer test-server-url)
                  timeout-ch (timeout 10000)
                  [result ch] (alts! [connect-ch timeout-ch])]
              (if (= ch timeout-ch)
                (throw (ex-info "Connection timeout" {:url test-server-url}))
                (do
                  (when (instance? js/Error result)
                    (throw result))
                  (<? S connect-ch)
                  (reset! connection-established true)
                  (js/console.log "[TEST] Connected to server")))))

          true))

;; =============================================================================
;; Test Schema
;; =============================================================================

(def test-schema
  [{:db/ident :person/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :person/age
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :person/email
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}])

;; =============================================================================
;; Tests
;; =============================================================================

(deftest ^:browser basic-kabel-peer-test
  (testing "Basic Kabel peer initialization"
    (async done
           (go
             (try
               (<! (ensure-peer-ready!))
               (is true "Peer initialized successfully")
               (catch js/Error e
                 (is false (str "Peer init error: " (.-message e))))
               (finally
                 (done)))))))

(deftest ^:browser dynamic-database-lifecycle-test
  (testing "Dynamic database creation, connection, and deletion"
    (async done
           (let [store-id (random-uuid)
                 db-name (str "test-dynamic-" store-id)]
             (go
               (try
            ;; Ensure peer is ready
                 (<! (ensure-peer-ready!))

            ;; Step 1: Create database
                 (js/console.log "\n[TEST] Step 1: Creating database with store-id:" (str store-id))
                 ;; Note: All tiered store :id values must match for konserve validation
                 ;; konserve uses :frontend-config/:backend-config to avoid collision with :backend :tiered
                 (let [config {:store {:backend :tiered
                                       :frontend-config {:backend :memory :id store-id}
                                       :backend-config {:backend :indexeddb :name db-name :id store-id}
                                       :id store-id}
                               :writer {:backend :kabel
                                        :peer-id test-server-id

                                        :local-peer @client-peer
                                        :url test-server-url}  ; Add explicit URL
                               :schema-flexibility :write
                               :keep-history? false}]
                   (let [create-result (<! (d/create-database config))]
                     (is (:success create-result) "Database created successfully")
                     (js/console.log "[TEST] Database created")

                ;; Step 2: Connect to database
                     (js/console.log "\n[TEST] Step 2: Connecting to database...")
                     (let [connect-result (d/connect config {:sync? false})
                           _ (js/console.log "[TEST] d/connect returned:" (pr-str (type connect-result)))
                           conn (<! connect-result)
                           _ (js/console.log "[TEST] Channel yielded:" (pr-str conn))
                           _ (js/console.log "[TEST] conn type:" (pr-str (type conn)))
                           ;; Check if conn is an exception (error from connect-kabel)
                           _ (when (instance? js/Error conn)
                               (js/console.error "[TEST] Connection returned error:" (.-message conn))
                               (throw conn))
                           _ (when (instance? ExceptionInfo conn)
                               (js/console.error "[TEST] Connection returned ExceptionInfo:" (ex-message conn) (ex-data conn))
                               (throw conn))]
                       (is (some? conn) "Connection established")
                       (when-not conn
                         (throw (ex-info "Connection is nil" {:config config})))
                       (is (some? @(:wrapped-atom conn)) "Connection has database")
                       (js/console.log "[TEST] Connected! max-tx:" (:max-tx @(:wrapped-atom conn)))

                ;; Step 3: Transact schema
                       (js/console.log "\n[TEST] Step 3: Transacting schema...")
                       (let [schema-tx-result (<! (d/transact! conn test-schema))]
                         (is (some? (:db-after schema-tx-result)) "Schema transaction succeeded")
                         (is (pos? (count (:tx-data schema-tx-result))) "Schema datoms added")
                         (js/console.log "[TEST] Schema transacted, max-tx:"
                                         (get-in schema-tx-result [:db-after :max-tx]))

                  ;; Step 4: Transact data
                         (js/console.log "\n[TEST] Step 4: Transacting data...")
                         (let [data-tx [{:person/name "Alice" :person/age 30 :person/email "alice@example.com"}
                                        {:person/name "Bob" :person/age 25 :person/email "bob@example.com"}
                                        {:person/name "Charlie" :person/age 35 :person/email "charlie@example.com"}]
                               tx-result (<! (d/transact! conn data-tx))]
                           (is (some? (:db-after tx-result)) "Data transaction succeeded")
                           (is (= 3 (count (filter #(= :person/name (:a %)) (:tx-data tx-result))))
                               "Three name datoms added")
                           (js/console.log "[TEST] Data transacted, max-tx:"
                                           (get-in tx-result [:db-after :max-tx]))

                    ;; Step 5: Query data
                           (js/console.log "\n[TEST] Step 5: Querying data...")
                           (let [db (:db-after tx-result)
                                 query-result (d/q '[:find ?name ?age
                                                     :where
                                                     [?e :person/name ?name]
                                                     [?e :person/age ?age]]
                                                   db)]
                             (is (= 3 (count query-result)) "Query returned 3 results")
                             (is (contains? (set (map first query-result)) "Alice")
                                 "Alice found in results")
                             (js/console.log "[TEST] Query returned" (count query-result) "results")

                      ;; Step 6: Test pull API
                             (js/console.log "\n[TEST] Step 6: Testing pull API...")
                             (let [entity-id (ffirst (d/q '[:find ?e :where [?e :person/name "Alice"]] db))
                                   pulled (d/pull db [:person/name :person/age :person/email] entity-id)]
                               (is (= "Alice" (:person/name pulled)) "Pull retrieved Alice's name")
                               (is (= 30 (:person/age pulled)) "Pull retrieved Alice's age")
                               (js/console.log "[TEST] Pull succeeded for Alice")

                        ;; Step 7: Test entity API
                               (js/console.log "\n[TEST] Step 7: Testing entity API...")
                               (let [entity (d/entity db entity-id)]
                                 (is (= "Alice" (:person/name entity)) "Entity has name")
                                 (is (= 30 (:person/age entity)) "Entity has age")
                                 (is (= "alice@example.com" (:person/email entity)) "Entity has email")
                                 (js/console.log "[TEST] Entity API succeeded")

                          ;; Step 8: Release connection
                                 (js/console.log "\n[TEST] Step 8: Releasing connection...")
                                 (d/release conn)
                                 (js/console.log "[TEST] Connection released")

                          ;; Step 9: Reconnect to verify persistence
                                 (js/console.log "\n[TEST] Step 9: Reconnecting to verify persistence...")
                                 (let [conn2 (<! (d/connect config {:sync? false}))]
                                   (is (some? conn2) "Reconnected successfully")
                                   (let [db2 (d/db conn2)
                                         query-result2 (d/q '[:find ?name ?age
                                                              :where
                                                              [?e :person/name ?name]
                                                              [?e :person/age ?age]]
                                                            db2)]
                                     (is (= 3 (count query-result2))
                                         "Data persisted after reconnect")
                                     (js/console.log "[TEST] Reconnected and verified" (count query-result2) "persisted entities")

                              ;; Release second connection
                                     (d/release conn2)

                              ;; Step 10: Delete database
                                     (js/console.log "\n[TEST] Step 10: Deleting database...")
                                     (let [delete-result (<! (d/delete-database config))]
                                       (is (some? delete-result) "Database deleted")
                                       (js/console.log "[TEST] Database deleted")

                                       (js/console.log "\n[TEST] ✅ All tests passed!"))))))))))))

                 (catch js/Error e
                   (is false (str "Test error: " (.-message e)))
                   (js/console.error "[TEST] Error:" e))
                 (finally
                   (js/console.log "[TEST] Calling done() callback...")
                   (done))))))))

(deftest ^:browser multiple-transactions-test
  (testing "Multiple transactions and query updates"
    (async done
           (let [store-id (random-uuid)
                 db-name (str "test-multi-tx-" store-id)]
             (go
               (try
                 (<! (ensure-peer-ready!))

                 (js/console.log "\n[TEST-MULTI] Creating database...")
                 ;; Note: All tiered store :id values must match for konserve validation
                 ;; konserve uses :frontend-config/:backend-config to avoid collision with :backend :tiered
                 (let [config {:store {:backend :tiered
                                       :frontend-config {:backend :memory :id store-id}
                                       :backend-config {:backend :indexeddb :name db-name :id store-id}
                                       :id store-id}
                               :writer {:backend :kabel
                                        :peer-id test-server-id

                                        :local-peer @client-peer}
                               :schema-flexibility :write
                               :keep-history? false}]

                   (<! (d/create-database config))
                   (let [conn (<! (d/connect config {:sync? false}))]

                ;; Transact schema
                     (<! (d/transact! conn test-schema))

                ;; First transaction
                     (js/console.log "[TEST-MULTI] First transaction...")
                     (<! (d/transact! conn [{:person/name "User1" :person/age 20}]))
                     (let [db1 (d/db conn)
                           count1 (count (d/q '[:find ?e :where [?e :person/name]] db1))]
                       (is (= 1 count1) "One entity after first transaction")

                  ;; Second transaction
                       (js/console.log "[TEST-MULTI] Second transaction...")
                       (<! (d/transact! conn [{:person/name "User2" :person/age 22}]))
                       (let [db2 (d/db conn)
                             count2 (count (d/q '[:find ?e :where [?e :person/name]] db2))]
                         (is (= 2 count2) "Two entities after second transaction")

                    ;; Third transaction
                         (js/console.log "[TEST-MULTI] Third transaction...")
                         (<! (d/transact! conn [{:person/name "User3" :person/age 24}]))
                         (let [db3 (d/db conn)
                               count3 (count (d/q '[:find ?e :where [?e :person/name]] db3))]
                           (is (= 3 count3) "Three entities after third transaction")

                           (js/console.log "[TEST-MULTI] ✅ Multiple transactions test passed!")

                      ;; Cleanup
                           (d/release conn)
                           (<! (d/delete-database config)))))))

                 (catch js/Error e
                   (is false (str "Multi-transaction test error: " (.-message e)))
                   (js/console.error "[TEST-MULTI] Error:" e))
                 (finally
                   (js/console.log "[TEST-MULTI] Calling done() callback...")
                   (done))))))))

;; =============================================================================
;; Test Runner
;; =============================================================================

(defn ^:export run-tests []
  (js/console.log "=== Starting Datahike Browser Integration Tests ===")
  (js/console.log "Prerequisites: JVM server must be running on" test-server-url)
  (cljs.test/run-tests 'datahike.kabel.browser-integration-test))
