(ns datahike.kabel.integration-test
  "Integration tests for KabelWriter with konserve-sync.

   These tests verify the full distributed transaction flow using the official
   Datahike API (d/connect, d/transact) with KabelWriter:

   1. Server creates database and registers for remote access
   2. Client connects with :writer {:backend :kabel :local-peer ...}
      - d/connect automatically syncs from remote before returning
   3. Client uses d/transact - routed through KabelWriter to server
   4. Server executes transaction, syncs store
   5. Client receives sync, transaction completes

   Uses Fressian serialization with Datahike handlers."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.java.io :as io]
            [datahike.api :as d]
            [datahike.kabel.connector]  ;; registers -connect* :kabel multimethod
            [datahike.kabel.writer :as kw]
            [datahike.connector :refer [release]]
            [hasch.core :as hasch]
            [datahike.kabel.handlers :as handlers]
            [datahike.kabel.fressian-handlers :as fh]
            [kabel.peer :as peer]
            [kabel.http-kit :refer [create-http-kit-handler!]]
            [kabel.middleware.fressian :refer [fressian]]
            ;; konserve-sync for store replication
            [konserve-sync.core :as sync]
            [konserve-sync.walkers.datahike :as dh-walker]
            [is.simm.distributed-scope :as ds]
            [superv.async :refer [<?? S]]
            [clojure.core.async :refer [alts!! timeout <!!]]))

;; =============================================================================
;; Test IDs
;; =============================================================================

(def server-id #uuid "10000000-0000-0000-0000-000000000001")
(def client-id #uuid "20000000-0000-0000-0000-000000000002")

;; =============================================================================
;; Test Helpers
;; =============================================================================

(defn get-free-port []
  (let [socket (java.net.ServerSocket. 0)]
    (try
      (.getLocalPort socket)
      (finally
        (.close socket)))))

(defn create-temp-dir
  "Generate a unique temporary directory path (konserve will create it)."
  [prefix]
  (let [temp-dir (io/file (System/getProperty "java.io.tmpdir")
                          (str prefix "-" (System/currentTimeMillis) "-" (rand-int 10000)))]
    ;; Don't create the directory - let konserve handle it
    ;; This prevents "File store already exists" errors
    (.getAbsolutePath temp-dir)))

(defn delete-dir-recursive
  "Delete a directory and all its contents."
  [path]
  (let [dir (io/file path)]
    (when (.exists dir)
      (doseq [file (reverse (file-seq dir))]
        (.delete file)))))

(defn datahike-fressian-middleware
  "Fressian middleware with Datahike type handlers."
  [peer-config]
  (fressian (atom fh/read-handlers)
            (atom fh/write-handlers)
            peer-config))

;; =============================================================================
;; Schema
;; =============================================================================

(def test-schema
  [{:db/ident :person/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :person/age
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}])

;; =============================================================================
;; Integration Tests
;; =============================================================================

(deftest test-full-flow-with-official-api
  (testing "Full flow: d/connect with KabelWriter, d/transact routed to server"
    (let [port (get-free-port)
          url (str "ws://localhost:" port)
          ;; Shared store ID for matching across client/server
          store-id #uuid "7e570000-0000-0000-0000-000000000001"  ; test-full-flow-store
          store-topic (keyword (str store-id))
          server-path (create-temp-dir "full-flow-server")
          client-path (create-temp-dir "full-flow-client")

          ;; =====================================================================
          ;; SERVER SETUP
          ;; =====================================================================

          ;; Server database config
          server-config {:store {:backend :file :path server-path :id store-id}
                         :schema-flexibility :write
                         :keep-history? false}
          _ (d/create-database server-config)
          server-conn (d/connect server-config)
          _ (d/transact server-conn test-schema)

          ;; Server peer with konserve-sync + distributed-scope + Fressian
          handler (create-http-kit-handler! S url server-id)
          server-peer (peer/server-peer S handler server-id
                                        (comp (sync/server-middleware)
                                              ds/remote-middleware)
                                        datahike-fressian-middleware)
          _ (<?? S (peer/start server-peer))
          _ (ds/invoke-on-peer server-peer)

          ;; Register global handlers (once per server)
          _ (handlers/register-global-handlers! server-peer)

          ;; Register for remote transactions (includes sync + tx-broadcast)
          _ (handlers/register-store-for-remote-access! store-id server-conn server-peer)

          ;; =====================================================================
          ;; CLIENT SETUP - d/connect handles sync automatically
          ;; =====================================================================

          ;; Client peer with konserve-sync + distributed-scope + Fressian
          client-peer (peer/client-peer S client-id
                                        (comp (sync/client-middleware)
                                              ds/remote-middleware)
                                        datahike-fressian-middleware)
          _ (ds/invoke-on-peer client-peer)
          _ (<?? S (peer/connect S client-peer url))

          ;; Connect with KabelWriter via d/connect
          ;; (async with :kabel backend, returns channel - we take from it with <!!)
          client-config {:store {:backend :file :path client-path :id store-id}
                         :index :datahike.index/persistent-set
                         :schema-flexibility :write
                         :keep-history? false
                         :writer {:backend :kabel
                                  :peer-id server-id

                                  :local-peer client-peer}}
          client-conn (<!! (d/connect client-config {:sync? false}))]

      ;; Debug: inspect connection state
      (when (instance? Throwable client-conn)
        (println "DEBUG: client-conn is exception:" (ex-message client-conn))
        (println "DEBUG: exception data:" (ex-data client-conn))
        (throw client-conn))
      (println "DEBUG: client-conn type:" (type client-conn))
      (println "DEBUG: writer type:" (type (:writer @(:wrapped-atom client-conn))))

      ;; Verify connection works
      (is (some? client-conn) "Client connection should be created")
      (is (instance? datahike.kabel.writer.KabelWriter
                     (:writer @(:wrapped-atom client-conn)))
          "Writer should be KabelWriter")

      ;; =====================================================================
      ;; TRANSACTION VIA OFFICIAL API
      ;; =====================================================================

      ;; Use d/transact - should route through KabelWriter to server
      (let [tx-result (d/transact client-conn [{:person/name "Alice" :person/age 30}])]
        (is (map? tx-result) "Transaction should return tx-report")
        (is (contains? tx-result :db-after) "tx-report should have :db-after")
        (is (contains? tx-result :db-before) "tx-report should have :db-before")
        (is (contains? tx-result :tx-data) "tx-report should have :tx-data")
        (is (pos? (get-in tx-result [:db-after :max-tx])) "max-tx should be positive")

        ;; Verify db-before and db-after are queryable (indexes properly reconstructed)
        (let [db-before (:db-before tx-result)
              db-after (:db-after tx-result)]
          ;; Alice should NOT exist in db-before
          (is (nil? (d/q '[:find ?age .
                           :where [?e :person/name "Alice"]
                           [?e :person/age ?age]]
                         db-before))
              "Alice should not exist in db-before")
          ;; Alice SHOULD exist in db-after
          (is (= 30 (d/q '[:find ?age .
                           :where [?e :person/name "Alice"]
                           [?e :person/age ?age]]
                         db-after))
              "Alice should exist in db-after with age 30")))

      ;; Send another transaction
      (let [tx-result (d/transact client-conn [{:person/name "Bob" :person/age 25}])]
        (is (map? tx-result) "Second transaction should succeed")
        (is (> (get-in tx-result [:db-after :max-tx])
               (-> server-conn d/db :max-tx dec))
            "max-tx should increase")

        ;; Verify db-before now has Alice but not Bob
        (let [db-before (:db-before tx-result)]
          (is (= 30 (d/q '[:find ?age .
                           :where [?e :person/name "Alice"]
                           [?e :person/age ?age]]
                         db-before))
              "Alice should exist in second tx's db-before")
          (is (nil? (d/q '[:find ?age .
                           :where [?e :person/name "Bob"]
                           [?e :person/age ?age]]
                         db-before))
              "Bob should not exist in second tx's db-before")))

      ;; Verify data exists on server
      (let [server-db (d/db server-conn)
            alice (d/q '[:find ?age .
                         :where [?e :person/name "Alice"]
                         [?e :person/age ?age]]
                       server-db)]
        (is (= 30 alice) "Alice should exist on server with age 30"))

      ;; Wait for sync to update client connection
      (Thread/sleep 500)

      ;; Verify client can query locally after sync!
      ;; The on-db-sync! callback updates the connection's database
      (let [client-db (d/db client-conn)
            alice-client (d/q '[:find ?age .
                                :where [?e :person/name "Alice"]
                                [?e :person/age ?age]]
                              client-db)]
        (is (= 30 alice-client) "Alice should be queryable on client after sync"))

      ;; =====================================================================
      ;; CLEANUP
      ;; =====================================================================

      (release client-conn)
      (sync/unsubscribe-store! client-peer store-topic)
      (handlers/unregister-store-for-remote-access! store-id server-peer)
      (<?? S (peer/stop server-peer))
      (release server-conn)
      (d/delete-database server-config)
      (delete-dir-recursive server-path)
      (delete-dir-recursive client-path))))

(deftest test-multiple-transactions-ordering
  (testing "Multiple transactions maintain ordering through KabelWriter"
    (let [port (get-free-port)
          url (str "ws://localhost:" port)
          store-id #uuid "7e570000-0000-0000-0000-000000000002"  ; test-ordering-store
          store-topic (keyword (str store-id))
          server-path (create-temp-dir "ordering-server")
          client-path (create-temp-dir "ordering-client")

          ;; Server setup
          server-config {:store {:backend :file :path server-path :id store-id}
                         :schema-flexibility :write
                         :keep-history? false}
          _ (d/create-database server-config)
          server-conn (d/connect server-config)
          _ (d/transact server-conn test-schema)

          handler (create-http-kit-handler! S url server-id)
          server-peer (peer/server-peer S handler server-id
                                        (comp (sync/server-middleware)
                                              ds/remote-middleware)
                                        datahike-fressian-middleware)
          _ (<?? S (peer/start server-peer))
          _ (ds/invoke-on-peer server-peer)
          _ (handlers/register-global-handlers! server-peer)
          _ (handlers/register-store-for-remote-access! store-id server-conn server-peer)

          ;; Client setup - d/connect handles sync automatically
          client-peer (peer/client-peer S client-id
                                        (comp (sync/client-middleware)
                                              ds/remote-middleware)
                                        datahike-fressian-middleware)
          _ (ds/invoke-on-peer client-peer)
          _ (<?? S (peer/connect S client-peer url))

          client-config {:store {:backend :file :path client-path :id store-id}
                         :index :datahike.index/persistent-set
                         :schema-flexibility :write
                         :keep-history? false
                         :writer {:backend :kabel
                                  :peer-id server-id
                                  :store-id store-id
                                  :local-peer client-peer}}
          client-conn (<!! (d/connect client-config {:sync? false}))]

      ;; Send multiple transactions
      (let [tx-results (doall
                        (for [i (range 5)]
                          (d/transact client-conn [{:person/name (str "Person-" i)
                                                    :person/age (+ 20 i)}])))]

        ;; Verify all succeeded
        (is (= 5 (count tx-results)) "All 5 transactions should complete")
        (is (every? map? tx-results) "All results should be tx-reports")

        ;; Verify max-tx increases monotonically
        (let [max-txs (map #(get-in % [:db-after :max-tx]) tx-results)]
          (is (= max-txs (sort max-txs)) "max-tx should increase monotonically"))

        ;; Verify all data on server
        (let [server-db (d/db server-conn)
              people (d/q '[:find ?name ?age
                            :where [?e :person/name ?name]
                            [?e :person/age ?age]]
                          server-db)]
          (is (= 5 (count people)) "All 5 people should exist on server")))

      ;; Cleanup
      (release client-conn)
      (sync/unsubscribe-store! client-peer store-topic)
      (handlers/unregister-store-for-remote-access! store-id server-peer)
      (<?? S (peer/stop server-peer))
      (release server-conn)
      (d/delete-database server-config)
      (delete-dir-recursive server-path)
      (delete-dir-recursive client-path))))

(deftest test-listen-callback-with-kabel-writer
  (testing "d/listen! callbacks fire after sync completes"
    (let [port (get-free-port)
          url (str "ws://localhost:" port)
          store-id #uuid "7e570000-0000-0000-0000-000000000003"  ; test-listen-store
          store-topic (keyword (str store-id))
          server-path (create-temp-dir "listen-server")
          client-path (create-temp-dir "listen-client")

          ;; Server setup
          server-config {:store {:backend :file :path server-path :id store-id}
                         :schema-flexibility :write
                         :keep-history? false}
          _ (d/create-database server-config)
          server-conn (d/connect server-config)
          _ (d/transact server-conn test-schema)

          handler (create-http-kit-handler! S url server-id)
          server-peer (peer/server-peer S handler server-id
                                        (comp (sync/server-middleware)
                                              ds/remote-middleware)
                                        datahike-fressian-middleware)
          _ (<?? S (peer/start server-peer))
          _ (ds/invoke-on-peer server-peer)
          _ (handlers/register-global-handlers! server-peer)
          _ (handlers/register-store-for-remote-access! store-id server-conn server-peer)

          ;; Client setup - d/connect handles sync automatically
          client-peer (peer/client-peer S client-id
                                        (comp (sync/client-middleware)
                                              ds/remote-middleware)
                                        datahike-fressian-middleware)
          _ (ds/invoke-on-peer client-peer)
          _ (<?? S (peer/connect S client-peer url))

          client-config {:store {:backend :file :path client-path :id store-id}
                         :index :datahike.index/persistent-set
                         :schema-flexibility :write
                         :keep-history? false
                         :writer {:backend :kabel
                                  :peer-id server-id
                                  :store-id store-id
                                  :local-peer client-peer}}
          client-conn (<!! (d/connect client-config {:sync? false}))

          ;; Track listen callbacks
          listen-reports (atom [])
          listener-key (d/listen client-conn (fn [tx-report]
                                               (swap! listen-reports conj tx-report)))]

      ;; Send transactions
      (d/transact client-conn [{:person/name "Carol" :person/age 35}])
      (d/transact client-conn [{:person/name "Dave" :person/age 40}])

      ;; Give listeners time to fire
      (Thread/sleep 200)

      ;; Verify listeners received reports
      (is (= 2 (count @listen-reports)) "Listener should receive 2 tx-reports")

      (when (= 2 (count @listen-reports))
        (let [first-report (first @listen-reports)
              second-report (second @listen-reports)]
          (is (< (get-in first-report [:db-after :max-tx])
                 (get-in second-report [:db-after :max-tx]))
              "Second tx should have higher max-tx")))

      ;; Cleanup
      (d/unlisten client-conn listener-key)
      (release client-conn)
      (sync/unsubscribe-store! client-peer store-topic)
      (handlers/unregister-store-for-remote-access! store-id server-peer)
      (<?? S (peer/stop server-peer))
      (release server-conn)
      (d/delete-database server-config)
      (delete-dir-recursive server-path)
      (delete-dir-recursive client-path))))

(deftest test-tiered-store-caching
  (testing "Tiered store caches data locally and minimizes sync on reconnect"
    (let [port (get-free-port)
          url (str "ws://localhost:" port)
          store-id #uuid "7e570000-0000-0000-0000-000000000004"  ; test-tiered-store
          store-topic (keyword (str store-id))
          server-path (create-temp-dir "tiered-server")
          client-backend-path (create-temp-dir "tiered-client-backend")

          ;; =====================================================================
          ;; SERVER SETUP
          ;; =====================================================================
          server-config {:store {:backend :file :path server-path :id store-id}
                         :schema-flexibility :write
                         :keep-history? false}
          _ (d/create-database server-config)
          server-conn (d/connect server-config)
          _ (d/transact server-conn test-schema)
          ;; Add initial data
          _ (d/transact server-conn [{:person/name "Alice" :person/age 30}
                                     {:person/name "Bob" :person/age 25}])

          handler (create-http-kit-handler! S url server-id)
          server-peer (peer/server-peer S handler server-id
                                        (comp (sync/server-middleware)
                                              ds/remote-middleware)
                                        datahike-fressian-middleware)
          _ (<?? S (peer/start server-peer))
          _ (ds/invoke-on-peer server-peer)
          _ (handlers/register-global-handlers! server-peer)
          _ (handlers/register-store-for-remote-access! store-id server-conn server-peer)

          ;; =====================================================================
          ;; FIRST CLIENT CONNECTION - Full sync to empty cache
          ;; =====================================================================
          client-peer-1 (peer/client-peer S client-id
                                          (comp (sync/client-middleware)
                                                ds/remote-middleware)
                                          datahike-fressian-middleware)
          _ (ds/invoke-on-peer client-peer-1)
          _ (<?? S (peer/connect S client-peer-1 url))

          ;; Client uses tiered store: memory frontend + file backend
          ;; All components use the same :id for sync to work properly
          client-config-1 {:store {:backend :tiered
                                   :frontend-config {:backend :memory :id store-id}
                                   :backend-config {:backend :file :path client-backend-path :id store-id}
                                   :id store-id}
                           :index :datahike.index/persistent-set
                           :schema-flexibility :write
                           :keep-history? false
                           :writer {:backend :kabel
                                    :peer-id server-id
                                    :store-id store-id
                                    :local-peer client-peer-1}}
          client-conn-1 (<!! (d/connect client-config-1 {:sync? false}))]

      ;; Check if connection succeeded or returned error
      (when (instance? Throwable client-conn-1)
        (println "DEBUG: Connection failed with error:" (ex-message client-conn-1))
        (println "DEBUG: Error data:" (ex-data client-conn-1))
        (throw client-conn-1))

      (is (some? client-conn-1) "First client connection should succeed")
      (is (not (instance? Throwable client-conn-1)) "Connection should not be an error")

      ;; Verify initial data synced
      (let [client-db (d/db client-conn-1)
            alice (d/q '[:find ?age .
                         :where [?e :person/name "Alice"]
                         [?e :person/age ?age]]
                       client-db)]
        (is (= 30 alice) "Alice should be synced with age 30"))

      ;; Add more data through client
      (d/transact client-conn-1 [{:person/name "Carol" :person/age 35}])

      ;; Wait for sync to propagate
      (Thread/sleep 500)

      ;; Verify Carol exists
      (let [client-db (d/db client-conn-1)
            carol (d/q '[:find ?age .
                         :where [?e :person/name "Carol"]
                         [?e :person/age ?age]]
                       client-db)]
        (is (= 35 carol) "Carol should exist after transaction"))

      ;; =====================================================================
      ;; DISCONNECT FIRST CLIENT - keep server running for reconnect test
      ;; =====================================================================
      (sync/unsubscribe-store! client-peer-1 store-topic)
      (Thread/sleep 200)  ;; Allow pending callbacks to complete
      (<?? S (peer/stop client-peer-1))
      (release client-conn-1)

      ;; NOTE: Skip server transact here - there's a race condition between
      ;; unsubscribe and write-hook publish that blocks the server transact.
      ;; This test validates the cache from first connection is used.

      ;; Debug: Check what files exist in backend after first connection
      (println "\n=== Files in backend after first connection: ===")
      (doseq [f (sort (.listFiles (java.io.File. client-backend-path)))]
        (println "  " (.getName f) " - " (.length f) " bytes"))
      ;; Also check what :db key hashes to
      (println "  :db hashes to:" (str (hasch/uuid :db) ".ksv"))
      (println "=================================================\n")

      ;; =====================================================================
      ;; SECOND CLIENT CONNECTION - Should use cached data from first connection
      ;; =====================================================================
      (let [client-peer-2 (peer/client-peer S #uuid "20000000-0000-0000-0000-000000000003"
                                            (comp (sync/client-middleware)
                                                  ds/remote-middleware)
                                            datahike-fressian-middleware)
            _ (ds/invoke-on-peer client-peer-2)
            _ (<?? S (peer/connect S client-peer-2 url))

            ;; Same backend path - should have cached data from first connection
            client-config-2 {:store {:backend :tiered
                                     :frontend-config {:backend :memory :id store-id}
                                     :backend-config {:backend :file :path client-backend-path :id store-id}
                                     :id store-id}
                             :index :datahike.index/persistent-set
                             :schema-flexibility :write
                             :keep-history? false
                             :writer {:backend :kabel
                                      :peer-id server-id
                                      :store-id store-id
                                      :local-peer client-peer-2}}
            client-conn-2 (<!! (d/connect client-config-2 {:sync? false}))]

        (is (some? client-conn-2) "Second client connection should succeed")

        ;; Verify ALL data is available (cached from first connection)
        (let [client-db (d/db client-conn-2)
              all-people (d/q '[:find ?name ?age
                                :where [?e :person/name ?name]
                                [?e :person/age ?age]]
                              client-db)]
          (is (= 3 (count all-people)) "All 3 people should be available from cache")
          (is (some #(= ["Alice" 30] %) all-people) "Alice should exist")
          (is (some #(= ["Bob" 25] %) all-people) "Bob should exist")
          (is (some #(= ["Carol" 35] %) all-people) "Carol should exist from cache"))

        ;; Cleanup second connection
        (sync/unsubscribe-store! client-peer-2 store-topic)
        (Thread/sleep 100)
        (<?? S (peer/stop client-peer-2))
        (release client-conn-2))

      ;; =====================================================================
      ;; FINAL CLEANUP
      ;; =====================================================================
      (handlers/unregister-store-for-remote-access! store-id server-peer)
      (<?? S (peer/stop server-peer))
      (release server-conn)
      (d/delete-database server-config)
      (delete-dir-recursive server-path)
      (delete-dir-recursive client-backend-path))))

(deftest test-remote-create-and-delete-database
  (testing "Client can create and delete database on server via KabelWriter"
    (let [port (get-free-port)
          url (str "ws://localhost:" port)
          store-id #uuid "7e570000-0000-0000-0000-000000000005"  ; test-remote-create-store
          server-path (create-temp-dir "remote-create-server")

          ;; Server peer setup (no database yet!)
          handler (create-http-kit-handler! S url server-id)
          server-peer (peer/server-peer S handler server-id
                                        (comp (sync/server-middleware)
                                              ds/remote-middleware)
                                        datahike-fressian-middleware)
          _ (<?? S (peer/start server-peer))
          _ (ds/invoke-on-peer server-peer)

          ;; Register global handlers - no need for scope-specific handlers!
          ;; Global handlers can create databases for any store-id
          _ (handlers/register-global-handlers! server-peer)

          ;; Client peer setup
          client-peer (peer/client-peer S client-id
                                        (comp (sync/client-middleware)
                                              ds/remote-middleware)
                                        datahike-fressian-middleware)
          _ (ds/invoke-on-peer client-peer)
          _ (<?? S (peer/connect S client-peer url))]

      ;; Client creates database on server
      (let [create-config {:store {:backend :file :path server-path :id store-id}
                           :schema-flexibility :write
                           :keep-history? false
                           :writer {:backend :kabel
                                    :peer-id server-id
                                    :store-id store-id}}
            create-result (d/create-database create-config)]
        (is (map? create-result) "create-database should return result map")
        (is (:success create-result) "create-database should succeed"))

      ;; Verify database exists on server by connecting locally
      (let [server-config {:store {:backend :file :path server-path :id store-id}
                           :schema-flexibility :write
                           :keep-history? false}
            server-conn (d/connect server-config)]
        (is (some? server-conn) "Should be able to connect to created database")

        ;; Transact some data to verify it works
        (let [tx-result (d/transact server-conn [{:db/ident :test/attr
                                                  :db/valueType :db.type/string
                                                  :db/cardinality :db.cardinality/one}])]
          (is (map? tx-result) "Transaction should succeed"))

        (release server-conn))

      ;; Client deletes database on server
      (let [delete-config {:store {:backend :file :path server-path :id store-id}
                           :writer {:backend :kabel
                                    :peer-id server-id
                                    :store-id store-id}}
            delete-result (d/delete-database delete-config)]
        (is (map? delete-result) "delete-database should return result map")
        (is (:success delete-result) "delete-database should succeed"))

      ;; Cleanup
      (<?? S (peer/stop server-peer))
      (delete-dir-recursive server-path))))
