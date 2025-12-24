(ns datahike.kabel.browser-test-server
  "JVM server for browser integration tests.
   
   Provides a lightweight Kabel server that:
   - Listens on fixed port 47296 for WebSocket connections
   - Registers global Datahike handlers for remote operations
   - Manages test database lifecycle
   - Uses file-backed store on JVM side
   
   Usage:
   (start-test-server!)  ; Start server
   (stop-test-server!)   ; Stop server"
  (:require [datahike.api :as d]
            [datahike.kabel.handlers :as handlers]
            [datahike.kabel.fressian-handlers :as fh]
            [kabel.peer :as peer]
            [kabel.http-kit :refer [create-http-kit-handler!]]
            [kabel.middleware.fressian :refer [fressian]]
            [konserve-sync.core :as sync]
            [is.simm.distributed-scope :refer [remote-middleware]]
            [superv.async :refer [S go-try <?]]
            [clojure.java.io :as io]
            [clojure.core.async :refer [<!!]]
            [taoensso.timbre :as log]))

;; =============================================================================
;; Configuration
;; =============================================================================

(def test-server-port 47296)
(def test-server-url (str "ws://localhost:" test-server-port))
(def test-server-id #uuid "aaaaaaaa-0000-0000-0000-000000000001")

;; =============================================================================
;; Server State
;; =============================================================================

(defonce ^:private server-state (atom nil))

;; =============================================================================
;; Helper Functions
;; =============================================================================

(defn create-temp-dir
  "Create a temporary directory for test stores."
  [prefix]
  (let [temp-dir (io/file (System/getProperty "java.io.tmpdir")
                          (str prefix "-" (System/currentTimeMillis) "-" (rand-int 10000)))]
    (.mkdirs temp-dir)
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

(defn server-store-config-fn
  "Create server-side store config for a given scope-id.
   Uses file backend since client's TieredStore won't work on JVM."
  [scope-id _client-config]
  (let [base-path (:temp-dir @server-state)]
    {:backend :file
     :path (str base-path "/" scope-id)
     :scope scope-id}))

;; =============================================================================
;; Server Lifecycle
;; =============================================================================

(defn start-test-server!
  "Start the test server for browser integration tests.
   
   Returns a map with:
   - :server - The kabel server peer
   - :temp-dir - Path to temporary directory for stores
   - :url - WebSocket URL for clients"
  []
  (when @server-state
    (throw (ex-info "Test server already running" @server-state)))

  (log/info "Starting browser test server on port" test-server-port)

  (let [temp-dir (create-temp-dir "datahike-browser-test")
        ;; Create server peer with distributed-scope and konserve-sync middleware
        server (peer/server-peer
                S
                (create-http-kit-handler! S test-server-url test-server-id)
                test-server-id
                 ;; Middleware composition (innermost runs first):
                 ;; 1. remote-middleware - distributed-scope for remote function invocation
                 ;; 2. sync/server-middleware - konserve-sync for store replication
                (comp (sync/server-middleware)
                      remote-middleware)
                datahike-fressian-middleware)]

    ;; Start the server
    (<!! (go-try S (<? S (peer/start server))))

    ;; Set up distributed-scope to process remote invocations
    ;; This enables the server to receive and execute remote function calls
    (is.simm.distributed-scope/invoke-on-peer server)

    ;; Register global handlers for remote Datahike operations
    ;; This enables clients to use d/create-database, d/delete-database, and d/transact
    (handlers/register-global-handlers! server {:store-config-fn server-store-config-fn})

    (log/info "Browser test server started successfully"
              {:url test-server-url
               :temp-dir temp-dir})

    (let [state {:server server
                 :temp-dir temp-dir
                 :url test-server-url}]
      (reset! server-state state)
      state)))

(defn stop-test-server!
  "Stop the test server and clean up resources."
  []
  (when-let [state @server-state]
    (log/info "Stopping browser test server")

    ;; Stop the server
    (when-let [server (:server state)]
      (<!! (go-try S (<? S (peer/stop server)))))

    ;; Clean up temp directory
    (when-let [temp-dir (:temp-dir state)]
      (delete-dir-recursive temp-dir))

    (reset! server-state nil)
    (log/info "Browser test server stopped")))

(defn get-server-info
  "Get current server info (url, etc.) if running."
  []
  @server-state)

;; =============================================================================
;; REPL Helper
;; =============================================================================

(comment
  ;; Start server for manual browser testing
  (start-test-server!)

  ;; Check server status
  (get-server-info)

  ;; Stop server
  (stop-test-server!))
