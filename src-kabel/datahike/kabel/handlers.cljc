(ns ^:no-doc datahike.kabel.handlers
  "Server-side handlers for remote datahike operations via kabel.

   This namespace provides GLOBAL handlers that are registered with distributed-scope
   to handle remote transaction requests. The handlers look up the local
   connection by store-id (passed in the request) and forward operations to its writer.

   Design:
   - Three global handlers: dispatch, create-database, delete-database
   - Scope-id is passed in the request payload, not in the handler name
   - The store-registry maps store-id -> {:conn connection :peer peer-atom}

   Usage (server-side):
   ```clojure
   ;; Register global handlers once at startup
   (register-global-handlers!)

   ;; Register a database for remote access
   (register-store-for-remote-access! store-id conn peer)

   ;; Now clients can send transactions to this store via KabelWriter
   ```"
  (:require [datahike.api :as d]
            [datahike.writer :as writer]
            [datahike.writing :as w]
            [is.simm.distributed-scope :as ds]
            [datahike.kabel.tx-broadcast :as tx-broadcast]
            [konserve-sync.core :as sync]
            [konserve-sync.walkers.datahike :as dh-walker]
            [kabel.peer :as peer]
            #?(:clj [superv.async :refer [go-try S <?]]
               :cljs [superv.async :refer [<?] :refer-macros [go-try]])
            #?(:clj [clojure.core.async :refer [go <! put!]]
               :cljs [clojure.core.async :refer [put!] :include-macros true])
            #?(:clj [taoensso.timbre :as log]
               :cljs [taoensso.timbre :as log :include-macros true]))
  #?(:cljs (:require-macros [clojure.core.async.macros :refer [go]])))

;; =============================================================================
;; Connection Registry
;; =============================================================================

;; Registry mapping store-id -> {:conn connection :peer peer-atom}
;; Populated when stores are registered for remote access
(defonce store-registry (atom {}))

(defn register-connection-for-store!
  "Register a connection for a store-id.

   Parameters:
   - store-id: UUID identifying the store
   - conn: The datahike connection
   - peer: The kabel peer atom (for tx-report publishing)"
  [store-id conn peer]
  (swap! store-registry assoc store-id {:conn conn :peer peer}))

(defn unregister-connection-for-store!
  "Unregister a connection for a store-id."
  [store-id]
  (swap! store-registry dissoc store-id))

(defn get-connection-for-store
  "Get the connection for a store-id, or nil if not registered."
  [store-id]
  (get-in @store-registry [store-id :conn]))

(defn get-peer-for-store
  "Get the peer for a store-id, or nil if not registered."
  [store-id]
  (get-in @store-registry [store-id :peer]))

;; =============================================================================
;; Global Dispatch Handler
;; =============================================================================

(defn global-dispatch-handler
  "Global dispatch handler that routes transactions by store-id.

   The handler:
   1. Extracts store-id from the request
   2. Looks up the connection by store-id
   3. Forwards the operation to the connection's writer
   4. Publishes tx-report to subscribers
   5. Returns the tx-report

   Request format:
   {:store-id UUID :arg-map {:op 'transact! :args [...]}}"
  [{:keys [store-id arg-map request-id]}]
  (go-try S
          (log/trace "Global dispatch handler" {:store-id store-id :op (:op arg-map)})
          (if-let [conn (get-connection-for-store store-id)]
            (let [;; Get writer from connection
                  writer (:writer @(:wrapped-atom conn))
            ;; Dispatch to local writer
                  tx-report (<? S (writer/dispatch! writer arg-map))]

        ;; Publish tx-report to subscribers (if peer registered)
              (when-let [peer (get-peer-for-store store-id)]
                (tx-broadcast/publish-tx-report! peer store-id tx-report request-id))

        ;; Return tx-report - Fressian handlers handle serialization
              tx-report)

            (throw (ex-info "Store not found for store-id"
                            {:store-id store-id
                             :registered-stores (keys @store-registry)})))))

;; =============================================================================
;; Global Create/Delete Database Handlers
;; =============================================================================

(defn default-store-config-fn
  "Default store config factory - uses the client's store config unchanged.

   This is suitable when client and server use the same store backend.
   Override by passing :store-config-fn to register-global-handlers!
   for deployments where server needs different store configuration.

   Parameters:
   - store-id: UUID identifying the store (from client's :store :id)
   - client-config: The config sent by the client (schema-flexibility, keep-history?, etc.)

   Returns: Store config map from client (preserves UUID :id from client)"
  [store-id client-config]
  ;; Return client's store config unchanged - :id is already a UUID
  (:store client-config))

(defn- make-create-database-handler
  "Create a create-database handler that closes over the peer and store-config-fn.

   This handler:
   1. Creates the database on the server with :self writer
   2. Connects to it
   3. Registers it for remote access (dispatch + sync)

   Request format:
   {:config {:writer {:store-id UUID :peer-id UUID}
             :schema-flexibility :write/:read
             :keep-history? bool
             ...}}

   The client sends logical config (schema-flexibility, keep-history?, etc.)
   and the store-id. The server uses store-config-fn to determine the actual store backend."
  [peer store-config-fn]
  (fn [{:keys [config]}]
    (go-try S
            #?(:clj (println "[SERVER] create-database handler invoked!" config)
               :cljs (.log js/console "[SERVER] create-database handler invoked!" config))
            (let [store-id (-> config :store :id)  ;; Extract UUID from store config
                  _ (do
                      #?(:clj (println "[SERVER] Processing store-id:" store-id)
                         :cljs (.log js/console "[SERVER] Processing store-id:" store-id))
                      (log/info "Global create-database request" {:store-id store-id}))

            ;; Build server-side config using store-config-fn
            ;; Client's store config is ignored - server controls the backend
                  store-config (store-config-fn store-id config)
                  server-config {:store store-config
                                 :writer {:backend :self}
                           ;; Preserve logical config from client
                                 :schema-flexibility (or (:schema-flexibility config) :write)
                                 :keep-history? (get config :keep-history? false)}
                  _ (<? S (w/create-database server-config))
                  _ (log/trace "Database created" {:store-id store-id})

            ;; Connect and register for remote access
            ;; Note: d/connect is synchronous in JVM Clojure
                  conn (d/connect server-config)
                  _ (log/trace "Connected" {:store-id store-id})

            ;; Register connection in store registry (use UUID directly)
                  _ (register-connection-for-store! store-id conn peer)

            ;; Register for konserve-sync (use UUID as topic to match client)
                  store (:store @(:wrapped-atom conn))
                  _ (sync/register-store! peer store-id store
                                          {:walk-fn dh-walker/datahike-walk-fn
                                           :key-sort-fn (fn [k] (if (= k :db) 1 0))})
                  _ (log/trace "Registered for sync" {:store-id store-id})

            ;; Register tx-report topic for pubsub (use UUID directly)
                  _ (tx-broadcast/register-tx-report-topic! peer store-id)]

              (log/info "Database created and registered" {:store-id store-id})
              {:success true :store-id store-id :config server-config}))))

(defn- make-delete-database-handler
  "Create a delete-database handler that closes over the store-config-fn.

   This handler:
   1. Unregisters from sync and tx-broadcast
   2. Releases the connection
   3. Deletes the database

   Request format:
   {:config {:writer {:store-id UUID :peer-id UUID} ...}}

   The client sends store-id. The server uses store-config-fn to determine
   which store to delete."
  [_peer store-config-fn]  ;; peer looked up from store-registry
  (fn [{:keys [config]}]
    (go-try S
            (let [store-id (-> config :store :id)  ;; Extract UUID from store config
                  _ (log/info "Global delete-database request" {:store-id store-id})
                  {:keys [conn peer]} (get @store-registry store-id)
            ;; Build server-side config using store-config-fn
                  store-config (store-config-fn store-id config)
                  server-config {:store store-config}]

              (when conn
          ;; Unregister from sync (use UUID directly)
                (when peer
                  (sync/unregister-store! peer store-id)
                  (tx-broadcast/unregister-tx-report-topic! peer store-id))

          ;; Remove from registry (use UUID directly)
                (unregister-connection-for-store! store-id)

          ;; Release connection
                (d/release conn)
                (log/trace "Connection released" {:store-id store-id}))

        ;; Delete database using server-side config
              (<? S (w/delete-database server-config))
              (log/info "Database deleted" {:store-id store-id})

              {:success true :store-id store-id}))))

;; =============================================================================
;; Global Handler Registration
;; =============================================================================

(defn register-global-handlers!
  "Register global handlers for datahike kabel operations.

   This registers three handlers that close over the provided peer:
   - datahike.kabel/dispatch - routes transactions by store-id
   - datahike.kabel/create-database - creates database and registers for access
   - datahike.kabel/delete-database - unregisters and deletes database

   Call this once per peer at startup. Each peer should register its own handlers.
   The handlers are idempotent - calling multiple times just updates the handlers.

   Parameters:
   - peer: The kabel peer atom that will handle requests
   - opts: (optional) Options map
     - :store-config-fn - (fn [store-id client-config] -> store-config)
       Function that returns the server-side store config for a given store-id.
       Default: `default-store-config-fn` which uses the client's store config
       with the store-id ensured.

   Example with custom store config (e.g., for different backend on server):
   ```clojure
   (register-global-handlers! peer
     {:store-config-fn (fn [store-id _config]
                         {:backend :file
                          :path (str \"/var/data/datahike/\" store-id)
                          :id store-id})})
   ```"
  ([peer] (register-global-handlers! peer {}))
  ([peer opts]
   (let [store-config-fn (or (:store-config-fn opts) default-store-config-fn)]
     (log/trace "Registering global datahike.kabel handlers" {:peer-id (some-> @peer :id)})
     #?(:clj (println "[SERVER] Registering handlers...")
        :cljs (.log js/console "[SERVER] Registering handlers..."))
     (ds/register-remote-fn! 'datahike.kabel/dispatch global-dispatch-handler)
     (ds/register-remote-fn! 'datahike.kabel/create-database (make-create-database-handler peer store-config-fn))
     (ds/register-remote-fn! 'datahike.kabel/delete-database (make-delete-database-handler peer store-config-fn))
     #?(:clj (println "[SERVER] Handlers registered: dispatch, create-database, delete-database")
        :cljs (.log js/console "[SERVER] Handlers registered: dispatch, create-database, delete-database")))))

;; =============================================================================
;; Legacy Scope-Specific Handler Registration (Deprecated)
;; =============================================================================

(defn register-store-handlers!
  "DEPRECATED: Use register-global-handlers! instead.

   This function is kept for backwards compatibility but does nothing
   since global handlers are now used."
  [store-id]
  (log/warn "register-store-handlers! is deprecated. Use register-global-handlers! instead."
            {:store-id store-id}))

(defn unregister-store-handlers!
  "DEPRECATED: Global handlers don't need per-scope unregistration.

   This function is kept for backwards compatibility but does nothing."
  [store-id]
  (log/warn "unregister-store-handlers! is deprecated. Global handlers are used."
            {:store-id store-id}))

;; =============================================================================
;; Convenience API
;; =============================================================================

(defn register-store-for-remote-access!
  "Register a datahike store for remote access via kabel.

   This function:
   1. Registers the connection in the scope registry
   2. Registers for konserve-sync
   3. Registers tx-report topic for pubsub broadcasting

   Note: Global handlers (dispatch, create-database, delete-database) must be
   registered first via register-global-handlers!.

   Call this on the server/owner peer to make a store available
   for remote transactions.

   Parameters:
   - store-id: UUID identifying the store (from store :id)
   - conn: The datahike connection
   - peer: The kabel peer atom"
  [store-id conn peer]
  (log/info "Registering store for remote access" {:store-id store-id})

  ;; Register connection lookup
  (register-connection-for-store! store-id conn peer)

  ;; Register for konserve-sync (use UUID directly as topic)
  (let [store (:store @(:wrapped-atom conn))]
    (sync/register-store! peer store-id store
                          {:walk-fn dh-walker/datahike-walk-fn
                           :key-sort-fn (fn [k] (if (= k :db) 1 0))}))

  ;; Register tx-report topic for pubsub
  (tx-broadcast/register-tx-report-topic! peer store-id))

(defn unregister-store-for-remote-access!
  "Unregister a datahike store from remote access.

   Cleans up all registrations made by register-store-for-remote-access!.

   Parameters:
   - store-id: UUID identifying the store (from store :id)
   - peer: The kabel peer atom"
  [store-id peer]
  (log/info "Unregistering store from remote access" {:store-id store-id})

  ;; Unregister from sync (use UUID directly as topic)
  (sync/unregister-store! peer store-id)

  ;; Unregister tx-report topic
  (tx-broadcast/unregister-tx-report-topic! peer store-id)

  ;; Remove from registry
  (unregister-connection-for-store! store-id))
