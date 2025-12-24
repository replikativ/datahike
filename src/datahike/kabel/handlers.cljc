(ns datahike.kabel.handlers
  "Server-side handlers for remote datahike operations via kabel.

   This namespace provides GLOBAL handlers that are registered with distributed-scope
   to handle remote transaction requests. The handlers look up the local
   connection by scope-id (passed in the request) and forward operations to its writer.

   Design:
   - Three global handlers: dispatch, create-database, delete-database
   - Scope-id is passed in the request payload, not in the handler name
   - The scope-registry maps scope-id -> {:conn connection :peer peer-atom}

   Usage (server-side):
   ```clojure
   ;; Register global handlers once at startup
   (register-global-handlers!)

   ;; Register a database for remote access
   (register-store-for-remote-access! scope-id conn peer)

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

;; Registry mapping scope-id -> {:conn connection :peer peer-atom}
;; Populated when stores are registered for remote access
(defonce scope-registry (atom {}))

(defn register-connection-for-scope!
  "Register a connection for a scope-id.

   Parameters:
   - scope-id: UUID identifying the store
   - conn: The datahike connection
   - peer: The kabel peer atom (for tx-report publishing)"
  [scope-id conn peer]
  (swap! scope-registry assoc scope-id {:conn conn :peer peer}))

(defn unregister-connection-for-scope!
  "Unregister a connection for a scope-id."
  [scope-id]
  (swap! scope-registry dissoc scope-id))

(defn get-connection-for-scope
  "Get the connection for a scope-id, or nil if not registered."
  [scope-id]
  (get-in @scope-registry [scope-id :conn]))

(defn get-peer-for-scope
  "Get the peer for a scope-id, or nil if not registered."
  [scope-id]
  (get-in @scope-registry [scope-id :peer]))

;; =============================================================================
;; Global Dispatch Handler
;; =============================================================================

(defn global-dispatch-handler
  "Global dispatch handler that routes transactions by scope-id.

   The handler:
   1. Extracts scope-id from the request
   2. Looks up the connection by scope-id
   3. Forwards the operation to the connection's writer
   4. Publishes tx-report to subscribers
   5. Returns the tx-report

   Request format:
   {:scope-id string :arg-map {:op 'transact! :args [...]}}"
  [{:keys [scope-id arg-map request-id]}]
  (go-try S
    ;; Normalize scope-id to string (store scopes are always strings)
          (let [scope-id (if (uuid? scope-id) (str scope-id) scope-id)]
            (log/trace "Global dispatch handler" {:scope-id scope-id :op (:op arg-map)})
            (if-let [conn (get-connection-for-scope scope-id)]
              (let [;; Get writer from connection
                    writer (:writer @(:wrapped-atom conn))
            ;; Dispatch to local writer
                    tx-report (<? S (writer/dispatch! writer arg-map))]

        ;; Publish tx-report to subscribers (if peer registered)
                (when-let [peer (get-peer-for-scope scope-id)]
                  (tx-broadcast/publish-tx-report! peer scope-id tx-report request-id))

        ;; Return tx-report - Fressian handlers handle serialization
                tx-report)

              (throw (ex-info "Store not found for scope"
                              {:scope-id scope-id
                               :registered-scopes (keys @scope-registry)}))))))

;; =============================================================================
;; Global Create/Delete Database Handlers
;; =============================================================================

(defn default-store-config-fn
  "Default store config factory - uses the client's store config with scope ensured.

   This is suitable when client and server use the same store backend.
   Override by passing :store-config-fn to register-global-handlers!
   for deployments where server needs different store configuration.

   Parameters:
   - scope-id: String identifier for this database
   - client-config: The config sent by the client (schema-flexibility, keep-history?, etc.)

   Returns: Store config map from client with :scope set to scope-id"
  [scope-id client-config]
  (assoc (:store client-config) :scope scope-id))

(defn- make-create-database-handler
  "Create a create-database handler that closes over the peer and store-config-fn.

   This handler:
   1. Creates the database on the server with :self writer
   2. Connects to it
   3. Registers it for remote access (dispatch + sync)

   Request format:
   {:config {:writer {:scope-id UUID :peer-id UUID}
             :schema-flexibility :write/:read
             :keep-history? bool
             ...}}

   The client sends logical config (schema-flexibility, keep-history?, etc.)
   and the scope-id. The server uses store-config-fn to determine the actual store backend."
  [peer store-config-fn]
  (fn [{:keys [config]}]
    (go-try S
            #?(:clj (println "[SERVER] create-database handler invoked!" config)
               :cljs (.log js/console "[SERVER] create-database handler invoked!" config))
            (let [scope-id (or (get-in config [:writer :scope-id])
                               (-> config :store :scope))
            ;; Normalize scope-id to string (store scopes are always strings)
                  scope-id (if (uuid? scope-id) (str scope-id) scope-id)
                  _ (do
                      #?(:clj (println "[SERVER] Processing scope-id:" scope-id)
                         :cljs (.log js/console "[SERVER] Processing scope-id:" scope-id))
                      (log/info "Global create-database request" {:scope-id scope-id}))

            ;; Build server-side config using store-config-fn
            ;; Client's store config is ignored - server controls the backend
                  store-config (store-config-fn scope-id config)
                  server-config {:store store-config
                                 :writer {:backend :self}
                           ;; Preserve logical config from client
                                 :schema-flexibility (or (:schema-flexibility config) :write)
                                 :keep-history? (get config :keep-history? false)}
                  _ (<? S (w/create-database server-config))
                  _ (log/trace "Database created" {:scope-id scope-id})

            ;; Connect and register for remote access
            ;; Note: d/connect is synchronous in JVM Clojure
                  conn (d/connect server-config)
                  _ (log/trace "Connected" {:scope-id scope-id})

            ;; Register connection in scope registry
                  _ (register-connection-for-scope! scope-id conn peer)

            ;; Register for konserve-sync
                  store (:store @(:wrapped-atom conn))
                  store-topic (keyword (str scope-id))
                  _ (sync/register-store! peer store-topic store
                                          {:walk-fn dh-walker/datahike-walk-fn
                                           :key-sort-fn (fn [k] (if (= k :db) 1 0))})
                  _ (log/trace "Registered for sync" {:scope-id scope-id :topic store-topic})

            ;; Register tx-report topic for pubsub
                  _ (tx-broadcast/register-tx-report-topic! peer scope-id)]

              (log/info "Database created and registered" {:scope-id scope-id})
              {:success true :scope-id scope-id :config server-config}))))

(defn- make-delete-database-handler
  "Create a delete-database handler that closes over the store-config-fn.

   This handler:
   1. Unregisters from sync and tx-broadcast
   2. Releases the connection
   3. Deletes the database

   Request format:
   {:config {:writer {:scope-id UUID :peer-id UUID} ...}}

   The client sends scope-id. The server uses store-config-fn to determine
   which store to delete."
  [_peer store-config-fn]  ;; peer looked up from scope-registry
  (fn [{:keys [config]}]
    (go-try S
            (let [scope-id (or (get-in config [:writer :scope-id])
                               (-> config :store :scope))
            ;; Normalize scope-id to string (store scopes are always strings)
                  scope-id (if (uuid? scope-id) (str scope-id) scope-id)
                  _ (log/info "Global delete-database request" {:scope-id scope-id})
                  {:keys [conn peer]} (get @scope-registry scope-id)
                  store-topic (keyword (str scope-id))
            ;; Build server-side config using store-config-fn
                  store-config (store-config-fn scope-id config)
                  server-config {:store store-config}]

              (when conn
          ;; Unregister from sync
                (when peer
                  (sync/unregister-store! peer store-topic)
                  (tx-broadcast/unregister-tx-report-topic! peer scope-id))

          ;; Remove from registry
                (unregister-connection-for-scope! scope-id)

          ;; Release connection
                (d/release conn)
                (log/trace "Connection released" {:scope-id scope-id}))

        ;; Delete database using server-side config
              (<? S (w/delete-database server-config))
              (log/info "Database deleted" {:scope-id scope-id})

              {:success true :scope-id scope-id}))))

;; =============================================================================
;; Global Handler Registration
;; =============================================================================

(defn register-global-handlers!
  "Register global handlers for datahike kabel operations.

   This registers three handlers that close over the provided peer:
   - datahike.kabel/dispatch - routes transactions by scope-id
   - datahike.kabel/create-database - creates database and registers for access
   - datahike.kabel/delete-database - unregisters and deletes database

   Call this once per peer at startup. Each peer should register its own handlers.
   The handlers are idempotent - calling multiple times just updates the handlers.

   Parameters:
   - peer: The kabel peer atom that will handle requests
   - opts: (optional) Options map
     - :store-config-fn - (fn [scope-id client-config] -> store-config)
       Function that returns the server-side store config for a given scope-id.
       Default: `default-store-config-fn` which uses the client's store config
       with the scope-id ensured.

   Example with custom store config (e.g., for different backend on server):
   ```clojure
   (register-global-handlers! peer
     {:store-config-fn (fn [scope-id _config]
                         {:backend :file
                          :path (str \"/var/data/datahike/\" scope-id)
                          :scope scope-id})})
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
  [scope-id]
  (log/warn "register-store-handlers! is deprecated. Use register-global-handlers! instead."
            {:scope-id scope-id}))

(defn unregister-store-handlers!
  "DEPRECATED: Global handlers don't need per-scope unregistration.

   This function is kept for backwards compatibility but does nothing."
  [scope-id]
  (log/warn "unregister-store-handlers! is deprecated. Global handlers are used."
            {:scope-id scope-id}))

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
   - scope-id: String or UUID identifying the store (will be normalized to string)
   - conn: The datahike connection
   - peer: The kabel peer atom"
  [scope-id conn peer]
  ;; Normalize scope-id to string
  (let [scope-id (if (uuid? scope-id) (str scope-id) scope-id)]
    (log/info "Registering store for remote access" {:scope-id scope-id})

    ;; Register connection lookup
    (register-connection-for-scope! scope-id conn peer)

    ;; Register for konserve-sync
    (let [store (:store @(:wrapped-atom conn))
          store-topic (keyword (str scope-id))]
      (sync/register-store! peer store-topic store
                            {:walk-fn dh-walker/datahike-walk-fn
                             :key-sort-fn (fn [k] (if (= k :db) 1 0))}))

    ;; Register tx-report topic for pubsub
    (tx-broadcast/register-tx-report-topic! peer scope-id)))

(defn unregister-store-for-remote-access!
  "Unregister a datahike store from remote access.

   Cleans up all registrations made by register-store-for-remote-access!.

   Parameters:
   - scope-id: String or UUID identifying the store (will be normalized to string)
   - peer: The kabel peer atom"
  [scope-id peer]
  ;; Normalize scope-id to string
  (let [scope-id (if (uuid? scope-id) (str scope-id) scope-id)]
    (log/info "Unregistering store from remote access" {:scope-id scope-id})

    ;; Unregister from sync
    (let [store-topic (keyword (str scope-id))]
      (sync/unregister-store! peer store-topic))

    ;; Unregister tx-report topic
    (tx-broadcast/unregister-tx-report-topic! peer scope-id)

    ;; Remove from registry
    (unregister-connection-for-scope! scope-id)))
