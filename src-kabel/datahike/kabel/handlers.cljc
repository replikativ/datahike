(ns ^:no-doc datahike.kabel.handlers
  "Server-side handlers for remote datahike operations via kabel.

   This namespace provides GLOBAL handlers that are registered with kabel.remote
   to handle remote transaction requests. The handlers look up the local
   connection by store-id and branch and forward operations to its writer.

   Design:
   - Three global handlers: dispatch, create-database, delete-database
   - Store-id and branch are passed in the request payload, not in the handler name
   - The store-registry maps [store-id branch] -> {:conn connection :peer peer-atom}

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
            [kabel.remote :as remote]
            [datahike.kabel.tx-broadcast :as tx-broadcast]
            [konserve-sync.core :as sync]
            ;; datahike's own walker — follows :db.type/store-ref values so referenced
            ;; blobs replicate, unlike konserve-sync's index-only walker.
            [datahike.kabel.walker :as dh-walker]
            [kabel.peer :as peer]
            #?(:clj [superv.async :refer [go-try S <?]]
               :cljs [superv.async :refer [<?] :refer-macros [go-try]])
            #?(:clj [clojure.core.async :as async :refer [go <! put!]]
               :cljs [clojure.core.async :refer [put!] :include-macros true])
            #?(:clj [replikativ.logging :as log]
               :cljs [replikativ.logging :as log :include-macros true]))
  #?(:cljs (:require-macros [clojure.core.async.macros :refer [go]])))

;; =============================================================================
;; Connection Registry
;; =============================================================================

;; Registry mapping [store-id branch] -> {:conn connection :peer peer-atom}.
;; A store can have several independently advancing branch heads; using only
;; store-id here can route a fork write to the trunk writer and leave its client
;; waiting forever for a watermark that never appears on the subscribed branch.
;; Populated when stores are registered for remote access
(defonce store-registry (atom {}))

(defn- registry-key [store-id branch]
  [store-id (or branch :db)])

(defn- connection-branch [conn]
  (or (some-> conn :wrapped-atom deref :config :branch) :db))

(defn register-connection-for-store!
  "Register a connection for a store-id and branch.

   Parameters:
   - store-id: UUID identifying the store
   - conn: The datahike connection
   - peer: The kabel peer atom (for tx-report publishing)"
  ([store-id conn peer]
   (register-connection-for-store! store-id (connection-branch conn) conn peer))
  ([store-id branch conn peer]
   (swap! store-registry assoc (registry-key store-id branch)
          {:conn conn :peer peer})))

(defn unregister-connection-for-store!
  "Unregister one branch, or every registered branch for a store-id."
  ([store-id]
   (swap! store-registry
          (fn [registry]
            (into {} (remove (fn [[[registered-store-id _] _]]
                               (= store-id registered-store-id)))
                  registry))))
  ([store-id branch]
   (swap! store-registry dissoc (registry-key store-id branch))))

(defn get-connection-for-store
  "Get the connection for a store-id and branch, or nil if not registered."
  ([store-id] (get-connection-for-store store-id :db))
  ([store-id branch]
   (get-in @store-registry [(registry-key store-id branch) :conn])))

(defn get-peer-for-store
  "Get the peer for a store-id and branch, or nil if not registered."
  ([store-id] (get-peer-for-store store-id :db))
  ([store-id branch]
   (get-in @store-registry [(registry-key store-id branch) :peer])))

(defn- store-registrations [store-id]
  (keep (fn [[[registered-store-id _] registration]]
          (when (= store-id registered-store-id) registration))
        @store-registry))

;; =============================================================================
;; Global Dispatch Handler
;; =============================================================================

(defn global-dispatch-handler
  "Global dispatch handler that routes transactions by store-id and branch.

   The handler:
   1. Extracts store-id and branch from the request
   2. Looks up the exact branch connection
   3. Forwards the operation to the connection's writer
   4. Publishes tx-report to subscribers
   5. Returns the tx-report

   Request format:
   Old clients that omit :branch address :db for wire compatibility.

   Request format:
   {:store-id UUID :branch :db :arg-map {:op 'transact! :args [...]}}"
  [{:keys [store-id branch arg-map request-id]
    :or {branch :db}}]
  (go-try S
          ;; The request id is echoed to every tx-report subscriber; a client
          ;; does not get to choose its shape. (Older callers omit it.)
          (when (and (some? request-id) (not (uuid? request-id)))
            (throw (ex-info "request-id must be a UUID"
                            {:type :datahike.kabel/invalid-request-id})))
          (log/trace "Global dispatch handler" {:store-id store-id
                                                :branch branch
                                                :op (:op arg-map)})
          (if-let [conn (get-connection-for-store store-id branch)]
            (let [;; Get writer from connection
                  writer (:writer @(:wrapped-atom conn))
            ;; Dispatch to local writer
                  tx-report (<? S (writer/dispatch! writer arg-map))]

        ;; Publish tx-report to subscribers (if peer registered)
              (when-let [peer (get-peer-for-store store-id branch)]
                (tx-broadcast/publish-tx-report! peer store-id tx-report request-id))

        ;; Return the op's result unchanged. Stripping the live DBs out of a
        ;; TxReport is the CODEC's job, done by the tag-27 write handler
        ;; `datahike.cbor` registers for the TxReport class — so it happens for
        ;; a TxReport and only for a TxReport. Projecting here instead meant
        ;; projecting every op's result, and `gc-storage!` returns a set.
              tx-report)

            (throw (ex-info "Store branch is not registered for remote writes"
                            {:store-id store-id
                             :branch branch
                             :registered-branches
                             (->> (keys @store-registry)
                                  (keep (fn [[registered-store-id registered-branch]]
                                          (when (= store-id registered-store-id)
                                            registered-branch)))
                                  set)})))))

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
            (let [store-id (-> config :store :id)  ;; Extract UUID from store config
                  _ (when-not (uuid? store-id)
                      (throw (ex-info "A browser database is identified by a UUID"
                                      {:type :datahike.kabel/invalid-store-id :store-id store-id})))
                  _ (log/info "Global create-database request" {:store-id store-id})

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

            ;; Connect and register for remote access. Asynchronously: this
            ;; handler runs inside a go block on some servers, and a blocking
            ;; connect there holds one of the dispatch pool's few threads.
                  conn (<? S (d/connect server-config {:sync? false}))
                  _ (log/trace "Connected" {:store-id store-id})

            ;; Register connection in store registry (use UUID directly)
                  _ (register-connection-for-store! store-id conn peer)

            ;; Register for konserve-sync (use UUID as topic to match client)
                  store (:store @(:wrapped-atom conn))
                  ;; no :key-sort-fn — the walk and the commit batch both carry the order
                  ;; (mutable branch pointers last); :always-send-mutable? so a branch head
                  ;; is never timestamp-deduped away. See register-store-for-remote-access!
                  _ (sync/register-store! peer store-id store
                                          {:walk-fn dh-walker/datahike-walk-fn
                                           :always-send-mutable? true})
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
                  _ (when-not (uuid? store-id)
                      (throw (ex-info "A browser database is identified by a UUID"
                                      {:type :datahike.kabel/invalid-store-id :store-id store-id})))
                  _ (log/info "Global delete-database request" {:store-id store-id})
                  registrations (vec (store-registrations store-id))
                  peer (:peer (first registrations))
                  conns (distinct (keep :conn registrations))
            ;; Build server-side config using store-config-fn
                  store-config (store-config-fn store-id config)
                  server-config {:store store-config}]

              (when (seq registrations)
          ;; Unregister from sync (use UUID directly)
                (when peer
                  (sync/unregister-store! peer store-id)
                  (tx-broadcast/unregister-tx-report-topic! peer store-id))

          ;; Remove from registry (use UUID directly)
                (unregister-connection-for-store! store-id)

          ;; Release every branch connection registered for this store.
          ;; Releasing closes stores and index writers, blocking work that must
          ;; not run on a go dispatch thread: on the JVM it runs on its own.
                #?(:clj  (<? S (async/thread (doseq [conn conns] (d/release conn)) true))
                   :cljs (doseq [conn conns] (d/release conn)))
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
     - :wrap-handler - (fn [handler] -> handler) applied to each handler as it
       is registered; the server wraps them in the dynamic bindings a client
       request runs under (e.g. the query function resolver).
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
   (let [store-config-fn (or (:store-config-fn opts) default-store-config-fn)
         wrap            (or (:wrap-handler opts) identity)]
     (log/trace "Registering global datahike.kabel handlers" {:peer-id (some-> @peer :id)})
     (remote/register! 'datahike.kabel/dispatch (wrap global-dispatch-handler))
     (remote/register! 'datahike.kabel/create-database (wrap (make-create-database-handler peer store-config-fn)))
     (remote/register! 'datahike.kabel/delete-database (wrap (make-delete-database-handler peer store-config-fn)))
     (log/info "Registered global datahike.kabel handlers" {:peer-id (some-> @peer :id)}))))

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
   - peer: The kabel peer atom
   - opts (optional): {:branches <scope>} forwarded to the walker. `:all`
     (default) syncs every branch's nodes so a subscriber can `branch-as-db`
     any branch locally; `:trunk` / a branch keyword / a coll scopes the
     initial node sync to those branches (lean replica of the active branch —
     out-of-scope branches must be fetched on demand). See
     `datahike.kabel.walker/datahike-walk-fn`."
  ([store-id conn peer]
   (register-store-for-remote-access! store-id conn peer nil))
  ([store-id conn peer {:keys [branches]}]
   (let [branch (connection-branch conn)]
     (log/info "Registering store for remote access" {:store-id store-id
                                                      :branch branch
                                                      :branches branches}))

  ;; Register connection lookup
   (register-connection-for-store! store-id conn peer)

  ;; Register for konserve-sync (use UUID directly as topic)
   (let [store   (:store @(:wrapped-atom conn))
         walk-fn (if branches
                   (fn [s opts] (dh-walker/datahike-walk-fn s (assoc opts :branches branches)))
                   dh-walker/datahike-walk-fn)]
     ;; No :key-sort-fn. The order is CARRIED, not guessed:
     ;;   • handshake — datahike-walk-fn returns an ordered vector, index nodes first and
     ;;     the mutable branch pointers LAST, and konserve-sync preserves walk order;
     ;;   • ongoing   — a commit is an ordered multi-assoc batch (branch head last), which
     ;;     konserve-sync relays verbatim.
     ;; Both give the one guarantee that matters: a branch head is applied only after every
     ;; index node it references. :key-sort-fn used to reconstruct that downstream by
     ;; sorting on the SHAPE of the key ("keywords are roots, sort them last") — a guess
     ;; that happened to fit datahike's keys, and would silently misorder any store whose
     ;; keys didn't.
     ;;
     ;; :always-send-mutable? — the branch heads must never be timestamp-deduped away.
     ;; Index nodes are content-addressed and write-once, so "does the peer have it?" is
     ;; settled by presence and the timestamps are harmless. A branch HEAD is the one
     ;; MUTABLE cell — same key, new value every commit — and there a wall-clock compare
     ;; across two machines answers the wrong question: a peer that wrote its own copy
     ;; (e.g. a tiered reader pre-filling its cache from the backing store before it
     ;; subscribes) stamps a LATER time than our commit, so we would conclude it is
     ;; "current" and skip the head entirely. It would then never receive one — and a
     ;; peer whose clock merely runs fast would sit on a stale head indefinitely.
     ;; Commit metadata already marks the nodes :immutable? and leaves the head unmarked,
     ;; so this needs no guesswork about keys.
     (sync/register-store! peer store-id store {:walk-fn walk-fn
                                                :always-send-mutable? true}))

  ;; Register tx-report topic for pubsub
   (tx-broadcast/register-tx-report-topic! peer store-id)))

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
