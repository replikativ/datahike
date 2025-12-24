(ns datahike.kabel.connector
  "Client-side connection for remote Datahike databases via KabelWriter.

   This namespace provides the internal connection logic for :kabel writer backends.
   It is called by datahike.connector when a kabel writer is detected."
  (:require [datahike.db :as db]
            [datahike.store :as ds]
            [datahike.writing :as dw]
            [datahike.writer :as w]
            [datahike.tools :as dt]
            [datahike.config :as dc]
            [datahike.kabel.writer :as kw]
            [datahike.kabel.fressian-handlers :as fh]
            [datahike.connections :refer [add-connection!]]
            [datahike.connector :refer [->Connection]]
            [konserve-sync.transport.kabel-pubsub :as kp]
            [konserve-sync.walkers.datahike :as dh-walker]
            [konserve.tiered :as kt]
            [konserve.core :as k]
            #?(:clj [clojure.core.async :refer [promise-chan put!]]
               :cljs [clojure.core.async :refer [promise-chan put!] :include-macros true])
            #?(:clj [superv.async :refer [go-try- <?-]]
               :cljs [superv.async :refer [go-try- <?-] :include-macros true])
            #?(:clj [taoensso.timbre :as log]
               :cljs [taoensso.timbre :as log :include-macros true])))

;; =============================================================================
;; TieredStore Support
;; =============================================================================

(defn- make-branch-walk-fn
  "Create a walk function for perform-walk-sync that walks a specific branch.

   The branch key (e.g., :db) is stored in the store like any other stored-db.
   This walker fetches the branch's stored-db and discovers all reachable BTSet
   node addresses from its indices."
  [branch]
  (fn [backend-store _root-values opts]
    ;; The datahike-walk-fn hardcodes :db, but we want to walk from `branch`.
    ;; For now, datahike-walk-fn works because branch defaults to :db.
    ;; TODO: Parameterize datahike-walk-fn to accept branch key
    (dh-walker/datahike-walk-fn backend-store opts)))

(defn- populate-tiered-from-cache!
  "For tiered stores, populate memory frontend from backend (IndexedDB) cache.

   This should be called BEFORE subscribing to konserve-sync so that:
   1. Memory has cached data for immediate queries
   2. Sync handshake sends accurate timestamps → server only sends newer keys

   Uses datahike walker to sync only reachable keys from the branch,
   avoiding syncing old/garbage data."
  [store store-config branch opts]
  (go-try-
   (let [frontend (:frontend-store store)
         backend (:backend-store store)]
     (log/trace "Populating tiered store from cache" {:branch branch})
      ;; Ready sub-stores individually (skip the default sync-on-connect)
      ;; Pass opts with :sync? false to ensure async operation
     (log/trace "Calling ready-store on frontend...")
     (<?- (ds/ready-store (assoc (:frontend-store store-config) :opts opts) frontend))
     (log/trace "Frontend ready. Calling ready-store on backend...")
     (<?- (ds/ready-store (assoc (:backend-store store-config) :opts opts) backend))
     (log/trace "Backend ready. Calling perform-walk-sync...")
      ;; Walk-sync with datahike walker for the branch
     (let [walk-opts (assoc opts :sync? false)
           _ (log/trace "Walk opts: sync?" (:sync? walk-opts))
           result (<?- (kt/perform-walk-sync
                        frontend
                        backend
                        [branch]  ;; Root keys = the branch
                        (make-branch-walk-fn branch)
                        walk-opts))]
       (log/trace "Tiered store populated from cache" {:keys-synced (count result)})
       result))))

;; =============================================================================
;; Connection
;; =============================================================================

(defn connect-kabel
  "Connect to a remote database via KabelWriter.

   This is a special connection flow for :kabel writer backends where the
   database doesn't exist locally - it will be synchronized from a remote peer.

   The flow:
   1. Create local store with Datahike handlers
   2. Set up sync subscription via konserve-sync
   3. Wait for initial :db to be synchronized
   4. Create connection with the synced database
   5. Create KabelWriter for remote transactions

   The writer config must include:
   - :peer-id - UUID of the remote peer (server) for RPC
   - :scope-id - UUID/keyword for the store topic (sync subscription)
   - :local-peer - The kabel client peer atom (for sync subscription)

   Called by datahike.connector/-connect* when writer backend is :kabel.

   Always returns a channel - kabel connections are inherently async due to
   network sync. Caller must take from the channel to get the connection."
  [raw-config opts]
  (go-try-
   (let [;; Normalize config with defaults (cache sizes, etc.)
          ;; This ensures connect-kabel can be called directly with raw user config
         config (dissoc (dc/load-config raw-config) :initial-tx :remote-peer :name)
         store-config (:store config)
         store-id (ds/store-identity store-config)
         branch (or (:branch config) :db)
         conn-id [store-id branch]
         {:keys [peer-id scope-id local-peer]} (:writer config)
         is-tiered? (= :tiered (:backend store-config))
         _ (log/trace "Connecting via KabelWriter" {:scope-id scope-id
                                                    :peer-id peer-id
                                                    :branch branch
                                                    :tiered? is-tiered?})

          ;; Validate required kabel config
         _ (when-not local-peer
             (dt/raise "KabelWriter requires :local-peer in writer config"
                       {:type :kabel-missing-local-peer
                        :config (:writer config)}))
         _ (when-not scope-id
             (dt/raise "KabelWriter requires :scope-id in writer config"
                       {:type :kabel-missing-scope-id
                        :config (:writer config)}))

          ;; 1. Create store with Datahike handlers
          ;; Use empty-store instead of connect-store because kabel clients
          ;; receive data via sync - the store may not exist yet
         _ (log/trace "Creating store..." {:backend (:backend store-config)
                                           :scope (:scope store-config)})
         raw-store (<?- (ds/empty-store (assoc store-config :opts opts)))
         _ (log/trace "Store created" {:raw-store (some? raw-store)})
         _ (when-not raw-store
             (dt/raise "Failed to create store." {:type :store-creation-failed
                                                  :backend (:backend store-config)
                                                  :scope (:scope store-config)}))
         store (ds/add-cache-and-handlers raw-store config)
         _ (log/trace "Store ready, adding handlers...")

          ;; 1b. For tiered stores, populate memory from backend BEFORE sync
          ;; This ensures sync handshake sends accurate timestamps for cached keys
          ;; For non-tiered stores, just call ready-store normally
         _ (if is-tiered?
             (<?- (populate-tiered-from-cache! store store-config branch opts))
             (<?- (ds/ready-store (assoc store-config :opts opts) store)))
         _ (log/trace "Store handlers ready" {:tiered? is-tiered?})

          ;; 1c. Register store with fressian handlers for BTSet reconstruction
          ;; This is needed BEFORE subscribing because sync messages contain BTSet addresses
         _ (fh/register-store! store-config store)
         _ (log/trace "Store registered with fressian handlers")

          ;; 1d. Check if we already have the branch key in cache (for tiered stores)
          ;; If so, we can use it immediately instead of waiting for sync
          ;; Replace server config with client config in cached value
         cached-stored-db (when is-tiered?
                            (when-let [cached (<?- (k/get store branch))]
                              (assoc cached :config config)))
         _ (when cached-stored-db
             (log/trace "Found cached branch key" {:branch branch
                                                   :max-tx (:max-tx cached-stored-db)}))

          ;; 2. Set up sync subscription and wait for initial :db
          ;; Use a single subscription that handles both initial sync and ongoing updates
          ;; The conn-atom switches behavior after connection is set up
         sync-complete-ch (promise-chan)
         stored-db-atom (atom cached-stored-db)  ;; Pre-fill with cached value if available
         conn-atom (atom nil)  ;; Set after connection is created
         store-topic (if (keyword? scope-id) scope-id (keyword (str scope-id)))
         _ (log/trace "Subscribing to store topic" {:store-topic store-topic :scope-id scope-id})

         _ (log/trace "Calling subscribe-store!")
         sub-result (<?- (kp/subscribe-store!
                          local-peer store-topic store
                          {:on-key-update
                           (fn [key value _op]
                             (when (= key branch)
                               ;; Replace server config with client config in synced stored-db
                               ;; The server's stored-db contains :backend :file which doesn't exist in browser
                               (let [value-with-client-config (assoc value :config config)]
                                 (if-let [conn @conn-atom]
                                   ;; Connection ready - use ongoing sync handler
                                   (do
                                     (log/trace "Ongoing sync update" {:branch branch :max-tx (:max-tx value)})
                                     (kw/on-db-sync! conn value-with-client-config))
                                   ;; Initial sync - signal completion
                                   (do
                                     (log/trace "Initial branch received via sync" {:branch branch
                                                                                    :max-tx (:max-tx value)})
                                     (reset! stored-db-atom value-with-client-config)
                                     (put! sync-complete-ch :synced))))))}))
         _ (log/trace "subscribe-store! returned" {:subscribed? (some? sub-result)})

          ;; 3. Wait for initial sync (or skip if we have cached data)
          ;; If we already have the branch from cache, no need to wait for sync
          ;; (handshake will verify timestamps and send only newer data if any)
         stored-db (if cached-stored-db
                     (do
                       (log/trace "Using cached branch key, skipping sync wait"
                                  {:max-tx (:max-tx cached-stored-db)})
                       cached-stored-db)
                     (do
                       (log/trace "Waiting for initial sync...")
                       (<?- sync-complete-ch)
                       (log/trace "Initial sync complete" {:max-tx (:max-tx @stored-db-atom)})
                       @stored-db-atom))

          ;; 4. Reconstruct deferred indexes and create connection
         _ (log/trace "Stored-db received" {:key-count (count (keys stored-db))
                                            :has-eavt? (some? (:eavt-key stored-db))})
         storage (:storage store)
         _ (log/trace "Storage ready" {:has-storage? (some? storage)})
         processed (fh/reconstruct-deferred-indexes stored-db storage)
         _ (log/trace "Indexes reconstructed" {:has-eavt? (some? (:eavt-key processed))})

          ;; Handle index from synced db
         [config store stored-db]
         (let [intended-index (:index config)
               stored-index (get-in processed [:config :index])]
           (if (and intended-index stored-index (not= intended-index stored-index))
             (do
               (log/warn (str "Stored index does not match configuration. Using stored: " stored-index))
               (let [config (assoc config :index stored-index)
                     store (ds/add-cache-and-handlers raw-store config)]
                 [config store processed]))
             [config store processed]))

          ;; Create connection - equivalent to conn-from-db but avoiding circular dep
          ;; The stored-db now has client config (replaced in on-key-update callback)
         _ (log/info "Creating connection with config"
                     {:store-backend (get-in config [:store :backend])
                      :writer-backend (get-in config [:writer :backend])})
         live-db (dw/stored->db stored-db store)
         _ (log/info "Connection created, checking db config"
                     {:db-store-backend (get-in live-db [:config :store :backend])})
         conn (->Connection (atom live-db :meta {:listeners (atom {})}))

          ;; 5. Create writer and wire up ongoing sync
          ;; Pass store-config for cleanup on shutdown
         _ (log/trace "Creating KabelWriter" {:peer-id peer-id :scope-id scope-id})
         writer-config (assoc (:writer config) :store-config store-config)
         writer (w/create-writer writer-config conn)
         _ (log/trace "Writer created")
         _ (swap! (:wrapped-atom conn) assoc :writer writer :store store)
         _ (log/trace "Writer attached to connection")

          ;; Set connection reference in writer (for tx-report reconstruction)
         _ (kw/set-connection! writer conn)

          ;; 6. Switch the subscription callback to use on-db-sync! for ongoing updates
          ;; By setting conn-atom, the on-key-update callback now calls on-db-sync!
          ;; instead of signaling sync-complete-ch (which was already delivered)
         _ (reset! conn-atom conn)
         _ (log/trace "Connection atom set, ongoing sync enabled")]

      ;; 7. Register connection
     (add-connection! conn-id conn)

     (log/info "KabelWriter connection complete" {:scope-id scope-id :max-tx (:max-tx stored-db)})
      ;; Return connection - caller must take from channel
     conn)))

;; =============================================================================
;; Multimethod Integration
;; =============================================================================

(defmethod datahike.connector/-connect* :kabel [config opts]
  (when (:sync? opts)
    (dt/raise "Kabel connections must be async. Use {:sync? false} or omit :sync? option."
              {:type :kabel-requires-async
               :config config}))
  (connect-kabel config (assoc opts :sync? false)))
