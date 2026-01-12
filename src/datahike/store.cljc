(ns ^:no-doc datahike.store
  "Datahike-specific store utilities.

   Most store lifecycle operations (create, connect, delete, release) are now
   handled by konserve.store directly. This namespace provides:

   - add-cache-and-handlers: Wraps konserve stores with LRU cache and BTSet handlers
   - store-identity: Returns store UUID from config
   - ready-store: Tiered-specific initialization (populate cache from backend)"
  (:require [konserve.tiered :as kt]
            [datahike.index :as di]
            [konserve.cache :as kc]
            #?(:clj [clojure.core.cache :as cache]
               :cljs [cljs.cache :as cache])
            [konserve.utils :refer [#?(:clj async+sync) *default-sync-translation*]
             #?@(:cljs [:refer-macros [async+sync]])]
            [superv.async #?(:clj :refer :cljs :refer-macros) [go-try- <?-]]
            #?(:cljs [clojure.core.async :refer-macros [go]])))

;; =============================================================================
;; Cache and Handlers
;; =============================================================================

(defn add-cache-and-handlers
  "Wrap a raw konserve store with LRU cache and Datahike BTSet handlers.

   The cache improves read performance by keeping frequently accessed keys
   in memory. The handlers enable persistent-sorted-set serialization."
  [raw-store config]
  (di/add-konserve-handlers
   config
   (kc/ensure-cache
    raw-store
    (atom (cache/lru-cache-factory {} :threshold (:store-cache-size config))))))

;; =============================================================================
;; Store Identity
;; =============================================================================

(defn store-identity
  "Returns the UUID that identifies the store.

   All konserve stores require an :id field containing a UUID.
   This is the stable identifier used for connection tracking,
   distributed coordination, and store matching."
  [config]
  (:id config))

;; =============================================================================
;; Ready Store (Tiered-Specific)
;; =============================================================================

(defmulti ready-store
  "Notify when the store is ready to use.

   Most backends are ready immediately after connection. The :tiered backend
   needs special handling to populate the memory frontend from the backend
   before use."
  {:arglists '([config store])}
  (fn [{:keys [backend]} _store]
    backend))

(defmethod ready-store :default [{:keys [opts]} _]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try- true)))

(defmethod ready-store :tiered [{:keys [opts frontend-config backend-config]} store]
  "Populate tiered store frontend from backend and sync on connect.

   This ensures:
   1. Memory frontend has cached data for immediate queries
   2. Subsequent sync handshakes send accurate timestamps (only fetch newer keys)"
  (async+sync (:sync? (or opts {:sync? true})) *default-sync-translation*
              (go-try-
               ;; Config uses :frontend-config/:backend-config (avoids collision with :backend :tiered)
               ;; Store record uses :frontend-store/:backend-store (field names in TieredStore)
               (<?- (ready-store (assoc frontend-config :opts opts) (:frontend-store store)))
               (<?- (ready-store (assoc backend-config :opts opts) (:backend-store store)))
               (<?- (kt/sync-on-connect store kt/populate-missing-strategy opts))
               true)))
