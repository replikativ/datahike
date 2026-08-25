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

(defn refresh-tiered-frontend
  "Materialize every backend key that is still missing from a tiered frontend.

   Datahike's query and transaction engines deliberately remain synchronous.
   An asynchronous durable backend (S3 in a browser) is therefore usable only
   behind a complete synchronous frontend. `ready-store` establishes that
   invariant at connect; this function restores it after another shared writer
   has moved the branch head and introduced immutable index nodes this process
   has never seen.

   The branch head must be read from the authoritative backend *before* this is
   called. S3's strongly consistent GET/LIST contract then guarantees that the
   following listing contains every object referenced by the observed head.
   Writes publish immutable objects before the mutable head, so after this call
   `stored->db` and the synchronous index engine cannot fall through to S3.

   This intentionally runs only when the observed Datahike commit id changed,
   not before every local transaction. Existing frontend keys are not fetched
   again."
  [store opts]
  (if (and (:frontend-store store) (:backend-store store))
    (kt/perform-sync (:frontend-store store)
                     (:backend-store store)
                     kt/populate-missing-strategy
                     opts)
    (async+sync (:sync? opts) *default-sync-translation*
                (go-try- true))))
