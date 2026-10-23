(ns ^:no-doc datahike.store
  "Datahike-specific store utilities.

   Most store lifecycle operations (create, connect, delete, release) are now
   handled by konserve.store directly. This namespace provides:

   - add-cache-and-handlers: Wraps konserve stores with LRU cache and BTSet handlers
   - store-identity: Returns store UUID from config
   - ready-store: Tiered-specific initialization (populate cache from backend)"
  (:require [konserve.tiered :as kt]
            [konserve.protocols :as kp]
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
   in memory. The handlers enable persistent-sorted-set serialization.

   Also attaches the schema-meta durability proof cell. It belongs to the STORE
   rather than to a store id: sibling connections (branches) each build their own
   store over their own konserve connection, so a per-store cell is proven and
   dropped with the thing it makes a claim about, and two stores sharing an `:id`
   can never borrow each other's proof. See `datahike.schema-cache`."
  [raw-store config]
  (assoc
   (di/add-konserve-handlers
    config
    (kc/ensure-cache
     raw-store
     (atom (cache/lru-cache-factory {} :threshold (:store-cache-size config)))))
   ;; The key and the cell's shape (`{:key … :at …}`, or nil) are
   ;; `datahike.schema-cache`'s; spelled out here rather than required, because
   ;; that namespace reads `datahike.config`, which reads this one. One literal
   ;; keyword is a cheaper coupling than breaking that cycle open.
   :datahike/schema-meta-durable (atom nil)))

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

(defn canonical-store-id
  "The live Konserve store's identity for write/GC coordination.

   Prefer the backend's attached/overridden `PStoreConfig` identity: a backend
   may collapse two configs that open the same physical bytes, which is required
   for a sound shared GC fence. `store-config` is only the compatibility fallback
   for stores constructed directly rather than through `konserve.store`."
  [store store-config]
  (or (kp/store-id store)
      (:id store-config)
      (throw (ex-info "A store used for coordinated writes has no identity."
                      {:type :datahike/store-id-missing}))))

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

(defmethod ready-store :tiered [{:keys [opts frontend-config backend-config populate-on-connect?]
                                 :or {populate-on-connect? true}}
                                store]
  "Populate tiered store frontend from backend and sync on connect.

   This ensures:
   1. Memory frontend has cached data for immediate queries
   2. Subsequent sync handshakes send accurate timestamps (only fetch newer keys)

   `:populate-on-connect? false` (JVM only) skips the full copy. The frontend
   then fills by READ-THROUGH: under `:read-policy :frontend-first` (the
   default) a key missing from the frontend is read from the backend and
   cached. That is sound wherever the backend can be read synchronously, which
   on the JVM every konserve backend can, so the frontend can be sized as a
   CACHE rather than as a mirror of the whole store. It is refused on
   ClojureScript, where the synchronous engine must never fall through to an
   asynchronous backend, and with `:read-policy :frontend-only`, under which a
   key missing from the frontend reads as absent."
  (async+sync (:sync? (or opts {:sync? true})) *default-sync-translation*
              (go-try-
               (when-not populate-on-connect?
                 #?(:cljs (throw (ex-info (str "A :tiered store must populate its frontend on connect on "
                                               "ClojureScript: the synchronous engine cannot read through to "
                                               "an asynchronous backend. Remove :populate-on-connect? false.")
                                          {:type :store/tiered-populate-required}))
                    :clj (when (= :frontend-only (:read-policy store))
                           (throw (ex-info (str ":populate-on-connect? false needs :read-policy :frontend-first: "
                                                "under :frontend-only a key the frontend does not hold reads "
                                                "as absent.")
                                           {:type :store/tiered-populate-required
                                            :read-policy (:read-policy store)})))))
               ;; Config uses :frontend-config/:backend-config (avoids collision with :backend :tiered)
               ;; Store record uses :frontend-store/:backend-store (field names in TieredStore)
               (<?- (ready-store (assoc frontend-config :opts opts) (:frontend-store store)))
               (<?- (ready-store (assoc backend-config :opts opts) (:backend-store store)))
               (when populate-on-connect?
                 (<?- (kt/sync-on-connect store kt/populate-missing-strategy opts)))
               true)))

(defn refresh-tiered-frontend
  "Materialize every backend key that is still missing from a tiered frontend.

   Datahike's query and transaction engines deliberately remain synchronous.
   An asynchronous durable backend (S3 in a browser) is therefore usable only
   behind a complete synchronous frontend. `ready-store` establishes that
   invariant at connect. Moved persistent-set heads now hydrate their structural
   delta directly; this full-store synchronization remains the compatibility
   fallback for legacy index implementations that cannot expose that delta.

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
