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
;; Store Identity
;; =============================================================================

(defn store-identity
  "Returns the UUID that identifies the store.

   All konserve stores require an :id field containing a UUID.
   This is the stable identifier used for connection tracking,
   distributed coordination, and store matching."
  [config]
  (:id config))

(defonce ^:private claimed-store-identities
  ;; {store-id -> {backend -> merged credential-stripped store config}}
  ;;
  ;; KEYED BY BACKEND, because that is the shape of the question. An `:id` is
  ;; konserve's LOGICAL identity, deliberately the same across the backends
  ;; holding one store: a `:tiered` store shares its id with its own
  ;; `:frontend-config` and `:backend-config`, and opening it through its durable
  ;; layer presents a second config for the same store, differing exactly in
  ;; `:backend`. So a differing backend is never a collision, and what remains
  ;; unambiguous is two configs naming the SAME backend in different places.
  ;;
  ;; Keying by backend says that once, and bounds the registry by the shape of
  ;; the composition — a handful of layers — rather than by a chosen number.
  ;;
  ;; Configs under one backend are MERGED rather than kept separately: a caller
  ;; may hold a fuller or sparser config than the one that claimed first, and if
  ;; they do not disagree, their union describes that store more completely than
  ;; either. Merging therefore makes the guard stronger as it sees more, where
  ;; first-claim-wins would let `{:backend :file :id X}` mask a later
  ;; `{:backend :file :path elsewhere :id X}`.
  (atom {}))

(defn- claimable
  "The part of a store config that says WHERE the bytes are.

   Credentials are stripped at every depth (`konserve.protocols/strip-credentials`
   walks nested `:tiered` configs too), because the same store may legitimately be
   opened with rotated credentials and because this map ends up in an `ex-info`."
  [store-config]
  (kp/strip-credentials store-config))

(defn- conflicting-keys
  "The keys two store configs DISAGREE on — present in both with different values.

   Not plain inequality: a store config legitimately carries keys that say
   nothing about which store it is (datahike's own tests connect one store with
   an extra property to prove such keys are ignored), and a caller may hold a
   fuller or sparser config than the one that claimed the id first. Absence is
   not disagreement; a differing `:path` or `:bucket` is.

   Only ever asked of two configs sharing a `:backend` — see
   `claimed-store-identities` for why that is the only comparison worth making."
  [a b]
  (into (sorted-set)
        (filter (fn [k] (and (contains? a k)
                             (contains? b k)
                             (not= (get a k) (get b k)))))
        (into (set (keys a)) (keys b))))

(defn claim-store-identity!
  "Bind `:id` to this store config for the lifetime of the process, or raise if
   that id is already bound to a DIFFERENT one.

   A store `:id` is not a label, it is a process-wide key: the connection
   registry (`[store-id branch]`), the GC guard's safe point, and the
   persistent-sorted-set storage registry are all keyed by it. Two stores
   sharing one id therefore resolve to each other — `d/connect` on the second
   config returns the FIRST connection, so a caller silently reads and writes
   the wrong database.

   Neither existing consistency check can catch that, and for the same reason:
   both reduce a store to its id. `connector`'s `:store-identity-mismatch`
   compares the id a store RECORDS against the id you connect with, and two
   stores that each self-consistently hold the same id both pass it. The
   cache-hit comparison runs configs through `normalize-config`, which drops
   `:store` entirely on the assumption that the id in the connection key already
   accounts for it. So nothing looks at where the bytes are, which is the only
   thing that distinguishes these two stores.

   Held until `delete-database` releases it: reusing an id for a different store
   after deleting the first is legitimate, reusing it for a second LIVE store is
   the bug. A config with no `:id`, or a backend that attaches none, claims
   nothing."
  [store-config]
  (when-let [store-id (store-identity store-config)]
    (let [wanted  (claimable store-config)
          backend (:backend wanted)
          held    (get-in @claimed-store-identities [store-id backend])
          clashes (when held (conflicting-keys held wanted))]
      (if (seq clashes)
        (throw (ex-info (str "Store id " store-id " is claimed by two store configurations that "
                             "disagree on " (pr-str clashes) ". "
                             "A store :id must identify exactly one store: it keys the connection "
                             "registry, the GC guard and the index storage registry, so a second "
                             "store sharing it resolves to the first and reads the wrong database. "
                             "Give each store its own :id — or, if these are the same store described "
                             "two ways, make the two configs identical.")
                        {:type :datahike/store-identity-collision
                         :store-id store-id
                         :claimed-by held
                         :requested wanted}))
        (swap! claimed-store-identities assoc-in [store-id backend]
               (merge held wanted)))))
  store-config)

(defn release-store-identity!
  "Drop the claim on `store-id`, so the id may be bound to a different store.

   Called by `delete-database`: once the store is gone, reusing its id elsewhere
   is no longer a collision. Releasing a connection does NOT release the claim —
   the store still exists, and an id that named it must not quietly come to name
   something else."
  [store-id]
  (swap! claimed-store-identities dissoc store-id)
  nil)

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
  (claim-store-identity! (:store config))
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
