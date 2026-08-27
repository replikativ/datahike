(ns ^:no-doc datahike.connections
  (:require [replikativ.logging :as log]))

;; Entry shape: {:conn <Connection|nil> :count <n> :node-cache <atom> :threshold <n>}
;;
;; A `:conn` of nil is a RESERVATION: a connect that has begun but not finished.
;; `get-connection` ignores it, so it neither satisfies nor blocks a lookup; it
;; exists so a sibling branch connecting concurrently finds the same node cache
;; instead of racing to build a second one.
(def ^:dynamic *connections* (atom {}))

(defn get-connection [conn-id]
  (when-let [conn (get-in @*connections* [conn-id :conn])]
    (swap! *connections* update-in [conn-id :count] inc)
    conn))

;; ---------------------------------------------------------------------------
;; Node cache
;; ---------------------------------------------------------------------------
;;
;; WHY IT LIVES HERE, in the connection registry, rather than in a global keyed
;; by store-id.
;;
;; The cache holds materialized index nodes keyed by storage address. It used to
;; be per-connection, and connections are keyed [store-id branch], so every
;; branch rebuilt the index from storage on its first read -- measured on a
;; 6.44M-datom store: 12,569 node restores, 2,920 ms, 6,566 MB allocated, against
;; 0 reads and ~600 ms once warm. It should be shared by every connection to a
;; store. But a process-global store-id -> cache map got three things wrong, all
;; of which this placement fixes structurally rather than by bookkeeping:
;;
;;   1. LIFETIME. A global needs an explicit drop on the last release, and every
;;      teardown path has to remember to call it. `delete-database` calls
;;      `delete-connection!` directly, bypassing `connector/release` -- so a drop
;;      placed there never ran, and afterwards the conn-id was gone so it could
;;      never run. Measured: 334 materialized nodes of a DELETED database pinned
;;      for the process lifetime. Here the cache is reachable only FROM the
;;      entries, so removing the last entry makes it garbage. No drop to forget.
;;
;;   2. FAILED CONNECTS. `create-storage` runs early in the connect path and the
;;      connection is registered much later; anything failing in between -- a
;;      missing branch, `ready-store`, deserialization, config validation, writer
;;      creation -- left a global entry nothing could ever remove. Here a
;;      reservation that is abandoned takes its cache with it.
;;
;;   3. ISOLATION. `*connections*` is dynamic, and `writer_alternating_test`
;;      rebinds it precisely to simulate independent processes ("what a second
;;      Lambda execution environment is") that must NOT share in-memory state. A
;;      global ignored that boundary in both directions: it shared caches across
;;      the simulated processes, and a release in one binding could drop a cache
;;      the other was still using. A rebinding now yields fresh caches, because
;;      the caches live in the rebound atom.
;;
;; Only the CACHE is shared. `pending-writes`, `freed-set`, `freelist` and
;; `stats` are per-connection write/accounting state and stay on the
;; CachedStorage instance.

(defn- sibling-cache
  "The node cache already chosen by a live connection or reservation to the same
   store at the same threshold, if any.

   Keyed on threshold as well as store, because `normalize-config` deliberately
   excludes `:store-cache-size` from connection equality -- two connections to
   one store may legitimately ask for different sizes. Handing the second the
   first's cache silently capped it, and `datahike.warm` reads the configured
   size to clamp its budget, so the warm then reported a budget it never had."
  [entries store-id threshold]
  (some (fn [[[sid _branch] entry]]
          (when (and (= sid store-id) (= threshold (:threshold entry)))
            (:node-cache entry)))
        entries))

(defn acquire-node-cache!
  "Reserve `conn-id` and return the node cache its storage must use.

   Atomic: the lookup of a sibling's cache and the installation of this
   reservation happen in one `swap!`, so two branches connecting concurrently
   cannot each create a cache for the same store. `make-cache` must be PURE --
   a `swap!` retry may call it and discard the result."
  [conn-id threshold make-cache]
  (let [store-id (first conn-id)
        entries  (swap! *connections*
                        (fn [m]
                          (if (contains? m conn-id)
                            m
                            (assoc m conn-id
                                   {:conn nil :count 0 :threshold threshold
                                    :node-cache (or (sibling-cache m store-id threshold)
                                                    (make-cache))}))))]
    (get-in entries [conn-id :node-cache])))

(defn abandon-reservation!
  "Remove a reservation whose connect never completed. A no-op once the entry
   has become a real connection."
  [conn-id]
  (swap! *connections*
         (fn [m] (if (nil? (get-in m [conn-id :conn])) (dissoc m conn-id) m)))
  nil)

(defn add-connection! [conn-id conn]
  ;; `update`, not `assoc`: fills in the reservation taken by
  ;; `acquire-node-cache!` without discarding the cache it selected.
  (swap! *connections* update conn-id merge {:conn conn :count 1}))

(defn delete-connection! [conn-id]
  (when-let [conn (get-connection conn-id)]
    (reset! conn :released)
    (swap! *connections* dissoc conn-id)))

(defn invalidate-store-connections!
  "Remove every local connection for `store-id` from the registry.

   A connection to a deleted database must not survive to be handed out again:
   its cached head can reference storage nodes that disappeared with the store."
  [store-id]
  (doseq [conn-id (filter (fn [[connection-store-id _branch]]
                            (= connection-store-id store-id))
                          (keys @*connections*))]
    (log/warn :datahike/delete-unreleased-connections {:connection conn-id})
    (delete-connection! conn-id)))
