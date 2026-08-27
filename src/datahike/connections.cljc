(ns ^:no-doc datahike.connections
  (:require [replikativ.logging :as log]))

;; Entry shape: {:conn <Connection|nil> :count <n> :leases #{<uuid>}
;;               :node-cache <atom> :threshold <n>}
;;
;; A `:conn` of nil is a RESERVATION: a connect that has begun but not finished.
;; `get-connection` ignores it, so it neither satisfies nor blocks a lookup; it
;; exists so a sibling branch connecting concurrently finds the same node cache
;; instead of racing to build a second one.
(def ^:dynamic *connections* (atom {}))

(defn get-connection [conn-id]
  ;; Lookup and ref-count increment must be one registry operation. Otherwise a
  ;; concurrent invalidation can remove the entry between them and `update-in`
  ;; recreates a connection-less entry.
  (get-in (swap! *connections*
                 (fn [m]
                   (if (get-in m [conn-id :conn])
                     (update-in m [conn-id :count] inc)
                     m)))
          [conn-id :conn]))

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
  "Reserve `conn-id` and return `[lease node-cache]` for its storage.

   Atomic: the lookup of a sibling's cache and the installation of this
   reservation happen in one `swap!`, so two branches connecting concurrently
   cannot each create a cache for the same store. `make-cache` must be PURE --
   a `swap!` retry may call it and discard the result."
  [conn-id threshold make-cache]
  (let [lease    (random-uuid)
        store-id (first conn-id)
        entries  (swap! *connections*
                        (fn [m]
                          (if (contains? m conn-id)
                            (update-in m [conn-id :leases] (fnil conj #{}) lease)
                            (assoc m conn-id
                                   {:conn nil :count 0 :leases #{lease}
                                    :threshold threshold
                                    :node-cache (or (sibling-cache m store-id threshold)
                                                    (make-cache))}))))]
    [lease (get-in entries [conn-id :node-cache])]))

(defn reserve-connection!
  "Return a lease that prevents an invalidated in-flight connect from later
   publishing itself back into the registry. Used by connectors that own their
   storage cache setup."
  [conn-id]
  (let [lease (random-uuid)]
    (swap! *connections*
           (fn [m]
             (if (contains? m conn-id)
               (update-in m [conn-id :leases] (fnil conj #{}) lease)
               (assoc m conn-id {:conn nil :count 0 :leases #{lease}}))))
    lease))

(defn abandon-reservation!
  "Remove a reservation whose connect never completed. A no-op once the entry
   has become a real connection."
  [conn-id lease]
  (swap! *connections*
         (fn [m]
           (let [m (update-in m [conn-id :leases] disj lease)]
             (if (and (nil? (get-in m [conn-id :conn]))
                      (empty? (get-in m [conn-id :leases])))
               (dissoc m conn-id)
               m))))
  nil)

(defn add-connection!
  "Publish `conn` only if its exact reservation survived invalidation."
  [conn-id lease conn]
  (let [entries (swap! *connections*
                       (fn [m]
                         (if (contains? (get-in m [conn-id :leases]) lease)
                           (-> m
                               (update conn-id merge {:conn conn :count 1})
                               (update-in [conn-id :leases] disj lease))
                           m)))]
    (identical? conn (get-in entries [conn-id :conn]))))

(defn delete-connection! [conn-id]
  (let [[before _] (swap-vals! *connections* dissoc conn-id)]
    (when-let [conn (get-in before [conn-id :conn])]
      (reset! conn :released))))

(defn invalidate-store-connections!
  "Remove every local connection for `store-id` from the registry.

   A connection to a deleted database must not survive to be handed out again:
   its cached head can reference storage nodes that disappeared with the store.

   EVERY writer backend whose `delete-database` deletes REMOTELY must call this
   on success, because nothing else in this process will. `:self` gets it via
   `writing/-delete-database*`; `:datahike-server` and `:kabel` call it from
   their own methods. A backend that forgets leaves a stale connection that a
   later `d/connect` will hand back -- which stayed latent until the optimistic
   overlay work started dereferencing the old root on a moved head."
  [store-id]
  (let [mine? (fn [[connection-store-id _branch]] (= connection-store-id store-id))
        [before _]
        (swap-vals! *connections*
                    (fn [m]
                      (reduce (fn [acc k] (if (mine? k) (dissoc acc k) acc))
                              m (keys m))))]
    ;; Reset only connections removed by the same atomic operation. A later
    ;; connection can neither be reset here nor publish with one of these leases.
    (doseq [[conn-id {:keys [conn]}] (filter (comp mine? key) before)]
      (log/warn :datahike/delete-unreleased-connections {:connection conn-id})
      (when conn (reset! conn :released)))))
