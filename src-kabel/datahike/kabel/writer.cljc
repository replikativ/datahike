(ns ^:no-doc datahike.kabel.writer
  "KabelWriter for remote transactions via kabel.remote.

   The KabelWriter sends transactions to a remote peer that owns the database,
   waits for the transaction to complete, and coordinates with konserve-sync
   to ensure the local store is synchronized before returning.

   Usage:
   ```clojure
   ;; In connection config
   {:store {:backend :file :path \"...\" :id store-id}
    :writer {:backend :kabel
             :peer-id server-peer-id}}
   ```"
  (:require [datahike.writer :as writer :refer [PWriter]]
            [datahike.writing :as dw]
            [datahike.connections :refer [invalidate-store-connections!]]
            [datahike.store :as dstore]
            [datahike.cbor :as dcbor]
            [datahike.tools :refer [throwable-promise]]
            [kabel.pubsub :as pubsub]
            [kabel.remote :as remote]
            [konserve-sync.transport.kabel-pubsub :as kp]
            [superv.async :refer [<?-]]
            #?(:clj [clojure.core.async :refer [go put! promise-chan timeout]]
               :cljs [clojure.core.async :refer [go put! promise-chan timeout]])
            #?(:clj [replikativ.logging :as log]
               :cljs [replikativ.logging :as log :include-macros true])))

;; =============================================================================
;; TX-Report Reconstruction (must be before defrecord)
;; =============================================================================

(defn- reconstruct-stored-db
  "Reconstruct a stored db map into a live DB.
   Returns nil if input is nil.

   If input is already a DB object (reconstructed by Fressian read handler),
   returns it as-is. Otherwise reconstructs from stored format.

   Parameters:
   - stored-db: The stored db map (may have deferred indexes) OR a live DB
   - store: The prepared konserve store"
  [stored-db store]
  (when stored-db
    (if (instance? datahike.db.DB stored-db)
      ;; Already a live DB - Fressian read handler found the store and reconstructed it
      stored-db
      ;; Stored map - reconstruct it now (roots already EAGER from the canonical read handler)
      (dw/stored->db stored-db store))))

(defn- reconstruct-tx-report
  "Reconstruct a tx-report from stored format to live DBs.
   Uses the connection's store for index reconstruction.

   Parameters:
   - tx-report: The raw tx-report with stored db-before/db-after
   - store: The prepared konserve store

   Returns a tx-report with live DB instances."
  [tx-report store]
  (log/trace "reconstruct-tx-report" {:has-db-before? (some? (:db-before tx-report))
                                      :has-db-after? (some? (:db-after tx-report))})
  (-> tx-report
      (update :db-before reconstruct-stored-db store)
      (update :db-after reconstruct-stored-db store)))

;; =============================================================================
;; KabelWriter Implementation
;; =============================================================================

(defn await-topic-release!
  "Yield true once the subscription `peer` holds on `topic` right now is gone.
   Kabel's unsubscribe is idempotent (a cancellation already in flight is
   joined, not repeated), so this only has to be pinned to the subscription's
   generation: a newer subscription made meanwhile on the same topic is left
   alone. Yields an exception after `timeout-ms` or on a refused unsubscribe."
  [peer topic timeout-ms]
  (go
    (let [generation (:generation (pubsub/subscription peer topic))]
      (if (nil? generation)
        true
        (let [[v _] (clojure.core.async/alts! [(kp/unsubscribe-store! peer topic)
                                               (timeout timeout-ms)])]
          (cond
            (not= generation (:generation (pubsub/subscription peer topic))) true
            (and (map? v) (:error v)) (let [e (:error v)]
                                        (if (instance? #?(:clj Throwable :cljs js/Error) e)
                                          e
                                          (ex-info "Store unsubscribe refused"
                                                   {:type :datahike.kabel/unsubscribe-failed :topic topic :error e})))
            :else (ex-info "Store subscription did not release in time"
                           {:type :datahike.kabel/unsubscribe-timeout :topic topic})))))))

(def default-sync-timeout-ms
  "How long a write waits for its own effect to replicate back before it
   fails; a lost sync must not park a caller forever."
  120000)

(defrecord KabelWriter
           [peer-id        ; UUID of the remote peer that owns the database
            local-peer     ; the local kabel peer connected to it, or nil
            store-id       ; UUID identifying the store/database (from store :id)
            branch         ; branch whose head this writer advances
            store-config   ; Store config for index-registry cleanup on shutdown
            pending-txs    ; atom: {request-id -> {:expected-max-tx ... :tx-report ... :ch promise-chan}}
            current-max-tx ; atom: current synced max-tx from konserve-sync
            listeners      ; atom: set of listen! callbacks to fire on tx completion
            conn-atom]     ; atom: reference to the connection (set after connect)

  PWriter

  (-dispatch! [_ {:keys [op] :as arg-map}]
    (let [result-ch (promise-chan)
          request-id (random-uuid)
          ;; Global dispatch handler - store-id is passed in the request
          remote-fn 'datahike.kabel/dispatch
          finish-barrier! (fn [remote-result]
                            (swap! pending-txs dissoc request-id)
                            (let [conn @conn-atom
                                  store (:store @(:wrapped-atom conn))]
                              (put! result-ch
                                    (reconstruct-stored-db remote-result store))))
          finalize! (fn [remote-result]
                      (swap! pending-txs dissoc request-id)
                      ;; Reconstruct tx-report with live DBs from connection's store
                      (let [conn @conn-atom
                            store (:store @(:wrapped-atom conn))
                            final-tx-report (reconstruct-tx-report remote-result store)]
                        ;; Fire listeners for this transaction
                        (doseq [callback @listeners]
                          (try
                            (callback final-tx-report)
                            (catch #?(:clj Exception :cljs js/Error) e
                              (log/error "Error in listen! callback" e))))
                        (put! result-ch final-tx-report)))]
      (go
        (try
          ;; 1. Send to remote peer
          ;; kabel resolves a nil local peer through its route registry
          (let [remote-result (<?- (remote/invoke local-peer peer-id
                                                  remote-fn
                                                  {:store-id store-id
                                                   :branch branch
                                                   :request-id request-id
                                                   :arg-map arg-map}))
                expected-max-tx (if (= op 'writer-barrier)
                                  (:max-tx remote-result)
                                  (get-in remote-result [:db-after :max-tx]))]
            (cond
              ;; Remote error - return immediately
              (instance? #?(:clj Throwable :cljs js/Error) remote-result)
              (put! result-ch remote-result)

              (= op 'writer-barrier)
              (let [wait-ch (promise-chan)]
                (swap! pending-txs assoc request-id
                       {:expected-max-tx expected-max-tx :ch wait-ch})
                (if (>= @current-max-tx expected-max-tx)
                  (finish-barrier! remote-result)
                  (let [[v port] (clojure.core.async/alts!
                                  [wait-ch (timeout default-sync-timeout-ms)])]
                    (cond
                      (and (= port wait-ch)
                           (instance? #?(:clj Throwable :cljs js/Error) v))
                      (do (swap! pending-txs dissoc request-id)
                          (put! result-ch v))

                      (= port wait-ch)
                      (finish-barrier! remote-result)

                      :else
                      (do (swap! pending-txs dissoc request-id)
                          (put! result-ch
                                (ex-info "Writer barrier settled remotely but its sync did not arrive in time"
                                         {:type :kabel/sync-timeout
                                          :request-id request-id
                                          :expected-max-tx expected-max-tx
                                          :timeout-ms default-sync-timeout-ms})))))))

              ;; Not a transaction report: gc-storage! returns a set, the
              ;; secondary-index ops a status map. Nothing to wait for.
              (not (number? expected-max-tx))
              (put! result-ch remote-result)

              ;; 2. Wait for sync to catch up before returning; keep the full
              ;; tx-report from the remote and release it once synced.
              :else
              (let [wait-ch (promise-chan)]
                (swap! pending-txs assoc request-id
                       {:expected-max-tx expected-max-tx
                        :tx-report remote-result
                        :ch wait-ch})
                (if (>= @current-max-tx expected-max-tx)
                  ;; Already synced: the sync may have arrived before the RPC returned
                  (finalize! remote-result)
                  ;; Bounded: the server applied the write, and a caller parked
                  ;; forever could never learn that.
                  (let [[v port] (clojure.core.async/alts! [wait-ch (timeout default-sync-timeout-ms)])]
                    (cond
                      ;; shutdown delivers its error on the wait channel
                      (and (= port wait-ch) (instance? #?(:clj Throwable :cljs js/Error) v))
                      (do (swap! pending-txs dissoc request-id)
                          (put! result-ch v))

                      (= port wait-ch)
                      (finalize! remote-result)

                      :else
                      (do (swap! pending-txs dissoc request-id)
                          (put! result-ch
                                (ex-info "Transaction applied on the server but its sync did not arrive in time"
                                         {:type :kabel/sync-timeout
                                          :request-id request-id
                                          :expected-max-tx expected-max-tx
                                          :timeout-ms default-sync-timeout-ms})))))))))
          (catch #?(:clj Throwable :cljs :default) e
            (log/error "Error in KabelWriter dispatch" e)
            (put! result-ch (if (instance? #?(:clj Throwable :cljs js/Error) e)
                              e
                              (ex-info "KabelWriter dispatch failed" {:error e}))))))
      result-ch))

  (-streaming? [_]
    ;; KabelWriter streams updates via konserve-sync
    true)

  (-shutdown [_]
    ;; Cancel all pending waiters with shutdown error
    (let [shutdown-error (ex-info "Writer shutdown" {:type :writer-shutdown})]
      (doseq [[_ {:keys [ch]}] @pending-txs]
        (put! ch shutdown-error))
      (reset! pending-txs {})
      ;; Drop the store from the index-reconstruction registry
      (when store-config
        (dcbor/unregister-store! store-config))
      ;; Release the store subscription this connection held on the peer. A
      ;; later connect on the same peer and store would otherwise be refused
      ;; as a duplicate subscription.
      (if local-peer
        (go
          (try
            (<?- (await-topic-release! local-peer store-id 30000))
            (catch #?(:clj Exception :cljs js/Error) e
              (log/warn "Store unsubscribe on writer shutdown failed" {:store-id store-id :error (ex-message e)})))
          true)
        (let [ch (promise-chan)]
          (put! ch true)
          ch)))))

;; =============================================================================
;; Connection Reference
;; =============================================================================

(defn set-connection!
  "Set the connection reference in the KabelWriter.
   Must be called after d/connect to enable tx-report reconstruction.

   Parameters:
   - writer: The KabelWriter instance
   - conn: The datahike connection"
  [writer conn]
  (reset! (:conn-atom writer) conn))

;; =============================================================================
;; Sync Update Handler
;; =============================================================================

(defn on-sync-update!
  "Called by konserve-sync when the :db key is updated.
   Resolves any pending transactions that are now synced.

   Parameters:
   - writer: The KabelWriter instance
   - new-max-tx: The max-tx from the newly synced db"
  [writer new-max-tx]
  (let [{:keys [pending-txs current-max-tx]} writer]
    ;; Update current max-tx
    (reset! current-max-tx new-max-tx)

    ;; Resolve any pending transactions that are now synced
    (doseq [[_ {:keys [expected-max-tx ch]}] @pending-txs]
      (when (>= new-max-tx expected-max-tx)
        (put! ch :synced)))))

(defn on-db-sync!
  "Called by konserve-sync when the :db key is updated.
   Updates the connection's database and resolves pending transactions.

   This is the main sync handler that should be used in the :on-key-update
   callback. It:
   1. Reconstructs deferred indexes from kabel Fressian format
   2. Converts the stored db to a live DB
   3. Updates the connection's wrapped-atom (preserving writer, config, etc.)
   4. Notifies pending transactions that are now synced

   Parameters:
   - conn: The datahike connection (must be created via d/connect first)
   - stored-db: The stored db value from konserve-sync (may be nil for deletes)"
  [conn stored-db]
  (when stored-db
    (let [wrapped-atom (:wrapped-atom conn)
          current-state @wrapped-atom
          writer (:writer current-state)
          ;; Get the prepared store and storage from the connection
          conn-store (:store current-state)
          ;; Convert stored format to live DB (roots already EAGER from the canonical read handler)
          live-db (dw/stored->db stored-db conn-store)
          ;; Do not propagate the old query-cache bucket. A synchronized
          ;; snapshot may combine our pending reports with foreign writes, so
          ;; their tx-data is not a complete old->new change set. Selective
          ;; propagation with an incomplete set can retain stale results; the
          ;; new DB key safely starts cold instead.
          ;; Merge new db with connection state (preserve writer, store, etc.)
          new-state (assoc live-db
                           :store conn-store
                           :writer writer)]
      ;; Update connection
      (reset! wrapped-atom new-state)
      (log/trace "Updated connection db via sync" {:max-tx (:max-tx live-db)})

      ;; Notify writer of sync (resolves pending transactions)
      (when writer
        (on-sync-update! writer (:max-tx live-db))))))

;; =============================================================================
;; Listener Management
;; =============================================================================

(defn add-listener!
  "Add a listen! callback to the writer.
   Callbacks are fired when transactions complete (after sync)."
  [writer callback]
  (swap! (:listeners writer) conj callback))

(defn remove-listener!
  "Remove a listen! callback from the writer."
  [writer callback]
  (swap! (:listeners writer) disj callback))

;; =============================================================================
;; Constructor
;; =============================================================================

(defn kabel-writer
  "Create a new KabelWriter.

   Parameters:
   - peer-id: UUID of the remote peer that owns the database
   - store-id: UUID identifying the store/database (extracted from store :id)
   - branch: branch whose head the remote writer must advance (default :db)
   - store-config: Store config for index-registry cleanup on shutdown

   Returns a KabelWriter instance."
  ([peer-id store-id store-config]
   (kabel-writer peer-id store-id :db store-config))
  ([peer-id store-id branch store-config]
   (kabel-writer peer-id nil store-id branch store-config))
  ([peer-id local-peer store-id branch store-config]
   (->KabelWriter peer-id
                  local-peer
                  store-id
                  branch
                  store-config
                  (atom {})   ; pending-txs
                  (atom 0)    ; current-max-tx
                  (atom #{})  ; listeners
                  (atom nil)))) ; conn-atom - set via set-connection! after d/connect

;; =============================================================================
;; Multimethod Extensions
;; =============================================================================

(defmethod writer/create-writer :kabel
  [{:keys [peer-id local-peer store-config branch]} connection]
  ;; Extract store-id from store config :id
  (let [store-id (:id store-config)
        branch (or branch
                   (some-> connection :wrapped-atom deref :config :branch)
                   :db)]
    (kabel-writer peer-id local-peer store-id branch store-config)))

(defmethod writer/create-database :kabel
  [config & _args]
  (let [{:keys [peer-id]} (:writer config)
        ;; Strip :local-peer from config - it's an atom that can't be serialized
        ;; and is only needed locally for connecting, not on the server
        remote-config (update config :writer dissoc :local-peer)
        p (throwable-promise)]
    (go
      (try
        ;; Global create-database handler - config contains store-id
        (let [result (<?- (remote/invoke (get-in config [:writer :local-peer]) peer-id
                                         'datahike.kabel/create-database
                                         {:config remote-config}))]
          (#?(:clj deliver :cljs put!) p result))
        (catch #?(:clj Exception :cljs js/Error) e
          (log/error "Error in create-database :kabel" e)
          (#?(:clj deliver :cljs put!) p e))))
    p))

(defmethod writer/delete-database :kabel
  [config & _args]
  (let [{:keys [peer-id]} (:writer config)
        ;; Strip :local-peer from config - it's an atom that can't be serialized
        remote-config (update config :writer dissoc :local-peer)
        p (throwable-promise)]
    (go
      (try
        ;; Global delete-database handler - config contains store-id
        (let [result (<?- (remote/invoke (get-in config [:writer :local-peer]) peer-id
                                         'datahike.kabel/delete-database
                                         {:config remote-config}))]
          ;; Same reason as the :datahike-server backend: the delete happened on
          ;; the remote peer, so nothing here has touched THIS process's
          ;; connection registry. Left alone, a later `d/connect` with the same
          ;; config hands back a connection whose head belongs to the deleted
          ;; database. Only on success -- a failed delete must leave the
          ;; connection usable.
          ;;
          ;; The server side needs no equivalent: its handler already releases
          ;; every registered branch connection before deleting
          ;; (kabel/handlers.cljc), and the delete itself goes through
          ;; `-delete-database*`, which invalidates.
          (invalidate-store-connections! (dstore/store-identity (:store config)))
          (#?(:clj deliver :cljs put!) p result))
        (catch #?(:clj Exception :cljs js/Error) e
          (log/error "Error in delete-database :kabel" e)
          (#?(:clj deliver :cljs put!) p e))))
    p))
