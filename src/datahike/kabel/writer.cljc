(ns datahike.kabel.writer
  "KabelWriter for remote transactions via kabel/distributed-scope.

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
            [datahike.kabel.fressian-handlers :as fh]
            [datahike.tools :refer [throwable-promise]]
            [is.simm.distributed-scope :as ds]
            [superv.async :refer [<?-]]
            #?(:clj [clojure.core.async :refer [go put! promise-chan close!]]
               :cljs [clojure.core.async :refer [go put! promise-chan close!]])
            #?(:clj [taoensso.timbre :as log]
               :cljs [taoensso.timbre :as log :include-macros true])))

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
      ;; Stored map - reconstruct it now
      (let [storage (:storage store)
            processed (fh/reconstruct-deferred-indexes stored-db storage)]
        (dw/stored->db processed store)))))

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

(defrecord KabelWriter
           [peer-id        ; UUID of the remote peer that owns the database
            store-id       ; UUID identifying the store/database (from store :id)
            store-config   ; Store config for fressian handler registry cleanup
            pending-txs    ; atom: {expected-max-tx -> {:tx-report ... :ch promise-chan}}
            current-max-tx ; atom: current synced max-tx from konserve-sync
            listeners      ; atom: set of listen! callbacks to fire on tx completion
            conn-atom]     ; atom: reference to the connection (set after connect)

  PWriter

  (-dispatch! [_ {:keys [op args] :as arg-map}]
    (let [result-ch (promise-chan)
          ;; Global dispatch handler - store-id is passed in the request
          remote-fn 'datahike.kabel/dispatch]
      (go
        (try
          ;; 1. Send to remote peer via distributed-scope
          (let [remote-result (<?- (ds/invoke-remote peer-id
                                                    remote-fn
                                                    {:store-id store-id
                                                     :arg-map arg-map}))]
            (if (instance? #?(:clj Throwable :cljs js/Error) remote-result)
              ;; Remote error - return immediately
              (put! result-ch remote-result)

              ;; 2. Wait for sync to catch up before returning
              ;; Keep full tx-report from remote, release when synced
              (let [expected-max-tx (get-in remote-result [:db-after :max-tx])
                    wait-ch (promise-chan)]

                ;; Register waiter with full tx-report
                (swap! pending-txs assoc expected-max-tx
                       {:tx-report remote-result :ch wait-ch})

                ;; Helper to finalize and return tx-report
                (let [finalize-and-return!
                      (fn []
                        (swap! pending-txs dissoc expected-max-tx)
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
                          ;; Return reconstructed tx-report
                          (put! result-ch final-tx-report)))]

                  ;; Check if already synced (sync may have arrived before RPC returned)
                  (if (>= @current-max-tx expected-max-tx)
                    (finalize-and-return!)

                    ;; Wait indefinitely for sync (no timeout)
                    ;; Cleanup happens on shutdown or connection close
                    (do
                      (<?- wait-ch)
                      (finalize-and-return!)))))))
          (catch #?(:clj Exception :cljs js/Error) e
            (log/error "Error in KabelWriter dispatch" e)
            (put! result-ch e))))
      result-ch))

  (-streaming? [_]
    ;; KabelWriter streams updates via konserve-sync
    true)

  (-shutdown [_]
    ;; Cancel all pending waiters with shutdown error
    (let [shutdown-error (ex-info "Writer shutdown" {:type :writer-shutdown})]
      (doseq [[max-tx {:keys [ch]}] @pending-txs]
        (put! ch shutdown-error))
      (reset! pending-txs {})
      ;; Unregister store from fressian handlers
      (when store-config
        (fh/unregister-store! store-config))
      ;; Return closed channel to signal completion
      (let [ch (promise-chan)]
        (put! ch true)
        ch))))

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
    (doseq [[expected-max-tx {:keys [ch]}] @pending-txs]
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
          storage (:storage conn-store)
          ;; Reconstruct deferred indexes before stored->db
          processed (fh/reconstruct-deferred-indexes stored-db storage)
          ;; Convert stored format to live DB
          live-db (dw/stored->db processed conn-store)
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
   - store-config: Store config for fressian handler registry cleanup on shutdown

   Returns a KabelWriter instance."
  [peer-id store-id store-config]
  (->KabelWriter peer-id
                 store-id
                 store-config
                 (atom {})   ; pending-txs
                 (atom 0)    ; current-max-tx
                 (atom #{})  ; listeners
                 (atom nil))) ; conn-atom - set via set-connection! after d/connect

;; =============================================================================
;; Multimethod Extensions
;; =============================================================================

(defmethod writer/create-writer :kabel
  [{:keys [peer-id store-config]} _connection]
  ;; Extract store-id from store config :id
  (let [store-id (:id store-config)]
    (kabel-writer peer-id store-id store-config)))

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
        (let [result (<?- (ds/invoke-remote peer-id
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
        (let [result (<?- (ds/invoke-remote peer-id
                                           'datahike.kabel/delete-database
                                           {:config remote-config}))]
          (#?(:clj deliver :cljs put!) p result))
        (catch #?(:clj Exception :cljs js/Error) e
          (log/error "Error in delete-database :kabel" e)
          (#?(:clj deliver :cljs put!) p e))))
    p))
