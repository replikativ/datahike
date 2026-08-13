(ns ^:no-doc datahike.kabel.tx-broadcast
  "Tx-report broadcasting via kabel.pubsub.

   This namespace provides functions for:
   - Server: registering tx-report topics and publishing tx-reports
   - Client: subscribing to tx-reports for remote databases

   Architecture:
   - Each database has a topic: :tx-report/<store-id>
   - Uses PubSubOnlyStrategy (no handshake, just receive publishes)
   - Deduplication via request-id (skip own transactions)"
  (:require [datahike.db.utils :as dbu]
            [datahike.writing :as dw]
            [kabel.pubsub :as pubsub]
            [kabel.pubsub.protocol :as proto]
            [kabel.peer :as peer]
            #?(:clj  [replikativ.logging :refer [debug info warn]]
               :cljs [replikativ.logging :refer [debug info warn] :include-macros true])
            #?(:clj [clojure.core.async :refer [go put! chan close!]]
               :cljs [clojure.core.async :refer [go put! chan close!] :include-macros true])))

;; =============================================================================
;; Topic Naming
;; =============================================================================

(defn tx-report-topic
  "Returns the topic keyword for tx-reports of a database.

   Example: (tx-report-topic \"a1b2c3d4-...\") => :tx-report/store-a1b2c3d4-...

   Note: The 'store-' prefix (kept as 'scope-' for backwards compatibility)
   ensures EDN compatibility, as keywords cannot start with a digit."
  [store-id]
  (keyword "tx-report" (str "scope-" store-id)))

;; =============================================================================
;; Server-Side API
;; =============================================================================

(defn register-tx-report-topic!
  "Register a tx-report topic for a database. Call on server startup.

   Parameters:
   - peer: The kabel peer atom
   - store-id: UUID identifying the database store

   Returns: The topic keyword"
  [peer store-id]
  (let [topic (tx-report-topic store-id)]
    (info {:event ::register-tx-report-topic
           :store-id store-id
           :topic topic})
    (pubsub/register-topic! peer topic
                            {:strategy (proto/pub-sub-only-strategy nil)})
    topic))

(defn unregister-tx-report-topic!
  "Unregister a tx-report topic. Call when database is removed.

   Parameters:
   - peer: The kabel peer atom
   - store-id: UUID identifying the database store

   Returns: The topic keyword"
  [peer store-id]
  (let [topic (tx-report-topic store-id)]
    (info {:event ::unregister-tx-report-topic
           :store-id store-id
           :topic topic})
    (pubsub/unregister-topic! peer topic)
    topic))

(defn tx-report->wire
  "Project a TxReport to a plain map fit for the wire.

  `:db-before` / `:db-after` are live DB values holding index roots, a storage
  handle and schema caches -- none of which may cross a connection. `db->stored`
  is what strips them, and until now it was called from inside the FRESSIAN
  WRITE HANDLER, which made a correct wire representation a property of one
  codec rather than of this namespace.

  Doing it here instead is codec-agnostic and removes a hard blocker: a
  TxReport is a defrecord, and a serializer that handles records natively (as
  CBOR tag 27 does) writes the record's raw fields with no opportunity for a
  write handler to intervene. Under such a codec the stripping would simply
  never happen, and the live state would go out on the wire with no error.

  The client already expects a plain map here -- `datahike.kabel.writer`'s
  `reconstruct-tx-report` / `reconstruct-stored-db` branch on stored-map vs
  live-DB -- so this changes nothing downstream."
  ;; Only project values that ARE databases. The old fressian handler fired
  ;; solely on a genuine TxReport record, so it never met anything else; doing
  ;; this at the publish site means we do, and a caller (a test stub, a
  ;; partially-built report) may legitimately carry a plain map here. Passing
  ;; those through unchanged preserves the previous behaviour exactly.
  [tx-report]
  (let [->stored (fn [db] (if (dbu/db? db) (second (dw/db->stored db false)) db))]
    (-> (into {} tx-report)
        (update :db-before ->stored)
        (update :db-after ->stored))))

(defn publish-tx-report!
  "Publish a tx-report to all subscribers. Called after each transaction.

   Parameters:
   - peer: The kabel peer atom
   - store-id: UUID identifying the database store
   - tx-report: The transaction report (with :db-before, :db-after, :tx-data, etc.)
   - request-id: Optional request-id for deduplication

   Returns: Channel yielding {:ok true :sent-count N} or {:error ...}"
  ([peer store-id tx-report]
   (publish-tx-report! peer store-id tx-report nil))
  ([peer store-id tx-report request-id]
   (let [topic (tx-report-topic store-id)
         ;; `:migration` STRIPPED here and only here.
         ;;
         ;; It is an import's source-id -> target-id map, threaded batch to
         ;; batch on the report because the writer owns the db and the caller
         ;; cannot reach into its loop (see `transact-entities-directly`). The
         ;; RETURN to the calling peer must therefore keep it — `handlers.cljc`
         ;; calls `tx-report->wire` separately for that, and the import breaks
         ;; without it.
         ;;
         ;; A SUBSCRIBER has no such need: nothing in this source tree reads
         ;; `:migration` off a broadcast. It is bookkeeping internal to one
         ;; import, and under the default `:eids :allocate` it holds one entry
         ;; per source entity — measured at 119 KB of wire for 20 000 entities,
         ;; growing across batches because the map accumulates. A million-entity
         ;; restore at the default `:batch-size` would fan roughly 33 MB of it
         ;; out to every subscriber, none of which looks at it.
         payload {:tx-report (dissoc (tx-report->wire tx-report) :migration)
                  :store-id store-id
                  :request-id request-id}]
     (debug {:event ::publish-tx-report
             :store-id store-id
             :request-id request-id
             :max-tx (get-in tx-report [:db-after :max-tx])})
     (pubsub/publish! peer topic payload))))

;; =============================================================================
;; Client-Side API
;; =============================================================================

(defn subscribe-tx-reports!
  "Subscribe to tx-reports for a database.

   Parameters:
   - peer: The kabel client peer atom
   - store-id: UUID identifying the database store
   - on-tx-report: (fn [payload]) callback receiving {:tx-report ... :store-id ... :request-id ...}

   Returns: Channel yielding {:ok topics} or {:error ...}"
  [peer store-id on-tx-report]
  (let [topic (tx-report-topic store-id)
        strategy (proto/pub-sub-only-strategy on-tx-report)]
    (info {:event ::subscribe-tx-reports
           :store-id store-id
           :topic topic})
    (pubsub/subscribe! peer #{topic}
                       {:strategies {topic strategy}})))

(defn unsubscribe-tx-reports!
  "Unsubscribe from tx-reports for a database.

   Parameters:
   - peer: The kabel client peer atom
   - store-id: UUID identifying the database store

   Returns: Channel yielding {:ok true}"
  [peer store-id]
  (let [topic (tx-report-topic store-id)]
    (info {:event ::unsubscribe-tx-reports
           :store-id store-id
           :topic topic})
    (pubsub/unsubscribe! peer #{topic})))

;; =============================================================================
;; Deduplication Helpers
;; =============================================================================

(defn make-tx-report-handler
  "Create a tx-report handler with deduplication support.

   Parameters:
   - pending-request-ids: Atom containing set of request-ids for own transactions
   - on-remote-tx: (fn [tx-report]) callback for transactions from other clients

   Returns: Handler function for subscribe-tx-reports!"
  [pending-request-ids on-remote-tx]
  (fn [{:keys [tx-report request-id]}]
    (if (and request-id (contains? @pending-request-ids request-id))
      ;; Own transaction - already handled via RPC, skip broadcast
      (do
        (debug {:event ::skip-own-tx-report :request-id request-id})
        (swap! pending-request-ids disj request-id))
      ;; Remote transaction - process
      (do
        (debug {:event ::handle-remote-tx-report
                :request-id request-id
                :max-tx (get-in tx-report [:db-after :max-tx])})
        (on-remote-tx tx-report)))))
