(ns ^:no-doc datahike.kabel.tx-broadcast
  "Tx-report broadcasting via kabel.pubsub.

   This namespace provides functions for:
   - Server: registering tx-report topics and publishing tx-reports
   - Client: subscribing to tx-reports for remote databases

   Architecture:
   - Each database has a topic: :tx-report/<store-id>
   - Uses PubSubOnlyStrategy (no handshake, just receive publishes)
   - Deduplication via request-id (skip own transactions)"
  (:require [kabel.pubsub :as pubsub]
            [kabel.pubsub.protocol :as proto]
            [kabel.peer :as peer]
            #?(:clj [kabel.platform-log :refer [debug info warn]])
            #?(:clj [clojure.core.async :refer [go put! chan close!]]
               :cljs [clojure.core.async :refer [go put! chan close!] :include-macros true]))
  #?(:cljs (:require-macros [kabel.platform-log :refer [debug info warn]])))

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
         payload {:tx-report tx-report
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
