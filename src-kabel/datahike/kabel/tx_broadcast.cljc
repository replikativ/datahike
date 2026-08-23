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
               :cljs [clojure.core.async :refer [go put! chan close!] :include-macros true])
            #?(:cljs [datahike.db :refer [TxReport]]))
  #?(:clj (:import [datahike.db TxReport])))

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


;; `tx-report->wire` USED TO LIVE HERE, and its removal is the point.
;;
;; It was moved out of the fressian write handler on the reasoning that a
;; correct wire representation should not be a property of one codec — a
;; serializer handling records natively (as CBOR tag 27 does) would write the
;; raw fields and the live DBs would go out with no error. Sound hazard, wrong
;; remedy: `datahike.cbor` REGISTERS a tag-27 write handler for TxReport that
;; projects, so the hazard is answered by registration, exactly as fressian
;; answered it — and `datahike.test.remote-cbor-test` is the test that stops a
;; type going unregistered.
;;
;; What the move cost was the DISPATCH. A write handler is keyed on the TYPE, so
;; it met a TxReport and nothing else. A publish site meets whatever the op
;; returned, and `gc-storage!` returns a SET: `(into {} #{uuid …})` threw
;; server-side, so `d/gc-storage` over a kabel peer died outright. A `map?` guard
;; would not have been enough either, because `update` on an absent key ADDS it —
;; `build-secondary-index!`'s result map was gaining `:db-before nil` and
;; `:db-after nil` on the wire.
;;
;; So the projection is back to one definition, in `datahike.cbor`, reached only
;; through the handler registered for the TxReport class. Ops return their own
;; results unchanged.

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
   ;; NOT every op that reaches this function produced a transaction. The
   ;; server has ONE global dispatch handler, so it hands us whatever
   ;; `writer/dispatch!` returned, and `default-write-fn-map` holds three ops
   ;; that are not report producers: `gc-storage!` (returns a SET of freed
   ;; keys), `build-secondary-index!` and `install-secondary-index!` (plain
   ;; status maps). The set threw outright — `dissoc` on a set is a
   ;; ClassCastException — so `d/gc-storage` over a kabel peer died at the
   ;; broadcast. The maps were quieter and worse: they went out on the
   ;; tx-report topic, where a subscriber reads them as transactions.
   ;;
   ;; So the topic decides. A subscriber to `tx-report-topic` asked for
   ;; transaction reports; an op that made no transaction has nothing to say
   ;; here and is simply not published. Keyed on the TYPE rather than on shape
   ;; — a status map has `:tx-data` no more than a set does, but `update` on an
   ;; absent key ADDS it, so a shape guard invites exactly the silent
   ;; key-fabrication this whole path already suffered once.
   (if-not (instance? TxReport tx-report)
     (do (debug {:event ::skip-non-tx-report
                 :store-id store-id
                 :request-id request-id
                 :result-type (type tx-report)})
         ;; A channel, because every other return from this function is one and
         ;; the caller `<?`s it.
         (go {:ok true :sent-count 0 :skipped :not-a-tx-report}))
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
         ;; `dissoc` on the record itself: `:migration` is an extension key, not
         ;; one of TxReport's five fields, so this stays a TxReport and the
         ;; codec's type-keyed handler still projects it on the way out. The
         ;; strip is genuinely publish-specific — see below — which is why it
         ;; stays here while the projection does not.
         payload {:tx-report (dissoc tx-report :migration)
                  :store-id store-id
                  :request-id request-id}]
     (debug {:event ::publish-tx-report
             :store-id store-id
             :request-id request-id
             :max-tx (get-in tx-report [:db-after :max-tx])})
       (pubsub/publish! peer topic payload)))))

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
