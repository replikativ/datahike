(ns datahike.metrics
  "Datahike's process-level metric instruments.

   Recording is intentionally independent of exposition: the writer records
   durable commits and branch-head conflicts into `replikativ.metrics`, while
   a host may snapshot and expose that registry however it chooses. Labels use
   the stable store id and branch, never storage paths or credentials."
  (:require [datahike.store :as store]
            [replikativ.metrics :as metrics]))

(def descriptions
  {:datahike_commit_seconds
   {:type :histogram
    :help "Time to make one writer batch durable, including the fenced branch-head write."}

   :datahike_transactions_total
   {:type :counter
    :help "Transactions made durable."}

   :datahike_transacted_datoms_total
   {:type :counter
    :help "Datoms written by durable transactions."}

   :datahike_head_conflicts_total
   {:type :counter
    :help "Transaction invocations whose branch head moved, by whether they were retried or failed."}

   :datahike_http_request_seconds
   {:type :histogram
    :help "Time to serve an HTTP API request, by operation and response status."}

   :datahike_http_rejected_total
   {:type :counter
    :help "HTTP requests refused for missing credentials, missing permission, or excessive size."}

   :datahike_connections
   {:type :gauge
    :help "Connection leases held in this process, by database and branch."}})

(defn describe!
  "Install Datahike's metric descriptions into the process registry.

   This is public chiefly so tests or hosts that deliberately reset the global
   registry can restore the descriptions. Ordinary processes get them when the
   namespace loads."
  []
  (doseq [[metric description] descriptions]
    (metrics/describe! metric description))
  nil)

;; Namespace loading installs descriptions; repeated loads are safe because
;; replikativ.metrics/describe! is idempotent.
(describe!)

(defn- keyword-label [k]
  (if-let [keyword-namespace (namespace k)]
    (str keyword-namespace "/" (name k))
    (name k)))

(defn db-labels
  "Low-cardinality labels for the database `config` describes."
  [config]
  {:database (some-> (store/store-identity (:store config)) str)
   :branch   (keyword-label (:branch config :db))})

(defn commit!
  "Record one durable writer batch.

   `milliseconds` is the batch's durable commit time, `transactions` is the
   number of transaction reports it made durable, and `datoms` is their total
   written datom count. A batch is one duration observation but may represent
   more than one transaction."
  [config milliseconds transactions datoms]
  (let [labels (db-labels config)]
    (metrics/observe! :datahike_commit_seconds labels (/ milliseconds 1000.0))
    (when (pos? transactions)
      (metrics/inc! :datahike_transactions_total labels transactions))
    (when (pos? datoms)
      (metrics/inc! :datahike_transacted_datoms_total labels datoms)))
  nil)

(defn head-conflict!
  "Record one transaction invocation whose branch-head fence was rejected.

   `outcome` is `:retried` after the invocation was queued for replay, or
   `:failed` when its caller was given the conflict."
  [config outcome]
  (metrics/inc! :datahike_head_conflicts_total
                (assoc (db-labels config) :outcome (name outcome)))
  nil)

(defn http-request!
  "Record an HTTP request for authorization operation `op`, response `status`,
   and the monotonic timer value returned by `replikativ.metrics/timer`."
  [op status started]
  (metrics/observe-since! :datahike_http_request_seconds
                          {:op (name op) :status (str status)}
                          started)
  nil)

(defn http-rejected!
  "Record a gate or authorization rejection.

   `reason` is `:unauthorized`, `:forbidden`, or `:too-large`."
  [reason]
  (metrics/inc! :datahike_http_rejected_total {:reason (name reason)})
  nil)

(defn connection-samples
  "Gauge samples for the live entries in a Datahike connection registry atom.

   Reservations whose `:conn` is nil are omitted. The value is the registry's
   reference count: one for its base lease plus any additional callers."
  [connections]
  (let [{:keys [type help]} (:datahike_connections descriptions)]
    (for [[[store-id branch] {:keys [conn count]}] @connections
          :when conn]
      {:name   :datahike_connections
       :type   type
       :help   help
       :labels {:database (str store-id)
                :branch   (keyword-label (or branch :db))}
       :value  (or count 1)})))
