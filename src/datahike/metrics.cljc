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
    :help "Transaction invocations whose branch head moved, by whether they were retried or failed."}})

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
