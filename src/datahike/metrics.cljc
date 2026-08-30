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
    :help "Connection leases held in this process, by database and branch."}

   :datahike_query_seconds
   {:type :histogram
    :help "Sampled caller-visible query latency, by outcome and result-cache disposition; see datahike_query_errors_total for exact failures."}

   :datahike_query_planning_seconds
   {:type :histogram
    :help "Sampled time to obtain a query plan, by outcome and plan-cache disposition; see datahike_query_planning_errors_total for exact failures."}

   :datahike_query_engine_total
   {:type :counter
    :help "Sampled uncached query executions, by engine and execution path."}

   :datahike_query_errors_total
   {:type :counter
    :help "Caller-visible query failures; unlike latency observations, every error is counted."}

   :datahike_query_planning_errors_total
   {:type :counter
    :help "Query planning failures; unlike latency observations, every error is counted."}

   :datahike_query_sample_every
   {:type :gauge
    :help "Successful queries represented by each sampled query observation."}})

(def ^:dynamic *query-metrics?*
  "Whether query, planner, and query-engine metrics are recorded.

   Enabled by default. The binding exists primarily for measuring the cost of
   instrumentation against the identical warmed query path; hosts with an
   exceptionally latency-sensitive embedded workload may also disable it."
  true)

(def ^:dynamic *query-metrics-sample-every*
  "Record one in this many successful queries. Errors are always recorded."
  256)

(def ^:dynamic *record-query-metrics?*
  "Internal scope shared by one query and its planner/engine work."
  false)

(defn describe!
  "Install Datahike's metric descriptions into the process registry.

   This is public chiefly so tests or hosts that deliberately reset the global
   registry can restore the descriptions. Ordinary processes get them when the
   namespace loads."
  []
  (doseq [[metric description] descriptions]
    (metrics/describe! metric description))
  (metrics/gauge! :datahike_query_sample_every {} *query-metrics-sample-every*)
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

(defn query-timer
  "Start a query/planner timer when query metrics are enabled, otherwise nil."
  ([] (query-timer *record-query-metrics?*))
  ([sampled?]
   (when (and *query-metrics?* sampled?)
     (metrics/timer))))

(defn sample-query?
  "Whether one successful query should record its query/planner/engine trace."
  []
  (let [sample-every (max 1 *query-metrics-sample-every*)]
    (and *query-metrics?*
         (or (= 1 sample-every)
             #?(:clj (zero? (.nextInt (java.util.concurrent.ThreadLocalRandom/current)
                                      (int sample-every)))
                :cljs (zero? (rand-int sample-every)))))))

(defn query!
  "Record one caller-visible query.

   `result-cache` is `:hit`, `:miss`, or `:bypass`; `outcome` is `:success`
  or `:error`. `started` comes from `query-timer`."
  [started result-cache outcome sampled?]
  (when (and *query-metrics?* (= :error outcome))
    (metrics/inc! :datahike_query_errors_total
                  {:result_cache (name result-cache)}))
  (when (and started sampled?)
    (metrics/observe-since! :datahike_query_seconds
                            {:outcome      (name outcome)
                             :result_cache (name result-cache)}
                            started))
  nil)

(defn query-planning!
  "Record one attempt to obtain a plan.

   `plan-cache` is `:hit` or `:miss`; `outcome` is `:success` or `:error`."
  [started plan-cache outcome]
  (when (and *query-metrics?* (= :error outcome))
    (metrics/inc! :datahike_query_planning_errors_total
                  {:plan_cache (name plan-cache)}))
  (when (and started *record-query-metrics?*)
    (metrics/observe-since! :datahike_query_planning_seconds
                            {:outcome    (name outcome)
                             :plan_cache (name plan-cache)}
                            started))
  nil)

(defn query-engine!
  "Count an uncached engine execution on a bounded engine/path pair."
  [engine path]
  (when (and *query-metrics?* *record-query-metrics?*)
    (metrics/inc! :datahike_query_engine_total
                  {:engine (name engine) :path (name path)}))
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

#?(:clj
   (do
     (defn- sum-series [series]
       (reduce + 0 (vals (or series {}))))

     (defn- histogram-summary [series]
       (reduce (fn [{:keys [count sum]} value]
                 {:count (+ count (or (:count value) 0))
                  :sum   (+ sum (or (:sum value) 0))})
               {:count 0 :sum 0.0}
               (vals (or series {}))))

     (defn- disposition-counts [series label]
       (reduce-kv (fn [counts labels value]
                    (update counts (keyword (get labels label "unknown"))
                            (fnil + 0) (or (:count value) 0)))
                  {}
                  (or series {})))

     (defn- database-metric [snapshot metric value-fn]
       (reduce-kv (fn [by-database labels value]
                    (if-let [database (:database labels)]
                      (update by-database database (fnil + 0) (value-fn value))
                      by-database))
                  {}
                  (get-in snapshot [metric :series])))

     (defn runtime-snapshot
       "Cheap process-local status for the standalone operator page.

        This reads only the metrics registry and raw state of connections the
        process already owns. It never opens a database, refreshes a branch,
        probes a store, or walks an index."
       ([connections] (runtime-snapshot connections (metrics/snapshot)))
       ([connections snapshot]
        (let [query-series    (get-in snapshot [:datahike_query_seconds :series])
              query-summary   (histogram-summary query-series)
              planning-series (get-in snapshot [:datahike_query_planning_seconds :series])
              planning-summary (histogram-summary planning-series)
              transactions    (database-metric snapshot :datahike_transactions_total identity)
              datoms          (database-metric snapshot :datahike_transacted_datoms_total identity)
              commits         (database-metric snapshot :datahike_commit_seconds #(or (:count %) 0))
              commit-seconds  (database-metric snapshot :datahike_commit_seconds #(or (:sum %) 0.0))
              conflicts       (database-metric snapshot :datahike_head_conflicts_total identity)
              live (reduce-kv
                    (fn [by-database [store-id branch] {:keys [conn count]}]
                      (if-not conn
                        by-database
                        (let [database (str store-id)
                              state    @(get conn :wrapped-atom)
                              basis-t  (when (map? state) (:max-tx state))]
                          (-> by-database
                              (update-in [database :leases] (fnil + 0) (or count 1))
                              (update-in [database :branches] (fnil conj #{})
                                         (keyword-label (or branch :db)))
                              (cond-> basis-t
                                (update-in [database :basis-t] #(max (or % basis-t) basis-t)))))))
                    {}
                    @connections)
              database-ids (into (set (keys live))
                                 (mapcat keys)
                                 [transactions datoms commits commit-seconds conflicts])]
          {:node
           {:sampled-queries       (:count query-summary)
            :average-query-ms      (when (pos? (:count query-summary))
                                     (* 1000.0 (/ (:sum query-summary) (:count query-summary))))
            :result-cache          (disposition-counts query-series :result_cache)
            :query-errors          (sum-series (get-in snapshot [:datahike_query_errors_total :series]))
            :sampled-plans         (:count planning-summary)
            :average-planning-ms   (when (pos? (:count planning-summary))
                                     (* 1000.0 (/ (:sum planning-summary) (:count planning-summary))))
            :plan-cache            (disposition-counts planning-series :plan_cache)
            :planning-errors       (sum-series (get-in snapshot [:datahike_query_planning_errors_total :series]))
            :query-sample-every    (get-in snapshot [:datahike_query_sample_every :series {}])}
           :databases
           (into {}
                 (for [database database-ids
                       :let [commit-count (get commits database 0)
                             seconds      (get commit-seconds database 0.0)
                             connection   (get live database)]]
                   [database
                    {:loaded?           (boolean connection)
                     :leases            (get connection :leases 0)
                     :branches          (-> connection :branches sort vec)
                     :basis-t           (:basis-t connection)
                     :transactions      (get transactions database 0)
                     :transacted-datoms (get datoms database 0)
                     :commits           commit-count
                     :average-commit-ms (when (pos? commit-count)
                                          (* 1000.0 (/ seconds commit-count)))
                     :head-conflicts     (get conflicts database 0)}]))})))))
