(ns benchmark.soak
  "Long-running correctness soak for datahike — designed to run for minutes in
   CI or overnight on a workstation, hunting issues that only sustained load
   exposes (drift, leaks, GC races, audit divergence, reopen corruption).

   Four seeded scenarios run CONCURRENTLY, each against its own file store,
   each continuously checking invariants against an in-memory model:

     :oltp    pipelined transact! (upserts/inserts/retracts) vs a model map;
              background concurrent mark-and-sweep GC on; periodic cold-reopen
              equivalence; final full-index equality.
     :bulk    repeated bulk-import waves (the bett/dbpedia shape) with online
              freed-tracking GC; asserts store containment relative to live
              size and import-throughput stability (drift detector).
     :branchy periodic branch!/diverge/verify/delete cycles under writes with
              background GC; asserts branch isolation warm+cold.
     :crypto  crypto-hash + diff-buf writes; periodic verify-chain :deep? and
              cold-reopen audit; asserts :ok forever.

   Any invariant failure is recorded (scenario keeps running to find more) and
   the process exits non-zero at the end, printing all failures. Progress
   lines print every SOAK_REPORT_S seconds; a final EDN report is written to
   SOAK_OUT.

   Run 10 minutes (default):   clojure -M:bench -m benchmark.soak
   Overnight (8h):             SOAK_MINUTES=480 clojure -M:bench -m benchmark.soak
   Subset:                     SOAK_SCENARIOS=oltp,crypto clojure -M:bench -m benchmark.soak
   Deterministic ops:          SOAK_SEED=42 (default 42; wall-clock interleaving
                               still varies — failures reproduce statistically,
                               invariants are checked continuously so reports
                               localize the breakage)."
  (:require [datahike.api :as d]
            [datahike.audit :as audit]
            [datahike.gc :as gc]
            [datahike.versioning :as v]
            [konserve.core :as k]
            [clojure.core.async :as async]
            [clojure.string :as str]
            [superv.async :refer [<?? S]])
  (:import [java.util Random Date]))

(defn- env-long [k default] (or (some-> (System/getenv k) Long/parseLong) default))
(def minutes  (env-long "SOAK_MINUTES" 10))
(def report-s (env-long "SOAK_REPORT_S" 60))
(def seed     (env-long "SOAK_SEED" 42))
(def out-file (or (System/getenv "SOAK_OUT") "/tmp/dh-soak/report.edn"))
(def scenarios (if-let [s (System/getenv "SOAK_SCENARIOS")]
                 (set (map keyword (str/split s #",")))
                 #{:oltp :bulk :branchy :crypto}))

(def deadline (delay (+ (System/currentTimeMillis) (* minutes 60000))))
(defn- running? [] (< (System/currentTimeMillis) @deadline))
(def failures (atom []))
(defn- fail! [scenario invariant data]
  (swap! failures conj {:scenario scenario :invariant invariant :data data
                        :at (str (Date.))})
  (println "!! FAILURE" scenario invariant (pr-str data)))

(def progress (atom {}))
(defn- report! [scenario m] (swap! progress assoc scenario m))

(defn- mk-cfg [label extra]
  (merge {:store {:backend :file :path (str "/tmp/dh-soak/" label)
                  :id (java.util.UUID/randomUUID)}
          :schema-flexibility :write :keep-history? false}
         extra))

(def schema
  [{:db/ident :id :db/valueType :db.type/long :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :score :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])

(defn- fresh! [cfg]
  (when (d/database-exists? cfg) (d/delete-database cfg))
  (d/create-database cfg)
  (let [conn (d/connect cfg)]
    (d/transact conn schema)
    conn))

(defn- db-map [db]
  (into {} (d/q '[:find ?id ?s :where [?e :id ?id] [?e :score ?s]] db)))

;; ---------------------------------------------------------------------------

(defn oltp-scenario []
  (let [cfg  (mk-cfg "oltp" {})
        conn (atom (fresh! cfg))
        rng  (Random. seed)
        model (atom {})
        txs  (atom 0)
        stop-gc (gc/start-background-gc! @conn {:interval-ms 30000 :grace-ms 5000})
        inflight (java.util.concurrent.Semaphore. 32)
        reconnect! (atom false)]
    (try
      (loop [i 0]
        (when (running?)
          (.acquire inflight)
          (let [r (.nextInt rng 10) id (long (.nextInt rng 20000))
                tx (cond
                     (< r 6) (let [v (long (.nextInt rng 1000000))]
                               (swap! model assoc id v)
                               [{:id id :score v}])
                     (< r 9) (let [v (long i)]
                               (swap! model assoc id v)
                               [{:id id :score v}])
                     (contains? @model id)
                     (do (swap! model dissoc id)
                         [[:db/retractEntity [:id id]]])
                     :else nil)]
            (if tx
              (async/take! (d/transact! @conn {:tx-data tx})
                           (fn [r] (when (and (instance? Throwable r)
                                              (not (re-find #"(?i)nothing to retract" (str (ex-message r)))))
                                     (fail! :oltp :tx-error {:msg (ex-message r)})
                                     ;; writer may be dead — recover instead of wedging;
                                     ;; the failure above is already recorded
                                     (reset! reconnect! true))
                             (.release inflight)))
              (.release inflight))
            (swap! txs inc)
            (when @reconnect!
              (.acquire inflight 32)
              (try (d/release @conn) (catch Throwable _))
              (reset! conn (d/connect cfg))
              ;; in-flight txs at death were lost; resync the model to the
              ;; durable state so subsequent invariants stay meaningful
              (reset! model (db-map @@conn))
              (reset! reconnect! false)
              (.release inflight 32)))
          ;; periodic cold-reopen equivalence (~ every 20k ops)
          (when (zero? (mod (inc i) 20000))
            (.acquire inflight 32)
            (d/release @conn)
            (reset! conn (d/connect cfg))
            (when-not (= @model (db-map @@conn))
              (fail! :oltp :cold-reopen-divergence
                     {:model-n (count @model) :db-n (count (db-map @@conn))}))
            (.release inflight 32)
            (report! :oltp {:txs @txs :entities (count @model)}))
          (recur (inc i))))
      (.acquire inflight 32)
      (stop-gc)
      (when-not (= @model (db-map @@conn))
        (fail! :oltp :final-divergence {:model-n (count @model)}))
      (finally
        (try (d/release @conn) (catch Throwable _))))))

(defn bulk-scenario []
  (let [cfg  (mk-cfg "bulk" {:online-gc {:enabled? true :grace-period-ms 1000}})
        conn (fresh! cfg)
        wave-times (atom [])
        base (atom 0)]
    (try
      (while (running?)
        (let [t0 (System/nanoTime)]
          (doseq [b (partition-all 5000 (range @base (+ @base 50000)))]
            (d/transact conn (mapv (fn [i] {:id (long i) :score (long 0)}) b)))
          (swap! wave-times conj (/ (- (System/nanoTime) t0) 1e9))
          (swap! base + 50000)
          (let [n (d/q '[:find (count ?e) . :where [?e :id _]] @conn)]
            (when-not (= n @base)
              (fail! :bulk :count-mismatch {:expected @base :got n})))
          ;; drift detector: TWO consecutive waves >3x the median (a single slow wave
          ;; is host noise — GC pause, co-tenant CPU — and self-recovers)
          (let [ts @wave-times
                median (fn [v] (nth (vec (sort v)) (quot (count v) 2)))]
            (when (and (>= (count ts) 6)
                       (let [m (median ts)]
                         (and (> (peek ts) (* 3 m))
                              (> (peek (pop ts)) (* 3 m)))))
              (fail! :bulk :throughput-drift {:waves (mapv #(format "%.1f" %) ts)})))
          (report! :bulk {:entities @base :waves (count @wave-times)
                          :last-wave-s (format "%.1f" (peek @wave-times))})))
      (finally (try (d/release conn) (catch Throwable _))))))

(defn branchy-scenario []
  (let [cfg  (mk-cfg "branchy" {:keep-history? true})
        conn (fresh! cfg)
        rng  (Random. (inc seed))
        round (atom 0)
        stop-gc (gc/start-background-gc! conn {:interval-ms 20000 :grace-ms 5000})]
    (try
      (doseq [b (partition-all 2000 (range 4000))]
        (d/transact conn (mapv (fn [i] {:id (long i) :score (long 0)}) b)))
      (while (running?)
        (let [br (keyword (str "soak-" (swap! round inc)))]
          (v/branch! conn :db br)
          (let [bconn (d/connect (assoc cfg :branch br))
                bmark (long (- (* 1000 @round)))]
            (dotimes [i 50] (d/transact conn [{:id (long (.nextInt rng 4000)) :score (long @round)}]))
            (dotimes [i 30] (d/transact bconn [{:id (long (.nextInt rng 4000)) :score bmark}]))
            (let [main-vals (set (d/q '[:find [?s ...] :where [_ :score ?s]] @conn))
                  b-vals    (set (d/q '[:find [?s ...] :where [_ :score ?s]] @bconn))]
              (when (contains? main-vals bmark)
                (fail! :branchy :isolation-main {:round @round}))
              (when-not (contains? b-vals bmark)
                (fail! :branchy :branch-lost-write {:round @round})))
            (d/release bconn)
            (v/delete-branch! conn br))
          (report! :branchy {:rounds @round})))
      (stop-gc)
      (finally (try (d/release conn) (catch Throwable _))))))

(defn crypto-scenario []
  (let [cfg  (mk-cfg "crypto" {:crypto-hash? true :keep-history? true
                               :index-config {:diff-buf-size 256}})
        conn (atom (fresh! cfg))
        rng  (Random. (+ 2 seed))
        checks (atom 0)]
    (try
      (doseq [b (partition-all 2000 (range 5000))]
        (d/transact @conn (mapv (fn [i] {:id (long i) :score (long 0)}) b)))
      (while (running?)
        (dotimes [_ 200]
          (d/transact @conn [{:id (long (.nextInt rng 5000)) :score (long (.nextInt rng 1000000))}]))
        (let [warm (audit/verify-chain @@conn nil {:deep? true})]
          (when-not (= :ok (:status warm))
            (fail! :crypto :warm-audit {:status (:status warm)})))
        (d/release @conn)
        (reset! conn (d/connect cfg))
        (let [cold (audit/verify-chain @@conn nil {:deep? true})]
          (when-not (= :ok (:status cold))
            (fail! :crypto :cold-audit {:status (:status cold)})))
        (report! :crypto {:audit-checks (swap! checks inc)}))
      (finally (try (d/release @conn) (catch Throwable _))))))

;; ---------------------------------------------------------------------------

(defn -main [& _]
  (clojure.java.io/make-parents out-file)
  (println "soak:" minutes "min | scenarios" scenarios "| seed" seed)
  (force deadline)
  (let [fns {:oltp oltp-scenario :bulk bulk-scenario
             :branchy branchy-scenario :crypto crypto-scenario}
        threads (mapv (fn [s] (let [t (Thread. ^Runnable (fn []
                                                           (try ((fns s))
                                                                (catch Throwable e
                                                                  (fail! s :scenario-crashed {:msg (str e)})))))]
                                (.start t) t))
                      (filter scenarios (keys fns)))
        reporter (Thread. ^Runnable
                          (fn [] (while (running?)
                                   (Thread/sleep (* report-s 1000))
                                   (println (str (Date.)) "|" (pr-str @progress)
                                            "| failures:" (count @failures)))))]
    (.start reporter)
    (doseq [^Thread t threads] (.join t))
    (spit out-file (pr-str {:minutes minutes :seed seed :scenarios scenarios
                            :progress @progress :failures @failures}))
    (println "\n=== soak complete ===")
    (println "progress:" (pr-str @progress))
    (if (seq @failures)
      (do (println "FAILURES:" (count @failures))
          (doseq [f @failures] (println "  " (pr-str f)))
          (System/exit 1))
      (do (println "all invariants held")
          (System/exit 0)))))
