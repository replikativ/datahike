(ns benchmark.gc-stress
  "Write-load × GC-strategy study against the file store backend.

   Each grid cell runs a sustained write load against a fresh file-backed
   database under one GC strategy, sampling store growth over time and
   recording per-commit latency. Answers: how does the store grow under
   different write shapes, which GC strategy contains it, and what does each
   strategy cost in commit latency / pauses?

   Axes:
     commit-size  1 | 100 | 10000 datoms per transact
     shape        :insert  fresh entities every commit
                  :upsert  value-updates of a fixed working set (dead versions pile up)
                  :churn   alternating insert / retractEntity batches
     gc           :none            no collection; freed backlog just grows
                  :online-commit   per-commit online GC ({:online-gc {:enabled? true}})
                  :online-bg       background thread (start-background-gc!)
                  :offline-period  gc-storage! every GC_OFFLINE_EVERY_MS (blocking)
                  :offline-end     one gc-storage! after the load stops

   Per cell it reports: tx/s, commit latency p50/p95/p99/max, store objects and
   bytes over time (samples), freed-address backlog, final + post-offline-GC
   store size, and cold-connect time after the run.

   Run (full grid, ~1 min per cell):
     clj -M:bench -m benchmark.gc-stress
   Focused:
     GC_GRID='{:commit-sizes [100] :shapes [:churn] :gcs [:none :online-commit]}' \\
       clj -M:bench -m benchmark.gc-stress

   Tunables (env):
     GC_CELL_MS            per-cell load duration (default 60000)
     GC_GRACE_MS           online-GC grace period (default 1000 — small so
                           collection is observable within a short cell)
     GC_BG_INTERVAL_MS     background-GC interval (default 2000)
     GC_OFFLINE_EVERY_MS   offline-periodic cadence (default 15000)
     GC_SAMPLE_MS          store-size sample cadence (default 2000)
     GC_DIFF_BUF           :index-config {:diff-buf-size N} (default 0)
     GC_FUSION             :fuse-index-roots? (default false)
     GC_KEEP_HISTORY       :keep-history? (default false)
     GC_GRID               EDN map overriding {:commit-sizes [..] :shapes [..] :gcs [..]}
     GC_OUT                EDN results file (default /tmp/dh-gc-stress/results.edn)"
  (:require [datahike.api :as d]
            [datahike.gc :as gc]
            [datahike.online-gc :as online-gc]
            [konserve.core :as k]
            [superv.async :refer [<?? S]]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.core.async :as async])
  (:import [java.util Date]
           [java.nio.file Files Paths]
           [java.io File]))

;; ---------------------------------------------------------------------------
;; env

(defn- env-long [k default] (or (some-> (System/getenv k) Long/parseLong) default))
(defn- env-bool [k default] (if-some [v (System/getenv k)] (Boolean/parseBoolean v) default))
(defn- env-edn  [k default] (or (some-> (System/getenv k) edn/read-string) default))

(def cell-ms          (env-long "GC_CELL_MS" 60000))
(def grace-ms         (env-long "GC_GRACE_MS" 1000))
(def bg-interval-ms   (env-long "GC_BG_INTERVAL_MS" 2000))
(def offline-every-ms (env-long "GC_OFFLINE_EVERY_MS" 15000))
(def sample-ms        (env-long "GC_SAMPLE_MS" 2000))
(def diff-buf         (env-long "GC_DIFF_BUF" 0))
(def fusion?          (env-bool "GC_FUSION" false))
(def keep-history?    (env-bool "GC_KEEP_HISTORY" false))
(def out-file         (or (System/getenv "GC_OUT") "/tmp/dh-gc-stress/results.edn"))

(def default-grid
  {:commit-sizes [1 100 10000]
   :shapes       [:insert :upsert :churn]
   :gcs          [:none :online-commit :online-bg :offline-period :offline-end]})

(def grid (merge default-grid (env-edn "GC_GRID" {})))

;; ---------------------------------------------------------------------------
;; store measurement

(defn- dir-bytes [^String path]
  (let [f (io/file path)]
    (if-not (.exists f)
      0
      (->> (file-seq f) (filter #(.isFile ^File %)) (map #(.length ^File %)) (reduce + 0)))))

(defn- store-objects [store] (count (k/keys store {:sync? true})))

(defn- freed-backlog [store] (some-> store :storage :freed-addresses deref count))

(defn- percentiles [xs]
  (if (empty? xs)
    {}
    (let [sorted (vec (sort xs))
          n (count sorted)
          at (fn [q] (nth sorted (min (dec n) (int (Math/floor (* q n))))))]
      {:n n
       :p50 (at 0.50) :p95 (at 0.95) :p99 (at 0.99)
       :max (peek sorted)})))

(defn- ms [nanos] (/ (double nanos) 1e6))

;; ---------------------------------------------------------------------------
;; write shapes — each returns tx-data for commit `i` of `size` datoms

(def ^:private working-set 10000)

(defn- tx-data [shape size i]
  (case shape
    ;; fresh entities: monotonically growing db
    :insert (mapv (fn [j] {:id (+ (* i size) j) :val j})
                  (range size))
    ;; update :val of a fixed working set: store data stable, dead versions accumulate
    :upsert (mapv (fn [j] {:id (mod (+ (* i size) j) working-set) :val (+ i j)})
                  (range size))
    ;; alternate: even commits insert a window, odd commits retract it
    :churn  (let [base (* (quot i 2) size)]
              (if (even? i)
                (mapv (fn [j] {:id (+ 1000000 base j) :val j}) (range size))
                (mapv (fn [j] [:db/retractEntity [:id (+ 1000000 base j)]]) (range size))))))

(def schema
  [{:db/ident :id  :db/valueType :db.type/long :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :val :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])

;; :upsert needs the working set present; :churn needs nothing; :insert nothing.
(defn- seed! [conn shape]
  (when (= shape :upsert)
    (doseq [batch (partition-all 5000 (range working-set))]
      (d/transact conn (mapv (fn [j] {:id j :val 0}) batch)))))

;; ---------------------------------------------------------------------------
;; one cell

(defn run-cell
  "Run one grid cell; returns a result map."
  [{:keys [commit-size shape gc] :as cell}]
  (let [label (str (name shape) "-" commit-size "-" (name gc))
        path  (str "/tmp/dh-gc-stress/" label)
        cfg   (cond-> {:store {:backend :file :path path :id (java.util.UUID/randomUUID)}
                       :schema-flexibility :write
                       :keep-history? keep-history?}
                (pos? diff-buf)         (assoc :index-config {:diff-buf-size diff-buf})
                fusion?                 (assoc :fuse-index-roots? true)
                (= gc :online-commit)   (assoc :online-gc {:enabled? true
                                                           :grace-period-ms grace-ms}))]
    (println "== cell" label "==")
    (when (d/database-exists? cfg) (d/delete-database cfg))
    (d/create-database cfg)
    (let [conn      (d/connect cfg)
          store     (:store @conn)
          _         (d/transact conn schema)
          _         (seed! conn shape)
          samples   (atom [])
          latencies (atom [])
          tx-count  (atom 0)
          gc-pauses (atom [])
          stop?     (atom false)
          t0        (System/currentTimeMillis)
          sampler   (future
                      (while (not @stop?)
                        (swap! samples conj
                               {:t-ms    (- (System/currentTimeMillis) t0)
                                :objects (store-objects store)
                                :bytes   (dir-bytes path)
                                :freed   (freed-backlog store)
                                :txs     @tx-count})
                        (Thread/sleep ^long sample-ms)))
          bg-stop   (when (= gc :online-bg)
                      (online-gc/start-background-gc! store {:enabled? true
                                                             :interval-ms bg-interval-ms
                                                             :grace-period-ms grace-ms}))
          last-offline (atom t0)]
      ;; load loop
      (loop [i 0]
        (when (< (- (System/currentTimeMillis) t0) cell-ms)
          (let [data (tx-data shape commit-size i)
                t1   (System/nanoTime)]
            (d/transact conn data)
            (swap! latencies conj (- (System/nanoTime) t1))
            (swap! tx-count inc))
          (when (and (= gc :offline-period)
                     (> (- (System/currentTimeMillis) @last-offline) offline-every-ms))
            (let [t1 (System/nanoTime)]
              (<?? S (gc/gc-storage! @conn (Date.)))
              (swap! gc-pauses conj (- (System/nanoTime) t1))
              (reset! last-offline (System/currentTimeMillis))))
          (recur (inc i))))
      (reset! stop? true)
      @sampler
      (when bg-stop (async/close! bg-stop))
      ;; end-of-run offline GC (also for :offline-end)
      (let [pre-gc {:objects (store-objects store) :bytes (dir-bytes path)
                    :freed (freed-backlog store)}
            _      (when (contains? #{:offline-end :offline-period} gc)
                     (let [t1 (System/nanoTime)]
                       (<?? S (gc/gc-storage! @conn (Date.)))
                       (swap! gc-pauses conj (- (System/nanoTime) t1))))
            post-gc (when (contains? #{:offline-end :offline-period} gc)
                      {:objects (store-objects store) :bytes (dir-bytes path)})
            _       (d/release conn)
            ;; cold connect after the dust settles
            t1      (System/nanoTime)
            conn2   (d/connect cfg)
            cold-ms (ms (- (System/nanoTime) t1))
            count-after (d/q '[:find (count ?e) . :where [?e :id _]] @conn2)]
        (d/release conn2)
        (let [wall-s (/ (- (System/currentTimeMillis) t0) 1000.0)
              res {:cell cell
                   :label label
                   :config {:diff-buf diff-buf :fusion fusion? :keep-history keep-history?
                            :cell-ms cell-ms :grace-ms grace-ms}
                   :txs @tx-count
                   :tx-per-s (/ @tx-count wall-s)
                   :datoms-per-s (/ (* @tx-count commit-size) wall-s)
                   :latency-ms (into {} (map (fn [[k v]] [k (if (number? v) (ms v) v)]))
                                     (percentiles @latencies))
                   :gc-pauses-ms (mapv ms @gc-pauses)
                   :pre-gc pre-gc
                   :post-gc post-gc
                   :cold-connect-ms cold-ms
                   :entity-count count-after
                   :samples @samples}]
          (println (format "   %d txs (%.1f tx/s, %.0f datoms/s) | lat p50 %.2f p99 %.2f ms | store %d objs %.1f MB (freed backlog %s)%s | cold-connect %.1f ms"
                           (long @tx-count) (:tx-per-s res) (:datoms-per-s res)
                           (get-in res [:latency-ms :p50] 0.0) (get-in res [:latency-ms :p99] 0.0)
                           (long (:objects pre-gc)) (/ (:bytes pre-gc) 1e6) (str (:freed pre-gc))
                           (if post-gc (format " → post-GC %d objs %.1f MB" (long (:objects post-gc)) (/ (:bytes post-gc) 1e6)) "")
                           cold-ms))
          res)))))

;; ---------------------------------------------------------------------------
;; grid runner + report

(defn- md-table [results]
  (let [fmt (fn [r]
              (format "| %-24s | %8d | %8.1f | %9.2f | %9.2f | %8d | %7.1f | %7s | %9s | %9.1f |"
                      (:label r) (:txs r) (:tx-per-s r)
                      (get-in r [:latency-ms :p50] 0.0) (get-in r [:latency-ms :p99] 0.0)
                      (long (get-in r [:pre-gc :objects]))
                      (/ (get-in r [:pre-gc :bytes]) 1e6)
                      (str (get-in r [:pre-gc :freed]))
                      (if-let [pg (:post-gc r)] (str (long (:objects pg))) "-")
                      (:cold-connect-ms r)))]
    (str "| cell                     |      txs |     tx/s |   p50 ms |   p99 ms |     objs |     MB |   freed | post-gc-o | cold-c ms |\n"
         "|--------------------------|----------|----------|----------|----------|----------|--------|---------|-----------|-----------|\n"
         (clojure.string/join "\n" (map fmt results)))))

(defn -main [& _]
  (io/make-parents out-file)
  (println "gc-stress grid:" grid)
  (println "cell-ms" cell-ms "| grace-ms" grace-ms "| diff-buf" diff-buf "| fusion" fusion? "| keep-history" keep-history?)
  (let [cells (for [size (:commit-sizes grid)
                    shape (:shapes grid)
                    gc (:gcs grid)]
                {:commit-size size :shape shape :gc gc})
        results (mapv run-cell cells)]
    (spit out-file (pr-str {:grid grid
                            :config {:diff-buf diff-buf :fusion fusion?
                                     :keep-history keep-history? :cell-ms cell-ms
                                     :grace-ms grace-ms :bg-interval-ms bg-interval-ms
                                     :offline-every-ms offline-every-ms}
                            :results results}))
    (println)
    (println (md-table results))
    (println "\nfull results (with time-series samples):" out-file)
    (shutdown-agents)))
