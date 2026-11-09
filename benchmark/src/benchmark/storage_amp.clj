(ns benchmark.storage-amp
  "Storage-amplification study: how fast does durable storage grow relative to
   live data, per configuration and per GC mode, on the :file backend?

   One invocation runs one or more RUNS (workload x config x gc-mode), each on a
   fresh store, and writes one CSV time series per run plus one summary row per
   run to `<out-dir>/summary.csv` (appended).

   Run (one EDN run spec, or a vector of them):
     clj -J-Xmx3g -M:bench -m benchmark.storage-amp \\
       '{:workload :w1 :gc :g0 :config {} :label \"w1-default-g0\"}'

   Run spec keys
     :label     string, names the CSV and the store directory
     :workload  :w1 small app | :w2 steady medium | :w3 bulk import
     :wl        workload parameter overrides (see `default-workloads`)
     :config    {:keep-history? bool    ; default true (datahike default)
                 :crypto-hash? bool     ; default false
                 :diff-buf n            ; :index-config {:diff-buf-size n}; default 0
                 :fuse? bool            ; :fuse-index-roots? true
                 :commit-graph? bool    ; false => :commit-graph? false
                 :exclusive? bool}      ; :writer-ownership :exclusive (GC floor 0)
     :gc        :g0 none | :g1 online | :g2 background full | :g3 background
                full with history window | :g4 g1+g2 | :g4w g1+g3
                (every run additionally ends with the G5 one-off collections, see
                below, so G5 needs no run of its own)
     :gc-opts   {:interval-ms 15000 :history-window-ms 60000}

   Every run ends with three post-run steps, each sampled:
     end          the state after the load (no GC beyond the run's own mode)
     end-gc-all   (gc-storage! db (Date. 0) {:min-age-ms 0}) — keep every commit
     end-gc-now   (gc-storage! db (Date.)   {:min-age-ms 0}) — keep only the head
   then a cold reconnect that counts entities (integrity check).

   Measurement sources (per sample):
     live_*      the head's mark: `datahike.gc/reachable-in-branch` (the GC's own
                 reachability walk) seeded at the branch head with a remove-before in
                 the future, so no parent commit is walked. Bytes = sum of the files
                 those keys map to (konserve key->store-key).
     store_*     every regular file under the store directory (apparent length).
     written_*   a konserve write hook counting keys of every assoc/multi-assoc/
                 update/bassoc and stat-ing the written file right after the write
                 (exact on-disk bytes per write); wchar = /proc/self/io wchar delta
                 (all bytes the JVM passed to write(2), cross-check).
     gc_*        datahike.gc/gc-storage! and datahike.online-gc/online-gc! are
                 wrapped (alter-var-root) to count cycles, swept keys, durations;
                 the background collector itself is the real
                 `datahike.gc/start-background-gc!`.
     commit throughput = commits / sum of d/transact wall time (sampling excluded)."
  (:require [datahike.api :as d]
            [datahike.gc :as gc]
            [datahike.online-gc :as online-gc]
            [konserve.core :as k]
            [konserve.impl.defaults :as kd]
            [superv.async :refer [<?? S]]
            [clojure.core.async :as async]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.util Date Random UUID]
           [java.io File]))

;; ---------------------------------------------------------------------------
;; workloads

(def default-workloads
  {;; small app: a ~25 MB-live database, then many 1-10 datom commits
   :w1 {:seed-entities 100000 :seed-tx-entities 2000
        :commits 20000 :sample-every 1000
        :insert-ratio 0.5 :min-datoms 1 :max-datoms 10}
   ;; steady medium: ~200 MB live, 100-1000 datom batches
   :w2 {:seed-entities 800000 :seed-tx-entities 2000
        :commits 2000 :sample-every 100
        :insert-ratio 0.3 :min-datoms 100 :max-datoms 1000}
   ;; bulk import: 3M datoms in 10k-datom commits
   :w3 {:seed-entities 0 :seed-tx-entities 2000
        :commits 300 :sample-every 10
        :insert-ratio 1.0 :min-datoms 10000 :max-datoms 10000}})

(def attrs-per-entity 5)

(def schema
  [{:db/ident :e/id :db/valueType :db.type/long :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :e/name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :e/n :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :e/score :db/valueType :db.type/double :db/cardinality :db.cardinality/one}
   {:db/ident :e/tag :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])

(def ^:private tags ["alpha" "beta" "gamma" "delta" "epsilon" "zeta" "eta" "theta"])

(defn- rand-name ^String [^Random r]
  (let [sb (StringBuilder.)]
    (dotimes [_ 24] (.append sb (char (+ 97 (.nextInt r 26)))))
    (.toString sb)))

(defn- new-entity [^Random r id]
  {:e/id id :e/name (rand-name r) :e/n (long (.nextInt r 1000000))
   :e/score (.nextDouble r) :e/tag (nth tags (.nextInt r (count tags)))})

(defn- update-datom [^Random r n-entities]
  (let [id (.nextInt r (int n-entities))]
    (case (.nextInt r 3)
      0 [:db/add [:e/id id] :e/n (long (.nextInt r 1000000))]
      1 [:db/add [:e/id id] :e/score (.nextDouble r)]
      2 [:db/add [:e/id id] :e/name (rand-name r)])))

(defn- commit-tx
  "tx-data for one load commit. Returns [tx-data new-entity-count]."
  [^Random r {:keys [insert-ratio min-datoms max-datoms]} n-entities]
  (let [target (+ min-datoms (.nextInt r (int (inc (- max-datoms min-datoms)))))]
    (if (or (zero? n-entities) (< (.nextDouble r) insert-ratio))
      ;; inserts: whole entities, at least one
      (let [k (max 1 (quot target attrs-per-entity))]
        [(mapv #(new-entity r (+ n-entities %)) (range k)) k])
      [(vec (repeatedly target #(update-datom r n-entities))) 0])))

;; ---------------------------------------------------------------------------
;; config

(defn make-config [path {:keys [keep-history? crypto-hash? diff-buf fuse?
                                commit-graph? exclusive?]
                         :or {keep-history? true crypto-hash? false}}
                   gc]
  (cond-> {:store {:backend :file :path path :id (UUID/randomUUID)}
           :schema-flexibility :write
           :keep-history? keep-history?
           :crypto-hash? crypto-hash?}
    (and diff-buf (pos? diff-buf)) (assoc :index-config {:diff-buf-size diff-buf})
    fuse? (assoc :fuse-index-roots? true)
    (false? commit-graph?) (assoc :commit-graph? false)
    exclusive? (assoc :writer {:backend :self :writer-ownership :exclusive})
    (#{:g1 :g4 :g4w} gc) (assoc :online-gc {:enabled? true :grace-period-ms 0})))

;; ---------------------------------------------------------------------------
;; instrumentation

(def ^:private gc-stats (atom nil))

(defn- reset-gc-stats! []
  (reset! gc-stats {:cycles [] :inflight 0 :ogc-calls 0 :ogc-collected 0}))

(defonce ^:private install-wrappers!
  (delay
    (let [orig-gc gc/gc-storage!
          orig-ogc online-gc/online-gc!]
      (alter-var-root
       #'gc/gc-storage!
       (fn [_]
         (fn [& args]
           (swap! gc-stats update :inflight inc)
           (let [t0 (System/nanoTime)
                 ch (apply orig-gc args)]
             (async/go
               (let [r (async/<! ch)
                     dt (/ (- (System/nanoTime) t0) 1e6)]
                 (swap! gc-stats #(-> %
                                      (update :inflight dec)
                                      (update :cycles conj
                                              {:t (System/currentTimeMillis) :ms dt
                                               :swept (if (coll? r) (count r) -1)
                                               :error (when (instance? Throwable r)
                                                        (.getMessage ^Throwable r))})))
                 r))))))
      (alter-var-root
       #'online-gc/online-gc!
       (fn [_]
         (fn [& args]
           (let [r (apply orig-ogc args)
                 rec! (fn [x] (when (number? x)
                                (swap! gc-stats #(-> % (update :ogc-calls inc)
                                                     (update :ogc-collected + x)))))]
             (if (number? r)
               (do (rec! r) r)
               (async/go (let [x (async/<! r)] (rec! x) x))))))))))

(defn- wchar []
  (try
    ;; not `slurp`: /proc files report length 0 and its stream copy fails
    (some->> (java.nio.file.Files/readAllLines
              (java.nio.file.Paths/get "/proc/self/io" (into-array String [])))
             (some #(when (str/starts-with? % "wchar:") %))
             (re-find #"\d+") Long/parseLong)
    (catch Exception _ -1)))

(defn- key-file ^File [^String path k]
  (io/file path (kd/key->store-key k)))

(defn- install-write-hook! [store ^String path counters]
  (let [stat! (fn [k]
                (let [len (.length (key-file path k))]
                  (swap! counters #(-> % (update :objects inc) (update :bytes + len)))))]
    (k/add-write-hook! store ::amp
                       (fn [{:keys [api-op key kvs]}]
                         (case api-op
                           :multi-assoc (doseq [kk (if (map? kvs) (keys kvs) (map first kvs))]
                                          (stat! kk))
                           (:assoc :assoc-in :update-in :update :bassoc) (stat! key)
                           (:dissoc) (swap! counters update :dissocs inc)
                           nil)))))

(defn- dir-stats [^String path]
  (let [fs (filter #(.isFile ^File %) (file-seq (io/file path)))]
    (loop [fs fs n 0 b 0]
      (if-let [f (first fs)]
        (recur (rest fs) (inc n) (+ b (.length ^File f)))
        {:files n :bytes b}))))

(def ^:private far-future (Date. (long 4.0e12)))

(defn- live-stats
  "Head-only mark via the collector's own reachability walk."
  [conn ^String path]
  (let [db @conn
        store (:store db)
        branch (get-in db [:config :branch])
        res (@#'gc/reachable-in-branch store branch far-future (:config db)
                                       (atom {}) {:sync? true :require-head? true})
        ks (conj (into (:reachable res) (:store-refs res)) :branches)
        bytes (reduce (fn [acc k] (+ acc (.length (key-file path k)))) 0 ks)]
    {:objects (count ks) :bytes bytes}))

(defn- heap-mb []
  (let [rt (Runtime/getRuntime)]
    (/ (- (.totalMemory rt) (.freeMemory rt)) 1048576.0)))

;; ---------------------------------------------------------------------------
;; sampling + csv

(def csv-cols
  [:label :workload :config :gc :phase :commit :entities :datoms :elapsed_ms
   :tx_ms_total :live_objects :live_bytes :store_files :store_bytes :amp
   :written_objects :written_bytes :wchar_bytes :hook_dissocs
   :gc_cycles :gc_swept_total :gc_ms_total :ogc_collected_total :heap_mb])

(defn- csv-line [m] (str/join "," (map #(let [v (get m %)]
                                          (cond (nil? v) ""
                                                (float? v) (format "%.3f" (double v))
                                                :else (str v)))
                                       csv-cols)))

(defn- take-sample [{:keys [conn path counters wchar0 t0 run]} phase commit entities tx-ns]
  (let [live (live-stats conn path)
        st (dir-stats path)
        {:keys [cycles ogc-collected]} @gc-stats
        c @counters]
    (merge run
           {:phase (name phase) :commit commit :entities entities
            :datoms (* attrs-per-entity entities)
            :elapsed_ms (- (System/currentTimeMillis) t0)
            :tx_ms_total (/ tx-ns 1e6)
            :live_objects (:objects live) :live_bytes (:bytes live)
            :store_files (:files st) :store_bytes (:bytes st)
            :amp (if (pos? (:bytes live)) (/ (double (:bytes st)) (:bytes live)) 0.0)
            :written_objects (:objects c) :written_bytes (:bytes c)
            :wchar_bytes (- (wchar) wchar0) :hook_dissocs (:dissocs c)
            :gc_cycles (count cycles) :gc_swept_total (reduce + 0 (map :swept cycles))
            :gc_ms_total (reduce + 0.0 (map :ms cycles))
            :ogc_collected_total ogc-collected
            :heap_mb (heap-mb)})))

(defn- config-label [{:keys [keep-history? crypto-hash? diff-buf fuse? commit-graph? exclusive?]
                      :or {keep-history? true crypto-hash? false}}]
  (str "kh" (if keep-history? 1 0) "-ch" (if crypto-hash? 1 0) "-db" (or diff-buf 0)
       (when fuse? "-fuse") (when (false? commit-graph?) "-nocg") (when exclusive? "-excl")))

;; ---------------------------------------------------------------------------
;; a run

(defn run-one!
  [{:keys [label workload wl config gc gc-opts store-root out-dir seed]
    :or {gc :g0 config {} seed 42}}]
  @install-wrappers!
  (reset-gc-stats!)
  (let [wl (merge (default-workloads workload) wl)
        {:keys [seed-entities seed-tx-entities commits sample-every]} wl
        {:keys [interval-ms history-window-ms] :or {interval-ms 15000}} gc-opts
        history-window-ms (if (#{:g3 :g4w} gc) (or history-window-ms 60000) nil)
        label (or label (str (name workload) "-" (config-label config) "-" (name gc)))
        path (str store-root "/" label)
        csv-file (io/file out-dir (str label ".csv"))
        summary-file (io/file out-dir "summary.csv")
        cfg (make-config path config gc)
        run {:label label :workload (name workload) :config (config-label config) :gc (name gc)}
        r (Random. seed)]
    (io/make-parents csv-file)
    (println "== run" label (pr-str cfg) (pr-str wl) "gc-opts" interval-ms history-window-ms)
    (when (.exists (io/file path)) (d/delete-database cfg))
    (let [created (try (d/create-database cfg) (d/connect cfg) (catch Throwable e e))]
      (if (instance? Throwable created)
        (let [msg (str "refused: " (.getMessage ^Throwable created))]
          (println "  " msg)
          (spit summary-file (str label "," (name workload) "," (config-label config) "," (name gc)
                                  ",REFUSED," (pr-str (str/replace msg "," ";")) "\n") :append true)
          (try (d/delete-database cfg) (catch Throwable _))
          {:label label :refused msg})
        (let [conn created
              store (:store @conn)
              counters (atom {:objects 0 :bytes 0 :dissocs 0})
              _ (install-write-hook! store path counters)
              ctx {:conn conn :path path :counters counters :wchar0 (wchar)
                   :t0 (System/currentTimeMillis) :run run}
              rows (atom [])
              sample! (fn [phase commit entities tx-ns]
                        (let [s (take-sample ctx phase commit entities tx-ns)]
                          (swap! rows conj s)
                          (println (format "  %-10s c=%6d ent=%8d live=%7.1fMB/%6d  store=%8.1fMB/%7d amp=%6.2f wr=%8.1fMB gc=%d/%d ogc=%d heap=%.0fMB"
                                           (:phase s) commit entities (/ (:live_bytes s) 1e6) (:live_objects s)
                                           (/ (:store_bytes s) 1e6) (:store_files s) (:amp s)
                                           (/ (:written_bytes s) 1e6) (:gc_cycles s) (:gc_swept_total s)
                                           (:ogc_collected_total s) (:heap_mb s)))
                          (flush)
                          s))
              _ (d/transact conn schema)
              ;; background full GC (G2/G3/G4) starts before the seed, like a
              ;; deployment that runs it from day one
              bg-stop (when (#{:g2 :g3 :g4 :g4w} gc)
                        (gc/start-background-gc! conn {:interval-ms interval-ms
                                                       :history-window-ms history-window-ms}))
              tx-ns (atom 0)
              transact! (fn [data]
                          (let [t (System/nanoTime)]
                            (d/transact conn data)
                            (swap! tx-ns + (- (System/nanoTime) t))))
              ;; seed in bulk commits
              _ (loop [n 0]
                  (when (< n seed-entities)
                    (let [k (min seed-tx-entities (- seed-entities n))]
                      (transact! (mapv #(new-entity r (+ n %)) (range k)))
                      (recur (+ n k)))))
              seed-sample (sample! :seed 0 seed-entities @tx-ns)
              seed-tx-ns @tx-ns
              entities (loop [i 0 entities seed-entities]
                         (if (< i commits)
                           (let [[data k] (commit-tx r wl entities)]
                             (transact! data)
                             (let [entities (+ entities k)
                                   i (inc i)]
                               (when (zero? (mod i sample-every))
                                 (sample! :load i entities @tx-ns))
                               (recur i entities)))
                           entities))
              _ (when bg-stop (bg-stop))
              ;; let an in-flight background cycle finish before the end samples
              _ (loop [w 0]
                  (when (and (< w 6000) (pos? (:inflight @gc-stats)))
                    (Thread/sleep 100)
                    (recur (inc w))))
              load-tx-ns (- @tx-ns seed-tx-ns)
              cycles-during-run (:cycles @gc-stats)
              end (sample! :end commits entities @tx-ns)
              t-all (System/nanoTime)
              swept-all (count (<?? S (gc/gc-storage! @conn (Date. 0) {:min-age-ms 0})))
              gc-all-ms (/ (- (System/nanoTime) t-all) 1e6)
              end-all (sample! :end-gc-all commits entities @tx-ns)
              t-now (System/nanoTime)
              swept-now (count (<?? S (gc/gc-storage! @conn (Date.) {:min-age-ms 0})))
              gc-now-ms (/ (- (System/nanoTime) t-now) 1e6)
              end-now (sample! :end-gc-now commits entities @tx-ns)
              _ (k/remove-write-hook! store ::amp)
              _ (d/release conn)
              ;; integrity: cold reconnect and count
              cold (try
                     (let [c2 (d/connect cfg)
                           n (d/q '[:find (count ?e) . :where [?e :e/id _]] @c2)]
                       (d/release c2)
                       (if (= n entities) "ok" (str "COUNT-MISMATCH " n " expected " entities)))
                     (catch Throwable e (str "ERROR " (.getMessage e))))
              load-phase (filter #(= "load" (:phase %)) @rows)
              growth-per-commit (when (seq load-phase)
                                  (/ (double (- (:store_bytes end) (:store_bytes seed-sample)))
                                     (max 1 commits)))
              written-per-commit (/ (double (- (:written_bytes end) (:written_bytes seed-sample)))
                                    (max 1 commits))
              live-growth-per-commit (/ (double (- (:live_bytes end) (:live_bytes seed-sample)))
                                        (max 1 commits))
              run-ms (map :ms cycles-during-run)
              summary (merge run
                             {:commits commits
                              :load_commits_per_s (if (pos? load-tx-ns) (/ commits (/ load-tx-ns 1e9)) 0.0)
                              :seed_s (/ seed-tx-ns 1e9)
                              :live_mb_seed (/ (:live_bytes seed-sample) 1e6)
                              :live_mb_end (/ (:live_bytes end) 1e6)
                              :store_mb_seed (/ (:store_bytes seed-sample) 1e6)
                              :store_mb_end (/ (:store_bytes end) 1e6)
                              :files_end (:store_files end)
                              :amp_seed (:amp seed-sample)
                              :amp_end (:amp end)
                              :store_growth_b_per_commit growth-per-commit
                              :live_growth_b_per_commit live-growth-per-commit
                              :written_b_per_commit written-per-commit
                              :written_mb_total (/ (:written_bytes end) 1e6)
                              :wchar_mb_total (/ (:wchar_bytes end) 1e6)
                              :run_gc_cycles (count cycles-during-run)
                              :run_gc_swept (reduce + 0 (map :swept cycles-during-run))
                              :run_gc_ms_mean (if (seq run-ms) (/ (reduce + run-ms) (count run-ms)) 0.0)
                              :run_gc_ms_max (if (seq run-ms) (apply max run-ms) 0.0)
                              :run_gc_errors (count (filter :error cycles-during-run))
                              :ogc_collected (:ogc-collected @gc-stats)
                              :end_gc_all_swept swept-all :end_gc_all_ms gc-all-ms
                              :store_mb_after_gc_all (/ (:store_bytes end-all) 1e6)
                              :files_after_gc_all (:store_files end-all)
                              :end_gc_now_swept swept-now :end_gc_now_ms gc-now-ms
                              :store_mb_after_gc_now (/ (:store_bytes end-now) 1e6)
                              :files_after_gc_now (:store_files end-now)
                              :amp_after_gc_now (:amp end-now)
                              :heap_mb_end (:heap_mb end)
                              :integrity cold
                              :history_window_ms history-window-ms
                              :interval_ms (when bg-stop interval-ms)})
              summary-cols [:label :workload :config :gc :commits :load_commits_per_s :seed_s
                            :live_mb_seed :live_mb_end :store_mb_seed :store_mb_end :files_end
                            :amp_seed :amp_end :store_growth_b_per_commit :live_growth_b_per_commit
                            :written_b_per_commit :written_mb_total :wchar_mb_total
                            :run_gc_cycles :run_gc_swept :run_gc_ms_mean :run_gc_ms_max :run_gc_errors
                            :ogc_collected :end_gc_all_swept :end_gc_all_ms :store_mb_after_gc_all
                            :files_after_gc_all :end_gc_now_swept :end_gc_now_ms :store_mb_after_gc_now
                            :files_after_gc_now :amp_after_gc_now :heap_mb_end :integrity
                            :history_window_ms :interval_ms]]
          (spit csv-file (str (str/join "," (map name csv-cols)) "\n"
                              (str/join "\n" (map csv-line @rows)) "\n"))
          (when-not (.exists summary-file)
            (spit summary-file (str (str/join "," (map name summary-cols)) "\n")))
          (spit summary-file (str (str/join "," (map #(let [v (get summary %)]
                                                        (cond (nil? v) ""
                                                              (float? v) (format "%.3f" (double v))
                                                              :else (str/replace (str v) "," ";")))
                                                     summary-cols)) "\n")
                :append true)
          (println "  summary" (pr-str summary))
          ;; free the disk: stores are measured, not kept
          (when-not (:keep-store? wl)
            (try (d/delete-database cfg) (catch Throwable e (println "  delete failed" (.getMessage e)))))
          summary)))))

(defn -main [& args]
  (let [spec (edn/read-string (first args))
        specs (if (map? spec) [spec] spec)
        store-root (or (System/getenv "AMP_STORE_ROOT") "/tmp/dh-storage-amp/stores")
        out-dir (or (System/getenv "AMP_OUT_DIR") "/tmp/dh-storage-amp/out")]
    (doseq [s specs]
      (try
        (run-one! (merge {:store-root store-root :out-dir out-dir} s))
        (catch Throwable e
          (println "RUN FAILED" (:label s) (.getMessage e))
          (.printStackTrace e)
          ;; never leave a failed run's store behind
          (when-let [l (:label s)]
            (let [dir (io/file store-root l)]
              (when (.exists dir)
                (doseq [f (reverse (file-seq dir))] (.delete ^File f))))))))
    (shutdown-agents)
    (System/exit 0)))
