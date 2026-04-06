(ns benchmark.souffle-bench
  "Soufflé (compiled Datalog) benchmark for recursive rules.
   Generates CSV fact files, compiles+runs Soufflé programs, measures wall-clock time.

   Usage:
     clj -M:bench-sql -m benchmark.souffle-bench"
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [benchmark.datascript-bench :as dsb]
   [datahike.api :as d]
   [datahike.query :as q]
   [replikativ.logging :as log])
  (:import [java.io File]))

(def souffle-bin (or (System/getenv "SOUFFLE_BIN") "souffle"))

(def tc-program
  "// Transitive closure
.decl follows(x:number, y:number)
.input follows

.decl tc(x:number, y:number)
tc(x, y) :- follows(x, y).
tc(x, y) :- tc(x, z), follows(z, y).

.output tc
")

;; ---------------------------------------------------------------------------
;; Data generation (same shapes as datascript_bench)

(defn- write-follows-csv!
  "Write follows(x,y) facts as tab-separated CSV for Soufflé."
  [^File dir tx-data]
  (with-open [w (io/writer (io/file dir "follows.facts"))]
    (doseq [entity tx-data
            :when (:follows entity)]
      (let [from-id (Long/parseLong (:db/id entity))
            to-id (Long/parseLong (:follows entity))]
        (.write w (str from-id "\t" to-id "\n"))))))

(defn- run-souffle!
  "Compile and run a Soufflé program. Returns {:time-ms :result-count}."
  [^String program-text ^File fact-dir ^String label]
  (let [dl-file (io/file fact-dir (str label ".dl"))
        out-dir (io/file fact-dir "out")]
    (.mkdirs out-dir)
    (spit dl-file program-text)

    ;; Compile to C++ and run (Soufflé -c flag)
    ;; Use interpreted mode (-) for benchmarking since compile overhead is one-time
    ;; Actually, use compiled mode for fair comparison — Soufflé's strength is compilation
    (let [;; First compile
          compile-result (let [pb (ProcessBuilder.
                                   [souffle-bin "-c" "-o" (.getPath (io/file fact-dir (str label "_bin")))
                                    (.getPath dl-file)
                                    "-F" (.getPath fact-dir)
                                    "-D" (.getPath out-dir)])
                               _ (.redirectErrorStream pb true)
                               proc (.start pb)]
                           (let [output (slurp (.getInputStream proc))
                                 exit (.waitFor proc)]
                             {:exit exit :output output}))
          binary (io/file fact-dir (str label "_bin"))]
      (if (and (zero? (:exit compile-result)) (.exists binary))
        ;; Run compiled binary, measure time
        (let [times (for [_ (range 5)]
                      (let [start (System/nanoTime)
                            pb (ProcessBuilder.
                                 [(.getPath binary)
                                  "-F" (.getPath fact-dir)
                                  "-D" (.getPath out-dir)])
                            _ (.redirectErrorStream pb true)
                            proc (.start pb)
                            output (slurp (.getInputStream proc))
                            exit (.waitFor proc)
                            elapsed (/ (- (System/nanoTime) start) 1e6)]
                        (when (not (zero? exit))
                          (println "  Soufflé run error:" output))
                        elapsed))
              result-file (io/file out-dir "tc.csv")
              result-count (when (.exists result-file)
                             (with-open [r (io/reader result-file)]
                               (count (line-seq r))))]
          {:time-ms (nth (sort times) 2) ;; median of 5
           :result-count result-count})
        ;; Compilation failed — try interpreted mode
        (do
          (println "  Soufflé compile failed:" (:output compile-result))
          (println "  Trying interpreted mode...")
          (let [times (for [_ (range 5)]
                        (let [start (System/nanoTime)
                              pb (ProcessBuilder.
                                   [souffle-bin (.getPath dl-file)
                                    "-F" (.getPath fact-dir)
                                    "-D" (.getPath out-dir)])
                              _ (.redirectErrorStream pb true)
                              proc (.start pb)
                              output (slurp (.getInputStream proc))
                              exit (.waitFor proc)
                              elapsed (/ (- (System/nanoTime) start) 1e6)]
                          (when (not (zero? exit))
                            (println "  Soufflé interp error:" output))
                          elapsed))
                result-file (io/file out-dir "tc.csv")
                result-count (when (.exists result-file)
                               (with-open [r (io/reader result-file)]
                                 (count (line-seq r))))]
            {:time-ms (nth (sort times) 2)
             :result-count result-count}))))))

;; ---------------------------------------------------------------------------
;; Benchmark runner

(defn run-rules
  "Compare recursive rule performance: Datahike vs Soufflé."
  []
  (alter-var-root #'q/*query-result-cache?* (constantly false))
  (println "\n=== RECURSIVE RULE BENCHMARKS: Datahike vs Soufflé ===")
  (println (str "  Query planner: " (System/getenv "DATAHIKE_QUERY_PLANNER")))

  (let [rule-query '[:find ?e ?e2
                      :in $ %
                      :where (follows ?e ?e2)]
        recursive-rule dsb/recursive-rule]

    (println (format "\n%-16s %-30s %10s %10s %10s  %s"
                     "Benchmark" "Description"
                     "Datahike" "Soufflé" "Ratio" "Winner"))
    (println (apply str (repeat 95 "-")))

    (doseq [rname dsb/rule-order]
      (let [{:keys [desc data-fn]} (get dsb/rule-benchmarks rname)
            tx-data (data-fn)

            ;; Datahike
            dh-conn (dsb/dh-empty-db)
            _ (d/transact dh-conn {:tx-data tx-data})
            dh-db @dh-conn
            _ (d/release dh-conn)

            dh-result (d/q rule-query dh-db recursive-rule)
            dh-count (count dh-result)

            ;; Warmup DH
            _ (dotimes [_ 50] (d/q rule-query dh-db recursive-rule))
            dh-ms (dsb/bench (d/q rule-query dh-db recursive-rule))

            ;; Soufflé
            fact-dir (io/file (str "/tmp/souffle-bench/" (name rname)))
            _ (.mkdirs fact-dir)
            _ (write-follows-csv! fact-dir tx-data)
            sf-result (run-souffle! tc-program fact-dir "tc")
            sf-ms (:time-ms sf-result)
            sf-count (:result-count sf-result)

            ratio (when (and dh-ms sf-ms (pos? sf-ms))
                    (/ dh-ms sf-ms))
            winner (cond
                     (nil? sf-ms) "DATAHIKE"
                     (< dh-ms sf-ms) "DATAHIKE"
                     :else "SOUFFLE")]
        (println (format "%-16s %-30s %9sms %9sms %9sx  %s"
                         (name rname) desc
                         (dsb/round dh-ms)
                         (if sf-ms (dsb/round sf-ms) "N/A")
                         (if ratio (format "%.1f" (double ratio)) "N/A")
                         winner))
        (println (format "  Results: DH=%d SF=%s%s"
                         dh-count
                         (or sf-count "?")
                         (if (and sf-count (= dh-count sf-count)) " ✓" " ✗"))))))

  (println "\nDone."))

(defn -main [& _]
  (run-rules)
  (System/exit 0))
