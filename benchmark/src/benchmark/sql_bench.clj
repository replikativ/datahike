(ns benchmark.sql-bench
  "Comparative benchmarks: Datahike vs PostgreSQL vs SQLite.
   Uses the same 20K people dataset as datascript-bench for fair comparison.

   Usage:
     DATAHIKE_COMPILED_QUERY=true clj -J--add-modules -Jjdk.incubator.vector -J-XX:UseAVX=2 \\
       -M:bench-sql -m benchmark.sql-bench [queries|aggregates|all]

   Requires: PostgreSQL running on localhost:5432 with user/password bench access."
  (:require
   [datahike.api :as d]
   [datahike.query :as q]
   [benchmark.datascript-bench :as dsb]
   [replikativ.logging :as log])
  (:import [java.sql DriverManager Connection PreparedStatement ResultSet]))

;; ---------------------------------------------------------------------------
;; Timing (reuse from datascript-bench)

(defmacro bench [& body]
  `(dsb/bench ~@body))

;; ---------------------------------------------------------------------------
;; JDBC helpers

(defn jdbc-conn ^Connection [url]
  (DriverManager/getConnection url))

(defn jdbc-execute! [^Connection conn ^String sql]
  (with-open [stmt (.createStatement conn)]
    (.execute stmt sql)))

(defn jdbc-query-count
  "Execute query via PreparedStatement, return row count."
  [^Connection conn ^String sql]
  (with-open [stmt (.prepareStatement conn sql)
              rs (.executeQuery stmt)]
    (loop [n 0]
      (if (.next rs) (recur (inc n)) n))))

(defn jdbc-query-consume
  "Execute query, consume all rows (force materialization). Returns nil."
  [^Connection conn ^String sql]
  (with-open [stmt (.prepareStatement conn sql)
              rs (.executeQuery stmt)]
    (let [ncols (.getColumnCount (.getMetaData rs))]
      (loop []
        (when (.next rs)
          (dotimes [i ncols]
            (.getObject rs (unchecked-inc-int i)))
          (recur))))))

(defn jdbc-query-single
  "Execute query returning a single-row result, return the row as a vector."
  [^Connection conn ^String sql]
  (with-open [stmt (.prepareStatement conn sql)
              rs (.executeQuery stmt)]
    (when (.next rs)
      (let [ncols (.getColumnCount (.getMetaData rs))]
        (mapv #(.getObject rs (unchecked-inc-int %)) (range ncols))))))

;; ---------------------------------------------------------------------------
;; Schema + data loading

(def create-table-sql
  "CREATE TABLE IF NOT EXISTS people (
     id INTEGER PRIMARY KEY,
     name TEXT NOT NULL,
     last_name TEXT NOT NULL,
     sex TEXT NOT NULL,
     age INTEGER NOT NULL,
     salary INTEGER NOT NULL
   )")

(def create-follows-sql
  "CREATE TABLE IF NOT EXISTS follows (
     follower_id INTEGER NOT NULL,
     followee_id INTEGER NOT NULL,
     PRIMARY KEY (follower_id, followee_id)
   )")

(def create-indexes-sql
  ["CREATE INDEX IF NOT EXISTS idx_people_name ON people(name)"
   "CREATE INDEX IF NOT EXISTS idx_people_sex ON people(sex)"
   "CREATE INDEX IF NOT EXISTS idx_people_age ON people(age)"
   "CREATE INDEX IF NOT EXISTS idx_people_salary ON people(salary)"
   "CREATE INDEX IF NOT EXISTS idx_people_last_name ON people(last_name)"
   "CREATE INDEX IF NOT EXISTS idx_follows_follower ON follows(follower_id)"
   "CREATE INDEX IF NOT EXISTS idx_follows_followee ON follows(followee_id)"])

(defn load-people! [^Connection conn people]
  (jdbc-execute! conn "DELETE FROM people")
  (jdbc-execute! conn "DELETE FROM follows")
  (with-open [ps (.prepareStatement conn
                   "INSERT INTO people (id, name, last_name, sex, age, salary) VALUES (?, ?, ?, ?, ?, ?)")]
    (doseq [p people]
      (let [id (Long/parseLong (:db/id p))]
        (.setLong ps 1 id)
        (.setString ps 2 (:name p))
        (.setString ps 3 (:last-name p))
        (.setString ps 4 (clojure.core/name (:sex p)))
        (.setLong ps 5 (:age p))
        (.setLong ps 6 (:salary p))
        (.addBatch ps)))
    (.executeBatch ps))
  (with-open [ps (.prepareStatement conn
                   "INSERT INTO follows (follower_id, followee_id) VALUES (?, ?)")]
    (doseq [p people
            :when (:follows p)]
      (let [fid (Long/parseLong (:db/id p))
            tid (Long/parseLong (:follows p))]
        (.setLong ps 1 fid)
        (.setLong ps 2 tid)
        (.addBatch ps)))
    (.executeBatch ps)))

;; ---------------------------------------------------------------------------
;; SQL equivalents of benchmark queries

(def sql-queries
  {:q1          "SELECT id FROM people WHERE name = 'Ivan'"
   :q2          "SELECT p.id, p.age FROM people p WHERE p.name = 'Ivan'"
   :q3          "SELECT p.id, p.age FROM people p WHERE p.name = 'Ivan' AND p.sex = 'male'"
   :q4          "SELECT p.id, p.last_name, p.age FROM people p WHERE p.name = 'Ivan' AND p.sex = 'male'"
   :q5          (str "SELECT p2.id, p2.last_name, p1.age FROM people p1 "
                     "JOIN people p2 ON p1.age = p2.age "
                     "WHERE p1.name = 'Ivan'")
   :qpred1      "SELECT id, salary FROM people WHERE salary > 50000"
   :qpred2      "SELECT id, salary FROM people WHERE salary > 50000"
   :q-or        "SELECT id FROM people WHERE name = 'Ivan' OR name = 'Petr'"
   :q-not       "SELECT id, age FROM people WHERE sex != 'male'"
   :q-pred-range "SELECT id, salary FROM people WHERE salary > 50000 AND salary < 80000"
   :q-5-merge   "SELECT id, name, last_name, age, salary FROM people WHERE sex = 'male'"
   :q-rule      (str "SELECT f.follower_id, f.followee_id FROM follows f")})

(def sql-agg-queries
  {:q-agg-avg    "SELECT AVG(salary) FROM people"
   :q-agg-group  "SELECT sex, AVG(salary), COUNT(*) FROM people GROUP BY sex"
   :q-agg-filter "SELECT AVG(salary), MIN(salary), MAX(salary) FROM people WHERE sex = 'male'"
   :q-agg-pred   "SELECT sex, AVG(salary) FROM people WHERE salary > 50000 GROUP BY sex"
   :q-agg-multi  "SELECT sex, name, AVG(salary) FROM people GROUP BY sex, name"
   :q-agg-stats  (str "SELECT AVG(salary), VARIANCE(salary), STDDEV(salary), "
                       "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) FROM people")})

;; SQLite doesn't have VARIANCE/STDDEV/PERCENTILE_CONT built-in
(def sqlite-agg-queries
  (assoc sql-agg-queries
    :q-agg-stats "SELECT AVG(salary), AVG(salary*salary) - AVG(salary)*AVG(salary), 0, 0 FROM people"))

;; ---------------------------------------------------------------------------
;; Recursive rule SQL (WITH RECURSIVE)

(def sql-recursive
  "WITH RECURSIVE tc(x, y) AS (
     SELECT follower_id, followee_id FROM follows
     UNION
     SELECT tc.x, f.followee_id FROM tc JOIN follows f ON tc.y = f.follower_id
   )
   SELECT x, y FROM tc")

;; ---------------------------------------------------------------------------
;; Result formatting

(defn pad [s width]
  (let [s (str s)]
    (if (< (count s) width)
      (str (apply str (repeat (- width (count s)) " ")) s)
      s)))

(defn round [n]
  (cond
    (> n 1)    (format "%.1f" (double n))
    (> n 0.01) (format "%.2f" (double n))
    :else      (format "%.3f" (double n))))

(defn print-header [& {:keys [stratum?] :or {stratum? false}}]
  (println)
  (let [compiled? (= "true" (System/getenv "DATAHIKE_COMPILED_QUERY"))
        dh-label (if compiled? "DH-compil" "DH-legacy")]
    (if stratum?
      (do
        (println (format "%-12s %-36s %10s %10s %10s %10s  %s"
                         "Benchmark" "Description"
                         dh-label "DH+strat" "Postgres" "SQLite" "Winner"))
        (println (apply str (repeat 115 "-"))))
      (do
        (println (format "%-12s %-36s %10s %10s %10s  %s"
                         "Benchmark" "Description"
                         dh-label "Postgres" "SQLite" "Winner"))
        (println (apply str (repeat 105 "-")))))))

(defn print-row
  ([bench-name desc dh-ms pg-ms sq-ms]
   (print-row bench-name desc dh-ms nil pg-ms sq-ms))
  ([bench-name desc dh-ms dh-st-ms pg-ms sq-ms]
   (let [times (remove nil? [dh-ms dh-st-ms pg-ms sq-ms])
         best (when (seq times) (apply min times))
         tag (fn [ms]
               (if (nil? ms) (pad "N/A" 10)
                   (let [s (pad (round ms) 10)]
                     (if (= ms best) (str s "*") (str s " ")))))
         winner (cond
                  (= best dh-ms)    "DATAHIKE"
                  (= best dh-st-ms) "DH+STRATUM"
                  (= best pg-ms)    "postgres"
                  (= best sq-ms)    "sqlite"
                  :else             "?")]
     (if dh-st-ms
       (println (format "%-12s %-36s %10s %10s %10s %10s  %s"
                        (name bench-name) desc
                        (tag dh-ms) (tag dh-st-ms) (tag pg-ms) (tag sq-ms) winner))
       (println (format "%-12s %-36s %10s %10s %10s  %s"
                        (name bench-name) desc
                        (tag dh-ms) (tag pg-ms) (tag sq-ms) winner))))))

;; ---------------------------------------------------------------------------
;; Run benchmarks

(defn setup-sql-db [^Connection conn]
  (jdbc-execute! conn create-table-sql)
  (jdbc-execute! conn create-follows-sql)
  (doseq [idx create-indexes-sql]
    (jdbc-execute! conn idx))
  (load-people! conn dsb/people20k)
  ;; Analyze for query planner
  (try (jdbc-execute! conn "ANALYZE") (catch Exception _)))

(defn run-queries
  "Run query benchmarks: Datahike vs PostgreSQL vs SQLite."
  []
  (alter-var-root #'q/*query-result-cache?* (constantly false))
  (println "\n=== SQL QUERY BENCHMARKS ===")
  (println (str "  Compiled query engine: " (System/getenv "DATAHIKE_COMPILED_QUERY")))

  ;; Datahike
  (let [dh-conn (dsb/dh-db-with-people)
        dh-db (let [db @dh-conn] (d/release dh-conn) db)]
    (println "  Datahike DB ready.")

    ;; PostgreSQL
    (let [pg-conn (try
                    (let [pg-url (or (System/getenv "BENCH_PG_URL")
                                     "jdbc:postgresql://localhost:5432/postgres")
                          c (jdbc-conn pg-url)]
                      (setup-sql-db c)
                      (println "  PostgreSQL ready.")
                      c)
                    (catch Exception e
                      (println "  PostgreSQL not available:" (.getMessage e))
                      nil))
          ;; SQLite in-memory
          sq-conn (try
                    (let [c (jdbc-conn "jdbc:sqlite::memory:")]
                      (setup-sql-db c)
                      (println "  SQLite ready.")
                      c)
                    (catch Exception e
                      (println "  SQLite not available:" (.getMessage e))
                      nil))]

      ;; JIT warmup
      (println "  JIT pre-warmup...")
      (let [qorder [:q1 :q2 :q3 :q4 :q5 :qpred1 :qpred2 :q-or :q-not :q-pred-range :q-5-merge :q-rule]]
        (doseq [qname qorder]
          (let [{:keys [query args]} (get dsb/queries qname)
                qargs (or args [])]
            (try (dotimes [_ 200] (apply d/q query dh-db qargs))
                 (catch Exception _)))))
      (println "  JIT warmup done.")

      (print-header)

      (let [qorder [:q1 :q2 :q3 :q4 :q5 :qpred1 :qpred2 :q-or :q-not :q-pred-range :q-5-merge :q-rule]]
        (doseq [qname qorder]
          (let [{:keys [desc query args]} (get dsb/queries qname)
                qargs (or args [])
                sql (get sql-queries qname)

                ;; Validate counts
                dh-count (count (apply d/q query dh-db qargs))
                pg-count (when (and pg-conn sql) (jdbc-query-count pg-conn sql))
                sq-count (when (and sq-conn sql) (jdbc-query-count sq-conn sql))

                _ (when (and pg-count (not= dh-count pg-count))
                    (println (format "  ⚠ RESULT MISMATCH %s: DH=%d PG=%d" (name qname) dh-count pg-count)))

                ;; Bench
                dh-ms (bench (apply d/q query dh-db qargs))
                pg-ms (when (and pg-conn sql) (bench (jdbc-query-consume pg-conn sql)))
                sq-ms (when (and sq-conn sql) (bench (jdbc-query-consume sq-conn sql)))]
            (print-row qname desc dh-ms pg-ms sq-ms)
            (println (format "  Results: DH=%d%s%s" dh-count
                             (if pg-count (format " PG=%d%s" pg-count
                                                  (if (= dh-count pg-count) " ✓" " ✗")) "")
                             (if sq-count (format " SQ=%d%s" sq-count
                                                  (if (= dh-count sq-count) " ✓" " ✗")) ""))))))

      ;; Cleanup
      (when sq-conn (.close sq-conn))
      (when pg-conn (.close pg-conn))
      (println "\nDone."))))

(defn run-aggregates
  "Run aggregate benchmarks: Datahike (PSS + stratum) vs PostgreSQL vs SQLite."
  []
  (alter-var-root #'q/*query-result-cache?* (constantly false))
  (println "\n=== SQL AGGREGATE BENCHMARKS ===")
  (println (str "  Compiled query engine: " (System/getenv "DATAHIKE_COMPILED_QUERY")))

  ;; DH PSS
  (let [dh-conn-tmp (dsb/dh-db-with-people)
        dh-db (let [db @dh-conn-tmp] (d/release dh-conn-tmp) db)]
    (println "  Datahike (PSS) ready.")

    ;; DH stratum
    (let [dh-stratum-db (try
                          (let [conn (dsb/dh-stratum-db-with-people)
                                db @conn]
                            (d/release conn)
                            (println "  Datahike (stratum) ready.")
                            db)
                          (catch Throwable e
                            (println "  Stratum not available:" (.getMessage e))
                            nil))
          ;; PostgreSQL
          pg-conn (try
                    (let [pg-url (or (System/getenv "BENCH_PG_URL")
                                     "jdbc:postgresql://localhost:5432/postgres")
                          c (jdbc-conn pg-url)]
                      (setup-sql-db c)
                      (println "  PostgreSQL ready.")
                      c)
                    (catch Exception e
                      (println "  PostgreSQL not available:" (.getMessage e))
                      nil))
          ;; SQLite
          sq-conn (try
                    (let [c (jdbc-conn "jdbc:sqlite::memory:")]
                      (setup-sql-db c)
                      (println "  SQLite ready.")
                      c)
                    (catch Exception e
                      (println "  SQLite not available:" (.getMessage e))
                      nil))]

      (print-header :stratum? true)

      (doseq [qname dsb/agg-order]
        (let [{:keys [desc query]} (get dsb/agg-queries qname)
              pg-sql (get sql-agg-queries qname)
              sq-sql (get sqlite-agg-queries qname)

              ;; DH PSS
              dh-result (try (d/q query dh-db)
                             (catch Exception e
                               (println (format "  ⚠ DH error %s: %s" (name qname) (.getMessage e)))
                               nil))
              ;; DH stratum
              dh-st-result (when dh-stratum-db
                             (try (d/q query dh-stratum-db)
                                  (catch Exception e
                                    (println (format "  ⚠ DH+strat error %s: %s" (name qname) (.getMessage e)))
                                    nil)))

              dh-ms (when dh-result (bench (d/q query dh-db)))
              dh-st-ms (when dh-st-result (bench (d/q query dh-stratum-db)))
              pg-ms (when (and pg-conn pg-sql)
                      (bench (jdbc-query-consume pg-conn pg-sql)))
              sq-ms (when (and sq-conn sq-sql)
                      (bench (jdbc-query-consume sq-conn sq-sql)))]
          (print-row qname desc dh-ms dh-st-ms pg-ms sq-ms)
          (println (format "  Results: DH=%s DH+st=%s"
                           (if dh-result (str (count dh-result)) "err")
                           (if dh-st-result (str (count dh-st-result)) "N/A")))))

      (when sq-conn (.close sq-conn))
      (when pg-conn (.close pg-conn))
      (println "\nDone."))))

(defn run-all []
  (run-queries)
  (run-aggregates))

(defn -main [& args]
  ;; replikativ.logging defaults to :warn
  (let [cmd (or (first args) "all")]
    (case cmd
      "queries"    (run-queries)
      "aggregates" (run-aggregates)
      "souffle"    (do (require 'benchmark.souffle-bench)
                       ((resolve 'benchmark.souffle-bench/run-rules)))
      "all"        (do (run-all)
                       (require 'benchmark.souffle-bench)
                       ((resolve 'benchmark.souffle-bench/run-rules)))
      (do (println (str "Unknown command: " cmd))
          (println "Usage: ... -m benchmark.sql-bench [queries|aggregates|souffle|all]")
          (System/exit 1))))
  (System/exit 0))
