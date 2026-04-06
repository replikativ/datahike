(ns benchmark.datascript-bench
  "Comparative benchmarks: Datahike vs Datalevin vs Datomic.
   Reproduces the datalevin/datascript benchmark suite for fair comparison.

   Usage:
     DATAHIKE_QUERY_PLANNER=true clj -M:bench-compare -m benchmark.datascript-bench [queries|writes|rules|all]

   From REPL:
     (require '[benchmark.datascript-bench :as bench])
     (bench/run-all)        ;; run all benchmarks
     (bench/run-queries)    ;; run only query benchmarks
     (bench/run-writes)     ;; run only write benchmarks"
  (:require
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.db.utils :as dbu]
   [datahike.query :as q]
   [replikativ.logging :as log])
  (:import [java.util Random]))

;; ---------------------------------------------------------------------------
;; Data generation (same as datalevin bench)

(def ^:private names      ["Ivan" "Petr" "Sergei" "Oleg" "Yuri" "Dmitry" "Fedor" "Denis"])
(def ^:private last-names ["Ivanov" "Petrov" "Sidorov" "Kovalev" "Kuznetsov" "Voronoi"])
(def ^:private sexes      [:male :female])

(def next-eid (volatile! 0))

(defn random-man []
  {:db/id     (str (vswap! next-eid inc))
   :name      (rand-nth names)
   :last-name (rand-nth last-names)
   :sex       (rand-nth sexes)
   :age       (long (rand-int 100))
   :salary    (long (rand-int 100000))})

(def people (repeatedly random-man))
(def people20k-base (vec (take 20000 (repeatedly random-man))))
;; Add :follows refs so rule benchmarks have data (~50% follow someone)
(def people20k
  (mapv (fn [p]
          (if (< (rand) 0.5)
            (assoc p :follows (str (inc (rand-int 20000))))
            p))
        people20k-base))

;; ---------------------------------------------------------------------------
;; Timing infrastructure (matches datalevin bench protocol)

(def ^:dynamic *warmup-t* 2000)
(def ^:dynamic *bench-t*  2000)
(def ^:dynamic *step*     10)
(def ^:dynamic *repeats*  5)

(defn now ^double [] (/ (System/nanoTime) 1000000.0))

(defn round [n]
  (cond
    (> n 1)    (format "%.1f" (double n))
    (> n 0.01) (format "%.2f" (double n))
    :else      (format "%.3f" (double n))))

(defn percentile [xs n]
  (let [sorted (sort xs)]
    (nth sorted (min (dec (count sorted))
                     (int (* n (count sorted)))))))

(defmacro dotime [duration & body]
  `(let [start-t# (now)
         end-t#   (+ ~duration start-t#)]
     (loop [iterations# *step*]
       (dotimes [_# *step*] ~@body)
       (let [now# (now)]
         (if (< now# end-t#)
           (recur (+ *step* iterations#))
           (double (/ (- now# start-t#) iterations#)))))))

(defmacro bench [& body]
  `(let [_#       (dotime *warmup-t* ~@body)
         results# (into []
                    (for [_# (range *repeats*)]
                      (dotime *bench-t* ~@body)))
         med#     (percentile results# 0.5)]
     med#))

(defmacro bench-10 [& body]
  `(let [_#       (dotime 2 ~@body)
         results# (into []
                    (for [_# (range *repeats*)]
                      (dotime 5 ~@body)))
         med#     (percentile results# 0.5)]
     med#))

(defmacro bench-once [& body]
  `(let [start-t# (now)]
     ~@body
     (- (now) start-t#)))

;; ---------------------------------------------------------------------------
;; Datahike setup

(def dh-schema
  [{:db/ident       :name
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index       true}
   {:db/ident       :last-name
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index       true}
   {:db/ident       :sex
    :db/valueType   :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/index       true}
   {:db/ident       :age
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one
    :db/index       true}
   {:db/ident       :salary
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one
    :db/index       true}
   {:db/ident       :follows
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/many}])

(defn dh-empty-db []
  (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
             :schema-flexibility :write
             :keep-history? false
             :attribute-refs? true
             :search-cache-size 0
             :index :datahike.index/persistent-set}]
    (d/delete-database cfg)
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn {:tx-data dh-schema})
      conn)))

(defn dh-db-with-people
  "Create a datahike connection with people20k loaded."
  []
  (let [conn (dh-empty-db)]
    (d/transact conn {:tx-data people20k})
    conn))

;; ---------------------------------------------------------------------------
;; Datalevin setup (loaded dynamically)

(defn dl-require! []
  (require '[datalevin.core :as dl]))

(def dl-schema
  {:follows   {:db/valueType   :db.type/ref
               :db/cardinality :db.cardinality/many}
   :name      {:db/valueType :db.type/string}
   :last-name {:db/valueType :db.type/string}
   :sex       {:db/valueType :db.type/keyword}
   :age       {:db/valueType :db.type/long}
   :salary    {:db/valueType :db.type/long}})

(def dl-opts
  {:wal?    false
   :kv-opts {:inmemory? true
             :wal?      false}})

(defn dl-empty-db []
  ((resolve 'datalevin.core/empty-db) nil dl-schema dl-opts))

(defn dl-db-with [db data]
  ((resolve 'datalevin.core/db-with) db data))

(defn dl-close [db]
  ((resolve 'datalevin.core/close-db) db))

(defn dl-q [query & args]
  ;; Disable datalevin query cache for fair benchmarking
  (let [cache-var (resolve 'datalevin.query/*cache?*)]
    (push-thread-bindings {cache-var false})
    (try
      (apply (resolve 'datalevin.core/q) query args)
      (finally
        (pop-thread-bindings)))))

(defn dl-db-with-people []
  (dl-db-with (dl-empty-db) people20k))

;; ---------------------------------------------------------------------------
;; Datomic setup (loaded dynamically)

(defn dt-require! []
  (require '[datomic.api :as datomic]))

(defn dt-new-conn
  ([] (dt-new-conn "bench"))
  ([db-name]
   (let [url (str "datomic:mem://" db-name)
         _ ((resolve 'datomic.api/delete-database) url)
         _ ((resolve 'datomic.api/create-database) url)
         conn ((resolve 'datomic.api/connect) url)]
     @((resolve 'datomic.api/transact) conn
       [{:db/id ((resolve 'datomic.api/tempid) :db.part/db)
         :db/ident :name :db/valueType :db.type/string
         :db/cardinality :db.cardinality/one :db.install/_attribute :db.part/db}
        {:db/id ((resolve 'datomic.api/tempid) :db.part/db)
         :db/ident :last-name :db/valueType :db.type/string
         :db/cardinality :db.cardinality/one :db.install/_attribute :db.part/db}
        {:db/id ((resolve 'datomic.api/tempid) :db.part/db)
         :db/ident :sex :db/valueType :db.type/keyword
         :db/cardinality :db.cardinality/one :db.install/_attribute :db.part/db}
        {:db/id ((resolve 'datomic.api/tempid) :db.part/db)
         :db/ident :age :db/valueType :db.type/long
         :db/cardinality :db.cardinality/one :db.install/_attribute :db.part/db}
        {:db/id ((resolve 'datomic.api/tempid) :db.part/db)
         :db/ident :salary :db/valueType :db.type/long
         :db/cardinality :db.cardinality/one :db.install/_attribute :db.part/db}
        {:db/id ((resolve 'datomic.api/tempid) :db.part/db)
         :db/ident :follows :db/valueType :db.type/ref
         :db/cardinality :db.cardinality/many :db.install/_attribute :db.part/db}])
     conn)))

(defn dt-db-with [conn tx-data]
  (-> ((resolve 'datomic.api/transact) conn tx-data)
      deref
      :db-after))

(defn dt-q [query & args]
  (apply (resolve 'datomic.api/q) query args))

(defn dt-db-with-people []
  (let [conn (dt-new-conn "db-people")]
    (dt-db-with conn people20k)
    ((resolve 'datomic.api/db) conn)))

;; ---------------------------------------------------------------------------
;; Query benchmarks

(def queries
  {:q1       {:desc "Simple lookup: [?e :name \"Ivan\"]"
              :query '[:find ?e
                        :where [?e :name "Ivan"]]}
   :q2       {:desc "Two-clause join: name=Ivan + age"
              :query '[:find ?e ?a
                        :where
                        [?e :name "Ivan"]
                        [?e :age ?a]]}
   :q2-switch {:desc "Reversed clause order: age then name=Ivan"
               :query '[:find ?e ?a
                         :where
                         [?e :age ?a]
                         [?e :name "Ivan"]]}
   :q3       {:desc "Three clauses: name=Ivan + age + sex=male"
              :query '[:find ?e ?a
                        :where
                        [?e :name "Ivan"]
                        [?e :age ?a]
                        [?e :sex :male]]}
   :q4       {:desc "Four clauses: name + last-name + age + sex"
              :query '[:find ?e ?l ?a
                        :where
                        [?e :name "Ivan"]
                        [?e :last-name ?l]
                        [?e :age ?a]
                        [?e :sex :male]]}
   :q5       {:desc "Value join: age shared between entities"
              :query '[:find ?e1 ?l ?a
                        :where
                        [?e :name "Ivan"]
                        [?e :age ?a]
                        [?e1 :age ?a]
                        [?e1 :last-name ?l]]}
   :qpred1   {:desc "Predicate: salary > 50000"
              :query '[:find ?e ?s
                        :where
                        [?e :salary ?s]
                        [(> ?s 50000)]]}
   :qpred2   {:desc "Predicate with :in binding: salary > ?min"
              :query '[:find ?e ?s
                        :in $ ?min_s
                        :where
                        [?e :salary ?s]
                        [(> ?s ?min_s)]]
              :args [50000]}
   :q-or     {:desc "OR disjunction"
              :query '[:find ?e
                        :where
                        (or [?e :name "Ivan"]
                            [?e :name "Petr"])]}
   :q-not    {:desc "NOT negation"
              :query '[:find ?e ?a
                        :where
                        [?e :age ?a]
                        (not [?e :sex :male])]}
   :q-pred-range {:desc "Range predicate: 50 < salary < 80000"
                  :query '[:find ?e ?s
                            :where
                            [?e :salary ?s]
                            [(> ?s 50000)]
                            [(< ?s 80000)]]}
   :q-5-merge {:desc "5 merges: name+last-name+age+salary+sex"
               :query '[:find ?e ?n ?l ?a ?s
                         :where
                         [?e :name ?n] [?e :last-name ?l] [?e :age ?a]
                         [?e :salary ?s] [?e :sex :male]]}
   :q-rule   {:desc "Non-recursive rule"
              :query '[:find ?e1 ?e2
                        :in $ %
                        :where (follow ?e1 ?e2)]
              :args ['[[(follow ?e1 ?e2) [?e1 :follows ?e2]]]]}
   :q-or-join {:desc "OR-join: name Ivan or Petr + age"
               :query '[:find ?e ?a
                         :where
                         [?e :age ?a]
                         (or-join [?e]
                           [?e :name "Ivan"]
                           [?e :name "Petr"])]}
   :q-not-join {:desc "NOT-join: has age, not sex=male"
                :query '[:find ?e ?a
                          :where
                          [?e :age ?a]
                          (not-join [?e]
                            [?e :sex :male])]}})

;; ---------------------------------------------------------------------------
;; Recursive rule benchmarks (matching datalevin's wide/long patterns)

(def recursive-rule
  '[[(follows ?x ?y)
     [?x :follows ?y]]
    [(follows ?x ?y)
     [?x :follows ?t]
     (follows ?t ?y)]])

(defn- wide-db-data
  "Generate wide tree: each node has `width` children, `depth` levels deep."
  ([depth width] (wide-db-data 1 depth width))
  ([id depth width]
   (if (pos? depth)
     (let [children (map #(+ (* id width) %) (range width))]
       (concat
        (map #(hash-map :db/id (str id) :name "Ivan" :follows (str %)) children)
        (mapcat #(wide-db-data % (dec depth) width) children)))
     [{:db/id (str id) :name "Ivan"}])))

(defn- long-db-data
  "Generate chain: `width` independent chains of `depth` links."
  [depth width]
  (apply concat
    (for [x (range width)
          y (range depth)
          :let [from (+ (* x (inc depth)) y)
                to   (+ (* x (inc depth)) y 1)]]
      [{:db/id   (str from) :name "Ivan" :follows (str to)}
       {:db/id   (str to)   :name "Ivan"}])))

(defn- dh-rule-db [tx-data]
  (let [conn (dh-empty-db)]
    (d/transact conn {:tx-data tx-data})
    (let [db @conn]
      (d/release conn)
      db)))

(defn- dl-rule-db [tx-data]
  (dl-db-with (dl-empty-db) tx-data))

(def rule-benchmarks
  {:rules-wide-3x3  {:desc "Recursive rule: wide tree 3x3"  :data-fn #(wide-db-data 3 3)}
   :rules-wide-5x3  {:desc "Recursive rule: wide tree 5x3"  :data-fn #(wide-db-data 5 3)}
   :rules-wide-7x3  {:desc "Recursive rule: wide tree 7x3"  :data-fn #(wide-db-data 7 3)}
   :rules-wide-4x6  {:desc "Recursive rule: wide tree 4x6"  :data-fn #(wide-db-data 4 6)}
   :rules-long-10x3 {:desc "Recursive rule: chain 10x3"     :data-fn #(long-db-data 10 3)}
   :rules-long-30x3 {:desc "Recursive rule: chain 30x3"     :data-fn #(long-db-data 30 3)}
   :rules-long-30x5 {:desc "Recursive rule: chain 30x5"     :data-fn #(long-db-data 30 5)}})

(def rule-order [:rules-wide-3x3 :rules-wide-5x3 :rules-wide-7x3 :rules-wide-4x6
                 :rules-long-10x3 :rules-long-30x3 :rules-long-30x5])

(def query-order [:q1 :q2 :q2-switch :q3 :q4 :q5 :qpred1 :qpred2
                  :q-or :q-not :q-or-join :q-not-join :q-pred-range :q-5-merge :q-rule])

;; ---------------------------------------------------------------------------
;; Aggregate query benchmarks

(def agg-queries
  {:q-agg-avg     {:desc "Aggregate: avg salary"
                   :query '[:find (avg ?s)
                             :where [?e :salary ?s]]}
   :q-agg-group   {:desc "Aggregate: avg+count salary by sex"
                   :query '[:find ?sex (avg ?s) (count ?e)
                             :where [?e :sex ?sex] [?e :salary ?s]]}
   :q-agg-filter  {:desc "Aggregate: avg/min/max male salary"
                   :query '[:find (avg ?s) (min ?s) (max ?s)
                             :where [?e :salary ?s] [?e :sex :male]]}
   :q-agg-pred    {:desc "Aggregate: avg salary>50k by sex"
                   :query '[:find ?sex (avg ?s)
                             :where [?e :salary ?s] [?e :sex ?sex]
                                    [(> ?s 50000)]]}
   :q-agg-multi   {:desc "Aggregate: avg salary by sex×name"
                   :query '[:find ?sex ?n (avg ?s)
                             :where [?e :sex ?sex] [?e :name ?n]
                                    [?e :salary ?s]]}
   :q-agg-stats   {:desc "Aggregate: variance+stddev+median"
                   :query '[:find (avg ?s) (variance ?s) (stddev ?s) (median ?s)
                             :where [?e :salary ?s]]}})

(def agg-order [:q-agg-avg :q-agg-group :q-agg-filter :q-agg-pred
                :q-agg-multi :q-agg-stats])

;; ---------------------------------------------------------------------------
;; Result formatting

(defn pad [s width]
  (let [s (str s)]
    (if (< (count s) width)
      (str (apply str (repeat (- width (count s)) " ")) s)
      s)))

(defn print-header []
  (println)
  (let [planner? (= "true" (System/getenv "DATAHIKE_QUERY_PLANNER"))
        dh-label (if planner? "DH-plan" "DH-legacy")]
    (println (format "%-12s %-45s %10s %10s %10s  %s"
                     "Benchmark" "Description"
                     dh-label "Datalevin" "Datomic" "Winner"))
    (println (apply str (repeat 110 "-")))))

(defn print-row [bench-name desc dh-ms dl-ms dt-ms]
  (let [times (remove nil? [dh-ms dl-ms dt-ms])
        best (when (seq times) (apply min times))
        tag (fn [ms label]
              (if (nil? ms) (pad "N/A" 10)
                  (let [s (pad (round ms) 10)]
                    (if (= ms best) (str s "*") (str s " ")))))
        winner (cond
                 (and dh-ms dl-ms dt-ms)
                 (cond (= best dh-ms) "DATAHIKE"
                       (= best dl-ms) "datalevin"
                       :else           "datomic")
                 (and dh-ms dl-ms)
                 (if (= best dh-ms) "DATAHIKE" "datalevin")
                 :else "?")]
    (println (format "%-12s %-45s %10s %10s %10s  %s"
                     (name bench-name) desc
                     (tag dh-ms "dh") (tag dl-ms "dl") (tag dt-ms "dt")
                     winner))))

;; ---------------------------------------------------------------------------
;; Run benchmarks

(defn- dh-inmemory-db
  "Create a datahike in-memory DB with people20k via proper create-database."
  []
  (let [conn (dh-db-with-people)
        db @conn]
    (d/release conn)
    db))

(defn run-queries
  "Run query benchmarks for all available engines.
   Uses per-query DBs to avoid plan-cache cross-contamination.
   Disables Datahike query result cache for fair comparison."
  [& {:keys [datalevin? datomic?] :or {datalevin? true datomic? true}}]
  ;; Disable datahike query result cache
  (alter-var-root #'q/*query-result-cache?* (constantly false))
  (println "Setting up databases with 20k people (query result cache OFF)...")
  (println (str "  Query planner: " (System/getenv "DATAHIKE_QUERY_PLANNER")))

  ;; Datahike: one DB per query to match Datalevin's benchmark approach
  (let [dh-dbs (into {} (map (fn [qname] [qname (dh-inmemory-db)]) query-order))]
    (println "  Datahike DBs ready.")

    ;; Datalevin: one DB per query
    (def ^:private dl-dbs-atom (atom nil))
    (when datalevin?
      (try
        (dl-require!)
        (reset! dl-dbs-atom
                (into {} (map (fn [qname] [qname (dl-db-with-people)]) query-order)))
        (println "  Datalevin DBs ready.")
        (catch Exception e
          (println "  Datalevin not available:" (.getMessage e)))))

    ;; Datomic
    (def ^:private dt-db-atom (atom nil))
    (when datomic?
      (try
        (dt-require!)
        (reset! dt-db-atom (dt-db-with-people))
        (println "  Datomic DB ready.")
        (catch Exception e
          (println "  Datomic not available:" (.getMessage e)))))

    ;; JIT pre-warmup: run ALL datahike queries to stabilize JVMCI compilation.
    ;; Without this, running Q1 (no merges) before Q2 (with merges) causes
    ;; execute_group_direct to be deoptimized and recompiled conservatively,
    ;; adding ~0.3ms overhead. Running all shapes first ensures JVMCI produces
    ;; polymorphic code that handles all cases efficiently.
    (println "  JIT pre-warmup (all query shapes)...")
    (let [warmup-db (first (vals dh-dbs))]
      (doseq [qname query-order]
        (let [{:keys [query args]} (get queries qname)
              qargs (or args [])]
          (try
            (dotimes [_ 500]
              (apply d/q query warmup-db qargs))
            (catch Exception _)))))
    (println "  JIT pre-warmup done.")

    (print-header)

    (doseq [qname query-order]
      (let [{:keys [desc query args]} (get queries qname)
            qargs (or args [])
            dh-db (get dh-dbs qname)

            ;; Validate result counts match
            dh-result (apply d/q query dh-db qargs)
            dh-count (count dh-result)
            dl-result (when-let [dl-dbs @dl-dbs-atom]
                        (apply dl-q query (get dl-dbs qname) qargs))
            dl-count (when dl-result (count dl-result))

            _ (when (and dl-count (not= dh-count dl-count))
                (println (format "  ⚠ RESULT MISMATCH %s: DH=%d DL=%d"
                                 (name qname) dh-count dl-count)))

            ;; Datahike
            dh-ms (bench (apply d/q query dh-db qargs))

            ;; Datalevin
            dl-ms (when-let [dl-dbs @dl-dbs-atom]
                    (bench (apply dl-q query (get dl-dbs qname) qargs)))

            ;; Datomic
            dt-ms (when @dt-db-atom
                    (bench (apply dt-q query @dt-db-atom qargs)))]
        (print-row qname desc dh-ms dl-ms dt-ms)
        (println (format "  Results: DH=%d%s" dh-count
                         (if dl-count (format " DL=%d%s" dl-count
                                              (if (= dh-count dl-count) " ✓" " ✗"))
                           "")))))

    ;; Cleanup
    (when-let [dl-dbs @dl-dbs-atom]
      (doseq [[_ db] dl-dbs] (dl-close db)))
    (println "\nDone.")))

(defn run-writes
  "Run write benchmarks for all available engines."
  [& {:keys [datalevin? datomic?] :or {datalevin? true datomic? true}}]

  ;; Check availability
  (when datalevin?
    (try (dl-require!) (catch Exception e (println "  Datalevin not available:" (.getMessage e)))))
  (when datomic?
    (try (dt-require!) (catch Exception e (println "  Datomic not available:" (.getMessage e)))))

  (print-header)

  ;; add-all: bulk insert 20k entities in one transaction
  (let [dh-ms (bench-10
               (let [conn (dh-empty-db)]
                 (d/transact conn {:tx-data people20k})
                 (d/release conn)))
        dl-ms (when (resolve 'datalevin.core/empty-db)
                (bench-10
                 (let [db (dl-db-with (dl-empty-db) people20k)]
                   (dl-close db))))
        dt-ms (when (resolve 'datomic.api/q)
                (bench-10
                 (let [conn (dt-new-conn (str "add-all-" (rand-int Integer/MAX_VALUE)))]
                   @((resolve 'datomic.api/transact) conn people20k))))]
    (print-row :add-all "Bulk insert 20k entities" dh-ms dl-ms dt-ms))

  ;; add-5: insert one entity per transaction
  (let [dh-ms (bench-10
               (let [conn (dh-empty-db)]
                 (doseq [p people20k]
                   (d/transact conn {:tx-data [p]}))
                 (d/release conn)))
        dl-ms (when (resolve 'datalevin.core/empty-db)
                (bench-10
                 (let [db (reduce (fn [db p] (dl-db-with db [p]))
                                  (dl-empty-db)
                                  people20k)]
                   (dl-close db))))
        dt-ms (when (resolve 'datomic.api/q)
                (bench-10
                 (let [conn (dt-new-conn (str "add5-" (rand-int Integer/MAX_VALUE)))]
                   (doseq [p people20k]
                     @((resolve 'datomic.api/transact) conn [p])))))]
    (print-row :add-5 "Insert 20k entities one-by-one" dh-ms dl-ms dt-ms))

  (println "\nDone."))

(defn run-rules
  "Run recursive rule benchmarks for all available engines."
  [& {:keys [datalevin? datomic?] :or {datalevin? true datomic? true}}]
  (alter-var-root #'q/*query-result-cache?* (constantly false))
  (println "\nRecursive rule benchmarks (query result cache OFF)...")

  (when datalevin?
    (try (dl-require!) (catch Exception _)))
  (when datomic?
    (try (dt-require!) (catch Exception _)))

  (let [rule-query '[:find ?e ?e2
                      :in $ %
                      :where (follows ?e ?e2)]]

    (print-header)

    (doseq [rname rule-order]
      (let [{:keys [desc data-fn]} (get rule-benchmarks rname)
            tx-data (data-fn)

            dh-db (dh-rule-db tx-data)
            dh-result (d/q rule-query dh-db recursive-rule)
            dh-count (count dh-result)

            dl-db (when (resolve 'datalevin.core/empty-db)
                    (dl-rule-db tx-data))
            dl-result (when dl-db (dl-q rule-query dl-db recursive-rule))
            dl-count (when dl-result (count dl-result))

            dt-db (when (resolve 'datomic.api/q)
                    (let [conn (dt-new-conn (str "rules-" (name rname)))]
                      (dt-db-with conn tx-data)
                      ((resolve 'datomic.api/db) conn)))
            dt-result (when dt-db (dt-q rule-query dt-db recursive-rule))
            dt-count (when dt-result (count dt-result))

            _ (when (and dl-count (not= dh-count dl-count))
                (println (format "  ⚠ RESULT MISMATCH %s: DH=%d DL=%d"
                                 (name rname) dh-count dl-count)))

            dh-ms (bench (d/q rule-query dh-db recursive-rule))
            dl-ms (when dl-db (bench (dl-q rule-query dl-db recursive-rule)))
            dt-ms (when dt-db (bench (dt-q rule-query dt-db recursive-rule)))]
        (print-row rname desc dh-ms dl-ms dt-ms)
        (println (format "  Results: DH=%d%s%s" dh-count
                         (if dl-count (format " DL=%d%s" dl-count
                                              (if (= dh-count dl-count) " ✓" " ✗"))
                           "")
                         (if dt-count (format " DT=%d%s" dt-count
                                              (if (= dh-count dt-count) " ✓" " ✗"))
                           "")))
        (when dl-db (dl-close dl-db))))

    (println "\nDone.")))

;; ---------------------------------------------------------------------------
;; Stratum-enabled DB setup

(defn dh-empty-db-stratum []
  (require 'datahike.index.secondary.stratum)
  (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
             :schema-flexibility :write
             :keep-history? false
             :attribute-refs? false  ;; secondary index schema attrs use implicit schema
             :search-cache-size 0    ;; which only works without attribute-refs
             :index :datahike.index/persistent-set}]
    (d/delete-database cfg)
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn {:tx-data dh-schema})
      (d/transact conn {:tx-data [{:db/ident :idx/analytics
                                   :db.secondary/type :stratum
                                   :db.secondary/attrs [:name :last-name :sex :age :salary]}]})
      (Thread/sleep 1000) ;; wait for backfill to complete
      conn)))

(defn dh-stratum-db-with-people []
  (let [conn (dh-empty-db-stratum)]
    (d/transact conn {:tx-data people20k})
    conn))

(defn run-aggregates
  "Run aggregate benchmarks: standard Datalog aggregates across all engines,
   comparing DH (PSS), DH+stratum (columnar), Datalevin, and Datomic."
  [& {:keys [datalevin? datomic?] :or {datalevin? true datomic? true}}]
  (alter-var-root #'q/*query-result-cache?* (constantly false))
  (println "\n=== AGGREGATE BENCHMARKS ===")
  (println "Setting up databases with 20k people (query result cache OFF)...")
  (println (str "  Query planner: " (System/getenv "DATAHIKE_QUERY_PLANNER")))

  ;; DH without stratum (PSS-based aggregates)
  (let [dh-db (dh-inmemory-db)]
    (println "  Datahike (PSS) DB ready.")

    ;; DH with stratum
    (let [dh-stratum-conn (try
                            (dh-stratum-db-with-people)
                            (catch Exception e
                              (println "  Stratum not available:" (.getMessage e))
                              nil))
          dh-stratum-db (when dh-stratum-conn
                          (let [db @dh-stratum-conn]
                            (d/release dh-stratum-conn)
                            db))]
      (when dh-stratum-db
        (println "  Datahike (stratum) DB ready."))

      ;; Datalevin
      (def ^:private dl-agg-db-atom (atom nil))
      (when datalevin?
        (try
          (dl-require!)
          (reset! dl-agg-db-atom (dl-db-with-people))
          (println "  Datalevin DB ready.")
          (catch Exception e
            (println "  Datalevin not available:" (.getMessage e)))))

      ;; Datomic
      (def ^:private dt-agg-db-atom (atom nil))
      (when datomic?
        (try
          (dt-require!)
          (reset! dt-agg-db-atom (dt-db-with-people))
          (println "  Datomic DB ready.")
          (catch Throwable e
            (println "  Datomic not available:" (.getMessage e)))))

      ;; Print header with extra column for stratum
      (println)
      (let [planner? (= "true" (System/getenv "DATAHIKE_QUERY_PLANNER"))
            dh-label (if planner? "DH-plan" "DH-legacy")]
        (println (format "%-12s %-36s %10s %10s %10s %10s  %s"
                         "Benchmark" "Description"
                         dh-label "DH+strat" "Datalevin" "Datomic" "Winner"))
        (println (apply str (repeat 120 "-"))))

      (doseq [qname agg-order]
        (let [{:keys [desc query]} (get agg-queries qname)

              ;; DH PSS result
              dh-result (try (d/q query dh-db)
                             (catch Exception e
                               (println (format "  ⚠ DH error on %s: %s" (name qname) (.getMessage e)))
                               nil))

              ;; DH stratum result
              dh-st-result (when dh-stratum-db
                             (try (d/q query dh-stratum-db)
                                  (catch Exception e
                                    (println (format "  ⚠ DH+stratum error on %s: %s"
                                                     (name qname) (.getMessage e)))
                                    nil)))

              ;; DL result
              dl-result (when @dl-agg-db-atom
                          (try (dl-q query @dl-agg-db-atom)
                               (catch Exception e
                                 (println (format "  ⚠ DL error on %s: %s" (name qname) (.getMessage e)))
                                 nil)))

              ;; DT result
              dt-result (when @dt-agg-db-atom
                          (try (dt-q query @dt-agg-db-atom)
                               (catch Exception e
                                 (println (format "  ⚠ DT error on %s: %s" (name qname) (.getMessage e)))
                                 nil)))

              ;; Bench DH PSS
              dh-ms (when dh-result
                      (bench (d/q query dh-db)))

              ;; Bench DH stratum
              dh-st-ms (when dh-st-result
                         (bench (d/q query dh-stratum-db)))

              ;; Bench DL
              dl-ms (when dl-result
                      (bench (dl-q query @dl-agg-db-atom)))

              ;; Bench DT
              dt-ms (when dt-result
                      (bench (dt-q query @dt-agg-db-atom)))

              ;; Find winner
              times (remove nil? [dh-ms dh-st-ms dl-ms dt-ms])
              best (when (seq times) (apply min times))
              tag (fn [ms]
                    (if (nil? ms)
                      (pad "N/A" 10)
                      (let [s (pad (round ms) 10)]
                        (if (= ms best) (str s "*") (str s " ")))))
              winner (cond
                       (= best dh-ms) "DH-PSS"
                       (= best dh-st-ms) "DH+STRATUM"
                       (= best dl-ms) "datalevin"
                       (= best dt-ms) "datomic"
                       :else "?")]
          (println (format "%-12s %-36s %10s %10s %10s %10s  %s"
                           (name qname) desc
                           (tag dh-ms) (tag dh-st-ms) (tag dl-ms) (tag dt-ms)
                           winner))
          (println (format "  Results: DH=%s DH+st=%s%s%s"
                           (if dh-result (str (count dh-result)) "err")
                           (if dh-st-result (str (count dh-st-result)) "N/A")
                           (if dl-result (format " DL=%d" (count dl-result)) "")
                           (if dt-result (format " DT=%d" (count dt-result)) "")))))

      ;; Cleanup
      (when-let [dl-db @dl-agg-db-atom]
        (dl-close dl-db))
      (println "\nDone."))))

;; ---------------------------------------------------------------------------
;; Temporal benchmarks (Datahike vs Datomic — only these two support history)

(def temporal-queries
  {:t-current-q1  {:desc "Current DB: name=Ivan"
                   :db-key :current
                   :query '[:find ?e :where [?e :name "Ivan"]]}
   :t-current-q2  {:desc "Current DB: name+age"
                   :db-key :current
                   :query '[:find ?e ?a :where [?e :name "Ivan"] [?e :age ?a]]}
   :t-asof-q1     {:desc "As-of: name=Ivan"
                   :db-key :as-of
                   :query '[:find ?e :where [?e :name "Ivan"]]}
   :t-asof-q2     {:desc "As-of: name+age"
                   :db-key :as-of
                   :query '[:find ?e ?a :where [?e :name "Ivan"] [?e :age ?a]]}
   :t-asof-q3     {:desc "As-of: name+age+sex"
                   :db-key :as-of
                   :query '[:find ?e ?a :where [?e :name "Ivan"] [?e :age ?a] [?e :sex :male]]}
   :t-hist-q1     {:desc "History: all names"
                   :db-key :history
                   :query '[:find ?e :where [?e :name]]}
   :t-hist-q2     {:desc "History: age+tx"
                   :db-key :history
                   :query '[:find ?e ?a ?tx :where [?e :age ?a ?tx]]}
   :t-hist-q3     {:desc "History: name+age join"
                   :db-key :history
                   :query '[:find ?e ?n ?a :where [?e :name ?n] [?e :age ?a]]}
   :t-hist-retract {:desc "History: retracted ages"
                    :db-key :history
                    :query '[:find ?e ?a :where [?e :age ?a _ false]]}})

(def temporal-query-order
  [:t-current-q1 :t-current-q2
   :t-asof-q1 :t-asof-q2 :t-asof-q3
   :t-hist-q1 :t-hist-q2 :t-hist-q3 :t-hist-retract])

(defn- print-temporal-header []
  (println)
  (let [planner? (= "true" (System/getenv "DATAHIKE_QUERY_PLANNER"))
        dh-label (if planner? "DH-plan" "DH-legacy")]
    (println (format "%-16s %-36s %10s %10s  %s"
                     "Benchmark" "Description" dh-label "Datomic" "Winner"))
    (println (apply str (repeat 90 "-")))))

(defn- print-temporal-row [bench-name desc dh-ms dt-ms]
  (let [times (remove nil? [dh-ms dt-ms])
        best (when (seq times) (apply min times))
        tag (fn [ms]
              (if (nil? ms) (pad "N/A" 10)
                  (let [s (pad (round ms) 10)]
                    (if (= ms best) (str s "*") (str s " ")))))
        winner (cond
                 (nil? dh-ms) "datomic"
                 (nil? dt-ms) "DATAHIKE"
                 (= best dh-ms) "DATAHIKE"
                 :else "datomic")]
    (println (format "%-16s %-36s %10s %10s  %s"
                     (name bench-name) desc (tag dh-ms) (tag dt-ms) winner))))

(defn run-temporal
  "Run temporal (history/as-of) benchmarks: Datahike vs Datomic.
   Datalevin has no time-travel support, so it's excluded."
  [& {:keys [datomic?] :or {datomic? true}}]
  (alter-var-root #'q/*query-result-cache?* (constantly false))
  (println "=== TEMPORAL BENCHMARKS (Datahike vs Datomic) ===")
  (println "Setting up history-enabled databases with 20k people + 2k modifications...")
  (println (str "  Query planner: " (System/getenv "DATAHIKE_QUERY_PLANNER")))

  ;; --- Datahike setup: history-enabled DB ---
  (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
             :schema-flexibility :write
             :keep-history? true
             :attribute-refs? true
             :search-cache-size 0
             :index :datahike.index/persistent-set}
        _ (d/create-database cfg)
        dh-conn (d/connect cfg)
        _ (d/transact dh-conn {:tx-data dh-schema})
        _ (d/transact dh-conn {:tx-data people20k})
        dh-tx1 (:max-tx (d/db dh-conn))
        ;; Get 2000 real entity IDs to modify
        dh-eids (take 2000 (map first (d/q '[:find ?e :where [?e :name]] (d/db dh-conn))))
        _ (d/transact dh-conn {:tx-data
                                (mapv (fn [eid] [:db/add eid :age (long (+ 100 (rand-int 50)))])
                                      dh-eids)})
        dh-db (d/db dh-conn)
        dh-dbs {:current dh-db
                :as-of   (d/as-of dh-db dh-tx1)
                :history (d/history dh-db)}]
    (println "  Datahike ready (history=true).")

    ;; --- Datomic setup ---
    (def ^:private dt-temporal-dbs (atom nil))
    (when datomic?
      (try
        (dt-require!)
        (let [url "datomic:mem://bench-temporal"
              _ ((resolve 'datomic.api/delete-database) url)
              _ ((resolve 'datomic.api/create-database) url)
              dt-conn ((resolve 'datomic.api/connect) url)]
          @((resolve 'datomic.api/transact) dt-conn
            [{:db/id ((resolve 'datomic.api/tempid) :db.part/db)
              :db/ident :name :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one :db.install/_attribute :db.part/db}
             {:db/id ((resolve 'datomic.api/tempid) :db.part/db)
              :db/ident :last-name :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one :db.install/_attribute :db.part/db}
             {:db/id ((resolve 'datomic.api/tempid) :db.part/db)
              :db/ident :sex :db/valueType :db.type/keyword
              :db/cardinality :db.cardinality/one :db.install/_attribute :db.part/db}
             {:db/id ((resolve 'datomic.api/tempid) :db.part/db)
              :db/ident :age :db/valueType :db.type/long
              :db/cardinality :db.cardinality/one :db.install/_attribute :db.part/db}
             {:db/id ((resolve 'datomic.api/tempid) :db.part/db)
              :db/ident :salary :db/valueType :db.type/long
              :db/cardinality :db.cardinality/one :db.install/_attribute :db.part/db}
             {:db/id ((resolve 'datomic.api/tempid) :db.part/db)
              :db/ident :follows :db/valueType :db.type/ref
              :db/cardinality :db.cardinality/many :db.install/_attribute :db.part/db}])
          @((resolve 'datomic.api/transact) dt-conn (vec people20k))
          (let [dt-tx1 (-> ((resolve 'datomic.api/db) dt-conn)
                           ((resolve 'datomic.api/basis-t)))
                dt-eids (take 2000 (map first
                                       (dt-q '[:find ?e :where [?e :name]]
                                             ((resolve 'datomic.api/db) dt-conn))))]
            @((resolve 'datomic.api/transact) dt-conn
              (mapv (fn [eid] [:db/add eid :age (long (+ 100 (rand-int 50)))]) dt-eids))
            (let [dt-db ((resolve 'datomic.api/db) dt-conn)]
              (reset! dt-temporal-dbs
                      {:current dt-db
                       :as-of   ((resolve 'datomic.api/as-of) dt-db dt-tx1)
                       :history ((resolve 'datomic.api/history) dt-db)}))))
        (println "  Datomic ready.")
        (catch Exception e
          (println "  Datomic not available:" (.getMessage e)))))

    ;; JIT warmup
    (println "  JIT pre-warmup...")
    (doseq [qname temporal-query-order]
      (let [{:keys [query db-key]} (get temporal-queries qname)
            db (get dh-dbs db-key)]
        (try (dotimes [_ 200] (d/q query db)) (catch Exception _))))
    (println "  JIT pre-warmup done.")

    (print-temporal-header)

    (doseq [qname temporal-query-order]
      (let [{:keys [desc query db-key]} (get temporal-queries qname)
            dh-db (get dh-dbs db-key)

            ;; Validate result counts
            dh-result (d/q query dh-db)
            dh-count (count dh-result)
            dt-result (when-let [dt-dbs @dt-temporal-dbs]
                        (dt-q query (get dt-dbs db-key)))
            dt-count (when dt-result (count dt-result))

            _ (when (and dt-count (not= dh-count dt-count))
                (println (format "  ⚠ RESULT MISMATCH %s: DH=%d DT=%d"
                                 (name qname) dh-count dt-count)))

            ;; Benchmark
            dh-ms (bench (d/q query dh-db))
            dt-ms (when @dt-temporal-dbs
                    (bench (dt-q query (get @dt-temporal-dbs db-key))))]
        (print-temporal-row qname desc dh-ms dt-ms)
        (println (format "  Results: DH=%d%s" dh-count
                         (if dt-count (format " DT=%d%s" dt-count
                                              (if (= dh-count dt-count) " ✓" " ✗"))
                           "")))))

    ;; Cleanup
    (d/release dh-conn)
    (println "\nDone.")))

;; ---------------------------------------------------------------------------
;; Cross-entity join benchmarks

(def ^:private join-schema
  [{:db/ident :p/name    :db/valueType :db.type/string  :db/cardinality :db.cardinality/one}
   {:db/ident :p/dept    :db/valueType :db.type/ref     :db/cardinality :db.cardinality/one :db/index true}
   {:db/ident :p/salary  :db/valueType :db.type/long    :db/cardinality :db.cardinality/one :db/index true}
   {:db/ident :d/name    :db/valueType :db.type/string  :db/cardinality :db.cardinality/one}
   {:db/ident :d/budget  :db/valueType :db.type/long    :db/cardinality :db.cardinality/one :db/index true}
   {:db/ident :d/div     :db/valueType :db.type/ref     :db/cardinality :db.cardinality/one}
   {:db/ident :div/name  :db/valueType :db.type/string  :db/cardinality :db.cardinality/one}])

(def ^:private dl-join-schema
  {:p/name   {:db/valueType :db.type/string}
   :p/dept   {:db/valueType :db.type/ref :db/index true}
   :p/salary {:db/valueType :db.type/long :db/index true}
   :d/name   {:db/valueType :db.type/string}
   :d/budget {:db/valueType :db.type/long :db/index true}
   :d/div    {:db/valueType :db.type/ref}
   :div/name {:db/valueType :db.type/string}})

;; Pre-generate deterministic salary data (fixed seed for reproducibility)
(def ^:private join-salaries
  (let [rng (Random. 42)]
    (mapv (fn [_] (+ 30000 (.nextInt rng 70000))) (range 20000))))

(defn- join-people-tx
  "Generate people tx-data using pre-computed salaries and a dept-id lookup fn."
  [dept-ids]
  (mapv (fn [i] {:p/name (str "p-" i)
                 :p/dept (nth dept-ids (mod i 100))
                 :p/salary (nth join-salaries i)})
        (range 20000)))

(defn- dh-join-db
  "Create datahike DB with 20k people, 100 departments, 10 divisions."
  []
  (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
             :schema-flexibility :write :keep-history? false
             :attribute-refs? false :search-cache-size 0
             :index :datahike.index/persistent-set}]
    (d/delete-database cfg)
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn {:tx-data join-schema})
      (d/transact conn {:tx-data (mapv (fn [i] {:db/id (- -1 i) :div/name (str "div-" i)})
                                       (range 10))})
      (let [div-ids (mapv first (d/q '[:find ?d :where [?d :div/name _]] @conn))]
        (d/transact conn {:tx-data (mapv (fn [i] {:db/id (- -100 i)
                                                   :d/name (str "dept-" i)
                                                   :d/budget (* i 5000)
                                                   :d/div (nth div-ids (mod i 10))})
                                         (range 100))}))
      (let [dept-ids (mapv first (d/q '[:find ?d :where [?d :d/name _]] @conn))]
        (d/transact conn {:tx-data (join-people-tx dept-ids)}))
      (let [db @conn] (d/release conn) db))))

(defn- dl-join-db []
  (let [dl-get-conn (resolve 'datalevin.core/get-conn)
        dl-transact (resolve 'datalevin.core/transact!)
        dl-db-fn    (resolve 'datalevin.core/db)
        dl-q-fn     (resolve 'datalevin.core/q)
        dir (str "/tmp/dlv-join-bench-" (System/currentTimeMillis))
        conn (dl-get-conn dir dl-join-schema)]
    (dl-transact conn (mapv (fn [i] {:db/id (- -1 i) :div/name (str "div-" i)}) (range 10)))
    (let [div-ids (mapv first (dl-q-fn '[:find ?d :where [?d :div/name _]] (dl-db-fn conn)))]
      (dl-transact conn (mapv (fn [i] {:db/id (- -100 i)
                                        :d/name (str "dept-" i)
                                        :d/budget (* i 5000)
                                        :d/div (nth div-ids (mod i 10))})
                               (range 100)))
      (let [dept-ids (mapv first (dl-q-fn '[:find ?d :where [?d :d/name _]] (dl-db-fn conn)))]
        (dl-transact conn (mapv (fn [i] {:p/name (str "p-" i)
                                          :p/dept (nth dept-ids (mod i 100))
                                          :p/salary (nth join-salaries i)})
                                 (range 20000)))))
    (let [db (dl-db-fn conn)]
      [db conn])))

(defn- dt-join-db []
  (let [dt-tempid  (resolve 'datomic.api/tempid)
        dt-transact (resolve 'datomic.api/transact)
        dt-db-fn   (resolve 'datomic.api/db)
        url "datomic:mem://join-bench"
        _ ((resolve 'datomic.api/delete-database) url)
        _ ((resolve 'datomic.api/create-database) url)
        conn ((resolve 'datomic.api/connect) url)]
    @(dt-transact conn
       (mapv (fn [[ident vtype]]
               {:db/id (dt-tempid :db.part/db)
                :db/ident ident :db/valueType vtype
                :db/cardinality :db.cardinality/one
                :db.install/_attribute :db.part/db})
             [[:p/name :db.type/string] [:p/dept :db.type/ref] [:p/salary :db.type/long]
              [:d/name :db.type/string] [:d/budget :db.type/long] [:d/div :db.type/ref]
              [:div/name :db.type/string]]))
    @(dt-transact conn (mapv (fn [i] {:db/id (dt-tempid :db.part/user)
                                       :div/name (str "div-" i)}) (range 10)))
    (let [div-ids (mapv first (dt-q '[:find ?d :where [?d :div/name _]] (dt-db-fn conn)))]
      @(dt-transact conn (mapv (fn [i] {:db/id (dt-tempid :db.part/user)
                                          :d/name (str "dept-" i)
                                          :d/budget (* i 5000)
                                          :d/div (nth div-ids (mod i 10))}) (range 100)))
      (let [dept-ids (mapv first (dt-q '[:find ?d :where [?d :d/name _]] (dt-db-fn conn)))]
        @(dt-transact conn (mapv (fn [i] {:db/id (dt-tempid :db.part/user)
                                            :p/name (str "p-" i)
                                            :p/dept (nth dept-ids (mod i 100))
                                            :p/salary (nth join-salaries i)}) (range 20000)))))
    (dt-db-fn conn)))

(def ^:private join-queries
  {:q-join-ref-1
   {:desc "Ref join: 1 dept → people (high selectivity)"
    :query '[:find ?pn ?dn
             :where [?d :d/name "dept-99"]
                    [?d :d/budget ?b]
                    [?e :p/dept ?d]
                    [?e :p/name ?pn]
                    [?d :d/name ?dn]]}
   :q-join-ref-10
   {:desc "Ref join: 10 depts → people"
    :query '[:find ?pn ?dn
             :where [?d :d/budget ?b] [(> ?b 450000)]
                    [?d :d/name ?dn]
                    [?e :p/dept ?d]
                    [?e :p/name ?pn]]}
   :q-join-pred
   {:desc "Ref join with predicate: budget > 400k"
    :query '[:find ?pn ?dn
             :where [?d :d/budget ?b] [(> ?b 400000)]
                    [?d :d/name ?dn]
                    [?e :p/dept ?d]
                    [?e :p/name ?pn]]}
   :q-join-chain
   {:desc "3-hop chain: person → dept → division"
    :query '[:find ?pn ?dn ?divn
             :where [?e :p/name ?pn]
                    [?e :p/dept ?d]
                    [?d :d/name ?dn]
                    [?d :d/div ?div]
                    [?div :div/name ?divn]]}
   :q-join-selective
   {:desc "Selective: salary > 90k → dept name"
    :query '[:find ?pn ?dn
             :where [?e :p/salary ?s] [(> ?s 90000)]
                    [?e :p/name ?pn]
                    [?e :p/dept ?d]
                    [?d :d/name ?dn]]}})

(def ^:private join-order [:q-join-ref-1 :q-join-ref-10 :q-join-pred :q-join-chain :q-join-selective])

(defn run-joins
  "Run cross-entity join benchmarks."
  [& {:keys [datalevin? datomic?] :or {datalevin? true datomic? true}}]
  (alter-var-root #'q/*query-result-cache?* (constantly false))
  (println "\n=== CROSS-ENTITY JOIN BENCHMARKS ===")
  (println "Setting up databases with 20k people + 100 depts + 10 divisions (deterministic salaries)...")
  (println (str "  Query planner: " (System/getenv "DATAHIKE_QUERY_PLANNER")))

  (let [dh-db (dh-join-db)]
    (println "  Datahike DB ready.")

    (def ^:private dl-join-db-atom (atom nil))
    (when datalevin?
      (try
        (dl-require!)
        (let [[db conn] (dl-join-db)]
          (reset! dl-join-db-atom db)
          (println "  Datalevin DB ready."))
        (catch Exception e
          (println "  Datalevin not available:" (.getMessage e)))))

    (def ^:private dt-join-db-atom (atom nil))
    (when datomic?
      (try
        (dt-require!)
        (reset! dt-join-db-atom (dt-join-db))
        (println "  Datomic DB ready.")
        (catch Throwable e
          (println "  Datomic not available:" (.getMessage e)))))

    ;; JIT warmup
    (println "  JIT pre-warmup...")
    (doseq [qname join-order]
      (let [{:keys [query]} (get join-queries qname)]
        (try (dotimes [_ 200] (d/q query dh-db)) (catch Exception _))))
    (println "  JIT pre-warmup done.")

    (print-header)

    (doseq [qname join-order]
      (let [{:keys [desc query]} (get join-queries qname)
            dh-result (d/q query dh-db)
            dh-count (count dh-result)
            dl-result (when @dl-join-db-atom (dl-q query @dl-join-db-atom))
            dl-count (when dl-result (count dl-result))
            dt-result (when @dt-join-db-atom (dt-q query @dt-join-db-atom))
            dt-count (when dt-result (count dt-result))

            _ (when (and dl-count (not= dh-count dl-count))
                (println (format "  ⚠ RESULT MISMATCH %s: DH=%d DL=%d"
                                 (name qname) dh-count dl-count)))
            _ (when (and dt-count (not= dh-count dt-count))
                (println (format "  ⚠ RESULT MISMATCH %s: DH=%d DT=%d"
                                 (name qname) dh-count dt-count)))

            dh-ms (bench (d/q query dh-db))
            dl-ms (when @dl-join-db-atom (bench (dl-q query @dl-join-db-atom)))
            dt-ms (when @dt-join-db-atom (bench (dt-q query @dt-join-db-atom)))]
        (print-row qname desc dh-ms dl-ms dt-ms)
        (println (format "  Results: DH=%d%s%s" dh-count
                         (if dl-count (format " DL=%d%s" dl-count
                                              (if (= dh-count dl-count) " ✓" " ✗"))
                           "")
                         (if dt-count (format " DT=%d%s" dt-count
                                              (if (= dh-count dt-count) " ✓" " ✗"))
                           "")))))

    (when-let [dl-db @dl-join-db-atom]
      (dl-close dl-db))
    (println "\nDone.")))

(defn run-all
  "Run all benchmarks."
  [& opts]
  (println "=== WRITE BENCHMARKS ===")
  (apply run-writes opts)
  (println)
  (println "=== QUERY BENCHMARKS ===")
  (apply run-queries opts)
  (println)
  (println "=== RECURSIVE RULE BENCHMARKS ===")
  (apply run-rules opts)
  (println)
  (apply run-aggregates opts)
  (println)
  (apply run-temporal opts)
  (println)
  (apply run-joins opts))

(defn run-queries-only-datahike
  "Quick benchmarks for datahike alone (no competitor deps needed)."
  []
  (run-queries :datalevin? false :datomic? false))

(defn -main
  "Entry point for comparative benchmarks.
   Usage: DATAHIKE_QUERY_PLANNER=true clj -M:bench-compare -m benchmark.datascript-bench [queries|writes|rules|all]"
  [& args]
  ;; replikativ.logging defaults to :warn
  (let [cmd (or (first args) "all")]
    (case cmd
      "queries"    (run-queries)
      "writes"     (run-writes)
      "rules"      (run-rules)
      "aggregates" (run-aggregates)
      "temporal"   (run-temporal)
      "joins"      (run-joins)
      "all"        (run-all)
      (do (println (str "Unknown command: " cmd))
          (println "Usage: ... -m benchmark.datascript-bench [queries|writes|rules|aggregates|temporal|joins|all]")
          (System/exit 1))))
  (System/exit 0))
