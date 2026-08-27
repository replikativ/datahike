(ns benchmark.join-strategy
  "Controlled inter-group join experiments.

  This is deliberately a kernel benchmark first: it models the compiled
  executor's materialized producer/consumer tuple lists while keeping database
  construction, planning, and result-set hashing out of the measurement.  The
  end-to-end query cases live beside it and should be used to validate any
  conclusion drawn here."
  (:require
   [datahike.api :as d]
   [datahike.datom :as datom]
   [datahike.lru :as lru]
   [datahike.query :as q]
   [datahike.query.index-ordered-aggregate :as ioa])
  (:import
   [java.lang.management ManagementFactory]
   [java.util ArrayList HashMap]))

(set! *warn-on-reflection* true)

(def ^:private scenarios
  [{:id :tiny-producer
    :producer [10 10 0]
    :consumer [100000 10000 0]
    :description "10 producer keys probe 100k rows; 100 matches"}
   {:id :tiny-producer-tail
    :producer [10 10 9990]
    :consumer [100000 10000 0]
    :description "10 producer keys at the tail of 100k consumer rows"}
   {:id :broad-one-to-many
    :producer [10000 10000 0]
    :consumer [100000 10000 0]
    :description "10k unique producer keys; ten consumer rows per key"}
   {:id :balanced-unique
    :producer [50000 50000 0]
    :consumer [50000 50000 0]
    :description "50k unique keys on both sides"}
   {:id :mostly-unmatched
    :producer [10000 10000 0]
    :consumer [100000 10000 9000]
    :description "broad scans with 10% key overlap"}
   {:id :no-match
    :producer [10000 10000 0]
    :consumer [100000 10000 20000]
    :description "broad scans with disjoint key ranges"}
   {:id :duplicates-both
    :producer [20000 10000 0]
    :consumer [100000 10000 0]
    :description "two producer and ten consumer rows per key"}
   {:id :one-long-run
    :producer [500 1 0]
    :consumer [500 1 0]
    :description "one equal-key run; 250k output pairs"}
   {:id :tuple-broad-one-to-many
    :producer [10000 10000 0 :tuple]
    :consumer [100000 10000 0 :tuple]
    :description "composite vector keys; ten consumer rows per key"}
   {:id :tuple-mostly-unmatched
    :producer [10000 10000 0 :tuple]
    :consumer [100000 10000 9000 :tuple]
    :description "composite vector keys with 10% overlap"}])

(defn- make-tuples
  "Return an Object[] of Object[key,payload] tuples, monotonically ordered by
  key. `distinct-keys` is capped by n; offset shifts the key range."
  (^objects [^long n ^long distinct-keys ^long offset]
   (make-tuples n distinct-keys offset :long))
  (^objects [^long n ^long distinct-keys ^long offset key-kind]
   (let [distinct-keys (max 1 (min n distinct-keys))
         ^objects out (object-array n)]
     (dotimes [i n]
       (let [k (+ offset (quot (* (long i) distinct-keys) n))
             k (if (= key-kind :tuple) [(quot k 1000) (mod k 1000)] k)
             ^objects tuple (object-array 2)]
         (aset tuple 0 (if (= key-kind :tuple) k (Long/valueOf (long k))))
         (aset tuple 1 (Long/valueOf i))
         (aset out i tuple)))
     out)))

(defn- append-pair! [^ArrayList out ^objects producer ^objects consumer]
  (let [^objects pair (object-array 2)]
    (aset pair 0 (aget producer 1))
    (aset pair 1 (aget consumer 1))
    (.add out pair)))

(defn hash-join
  "Hash the producer exactly as the compiled probe-map path does. Returns the
  output cardinality. With materialize? true, also allocates one projected tuple
  for every output pair."
  ^long [^objects producer ^objects consumer materialize?]
  (let [pn (alength producer)
        cn (alength consumer)
        ^HashMap table (HashMap. (max 16 pn))]
    (dotimes [i pn]
      (let [^objects tuple (aget producer i)
            k (aget tuple 0)
            ^ArrayList run (.get table k)]
        (if run
          (.add run tuple)
          (.put table k (doto (ArrayList. 4) (.add tuple))))))
    (let [^ArrayList out (when materialize? (ArrayList.))]
      (loop [i 0
             n 0]
        (if (== i cn)
          n
          (let [^objects consumer-tuple (aget consumer i)
                ^ArrayList run (.get table (aget consumer-tuple 0))]
            (if-not run
              (recur (inc i) n)
              (let [rn (.size run)]
                (when materialize?
                  (dotimes [j rn]
                    (append-pair! out (.get run j) consumer-tuple)))
                (recur (inc i) (+ n rn))))))))))

(defn merge-join
  "Walk two ordered tuple arrays, consuming complete equal-key runs on both
  sides. Returns the output cardinality; optionally materializes projected
  output tuples."
  ^long [^objects producer ^objects consumer materialize?]
  (let [pn (alength producer)
        cn (alength consumer)
        ^ArrayList out (when materialize? (ArrayList.))]
    (loop [pi 0
           ci 0
           n 0]
      (if (or (== pi pn) (== ci cn))
        n
        (let [^objects pt (aget producer pi)
              ^objects ct (aget consumer ci)
              pk (aget pt 0)
              ck (aget ct 0)
              cmp (long (datom/compare-value pk ck))]
          (cond
            (neg? cmp) (recur (inc pi) ci n)
            (pos? cmp) (recur pi (inc ci) n)
            :else
            (let [pi' (long (loop [i (inc pi)]
                              (if (and (< i pn)
                                       (zero? (datom/compare-value
                                               (aget ^objects (aget producer i) 0) pk)))
                                (recur (inc i))
                                i)))
                  ci' (long (loop [i (inc ci)]
                              (if (and (< i cn)
                                       (zero? (datom/compare-value
                                               (aget ^objects (aget consumer i) 0) ck)))
                                (recur (inc i))
                                i)))
                  prun (- pi' pi)
                  crun (- ci' ci)]
              (when materialize?
                (dotimes [p prun]
                  (let [^objects producer-tuple (aget producer (+ pi p))]
                    (dotimes [c crun]
                      (append-pair! out producer-tuple
                                    (aget consumer (+ ci c)))))))
              (recur pi' ci' (+ n (* prun crun))))))))))

(defn- thread-mx-bean []
  (let [bean (ManagementFactory/getThreadMXBean)]
    (when (instance? com.sun.management.ThreadMXBean bean)
      (let [^com.sun.management.ThreadMXBean bean bean]
        (when (and (.isThreadAllocatedMemorySupported bean)
                   (not (.isThreadAllocatedMemoryEnabled bean)))
          (.setThreadAllocatedMemoryEnabled bean true))
        bean))))

(defn- median [xs]
  (nth (vec (sort xs)) (quot (count xs) 2)))

(defn measure
  "Warm f, then report median wall time and current-thread allocation per call."
  ([f] (measure f identity))
  ([f summarize]
   (dotimes [_ 5] (f))
   (let [calibration-start (System/nanoTime)
         calibration-result (f)
         calibration-ns (max 1 (- (System/nanoTime) calibration-start))
         ;; Long enough to drown out nanoTime and nREPL noise, short enough that
         ;; materialized-output cases do not create giant garbage waves.
         iterations (long (max 1 (min 1000 (Math/ceil (/ 2.0e7 calibration-ns)))))
         bean (thread-mx-bean)
         tid (.threadId (Thread/currentThread))
         samples
         (vec
          (for [_ (range 7)]
            (let [a0 (when bean (.getThreadAllocatedBytes ^com.sun.management.ThreadMXBean bean tid))
                  t0 (System/nanoTime)
                  result (loop [i 0 result calibration-result]
                           (if (== i iterations)
                             result
                             (recur (inc i) (f))))
                  ns (- (System/nanoTime) t0)
                  allocated (when bean (- (.getThreadAllocatedBytes ^com.sun.management.ThreadMXBean bean tid) a0))]
              {:ns ns :allocated allocated :result (summarize result)})))]
     {:ms (/ (double (median (map :ns samples))) iterations 1e6)
      :allocated-mb (when bean (/ (double (median (map :allocated samples))) iterations 1048576.0))
      :iterations iterations
      :result (summarize calibration-result)})))

(defn measure-scenario [{:keys [id producer consumer description]} materialize?]
  (let [^objects p (apply make-tuples producer)
        ^objects c (apply make-tuples consumer)
        hash-result (measure #(hash-join p c materialize?))
        merge-result (measure #(merge-join p c materialize?))]
    (assert (= (:result hash-result) (:result merge-result)))
    {:id id
     :description description
     :producer-rows (alength p)
     :consumer-rows (alength c)
     :output-rows (:result hash-result)
     :materialize? materialize?
     :hash hash-result
     :merge merge-result
     :ratio (/ (:ms merge-result) (:ms hash-result))}))

(defn run-kernels
  ([] (run-kernels false))
  ([materialize?]
   (mapv #(measure-scenario % materialize?) scenarios)))

(defn print-results [results]
  (println (format "%-19s %9s %9s %10s %10s %10s %9s %9s %8s"
                   "shape" "producer" "consumer" "output" "hash ms" "merge ms"
                   "hash MB" "merge MB" "ratio"))
  (doseq [{:keys [id producer-rows consumer-rows output-rows hash merge ratio]} results]
    (println (format "%-19s %9d %9d %10d %10.3f %10.3f %9.2f %9.2f %8.2f"
                     (name id) producer-rows consumer-rows output-rows
                     (:ms hash) (:ms merge)
                     (or (:allocated-mb hash) 0.0) (or (:allocated-mb merge) 0.0)
                     ratio)))
  results)

;; ---------------------------------------------------------------------------
;; End-to-end query fixtures

(def ^:private long-query-schema
  [{:db/ident :l/key :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :l/payload :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :r/key :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one :db/index true}
   {:db/ident :r/filter :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one :db/index true}
   {:db/ident :r/payload :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}])

(def ^:private tuple-query-schema
  [{:db/ident :l/k1 :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :l/k2 :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :l/key :db/valueType :db.type/tuple :db/cardinality :db.cardinality/one
    :db/tupleAttrs [:l/k1 :l/k2] :db/unique :db.unique/identity}
   {:db/ident :l/payload :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :r/k1 :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :r/k2 :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :r/key :db/valueType :db.type/tuple :db/cardinality :db.cardinality/one
    :db/tupleAttrs [:r/k1 :r/k2] :db/index true}
   {:db/ident :r/filter :db/valueType :db.type/long :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :r/payload :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])

(defn- key-parts [^long k]
  [(quot k 1000) (mod k 1000)])

(defn query-db
  "Build a deterministic two-relation fixture. Right rows are ordered into
  `right-distinct` equal-key runs; right-offset controls overlap with the left
  key range. Returns {:conn :db} so the caller controls lifetime."
  [{:keys [key-kind left-rows right-rows right-distinct right-offset filter-distinct]
    :or {key-kind :long left-rows 5000 right-rows 50000
         right-distinct 5000 right-offset 0 filter-distinct 30000}}]
  (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
             :schema-flexibility :write
             :keep-history? false
             :attribute-refs? false
             :search-cache-size 0
             :value-caps :default
             :index :datahike.index/persistent-set}
        _ (d/create-database cfg)
        conn (d/connect cfg)
        tuple? (= key-kind :tuple)
        left-row (fn [i]
                   (if tuple?
                     (let [[k1 k2] (key-parts i)]
                       {:l/k1 k1 :l/k2 k2 :l/payload i})
                     {:l/key i :l/payload i}))
        right-row (fn [i]
                    (let [k (+ right-offset
                               (quot (* (long i) right-distinct) right-rows))]
                      (if tuple?
                        (let [[k1 k2] (key-parts k)]
                          {:r/k1 k1 :r/k2 k2
                           :r/filter (mod i filter-distinct) :r/payload i})
                        {:r/key k :r/filter (mod i filter-distinct) :r/payload i})))]
    (d/transact conn {:tx-data (if tuple? tuple-query-schema long-query-schema)})
    (doseq [batch (partition-all 10000 (map left-row (range left-rows)))]
      (d/transact conn {:tx-data (vec batch)}))
    (doseq [batch (partition-all 10000 (map right-row (range right-rows)))]
      (d/transact conn {:tx-data (vec batch)}))
    {:conn conn :db @conn
     :shape {:key-kind key-kind :left-rows left-rows :right-rows right-rows
             :right-distinct right-distinct :right-offset right-offset
             :filter-distinct filter-distinct}}))

(def broad-value-query
  '[:find ?lp ?rp
    :where
    [?l :l/key ?k]
    [?l :l/payload ?lp]
    [?r :r/key ?k]
    [?r :r/payload ?rp]])

(def semijoin-query
  '[:find ?rp
    :where
    [?l :l/key ?k]
    [?r :r/key ?k]
    [?r :r/payload ?rp]])

(def aggregate-query
  '[:find ?k ?lp (count ?r)
    :where
    [?l :l/key ?k]
    [?l :l/payload ?lp]
    [?r :r/key ?k]])

(def filtered-aggregate-query
  '[:find ?k ?lp (count ?r)
    :in $ ?max-filter
    :where
    [?l :l/key ?k]
    [?l :l/payload ?lp]
    [?r :r/key ?k]
    [?r :r/filter ?filter]
    [(<= ?filter ?max-filter)]])

(def branch-shape-query
  '[:find ?lp (count ?r)
    :in $ ?floor ?max-filter
    :where
    [?l :l/key ?k]
    [?l :l/payload ?lp]
    [(>= ?lp ?floor)]
    [?r :r/key ?k]
    [?r :r/filter ?filter]
    [(<= ?filter ?max-filter)]])

(defn measure-query
  "Measure the current compiled/hash path and relational fallback in the same
  warmed JVM. Result equality is checked before timing."
  [db query & inputs]
  ;; Plans contain data-dependent cardinalities but the current global cache
  ;; key does not contain data (#963). Every independently-built fixture must
  ;; therefore begin with a cold plan cache; timing below still measures the
  ;; normal warm-plan execution path.
  (vreset! @#'q/plan-cache (lru/lru q/lru-cache-size))
  (let [compiled-result (binding [q/*disable-planner* false
                                  q/*query-result-cache?* false]
                          (apply d/q query db inputs))
        fallback-result (binding [q/*disable-planner* true
                                  q/*query-result-cache?* false]
                          (apply d/q query db inputs))]
    (assert (= (set compiled-result) (set fallback-result)))
    {:rows (count compiled-result)
     :compiled (measure #(binding [q/*disable-planner* false
                                   q/*query-result-cache?* false]
                           (apply d/q query db inputs))
                        count)
     :fallback (measure #(binding [q/*disable-planner* true
                                   q/*query-result-cache?* false]
                           (apply d/q query db inputs))
                        count)}))

(defn print-query-result [label {:keys [rows compiled fallback] :as result}]
  (println (format "%-28s rows=%-8d compiled=%8.2fms/%7.1fMB fallback=%8.2fms/%7.1fMB ratio=%5.2fx"
                   label rows (:ms compiled) (:allocated-mb compiled)
                   (:ms fallback) (:allocated-mb fallback)
                   (/ (:ms compiled) (:ms fallback))))
  result)

(defn measure-ordered-query [db query & inputs]
  (vreset! @#'q/plan-cache (lru/lru q/lru-cache-size))
  (let [run (fn [enabled?]
              (binding [q/*disable-planner* false
                        q/*query-result-cache?* false
                        ioa/*enabled* enabled?]
                (apply d/q query db inputs)))
        baseline-result (run false)
        ordered-result (run true)]
    (assert (= (set baseline-result) (set ordered-result)))
    (let [baseline (measure #(run false) count)
          ordered (measure #(run true) count)]
      {:rows (count ordered-result)
       :baseline baseline
       :ordered ordered
       :time-ratio (/ (:ms ordered) (:ms baseline))
       :allocation-ratio (when (and (:allocated-mb ordered) (:allocated-mb baseline))
                           (/ (:allocated-mb ordered) (:allocated-mb baseline)))})))

(defn print-ordered-result
  [label {:keys [rows baseline ordered time-ratio allocation-ratio] :as result}]
  (println (format "%s rows=%d\n  relation %8.2fms / %7.1fMB\n  ordered  %8.2fms / %7.1fMB\n  ratios   %8.2fx / %7.2fx"
                   label rows
                   (:ms baseline) (:allocated-mb baseline)
                   (:ms ordered) (:allocated-mb ordered)
                   time-ratio (or allocation-ratio Double/NaN)))
  result)

(defn- entity-value-map
  ^HashMap [db attr]
  (let [out (HashMap.)]
    (doseq [d (d/datoms db {:index :aevt :components [attr]})]
      (.put out (:e d) (:v d)))
    out))

(defn avet-merge-query
  "Hand-written oracle for the broad-value query. Both key attributes are read
  in AVET order, while payloads are materialized by entity. This deliberately
  exposes the physical-index trade-off hidden by the current AEVT entity-group
  plan; it is not production executor code."
  [db materialize?]
  (let [^HashMap left-payloads (entity-value-map db :l/payload)
        ^HashMap right-payloads (entity-value-map db :r/payload)
        left (vec (d/datoms db {:index :avet :components [:l/key]}))
        right (vec (d/datoms db {:index :avet :components [:r/key]}))
        ln (count left)
        rn (count right)
        ^java.util.HashSet out (when materialize? (java.util.HashSet.))]
    (loop [li 0 ri 0 n 0]
      (if (or (== li ln) (== ri rn))
        (if materialize? (.size out) n)
        (let [ld (nth left li)
              rd (nth right ri)
              lk (:v ld)
              rk (:v rd)
              cmp (long (datom/compare-value lk rk))]
          (cond
            (neg? cmp) (recur (inc li) ri n)
            (pos? cmp) (recur li (inc ri) n)
            :else
            (let [li' (long (loop [i (inc li)]
                              (if (and (< i ln)
                                       (zero? (datom/compare-value (:v (nth left i)) lk)))
                                (recur (inc i))
                                i)))
                  ri' (long (loop [i (inc ri)]
                              (if (and (< i rn)
                                       (zero? (datom/compare-value (:v (nth right i)) rk)))
                                (recur (inc i))
                                i)))]
              (when materialize?
                (doseq [lidx (range li li')
                        ridx (range ri ri')]
                  (.add out [(.get left-payloads (:e (nth left lidx)))
                             (.get right-payloads (:e (nth right ridx)))])))
              (recur li' ri' (+ n (* (- li' li) (- ri' ri)))))))))))

(defn avet-merge-aggregate
  "Oracle for aggregate-query: exploit the matching consumer run directly
  instead of materializing every joined pair before grouping it again."
  [db]
  (let [^HashMap left-payloads (entity-value-map db :l/payload)
        left (vec (d/datoms db {:index :avet :components [:l/key]}))
        right (vec (d/datoms db {:index :avet :components [:r/key]}))
        ln (count left)
        rn (count right)
        ^java.util.HashSet out (java.util.HashSet.)]
    (loop [li 0 ri 0]
      (if (or (== li ln) (== ri rn))
        out
        (let [ld (nth left li)
              rd (nth right ri)
              lk (:v ld)
              rk (:v rd)
              cmp (long (datom/compare-value lk rk))]
          (cond
            (neg? cmp) (recur (inc li) ri)
            (pos? cmp) (recur li (inc ri))
            :else
            (let [li' (long (loop [i (inc li)]
                              (if (and (< i ln)
                                       (zero? (datom/compare-value (:v (nth left i)) lk)))
                                (recur (inc i))
                                i)))
                  ri' (long (loop [i (inc ri)]
                              (if (and (< i rn)
                                       (zero? (datom/compare-value (:v (nth right i)) rk)))
                                (recur (inc i))
                                i)))
                  right-count (- ri' ri)]
              (doseq [lidx (range li li')]
                (let [left-datom (nth left lidx)]
                  (.add out [lk (.get left-payloads (:e left-datom)) right-count])))
              (recur li' ri'))))))))

(defn avet-merge-filtered-aggregate
  "Oracle for filtered-aggregate-query. It scans the foreign key in join-key
  order, looks up the predicate value by entity, and counts qualifying rows in
  each equal-key run without materializing the intermediate join relation."
  [db max-filter]
  (let [^HashMap left-payloads (entity-value-map db :l/payload)
        ^HashMap right-filters (entity-value-map db :r/filter)
        left (vec (d/datoms db {:index :avet :components [:l/key]}))
        right (vec (d/datoms db {:index :avet :components [:r/key]}))
        ln (count left)
        rn (count right)
        ^java.util.HashSet out (java.util.HashSet.)]
    (loop [li 0 ri 0]
      (if (or (== li ln) (== ri rn))
        out
        (let [ld (nth left li)
              rd (nth right ri)
              lk (:v ld)
              rk (:v rd)
              cmp (long (datom/compare-value lk rk))]
          (cond
            (neg? cmp) (recur (inc li) ri)
            (pos? cmp) (recur li (inc ri))
            :else
            (let [li' (long (loop [i (inc li)]
                              (if (and (< i ln)
                                       (zero? (datom/compare-value (:v (nth left i)) lk)))
                                (recur (inc i))
                                i)))
                  ri' (long (loop [i (inc ri)]
                              (if (and (< i rn)
                                       (zero? (datom/compare-value (:v (nth right i)) rk)))
                                (recur (inc i))
                                i)))
                  right-count (loop [i ri n 0]
                                (if (== i ri')
                                  n
                                  (recur (inc i)
                                         (if (<= (long (.get right-filters
                                                             (:e (nth right i))))
                                                 (long max-filter))
                                           (inc n)
                                           n))))]
              (when (pos? right-count)
                (doseq [lidx (range li li')]
                  (let [left-datom (nth left lidx)]
                    (.add out [lk (.get left-payloads (:e left-datom)) right-count]))))
              (recur li' ri'))))))))

(defn filter-hash-aggregate
  "Alternative oracle for filtered-aggregate-query. Drive the selective filter
  AVET, fetch each qualifying entity's join key, aggregate counts in a hash map,
  then probe it while walking the left key AVET. This is the competing strategy
  for selective predicates; unlike avet-merge-filtered-aggregate it need not scan
  every right-side join key."
  [db max-filter]
  (let [^HashMap left-payloads (entity-value-map db :l/payload)
        ^HashMap counts (HashMap.)
        filtered (take-while #(<= (long (:v %)) (long max-filter))
                             (d/datoms db {:index :avet :components [:r/filter]}))]
    (doseq [filter-datom filtered]
      (let [k (:r/key (d/entity db (:e filter-datom)))]
        (.put counts k (inc (long (or (.get counts k) 0))))))
    (let [^java.util.HashSet out (java.util.HashSet.)]
      (doseq [left-datom (d/datoms db {:index :avet :components [:l/key]})]
        (let [k (:v left-datom)
              n (.get counts k)]
          (when n
            (.add out [k (.get left-payloads (:e left-datom)) n]))))
      out)))

(defn -main [& args]
  (let [query? (boolean (some #{"--query"} args))
        scale? (boolean (some #{"--scale"} args))
        assert? (boolean (some #{"--assert"} args))]
    (if-not query?
      (print-results (run-kernels (boolean (some #{"--materialize"} args))))
      (let [left-rows (if scale? 30000 5000)
            right-rows (* 10 left-rows)
            fixture (query-db {:key-kind :tuple
                               :left-rows left-rows
                               :right-rows right-rows
                               :right-distinct left-rows
                               :filter-distinct 30000})
            result (print-ordered-result
                    (format "ordered aggregate %d x %d" left-rows right-rows)
                    (measure-ordered-query (:db fixture) branch-shape-query 0 15000))
            bad? (or (> (:time-ratio result) 0.75)
                     (and (:allocation-ratio result)
                          (> (:allocation-ratio result) 0.80)))]
        (when (and assert? bad?)
          (println "REGRESSION: ordered aggregate missed its relative performance bounds")
          (System/exit 1))))))
