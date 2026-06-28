(ns benchmark.planner-regression
  "Planner regression suite — the 'planner is never much slower than the base
   engine' contract, on the query shapes that have historically regressed when the
   compiled query planner is turned on (validated against jobtech-taxonomy-api).

   Every shape runs the SAME query twice — once through the COMPILED planner
   (`*disable-planner* false`) and once through the relational BASE engine
   (`*disable-planner* true`) — on the SAME db, and reports `ratio = compiled/base`.
   A correctness check (compiled result set == base result set) is piggy-backed on
   each shape, so a planner *correctness* regression fails the suite too.

   Shapes (the families this PR fixed):
     sip-single        :in-bound single pattern            (SIP point-lookups)
     sip-entity-group  :in-bound scan + merge              (fused entity-group)
     rule-or           rule body with an OR (fwd/rev edge) (the jobtech `edge` rule)
     recursive-ground  ground-rooted recursive rule        (selective-generator ordering)
     asof-entity-group :in-bound entity-group over as-of   (fused temporal entity-group)
     asof-recursive    recursive rule over a DATE as-of    (temporal recursion + date→tx cache)

   Run (human table):
     clj -M:bench-compare -m benchmark.planner-regression
   CI gate (exit 1 if any shape regressed — wrong results, or planner > THRESHOLD x
   base while the base time is above a noise floor):
     clj -M:bench-compare -m benchmark.planner-regression --assert

   Tunables (env): PR_NODES (current-db node count), PR_TXES (as-of history depth),
   PR_THRESHOLD (max compiled/base ratio), PR_FLOOR_MS (don't gate below this base
   time — bench noise dominates sub-ms ratios)."
  (:require
   [datahike.api :as d]
   [datahike.query :as q]
   [benchmark.datascript-bench :as dsb])
  (:import [java.util Date]))

;; ---------------------------------------------------------------------------
;; Synthetic stores

(def schema
  [{:db/ident :node/id   :db/valueType :db.type/long :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :node/data :db/valueType :db.type/long :db/cardinality :db.cardinality/one :db/index true}
   {:db/ident :edge/from :db/valueType :db.type/ref  :db/cardinality :db.cardinality/one}
   {:db/ident :edge/to   :db/valueType :db.type/ref  :db/cardinality :db.cardinality/one}])

(defn- fresh-conn [history?]
  (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
             :schema-flexibility :write
             :keep-history? history?
             :search-cache-size 0
             :index :datahike.index/persistent-set}]
    (d/delete-database cfg)
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn {:tx-data schema})
      conn)))

(defn current-db
  "n nodes (unique :node/id; sparse :node/data on ~10%) in ONE transaction, plus a
   LINEAR chain of edges i->i+1. Linear (not a ring) so a ground-rooted `reaches`
   from a node near the end reaches only a few nodes while :node/id stays large —
   the shape where the recursive-rule-must-lead ordering actually matters."
  [n]
  (let [conn (fresh-conn false)]
    (doseq [batch (partition-all 5000 (range n))]
      (d/transact conn {:tx-data (mapv (fn [i] (cond-> {:node/id i}
                                                 (zero? (mod i 10)) (assoc :node/data (* i 7))))
                                       batch)}))
    (doseq [batch (partition-all 5000 (range (dec n)))]
      (d/transact conn {:tx-data (mapv (fn [i] {:edge/from [:node/id i] :edge/to [:node/id (inc i)]})
                                       batch)}))
    (d/db conn)))

(defn temporal-conn
  "A keep-history conn built over n-tx INCREMENTAL transactions (one node + linking
   edge each), so the `:db/txInstant` log has ~n-tx entries — enough that a
   date-based as-of's date->tx-id resolution is non-trivial (the thing the
   resolution cache guards). Returns the live conn (so callers can take as-of)."
  [n-tx]
  (let [conn (fresh-conn true)]
    (d/transact conn {:tx-data [{:node/id 0 :node/data 0}]})
    (doseq [i (range 1 n-tx)]
      (d/transact conn {:tx-data [(cond-> {:node/id i}
                                    (zero? (mod i 10)) (assoc :node/data (* i 7)))
                                  {:edge/from [:node/id (dec i)] :edge/to [:node/id i]}]}))
    conn))

(def edge-rules
  '[[(linked ?a ?b ?r)
     (or (and [?r :edge/from ?a] [?r :edge/to ?b])
         (and [?r :edge/to ?a]   [?r :edge/from ?b]))]])

(def reaches-rules
  '[[(reaches ?a ?b) [?r :edge/from ?a] [?r :edge/to ?b]]
    [(reaches ?a ?b) [?r :edge/from ?a] [?r :edge/to ?c] (reaches ?c ?b)]])

;; ---------------------------------------------------------------------------
;; Shapes — each {:id :desc :ctx <built once> :run (fn [ctx] result-seq)}

(defn- shapes [{:keys [nodes txes]}]
  (let [cdb   (current-db nodes)
        tconn (temporal-conn txes)
        tnum  (d/as-of @tconn (:max-tx @tconn))   ;; numeric-tx as-of
        tdate (d/as-of @tconn (Date.))            ;; date as-of (exercises date->tx cache)
        ids-1k  (vec (range (min 1000 nodes)))
        ids-100 (vec (range (min 100 nodes)))
        ids-500 (vec (range (min 500 txes)))
        ;; a root near the chain end → reaches only a handful, while :node/id is huge.
        ;; Pass the resolved ENTITY id (not a [:node/id n] lookup-ref — engines don't
        ;; bind a lookup-ref uniformly as an :in scalar).
        eid-of  (fn [db id] (ffirst (d/q '[:find ?e :in $ ?id :where [?e :node/id ?id]] db id)))
        root    (eid-of cdb (- nodes 50))
        troot   (eid-of tdate (- txes 50))]
    [{:id :sip-single :desc "[?e :node/id ?id]  :in [?id ...]"
      :ctx {:db cdb :ids ids-1k}
      :run (fn [{:keys [db ids]}]
             (d/q '{:find [?id] :in [$ [?id ...]] :where [[?e :node/id ?id]]} db ids))}

     {:id :sip-entity-group :desc "[?e :node/id ?id][?e :node/data ?d]  :in [?id ...]"
      :ctx {:db cdb :ids ids-1k}
      :run (fn [{:keys [db ids]}]
             (d/q '{:find [?id ?d] :in [$ [?id ...]] :where [[?e :node/id ?id] [?e :node/data ?d]]} db ids))}

     {:id :rule-or :desc "(linked ?a ?b ?r) — rule body with OR  :in [?ida ...]"
      :ctx {:db cdb :ids ids-100}
      :run (fn [{:keys [db ids]}]
             (d/q '{:find [?ida ?idb] :in [$ % [?ida ...]]
                    :where [[?a :node/id ?ida] (linked ?a ?b ?r) [?b :node/id ?idb]]}
                  db edge-rules ids))}

     {:id :recursive-ground :desc "(reaches ?start ?b)[?b :node/id ?bid] — ground root"
      :ctx {:db cdb :start root}
      :run (fn [{:keys [db start]}]
             (d/q '{:find [?bid] :in [$ % ?start]
                    :where [(reaches ?start ?b) [?b :node/id ?bid]]}
                  db reaches-rules start))}

     {:id :asof-entity-group :desc "[?e :node/id ?id][?e :node/data ?d]  :in over as-of (numeric tx)"
      :ctx {:db tnum :ids ids-500}
      :run (fn [{:keys [db ids]}]
             (d/q '{:find [?id ?d] :in [$ [?id ...]] :where [[?e :node/id ?id] [?e :node/data ?d]]} db ids))}

     {:id :asof-recursive :desc "(reaches ?start ?b)[?b :node/id ?bid] over a DATE as-of"
      :ctx {:db tdate :start troot}
      :run (fn [{:keys [db start]}]
             (d/q '{:find [?bid] :in [$ % ?start]
                    :where [(reaches ?start ?b) [?b :node/id ?bid]]}
                  db reaches-rules start))}]))

;; ---------------------------------------------------------------------------
;; Measurement

(defn- measure-shape [{:keys [id desc ctx run]} threshold floor-ms]
  (let [on-res  (binding [q/*disable-planner* false] (run ctx))
        off-res (binding [q/*disable-planner* true]  (run ctx))
        agree?  (= (set on-res) (set off-res))
        on      (binding [q/*disable-planner* false] (dsb/bench (run ctx)))
        off     (binding [q/*disable-planner* true]  (dsb/bench (run ctx)))
        ratio   (/ on (max 1e-6 off))
        status  (cond
                  (not agree?)                            :WRONG
                  (and (> ratio threshold) (> off floor-ms)) :SLOW
                  :else                                   :OK)]
    {:id id :desc desc :on on :off off :ratio ratio :n (count on-res) :status status}))

(defn -main [& args]
  (let [assert? (some #{"--assert"} args)
        envl    (fn [k d] (if-let [v (System/getenv k)] (read-string v) d))
        nodes   (envl "PR_NODES" 4000)
        txes    (envl "PR_TXES" 300)
        threshold (double (envl "PR_THRESHOLD" 2.0))
        floor-ms  (double (envl "PR_FLOOR_MS" 3.0))]
    ;; result cache hides re-execution cost (+ a known staleness bug, #808)
    (alter-var-root #'q/*query-result-cache?* (constantly false))
    ;; lighter timing than the dsb defaults (2s windows) — stable enough for a ratio
    (binding [dsb/*warmup-t* 300 dsb/*bench-t* 300 dsb/*repeats* 5]
      (println (format "\nPlanner regression — %d nodes, %d-tx history, threshold %.1fx, floor %.1fms%s\n"
                       nodes txes threshold floor-ms (if assert? "  [--assert]" "")))
      (println (format "%-20s %10s %10s %8s %8s  %-6s %s"
                       "shape" "compiled" "base" "ratio" "rows" "status" "description"))
      (println (apply str (repeat 110 "-")))
      (flush)
      ;; Build the synthetic stores, then measure shape-by-shape, FLUSHING each
      ;; line as it lands. The build + each measurement is otherwise silent for
      ;; tens of seconds; streaming progress keeps CI's no-output watchdog from
      ;; tripping on a run that is in fact making progress.
      (println (format ";; building synthetic stores (%d nodes + %d-tx history) …" nodes txes))
      (flush)
      (let [shps    (shapes {:nodes nodes :txes txes})
            _       (do (println ";; stores ready — measuring shapes …") (flush))
            results (mapv (fn [{:keys [id desc on off ratio n status] :as r}]
                            (println (format "%-20s %9sms %9sms %7sx %8d  %-6s %s"
                                             (name id) (dsb/round on) (dsb/round off) (dsb/round ratio) n (name status) desc))
                            (flush)
                            r)
                          (map #(measure-shape % threshold floor-ms) shps))]
        (let [bad (filterv #(not= :OK (:status %)) results)]
          (println)
          (if (seq bad)
            (do (println (format "REGRESSIONS: %s"
                                 (mapv (fn [{:keys [id status ratio]}]
                                         (format "%s(%s %.2fx)" (name id) (name status) (double ratio)))
                                       bad)))
                (when assert? (System/exit 1)))
            (println "OK — planner ≤ threshold (and results agree) on all shapes.")))))))
