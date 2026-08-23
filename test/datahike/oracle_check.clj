(ns datahike.oracle-check
  "Three-way comparison harness: base engine / planner / naive oracle.

   Every run of a case produces one of
     [:ok v]           a result
     [:raised msg]     the implementation refused the query
     [:unsupported m]  the ORACLE does not cover this shape (never a mismatch)
     [:timeout]        did not finish in the budget

   `classify` turns the triple into one of a small set of verdicts, which is
   what makes triage fast: a human only ever has to look at the cases whose
   verdict is :oracle-vs-both or :three-way."
  (:require
   [datahike.api :as d]
   [datahike.query :as q]
   [datahike.oracle :as o]))

(def ^:dynamic *timeout-ms* 5000)

(defn- with-timeout [f]
  (let [fut (future (f))
        r (deref fut *timeout-ms* ::timeout)]
    (when (= r ::timeout) (future-cancel fut))
    (if (= r ::timeout) [:timeout] r)))

(defn- attempt [f]
  (with-timeout
    (fn []
      (try [:ok (f)]
           (catch clojure.lang.ExceptionInfo e
             (if (:oracle/unsupported (ex-data e))
               [:unsupported (ex-message e)]
               [:raised (ex-message e)]))
           (catch Throwable e [:raised (str (.getMessage e))])))))

(defn run-engine [disable? query args opts]
  (attempt (fn []
             (binding [q/*disable-planner* disable?
                       q/*query-result-cache?* false]
               (if (seq opts)
                 (d/q (assoc opts :query query :args (vec args)))
                 (apply d/q query args))))))

(defn run-oracle [query args]
  (attempt (fn [] (apply o/q query args))))

(defn warm-up!
  "The planner's aggregate path class-loads a columnar delegate on first use —
   9.4s on this machine, which a per-case timeout would report as a hang. Run
   one query of each shape family before timing anything."
  [db]
  (doseq [qry ['[:find ?e :where [?e :db/ident ?i]]
               '[:find (count ?e) :where [?e :db/ident ?i]]
               '[:find ?e ?i :with ?i :where [?e :db/ident ?i]]]]
    (doseq [d [true false]]
      (try (binding [q/*disable-planner* d q/*query-result-cache?* false]
             (d/q qry db))
           (catch Throwable _ nil)))))

(defn- norm
  "Order-insensitive comparison form: every relation-shaped result becomes a
   MULTISET of row vectors.

   Multiset rather than set, and for both containers, because the engines are
   not consistent about the container they return — the same query can come
   back as #{} from one path and [] from another, and a :with query returns a
   VECTOR with intentional duplicates. Comparing multisets makes the container
   irrelevant while still catching a missing, extra or duplicated row.
   `duplicates` below reports the container-level claim separately."
  [r]
  (if (coll? r)
    (frequencies (map #(if (sequential? %) (vec %) %) r))
    r))

(defn duplicates
  "Rows a result claims more than once. For a query with no :with and no
   aggregate this must be empty — the answer is a set."
  [r]
  (when (coll? r)
    (into {} (filter (fn [[_ n]] (> n 1))) (frequencies (map vec (filter sequential? r))))))

(defn same? [a b]
  (and (= (first a) (first b))
       (or (not= :ok (first a)) (= (norm (second a)) (norm (second b))))))

(defn classify [base planner oracle]
  (cond
    (= :unsupported (first oracle)) :oracle-skip
    (= :timeout (first oracle))     :oracle-timeout
    (and (same? base planner) (same? base oracle)) :agree
    ;; the blind-spot verdict: both engines agree, the oracle does not
    (and (same? base planner) (not (same? base oracle))) :oracle-vs-both
    (and (not (same? base planner)) (same? base oracle)) :planner-bug
    (and (not (same? base planner)) (same? planner oracle)) :base-bug
    :else :three-way))

(defn check
  "Run one case three ways. `case-map` = {:query q :args [db …] :opts {} :label l}"
  [{:keys [query args opts label]}]
  (let [base (run-engine true query args opts)
        planner (run-engine false query args opts)
        oracle (run-oracle query args)]
    {:label label :query query :opts opts
     :base base :planner planner :oracle oracle
     :verdict (classify base planner oracle)}))

(defn report-line [{:keys [label query base planner oracle verdict]}]
  (str verdict " " (when label (str "[" label "] ")) (pr-str query)
       "\n    base    " (pr-str base)
       "\n    planner " (pr-str planner)
       "\n    oracle  " (pr-str oracle)))

(defn summarize [results]
  (let [by (group-by :verdict results)]
    {:counts (into (sorted-map) (map (fn [[k v]] [k (count v)])) by)
     :interesting (vec (mapcat by [:oracle-vs-both :three-way :planner-bug :base-bug]))}))
