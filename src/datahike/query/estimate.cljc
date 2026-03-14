(ns datahike.query.estimate
  "Cardinality estimation for the compiled query engine.
   Uses PSS count-slice for O(log n) cardinality estimates per pattern."
  (:require
   [datahike.constants :refer [e0 tx0 emax txmax]]
   [datahike.datom :as datom :refer [datom]]
   [datahike.db.interface :as dbi]
   [datahike.db.utils :as dbu]
   [datahike.index.interface :as di]
   [datahike.query.analyze :as analyze]
   [taoensso.timbre :as log]))

#?(:clj (set! *warn-on-reflection* true))

;; ---------------------------------------------------------------------------
;; Comparators for count-slice

(defn cmp-entity-only
  "Compare datoms by entity field only."
  [^datahike.datom.Datom d1 ^datahike.datom.Datom d2]
  (compare (.-e d1) (.-e d2)))

(defn cmp-attr-only
  "Compare datoms by attribute field only."
  [^datahike.datom.Datom d1 ^datahike.datom.Datom d2]
  (datom/cmp-attr-quick (.-a d1) (.-a d2)))

;; ---------------------------------------------------------------------------
;; Pattern cardinality estimation

(defn estimate-pattern
  "Estimate the cardinality (number of matching datoms) for a pattern clause.
   Uses count-slice on the appropriate index with partial comparators.
   Returns a long estimate, or nil if estimation is not possible."
  [db pattern-info schema-info]
  (let [{:keys [e a v tx]} pattern-info
        ground? (fn [x] (and (some? x) (not (symbol? x))))
        e-ground? (ground? e)
        a-ground? (ground? a)
        v-ground? (ground? v)
        ;; Resolve attribute for attribute-refs databases
        ;; Guard with keyword? to avoid double-resolution when attrs are already numeric
        resolved-a (when a-ground?
                     (if (and (:attribute-refs? (dbi/-config db)) (keyword? a))
                       (dbi/-ref-for db a)
                       a))]
    (cond
      ;; [e a ?v] — single datom for card-one, count EA range for card-many
      (and e-ground? a-ground? (not v-ground?))
      (if (:card-one? schema-info)
        1
        (di/-count-slice (:eavt db)
                         (datom (long e) resolved-a nil tx0)
                         (datom (long e) resolved-a nil txmax)
                         (datom/index-type->cmp-replace :eavt)))

      ;; [e a v] — exact point, 0 or 1
      (and e-ground? a-ground? v-ground?)
      1

      ;; [?e a v] — count on AVET if indexed, otherwise estimate from AEVT attr count
      (and (not e-ground?) a-ground? v-ground?)
      (if (:indexed? schema-info)
        (di/-count-slice (:avet db)
                         (datom e0 resolved-a v tx0)
                         (datom emax resolved-a v txmax)
                         datom/cmp-datoms-av-only)
        ;; Without AVET, estimate: (attr-count / 10) — 10% selectivity on value
        (quot (di/-count-slice (:aevt db)
                               (datom e0 resolved-a nil tx0)
                               (datom emax resolved-a nil txmax)
                               cmp-attr-only)
              10))

      ;; [?e a ?v] — all datoms for attribute
      (and (not e-ground?) a-ground? (not v-ground?))
      (di/-count-slice (:aevt db)
                       (datom e0 resolved-a nil tx0)
                       (datom emax resolved-a nil txmax)
                       cmp-attr-only)

      ;; [e ?a ?v] — all datoms for entity
      (and e-ground? (not a-ground?) (not v-ground?))
      (di/-count-slice (:eavt db)
                       (datom (long e) nil nil tx0)
                       (datom (long e) nil nil txmax)
                       cmp-entity-only)

      ;; [?e ?a ?v] — full scan (worst case)
      :else
      (di/-count (:eavt db)))))

(defn estimate-predicate-selectivity-heuristic
  "Heuristic selectivity estimate for a predicate operator.
   Used as fallback when sampling is not possible."
  [op]
  (case op
    (= ==) 0.1
    (not= !=) 0.9
    (> < >= <=) 0.33
    0.5))

(def ^:private ^:const sample-size 64)

(defn sample-predicate-selectivity
  "Sample datoms from the index and apply the predicate to estimate
   actual selectivity. Returns a double in (0.01, 1.0].

   Uses prefix sampling: takes the first N datoms from the index slice.

   Parameters:
   - db: the database
   - scan-op: the pattern-scan op that provides the values
   - pred-op: symbol of the comparison operator (e.g. '> '< '>=)
   - pred-var: the variable in the pattern that the predicate filters
   - const-val: the constant value the predicate compares against

   Falls back to heuristic if sampling fails or yields no datoms."
  [db scan-op pred-op pred-var const-val]
  (try
    (let [clause (:clause scan-op)
          ;; Determine which datom field pred-var maps to in the scan clause
          var-field (some (fn [[i sym]]
                           (when (= sym pred-var) i))
                         (map-indexed vector clause))
          _ (when-not var-field
              (throw (ex-info "predicate var not found in scan clause"
                              {:pred-var pred-var :clause clause})))
          a-val (nth clause 1)
          resolved-a (if (and a-val (not (symbol? a-val)))
                       (if (:attribute-refs? (dbi/-config db))
                         (if (keyword? a-val)
                           (dbi/-ref-for db a-val)
                           a-val) ;; already numeric, don't re-resolve
                         a-val)
                       nil)
          ;; Use AEVT for attribute-scoped sampling (ordered by entity)
          index-key (or (:index scan-op) :aevt)
          index (get db index-key)
          _ (when-not index
              (throw (ex-info "index not available for selectivity sampling"
                              {:index-key index-key})))
          ;; Build from/to datoms
          from-d (case index-key
                   :eavt (datom e0 nil nil tx0)
                   :aevt (if resolved-a
                           (datom e0 resolved-a nil tx0)
                           (datom e0 nil nil tx0))
                   :avet (if resolved-a
                           (datom e0 resolved-a nil tx0)
                           (datom e0 nil nil tx0)))
          to-d (case index-key
                 :eavt (datom emax nil nil txmax)
                 :aevt (if resolved-a
                         (datom emax resolved-a nil txmax)
                         (datom emax nil nil txmax))
                 :avet (if resolved-a
                         (datom emax resolved-a nil txmax)
                         (datom emax nil nil txmax)))
          index-type (case index-key :eavt :eavt :aevt :aevt :avet :avet :eavt)
          ;; Sample first N datoms from the slice. For randomly distributed
          ;; attribute values this gives representative selectivity estimates.
          ;; Cost: ~64 datom reads = ~3µs (negligible vs execution).
          datoms (into [] (take sample-size) (di/-slice index from-d to-d index-type))
          n-sampled (count datoms)]
      (if (zero? n-sampled)
        (estimate-predicate-selectivity-heuristic pred-op)
        (let [;; Extract the value from each datom based on var-field position
              extract-fn (case (int var-field)
                           0 (fn [^datahike.datom.Datom d] (.-e d))
                           1 (fn [^datahike.datom.Datom d] (.-a d))
                           2 (fn [^datahike.datom.Datom d] (.-v d))
                           3 (fn [^datahike.datom.Datom d] (.-tx d)))
              ;; Resolve the comparison fn
              cmp-fn (case pred-op
                       >  #(> (compare %1 %2) 0)
                       <  #(< (compare %1 %2) 0)
                       >= #(>= (compare %1 %2) 0)
                       <= #(<= (compare %1 %2) 0)
                       =  #(= %1 %2)
                       == #(== %1 %2)
                       not= #(not= %1 %2)
                       nil)
              _ (when-not cmp-fn
                  (throw (ex-info "unsupported predicate operator for sampling"
                                  {:pred-op pred-op
                                   :supported '(> < >= <= = == not=)})))
              passing (count (filter (fn [d]
                                       (let [v (extract-fn d)]
                                         (try (cmp-fn v const-val)
                                              (catch #?(:clj Exception :cljs :default) e
                                                (log/debug "selectivity sample comparison failed:"
                                                           {:value v :const const-val :error #?(:clj (.getMessage ^Exception e) :cljs (str e))})
                                                false))))
                                     datoms))
              rate (/ (double passing) n-sampled)]
          (min 1.0 (max 0.01 rate)))))
    (catch #?(:clj Exception :cljs :default) e
      (log/debug "predicate selectivity sampling failed, using heuristic:"
                 {:pred-op pred-op :error #?(:clj (.getMessage ^Exception e) :cljs (str e))})
      (estimate-predicate-selectivity-heuristic pred-op))))

(defn estimate-pushdown-range
  "Estimate cardinality after applying pushdown predicates to a pattern.
   Uses sampled predicate selectivity when db and scan-op are provided,
   falls back to heuristic otherwise."
  ([base-estimate pushdown-preds]
   (reduce (fn [est pred]
             (long (* est (estimate-predicate-selectivity-heuristic (:op pred)))))
           base-estimate
           pushdown-preds))
  ([base-estimate pushdown-preds db scan-op]
   (reduce (fn [est pred]
             (let [sel (if (and db scan-op (:var pred) (:const-val pred))
                         (sample-predicate-selectivity db scan-op (:op pred)
                                                      (:var pred) (:const-val pred))
                         (estimate-predicate-selectivity-heuristic (:op pred)))]
               (long (* est sel))))
           base-estimate
           pushdown-preds)))

;; ---------------------------------------------------------------------------
;; Conditional pass-rate estimation for merge ordering

(defn estimate-conditional-pass-rate
  "P(entity passes merge | entity from scan). Returns double in (0.001, 1.0].
   merge-count = estimated cardinality for the merge pattern.
   total-entities = total entity count in the DB."
  [merge-card total-entities]
  (let [total (max 1 (long total-entities))
        rate (/ (double merge-card) total)]
    (min 1.0 (max 0.001 rate))))

(defn estimate-total-entities
  "Estimate total distinct entities in the database.
   NOTE: counts total datoms, not distinct entities. This overestimates
   for entities with many attributes but is acceptable for merge ordering."
  [db]
  (let [eavt (:eavt db)]
    (max 1 (di/-count eavt))))
