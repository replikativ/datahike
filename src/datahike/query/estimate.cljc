(ns datahike.query.estimate
  "Cardinality estimation for the query planner.
   Uses PSS count-slice for O(log n) cardinality estimates per pattern."
  (:require
   [datahike.constants :refer [e0 tx0 emax txmax]]
   [datahike.datom :as datom :refer [datom]]
   [datahike.db.interface :as dbi]
   [datahike.db.utils :as dbu]
   [datahike.index.interface :as di]
   [datahike.tools :as dt]
   [is.simm.partial-cps.async :as pca :refer [async]]
   #?(:cljs [org.replikativ.persistent-sorted-set.btset :as btset])
   [datahike.query.analyze :as analyze]
   [replikativ.logging :as log]))

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

(def ^:private ^:const sample-size 64)

(def ^:private heuristic-warned? (atom false))

;; ---------------------------------------------------------------------------
;; Heuristic fallback estimation (no subtree counts available)

(def memo-key
  "When present on the db value, estimator storage reads consult this memo
   (an atom of {:recording? bool :values {k v} :requests {k descriptor}}):
   value hit → pure; recording miss → log the request, return a fallback
   (the plan built from fallbacks is DISCARDED); otherwise → plain sync read
   (the JVM / warm path, unchanged). This is what makes query PLANNING work
   with zero warmup on cold async-only stores: a record pass enumerates the
   exact reads, they are fetched concurrently, and the real plan runs pure."
  :datahike.query.estimate/memo)

(defn- memo-count-slice [db index-kw from to cmp fallback]
  (let [memo (get db memo-key)]
    (if (nil? memo)
      (di/-count-slice (get db index-kw) from to cmp)
      (let [k [:count-slice index-kw from to]
            {:keys [values recording?]} @memo]
        (if-some [v (get values k)]
          v
          (if recording?
            (do (swap! memo update :requests assoc k
                       {:op :count-slice :index (get db index-kw) :from from :to to :cmp cmp})
                fallback)
            (di/-count-slice (get db index-kw) from to cmp)))))))

(defn- memo-count-index [db index-kw fallback]
  (let [memo (get db memo-key)]
    (if (nil? memo)
      (di/-count (get db index-kw))
      (let [k [:count index-kw]
            {:keys [values recording?]} @memo]
        (if-some [v (get values k)]
          v
          (if recording?
            (do (swap! memo update :requests assoc k {:op :count :index (get db index-kw)})
                fallback)
            (di/-count (get db index-kw))))))))

(defn- memo-sample
  "First-n datoms of a slice as a vector ([] fallback while recording)."
  [db index-kw from to itype n]
  (let [memo (get db memo-key)]
    (if (nil? memo)
      (into [] (take n) (di/-slice (get db index-kw) from to itype))
      (let [k [:sample index-kw from to n]
            {:keys [values recording?]} @memo]
        (if-some [v (get values k)]
          v
          (if recording?
            (do (swap! memo update :requests assoc k
                       {:op :sample :index (get db index-kw) :from from :to to :itype itype :n n})
                [])
            (into [] (take n) (di/-slice (get db index-kw) from to itype))))))))

#?(:cljs
   (defn fetch-estimate-requests-step
     "Asynchronously execute every recorded estimator read (concurrently via
      pca/all), fill :values, and flip :recording? off. Returns an async
      expression yielding the memo."
     [memo]
     (async
      (let [reqs (vec (:requests @memo))
            exprs (mapv (fn [[k {:keys [op index from to cmp itype n]}]]
                          (case op
                            :count-slice
                            (async [k (pca/await (di/-count-slice index from to cmp {:sync? false}))])
                            :count
                            (async [k (pca/await (di/-count index {:sync? false}))])
                            :sample
                            (async [k (let [s0 (pca/await (di/-slice index from to itype {:sync? false}))]
                                        (loop [s s0 acc []]
                                          (if (or (nil? s) (>= (count acc) n))
                                            acc
                                            (let [step (pca/await (btset/achunk-next s))]
                                              (if (nil? step)
                                                acc
                                                (let [keys (nth step 0) start (nth step 1)
                                                      end (nth step 2) nxt (nth step 3)]
                                                  (recur nxt
                                                         (loop [i start a acc]
                                                           (if (and (< i end) (< (count a) n))
                                                             (recur (inc i) (conj a (aget keys i)))
                                                             a)))))))))])))
                        reqs)
            kvs (pca/await (pca/all exprs))]
        (swap! memo (fn [m] (-> m
                                (update :values into kvs)
                                (assoc :recording? false)
                                (assoc :requests {}))))
        memo))))

(defn- estimate-pattern-heuristic
  "Heuristic cardinality estimation for indices without precomputed subtree counts.
   Uses total datom count (O(1)) divided by pattern-specific factors.
   Rough but gives differential estimates so the planner can still order joins."
  [db pattern-info schema-info]
  (when-not @heuristic-warned?
    (reset! heuristic-warned? true)
    (log/info :datahike/estimate-heuristic-fallback
              "Index lacks precomputed subtree counts (old database format). Using heuristic estimates for query planning. Consider re-indexing for optimal performance."))
  (let [{:keys [e a v]} pattern-info
        ground? (fn [x] (and (some? x) (not (symbol? x))))
        e-ground? (ground? e)
        a-ground? (ground? a)
        v-ground? (ground? v)
        total (max 1 (di/-count (:eavt db)))]
    (cond
      ;; [e a ?v] — single datom for card-one, small fan-out for card-many
      (and e-ground? a-ground? (not v-ground?))
      (if (:card-one? schema-info) 1 5)

      ;; [e a v] — exact point
      (and e-ground? a-ground? v-ground?)
      1

      ;; [?e a v] — ~1% of attribute datoms
      (and (not e-ground?) a-ground? v-ground?)
      (max 1 (quot total 1000))

      ;; [?e a ?v] — assume ~20 attributes, so attribute has ~1/20 of total
      (and (not e-ground?) a-ground? (not v-ground?))
      (max 1 (quot total 20))

      ;; [e ?a ?v] — typical entity width
      (and e-ground? (not a-ground?) (not v-ground?))
      20

      ;; [?e ?a ?v] — full scan
      :else
      total)))

;; ---------------------------------------------------------------------------
;; Pattern cardinality estimation

(defn- estimate-pattern-counted
  "Estimate cardinality using O(log n) count-slice on indices with subtree counts."
  [db pattern-info schema-info]
  (let [{:keys [e a v tx]} pattern-info
        ground? (fn [x] (and (some? x) (not (symbol? x))))
        e-ground? (ground? e)
        a-ground? (ground? a)
        v-ground? (ground? v)
        resolved-a (when a-ground?
                     (if (and (:attribute-refs? (dbi/-config db)) (keyword? a))
                       (dbi/-ref-for db a)
                       a))]
    (cond
      ;; [e a ?v] — single datom for card-one, count EA range for card-many
      (and e-ground? a-ground? (not v-ground?))
      (if (:card-one? schema-info)
        1
        (memo-count-slice db :eavt
                          (datom (long e) resolved-a nil tx0)
                          (datom (long e) resolved-a nil txmax)
                          (datom/index-type->cmp-replace :eavt) 4))

      ;; [e a v] — exact point, 0 or 1
      (and e-ground? a-ground? v-ground?)
      1

      ;; [?e a v] — count on AVET if indexed, otherwise sample from AEVT
      (and (not e-ground?) a-ground? v-ground?)
      (if (:indexed? schema-info)
        (memo-count-slice db :avet
                          (datom e0 resolved-a v tx0)
                          (datom emax resolved-a v txmax)
                          datom/cmp-datoms-av-only 100)
        ;; Without AVET, sample datoms from the attribute slice to estimate
        ;; what fraction have this value. More accurate than fixed 10% heuristic.
        (let [attr-count (memo-count-slice db :aevt
                                           (datom e0 resolved-a nil tx0)
                                           (datom emax resolved-a nil txmax)
                                           cmp-attr-only 1000)
              ;; one sample request serves both regimes: when the attribute
              ;; fits in sample-size the sample IS the whole slice (exact),
              ;; otherwise it extrapolates — keeping the recorded request set
              ;; identical between the record pass and the real pass
              datoms (memo-sample db :aevt
                                  (datom e0 resolved-a nil tx0)
                                  (datom emax resolved-a nil txmax) :aevt sample-size)
              matching (count (filter #(= (.-v ^datahike.datom.Datom %) v) datoms))]
          (if (<= attr-count sample-size)
            (max 1 matching)
            (let [n-sampled (count datoms)]
              (if (zero? n-sampled)
                (max 1 (quot attr-count 10))
                (let [rate (/ (double matching) n-sampled)]
                  (max 1 (long (* attr-count (max 0.01 rate))))))))))

      ;; [?e a ?v] — all datoms for attribute
      (and (not e-ground?) a-ground? (not v-ground?))
      (memo-count-slice db :aevt
                        (datom e0 resolved-a nil tx0)
                        (datom emax resolved-a nil txmax)
                        cmp-attr-only 1000)

      ;; [e ?a ?v] — all datoms for entity
      (and e-ground? (not a-ground?) (not v-ground?))
      (memo-count-slice db :eavt
                        (datom (long e) nil nil tx0)
                        (datom (long e) nil nil txmax)
                        cmp-entity-only 10)

      ;; [?e ?a ?v] — full scan (worst case)
      :else
      (memo-count-index db :eavt 10000))))

(defn estimate-pattern
  "Estimate the cardinality (number of matching datoms) for a pattern clause.
   Uses count-slice on the appropriate index with partial comparators when
   subtree counts are available, otherwise falls back to O(1) heuristics.
   Returns a long estimate, or nil if estimation is not possible."
  [db pattern-info schema-info]
  (if (di/-has-subtree-counts? (:eavt db))
    (estimate-pattern-counted db pattern-info schema-info)
    (estimate-pattern-heuristic db pattern-info schema-info)))

;; ---------------------------------------------------------------------------
;; Distinct-value sampling for fan-out estimation
;;
;; When a pattern's value position is a free var that's bound from upstream,
;; the effective per-call cardinality is the AVERAGE matches per value, not
;; the total attribute count. We approximate this as
;;     attr-total-count / distinct-value-count
;; sampling distinct values from the AVET / AEVT slice.

(defn- sample-distinct-v-count
  "Estimate the number of distinct values for a ground attribute.
   For unique attrs, returns attr-count (1:1). For others, samples up to
   `sample-size` datoms from AVET (preferred) or AEVT and counts distinct vals,
   extrapolating against the total slice count.
   Returns a positive long, or nil if estimation isn't possible."
  [db pattern-info schema-info]
  (let [{:keys [a]} pattern-info
        ground? (fn [x] (and (some? x) (not (symbol? x))))]
    (when (ground? a)
      (let [resolved-a (if (and (:attribute-refs? (dbi/-config db)) (keyword? a))
                         (dbi/-ref-for db a)
                         a)
            attr-count (memo-count-slice db :aevt
                                         (datom e0 resolved-a nil tx0)
                                         (datom emax resolved-a nil txmax)
                                         cmp-attr-only 1000)]
        (cond
          (zero? attr-count) 1
          (:unique? schema-info) attr-count
          :else
          (let [;; Prefer AVET (already sorted by value) — distinct values appear in runs
                index-key (if (and (:indexed? schema-info) (some? (:avet db))) :avet :aevt)
                ;; single sample request serves both regimes (see
                ;; estimate-pattern-counted): a slice that fits in sample-size
                ;; is covered entirely by the sample — exact count
                datoms (memo-sample db index-key
                                    (datom e0 resolved-a nil tx0)
                                    (datom emax resolved-a nil txmax)
                                    index-key sample-size)
                n-sampled (count datoms)
                n-distinct (count (into #{} (map (fn [^datahike.datom.Datom d] (.-v d))) datoms))]
            (if (<= attr-count sample-size)
              (max 1 n-distinct)
              (if (zero? n-sampled)
                attr-count
                (max 1 (long (* attr-count
                                (/ (double n-distinct) (double n-sampled)))))))))))))

;; ---------------------------------------------------------------------------
;; Bound-aware pattern cardinality estimation
;;
;; A free-var position whose var is bound from upstream context is effectively
;; a probe constraint, not a free position: at execution time the lookup is
;; per-bound-value, not a full attribute scan. The CARDINALITY OF THE BOUND VAR
;; is the deciding factor:
;;   - Bound var with N values × per-value fan-out = effective scan size
;;   - Free var → attr-total-count (the existing estimator's answer)
;;
;; `bound-var-cards` is a {var-symbol → cardinality-upper-bound} map. Vars not
;; in the map are treated as free.

(defn estimate-pattern-with-bindings
  "Estimate cardinality for a pattern clause, factoring in upstream-bound
   free variables. `bound-var-cards` maps each bound var symbol to its known
   cardinality upper bound (output rel size of the producing op). Vars not in
   the map are treated as free (pre-existing behavior).

   Semantics:
     - Both e and v positions bound (or one bound + the other ground):
         min(bound-card, base) — at most this many matches across all probes.
     - Only e bound (free or ground v):
         For card-one: e-card (each entity has at most 1 match for this attr).
         For card-many: e-card × (attr-total / max-eid) approximation.
     - Only v bound (free e), attr indexed:
         v-card × (attr-total / distinct-v) — per-value fan-out from AVET.
     - Only v bound, attr not indexed: v-card × small-factor (capped by base).
     - Neither bound: existing estimate-pattern result.

   When `bound-var-cards` is empty, returns the same value as the 3-arity
   `estimate-pattern`. Defensive: if a caller still passes a plain set
   instead of a map (legacy interface), entries are treated as bound with
   unknown cardinality — fall back to 3-arity behavior for those vars."
  ([db pattern-info schema-info bound-var-cards]
   (estimate-pattern-with-bindings
    db pattern-info schema-info bound-var-cards
    (estimate-pattern db pattern-info schema-info)))
  ([db pattern-info schema-info bound-var-cards base]
   ;; `base` is the unconstrained estimate; callers in a hot loop (the group DP)
   ;; precompute it once per pattern to avoid re-sampling the index per probe.
   (let [;; Tolerate both map (var → card) and set (legacy) forms.
         card-of (fn [m k]
                   (when (contains? m k)
                     (let [v (get m k)]
                       (when (number? v) (long v)))))]
     (if (or (empty? bound-var-cards) (nil? base))
       base
       (let [{:keys [e v]} pattern-info
             free-var? analyze/free-var?
             e-card (when (free-var? e) (card-of bound-var-cards e))
             v-card (when (free-var? v) (card-of bound-var-cards v))]
         (cond
           ;; No bound free vars in this pattern — base estimate stands.
           (and (nil? e-card) (nil? v-card))
           base

           ;; Both bound — point lookup; matches ≤ min(both cards, base).
           (and e-card v-card)
           (max 1 (long (min e-card v-card (or base 1))))

           ;; Entity bound: per-entity probes.
           e-card
           (if (:card-one? schema-info)
             ;; Each bound entity contributes at most 1 datom for this attr.
             (max 1 (long (min e-card base)))
             ;; Cardinality-many: rough per-entity fan-out.
             (let [max-eid (max 1 (long (dbi/-max-eid db)))
                   fan-out (max 1 (long (/ base max-eid)))]
               (max 1 (long (min base (* e-card fan-out))))))

           ;; Value bound: per-value probes via AVET (or non-indexed fallback).
           v-card
           (if (:indexed? schema-info)
             (let [distinct-v (or (sample-distinct-v-count db pattern-info schema-info)
                                  (max 1 (long (/ base 10))))
                   per-v (max 1 (long (/ base (max 1 distinct-v))))]
               (max 1 (long (min base (* v-card per-v)))))
             ;; Non-indexed attr: lacking AVET, use a softer reduction.
             ;; The existing legacy/temporal path will scan AEVT, so we still
             ;; pay attribute-count cost; cap at attr-count to avoid false
             ;; over-estimates of selectivity.
             (max 1 (long (min base (* v-card 10)))))))))))

(defn estimate-predicate-selectivity-heuristic
  "Heuristic selectivity estimate for a predicate operator.
   Used as fallback when sampling is not possible."
  [op]
  (case op
    (= ==) 0.1
    (not= !=) 0.9
    (> < >= <=) 0.33
    0.5))

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
          datoms (memo-sample db index-key from-d to-d index-type sample-size)
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
                                                (when (dt/rethrowable? e) (throw e))
                                                (log/debug :datahike/selectivity-sample-failed "selectivity sample comparison failed"
                                                           {:value v :const const-val :error #?(:clj (.getMessage ^Exception e) :cljs (str e))})
                                                false))))
                                     datoms))
              rate (/ (double passing) n-sampled)]
          (min 1.0 (max 0.01 rate)))))
    (catch #?(:clj Exception :cljs :default) e
      ;; a cold-store fault must NOT degrade into a heuristic estimate — the
      ;; degraded plan would be cached under a warmth-independent key
      (when (dt/rethrowable? e) (throw e))
      (log/debug :datahike/selectivity-heuristic "predicate selectivity sampling failed, using heuristic"
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
  (max 1 (memo-count-index db :eavt 10000)))
