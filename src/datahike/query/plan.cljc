(ns datahike.query.plan
  "Query plan construction for the query planner.
   Creates optimized execution plans with join ordering, predicate pushdown,
   and index selection.

   Plan structure:
   - :entity-group — fused scan+merges for one entity var, may include anti-merges (NOT)
   - :pattern-scan — single pattern scan (when group has only 1 pattern)
   - :predicate, :function — filter/binding ops
   - :or, :or-join — union over sub-plans
   - :not, :not-join — subtraction via sub-plans (when not foldable into anti-merge)
   - :passthrough — delegates to legacy engine"
  (:require
   [clojure.set]
   [clojure.walk]
   [datahike.constants :refer [e0 tx0 emax txmax]]
   [datahike.datom :as datom :refer [datom]]
   [datahike.db.interface :as dbi]
   [datahike.db.utils :as dbu]
   [datahike.index.interface :as di]
   [replikativ.logging :as log]
   [datahike.query.analyze :as analyze]
   [datahike.query.estimate :as estimate]
   [datahike.query.ir :as ir]))

#?(:clj (set! *warn-on-reflection* true))

;; ---------------------------------------------------------------------------
;; Index selection

(defn- select-index
  "Choose the best index for a pattern based on which components are ground
   and whether pushdown predicates constrain the value."
  [pattern-info schema-info has-avet? has-pushdown?]
  (let [{:keys [e a v]} pattern-info
        ground? (fn [x] (and (some? x) (not (symbol? x))))
        e? (ground? e)
        a? (ground? a)
        v? (ground? v)]
    (cond
      e? :eavt
      (and a? v? has-avet? (:indexed? schema-info)) :avet
      (and a? has-pushdown? has-avet? (:indexed? schema-info)) :avet
      a? :aevt
      :else :eavt)))

;; ---------------------------------------------------------------------------
;; Pushdown bound computation

(defn- update-bound [bounds bound-key aggr-fn const-val]
  (update bounds bound-key (fn [cur] (if cur (aggr-fn cur const-val) const-val))))

(defn pushdown-to-bounds
  "Convert pushdown predicates into tighter datom bounds for index slice.
   Returns {:from-v val-or-nil, :to-v val-or-nil, :strict-preds [...]}.
   Strict inequalities (> <) use inclusive bounds for the slice, and the
   original predicates are kept in :strict-preds for post-filtering."
  [pushdown-preds]
  (reduce
   (fn [bounds {:keys [op const-val] :as pred}]
     (case op
       >  (-> (update-bound bounds :from-v max const-val)
              (update :strict-preds (fnil conj []) pred))
       >= (update-bound bounds :from-v max const-val)
       <  (-> (update-bound bounds :to-v min const-val)
              (update :strict-preds (fnil conj []) pred))
       <= (update-bound bounds :to-v min const-val)
       =  (-> bounds
              (assoc :from-v const-val)
              (assoc :to-v const-val))
       == (-> bounds
              (assoc :from-v const-val)
              (assoc :to-v const-val))
       not= (update bounds :strict-preds (fnil conj []) pred)
       (throw (ex-info "Unhandled op in pushdown-to-bounds — every op in analyze/range-ops must have a case arm here"
                       {:op op :pred pred}))))
   {:from-v nil :to-v nil}
   pushdown-preds))

;; ---------------------------------------------------------------------------
;; Pattern plan ops

(defn- pattern-output-var-cards
  "Per-output-var cardinality upper bounds for a pattern-scan op.
   Each free var in the pattern is bounded by the pattern's estimated-card
   (the slice size). For card-one attributes the bound is tight on the
   entity position; for card-many it is conservative. Refining (e.g. with
   distinct-v sampling) can tighten value-position bounds in a follow-up."
  [pattern-info est]
  (into {}
        (map (fn [v] [v est]))
        (filter analyze/free-var? (:vars pattern-info))))

(defn plan-pattern-op
  "Create a plan operation for a pattern clause.
   `bound-var-cards` is a {var → cardinality} map of upstream-bound vars
   used for cardinality estimation. Membership-check semantics (`contains?`)
   match the prior `bound-vars` set, so internal logic that only checks
   binding without using the cardinality is unchanged.
   Returns [op actually-consumed-pred-clauses]."
  [db pattern-info schema-info pushdown-preds bound-var-cards]
  (let [{:keys [e a v tx pattern]} pattern-info
        ground? (fn [x] (and (some? x) (not (symbol? x))))
        e? (ground? e)
        a? (ground? a)
        v? (ground? v)
        has-avet? (boolean (:avet db))
        index (select-index pattern-info schema-info has-avet? (seq pushdown-preds))
        effective-preds (if (= :avet index) pushdown-preds [])
        ;; Base estimate (unconstrained by upstream bindings) — preserves the
        ;; existing assemble-entity-group pass-rate semantics (pass-rate is
        ;; merge-est / total-entities, which assumes merge-est is the full
        ;; attribute count, not a bound-aware filtered count).
        base-est (or (estimate/estimate-pattern db pattern-info schema-info)
                     (di/-count (:eavt db)))
        ;; Build a partial scan-op for sampling context
        scan-ctx {:clause (:clause pattern-info) :index index}
        est (if (seq effective-preds)
              (estimate/estimate-pushdown-range base-est effective-preds db scan-ctx)
              base-est)
        ;; Bound-aware estimate — used for scan-vs-merge selection inside an
        ;; entity-group (and other op-ordering decisions that should reflect
        ;; upstream constraints). Defaults to the base estimate when
        ;; bound-var-cards is empty.
        ;;
        ;; SCAN COST vs OUTPUT CARD: estimate-pattern-with-bindings models
        ;; the OUTPUT cardinality after applying upstream constraints. For
        ;; an INDEXED attribute on the v-position, OUTPUT ≈ SCAN COST
        ;; because AVET yields exactly the matching slice. For a
        ;; NON-INDEXED v-bound pattern, we lack AVET — execution must scan
        ;; AEVT for the full attribute and post-filter, so the actual scan
        ;; cost is `base` regardless of how selective v is. dp-order-fuse-ops
        ;; uses scan-card as a SCAN COST proxy when picking the entity-
        ;; group's driving scan; using OUTPUT here would mis-rate a
        ;; non-indexed v-bound pattern as a cheap scan candidate.
        v-bound? (and (analyze/free-var? v)
                      (contains? bound-var-cards v)
                      (let [vc (get bound-var-cards v)] (number? vc)))
        bound-aware (when (seq bound-var-cards)
                      (estimate/estimate-pattern-with-bindings
                       db pattern-info schema-info bound-var-cards))
        scan-est (cond
                   (empty? bound-var-cards) est
                   ;; Non-indexed attr with v as the only bound free var:
                   ;; scan cost = base (full AEVT scan + post-filter).
                   (and (not (:indexed? schema-info))
                        v-bound?
                        (or e? (analyze/free-var? e))
                        (not (and (analyze/free-var? e)
                                  (contains? bound-var-cards e))))
                   est
                   :else (or bound-aware est))
        e-bound? (and (analyze/free-var? e)
                      (contains? bound-var-cards e))
        join-method (cond
                      e-bound? :lookup
                      (empty? bound-var-cards) :scan
                      :else :hash)]
    [(cond-> {:op :pattern-scan
              :clause pattern
              :index index
              :schema-info schema-info
              :pushdown-preds effective-preds
              :estimated-card est
              :scan-card scan-est
              :output-var-cards (pattern-output-var-cards pattern-info est)
              :join-method join-method
              :vars (:vars pattern-info)
              :e-ground (when e? e)
              :v-ground (when v? v)}
        ;; Propagate optional flag for get-else-derived scans
       (:optional? pattern-info)
       (assoc :optional? true :default-value (:default-value pattern-info)))
     (when (seq effective-preds)
       (into #{} (map :pred-clause) effective-preds))]))

(defn estimate-standalone-predicate
  "Estimate selectivity for a standalone predicate by finding the pattern
   it depends on and sampling."
  [db pred-info pattern-ops]
  (let [args (:args pred-info)
        op (:fn-sym pred-info)]
    (when (and (contains? analyze/range-ops op)
               (= 2 (count args)))
      (let [[arg1 arg2] args
            [pred-var const-val]
            (cond
              (and (analyze/free-var? arg1) (not (analyze/free-var? arg2)))
              [arg1 arg2]
              (and (analyze/free-var? arg2) (not (analyze/free-var? arg1)))
              [arg2 arg1]
              :else nil)]
        (when (and pred-var const-val)
          ;; Find the pattern-scan op that produces this variable
          (some (fn [pop]
                  (when (and (= :pattern-scan (:op pop))
                             (contains? (:vars pop) pred-var))
                    (estimate/sample-predicate-selectivity db pop op pred-var const-val)))
                pattern-ops))))))

(defn plan-predicate-op
  ([pred-info]
   (plan-predicate-op pred-info nil nil))
  ([pred-info db pattern-ops]
   (let [sampled-sel (when db (estimate-standalone-predicate db pred-info pattern-ops))]
     {:op :predicate
      :clause (:clause pred-info)
      :fn-sym (:fn-sym pred-info)
      :args (:args pred-info)
      :vars (:vars pred-info)
      :estimated-selectivity sampled-sel
      :estimated-card nil})))

(defn- resolve-external-engine-meta
  "If fn-sym is a namespaced symbol that resolves to a var with
   :datahike/external-engine metadata, return that metadata. Else nil."
  [fn-sym]
  #?(:cljs nil
     :clj (when (and (symbol? fn-sym) (namespace fn-sym))
            (try
              (when-some [v (resolve fn-sym)]
                (:datahike/external-engine (meta v)))
              (catch Exception e
                (log/debug :datahike/external-engine-meta-failed {:fn-sym fn-sym :error (.getMessage e)})
                nil)))))

(defn binding-shape
  "Classify a function clause's :binding form:
     :scalar      ?x          → one value (the whole result)
     :tuple       [?a ?b]     → one tuple
     :collection  [?x ...]    → many rows (the result collection is iterated)
     :relation    [[?a ?b]]   → many rows (a relation of tuples)"
  [binding]
  (cond
    (symbol? binding) :scalar
    (and (vector? binding) (= 2 (count binding)) (= '... (second binding))) :collection
    (and (vector? binding) (vector? (first binding))) :relation
    (vector? binding) :tuple
    :else :scalar))

(defn resolve-fn-output-cardinality
  "Output-row estimate for a function bind carrying :datahike/output-cardinality
   metadata. This lets any user function (e.g. a graph algorithm whose result
   size is data-dependent) tell the planner how many rows its binding produces,
   instead of the planner treating every function bind as an opaque unknown.

   The metadata describes the size of the RETURNED COLLECTION, so it only
   applies to bindings that destructure it — `[?x ...]` (collection) and
   `[[?a ?b]]` (relation). Scalar/tuple bindings hold the whole result in a
   single row and are left to the planner's defaults.

   The metadata value may be:
     - a number     → that many rows
     - a fn [ctx]   → computed, where ctx is
                      {:db :fn-sym :args :binding :provenance}. `:provenance`
                      maps each var bound by a scalar function bind to the form
                      that produced it (e.g. {?g (attr-graph :follows)}), so a
                      cost model can resolve a graph passed by variable back to
                      its constructor and introspect it.
   Returns a positive long, or nil when no usable estimate is available.
   Purely additive: functions without the metadata are unaffected."
  [fn-sym args binding db provenance]
  #?(:cljs nil
     :clj (when (and (symbol? fn-sym) (namespace fn-sym)
                     (contains? #{:collection :relation} (binding-shape binding)))
            (try
              (when-some [v (resolve fn-sym)]
                (when-some [c (:datahike/output-cardinality (meta v))]
                  (let [n (cond
                            (number? c) c
                            (fn? c)     (c {:db db :fn-sym fn-sym :args args
                                            :binding binding :provenance provenance})
                            :else       nil)]
                    (when (number? n) (max 1 (long n))))))
              (catch Exception e
                (log/debug :datahike/output-cardinality-meta-failed {:fn-sym fn-sym :error (.getMessage e)})
                nil)))))

(def default-in-collection-card
  "Seed cardinality for an :in collection/relation binding whose size is unknown
   at plan time. Matches the plain-function default used by op-cost /
   plan-function-op (100): between a point lookup (1) and a full attribute scan,
   so a join driven by a passed collection is ordered as a real multi-row source
   instead of a singleton."
  100)

(defn source-cards
  "Unified produce-side cardinality seed for the planner's {var → card} map.

   The planner is an abstract interpreter over cardinalities: it seeds a
   {var → card} map from `sources`, propagates it through the join graph
   (order-plan-ops / dp-order-groups), and costs ops. There are three kinds of
   source; this is the single place that defines how each one seeds the map.

   Dispatch on :kind:
     {:kind :pattern :classified ci :db db}
        → attribute-stats estimate for an LScan's free output vars
     {:kind :bind    :classified ci :db db :provenance prov}
        → :datahike/output-cardinality estimate for an LBind's free vars
          (ci carries :fn-sym / :args / :binding)
     {:kind :input   :shape <kw> :vars <syms>}
        → shape-based seed for :in vars: collection/relation bindings bind many
          rows (`default-in-collection-card`); scalar/tuple bind one row and are
          left to the card-1 placeholder. Value-independent, so it is safe to
          fold into the plan cache key.

   Returns {var → long}, or nil when no useful estimate applies (callers treat
   nil as 'no contribution')."
  [{:keys [kind] :as source}]
  (case kind
    :pattern
    (let [{:keys [classified db]} source
          si (analyze/pattern-schema-info db classified)
          est (or (estimate/estimate-pattern db classified si)
                  (di/-count (:eavt db)))
          free-output-vars (filter analyze/free-var? (:vars classified))]
      (when (seq free-output-vars)
        (zipmap free-output-vars (repeat (long est)))))

    :bind
    (let [{:keys [classified db provenance]} source
          card (resolve-fn-output-cardinality
                (:fn-sym classified) (:args classified) (:binding classified) db provenance)]
      (when card
        (let [bvars (filter analyze/free-var? (analyze/extract-vars (:binding classified)))]
          (when (seq bvars)
            (zipmap bvars (repeat (long card)))))))

    :input
    (let [{:keys [shape vars]} source]
      (when (contains? #{:collection :relation} shape)
        (not-empty (zipmap vars (repeat (long default-in-collection-card))))))

    nil))

(defn resolve-fn-exec-cost
  "If `fn-sym` resolves to a var carrying :datahike/cost metadata, return a
   1-arg cost function `(fn [input-rows] total-cost)`, closing over `db`, the
   clause `args`, and `provenance`. The metadata may be:
     - a number    → a per-call cost; total = number × input-rows
     - a fn [ctx]  → total cost, where ctx is
                     {:db :fn-sym :args :provenance :input-rows}
   Returns nil when there is no metadata. Lets a function tell the planner how
   expensive it is to run, so the ordering can defer it behind selective filters
   that shrink its input. Purely additive: unannotated functions cost 1."
  [fn-sym args db provenance]
  #?(:cljs nil
     :clj (when (and (symbol? fn-sym) (namespace fn-sym))
            (try
              (when-some [v (resolve fn-sym)]
                (when-some [c (:datahike/cost (meta v))]
                  (cond
                    (number? c) (fn [input-rows] (* c input-rows))
                    (fn? c)     (fn [input-rows]
                                  (let [r (c {:db db :fn-sym fn-sym :args args
                                              :provenance provenance :input-rows input-rows})]
                                    (if (number? r) r input-rows)))
                    :else       nil)))
              (catch Exception e
                (log/debug :datahike/cost-meta-failed {:fn-sym fn-sym :error (.getMessage e)})
                nil)))))

(defn- detect-external-engine-mode
  "Determine execution mode from binding form and engine metadata.
   Returns :filter, :retrieval, or :solver."
  [binding-form engine-meta]
  (let [binding-cols (:binding-columns engine-meta)
        ;; binding-form is [[?e]] or [[?e ?score]] etc.
        n-bound (if (and (sequential? binding-form)
                         (sequential? (first binding-form)))
                  (count (first binding-form))
                  1)]
    (cond
      ;; Dynamic binding columns (solver/aggregate) — check if entity-id is first
      (= :dynamic binding-cols)
      :solver

      ;; Passthrough (sort) — same columns in and out
      (= :passthrough binding-cols)
      :solver

      ;; Has entity-id as first column
      (and (sequential? binding-cols)
           (= :entity-id (first binding-cols)))
      (if (= 1 n-bound) :filter :retrieval)

      ;; No entity-id → solver/generator
      :else :solver)))

(defn- plan-external-engine-op
  "Create a plan op for an external engine function call."
  [fn-info engine-meta db]
  (let [mode (detect-external-engine-mode (:binding fn-info) engine-meta)
        args (:args fn-info)
        idx-key (:index-key engine-meta)
        idx-ident (when idx-key (nth (vec args) idx-key nil))
        cost-fn (:cost-model engine-meta)
        binding-form (:binding fn-info)
        n-bound (if (and (sequential? binding-form)
                         (sequential? (first binding-form)))
                  (count (first binding-form))
                  1)
        cost (when cost-fn
               (try (cost-fn db idx-ident args n-bound)
                    (catch #?(:clj Exception :cljs js/Error) e
                      (log/warn :datahike/external-engine-cost-failed {:error #?(:clj (.getMessage e) :cljs (.-message e))})
                      nil)))]
    {:op :external-engine
     :clause (:clause fn-info)
     :fn-sym (:fn-sym fn-info)
     :args args
     :binding (:binding fn-info)
     :vars (:vars fn-info)
     :mode mode
     :idx-ident idx-ident
     :engine-meta engine-meta
     :accepts-entity-filter? (:accepts-entity-filter? engine-meta false)
     ;; The engine declares its binding requirements via the :input-vars
     ;; key in its var metadata. Recognised values:
     ;;   :all-bound  → every input arg must be bound (the safe default;
     ;;                 matches stratum's three exposed engines, all of
     ;;                 which need their referenced columns materialised
     ;;                 before they can run).
     ;;   :any-bound  → at least one input arg must be bound (for solvers
     ;;                 that can work backwards from any known value).
     ;;   nil         → fall back to :all-bound at op-cost time (strict
     ;;                 default — engines that need looser semantics must
     ;;                 declare them explicitly).
     :input-vars-spec (:input-vars engine-meta)
     :estimated-card (or (:estimated-card cost) 100)}))

(defn- function-binding-vars
  "Extract free vars from a function-clause :binding spec.
   Handles BindScalar, BindColl, BindTuple, BindIgnore — but at the planner
   level :binding is most often the raw symbol or vector form, so we just
   collect free-var symbols recursively."
  [binding]
  (into #{} (filter analyze/free-var?) (analyze/extract-vars binding)))

(defn- function-output-var-cards
  "Per-binding-var cardinality bounds for a function op.
     - (identity X)  → bind-var card = 1 (single value)
     - (ground COLL) → bind-var card = (count COLL) when literal; else nil
   For other functions we don't know the output cardinality at plan time
   without sampling, so omit them — downstream tightens via :estimated-card."
  [fn-sym args binding]
  (let [bvars (function-binding-vars binding)]
    (case fn-sym
      identity
      (when (= 1 (count bvars))
        (zipmap bvars (repeat 1)))

      ground
      (let [coll (first args)]
        (cond
          (and (= 1 (count bvars)) (counted? coll))
          (zipmap bvars (repeat (max 1 (count coll))))

          ;; (ground [[?a ?b]]) tuple-binding shape — coll is a single-element
          ;; vector wrapping a tuple destructure; the bindings are produced
          ;; collectively, count is # of tuples in the source.
          (and (counted? coll) (sequential? (first coll)))
          (zipmap bvars (repeat (max 1 (count coll))))

          :else nil))

      ;; Unknown function → omit; ops that consume this rel still see
      ;; :estimated-card for ordering.
      nil)))

(defn plan-function-op
  "Create a plan op for a function clause. Checks for external engine metadata.
   `provenance` maps scalar-bind vars to the form that produced them, so an
   algorithm's :datahike/output-cardinality cost model can resolve a graph
   passed by variable back to its constructor."
  ([fn-info] (plan-function-op fn-info nil nil))
  ([fn-info db] (plan-function-op fn-info db nil))
  ([fn-info db provenance]
   (let [engine-meta (resolve-external-engine-meta (:fn-sym fn-info))]
     (if engine-meta
       (plan-external-engine-op fn-info engine-meta db)
       (let [out-cards (function-output-var-cards
                        (:fn-sym fn-info) (:args fn-info) (:binding fn-info))
             ;; Let a fn var advertise its output cardinality via metadata
             ;; (:datahike/output-cardinality) so data-dependent binds aren't
             ;; opaque to the planner. Only used when the static rules above
             ;; didn't already determine the cards.
             meta-card (when-not out-cards
                         (resolve-fn-output-cardinality
                          (:fn-sym fn-info) (:args fn-info) (:binding fn-info) db provenance))
             bvars (when meta-card (function-binding-vars (:binding fn-info)))
             meta-cards (when (seq bvars) (zipmap bvars (repeat meta-card)))
             out-cards (or out-cards meta-cards)
             ;; Execution-cost model (:datahike/cost) — lets the planner defer an
             ;; expensive function behind selective filters that shrink its input.
             exec-cost-fn (resolve-fn-exec-cost
                           (:fn-sym fn-info) (:args fn-info) db provenance)]
         (cond-> {:op :function
                  :clause (:clause fn-info)
                  :fn-sym (:fn-sym fn-info)
                  :args (:args fn-info)
                  :binding (:binding fn-info)
                  :vars (:vars fn-info)
                  :estimated-card meta-card}
           out-cards (assoc :output-var-cards out-cards)
           exec-cost-fn (assoc :exec-cost-fn exec-cost-fn)))))))

(defn plan-passthrough-op [clause-info]
  {:op :passthrough
   :clause (:clause clause-info)
   :type (:type clause-info)
   :vars (:vars clause-info)
   :estimated-card nil})

;; ---------------------------------------------------------------------------
;; DP-based optimal merge ordering

(defn can-be-merge?
  "A pattern op can serve as a merge (lookupGE) only if its attribute position
   is ground (not a variable). Variable-attribute patterns must be the scan."
  [op]
  (let [a (second (:clause op))]
    (and (some? a) (not (symbol? a)))))

(defn- scan-cost-of
  "Cost a pattern would have if used as the entity-group's scan, based on
   bound-aware :scan-card when available, falling back to :estimated-card."
  [op]
  (long (or (:scan-card op) (:estimated-card op) 0)))

(defn dp-order-fuse-ops
  "For N pattern ops on same entity var, find optimal (scan, merge-order).
   Uses short-circuit AND ordering: sort merges by ascending
   merge-cost / (1 - pass-rate). Try each pattern as scan, pick minimum total cost.

   Scan selection uses bound-aware :scan-card so a pattern whose value is
   constrained by upstream context (smaller effective scan size) is preferred
   over a pattern with all-free positions on the same attribute. Pass-rate
   computation still uses :estimated-card (the base attribute count) to
   preserve the existing per-entity match-probability semantics."
  [db pattern-ops total-entities]
  (let [n (count pattern-ops)
        ;; Pass-rate estimates use base (unconstrained) cardinality.
        estimates (mapv (fn [op]
                          (max 1 (or (:estimated-card op) total-entities)))
                        pattern-ops)]
    (if (<= n 2)
      (let [;; Patterns with variable attribute can only be scan, not merge
            mergeable (filterv can-be-merge? pattern-ops)
            scan-only (filterv #(not (can-be-merge? %)) pattern-ops)
            ;; Optional ops (from get-else / LOptionalScan) should not drive the scan
            ;; because entities without the optional attribute would be skipped entirely.
            ;; Prefer non-optional ops as scan; optional ops as merges.
            non-optional (filterv #(not (:optional? %)) pattern-ops)]
        (if (seq scan-only)
          ;; Force scan-only pattern as scan, rest as merges
          {:scan (first scan-only) :merges (vec (concat (rest scan-only) mergeable))}
          (if (seq non-optional)
            ;; Prefer non-optional as scan; pick the lowest BOUND-AWARE cost.
            (let [sorted (sort-by scan-cost-of non-optional)]
              {:scan (first sorted) :merges (vec (concat (rest sorted)
                                                         (filterv :optional? pattern-ops)))})
            ;; All optional — pick lowest scan-cost as scan
            (let [sorted (sort-by scan-cost-of pattern-ops)]
              {:scan (first sorted) :merges (vec (rest sorted))}))))
      (let [has-non-optional? (some #(not (:optional? %)) pattern-ops)
            candidates
            (for [si (range n)
                  ;; Only consider scan positions where all remaining ops can be merges.
                  ;; Optional ops should not drive scan (entities without the attribute
                  ;; would be skipped), unless ALL ops are optional.
                  :when (and (every? can-be-merge?
                                     (keep-indexed (fn [i op] (when (not= i si) op)) pattern-ops))
                             (or (not has-non-optional?)
                                 (not (:optional? (nth pattern-ops si)))))]
              (let [scan-op (nth pattern-ops si)
                    ;; Use bound-aware :scan-card for the scan-side cost; merges
                    ;; still use the unconstrained :estimated-card via `estimates`
                    ;; for pass-rate computation.
                    scan-card (double (scan-cost-of scan-op))
                    merges (into [] (keep-indexed
                                     (fn [i op]
                                       (when (not= i si)
                                         (let [est (double (nth estimates i))
                                               pass-rate (estimate/estimate-conditional-pass-rate est total-entities)
                                               sort-key (/ 1.0 (max 0.001 (- 1.0 pass-rate)))]
                                           {:op op :pass-rate pass-rate :sort-key sort-key}))))
                                 pattern-ops)
                    sorted-merges (sort-by :sort-key merges)
                    total-cost (reduce (fn [[cost cum-pass] m]
                                         [(+ cost (* scan-card cum-pass))
                                          (* cum-pass (:pass-rate m))])
                                       [scan-card 1.0]
                                       sorted-merges)]
                {:scan scan-op
                 :merges (mapv :op sorted-merges)
                 :cost (first total-cost)}))]
        (if (seq candidates)
          (apply min-key :cost candidates)
          ;; Fallback: if no valid scan/merge partitioning exists (multiple variable-attr ops),
          ;; pick lowest BOUND-AWARE cardinality as scan
          (let [sorted (sort-by scan-cost-of pattern-ops)]
            {:scan (first sorted) :merges (vec (rest sorted))}))))))

;; ---------------------------------------------------------------------------
;; Pipeline construction
;;
;; Determines the fused execution path from plan-time data and annotates
;; entity-group ops with a PPipeline record.

(defn build-pipeline
  "Build a PPipeline for an entity-group or standalone scan.
   Determines the fused execution path from plan-time data only.
   Does NOT pre-compute runtime values (slice bounds, filters, arrays)."
  [scan-op merge-ops db]
  (let [n-merges (count merge-ops)
        {:keys [clause index]} scan-op
        [_e a _v _tx] clause
        scan-attr-ground? (and (some? a) (not (symbol? a)))
        has-card-many? (boolean (some #(not (get-in % [:schema-info :card-one?] true)) merge-ops))
        has-anti? (boolean (some :anti? merge-ops))
        has-optional? (boolean (some :optional? merge-ops))
        use-cursors? #?(:clj (and (pos? n-merges)
                                  (or (= index :eavt)
                                      (= index :aevt)
                                      (and (= index :avet)
                                           (let [sv (get clause 2)]
                                             (and (some? sv) (not (analyze/free-var? sv)))))))
                        :cljs false)
        use-sorted-scan? #?(:clj (and use-cursors?
                                      (pos? n-merges)
                                      scan-attr-ground?
                                      (not has-card-many?)
                                      (not has-anti?)
                                      (not has-optional?))
                            :cljs false)
        attr-refs? (:attribute-refs? (dbi/-config db))
        fused-path (cond
                     (zero? n-merges) :scan-only
                     has-optional?    :per-cursor-merge
                     has-card-many?   :card-many-merge
                     use-sorted-scan? :sorted-merge
                     :else            :per-cursor-merge)
        steps (cond-> [(ir/->PIndexScan index clause scan-attr-ground?)
                       (ir/->PGroundFilter true (boolean (seq (:pushdown-preds scan-op))))]
                true (conj (ir/->PProbeFilter nil))
                (and (pos? n-merges) (= fused-path :sorted-merge))
                (conj (ir/->PSortedMerge n-merges))
                (and (pos? n-merges) (= fused-path :per-cursor-merge))
                (conj (ir/->PPerCursorMerge n-merges has-anti?))
                (and (pos? n-merges) (= fused-path :card-many-merge))
                (conj (ir/->PCardManyMerge n-merges))
                true (conj (ir/->PEmitTuple 0)))]
    (ir/->PPipeline (vec steps) fused-path use-cursors? (boolean attr-refs?))))

(defn args-free-vars
  "Walk an :args list recursively, collecting every free variable that
   appears anywhere inside. Mirrors `analyze/extract-vars`, which is
   already the recursive contract for `:vars` on every clause type.

   Why recurse: SQL translators (and any other producer of nested
   boolean / arithmetic expressions) emit clauses like
       [(and (= ?x ?p) (some? ?y)) ?out]
   The :args here are seq forms, not symbols. A flat top-level
   `(filter free-var?)` returns #{}, so the planner thinks the op has
   no inputs and orders it before its producers — leaving the legacy
   bind-by-fn path to fail with nil ctx until the next iteration.
   The legacy engine is fine with nested expressions because it walks
   them at runtime via `interpret-form`; the planner just has to
   recognise the same shape during ordering."
  [args]
  (into #{} (mapcat analyze/extract-vars) args))

(defn- external-engine-spec-vars
  "Collect implicit input vars from a stratum-style external-engine
   query-spec map. The engine's `aggregate` / `window` / etc. reference
   their input columns as keywords inside :group / :agg / :order /
   :window / :select, not as ?-vars in :args. `execute-external-engine`
   in execute.cljc maps these keywords to ?-symbols at runtime
   (e.g. :dept → ?dept) — this helper mirrors that mapping at plan
   time so the binding-policy can require those vars bound before
   running the engine. Without it, the engine schedules before its
   producers and runs on an empty :rels.

   Returns a set of '?col-name symbols. Returns #{} when the first
   arg isn't a map (e.g. raw scalar input)."
  [args]
  (let [query-spec (first args)]
    (if-not (map? query-spec)
      #{}
      (let [kws (concat (:group query-spec)
                        ;; Each agg-spec last keyword is the column ref
                        ;; (e.g. [:avg :salary] → :salary,
                        ;; [:as :total [:sum :salary]] → :salary)
                        (keep (fn [agg]
                                (when (and (sequential? agg)
                                           (> (count agg) 1))
                                  (let [c (last agg)]
                                    (when (keyword? c) c))))
                              (:agg query-spec))
                        (mapcat (fn [w]
                                  (concat (:partition-by w)
                                          (map first (:order-by w))
                                          (when-let [c (:col w)]
                                            (when (keyword? c) [c]))))
                                (:window query-spec))
                        (when-let [sel (:select query-spec)]
                          (filter keyword? sel))
                        (map first (:order query-spec)))]
        (into #{}
              (comp (filter keyword?)
                    (map #(symbol (str "?" (name %)))))
              kws)))))

(defn- op-input-vars
  "Return only the *input* vars of an op — vars it consumes, not vars it produces.
   For :function ops, output vars (bound by the function) are excluded.
   For all other ops, :vars is already the set of input vars."
  [op]
  (if (= :function (:op op))
    (args-free-vars (:args op))
    (:vars op)))

(defn post-op-direct-eligible?
  "Check whether a predicate or function op can be evaluated in the direct
   (post-filter) path. Excludes:
   - ops that reference source symbols ($, %) in args
   - functions with non-scalar binding forms (tuple [?a ?b], collection [[?x ...]])"
  [op]
  (let [args (:args op)
        binding (:binding op)
        fn-sym (:fn-sym op)]
    (and
     ;; fn-sym must not be a variable (e.g. ?pred in [(?pred ?x)])
     (not (analyze/free-var? fn-sym))
     ;; Only scalar bindings supported (symbol or nil for predicates)
     (or (nil? binding) (symbol? binding))
     ;; All args must be constants or free variables (no source/rule symbols)
     (every? (fn [a]
               (or (not (symbol? a))
                   (analyze/free-var? a)))
             args))))

(defn structurally-fusable?
  "Check if a plan's ops are structurally eligible for the direct-to-HashSet path.
   Allows groups (entity-group, pattern-scan) plus predicates, functions, and
   simple NOT-JOINs for single-group plans only.
   Functions that reference source symbols ($) are excluded.
   This is a necessary but not sufficient condition — runtime still checks find-var coverage."
  [ops]
  (let [groups (filterv #(#{:entity-group :pattern-scan} (:op %)) ops)
        has-post-ops? (not (every? #(#{:entity-group :pattern-scan} (:op %)) ops))]
    (and (seq ops)
         (every? #(#{:entity-group :pattern-scan :predicate :function :not-join} (:op %)) ops)
         (not-any? :source ops)
         ;; Post-ops only supported for single-group plans
         (or (not has-post-ops?)
             (= 1 (count groups)))
         ;; All predicate/function ops must be direct-eligible
         (every? (fn [op]
                   (or (#{:entity-group :pattern-scan :not-join} (:op op))
                       (post-op-direct-eligible? op)))
                 ops)
         ;; Verify all predicate/function/not-join input vars are bound by preceding groups
         (let [group-vars (into #{} (mapcat :vars) groups)]
           (every? (fn [op]
                     (or (#{:entity-group :pattern-scan} (:op op))
                         (= :not-join (:op op))  ;; join-vars checked at runtime
                         (clojure.set/subset? (op-input-vars op) group-vars)))
                   ops)))))

;; ---------------------------------------------------------------------------
;; Entity group assembly
;;
;; An entity group bundles all pattern ops on the same entity variable into
;; a single plan node: one scan + zero or more merge lookups.
;; This enables fused execution (single pass over scan datoms, lookupGE per merge).

(defn assemble-entity-group
  "Build an :entity-group op from pattern-ops on the same entity-var.
   Applies DP merge ordering, folds anti-merges, computes pipeline annotation.
   Returns {:op entity-group-op, :merge-lost-preds #{consumed-pred-clauses-on-merges}}."
  [db entity-var source pattern-ops anti-ops total-entities]
  (let [{:keys [scan merges]}
        (dp-order-fuse-ops db pattern-ops total-entities)
        ;; Merge ops = DP-ordered merges + anti-merges
        ;; Sort anti-merges by their filtering power (most selective first)
        all-merges (into (vec merges) anti-ops)
        merge-ops (let [normal (filterv #(not (:anti? %)) all-merges)
                        anti (filterv :anti? all-merges)]
                    (if (empty? anti)
                      normal
                      (let [anti-with-pass (mapv (fn [op]
                                                   (let [est (max 1 (or (:estimated-card op) total-entities))
                                                         match-rate (estimate/estimate-conditional-pass-rate est total-entities)]
                                                     (assoc op ::anti-pass-rate (- 1.0 match-rate))))
                                                 anti)
                            sorted-anti (sort-by ::anti-pass-rate anti-with-pass)]
                        (into (vec normal)
                              (mapv #(dissoc % ::anti-pass-rate) sorted-anti)))))
        ;; Estimate output cardinality
        scan-card (max 1 (or (:estimated-card scan) total-entities))
        group-card (reduce (fn [card merge-op]
                             (let [merge-est (max 1 (or (:estimated-card merge-op) total-entities))
                                   pass-rate (estimate/estimate-conditional-pass-rate merge-est total-entities)]
                               (cond
                                 ;; Optional merges don't filter — all entities pass
                                 (:optional? merge-op) card
                                 (:anti? merge-op)
                                 (long (* card (max 0.01 (- 1.0 pass-rate))))
                                 :else
                                 (long (* card pass-rate)))))
                           scan-card
                           merge-ops)
        output-vars (into #{} (mapcat :vars) (into pattern-ops anti-ops))
        ;; Per-output-var cardinality. The group's output rel size is `group-card`,
        ;; which bounds every var the group produces. For tighter per-var bounds
        ;; we'd need to track which patterns produce which vars + their individual
        ;; cardinalities — for now the group-level bound suffices for downstream
        ;; planning decisions (it differentiates a 4k-tuple group from a 150k one).
        group-card-final (max 1 group-card)
        output-var-cards (into {}
                               (comp (filter analyze/free-var?)
                                     (map (fn [v] [v group-card-final])))
                               output-vars)
        ;; Merge-ops' pushdown preds can't be applied (merge uses EAVT lookupGE,
        ;; not AVET scan). Collect them so they can be restored as standalone preds.
        merge-lost-preds (into #{} (comp (mapcat :pushdown-preds) (map :pred-clause)) merge-ops)
        final-scan (assoc scan :join-method :scan)
        final-merges (mapv #(assoc % :join-method :lookup) merge-ops)
        eg-op (cond-> {:op :entity-group
                       :entity-var entity-var
                       :scan-op final-scan
                       :merge-ops final-merges
                       :output-vars output-vars
                       :output-var-cards output-var-cards
                       :vars output-vars
                       :estimated-card group-card-final
                       :pipeline (build-pipeline final-scan final-merges db)}
                source (assoc :source source))]
    {:op eg-op
     :merge-lost-preds merge-lost-preds}))

;; ---------------------------------------------------------------------------
;; Entity group partitioning

(defn- build-entity-groups
  "Partition pattern ops by entity variable and build entity-group nodes.
   For groups with a single op, returns the op unchanged.
   For multi-op groups, creates an :entity-group node with optimal merge ordering.

   Also folds simple NOT clauses into anti-merges within their entity group
   when possible (the NOT contains a single pattern on the same entity var)."
  [db pattern-ops not-ops total-entities]
  (let [;; Group patterns by [entity-var source]. Blank/anonymous entity vars
        ;; (like _ or _e — anything not starting with ?) must NOT be grouped
        ;; together, since each _ is an independent anonymous variable.
        ;; Ground constants (numbers, keywords, strings) CAN share a group.
        blank-counter (atom 0)
        groups (group-by (fn [op]
                           (let [e-var (first (:clause op))]
                             (cond
                               (analyze/free-var? e-var)
                               [e-var (:source op)]
                               ;; Blank _ symbol — each gets unique group
                               (and (symbol? e-var)
                                    (.startsWith (name e-var) "_"))
                               [(symbol (str "_anon_" (swap! blank-counter inc))) (:source op)]
                               ;; Ground constant — group by value
                               :else
                               [e-var (:source op)])))
                         pattern-ops)

        ;; Identify which NOT ops can be folded into entity groups
        ;; A NOT is foldable if it has a single sub-op that is a pattern-scan
        ;; on an entity var that has a group
        foldable-nots
        (reduce
         (fn [acc not-op]
           (let [sub-plan (:sub-plan not-op)
                 sub-ops (:ops sub-plan)]
             (if (and (= 1 (count sub-ops))
                      (= :pattern-scan (:op (first sub-ops)))
                      ;; Only fold if the anti-merge uses the same source (or no source)
                      ;; as the entity group's patterns. Cross-source anti-merges
                      ;; can't share the same EAVT index.
                      (nil? (:source (first sub-ops)))
                      (nil? (:source not-op))
                      (let [e-var (first (:clause (first sub-ops)))]
                        (and (analyze/free-var? e-var)
                             (contains? groups [e-var nil]))))
               (let [anti-op (assoc (first sub-ops) :anti? true)
                     e-var (first (:clause anti-op))]
                 (update acc [e-var nil] (fnil conj []) {:anti-op anti-op
                                                         :not-op not-op}))
               acc)))
         {}
         not-ops)

        folded-not-clauses (into #{} (mapcat (fn [[_ entries]]
                                               (map (comp :clause :not-op) entries)))
                                 foldable-nots)

        ;; Build entity-group nodes
        entity-groups
        (mapv
         (fn [[[e-var source] ops]]
           (let [anti-ops (mapv :anti-op (get foldable-nots [e-var source]))
                 all-ops (into (vec ops) anti-ops)]
             (if (and (= 1 (count all-ops)) (not (:anti? (first all-ops))))
               ;; Single pattern, no anti-merges — keep as plain pattern-scan
               (let [op (first ops)]
                 (assoc op :pipeline (build-pipeline op [] db)))
               ;; Multi-op group — delegate to shared assembly
               (let [non-anti (filterv #(not (:anti? %)) all-ops)
                     {:keys [op merge-lost-preds]}
                     (assemble-entity-group db e-var source non-anti anti-ops total-entities)]
                 (assoc op :merge-lost-preds merge-lost-preds)))))
         groups)

        ;; NOT ops that weren't folded
        remaining-nots (filterv #(not (contains? folded-not-clauses (:clause %))) not-ops)]

    {:entity-groups entity-groups
     :remaining-nots remaining-nots}))

;; ---------------------------------------------------------------------------
;; Inter-group join detection

(defn detect-inter-group-joins
  "Detect shared variables between entity groups.
   Returns a map of {group-idx → {:probe-vars #{shared-vars}, :producer-idx N}}."
  [entity-groups]
  (let [n (count entity-groups)
        ;; For each group, collect its output vars
        group-vars (mapv (fn [g]
                           (if (= :entity-group (:op g))
                             (:output-vars g)
                             (:vars g)))
                         entity-groups)]
    (reduce
     (fn [acc i]
       (let [my-vars (nth group-vars i)
             ;; Find the earliest group that produces a var I need
             shared (for [j (range i)
                          :let [their-vars (nth group-vars j)
                                common (clojure.set/intersection my-vars their-vars)]
                          :when (seq common)]
                      {:producer-idx j :probe-vars common})]
         (if (seq shared)
           (assoc acc i (first shared))
           acc)))
     {}
     (range n))))

;; ---------------------------------------------------------------------------
;; DP inter-group join ordering

(defn- group-effective-card
  "Effective output cardinality for a group, accounting for attached predicates.

   Prefers the bound-aware :scan-card (set on single pattern-scan ops whose
   entity/value var is bound by an :in input or an upstream producer) over the
   unconstrained :estimated-card, so a selective lookup-by-bound-var is ordered
   ahead of a full-attribute scan instead of behind it. :estimated-card is left
   intact for assemble-entity-group's pass-rate math; only the ORDERING cost
   reads the bound-aware value here."
  ^long [group]
  (let [base (long (or (:scan-card group) (:estimated-card group) 1000000))]
    (if-let [preds (seq (:attached-preds group))]
      (max 1 (long (* base (Math/pow 0.33 (count preds)))))
      base)))

(defn- build-group-join-graph
  "Build adjacency map for groups connected by shared variables.
   Returns {[i j] → #{shared-vars}} for all pairs with shared vars."
  [groups]
  (let [n (count groups)
        gvars (mapv (fn [g]
                      (if (= :entity-group (:op g))
                        (:output-vars g)
                        (:vars g)))
                    groups)]
    (into {}
          (for [i (range n)
                j (range (inc i) n)
                :let [common (clojure.set/intersection (nth gvars i) (nth gvars j))]
                :when (seq common)]
            [[i j] common]))))

(def ^:private ^:const dp-group-threshold
  "Maximum number of entity groups for exact DP enumeration.
   Beyond this, fall back to greedy (GOO-style) ordering.
   16 groups = 65536 DP states ≈ 1ms. 20 = 1M states ≈ 50ms."
  16)

(defn- greedy-order-groups
  "Greedy Operator Ordering (GOO) for large group counts.
   At each step, pick the cheapest group that connects to the partial plan.
   O(n²) — equivalent to DuckDB's approximate fallback."
  [groups]
  (let [n (count groups)
        cards (long-array (map group-effective-card groups))
        edges (build-group-join-graph groups)
        adj (mapv (fn [i]
                    (into #{}
                          (keep (fn [j]
                                  (when (and (not= i j)
                                             (or (contains? edges [i j])
                                                 (contains? edges [j i])))
                                    j)))
                          (range n)))
                  (range n))]
    (loop [placed #{}
           order []
           remaining (set (range n))]
      (if (empty? remaining)
        order
        (if (empty? placed)
          ;; First group: pick lowest cardinality
          (let [best (apply min-key #(aget cards (int %)) remaining)]
            (recur (conj placed best) (conj order best) (disj remaining best)))
          ;; Pick cheapest connected group; if none connected, pick cheapest overall
          (let [connected (filterv (fn [i]
                                     (some #(contains? placed %) (nth adj i)))
                                   remaining)
                candidates (if (seq connected) connected (vec remaining))
                best (apply min-key #(aget cards (int %)) candidates)]
            (recur (conj placed best) (conj order best) (disj remaining best))))))))

(defn- group-probe-info
  "Per-group [pattern-info schema-info base] used to RE-ESTIMATE a group's output
   under upstream bindings (the bound-aware DP join estimate). For a single
   pattern-scan the pattern is its clause; for a fused entity-group we proxy with
   the driving scan-op's pattern (a sound lower bound on the group's selectivity
   against a bound var). `base` is the UNCONSTRAINED attribute estimate so
   estimate-pattern-with-bindings is not forced to re-sample the index per probe.
   Returns [nil nil base] when the group has no usable pattern."
  [group]
  (case (:op group)
    :pattern-scan
    [(analyze/classify-clause (:clause group))
     (:schema-info group)
     (long (or (:estimated-card group) (group-effective-card group)))]
    :entity-group
    (let [s (:scan-op group)]
      (if (:clause s)
        [(analyze/classify-clause (:clause s))
         (:schema-info s)
         (long (or (:estimated-card s) (group-effective-card group)))]
        [nil nil (group-effective-card group)]))
    [nil nil (group-effective-card group)]))

(defn- bound-aware-join-rows
  "Estimate the join output of a partial plan (`rows` rows, `cards` = var→card of
   what is already bound) extended by group i, given i's [pinfo sinfo base].
   When db/pinfo are available and i shares a bound var, the result is the most
   selective of two NON-INCREASING bounds (a probe can only filter the running
   relation — a fan-out adds columns but the join key still bounds surviving rows
   by `rows`):
     i-eff   — the probe's own output under the bindings (containment join
               estimate); caps `rows` at the probe size.
     covered — rows × attribute coverage (base / max-eid), the fraction of the
               entity domain that actually carries the probe attribute. This
               expresses SELECTIVITY the containment estimate cannot: a sparse
               ref-join (only 5 of 200 bound entities are :flagged) is row-reducing
               even though i-eff = min(e-card, base) = 200.
   This replaces the old (max rows i-card) which could never represent a
   row-reducing join. Kept LOCAL to the DP so scan-card estimation (and its tests)
   are untouched; a dense attribute (coverage ≈ 1) yields no spurious cut. Falls
   back to (max rows i-card) when bound-aware estimation is not possible."
  [db rows i-card cards pinfo sinfo base]
  (if (or (nil? db) (nil? pinfo))
    (Math/max (long rows) (long i-card))
    (let [bound (select-keys cards (:vars pinfo))]
      (if (empty? bound)
        ;; No shared bound var — cross/independent extension: old upper bound.
        (Math/max (long rows) (long i-card))
        (let [i-eff (long (estimate/estimate-pattern-with-bindings
                           db pinfo sinfo bound base))
              max-eid (max 1 (long (dbi/-max-eid db)))
              sel (min 1.0 (/ (double base) (double max-eid)))
              covered (long (Math/ceil (* (double rows) sel)))]
          (max 1 (min (long rows) i-eff (max 1 covered))))))))

(defn- cap-cards
  "All vars of a relation with `rows` rows have ≤ rows distinct values. Merge group
   i's output cards into the running map (min) and cap every entry at `rows`, so a
   selective join shrinks the carried cardinalities (used by Step 3 to feed a
   function a smaller input)."
  [cards group rows]
  (let [merged (merge-with min cards (or (:output-var-cards group) {}))
        cap (long rows)]
    (persistent!
     (reduce-kv (fn [m k v] (assoc! m k (min (long v) cap)))
                (transient {}) merged))))

(defn- dp-order-groups
  "DP bitmask enumeration for inter-group join ordering.
   Minimizes total intermediate cardinality (sum of rows at each step).
   Only considers connected extensions (groups sharing vars with the partial plan)
   to avoid cross-products. Falls back to greedy ordering when n > dp-group-threshold,
   and to cardinality-ordered append for disconnected components.

   When `db` is supplied, the per-step join estimate is BOUND-AWARE (see
   bound-aware-join-rows): a selective probe reduces the carried row count instead
   of the old (max rows i-card) lower bound. Passing nil db preserves the prior
   purely-cardinality behaviour."
  ([groups] (dp-order-groups groups nil))
  ([groups db]
   (let [n (count groups)]
     (cond
       (<= n 1) (vec (range n))
       (> n dp-group-threshold) (greedy-order-groups groups)
       :else
       (let [cards (long-array (map group-effective-card groups))
             edges (build-group-join-graph groups)
            ;; Precompute adjacency: for each group i, which groups share vars?
             adj (mapv (fn [i]
                         (into #{}
                               (keep (fn [j]
                                       (when (and (not= i j)
                                                  (or (contains? edges [i j])
                                                      (contains? edges [j i])))
                                         j)))
                               (range n)))
                       (range n))
            ;; Per-group [pattern-info schema-info base] for bound-aware probes.
             probe (when db (mapv group-probe-info groups))
             full (unchecked-dec (bit-shift-left 1 n))
             dp (object-array (unchecked-inc full))]
        ;; Base cases: each single group as a starting point. State carries a
        ;; :cards map (var→card) threaded through the join estimate.
         (dotimes [i n]
           (let [mask (bit-shift-left 1 i)]
             (aset dp mask {:cost (aget cards i)
                            :order [i]
                            :rows (aget cards i)
                            :cards (cap-cards {} (nth groups i) (aget cards i))})))
        ;; Fill DP table: for each subset, try extending by one connected group
         (doseq [mask (range 1 (unchecked-inc full))]
           (when (aget dp (int mask))
             (let [{:keys [cost order rows] cards-map :cards} (aget dp (int mask))
                   rows (long rows)
                   cost (long cost)]
              ;; Find groups not in mask that are adjacent to some group in mask
               (doseq [i (range n)]
                 (when (zero? (bit-and mask (bit-shift-left 1 i)))
                  ;; Connectivity check: i must share vars with some group in mask
                   (let [connected? (some (fn [j]
                                            (pos? (bit-and mask (bit-shift-left 1 j))))
                                          (nth adj i))]
                     (when connected?
                       (let [new-mask (bit-or mask (bit-shift-left 1 i))
                             i-card (long (aget cards i))
                             [pinfo sinfo base] (when probe (nth probe i))
                            ;; Bound-aware join: a selective probe REDUCES rows
                            ;; (was always max(rows,i-card), never row-reducing).
                             join-rows (long (bound-aware-join-rows
                                              db rows i-card cards-map pinfo sinfo base))
                             new-cost (+ cost join-rows)
                             new-cards (cap-cards cards-map (nth groups i) join-rows)
                             prev (aget dp (int new-mask))]
                         (when (or (nil? prev) (< new-cost (long (:cost prev))))
                           (aset dp (int new-mask)
                                 {:cost new-cost
                                  :order (conj order i)
                                  :rows join-rows
                                  :cards new-cards}))))))))))
        ;; Extract result: if graph is connected, dp[full] has the answer.
        ;; If disconnected (multiple components), chain components by cardinality.
         (if-let [result (aget dp (int full))]
           (:order result)
          ;; Disconnected graph: find largest solved component, then chain remaining
           (let [popcount (fn [^long x]
                            #?(:clj (Long/bitCount x)
                               :cljs (loop [v x c 0]
                                       (if (zero? v) c
                                           (recur (bit-and v (dec v)) (inc c))))))
                 best-mask (reduce (fn [best mask]
                                     (if (aget dp (int mask))
                                       (if (> (long (popcount mask)) (long (popcount best)))
                                         mask best)
                                       best))
                                   0
                                   (range 1 (unchecked-inc full)))
                 main-order (:order (aget dp (int best-mask)))
                 remaining-idxs (filterv (fn [i] (zero? (bit-and best-mask (bit-shift-left 1 i))))
                                         (range n))
                ;; Sort remaining disconnected groups by ascending cardinality
                 sorted-remaining (sort-by #(aget cards (int %)) remaining-idxs)]
             (into (vec main-order) sorted-remaining))))))))

;; ---------------------------------------------------------------------------
;; Cost-based ordering for mixed ops (groups + predicates + functions + OR/NOT)
;;
;; The contract: each op type declares which vars MUST be bound for the op to
;; run correctly at execute time. `op-required-vars` is the single source of
;; truth — adding a new op type means extending this function. `op-cost` then
;; uses it to decide whether an op is runnable (and at what cost) given the
;; current bound-vars set.
;;
;; The split between "input" vars (must be bound) and "output" vars (produced
;; by the op) was historically encoded ad-hoc per op type inside op-cost,
;; which led to drift: e.g. `:external-engine` solver mode used `(some
;; bound-vars op-vars)` while filter/retrieval used `(every? bound-vars
;; input-args)`, even though both have the same fundamental need (input
;; columns must be materialised before the engine can use them). Centralising
;; the contract here makes it easy to audit and to extend.

(def ^:private max-cost #?(:clj Long/MAX_VALUE :cljs (.-MAX_SAFE_INTEGER js/Number)))

(declare branch-produced-vars)

(defn- op-produced-vars
  "Which free-vars an op binds when it runs (its outputs). Used by
   `branch-produced-vars` to compute what an OR-JOIN branch contributes
   to its result rel before limit-rel projects to join-vars.

   - Pattern-scans / entity-groups produce all free-var positions of
     their clause(s).
   - Function / external-engine produce their :binding free-vars.
   - Predicate, NOT, NOT-JOIN produce nothing (pure filters).
   - OR / OR-JOIN: every branch must produce a var for it to be a
     produced var of the OR — recurse via `branch-produced-vars` and
     intersect across branches.
   - Rule calls produce free call-args (the rule body fills them in).
   - Rule-lookup produces its call-args (read from accumulator).
   - Anything unknown produces nothing — the safe assumption."
  [op]
  (case (:op op)
    (:entity-group :pattern-scan)
    (let [scans (cons (:scan-op op) (:merge-ops op))
          all-clauses (filter some? (cons (:clause op) (map :clause scans)))]
      (into #{} (filter analyze/free-var?) (mapcat seq all-clauses)))

    :function
    (into #{} (filter analyze/free-var?) (analyze/extract-vars (:binding op)))

    :external-engine
    (into #{} (filter analyze/free-var?) (analyze/extract-vars (:binding op)))

    (:rule-call :recursive-rule :rule-lookup)
    (into #{} (filter analyze/free-var?) (:call-args op))

    (:or :or-join)
    ;; Vars EVERY branch produces. (See branch-produced-vars / OR-JOIN
    ;; required-vars docstring for the full reasoning.)
    (let [branches (:branches op)
          per-branch (mapv branch-produced-vars branches)]
      (if (seq per-branch)
        (reduce clojure.set/intersection (map set per-branch))
        #{}))

    ;; :predicate, :not, :not-join, :passthrough, :maybe-pred, anything
    ;; unknown → no produced vars.
    #{}))

(defn- branch-produced-vars
  "Vars that an OR-JOIN branch's sub-plan produces internally — the
   union of `op-produced-vars` over the branch's :ops. Used to compute
   the OR(-JOIN)'s required-from-outer set."
  [sub-plan]
  (reduce (fn [acc op] (clojure.set/union acc (op-produced-vars op)))
          #{}
          (:ops sub-plan)))

(defn op-required-vars
  "The set of vars that MUST be bound before `op` can execute correctly.

   Returns a tuple [vars policy] where `policy` is one of:
     :all     — every var in `vars` must be bound
     :any     — at least one var in `vars` must be bound (used by external
                engines that can solve from any known column)
     :none    — op can run unconditionally (producer ops)

   This is consumed by `op-cost`: returning :none means the op is always
   runnable; :all/:any check `bound-vars` accordingly. `vars` is empty
   when policy is :none.

   Per op type (see also docstrings in plan-*-op constructors):

     :entity-group, :pattern-scan
       Producer; emits all its free-var positions. EXCEPT when
       :optional? is set (LOptionalScan from get-else, which routes
       through bind-by-fn at execute time and silently binds the e-var
       to nil if it isn't yet in ctx.rels — see commit c8a11a0e).

     :predicate
       Pure filter; needs every var it references bound — there are no
       outputs.

     :function
       Computes a value from `:args`, binds it to `:binding`. Inputs
       are the free-var symbols inside :args, recursively.

     :external-engine
       Honours the `:input-vars` declaration on the engine's var
       metadata (`:datahike/external-engine`). Stratum's three engines
       all declare `:all-bound`. A nil/absent declaration falls back
       to :all on input-args — the safe default. An engine that can
       work backwards from any one column declares `:any-bound`.

     :rule-lookup
       Producer; reads from `:rule-accumulators` in ctx (populated by
       the surrounding :recursive-rule fixpoint), so its dependency is
       structural, not bound-vars-based.

     :recursive-rule
       Currently strict: every var in :vars (= all free call-args)
       must be bound. This prevents the rule from running as a
       pure producer. Loosening would require knowing per-rule which
       call-args are inputs vs outputs, which requires rule-body
       analysis — left for follow-up. Documented here for reference.

     :or, :or-join
       For an OR(-JOIN) to produce a sound rel, every branch's
       output (after limit-rel projects to join-vars) must contain
       the FULL set of join-vars; otherwise the cross-branch sum-rel
       fails with a different-attrs error. A branch's output covers
       the join-vars produced by its own ops PLUS the join-vars
       carried in via limited-ctx from the outer scope. So the OR's
       binding contract is:
         required-from-outer = join-vars - vars-EVERY-branch-produces
       i.e. for every join-var, either every branch produces it
       internally, or it must already be in the outer ctx. Computed
       here from the planned sub-plans via `branch-produced-vars`,
       which walks each branch's :ops and unions the vars its
       producer ops bind. Policy is :all on the resulting set.

       Surface symptom in jobtech: the changelog `(or-join […] …)`
       has a NOT-only branch that binds nothing of `[?c ?tx ?tx-kind]`
       except `?tx-kind` (via ground); under the previous :any
       policy the planner reordered the binder pattern AFTER the
       OR-JOIN and the NOT-only branch produced a rel without
       `?tx`, tripping sum-rel against the other branches.

     :not, :not-join
       Anti-join; needs the WHOLE set of join-vars / referenced vars
       bound so we can subtract from a fully-formed candidate set.

     :passthrough, anything else
       Unknown / fallback op — treated as un-runnable until it gets
       a concrete contract."
  [op]
  (case (:op op)
    (:entity-group :pattern-scan)
    (cond
      ;; A variable-attribute driving scan (`[?e ?a ?v]` with ?a a logic var)
      ;; whose attr/value vars are produced elsewhere is a CORRELATED join, not
      ;; a fresh producer: order-plan-ops stamps :requires-bound so it waits for
      ;; those vars. Without this it is DP-ordered as a producer and can run
      ;; before its value's producer, collapsing the join onto the non-selective
      ;; attribute var (a Cartesian product across sources).
      (seq (:requires-bound op))
      [(:requires-bound op) :all]

      (and (:optional? op)
           (let [e (first (:clause op))]
             (analyze/free-var? e)))
      [#{(first (:clause op))} :all]

      :else [#{} :none])

    :predicate
    [(set (:vars op)) :all]

    :function
    [(args-free-vars (:args op)) :all]

    :external-engine
    (let [direct-args (args-free-vars (:args op))
          ;; Stratum-style engines reference their input columns as
          ;; keywords inside the query-spec (e.g. :group [:dept]
          ;; :agg [[:avg :salary]]). The runtime in
          ;; execute-external-engine :solver mode maps :dept → ?dept
          ;; before projecting the input relation. The planner must
          ;; recognise the same convention or it schedules the
          ;; engine before its producers and runs it on an empty ctx.
          spec-args (external-engine-spec-vars (:args op))
          input-args (into direct-args spec-args)
          spec (:input-vars-spec op)]
      (case spec
        :any-bound [input-args :any]
        ;; :all-bound and any unrecognised value default to strict.
        ;; nil (no declaration) also defaults to strict — the safe
        ;; assumption.
        [input-args :all]))

    :rule-lookup
    [#{} :none]

    :recursive-rule
    ;; A recursive rule's required-vars is an ORDERING hint, not a correctness
    ;; gate: execute-recursive-rule computes the rule's relation independently and
    ;; hash-joins it in (collapse-rels), so it is correct as a generator (some
    ;; args bound), a full producer (none bound), or a semijoin (all bound).
    ;; A GROUND (literal) call-arg — e.g. a scalar :in root const-substituted into
    ;; the call before planning — makes the magic-set fixpoint SELECTIVE, so the
    ;; rule is a cheap producer that should LEAD. The old `[(:vars op) :all]`
    ;; required the rule's OUTPUT var to be pre-bound, forcing it to run as a late
    ;; filter behind a broad attribute scan that binds the output first (the
    ;; jobtech `indirectly-replaced-by` full-scan). Recognize a ground call-arg and
    ;; mark it runnable with nothing bound; otherwise stay ready once ANY call-arg
    ;; var is bound (covers a collection/tuple :in root, seeded from bound-vars).
    (let [free-args (filterv analyze/free-var? (:call-args op))]
      (cond
        (some #(not (analyze/free-var? %)) (:call-args op)) [#{} :none]
        (seq free-args)                                     [(set free-args) :any]
        :else                                               [#{} :none]))

    (:not :not-join)
    [(set (or (:join-vars op) (:vars op))) :all]

    (:or :or-join)
    ;; Required-from-outer = join-vars MINUS the vars EVERY branch
    ;; produces. Vars that only some branches produce can't be
    ;; relied on across the union — they must come from outer ctx.
    (let [join-vars (set (or (:join-vars op) (:vars op)))
          per-branch (mapv branch-produced-vars (:branches op))
          covered    (if (seq per-branch)
                       (reduce clojure.set/intersection (map set per-branch))
                       #{})
          required   (clojure.set/difference join-vars covered)]
      [required :all])

    ;; Unknown / passthrough — gate forever (caller must decide what to do).
    [#{} :all]))

(defn- op-output-cards
  "The {var → cardinality} an op contributes once it runs. Used to keep a
   running per-var cardinality map through ordering."
  [op]
  (or (:output-var-cards op) {}))

(defn- function-input-rows
  "Estimated number of times a function op runs = size of the relation over its
   input (argument) free vars, approximated as the product of their known
   cardinalities (unknown → 1; the graph/source args are singletons)."
  [op var-cards]
  (reduce (fn [acc v] (* acc (max 1 (long (get var-cards v 1)))))
          1
          (args-free-vars (:args op))))

(defn op-cost
  "Cost of running `op` next given the currently bound vars and their
   cardinalities. Unrunnable ops cost `max-cost`. Producers cost their
   cardinality, predicates 0. Functions cost 1 by default, UNLESS the function
   declares an execution-cost model (`:exec-cost-fn`, from :datahike/cost
   metadata) — then cost = model(input-rows), so an expensive function that
   would run on many input rows is deferred behind selective filters."
  ([op bound-vars] (op-cost op bound-vars {}))
  ([op bound-vars var-cards]
   (let [[required policy] (op-required-vars op)
         ready? (case policy
                  :none true
                  :all  (or (empty? required)
                            (every? #(contains? bound-vars %) required))
                  :any  (some #(contains? bound-vars %) required))]
     (if-not ready?
       max-cost
       (case (:op op)
         (:entity-group :pattern-scan) (group-effective-card op)
         :predicate                    0
         :function                     (if-let [f (:exec-cost-fn op)]
                                         (max 1 (long (f (function-input-rows op var-cards))))
                                         1)
         (or (:estimated-card op) 100))))))

(defn- group-var-attr-clauses
  "Every variable-attribute clause `[?e ?a ?v]` (attribute in a logic var) in an
   entity-group / pattern-scan — its DRIVING scan AND its merge ops — each as
   {:attr-var :val-var}. A variable attribute forces a full scan, so such a
   pattern must be correlated on its (attr, value) rather than DP-ordered as a
   producer. Covering merge ops too handles the case where a fixed-attr pattern
   wins the driving-scan slot and the variable-attribute pattern is a merge."
  [op]
  (when (#{:entity-group :pattern-scan} (:op op))
    (let [clauses (if (= :entity-group (:op op))
                    (keep :clause (cons (:scan-op op) (:merge-ops op)))
                    (some-> (:clause op) vector))]
      (for [clause clauses
            :when (and (sequential? clause) (>= (count clause) 3)
                       (analyze/free-var? (nth clause 1)))]
        {:attr-var (nth clause 1)
         :val-var  (let [v (nth clause 2)] (when (analyze/free-var? v) v))}))))

(defn mark-correlated-var-attr-scans
  "Stamp `:requires-bound` on any variable-attribute scan (driving scan or
   merge) whose attr (and/or value) var is produced by ANOTHER op — turning it
   from a DP-ordered producer into a dependency-ordered correlated join. Reuses
   the canonical `op-produced-vars` (so rule-call / external-engine producers
   count too). Clearing-idempotent: recomputes and drops a stale marker, so it
   is safe to re-run on a replan suffix."
  [ops]
  (let [produce-count (frequencies (mapcat op-produced-vars ops))
        bound-elsewhere? (fn [op v]
                           (and (some? v)
                                (> (long (get produce-count v 0))
                                   (if (contains? (op-produced-vars op) v) 1 0))))]
    (mapv (fn [op]
            (let [req (into #{}
                            (mapcat (fn [{:keys [attr-var val-var]}]
                                      (cond-> []
                                        (bound-elsewhere? op attr-var) (conj attr-var)
                                        (bound-elsewhere? op val-var)  (conj val-var))))
                            (group-var-attr-clauses op))]
              (if (seq req)
                (assoc op :requires-bound req)
                (dissoc op :requires-bound))))
          ops)))

(defn order-plan-ops
  "Order plan operations using DP for entity-group ordering and greedy
   interleaving for dependency-constrained ops (predicates, functions, etc.).
   The DP phase finds the optimal group execution order by minimizing
   total intermediate cardinality. The greedy phase then inserts non-group
   ops at the earliest point where their dependencies are satisfied.

   `outer-bound-vars` (optional) seeds the local bound-vars accumulator
   used by op-cost runnability checks. When this fn is called for a
   nested plan (e.g. an or-join branch's sub-plan, or a :not body),
   the outer scope's already-bound vars (or-join shared-vars,
   ancestor-bound function outputs, …) MUST be seeded — otherwise
   ops like `:not` whose op-required-vars contract demands those vars
   bound will be marked unrunnable and surface as `Insufficient
   bindings` even though they're well-formed at execute time. Top-
   level callers (lower's outer scope) pass `nil` / omit and we treat
   the outer scope as empty."
  ([ops] (order-plan-ops ops nil nil))
  ([ops outer-bound-vars] (order-plan-ops ops outer-bound-vars nil))
  ([ops outer-bound-vars db]
   (let [;; Correlated variable-attribute driving scans get :requires-bound so
        ;; they leave the DP producer pool and wait for their (attr, value) —
        ;; see mark-correlated-var-attr-scans.
         ops (mark-correlated-var-attr-scans ops)
        ;; LOptionalScan-derived pattern-scans (get-else with default) need
        ;; their entity var bound BEFORE they execute — at runtime the
        ;; optional path goes through bind-by-fn(get-else) which evaluates
        ;; per-row, and a row without the e-var bound emits a tuple where
        ;; e-var is nil and the bind-var is the default value. That nil-e
        ;; row poisons every downstream op that uses ?e, surfacing as
        ;; all-nil-tuple find results in jobtech daynotes tests. Treating
        ;; optional scans as regular groups lets dp-order-groups place
        ;; them by cardinality alone (no dependency awareness), so they
        ;; can land before their binders. Demoting them to non-groups
        ;; routes them through the cost-based interleaver, which checks
        ;; bound-vars via op-cost and waits until the e-var is bound.
        ;; A :requires-bound (correlated var-attr) scan is demoted the same way.
         optional-scan? (fn [op]
                          (and (= :pattern-scan (:op op))
                               (:optional? op)))
         dependency-ordered? (fn [op]
                               (or (optional-scan? op) (seq (:requires-bound op))))
         groups (filterv #(and (#{:entity-group :pattern-scan} (:op %))
                               (not (dependency-ordered? %))) ops)
         non-groups (filterv #(or (not (#{:entity-group :pattern-scan} (:op %)))
                                  (dependency-ordered? %)) ops)
         seed-bound (cond
                      (nil? outer-bound-vars) #{}
                      (set? outer-bound-vars) outer-bound-vars
                      (map? outer-bound-vars) (set (keys outer-bound-vars))
                      :else (set outer-bound-vars))
         ;; Running per-var cardinality map, threaded alongside bound-vars so a
         ;; function's :exec-cost-fn can be costed against how many input rows it
         ;; would run on at this point in the order.
         seed-cards (if (map? outer-bound-vars) outer-bound-vars {})]
     (if (empty? groups)
      ;; No groups — pure greedy on non-group ops
       (loop [remaining (set ops)
              bound-vars seed-bound
              var-cards seed-cards
              ordered []]
         (if (empty? remaining)
           ordered
           (let [scored (map (fn [op] [op (op-cost op bound-vars var-cards)]) remaining)
                 executable (filter #(< (second %) max-cost) scored)
                 best (first (sort-by second (if (seq executable) executable scored)))
                 [chosen-op _] best]
             (recur (disj remaining chosen-op)
                    (into bound-vars (:vars chosen-op))
                    (merge-with min var-cards (op-output-cards chosen-op))
                    (conj ordered chosen-op)))))
      ;; DP-order groups, then greedily interleave non-group ops
       (let [dp-order (dp-order-groups groups db)
             ordered-groups (mapv #(nth groups %) dp-order)]
         (if (empty? non-groups)
           ordered-groups
          ;; Interleave: walk the group order, inserting non-group ops
          ;; as soon as their dependencies are satisfied
           (loop [group-q (seq ordered-groups)
                  remaining (set non-groups)
                  bound-vars seed-bound
                  var-cards seed-cards
                  result []]
             (if (and (nil? group-q) (empty? remaining))
               result
              ;; Pick the globally cheapest READY action — either the cheapest
              ;; ready non-group op OR the next pre-ordered group — rather than
              ;; always flushing ready non-groups first. A rule/OR op that is a
              ;; pure producer (op-required-vars :none) is "ready" with nothing
              ;; bound, but is often far more expensive than a selective binder
              ;; group that would constrain it; flushing it first made it scan
              ;; its whole relation set as a generator (jobtech `edge` rule:
              ;; 17s → ~ms once the concept binder runs first). Readiness still
              ;; gates correctness (op-required-vars); among ready actions we
              ;; now order purely by cost. Groups are preferred on ties since
              ;; they bind producer vars that can unlock/cheapen pending ops.
               (let [ready    (filterv (fn [op] (< (op-cost op bound-vars var-cards) max-cost)) remaining)
                     ;; One cost model: op-cost = rows × per-unit for every op.
                     ;; This statically DEFERS an expensive function behind both
                     ;; row-reducing joins and cheaper functions (correct), and
                     ;; also behind an input-EXPANDING join (incorrect) — the
                     ;; latter is the one case the executor's hoist corrects at
                     ;; runtime (execute.cljc/hoist-expensive-fn), since whether a
                     ;; join expands or reduces is not knowable statically.
                     best-ng  (when (seq ready)
                                (apply min-key #(op-cost % bound-vars var-cards) ready))
                     ng-cost  (when best-ng (long (op-cost best-ng bound-vars var-cards)))
                     next-g   (first group-q)
                     g-cost   (when next-g (long (group-effective-card next-g)))
                     ;; choose :group | :non-group | :force
                     choice   (cond
                                (and best-ng next-g) (if (< ng-cost g-cost) :non-group :group)
                                best-ng              :non-group
                                next-g               :group
                                :else                :force)]
                 (case choice
                   :group     (recur (next group-q)
                                     remaining
                                     (into bound-vars (:vars next-g))
                                     (merge-with min var-cards (op-output-cards next-g))
                                     (conj result next-g))
                   :non-group (recur group-q
                                     (disj remaining best-ng)
                                     (into bound-vars (:vars best-ng))
                                     (merge-with min var-cards (op-output-cards best-ng))
                                     (conj result best-ng))
                   ;; No more groups, remaining ops still unready — force the
                   ;; cheapest so we make progress (preserves prior behaviour).
                   :force     (let [scored (map (fn [op] [op (op-cost op bound-vars var-cards)]) remaining)
                                    [chosen-op _] (first (sort-by second scored))]
                                (recur nil
                                       (disj remaining chosen-op)
                                       (into bound-vars (:vars chosen-op))
                                       (merge-with min var-cards (op-output-cards chosen-op))
                                       (conj result chosen-op)))))))))))))

;; ---------------------------------------------------------------------------
;; OR / NOT / Rule plan ops

;; ---------------------------------------------------------------------------
;; Rule SCC detection and expansion

(defn- rule-call-graph
  "Build a call graph from rules: {rule-name → #{called-rule-names}}"
  [rules]
  (into {}
        (map (fn [[rule-name branches]]
               [rule-name
                (into #{}
                      (comp (mapcat (fn [branch]
                                      (let [[_head & body] branch]
                                        (filter #(and (sequential? %)
                                                      (symbol? (first %))
                                                      (contains? rules (first %)))
                                                body))))
                            (map first))
                      branches)]))
        rules))

(defn- tarjan-scc
  "Tarjan's SCC algorithm. Returns SCCs in reverse topological order.
   Each SCC is a set of rule names."
  [graph]
  (let [index (volatile! 0)
        stack (volatile! [])
        on-stack (volatile! #{})
        indices (volatile! {})
        lowlinks (volatile! {})
        result (volatile! [])]
    (letfn [(strong-connect [v]
              (let [idx @index]
                (vswap! indices assoc v idx)
                (vswap! lowlinks assoc v idx)
                (vswap! index inc)
                (vswap! stack conj v)
                (vswap! on-stack conj v)
                (doseq [w (get graph v #{})]
                  (cond
                    (not (contains? @indices w))
                    (do (strong-connect w)
                        (vswap! lowlinks update v min (get @lowlinks w)))
                    (contains? @on-stack w)
                    (vswap! lowlinks update v min (get @indices w))))
                (when (= (get @lowlinks v) (get @indices v))
                  ;; v is root of an SCC — pop stack until v
                  (loop [scc #{}]
                    (let [w (peek @stack)]
                      (vswap! stack pop)
                      (vswap! on-stack disj w)
                      (let [scc' (conj scc w)]
                        (if (= w v)
                          (vswap! result conj scc')
                          (recur scc'))))))))]
      (doseq [v (keys graph)]
        (when-not (contains? @indices v)
          (strong-connect v))))
    @result))

(defn compute-rule-sccs
  "Compute SCCs for the rule graph. Returns a map:
   {rule-name → {:scc #{rule-names} :recursive? bool}}"
  [rules]
  (let [graph (rule-call-graph rules)
        sccs (tarjan-scc graph)]
    (into {}
          (mapcat (fn [scc]
                    (let [recursive? (or (> (count scc) 1)
                                         ;; Single-node SCC that calls itself
                                         (let [r (first scc)]
                                           (contains? (get graph r #{}) r)))]
                      (map (fn [r] [r {:scc scc :recursive? recursive?}]) scc))))
          sccs)))

(defn plan-rule-lookup-op
  "Create a plan op for a rule-call inside a recursive branch.
   The :mode is :delta or :main, indicating which accumulator to read from."
  [clause-info mode]
  (let [[rule-name & call-args] (:clause clause-info)]
    {:op :rule-lookup
     :clause (:clause clause-info)
     :rule-name rule-name
     :call-args (vec call-args)
     :mode mode
     :vars (:vars clause-info)
     :estimated-card 100}))

;; ---------------------------------------------------------------------------
;; Public API

(defn replan
  "Adaptively re-plan remaining operations after observing actual cardinality."
  [plan executed-idx actual-card db]
  (let [remaining-ops (subvec (vec (:ops plan)) (inc executed-idx))
        executed-ops (subvec (vec (:ops plan)) 0 (inc executed-idx))
        bound-vars (into #{} (mapcat :vars) executed-ops)
        re-estimated (mapv (fn [op]
                             (if (= :pattern-scan (:op op))
                               (let [new-est (estimate/estimate-pattern
                                              db
                                              (analyze/classify-clause (:clause op))
                                              (:schema-info op))]
                                 (assoc op :estimated-card (or new-est (:estimated-card op))))
                               op))
                           remaining-ops)
        ;; Seed re-ordering with bound-vars from the already-executed
        ;; prefix: the runnability check for the remaining ops has to
        ;; honour what's already bound or :not / :predicate / :function
        ;; ops will be wrongly marked as Insufficient.
        re-ordered (order-plan-ops re-estimated bound-vars db)]
    (assoc plan :ops (into (vec executed-ops) re-ordered))))
