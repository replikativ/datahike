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

(declare create-plan)

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
  "Create a plan op for a function clause. Checks for external engine metadata."
  ([fn-info] (plan-function-op fn-info nil))
  ([fn-info db]
   (let [engine-meta (resolve-external-engine-meta (:fn-sym fn-info))]
     (if engine-meta
       (plan-external-engine-op fn-info engine-meta db)
       (let [out-cards (function-output-var-cards
                        (:fn-sym fn-info) (:args fn-info) (:binding fn-info))]
         (cond-> {:op :function
                  :clause (:clause fn-info)
                  :fn-sym (:fn-sym fn-info)
                  :args (:args fn-info)
                  :binding (:binding fn-info)
                  :vars (:vars fn-info)
                  :estimated-card nil}
           out-cards (assoc :output-var-cards out-cards)))))))

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

(defn- args-free-vars
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
  "Effective output cardinality for a group, accounting for attached predicates."
  ^long [group]
  (let [base (long (or (:estimated-card group) 1000000))]
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

(defn- dp-order-groups
  "DP bitmask enumeration for inter-group join ordering.
   Minimizes total intermediate cardinality (sum of rows at each step).
   Only considers connected extensions (groups sharing vars with the partial plan)
   to avoid cross-products. Falls back to greedy ordering when n > dp-group-threshold,
   and to cardinality-ordered append for disconnected components."
  [groups]
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
            ;; Join selectivity: when joining group i into a partial plan,
            ;; estimate result = max(plan-rows, group-rows) for FK-like joins.
            ;; For M:N we'd need distinct counts; this is a safe upper bound.
            full (unchecked-dec (bit-shift-left 1 n))
            dp (object-array (unchecked-inc full))]
        ;; Base cases: each single group as a starting point
        (dotimes [i n]
          (let [mask (bit-shift-left 1 i)]
            (aset dp mask {:cost (aget cards i)
                           :order [i]
                           :rows (aget cards i)})))
        ;; Fill DP table: for each subset, try extending by one connected group
        (doseq [mask (range 1 (unchecked-inc full))]
          (when (aget dp (int mask))
            (let [{:keys [cost order rows]} (aget dp (int mask))
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
                            ;; Join cardinality estimate: for an equi-join on shared
                            ;; vars, result ≈ max(probe-side, build-side) for FK joins.
                            ;; Conservative: use max as the join output.
                            join-rows (Math/max rows i-card)
                            new-cost (+ cost join-rows)
                            prev (aget dp (int new-mask))]
                        (when (or (nil? prev) (< new-cost (long (:cost prev))))
                          (aset dp (int new-mask)
                                {:cost new-cost
                                 :order (conj order i)
                                 :rows join-rows}))))))))))
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
            (into (vec main-order) sorted-remaining)))))))

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
    (if (and (:optional? op)
             (let [e (first (:clause op))]
               (analyze/free-var? e)))
      [#{(first (:clause op))} :all]
      [#{} :none])

    :predicate
    [(set (:vars op)) :all]

    :function
    [(args-free-vars (:args op)) :all]

    :external-engine
    (let [input-args (args-free-vars (:args op))
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
    [(set (or (:join-vars op) (:vars op))) :all]

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

(defn op-cost [op bound-vars]
  (let [[required policy] (op-required-vars op)
        ready? (case policy
                 :none true
                 :all  (or (empty? required)
                           (every? #(contains? bound-vars %) required))
                 :any  (some #(contains? bound-vars %) required))]
    (if-not ready?
      max-cost
      ;; Per-op cost when ready: producers use cardinality, filters cost 0,
      ;; functions cost 1, others use :estimated-card or fall back to 100.
      (case (:op op)
        (:entity-group :pattern-scan) (group-effective-card op)
        :predicate                    0
        :function                     1
        (or (:estimated-card op) 100)))))

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
   level callers (create-plan's outer scope) pass `nil` / omit and
   we treat the outer scope as empty."
  ([ops] (order-plan-ops ops nil))
  ([ops outer-bound-vars]
   (let [;; LOptionalScan-derived pattern-scans (get-else with default) need
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
         optional-scan? (fn [op]
                          (and (= :pattern-scan (:op op))
                               (:optional? op)))
         groups (filterv #(and (#{:entity-group :pattern-scan} (:op %))
                               (not (optional-scan? %))) ops)
         non-groups (filterv #(or (not (#{:entity-group :pattern-scan} (:op %)))
                                  (optional-scan? %)) ops)
         seed-bound (cond
                      (nil? outer-bound-vars) #{}
                      (set? outer-bound-vars) outer-bound-vars
                      (map? outer-bound-vars) (set (keys outer-bound-vars))
                      :else (set outer-bound-vars))]
     (if (empty? groups)
      ;; No groups — pure greedy on non-group ops
       (loop [remaining (set ops)
              bound-vars seed-bound
              ordered []]
         (if (empty? remaining)
           ordered
           (let [scored (map (fn [op] [op (op-cost op bound-vars)]) remaining)
                 executable (filter #(< (second %) max-cost) scored)
                 best (first (sort-by second (if (seq executable) executable scored)))
                 [chosen-op _] best]
             (recur (disj remaining chosen-op)
                    (into bound-vars (:vars chosen-op))
                    (conj ordered chosen-op)))))
      ;; DP-order groups, then greedily interleave non-group ops
       (let [dp-order (dp-order-groups groups)
             ordered-groups (mapv #(nth groups %) dp-order)]
         (if (empty? non-groups)
           ordered-groups
          ;; Interleave: walk the group order, inserting non-group ops
          ;; as soon as their dependencies are satisfied
           (loop [group-q (seq ordered-groups)
                  remaining (set non-groups)
                  bound-vars seed-bound
                  result []]
             (if (and (nil? group-q) (empty? remaining))
               result
              ;; First, flush any non-group ops whose deps are now satisfied
               (let [ready (filterv (fn [op] (< (op-cost op bound-vars) max-cost)) remaining)]
                 (if (seq ready)
                  ;; Insert the cheapest ready non-group op
                   (let [best (first (sort-by #(op-cost % bound-vars) ready))]
                     (recur group-q
                            (disj remaining best)
                            (into bound-vars (:vars best))
                            (conj result best)))
                  ;; No ready non-group ops — emit next group
                   (if group-q
                     (let [g (first group-q)]
                       (recur (next group-q)
                              remaining
                              (into bound-vars (:vars g))
                              (conj result g)))
                    ;; No more groups but remaining ops still unready — force them
                     (let [scored (map (fn [op] [op (op-cost op bound-vars)]) remaining)
                           best (first (sort-by second scored))
                           [chosen-op _] best]
                       (recur nil
                              (disj remaining chosen-op)
                              (into bound-vars (:vars chosen-op))
                              (conj result chosen-op))))))))))))))

;; ---------------------------------------------------------------------------
;; OR / NOT / Rule plan ops

(defn- normalize-and-plan-branches
  "Normalize branch clauses and create sub-plans for each branch.
   Used by both OR and OR-JOIN planning.

   Branch forms:
   - Single data pattern:  [?e :attr ?v]       → wrap as [[?e :attr ?v]]
   - Single predicate:     [(pred ?a ?b)]       → wrap as [[(pred ?a ?b)]]
   - Multiple clauses:     [[?e :a ?v] [(> ?v 5)]] → use as-is
   - AND compound:         (and [?e :a ?v] ...)  → use as-is

   The key distinction: a multi-clause branch has a vector as its first element
   (Datalog clauses are vectors). A single predicate like [(= ?a 1)] has a list
   as its first element (the function call expression)."
  [db branches bound-vars rules]
  (mapv (fn [branch]
          (let [branch-clauses (if (and (sequential? branch)
                                        (not (vector? (first branch))))
                                 [branch]
                                 (vec branch))]
            (create-plan db branch-clauses bound-vars rules)))
        branches))

(defn plan-or-op
  "Plan an OR or OR-JOIN clause. When join-vars? is true, validates and
   includes :join-vars in the op (OR-JOIN semantics)."
  ([db clause-info bound-vars rules]
   (plan-or-op db clause-info bound-vars rules false))
  ([db clause-info bound-vars rules join-vars?]
   (let [join-vars (when join-vars?
                     (let [raw (:join-vars clause-info)]
                       (when (some sequential? raw)
                         (throw (ex-info (str "Insufficient bindings: "
                                              (into #{} (mapcat analyze/extract-vars) raw)
                                              " not bound in " (:clause clause-info))
                                         {:error :query/where :form (:clause clause-info)})))
                       (set raw)))
         sub-plans (normalize-and-plan-branches db (:branches clause-info) bound-vars rules)
         total-est (reduce + 0 (keep (fn [p] (some :estimated-card (:ops p))) sub-plans))]
     (cond-> {:op (if join-vars? :or-join :or)
              :clause (:clause clause-info)
              :branches sub-plans
              :vars (:vars clause-info)
              :estimated-card (max 1 total-est)}
       join-vars? (assoc :join-vars join-vars)))))

(defn plan-not-op
  "Plan a NOT or NOT-JOIN clause. When join-vars? is true, scopes the
   sub-plan to only the join-vars (NOT-JOIN semantics)."
  ([db clause-info bound-vars rules]
   (plan-not-op db clause-info bound-vars rules false))
  ([db clause-info bound-vars rules join-vars?]
   (let [join-vars (when join-vars? (set (:join-vars clause-info)))
         sub-plan (create-plan db (vec (:sub-clauses clause-info))
                               (if join-vars? join-vars bound-vars) rules)]
     (cond-> {:op (if join-vars? :not-join :not)
              :clause (:clause clause-info)
              :sub-plan sub-plan
              :vars (:vars clause-info)
              :estimated-card nil}
       join-vars? (assoc :join-vars join-vars)))))

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

(defn- rename-branch-vars
  "Rename variables in a rule branch body, substituting rule-args with call-args.
   Constant call-args get synthetic variables with identity-binding preamble so they
   are available to function clauses in the body. Internal vars get unique suffixes.
   Returns vector of renamed clauses."
  [branch call-args seqid db]
  (let [[[_ & rule-args] & clauses] branch
        ;; Replace constant call-args with synthetic variables
        call-args-safe (map-indexed (fn [i arg]
                                      (if (analyze/free-var? arg)
                                        arg
                                        (symbol (str "?__const__" i "__auto__" seqid))))
                                    call-args)
        ;; Build identity-binding clauses for constant args (not equality predicates!)
        ;; [(identity 42) ?__const__0__auto__1] creates a binding for the synthetic var.
        const-bindings (into []
                             (keep (fn [[safe orig]]
                                     (when (not= safe orig)
                                       [(list 'identity orig) safe])))
                             (map vector call-args-safe call-args))
        replacements (zipmap rule-args call-args-safe)
        ;; Resolve keyword attrs to entity refs in attribute-refs mode.
        ;;
        ;; The resolution must recurse through compound clause forms
        ;; (or, or-join, and, not, not-join, source-prefixed) to reach
        ;; data patterns nested inside them. Without recursion, a rule
        ;; body like `(or-join [...] [?e :attr ?v] ...)` would keep its
        ;; inner pattern's attribute as a keyword while the outer
        ;; query's clauses get resolved by substitute-consts-with-lookup-refs
        ;; — at execute-time the lookup-batch-search path then slices
        ;; AEVT for the keyword, finds zero datoms (datoms in
        ;; :attribute-refs? mode are stored with attr=eid, not keyword),
        ;; and the rule branch silently produces empty results. Surface
        ;; symptom in jobtech: 3 changelog tests on HistoricalDB return
        ;; 0 rows instead of the expected change events.
        attr-refs? (:attribute-refs? (dbi/-config db))
        data-pattern? (fn [x]
                        (and (vector? x)
                             (let [f (first x)]
                               (or (and (symbol? f) (analyze/free-var? f))
                                   (number? f)
                                   (and (vector? f) (= 2 (count f)))))
                             (or (keyword? (second x))
                                 (and (symbol? (second x)) (analyze/free-var? (second x)))
                                 (number? (second x)))))
        resolve-attr-in-pattern (fn [pat]
                                  (if (and attr-refs? (keyword? (second pat)))
                                    (assoc pat 1 (dbi/-ref-for db (second pat)))
                                    pat))
        resolve-recursive (fn resolve-recursive [form]
                            (cond
                              ;; Data pattern: resolve its attr.
                              (data-pattern? form)
                              (resolve-attr-in-pattern form)

                              ;; Compound list form (or, or-join, and, not, not-join, etc.)
                              ;; — recurse into elements that look like clauses.
                              (and (sequential? form)
                                   (symbol? (first form))
                                   (#{'or 'or-join 'and 'not 'not-join} (first form)))
                              (let [head (first form)
                                    ;; or-join / not-join have a vars vector after the head
                                    [pre-rest body] (case head
                                                      (or-join not-join) [(take 2 form) (drop 2 form)]
                                                      [(take 1 form) (rest form)])]
                                (concat pre-rest (map resolve-recursive body)))

                              :else form))
        renamed (mapv (fn [c]
                        (resolve-recursive
                         (clojure.walk/postwalk
                          (fn [x]
                            (if (analyze/free-var? x)
                              (if (contains? replacements x)
                                (get replacements x)
                                (symbol (str (name x) "__auto__" seqid)))
                              x))
                          c)))
                      clauses)]
    ;; Put const-bindings first so synthetic vars are bound before body uses them
    (into const-bindings renamed)))

(defn plan-rule-op [db clause-info bound-vars rules scc-info]
  (let [[rule-name & call-args] (:clause clause-info)
        ;; Validate: non-var rule args must be scalars (not collections/maps)
        _ (doseq [arg call-args]
            (when (and (not (analyze/free-var? arg))
                       (not (nil? arg))
                       (or (and (vector? arg)
                                (not= 2 (count arg)))  ;; allow lookup-refs [attr val]
                           (map? arg)
                           (set? arg)))
              (throw (ex-info (str "Bad format for value in pattern, must be a scalar, nil or a vector of two elements. Got: " (pr-str arg))
                              {:error :query/where
                               :form (:clause clause-info)}))))
        branches (get rules rule-name)]
    (if (not branches)
      (plan-passthrough-op clause-info)
      (let [{:keys [scc recursive?]} (get scc-info rule-name)]
        (if recursive?
          ;; Recursive rule — pre-build branch plans with clause versions.
          ;; IMPORTANT: We use the rule head vars (all free) for branch renaming,
          ;; NOT the call-args (which may contain constants like 62).
          ;; Constants are filtered AFTER the fixpoint completes.
          ;; This ensures the recursive accumulator contains the full relation.
          ;;
          ;; For mutual recursion (SCC with multiple rules), we collect base/rec
          ;; branches from ALL rules in the SCC. Each rule has its own accumulator.
          (let [seqid (gensym "r")
                scc-rule-names scc
                is-scc-call? (fn [c]
                               (and (sequential? c)
                                    (symbol? (first c))
                                    (contains? scc-rule-names (first c))))
                ;; For each SCC rule, extract head vars and build branch plans
                scc-rule-plans
                (into {}
                      (map (fn [rn]
                             (let [rn-branches (get rules rn)
                                   head-vars (vec (rest (first (first rn-branches))))
                                   free-call-args (mapv (fn [hv]
                                                          (if (analyze/free-var? hv)
                                                            hv
                                                            (symbol (str "?" (name hv)))))
                                                        head-vars)
                                   is-base? (fn [branch]
                                              (let [[_head & body] branch]
                                                (not (some is-scc-call? body))))
                                   base-bs (filterv is-base? rn-branches)
                                   rec-bs (filterv (complement is-base?) rn-branches)
                          ;; Head vars are NOT pre-bound at the start of either
                          ;; base or recursive branch bodies:
                          ;;
                          ;;  - Base branch: head-vars are produced by the body
                          ;;    (a pattern binds them, a function computes them).
                          ;;  - Recursive branch: head-vars are produced by the
                          ;;    branch's rule-lookup op (the accumulator scan),
                          ;;    which is itself a regular op the planner orders.
                          ;;    Rule-lookup's outputs propagate through the
                          ;;    bindedness tracker like any other producer.
                          ;;
                          ;; Earlier code added head-vars to branch-bound with a
                          ;; conservative card-1 placeholder, intending it as a
                          ;; cardinality hint. Under the new op-required-vars
                          ;; contract, however, any entry in bound-vars is
                          ;; treated as runnability-bound — so :function ops
                          ;; that reference a head-var (e.g. `[(str ?id "/")
                          ;; ?path]`) would appear runnable from clause-zero,
                          ;; get cost-ordered AHEAD of the pattern that
                          ;; actually binds the var, and produce a relation
                          ;; missing the function's output binding. The
                          ;; downstream rel-dedup-into! then sees a head-var
                          ;; missing from :attrs and NPEs.
                          ;;
                          ;; Fix: keep branch-bound = the OUTER scope's bound
                          ;; vars only. Cardinality estimation for patterns
                          ;; that reference head-vars now correctly treats them
                          ;; as free (worst-case attribute-total estimate)
                          ;; until the body's own producer op binds them with
                          ;; a known card.
                                   branch-bound bound-vars
                                   ;; Route rule branch bodies through the full IR pipeline
                                   ;; (build-logical-plan → lower) rather than the physical-only
                                   ;; create-plan. Top-level queries get the logical pass for free
                                   ;; from `create-plan-via-ir` in query.cljc; rule bodies didn't,
                                   ;; so kernel features that depend on logical recognition (most
                                   ;; visibly `[(get-else $ ?e :attr default) ?v]` being promoted
                                   ;; to an `LOptionalScan` that binds `?e`) silently degraded
                                   ;; inside rule bodies. requiring-resolve breaks the cycle:
                                   ;; lower.cljc already requires plan.cljc, and plan-rule-op needs
                                   ;; to call lower for each rule branch.
                                   build-logical (requiring-resolve
                                                  'datahike.query.logical/build-logical-plan)
                                   lower-fn (requiring-resolve
                                             'datahike.query.lower/lower)
                                   ;; build-logical-plan expects a set, but bound-vars
                                   ;; can arrive as a {var → cardinality} map (lower.cljc
                                   ;; passes bvc here). lower itself accepts either form.
                                   branch-bound-set (cond (map? branch-bound) (set (keys branch-bound))
                                                          (set? branch-bound) branch-bound
                                                          :else (set branch-bound))
                                   plan-branch (fn plan-branch
                                                 [branch-clauses guarded]
                                                 (let [logical (build-logical
                                                                db branch-clauses branch-bound-set
                                                                rules guarded)]
                                                   (lower-fn logical db rules)))
                                   base-ps (mapv (fn [b]
                                                   (let [renamed (rename-branch-vars b free-call-args seqid db)]
                                                     (plan-branch (vec renamed) nil)))
                                                 base-bs)
                                   rec-cvs
                                   (vec (mapcat
                                         (fn [branch]
                                           (let [renamed (rename-branch-vars branch free-call-args seqid db)
                                                 scc-indices (keep-indexed
                                                              (fn [i c] (when (is-scc-call? c) i))
                                                              renamed)]
                                             (map (fn [delta-idx]
                                                    (let [versioned
                                                          (vec (map-indexed
                                                                (fn [i c]
                                                                  (if (is-scc-call? c)
                                                                    (with-meta (vec c)
                                                                      {:rule-lookup-mode
                                                                       (if (= i delta-idx) :delta :main)})
                                                                    c))
                                                                renamed))]
                                                      (plan-branch versioned scc-rule-names)))
                                                  scc-indices)))
                                         rec-bs))]
                               [rn {:head-vars free-call-args
                                    :base-plans base-ps
                                    :rec-clause-versions rec-cvs}])))
                      scc-rule-names)
                ;; Note: an earlier `has-scanless-base?` guard nilled out
                ;; `scc-rule-plans` whenever a base case lacked an
                ;; `:entity-group` / `:pattern-scan` op (e.g. SQL
                ;; `WITH RECURSIVE … (SELECT 1 …)` anchor lowering to
                ;; `[(identity 1) ?n]`, or a `[(ground […]) [?v ...]]`
                ;; collection seed), routing such rules to `legacy/solve-rule`.
                ;; Legacy can't evaluate recursive bodies that bind head vars
                ;; through `:function` ops and then filter them with predicates
                ;; — those queries hung or failed.
                ;;
                ;; The fixpoint executor already handles function-only base
                ;; cases: `execute-branch-plans` runs the base plan against
                ;; an empty ctx, `legacy/bind-by-fn` produces a single-tuple
                ;; Relation, and the recursive branch's `rule-lookup` ops feed
                ;; off the accumulator as usual. Magic sets are silently
                ;; skipped when `base-scan-attr` is nil (the
                ;; `and magic-demand base-scan-attr …` check in
                ;; `execute-recursive-rule`). The guard was redundant and
                ;; introduced a real regression for scanless-base recursion.
                ;; Extract base scan attribute for magic set optimization
                base-scan-attr
                (when scc-rule-plans
                  (let [bp (first (:base-plans (get scc-rule-plans rule-name)))]
                    (when bp
                      (some (fn [op]
                              (case (:op op)
                                :entity-group (get (:clause (:scan-op op)) 1)
                                :pattern-scan (get (:clause op) 1)
                                nil))
                            (:ops bp)))))]
            {:op :recursive-rule
             :clause (:clause clause-info)
             :rule-name rule-name
             :call-args (vec call-args)
             :head-vars (when scc-rule-plans (:head-vars (get scc-rule-plans rule-name)))
             :scc-rule-names scc-rule-names
             :scc-rule-plans scc-rule-plans
             :base-plans (when scc-rule-plans (:base-plans (get scc-rule-plans rule-name)))
             :rec-clause-versions (when scc-rule-plans (:rec-clause-versions (get scc-rule-plans rule-name)))
             :base-scan-attr base-scan-attr
             :vars (:vars clause-info)
             :estimated-card nil})
          ;; Non-recursive — expand to OR
          (let [seqid (gensym "r")
                expanded (for [branch branches]
                           (rename-branch-vars branch call-args seqid db))
                ;; Body's bound-vars = outer scope's bound-vars only.
                ;;
                ;; The const-safe-vars (synthetic vars wrapping non-var call-args)
                ;; are produced by the [(identity X) safe-var] preamble that
                ;; rename-branch-vars prepends to each branch body. They are
                ;; NOT bound at the start of the body — they're bound by the
                ;; identity op's execution. Pre-binding them in rule-bound
                ;; would look like a cardinality hint (card 1, since identity
                ;; produces a single value) but under the central op-required-
                ;; vars contract, any entry in bound-vars is read as runnability
                ;; -bound. A predicate `[(< ?safe 100)]` referencing a pre-
                ;; bound safe-var would then appear runnable from clause-zero,
                ;; cost-order ahead of the identity preamble, and run against
                ;; an empty :rels at execute time.
                ;;
                ;; The card-1 cardinality hint we want for downstream
                ;; estimation reaches subsequent clauses naturally via
                ;; bvc-eff threading: identity is the first clause processed
                ;; in create-plan's reduce, its function-output-var-cards
                ;; map (?safe → 1) is folded into bvc-eff by extend-bvc, and
                ;; every later clause sees safe-var → 1 in its planning
                ;; environment without it leaking into the runnability
                ;; check's seed-bound set.
                ;; Same IR-pipeline routing as the recursive branch above —
                ;; rule bodies need the logical pass so `[(get-else …) ?v]`
                ;; becomes `LOptionalScan` (otherwise `?e` stays unbound
                ;; in the rule body). requiring-resolve breaks the
                ;; plan↔lower cycle.
                build-logical (requiring-resolve
                               'datahike.query.logical/build-logical-plan)
                lower-fn (requiring-resolve
                          'datahike.query.lower/lower)
                ;; See the recursive-branch comment above: bound-vars may
                ;; arrive as a Map (var → card); build-logical-plan wants a Set.
                bound-set (cond (map? bound-vars) (set (keys bound-vars))
                                (set? bound-vars) bound-vars
                                :else (set bound-vars))
                sub-plans (mapv (fn [branch-clauses]
                                  (let [logical (build-logical
                                                 db (vec branch-clauses) bound-set rules)]
                                    (lower-fn logical db rules)))
                                expanded)
                total-est (reduce + 0 (keep (fn [p] (some :estimated-card (:ops p))) sub-plans))]
            {:op :or
             :clause (:clause clause-info)
             :branches sub-plans
             :vars (:vars clause-info)
             :estimated-card (max 1 total-est)}))))))

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

(defn create-plan
  "Create an optimized query plan from where clauses.
   Steps:
   1. Classify all clauses
   2. Detect pushable predicates
   3. Build pattern ops, fold into entity-groups with anti-merge NOT
   4. Order groups + remaining ops by estimated cardinality
   5. Detect inter-group value joins (probe-vars)

   The optional `guarded-rules` param is a set of rule names that are in the
   current SCC — calls to these become :rule-lookup ops instead of recursive expansion."
  ([db where-clauses bound-vars]
   (create-plan db where-clauses bound-vars nil))
  ([db where-clauses bound-vars rules]
   (create-plan db where-clauses bound-vars rules nil))
  ([db where-clauses bound-vars rules guarded-rules]
   (let [;; Step 1: Classify
         classified (mapv analyze/classify-clause where-clauses)

         ;; Step 1b: Pre-collect all pattern vars from the full clause tree.
         ;; This includes vars from patterns inside NOT/OR/source-prefix.
         ;; Used as enriched bound-vars when building sub-plans, so nested
         ;; NOT/OR know what vars will be available from the outer context.
         ;;
         ;; bound-vars may be a Map (var → card) under the bound-var-cards
         ;; model or a Set (legacy interface). When a Map: preserve the
         ;; outer entries verbatim (so cards reach inner pattern estimation
         ;; via plan-or-op / plan-rule-op / plan-not-op → create-plan), and
         ;; mark newly-extracted in-scope vars with a non-numeric sentinel.
         ;; estimate-pattern-with-bindings's card-of returns nil for
         ;; non-numbers, so sentinel entries are ignored for cardinality
         ;; estimation while still passing `contains?` membership checks
         ;; needed for join-var validation and rule-call detection.
         all-clause-vars (let [extracted (into #{} (mapcat analyze/extract-vars) where-clauses)]
                           (cond
                             (map? bound-vars)
                             (into bound-vars
                                   (map (fn [v] [v ::in-scope]))
                                   (remove (set (keys bound-vars)) extracted))

                             (set? bound-vars)
                             (into bound-vars extracted)

                             :else
                             (into (set bound-vars) extracted)))

         ;; Pre-compute SCC info for rules (once per plan, not per rule-op)
         scc-info (when rules (compute-rule-sccs rules))

         ;; Step 2: Detect pushdown candidates
         {:keys [pushdowns consumed]} (analyze/detect-pushdown classified bound-vars)

         ;; Step 3: Build raw ops — track consumed pushdown preds.
         ;;
         ;; bvc-eff threads cardinality propagation through the clause stream
         ;; in classification order: each op's :output-var-cards extends the
         ;; effective bound-var-cards for SUBSEQUENT ops in the same plan
         ;; scope. Critical for queries where a function or pattern
         ;; introduces a tightly-bounded var that a later pattern can use as
         ;; a probe constraint — e.g. the `(ground inv-map) [[?type ?reverse-type]]`
         ;; preamble inside the -reverse-edge rule body, which binds
         ;; ?reverse-type to a card-N collection that the next pattern
         ;; `[?r :type ?reverse-type]` should use as scan-card. Without
         ;; this thread, every pattern in the body sees only outer
         ;; bound-vars and falls back to base attribute estimates,
         ;; producing wrong scan-vs-merge selection (the regression that
         ;; made the GraphQL edge fetcher 40×+ slower than legacy).
         init-bvc (cond (map? bound-vars) bound-vars
                        (set? bound-vars) (zipmap bound-vars (repeat ::in-scope))
                        :else (zipmap (set bound-vars) (repeat ::in-scope)))
         {:keys [ops actual-consumed not-ops]}
         (reduce
          (fn [acc ci]
            ;; Check for rule calls (classified as :pattern but first elem is rule name)
            (let [bvc-eff (:bvc-eff acc)
                  is-rule-call? (and rules
                                     (= :pattern (:type ci))
                                     (symbol? (:e ci))
                                     (not (analyze/free-var? (:e ci)))
                                     (contains? rules (:e ci)))
                  ;; Check for metadata-tagged rule-lookup (from clause version generation)
                  lookup-mode (when is-rule-call?
                                (:rule-lookup-mode (meta (:clause ci))))
                  ;; Extend an acc's :bvc-eff with op's :output-var-cards
                  ;; (numeric entries only — sentinels untouched). Returns acc'.
                  ;; MIN on collisions keeps the tightest known bound.
                  ;;
                  ;; Restricted to FUNCTION ops only: pattern-scan ops also expose
                  ;; :output-var-cards, but propagating them eagerly would mark
                  ;; sibling pattern entity-vars (e.g. ?r in `[?r :concept-1 ?c]`
                  ;; followed by `[?r :type ?t]`) as upstream-bound when planning
                  ;; the next pattern. dp-order-fuse-ops fuses these into a
                  ;; single entity-group at execution time — the scan PRODUCES
                  ;; ?r, it doesn't consume it — so estimating later patterns
                  ;; with ?r bound yields the both-bound branch min(e,v,base)
                  ;; which underestimates by ~base/distinct-v×. Function ops
                  ;; (identity / ground) genuinely bind their output vars
                  ;; before subsequent ops execute, so threading those is safe.
                  extend-bvc (fn [acc' op]
                               (if (and (map? op) (= :function (:op op)))
                                 (if-let [out (:output-var-cards op)]
                                   (assoc acc' :bvc-eff
                                          (reduce-kv
                                           (fn [m v c]
                                             (let [cur (get m v)]
                                               (cond
                                                 (and (number? cur) (number? c)) (assoc m v (long (min cur c)))
                                                 (number? c) (assoc m v c)
                                                 :else m)))
                                           (:bvc-eff acc') out))
                                   acc')
                                 acc'))]
              (if (and is-rule-call? guarded-rules
                       (contains? guarded-rules (:e ci)))
                ;; Inside a recursive branch plan — emit :rule-lookup
                (let [rule-ci (assoc ci :type :rule-call
                                     :vars (into #{} (filter analyze/free-var?) (rest (:clause ci))))
                      mode (or lookup-mode :main)]
                  (update acc :ops conj (plan-rule-lookup-op rule-ci mode)))
                (if is-rule-call?
                  (let [rule-ci (assoc ci :type :rule-call
                                       :vars (into #{} (filter analyze/free-var?) (rest (:clause ci))))
                        op (plan-rule-op db rule-ci bvc-eff rules scc-info)]
                    (-> acc (update :ops conj op) (extend-bvc op)))

                  (case (:type ci)
                    :pattern
                    (let [schema-info (analyze/pattern-schema-info db ci)
                          preds (get pushdowns (:clause ci) [])
                          [op consumed-preds] (plan-pattern-op db ci schema-info preds bvc-eff)]
                      (-> acc
                          (update :ops conj op)
                          (update :actual-consumed into (or consumed-preds #{}))
                          (extend-bvc op)))

                    :function (let [op (plan-function-op ci db)]
                                (-> acc (update :ops conj op) (extend-bvc op)))
                    :predicate (update acc :ops conj [:maybe-pred ci])
                    :or (let [op (plan-or-op db ci bvc-eff rules)]
                          (-> acc (update :ops conj op) (extend-bvc op)))
                    :or-join (let [op (plan-or-op db ci bvc-eff rules true)]
                               (-> acc (update :ops conj op) (extend-bvc op)))
                    ;; Collect NOT ops separately for anti-merge folding
                    :not (update acc :not-ops conj (plan-not-op db ci bvc-eff rules))
                    :not-join (update acc :ops conj (plan-not-op db ci bvc-eff rules true))
                    :and (let [sub-plan (create-plan db (vec (:sub-clauses ci)) bvc-eff rules)]
                           (update acc :ops into (:ops sub-plan)))

                    ;; Source-prefixed clause: ($2 not ...) or [$2 ?e :attr ?v]
                    ;; Unwrap the source, re-classify inner clause, and tag with :source
                    :source-prefix
                    (let [source-sym (:source-sym ci)
                          inner (:inner-clause ci)
                          inner-ci (analyze/classify-clause inner)
                          inner-ci (assoc inner-ci :source source-sym)]
                      (case (:type inner-ci)
                        :pattern
                        (let [schema-info (analyze/pattern-schema-info db inner-ci)
                              [op _] (plan-pattern-op db inner-ci schema-info [] bvc-eff)]
                          (-> acc (update :ops conj (assoc op :source source-sym)) (extend-bvc op)))
                        :not
                        (let [op (plan-not-op db inner-ci bvc-eff rules)]
                          (update acc :ops conj (assoc op :source source-sym)))
                        :not-join
                        (let [op (plan-not-op db inner-ci bvc-eff rules true)]
                          (update acc :ops conj (assoc op :source source-sym)))
                        :or
                        (update acc :ops conj (assoc (plan-or-op db inner-ci bvc-eff rules) :source source-sym))
                        :or-join
                        (update acc :ops conj (assoc (plan-or-op db inner-ci bvc-eff rules true) :source source-sym))
                        ;; Source-prefix wrapping another source-prefix or unknown
                        (update acc :ops conj (plan-passthrough-op ci))))

                    (update acc :ops conj (plan-passthrough-op ci)))))))
          {:ops [] :actual-consumed #{} :not-ops [] :bvc-eff init-bvc}
          classified)

         ;; Collect pattern ops first for predicate sampling
         raw-pattern-ops (filterv #(and (map? %) (= :pattern-scan (:op %))) ops)

         ;; Resolve deferred predicates (with sampling for selectivity)
         resolved-ops (reduce
                       (fn [acc op]
                         (if (and (vector? op) (= :maybe-pred (first op)))
                           (let [ci (second op)]
                             (if (contains? actual-consumed (:clause ci))
                               acc
                               (conj acc (plan-predicate-op ci db raw-pattern-ops))))
                           (conj acc op)))
                       []
                       ops)

         ;; Step 3b: Separate pattern ops from non-pattern ops
         pattern-ops (filterv #(= :pattern-scan (:op %)) resolved-ops)
         other-ops (filterv #(not= :pattern-scan (:op %)) resolved-ops)

         ;; Step 3c: Build entity groups (includes anti-merge NOT folding)
         total-entities (estimate/estimate-total-entities db)
         {:keys [entity-groups remaining-nots]}
         (if (seq pattern-ops)
           (build-entity-groups db pattern-ops not-ops total-entities)
           {:entity-groups [] :remaining-nots not-ops})

         ;; Step 3d: Restore predicates that were consumed as pushdowns but ended up
         ;; on merge-ops (where pushdowns can't be applied — merges use EAVT lookupGE).
         merge-lost-pred-clauses (into #{} (comp (filter #(= :entity-group (:op %)))
                                                 (mapcat :merge-lost-preds))
                                       entity-groups)
         restored-preds (when (seq merge-lost-pred-clauses)
                          (let [ci-by-clause (into {} (map (fn [ci] [(:clause ci) ci])) classified)]
                            (mapv (fn [pred-clause]
                                    (plan-predicate-op (ci-by-clause pred-clause) db raw-pattern-ops))
                                  merge-lost-pred-clauses)))
         other-ops (if (seq restored-preds)
                     (into other-ops restored-preds)
                     other-ops)
         ;; Strip internal :merge-lost-preds from entity-group ops
         entity-groups (mapv #(dissoc % :merge-lost-preds) entity-groups)

         ;; Step 4: Order everything together. Seed the cost-based
         ;; runnability check with the outer scope's bound-vars so
         ;; nested plans (or-join branches, :not bodies, …) recognise
         ;; vars bound by their ancestors. Without the seed, an
         ;; or-join branch that wraps a `:not` referencing a shared-var
         ;; bound from the outer scope is rejected with `Insufficient
         ;; bindings` — the shared-var is bound at execute time but
         ;; the local order-plan-ops doesn't know that.
         all-ops (into (into (vec entity-groups) other-ops) remaining-nots)
         ordered-ops (order-plan-ops all-ops bound-vars)

         ;; Step 5: Detect inter-group value joins
         group-joins (detect-inter-group-joins
                      (filterv #(#{:entity-group :pattern-scan} (:op %)) ordered-ops))

         ;; Step 6: Validate NOT/NOT-JOIN bindings post-ordering.
         ;; Walk the ordered ops and accumulate bound vars. Each NOT must have
         ;; at least one var bound by ops that PRECEDE it in the execution order.
         ;; This correctly handles reordering: (not [?e ...]) [?e :name] gets
         ;; reordered to [?e :name] (not [?e ...]), and ?e is bound before NOT.
         _ (loop [remaining ordered-ops
                  ;; bound-vars may be a set (legacy) or a map (var → card).
                  ;; vars-so-far is purely a membership set for NOT validation,
                  ;; so normalise to a set up front.
                  vars-so-far (cond (map? bound-vars) (set (keys bound-vars))
                                    (set? bound-vars) bound-vars
                                    :else (set bound-vars))]
             (when (seq remaining)
               (let [op (first remaining)]
                 (when (#{:not :not-join} (:op op))
                   (let [not-vars (:vars op)]
                     (when (empty? (clojure.set/intersection not-vars vars-so-far))
                       (throw (ex-info (str "Insufficient bindings: none of " not-vars
                                            " is bound in " (:clause op))
                                       {:error :query/where
                                        :form (:clause op)})))))
                 (recur (rest remaining)
                        (into vars-so-far
                              (case (:op op)
                                :entity-group (into (:vars (:scan-op op))
                                                    (mapcat :vars (:merge-ops op)))
                                :pattern-scan (:vars op)
                                ;; plan-function-op stores the result var(s) in
                                ;; :binding (scalar, tuple, list, or map). The
                                ;; legacy `:bind-vars` key is never set —
                                ;; reading it lost the result-var contribution
                                ;; and falsely tripped the NOT validation when
                                ;; a function-chain output was the only var
                                ;; reaching a NOT clause. Mirror lower.cljc's
                                ;; identical loop.
                                :function (analyze/extract-vars (:binding op))
                                nil))))))]

     {:ops ordered-ops
      :consumed-preds (if (seq merge-lost-pred-clauses)
                        (reduce disj actual-consumed merge-lost-pred-clauses)
                        actual-consumed)
      :classified classified
      :group-joins group-joins
      :has-passthrough? (some #(= :passthrough (:op %)) ordered-ops)})))

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
        re-ordered (order-plan-ops re-estimated bound-vars)]
    (assoc plan :ops (into (vec executed-ops) re-ordered))))
