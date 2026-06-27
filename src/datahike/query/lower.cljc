(ns datahike.query.lower
  "Lowering pass: transforms a LogicalPlan into a physical plan map.

   Makes all physical decisions:
   - Predicate pushdown detection
   - Index selection per pattern
   - DP-optimal merge ordering within entity groups
   - Anti-merge selectivity sorting
   - Greedy cost-based operation ordering
   - Inter-group hash-probe join detection
   - NOT/NOT-JOIN binding validation

   The output is a physical plan map:
   {:ops, :consumed-preds, :classified, :group-joins, :has-passthrough?}
   consumed by execute.cljc."
  (:require
   [clojure.set]
   [clojure.walk]
   [datahike.db.interface :as dbi]
   [datahike.index.interface :as di]
   [datahike.query.analyze :as analyze]
   [datahike.query.estimate :as estimate]
   [datahike.query.ir :as ir]
   [datahike.query.logical :as logical]
   [datahike.query.plan :as plan]))

#?(:clj (set! *warn-on-reflection* true))

;; ---------------------------------------------------------------------------
;; IR node → classified clause-info reconstruction
;;
;; plan.cljc helpers expect classified clause maps. These converters
;; reconstruct them from IR record fields.

(defn- scan->classified
  "Convert an LScan or LOptionalScan back to a classified clause map for
   plan helpers. Shared pattern fields are read via keyword access; the
   optional-only extras (`:optional?` / `:default-value`) are attached
   only when the input is an LOptionalScan."
  [scan]
  (cond-> {:type :pattern
           :clause (:clause scan)
           :pattern (vec (:clause scan))
           :vars (:vars scan)
           :e (:e scan)
           :a (:a scan)
           :v (:v scan)
           :tx (:tx scan)}
    (instance? datahike.query.ir.LOptionalScan scan)
    (assoc :optional? true
           :default-value (:default-value scan))))

(defn- filter->classified
  "Convert an LFilter back to a classified clause map."
  [^datahike.query.ir.LFilter f]
  {:type :predicate
   :clause (.-clause f)
   :fn-sym (.-fn_sym f)
   :args (.-args f)
   :vars (.-vars f)})

(defn- bind->classified
  "Convert an LBind back to a classified clause map."
  [^datahike.query.ir.LBind b]
  {:type :function
   :clause (.-clause b)
   :fn-sym (.-fn_sym b)
   :args (.-args b)
   :binding (.-binding b)
   :vars (.-vars b)})

(defn- anti-join->classified
  "Convert an LAntiJoin back to a classified clause map."
  [^datahike.query.ir.LAntiJoin aj]
  (let [t (.-type aj)]
    (cond-> {:type t
             :clause (.-clause aj)
             :vars (.-vars aj)
             :sub-clauses (.-sub_nodes aj)}
      (= t :not-join) (assoc :join-vars (.-join_vars aj)))))

(defn- union->classified
  "Convert an LUnion back to a classified clause map."
  [^datahike.query.ir.LUnion u]
  (let [t (.-type u)
        clause (.-clause u)]
    (cond-> {:type t
             :clause clause
             :vars (.-vars u)
             :branches (case t
                         :or (rest clause)
                         :or-join (drop 2 clause))}
      (= t :or-join) (assoc :join-vars (.-join_vars u)))))

(defn- rule-call->classified
  "Convert an LRuleCall back to a classified clause map."
  [^datahike.query.ir.LRuleCall rc]
  {:type :rule-call
   :clause (.-clause rc)
   :vars (.-vars rc)
   :e (.-rule_name rc)})

(defn- rule-lookup->classified
  "Convert an LRuleLookup to args for plan/plan-rule-lookup-op."
  [^datahike.query.ir.LRuleLookup rl]
  {:type :rule-call
   :clause (.-clause rl)
   :vars (.-vars rl)
   :e (.-rule_name rl)})

(defn- passthrough->classified
  "Convert an LPassthrough back to a classified clause map."
  [^datahike.query.ir.LPassthrough p]
  {:type (.-type p)
   :clause (.-clause p)
   :vars (.-vars p)})
;; ---------------------------------------------------------------------------
;; Sub-plan factories (OR / NOT / rule expansion)
;;
;; These factories used to live in plan.cljc, but they need to call the
;; lowering pipeline recursively (for each OR branch, NOT body, or rule
;; branch the planner sees, we re-run build-logical-plan → lower so the
;; sub-plan gets the same logical recognition top-level queries get).
;; Putting them here keeps the dependency one-way (lower → plan, never
;; the reverse) and lets the sub-plan calls go straight to `lower`
;; without the `requiring-resolve` dance the previous layout required.

(declare lower)

(defn- plan-via-ir
  "Lower a sub-plan via the full IR pipeline (`build-logical-plan →
   lower`). The single point where the sub-plan factories below enter
   the lowering pipeline.

   `bound-vars` may arrive as a Set (legacy interface) or a Map
   (`{var → cardinality}` — the form lower threads through as `bvc`).
   `build-logical-plan` wants a Set; lower itself accepts either."
  ([db clauses bound-vars rules]
   (plan-via-ir db clauses bound-vars rules nil))
  ([db clauses bound-vars rules guarded-rules]
   (let [bound-set (cond (map? bound-vars) (set (keys bound-vars))
                         (set? bound-vars) bound-vars
                         :else (set bound-vars))
         logical-plan (logical/build-logical-plan db (vec clauses) bound-set
                                                  rules guarded-rules)]
     (lower logical-plan db rules))))

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
   as its first element (the function call expression).

   Sub-plans are built via the full IR pipeline so each branch gets the
   logical pass (LOptionalScan recognition for get-else, etc.) before
   the physical lowering — same as top-level queries."
  [db branches bound-vars rules]
  (mapv (fn [branch]
          (let [branch-clauses (if (and (sequential? branch)
                                        (not (vector? (first branch))))
                                 [branch]
                                 (vec branch))]
            (plan-via-ir db branch-clauses bound-vars rules)))
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
         sub-plan (plan-via-ir db (:sub-clauses clause-info)
                               (if join-vars? join-vars bound-vars) rules)]
     (cond-> {:op (if join-vars? :not-join :not)
              :clause (:clause clause-info)
              :sub-plan sub-plan
              :vars (:vars clause-info)
              :estimated-card nil}
       join-vars? (assoc :join-vars join-vars)))))

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
      (plan/plan-passthrough-op clause-info)
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
                                   ;; Rule branch bodies go through the
                                   ;; shared IR-pipeline helper — same
                                   ;; routing OR / NOT / AND sub-plans
                                   ;; use, so kernel features that
                                   ;; depend on logical recognition
                                   ;; (most visibly `[(get-else $ ?e
                                   ;; :attr default) ?v]` becoming an
                                   ;; LOptionalScan that binds `?e`)
                                   ;; apply uniformly inside rule
                                   ;; bodies.
                                   plan-branch (fn plan-branch
                                                 [branch-clauses guarded]
                                                 (plan-via-ir db branch-clauses branch-bound
                                                              rules guarded))
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
                ;; Extract base scan attribute for magic set optimization.
                ;;
                ;; magic-base-scan REPLACES the whole base case with single-attr
                ;; EAVT point lookups from the demand entities, mapping each
                ;; scanned datom's [entity value] onto the rule's two head vars.
                ;; That is correct ONLY for a *linear single ref-edge* base —
                ;; exactly one base branch, that branch a single scan op
                ;; [?h0 attr ?h1] over the two head vars, with `attr` a ref so the
                ;; propagated value is itself an entity (re-scanned next iteration).
                ;;
                ;; Anything else must leave base-scan-attr nil and fall back to the
                ;; general fixpoint (execute-branch-plans runs every base branch):
                ;;   - multiple base branches (only one attr would be scanned);
                ;;   - a value-join base like [?a :id ?x][?c :id ?x] (?c is a
                ;;     different entity sharing a value, not attr's value — the old
                ;;     code grabbed :id off the first pattern and fed the string
                ;;     value into the entity slot → ClassCastException);
                ;;   - a non-ref attr (the head value is a scalar, not an entity).
                base-scan-attr
                (when scc-rule-plans
                  (let [rule-plan  (get scc-rule-plans rule-name)
                        base-plans (:base-plans rule-plan)
                        head-vars  (:head-vars rule-plan)]
                    (when (and (= 1 (count base-plans)) (= 2 (count head-vars)))
                      (let [ops (:ops (first base-plans))]
                        (when (= 1 (count ops))
                          (let [op (first ops)
                                [clause sinfo]
                                (case (:op op)
                                  :pattern-scan [(:clause op) (:schema-info op)]
                                  :entity-group (when (empty? (:merge-ops op))
                                                  [(:clause (:scan-op op))
                                                   (:schema-info (:scan-op op))])
                                  nil)
                                [e a v] clause]
                            (when (and clause
                                       (:ref? sinfo)
                                       (= (set head-vars) #{e v}))
                              a)))))))]
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
                ;; Same IR-pipeline routing as the recursive branch above
                ;; and as the OR / NOT / AND sub-plan paths in create-plan.
                sub-plans (mapv (fn [branch-clauses]
                                  (plan-via-ir db branch-clauses bound-vars rules))
                                expanded)
                total-est (reduce + 0 (keep (fn [p] (some :estimated-card (:ops p))) sub-plans))]
            {:op :or
             :clause (:clause clause-info)
             :branches sub-plans
             :vars (:vars clause-info)
             :estimated-card (max 1 total-est)}))))))

;; ---------------------------------------------------------------------------
;; Collect all scans from logical plan (including from entity joins)

(defn- collect-all-scans
  "Extract all LScan nodes from the logical plan, including those
   nested inside LEntityJoin nodes."
  [nodes]
  (reduce
   (fn [acc node]
     (cond
       (ir/scan? node)
       (conj acc node)

       (instance? datahike.query.ir.LEntityJoin node)
       (let [^datahike.query.ir.LEntityJoin ej node]
         (into (into acc (.-scans ej)) (.-anti_scans ej)))

       :else acc))
   []
   nodes))

;; Pipeline construction and structural fusability are now in plan.cljc:
;; plan/build-pipeline, plan/structurally-fusable?

;; ---------------------------------------------------------------------------
;; Per-node output cardinality estimates for outer bound-var-cards propagation
;;
;; To make scan-vs-merge selection inside a rule body / OR branch / entity-group
;; aware of how constrained an upstream-bound var actually is, we estimate per-LP-
;; node output-var cardinalities BEFORE lowering, then for each node N feed the
;; UNION of OTHER nodes' estimates as `bound-var-cards` into N's lowering. This
;; replaces the prior "vars are either bound or free" model with "vars are
;; either bound with-known-cardinality or free", so estimate-pattern-with-bindings
;; can produce differentiated estimates instead of returning the attribute-total
;; for every pattern with the same attribute.

(defn- estimate-node-output-cards
  "Estimate output cardinality per free-var for an LP node, without lowering it.
   Returns {var-symbol → long} or nil if the estimate isn't useful.

   Currently handles pure data-pattern nodes (LScan, LOptionalScan) where the
   estimate is direct and cheap. Other node types return nil — their downstream
   propagation falls back to the existing all-vars-treated-as-free behavior.
   Extensions for LEntityJoin / LRuleCall / LUnion can be added in follow-ups
   as we measure their planning impact."
  [node db]
  (cond
    (ir/scan? node)
    (let [ci (scan->classified node)
          si (analyze/pattern-schema-info db ci)
          est (or (estimate/estimate-pattern db ci si)
                  (di/-count (:eavt db)))
          free-output-vars (filter analyze/free-var? (:vars ci))]
      (when (seq free-output-vars)
        (zipmap free-output-vars (repeat (long est)))))

    :else nil))

(defn- bound-var-cards-for-node
  "Compute the bound-var-cards map to pass when lowering `node`: the merge of
   every OTHER node's output-card estimate plus the static initial bound-vars
   (treated as known but with unknown cardinality — placeholder card 1 since
   :in-bound vars are typically singletons in practice and a placeholder is
   safer than omitting them entirely).

   When duplicate vars appear across multiple producers, take the MIN — the
   tightest known bound."
  [node node->cards initial-bound-vars]
  (let [merged (reduce-kv (fn [acc other-node other-cards]
                            (if (or (identical? other-node node) (nil? other-cards))
                              acc
                              (reduce-kv (fn [acc' v c]
                                           (let [prev (get acc' v)]
                                             (assoc acc' v (if prev (min prev c) c))))
                                         acc
                                         other-cards)))
                          {}
                          node->cards)]
    ;; Add initial :in-bound vars with placeholder cardinality.
    ;; Existing call sites that pass a Set still work via estimate-pattern-with-bindings'
    ;; defensive lookup, but going forward callers within the planner should pass
    ;; this enriched map.
    (reduce (fn [m v] (if (contains? m v) m (assoc m v 1)))
            merged
            (seq initial-bound-vars))))

;; ---------------------------------------------------------------------------
;; Entity group construction from LEntityJoin

(defn- lower-entity-join
  "Lower an LEntityJoin to an :entity-group physical op.
   - Builds pattern-scan ops for each scan via plan/plan-pattern-op
   - Applies DP merge ordering
   - Sorts anti-merges by selectivity
   - Computes output cardinality estimate"
  [^datahike.query.ir.LEntityJoin ej db pushdowns consumed-acc bound-vars total-entities]
  (let [scans (.-scans ej)
        anti-scans (.-anti_scans ej)
        source (.-source ej)

        ;; Build physical pattern-scan ops for each scan
        {scan-ops :ops new-consumed :consumed}
        (reduce
         (fn [acc scan]
           (let [ci (scan->classified scan)
                 schema-info (analyze/pattern-schema-info db ci)
                 preds (get pushdowns (:clause ci) [])
                 [op consumed-preds] (plan/plan-pattern-op db ci schema-info preds bound-vars)]
             (-> acc
                 (update :ops conj (cond-> op source (assoc :source source)))
                 (update :consumed into (or consumed-preds #{})))))
         {:ops [] :consumed #{}}
         scans)

        ;; Build anti-merge ops
        anti-ops
        (mapv (fn [anti-scan]
                (let [ci (scan->classified anti-scan)
                      schema-info (analyze/pattern-schema-info db ci)
                      [op _] (plan/plan-pattern-op db ci schema-info [] bound-vars)]
                  (assoc op :anti? true)))
              anti-scans)

        ;; Delegate DP ordering, anti-merge sorting, cardinality estimation,
        ;; and pipeline construction to the shared primitive.
        {:keys [op merge-lost-preds]}
        (plan/assemble-entity-group db (.-entity_var ej) source
                                    scan-ops anti-ops total-entities)]

    {:op (assoc op :merge-lost-preds merge-lost-preds)
     :consumed new-consumed}))

(defn- lower-standalone-scan
  "Lower a standalone LScan or LOptionalScan to a :pattern-scan physical op."
  [scan db pushdowns consumed-acc bound-vars]
  (let [ci (scan->classified scan)
        source (:source scan)
        schema-info (analyze/pattern-schema-info db ci)
        preds (get pushdowns (:clause ci) [])
        [op consumed-preds] (plan/plan-pattern-op db ci schema-info preds bound-vars)]
    {:op (cond-> (assoc op :pipeline (plan/build-pipeline op [] db))
           source (assoc :source source))
     :consumed (or consumed-preds #{})}))

;; ---------------------------------------------------------------------------
;; Main lowering pass

(defn lower
  "Lower a LogicalPlan to a physical plan map.

   The output is a physical plan map:
   {:ops [ordered-physical-ops...]
    :consumed-preds #{consumed-predicate-clauses}
    :classified [classified-clause-maps...]
    :group-joins {consumer-idx → {:producer-idx :probe-vars}}
    :has-passthrough? boolean}"
  [logical-plan db rules]
  (let [nodes (.-nodes ^datahike.query.ir.LogicalPlan logical-plan)
        bound-vars (.-bound_vars ^datahike.query.ir.LogicalPlan logical-plan)
        classified (.-classified ^datahike.query.ir.LogicalPlan logical-plan)
        all-clause-vars (into bound-vars (mapcat :vars) classified)

        ;; Pre-compute SCC info for rules
        scc-info (when rules (plan/compute-rule-sccs rules))

        ;; ---------------------------------------------------------------
        ;; Step 1: Pushdown detection
        ;; Collect all pattern + predicate classified clauses for pushdown analysis
        all-scans (collect-all-scans nodes)
        scan-classifieds (mapv scan->classified all-scans)
        filter-nodes (filterv #(instance? datahike.query.ir.LFilter %) nodes)
        filter-classifieds (mapv filter->classified filter-nodes)

        {:keys [pushdowns consumed]}
        (analyze/detect-pushdown
         (into scan-classifieds filter-classifieds)
         bound-vars)

        ;; ---------------------------------------------------------------
        ;; Step 2: Lower each node to physical op(s)
        total-entities (estimate/estimate-total-entities db)

        ;; Per-node output-card estimates + initial bound-vars (Map form).
        ;; SOURCE-ORDER directional bvc: each node's bound-var-cards is the
        ;; union of `initial-bvc` and outputs of every node with a STRICTLY
        ;; LOWER `:source-idx` (set by build-logical-plan). This avoids the
        ;; over-binding bug where build-logical-plan groups all
        ;; entity-nodes BEFORE rule-nodes, causing the lower.cljc reduce
        ;; to add later-in-source patterns' outputs to the rule call's
        ;; bvc — making the rule body's planner treat consumed-by-later
        ;; vars (e.g. ?related-c in the edge query) as upstream-bound
        ;; probe constraints. Source order is sound (Datalog is set
        ;; semantics; correctness doesn't depend on bvc accuracy — only
        ;; estimate quality and downstream scan choice). Datalog
        ;; programmers conventionally write clauses in dependency order,
        ;; so source-idx is a reasonable proxy for "what's bound entering
        ;; this clause." The execution path itself is reordered later by
        ;; order-plan-ops based on actual cost, independent of this.
        node->output-cards (zipmap nodes (map #(estimate-node-output-cards % db) nodes))
        ;; Per-node BINDING vars — the free vars a node binds for
        ;; downstream consumption. Distinct from output-cards (which
        ;; carries numeric cardinality estimates and is restricted to
        ;; scan nodes to avoid the sibling-pattern-bound regression
        ;; called out in the extend-bvc comment block in plan.cljc).
        ;; Bindedness is the contract that the post-ordering NOT
        ;; validation and op-required-vars runnability checks rely
        ;; on; without it, an or-join branch whose :not references a
        ;; var bound by an outer get-else / function clause is
        ;; rejected with `Insufficient bindings` even though the var
        ;; is bound at execute time. We add bindings as ::in-scope
        ;; sentinels — estimate-pattern-with-bindings' card-of treats
        ;; non-numeric entries as "bound but unknown card", so this
        ;; doesn't perturb cardinality estimation.
        node-binding-vars (fn [^datahike.query.ir.LogicalPlan _logical n]
                            (cond
                              (ir/scan? n)
                              (filter analyze/free-var? (:vars (scan->classified n)))
                              (instance? datahike.query.ir.LBind n)
                              (analyze/extract-vars (.-binding ^datahike.query.ir.LBind n))
                              (instance? datahike.query.ir.LEntityJoin n)
                              (let [scans (concat (.-scans ^datahike.query.ir.LEntityJoin n) [])]
                                (mapcat (fn [s] (filter analyze/free-var? (:vars (scan->classified s))))
                                        scans))
                              (instance? datahike.query.ir.LUnion n)
                              (let [jv (.-join_vars ^datahike.query.ir.LUnion n)]
                                (when jv (vec jv)))
                              (instance? datahike.query.ir.LRuleCall n)
                              (let [args (.-call_args ^datahike.query.ir.LRuleCall n)]
                                (filter analyze/free-var? args))
                              :else nil))
        node->binding-vars (zipmap nodes (map #(node-binding-vars logical-plan %) nodes))
        ;; Nodes without :source-idx (AND-flattened sub-plans, etc.) are
        ;; treated as "comes last" via a sentinel beyond any real index.
        ;; #?(:clj Long/MAX_VALUE :cljs js/Number.MAX_SAFE_INTEGER) keeps
        ;; this portable across both the JVM and the Node target.
        max-src-idx #?(:clj Long/MAX_VALUE :cljs (.-MAX_SAFE_INTEGER js/Number))
        node->src-idx (into {} (map (fn [n] [n (or (:source-idx (meta n))
                                                   max-src-idx)])) nodes)
        initial-bvc (cond (map? bound-vars) bound-vars
                          :else (zipmap (or bound-vars #{}) (repeat 1)))
        merge-cards (fn [acc-map outs]
                      (reduce-kv
                       (fn [m v c]
                         (let [cur (get m v)]
                           (cond
                             (and (number? cur) (number? c)) (assoc m v (long (min cur c)))
                             (number? c) (assoc m v c)
                             :else m)))
                       acc-map outs))
        merge-bindings (fn [acc-map vs]
                         (reduce (fn [m v]
                                   (if (contains? m v) m (assoc m v ::in-scope)))
                                 acc-map
                                 (or vs [])))
        ;; Pre-compute bvc per node from PRECEDING source-idx nodes'
        ;; outputs (cardinality, scan-only) and bindings (every binder
        ;; node).
        node->bvc (into {}
                        (map (fn [n]
                               (let [my-idx (node->src-idx n)
                                     prior (filter (fn [other]
                                                     (let [oi (node->src-idx other)]
                                                       (and (not (identical? other n))
                                                            (< oi my-idx))))
                                                   nodes)
                                     bvc (reduce (fn [acc o]
                                                   (cond-> acc
                                                     (some? (node->output-cards o))
                                                     (merge-cards (node->output-cards o))
                                                     (seq (node->binding-vars o))
                                                     (merge-bindings (node->binding-vars o))))
                                                 initial-bvc prior)]
                                 [n bvc])))
                        nodes)

        {:keys [ops actual-consumed not-ops]}
        (reduce
         (fn [acc node]
           (let [bvc (get node->bvc node initial-bvc)]
             (cond
               ;; LEntityJoin → entity-group op
               (instance? datahike.query.ir.LEntityJoin node)
               (let [{:keys [op consumed]}
                     (lower-entity-join node db pushdowns (:actual-consumed acc)
                                        bvc total-entities)]
                 (-> acc
                     (update :ops conj op)
                     (update :actual-consumed into consumed)))

               ;; Standalone LScan or LOptionalScan → pattern-scan op
               (ir/scan? node)
               (let [{:keys [op consumed]}
                     (lower-standalone-scan node db pushdowns (:actual-consumed acc)
                                            bvc)]
                 (-> acc
                     (update :ops conj op)
                     (update :actual-consumed into consumed)))

               ;; LFilter → deferred predicate (may be consumed by pushdown)
               (instance? datahike.query.ir.LFilter node)
               (update acc :ops conj [:maybe-pred (filter->classified node)])

               ;; LBind → function op
               (instance? datahike.query.ir.LBind node)
               (update acc :ops conj (plan/plan-function-op (bind->classified node) db))

               ;; LAntiJoin → NOT/NOT-JOIN op (delegate to plan.cljc)
               (instance? datahike.query.ir.LAntiJoin node)
               (let [ci (anti-join->classified node)
                     join-vars? (= :not-join (.-type ^datahike.query.ir.LAntiJoin node))
                     op (plan-not-op db ci all-clause-vars rules join-vars?)]
                 (if join-vars?
                   (update acc :ops conj op)
                   (update acc :not-ops conj op)))

               ;; LUnion → OR/OR-JOIN op (delegate to plan.cljc)
               (instance? datahike.query.ir.LUnion node)
               (let [ci (union->classified node)
                     join-vars? (= :or-join (.-type ^datahike.query.ir.LUnion node))]
                 (update acc :ops conj (plan-or-op db ci bvc rules join-vars?)))

               ;; LRuleCall → delegate to plan-rule-op
               (instance? datahike.query.ir.LRuleCall node)
               (let [ci (rule-call->classified node)]
                 (update acc :ops conj (plan-rule-op db ci bvc rules scc-info)))

             ;; LRuleLookup → rule lookup op
               (instance? datahike.query.ir.LRuleLookup node)
               (let [ci (rule-lookup->classified node)]
                 (update acc :ops conj
                         (plan/plan-rule-lookup-op ci (.-mode ^datahike.query.ir.LRuleLookup node))))

             ;; LPassthrough → passthrough op
               (instance? datahike.query.ir.LPassthrough node)
               (let [ci (passthrough->classified node)
                     ptype (.-type ^datahike.query.ir.LPassthrough node)]
                 (if (= :source-prefix ptype)
                 ;; Source-prefix passthrough: re-classify inner clause and delegate
                   (let [clause (.-clause ^datahike.query.ir.LPassthrough node)
                         source-sym (first clause)
                         inner-clause (vec (rest clause))
                         inner-ci (analyze/classify-clause inner-clause)
                         inner-ci (assoc inner-ci :source source-sym)]
                     (case (:type inner-ci)
                       :not (let [op (plan-not-op db inner-ci all-clause-vars rules)]
                              (update acc :ops conj (assoc op :source source-sym)))
                       :not-join (let [op (plan-not-op db inner-ci all-clause-vars rules true)]
                                   (update acc :ops conj (assoc op :source source-sym)))
                       :or (update acc :ops conj
                                   (assoc (plan-or-op db inner-ci all-clause-vars rules) :source source-sym))
                       :or-join (update acc :ops conj
                                        (assoc (plan-or-op db inner-ci all-clause-vars rules true) :source source-sym))
                       (update acc :ops conj (plan/plan-passthrough-op ci))))
                 ;; Regular passthrough
                   (update acc :ops conj (plan/plan-passthrough-op ci))))

               ;; Anything else → passthrough
               :else
               (update acc :ops conj (plan/plan-passthrough-op
                                      {:clause nil :type :unknown :vars #{}})))))
         {:ops [] :actual-consumed #{} :not-ops []}
         nodes)

        ;; ---------------------------------------------------------------
        ;; Step 3: Resolve deferred predicates
        ;; Collect pattern ops for predicate sampling
        raw-pattern-ops (filterv #(and (map? %) (= :pattern-scan (:op %))) ops)

        resolved-ops
        (reduce
         (fn [acc op]
           (if (and (vector? op) (= :maybe-pred (first op)))
             (let [ci (second op)]
               (if (contains? actual-consumed (:clause ci))
                 acc ;; consumed by pushdown
                 (conj acc (plan/plan-predicate-op ci db raw-pattern-ops))))
             (conj acc op)))
         []
         ops)

        ;; ---------------------------------------------------------------
        ;; Step 4: Separate entity-groups from other ops, then build entity groups
        ;; from pattern-scan ops that weren't already part of an LEntityJoin
        pattern-ops (filterv #(= :pattern-scan (:op %)) resolved-ops)
        entity-group-ops (filterv #(= :entity-group (:op %)) resolved-ops)
        other-ops (filterv #(not (#{:pattern-scan :entity-group} (:op %))) resolved-ops)

        ;; Entity groups from LEntityJoin are already built.
        ;; Standalone pattern-scan ops stay as-is (they came from standalone LScans).
        ;; We do NOT re-group them — the logical IR already decided the grouping.
        all-groups (into (vec entity-group-ops) pattern-ops)

        ;; Step 4b: Restore predicates consumed as pushdowns on merge-ops.
        ;; Merges use EAVT lookupGE, not AVET scans, so pushdowns can't be applied.
        ;; Collect them and re-emit as standalone predicate ops.
        merge-lost-pred-clauses (into #{} (comp (filter #(= :entity-group (:op %)))
                                                (mapcat :merge-lost-preds))
                                      entity-group-ops)
        restored-preds (when (seq merge-lost-pred-clauses)
                         (let [ci-by-clause (into {} (map (fn [ci] [(:clause ci) ci])) classified)]
                           (mapv (fn [pred-clause]
                                   (plan/plan-predicate-op (ci-by-clause pred-clause) db raw-pattern-ops))
                                 merge-lost-pred-clauses)))
        other-ops (if (seq restored-preds)
                    (into other-ops restored-preds)
                    other-ops)
        all-groups (mapv #(dissoc % :merge-lost-preds) all-groups)
        actual-consumed (if (seq merge-lost-pred-clauses)
                          (reduce disj actual-consumed merge-lost-pred-clauses)
                          actual-consumed)

        ;; ---------------------------------------------------------------
        ;; Step 4c: Attach single-group predicates to their entity group.
        ;; A predicate whose free-var args are a subset of exactly one group's
        ;; vars is evaluated during that group's execution, not as a post-filter.
        {:keys [all-groups other-ops]}
        (let [pred-ops   (filterv #(= :predicate (:op %)) other-ops)
              non-preds  (filterv #(not= :predicate (:op %)) other-ops)]
          (if (empty? pred-ops)
            {:all-groups all-groups :other-ops other-ops}
            (let [group-vars (mapv #(or (:output-vars %) (:vars %)) all-groups)]
              (reduce
               (fn [{:keys [all-groups other-ops]} pred]
                 (let [pred-free (into #{} (filter analyze/free-var?) (:args pred))
                       matching  (keep-indexed
                                  (fn [i gvars]
                                    (when (clojure.set/subset? pred-free gvars) i))
                                  group-vars)]
                   (if (and (= 1 (count matching))
                            (plan/post-op-direct-eligible? pred))
                     ;; Attach to the single matching group
                     (let [gi (first matching)]
                       {:all-groups (update all-groups gi
                                            update :attached-preds (fnil conj []) pred)
                        :other-ops other-ops})
                     ;; Multi-group or no-group: keep as standalone
                     {:all-groups all-groups
                      :other-ops (conj other-ops pred)})))
               {:all-groups all-groups :other-ops non-preds}
               pred-ops))))

        ;; ---------------------------------------------------------------
        ;; Step 5: Order everything (entity-groups + other ops + remaining NOTs)
        all-ops (into (into all-groups other-ops) not-ops)
        ordered-ops (plan/order-plan-ops all-ops)

        ;; ---------------------------------------------------------------
        ;; Step 6: Detect inter-group value joins
        group-joins (plan/detect-inter-group-joins
                     (filterv #(#{:entity-group :pattern-scan} (:op %)) ordered-ops))

        ;; ---------------------------------------------------------------
        ;; Step 7: NOT binding validation.
        ;; Walks the ordered ops in execution order, tracking which vars
        ;; are bound after each op runs. NOT/NOT-JOIN must have at least
        ;; one of its vars bound by a prior op (legacy semantics).
        ;;
        ;; The per-op contribution-set must mirror what the executor
        ;; will actually bind:
        ;;  - :entity-group → scan + merge vars
        ;;  - :pattern-scan → pattern vars
        ;;  - :function     → the result-binding var (`:binding` from
        ;;                    plan-function-op). Predicates produce no
        ;;                    new bindings; or/or-join handle their own
        ;;                    binding internally.
        ;; Earlier this case used `(:bind-vars op)` which plan-function-op
        ;; never sets — function ops looked like they bound nothing, so
        ;; any subsequent NOT/predicate whose only required var came from
        ;; a function chain (e.g. `format_type(...)` feeding NOT IN) was
        ;; falsely rejected with "Insufficient bindings".
        _ (loop [remaining ordered-ops
                 vars-so-far bound-vars]
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
                               :function (analyze/extract-vars (:binding op))
                               nil))))))]

    {:ops ordered-ops
     :consumed-preds actual-consumed
     :classified classified
     :group-joins group-joins
     :has-passthrough? (boolean (some #(= :passthrough (:op %)) ordered-ops))
     :structurally-fusable? (plan/structurally-fusable? ordered-ops)}))
