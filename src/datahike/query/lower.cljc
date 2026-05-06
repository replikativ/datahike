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
   [datahike.db.interface :as dbi]
   [datahike.index.interface :as di]
   [datahike.query.analyze :as analyze]
   [datahike.query.estimate :as estimate]
   [datahike.query.ir :as ir]
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
                     op (plan/plan-not-op db ci all-clause-vars rules join-vars?)]
                 (if join-vars?
                   (update acc :ops conj op)
                   (update acc :not-ops conj op)))

               ;; LUnion → OR/OR-JOIN op (delegate to plan.cljc)
               (instance? datahike.query.ir.LUnion node)
               (let [ci (union->classified node)
                     join-vars? (= :or-join (.-type ^datahike.query.ir.LUnion node))]
                 (update acc :ops conj (plan/plan-or-op db ci bvc rules join-vars?)))

               ;; LRuleCall → delegate to plan/plan-rule-op
               (instance? datahike.query.ir.LRuleCall node)
               (let [ci (rule-call->classified node)]
                 (update acc :ops conj (plan/plan-rule-op db ci bvc rules scc-info)))

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
                       :not (let [op (plan/plan-not-op db inner-ci all-clause-vars rules)]
                              (update acc :ops conj (assoc op :source source-sym)))
                       :not-join (let [op (plan/plan-not-op db inner-ci all-clause-vars rules true)]
                                   (update acc :ops conj (assoc op :source source-sym)))
                       :or (update acc :ops conj
                                   (assoc (plan/plan-or-op db inner-ci all-clause-vars rules) :source source-sym))
                       :or-join (update acc :ops conj
                                        (assoc (plan/plan-or-op db inner-ci all-clause-vars rules true) :source source-sym))
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
