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
       bounds))
   {:from-v nil :to-v nil}
   pushdown-preds))

;; ---------------------------------------------------------------------------
;; Pattern plan ops

(defn plan-pattern-op
  "Create a plan operation for a pattern clause.
   Returns [op actually-consumed-pred-clauses]."
  [db pattern-info schema-info pushdown-preds bound-vars]
  (let [{:keys [e a v tx pattern]} pattern-info
        ground? (fn [x] (and (some? x) (not (symbol? x))))
        e? (ground? e)
        a? (ground? a)
        v? (ground? v)
        has-avet? (boolean (:avet db))
        index (select-index pattern-info schema-info has-avet? (seq pushdown-preds))
        effective-preds (if (= :avet index) pushdown-preds [])
        base-est (or (estimate/estimate-pattern db pattern-info schema-info)
                     (di/-count (:eavt db)))
        ;; Build a partial scan-op for sampling context
        scan-ctx {:clause (:clause pattern-info) :index index}
        est (if (seq effective-preds)
              (estimate/estimate-pushdown-range base-est effective-preds db scan-ctx)
              base-est)
        e-bound? (and (analyze/free-var? e)
                      (contains? bound-vars e))
        join-method (cond
                      e-bound? :lookup
                      (empty? bound-vars) :scan
                      :else :hash)]
    [{:op :pattern-scan
      :clause pattern
      :index index
      :schema-info schema-info
      :pushdown-preds effective-preds
      :estimated-card est
      :join-method join-method
      :vars (:vars pattern-info)
      :e-ground (when e? e)
      :v-ground (when v? v)}
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
     :estimated-card (or (:estimated-card cost) 100)}))

(defn plan-function-op
  "Create a plan op for a function clause. Checks for external engine metadata."
  ([fn-info] (plan-function-op fn-info nil))
  ([fn-info db]
   (let [engine-meta (resolve-external-engine-meta (:fn-sym fn-info))]
     (if engine-meta
       (plan-external-engine-op fn-info engine-meta db)
       {:op :function
        :clause (:clause fn-info)
        :fn-sym (:fn-sym fn-info)
        :args (:args fn-info)
        :binding (:binding fn-info)
        :vars (:vars fn-info)
        :estimated-card nil}))))

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

(defn dp-order-fuse-ops
  "For N pattern ops on same entity var, find optimal (scan, merge-order).
   Uses short-circuit AND ordering: sort merges by ascending
   merge-cost / (1 - pass-rate). Try each pattern as scan, pick minimum total cost."
  [db pattern-ops total-entities]
  (let [n (count pattern-ops)
        estimates (mapv (fn [op]
                          (max 1 (or (:estimated-card op) total-entities)))
                        pattern-ops)]
    (if (<= n 2)
      (let [;; Patterns with variable attribute can only be scan, not merge
            mergeable (filterv can-be-merge? pattern-ops)
            scan-only (filterv #(not (can-be-merge? %)) pattern-ops)]
        (if (seq scan-only)
          ;; Force scan-only pattern as scan, rest as merges
          {:scan (first scan-only) :merges (vec (concat (rest scan-only) mergeable))}
          ;; All can be merges — pick lowest cardinality as scan
          (let [sorted (sort-by :estimated-card pattern-ops)]
            {:scan (first sorted) :merges (vec (rest sorted))})))
      (let [candidates
            (for [si (range n)
                  ;; Only consider scan positions where all remaining ops can be merges
                  :when (every? can-be-merge?
                                (keep-indexed (fn [i op] (when (not= i si) op)) pattern-ops))]
              (let [scan-op (nth pattern-ops si)
                    scan-card (double (nth estimates si))
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
          ;; just pick lowest cardinality as scan
          (let [sorted (sort-by :estimated-card pattern-ops)]
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
                                      (not has-anti?))
                            :cljs false)
        attr-refs? (:attribute-refs? (dbi/-config db))
        fused-path (cond
                     (zero? n-merges) :scan-only
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

(defn- op-input-vars
  "Return only the *input* vars of an op — vars it consumes, not vars it produces.
   For :function ops, output vars (bound by the function) are excluded.
   For all other ops, :vars is already the set of input vars."
  [op]
  (if (= :function (:op op))
    ;; Input vars are the free variables in :args
    (into #{} (filter analyze/free-var?) (:args op))
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
                               (if (:anti? merge-op)
                                 (long (* card (max 0.01 (- 1.0 pass-rate))))
                                 (long (* card pass-rate)))))
                           scan-card
                           merge-ops)
        output-vars (into #{} (mapcat :vars) (into pattern-ops anti-ops))
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
                       :vars output-vars
                       :estimated-card (max 1 group-card)
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

(def ^:private max-cost #?(:clj Long/MAX_VALUE :cljs (.-MAX_SAFE_INTEGER js/Number)))

(defn op-cost [op bound-vars]
  (case (:op op)
    (:entity-group :pattern-scan)
    (group-effective-card op)

    :predicate
    (if (every? #(contains? bound-vars %) (:vars op)) 0 max-cost)

    :function
    (let [input-vars (into #{} (filter analyze/free-var?) (:args op))]
      (if (every? #(contains? bound-vars %) input-vars) 1 max-cost))

    :external-engine
    (if (= :solver (:mode op))
      ;; Solver mode needs populated context — require at least some of its
      ;; vars to be bound first (shared vars come from entity-group patterns)
      (let [op-vars (into #{} (filter analyze/free-var?) (:vars op))]
        (if (some #(contains? bound-vars %) op-vars)
          (or (:estimated-card op) 100)
          max-cost))
      ;; Filter/retrieval: check args for free var deps
      (let [input-vars (into #{} (filter analyze/free-var?) (:args op))]
        (if (every? #(contains? bound-vars %) input-vars)
          (or (:estimated-card op) 100)
          max-cost)))

    :rule-lookup
    (or (:estimated-card op) 100)

    (:or :or-join :not :not-join :recursive-rule)
    (let [required (or (:join-vars op) (:vars op))]
      (if (every? #(contains? bound-vars %) required)
        (or (:estimated-card op) 100)
        max-cost))

    max-cost))

(defn order-plan-ops
  "Order plan operations using DP for entity-group ordering and greedy
   interleaving for dependency-constrained ops (predicates, functions, etc.).
   The DP phase finds the optimal group execution order by minimizing
   total intermediate cardinality. The greedy phase then inserts non-group
   ops at the earliest point where their dependencies are satisfied."
  [ops]
  (let [groups (filterv #(#{:entity-group :pattern-scan} (:op %)) ops)
        non-groups (filterv #(not (#{:entity-group :pattern-scan} (:op %))) ops)]
    (if (empty? groups)
      ;; No groups — pure greedy on non-group ops
      (loop [remaining (set ops)
             bound-vars #{}
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
                 bound-vars #{}
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
                             (conj result chosen-op)))))))))))))


;; ---------------------------------------------------------------------------
;; OR / NOT / Rule plan ops

(defn- normalize-and-plan-branches
  "Normalize branch clauses and create sub-plans for each branch.
   Used by both OR and OR-JOIN planning."
  [db branches bound-vars rules]
  (mapv (fn [branch]
          (let [branch-clauses (if (and (sequential? branch)
                                        (not (sequential? (first branch))))
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
        ;; Resolve keyword attrs to entity refs in attribute-refs mode
        attr-refs? (:attribute-refs? (dbi/-config db))
        resolve-clause (fn [clause]
                         (if (and attr-refs?
                                  (vector? clause)
                                  (not (sequential? (first clause)))
                                  (keyword? (second clause)))
                           (assoc clause 1 (dbi/-ref-for db (second clause)))
                           clause))
        renamed (mapv (fn [c]
                        (resolve-clause
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
                          ;; Head vars are bound within rule branches (from call args / accumulator)
                                   branch-bound (into bound-vars (filter analyze/free-var?) free-call-args)
                                   base-ps (mapv (fn [b]
                                                   (let [renamed (rename-branch-vars b free-call-args seqid db)]
                                                     (create-plan db (vec renamed) branch-bound rules)))
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
                                                      (create-plan db versioned branch-bound rules scc-rule-names)))
                                                  scc-indices)))
                                         rec-bs))]
                               [rn {:head-vars free-call-args
                                    :base-plans base-ps
                                    :rec-clause-versions rec-cvs}])))
                      scc-rule-names)
                ;; Check if any base case lacks data patterns (e.g. [(identity ?a) ?c]).
                ;; Such base cases can't be executed by the planned fixpoint engine
                ;; because head vars have no data source — fall back to legacy.
                has-scanless-base?
                (some (fn [[_ {:keys [base-plans]}]]
                        (some (fn [bp]
                                (not (some #(#{:entity-group :pattern-scan} (:op %))
                                           (:ops bp))))
                              base-plans))
                      scc-rule-plans)
                ;; If scanless base cases, nil out scc-rule-plans to trigger legacy fallback
                scc-rule-plans (when-not has-scanless-base? scc-rule-plans)
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
                ;; Call args that are free vars are bound within the rule body
                rule-bound (into bound-vars (filter analyze/free-var?) call-args)
                sub-plans (mapv #(create-plan db (vec %) rule-bound rules) expanded)
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
         all-clause-vars (into bound-vars
                               (mapcat analyze/extract-vars)
                               where-clauses)

         ;; Pre-compute SCC info for rules (once per plan, not per rule-op)
         scc-info (when rules (compute-rule-sccs rules))

         ;; Step 2: Detect pushdown candidates
         {:keys [pushdowns consumed]} (analyze/detect-pushdown classified bound-vars)

         ;; Step 3: Build raw ops — track consumed pushdown preds
         {:keys [ops actual-consumed not-ops]}
         (reduce
          (fn [acc ci]
            ;; Check for rule calls (classified as :pattern but first elem is rule name)
            (let [is-rule-call? (and rules
                                     (= :pattern (:type ci))
                                     (symbol? (:e ci))
                                     (not (analyze/free-var? (:e ci)))
                                     (contains? rules (:e ci)))
                  ;; Check for metadata-tagged rule-lookup (from clause version generation)
                  lookup-mode (when is-rule-call?
                                (:rule-lookup-mode (meta (:clause ci))))]
              (if (and is-rule-call? guarded-rules
                       (contains? guarded-rules (:e ci)))
                ;; Inside a recursive branch plan — emit :rule-lookup
                (let [rule-ci (assoc ci :type :rule-call
                                     :vars (into #{} (filter analyze/free-var?) (rest (:clause ci))))
                      mode (or lookup-mode :main)]
                  (update acc :ops conj (plan-rule-lookup-op rule-ci mode)))
                (if is-rule-call?
                  (let [rule-ci (assoc ci :type :rule-call
                                       :vars (into #{} (filter analyze/free-var?) (rest (:clause ci))))]
                    (update acc :ops conj (plan-rule-op db rule-ci all-clause-vars rules scc-info)))

                  (case (:type ci)
                    :pattern
                    (let [schema-info (analyze/pattern-schema-info db ci)
                          preds (get pushdowns (:clause ci) [])
                          [op consumed-preds] (plan-pattern-op db ci schema-info preds bound-vars)]
                      (-> acc
                          (update :ops conj op)
                          (update :actual-consumed into (or consumed-preds #{}))))

                    :function (update acc :ops conj (plan-function-op ci db))
                    :predicate (update acc :ops conj [:maybe-pred ci])
                    :or (update acc :ops conj (plan-or-op db ci all-clause-vars rules))
                    :or-join (update acc :ops conj (plan-or-op db ci all-clause-vars rules true))
                    ;; Collect NOT ops separately for anti-merge folding
                    :not (update acc :not-ops conj (plan-not-op db ci all-clause-vars rules))
                    :not-join (update acc :ops conj (plan-not-op db ci all-clause-vars rules true))
                    :and (let [sub-plan (create-plan db (vec (:sub-clauses ci)) all-clause-vars rules)]
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
                              [op _] (plan-pattern-op db inner-ci schema-info [] bound-vars)]
                          (update acc :ops conj (assoc op :source source-sym)))
                        :not
                        (let [op (plan-not-op db inner-ci all-clause-vars rules)]
                          (update acc :ops conj (assoc op :source source-sym)))
                        :not-join
                        (let [op (plan-not-op db inner-ci all-clause-vars rules true)]
                          (update acc :ops conj (assoc op :source source-sym)))
                        :or
                        (update acc :ops conj (assoc (plan-or-op db inner-ci all-clause-vars rules) :source source-sym))
                        :or-join
                        (update acc :ops conj (assoc (plan-or-op db inner-ci all-clause-vars rules true) :source source-sym))
                        ;; Source-prefix wrapping another source-prefix or unknown
                        (update acc :ops conj (plan-passthrough-op ci))))

                    (update acc :ops conj (plan-passthrough-op ci)))))))
          {:ops [] :actual-consumed #{} :not-ops []}
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

         ;; Step 4: Order everything together
         all-ops (into (into (vec entity-groups) other-ops) remaining-nots)
         ordered-ops (order-plan-ops all-ops)

         ;; Step 5: Detect inter-group value joins
         group-joins (detect-inter-group-joins
                      (filterv #(#{:entity-group :pattern-scan} (:op %)) ordered-ops))

         ;; Step 6: Validate NOT/NOT-JOIN bindings post-ordering.
         ;; Walk the ordered ops and accumulate bound vars. Each NOT must have
         ;; at least one var bound by ops that PRECEDE it in the execution order.
         ;; This correctly handles reordering: (not [?e ...]) [?e :name] gets
         ;; reordered to [?e :name] (not [?e ...]), and ?e is bound before NOT.
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
                                :function (into #{} (filter analyze/free-var?) (:bind-vars op))
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
        re-ordered (order-plan-ops re-estimated)]
    (assoc plan :ops (into (vec executed-ops) re-ordered))))
