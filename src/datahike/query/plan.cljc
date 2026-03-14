(ns datahike.query.plan
  "Query plan construction for the compiled query engine.
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
   [taoensso.timbre :as log]
   [datahike.query.analyze :as analyze]
   [datahike.query.estimate :as estimate]))

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

(defn pushdown-to-bounds
  "Convert pushdown predicates into tighter datom bounds for index slice.
   Returns {:from-v val-or-nil, :to-v val-or-nil, :strict-preds [...]}.
   Strict inequalities (> <) use inclusive bounds for the slice, and the
   original predicates are kept in :strict-preds for post-filtering."
  [pushdown-preds]
  (reduce
   (fn [bounds {:keys [op const-val] :as pred}]
     (case op
       >  (-> (update bounds :from-v
                      (fn [cur] (if cur (max cur const-val) const-val)))
              (update :strict-preds (fnil conj []) pred))
       >= (update bounds :from-v
                  (fn [cur] (if cur (max cur const-val) const-val)))
       <  (-> (update bounds :to-v
                      (fn [cur] (if cur (min cur const-val) const-val)))
              (update :strict-preds (fnil conj []) pred))
       <= (update bounds :to-v
                  (fn [cur] (if cur (min cur const-val) const-val)))
       =  (-> bounds
              (assoc :from-v const-val)
              (assoc :to-v const-val))
       bounds))
   {:from-v nil :to-v nil}
   pushdown-preds))

;; ---------------------------------------------------------------------------
;; Pattern plan ops

(defn- plan-pattern-op
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
      :vars (:vars pattern-info)}
     (when (seq effective-preds)
       (into #{} (map :pred-clause) effective-preds))]))

(defn- estimate-standalone-predicate
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

(defn- plan-predicate-op
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
                (log/debug "external-engine meta resolution failed for" fn-sym ":" (.getMessage e))
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
                      (log/warn "external-engine cost-model failed:" #?(:clj (.getMessage e) :cljs (.-message e)))
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

(defn- plan-function-op
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

(defn- plan-passthrough-op [clause-info]
  {:op :passthrough
   :clause (:clause clause-info)
   :type (:type clause-info)
   :vars (:vars clause-info)
   :estimated-card nil})

;; ---------------------------------------------------------------------------
;; DP-based optimal merge ordering

(defn- dp-order-fuse-ops
  "For N pattern ops on same entity var, find optimal (scan, merge-order).
   Uses short-circuit AND ordering: sort merges by ascending
   merge-cost / (1 - pass-rate). Try each pattern as scan, pick minimum total cost."
  [db pattern-ops total-entities]
  (let [n (count pattern-ops)
        estimates (mapv (fn [op]
                          (max 1 (or (:estimated-card op) total-entities)))
                        pattern-ops)]
    (if (<= n 2)
      (let [sorted (sort-by :estimated-card pattern-ops)]
        {:scan (first sorted) :merges (vec (rest sorted))})
      (let [candidates
            (for [si (range n)]
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
        (apply min-key :cost candidates)))))

;; ---------------------------------------------------------------------------
;; Entity group construction
;;
;; An entity group bundles all pattern ops on the same entity variable into
;; a single plan node: one scan + zero or more merge lookups.
;; This enables fused execution (single pass over scan datoms, lookupGE per merge).

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
               (first ops)
               ;; Multi-op group — build entity-group with DP ordering
               (let [{:keys [scan merges]}
                     (dp-order-fuse-ops db
                                        ;; Only non-anti ops participate in scan selection
                                        (filterv #(not (:anti? %)) all-ops)
                                        total-entities)
                     ;; Merge ops = DP-ordered merges + anti-merges
                     ;; Sort anti-merges into the merge order by their filtering power:
                     ;; anti-merge with low pass-rate (strong filter) should go early
                     all-merges (into (vec merges) anti-ops)
                     merge-ops (let [normal (filterv #(not (:anti? %)) all-merges)
                                     anti (filterv :anti? all-merges)]
                                 (if (empty? anti)
                                   normal
                                   ;; Append anti-merges sorted by selectivity (most selective first)
                                   ;; Anti pass-rate = 1 - P(match), lower = more selective
                                   (let [anti-with-pass (mapv (fn [op]
                                                                (let [est (max 1 (or (:estimated-card op) total-entities))
                                                                      match-rate (estimate/estimate-conditional-pass-rate est total-entities)]
                                                                  (assoc op ::anti-pass-rate (- 1.0 match-rate))))
                                                              anti)
                                         ;; Sort anti-merges by pass-rate ascending (most selective first)
                                         sorted-anti (sort-by ::anti-pass-rate anti-with-pass)
                                         ;; Place anti-merges after normal merges, sorted by selectivity
                                         result (into (vec normal) (mapv #(dissoc % ::anti-pass-rate) sorted-anti))]
                                     result)))
                     ;; Estimate output cardinality
                     scan-card (max 1 (or (:estimated-card scan) total-entities))
                     group-card (reduce (fn [card merge-op]
                                          (let [merge-est (max 1 (or (:estimated-card merge-op) total-entities))
                                                pass-rate (estimate/estimate-conditional-pass-rate merge-est total-entities)]
                                            (if (:anti? merge-op)
                                              ;; Anti-merge: pass = 1 - P(match)
                                              (long (* card (max 0.01 (- 1.0 pass-rate))))
                                              (long (* card pass-rate)))))
                                        scan-card
                                        merge-ops)
                     output-vars (into #{} (mapcat :vars) all-ops)]
                 (cond-> {:op :entity-group
                         :entity-var e-var
                         :scan-op (assoc scan :join-method :scan)
                         :merge-ops (mapv #(assoc % :join-method :lookup) merge-ops)
                         :output-vars output-vars
                         :vars output-vars
                         :estimated-card (max 1 group-card)}
                  source (assoc :source source))))))
         groups)

        ;; NOT ops that weren't folded
        remaining-nots (filterv #(not (contains? folded-not-clauses (:clause %))) not-ops)]

    {:entity-groups entity-groups
     :remaining-nots remaining-nots}))

;; ---------------------------------------------------------------------------
;; Inter-group join detection

(defn- detect-inter-group-joins
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
;; Greedy ordering for mixed ops (entity-groups + predicates + functions + OR/NOT)

(def ^:private max-cost #?(:clj Long/MAX_VALUE :cljs (.-MAX_SAFE_INTEGER js/Number)))

(defn- op-cost [op bound-vars]
  (case (:op op)
    (:entity-group :pattern-scan)
    (or (:estimated-card op) max-cost)

    :predicate
    (if (every? #(contains? bound-vars %) (:vars op)) 0 max-cost)

    :function
    (let [input-vars (into #{} (filter analyze/free-var?) (:args op))]
      (if (every? #(contains? bound-vars %) input-vars) 1 max-cost))

    :external-engine
    (let [input-vars (into #{} (filter analyze/free-var?) (:args op))]
      (if (every? #(contains? bound-vars %) input-vars)
        ;; Filter/retrieval with no input deps → place early (low cost)
        ;; Solver with input deps → after deps are bound
        (or (:estimated-card op) 100)
        max-cost))

    :rule-lookup
    (or (:estimated-card op) 100)

    (:or :or-join :not :not-join :recursive-rule)
    (let [required (or (:join-vars op) (:vars op))]
      (if (every? #(contains? bound-vars %) required)
        (or (:estimated-card op) 100)
        max-cost))

    max-cost))

(defn- order-plan-ops
  "Order plan operations (entity-groups, predicates, OR/NOT, etc.)
   greedily by ascending estimated cost. Predicates placed immediately
   after their dependencies are satisfied."
  [ops]
  (loop [remaining (set ops)
         bound-vars #{}
         ordered []]
    (if (empty? remaining)
      ordered
      (let [scored (map (fn [op] [op (op-cost op bound-vars)]) remaining)
            executable (filter #(< (second %) max-cost) scored)
            best (if (seq executable)
                   (first (sort-by second executable))
                   (first (sort-by second scored)))
            [chosen-op _] best
            new-vars (into bound-vars (:vars chosen-op))]
        (recur (disj remaining chosen-op)
               new-vars
               (conj ordered chosen-op))))))

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

(defn- plan-or-op [db clause-info bound-vars rules]
  (let [sub-plans (normalize-and-plan-branches db (:branches clause-info) bound-vars rules)
        total-est (reduce + 0 (keep (fn [p] (some :estimated-card (:ops p))) sub-plans))]
    {:op :or
     :clause (:clause clause-info)
     :branches sub-plans
     :vars (:vars clause-info)
     :estimated-card (max 1 total-est)}))

(defn- plan-or-join-op [db clause-info bound-vars rules]
  (let [raw-join-vars (:join-vars clause-info)
        _ (when (some sequential? raw-join-vars)
            (throw (ex-info (str "Insufficient bindings: "
                                 (into #{} (mapcat analyze/extract-vars) raw-join-vars)
                                 " not bound in " (:clause clause-info))
                            {:error :query/where
                             :form (:clause clause-info)})))
        join-vars (set raw-join-vars)
        sub-plans (normalize-and-plan-branches db (:branches clause-info) bound-vars rules)
        total-est (reduce + 0 (keep (fn [p] (some :estimated-card (:ops p))) sub-plans))]
    {:op :or-join
     :clause (:clause clause-info)
     :join-vars join-vars
     :branches sub-plans
     :vars (:vars clause-info)
     :estimated-card (max 1 total-est)}))

(defn- plan-not-op [db clause-info bound-vars rules]
  (let [sub-plan (create-plan db (vec (:sub-clauses clause-info)) bound-vars rules)]
    {:op :not
     :clause (:clause clause-info)
     :sub-plan sub-plan
     :vars (:vars clause-info)
     :estimated-card nil}))

(defn- plan-not-join-op [db clause-info bound-vars rules]
  (let [join-vars (set (:join-vars clause-info))
        ;; Sub-plan starts with only join-vars bound — inner NOT validation
        ;; needs accurate binding tracking, not all-clause-vars from outer scope
        sub-plan (create-plan db (vec (:sub-clauses clause-info)) join-vars rules)]
    {:op :not-join
     :clause (:clause clause-info)
     :join-vars join-vars
     :sub-plan sub-plan
     :vars (:vars clause-info)
     :estimated-card nil}))

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

(defn- compute-rule-sccs
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

(defn- plan-rule-op [db clause-info bound-vars rules scc-info]
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
          ;; Recursive rule — pre-compile branch plans with clause versions.
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
                ;; Such base cases can't be executed by the compiled fixpoint engine
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

(defn- plan-rule-lookup-op
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
                    :or-join (update acc :ops conj (plan-or-join-op db ci all-clause-vars rules))
                    ;; Collect NOT ops separately for anti-merge folding
                    :not (update acc :not-ops conj (plan-not-op db ci all-clause-vars rules))
                    :not-join (update acc :ops conj (plan-not-join-op db ci all-clause-vars rules))
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
                        (let [op (plan-not-join-op db inner-ci all-clause-vars rules)]
                          (update acc :ops conj (assoc op :source source-sym)))
                        :or
                        (update acc :ops conj (assoc (plan-or-op db inner-ci all-clause-vars rules) :source source-sym))
                        :or-join
                        (update acc :ops conj (assoc (plan-or-join-op db inner-ci all-clause-vars rules) :source source-sym))
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
      :consumed-preds actual-consumed
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
