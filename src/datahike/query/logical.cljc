(ns datahike.query.logical
  "Logical IR builder.

   Transforms where-clauses into a compositional LogicalPlan:
   - Classifies clauses (via analyze.cljc)
   - Groups patterns by entity variable into LEntityJoin nodes
   - Folds simple NOT clauses into entity groups as anti-scans
   - Preserves predicates, functions, OR, rules as separate IR nodes

   No physical decisions (index selection, merge ordering, pushdown)
   are made here — those belong in the lowering pass (lower.cljc)."
  (:require
   [datahike.query.analyze :as analyze]
   [datahike.query.ir :as ir]))

#?(:clj (set! *warn-on-reflection* true))

;; ---------------------------------------------------------------------------
;; Helpers

(defn- make-scan
  "Build an LScan from a classified pattern clause."
  [ci source]
  (ir/->LScan (:clause ci) (:vars ci)
              (:e ci) (:a ci) (:v ci) (:tx ci)
              source))

(defn- rule-call?
  "Is this classified clause a rule call? A rule call looks like a pattern
   but its entity position is a rule name symbol (not a ?var, not blank)."
  [ci rules]
  (and rules
       (= :pattern (:type ci))
       (symbol? (:e ci))
       (not (analyze/free-var? (:e ci)))
       (contains? rules (:e ci))))

(defn- blank-var?
  "Blank/anonymous Datalog variable — a symbol whose name starts with `_`.
   Each occurrence should get its own group so patterns don't accidentally
   share an entity."
  [x]
  (and (symbol? x)
       #?(:clj  (.startsWith (name x) "_")
          :cljs (= "_" (subs (name x) 0 1)))))

(defn- entity-group-key
  "Grouping key for scans: [entity-var source]. Blank/anonymous vars
   each get a unique group. Works with both LScan and LOptionalScan
   since both expose :e and :source via keyword access."
  [scan blank-counter]
  (let [e-var  (:e scan)
        source (:source scan)]
    (if (blank-var? e-var)
      [(symbol (str "_anon_" (swap! blank-counter inc))) source]
      [e-var source])))

;; ---------------------------------------------------------------------------
;; NOT → anti-scan folding

(defn- foldable-not?
  "A NOT clause can fold into an entity group's anti-scans when:
   1. It is a plain NOT (not NOT-JOIN)
   2. It has exactly one sub-clause that is a data pattern
   3. The pattern's entity var has a scan group
   4. No source prefix (same default source)"
  [not-entry scan-groups]
  (let [{:keys [ci type source]} not-entry]
    (and (= type :not)
         (nil? source)
         (= 1 (count (:sub-clauses ci)))
         (let [sub-ci (analyze/classify-clause (first (:sub-clauses ci)))]
           (and (= :pattern (:type sub-ci))
                (analyze/free-var? (:e sub-ci))
                (contains? scan-groups [(:e sub-ci) nil]))))))

;; ---------------------------------------------------------------------------
;; Main builder

(declare build-logical-plan)

(defn build-logical-plan
  "Build a LogicalPlan from where-clauses.

   The plan contains IR nodes (LEntityJoin, LFilter, LBind, etc.)
   in no particular order — ordering is a physical decision for lower.cljc.

   Parameters:
   - db: database (for schema introspection in rule expansion)
   - where-clauses: vector of Datalog where clauses
   - bound-vars: set of externally bound variables (:in bindings)
   - rules: parsed rules map {rule-name → [branches...]} or nil
   - guarded-rules: set of rule names in current SCC (for recursive branches)"
  ([db where-clauses bound-vars rules]
   (build-logical-plan db where-clauses bound-vars rules nil))
  ([db where-clauses bound-vars rules guarded-rules]
   (let [classified (mapv analyze/classify-clause where-clauses)
         all-clause-vars (into bound-vars
                               (mapcat analyze/extract-vars)
                               where-clauses)

         ;; Walk classified clauses → accumulate IR node collections
         {:keys [scans filters binds nots or-nodes rule-nodes passthrough-nodes and-nodes]}
         (reduce
          (fn [acc ci]
            (let [is-rule? (rule-call? ci rules)
                  lookup-mode (when is-rule?
                                (:rule-lookup-mode (meta (:clause ci))))]
              (cond
                ;; Guarded rule call (inside recursive branch plan)
                (and is-rule? guarded-rules
                     (contains? guarded-rules (:e ci)))
                (let [rule-vars (into #{} (filter analyze/free-var?) (rest (:clause ci)))
                      mode (or lookup-mode :main)]
                  (update acc :rule-nodes conj
                          (ir/->LRuleLookup (:clause ci) (:e ci)
                                            (vec (rest (:clause ci))) mode rule-vars)))

                ;; Normal rule call — opaque node for lowering to expand
                is-rule?
                (let [rule-vars (into #{} (filter analyze/free-var?) (rest (:clause ci)))]
                  (update acc :rule-nodes conj
                          (ir/->LRuleCall (:clause ci) (:e ci)
                                          (vec (rest (:clause ci))) rule-vars)))

                :else
                (case (:type ci)
                  ;; Data pattern → LScan
                  :pattern
                  (update acc :scans conj (make-scan ci nil))

                  ;; Predicate → LFilter
                  :predicate
                  (update acc :filters conj
                          (ir/->LFilter (:clause ci) (:fn-sym ci) (:args ci) (:vars ci)))

                  ;; Function binding → LBind (or LOptionalScan for get-else)
                  :function
                  (if (and (= 'get-else (:fn-sym ci))
                           ;; Preserve legacy `get-else` semantics: a nil
                           ;; default is rejected at runtime with
                           ;; "get-else: nil default value is not supported".
                           ;; Fall through to LBind so that check still fires.
                           (some? (nth (:args ci) 3 nil)))
                    ;; Recognize get-else as an optional scan:
                    ;; [(get-else $ ?e :attr default) ?v]
                    ;; args = ($ ?e :attr default), binding = ?v
                    (let [args (:args ci)
                          e-var (second args)
                          attr (nth args 2)
                          default-val (nth args 3)
                          bind-var (:binding ci)
                          scan-vars (cond-> #{bind-var}
                                     (and (symbol? e-var)
                                          (analyze/free-var? e-var))
                                     (conj e-var))]
                      (update acc :scans conj
                              (ir/->LOptionalScan
                               [e-var attr bind-var]  ;; synthetic clause
                               scan-vars
                               e-var attr bind-var nil ;; e a v tx
                               nil                     ;; source
                               default-val             ;; default value
                               bind-var)))             ;; binding var
                    ;; Regular function → LBind
                    (update acc :binds conj
                            (ir/->LBind (:clause ci) (:fn-sym ci) (:args ci)
                                        (:binding ci) (:vars ci))))

                  ;; NOT → collect for anti-merge folding
                  :not
                  (update acc :nots conj {:ci ci :type :not :source nil})

                  ;; NOT-JOIN → always stays separate (has explicit join vars)
                  :not-join
                  (update acc :nots conj {:ci ci :type :not-join :source nil})

                  ;; OR → LUnion (branches built by lowering)
                  :or
                  (update acc :or-nodes conj
                          (ir/->LUnion (:clause ci) nil (:vars ci) nil :or))

                  ;; OR-JOIN → LUnion with join vars
                  :or-join
                  (update acc :or-nodes conj
                          (ir/->LUnion (:clause ci) nil (:vars ci)
                                       (set (:join-vars ci)) :or-join))

                  ;; AND → flatten: build sub-plan (which does its own entity
                  ;; grouping) and merge resulting nodes into parent as-is
                  :and
                  (let [sub-plan (build-logical-plan db (vec (:sub-clauses ci))
                                                     all-clause-vars rules guarded-rules)]
                    (update acc :and-nodes into
                            (.-nodes ^datahike.query.ir.LogicalPlan sub-plan)))

                  ;; Source-prefixed clause → unwrap, tag with source
                  :source-prefix
                  (let [source-sym (:source-sym ci)
                        inner (:inner-clause ci)
                        inner-ci (analyze/classify-clause inner)]
                    (case (:type inner-ci)
                      :pattern
                      (update acc :scans conj (make-scan inner-ci source-sym))

                      ;; Source-prefixed NOT/OR → LPassthrough (lowering delegates)
                      (:not :not-join :or :or-join)
                      (update acc :passthrough-nodes conj
                              (ir/->LPassthrough (:clause ci) :source-prefix (:vars ci)))

                      ;; Other source-prefix → passthrough
                      (update acc :passthrough-nodes conj
                              (ir/->LPassthrough (:clause ci) :source-prefix (:vars ci)))))

                  ;; Unknown → passthrough
                  (update acc :passthrough-nodes conj
                          (ir/->LPassthrough (:clause ci) (:type ci) (:vars ci)))))))
          {:scans [] :filters [] :binds [] :nots [] :or-nodes []
           :rule-nodes [] :passthrough-nodes [] :and-nodes []}
          classified)

         ;; ---------------------------------------------------------------
         ;; Group scans by entity variable
         blank-counter (atom 0)
         scan-groups (group-by #(entity-group-key % blank-counter) scans)

         ;; Identify foldable NOTs (single-pattern NOT on a grouped entity var)
         foldable-nots
         (reduce
          (fn [acc not-entry]
            (if (and (nil? (:ir-node not-entry))  ;; not already an IR node from AND flattening
                     (foldable-not? not-entry scan-groups))
              (let [sub-ci (analyze/classify-clause (first (:sub-clauses (:ci not-entry))))
                    anti-scan (make-scan sub-ci nil)]
                (update acc [(:e sub-ci) nil] (fnil conj [])
                        {:anti-scan anti-scan :not-entry not-entry}))
              acc))
          {}
          nots)

         folded-not-clauses (into #{}
                                  (mapcat (fn [[_ entries]]
                                            (map (comp :clause :ci :not-entry) entries)))
                                  foldable-nots)

         ;; Build entity join nodes from scan groups
         entity-nodes
         (mapv
          (fn [[[e-var source] group-scans]]
            (let [anti-entries (get foldable-nots [e-var source])
                  anti-scans (mapv :anti-scan anti-entries)
                  all-vars (into (into #{} (mapcat :vars) group-scans)
                                 (mapcat :vars) anti-scans)]
              (if (and (= 1 (count group-scans)) (empty? anti-scans))
                ;; Single scan, no anti-merges → standalone LScan
                (first group-scans)
                ;; Multi-scan group → LEntityJoin
                (ir/->LEntityJoin e-var (vec group-scans) anti-scans all-vars source))))
          scan-groups)

         ;; Remaining NOTs (not folded into entity groups)
         remaining-nots
         (into []
               (comp
                (remove #(contains? folded-not-clauses (:clause (:ci %))))
                (map (fn [not-entry]
                       (if-let [ir-node (:ir-node not-entry)]
                         ;; Already an IR node from AND flattening
                         ir-node
                         ;; Build LAntiJoin from clause-info
                         (let [{:keys [ci type]} not-entry]
                           (ir/->LAntiJoin (:clause ci) (:sub-clauses ci) (:vars ci)
                                           (when (= type :not-join) (set (:join-vars ci)))
                                           type))))))
               nots)

         ;; Assemble all nodes (UNORDERED — ordering is physical)
         all-nodes (-> (vec entity-nodes)
                       (into filters)
                       (into binds)
                       (into remaining-nots)
                       (into or-nodes)
                       (into rule-nodes)
                       (into passthrough-nodes)
                       (into and-nodes))]

     (ir/->LogicalPlan all-nodes bound-vars classified))))
