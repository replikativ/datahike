(ns datahike.query.ir
  "IR record definitions for the query pipeline.

   Two layers:
   - Logical IR: algebraic, compositional query representation
     (LEntityJoin, LScan, LFilter, etc.). No physical decisions.
   - Pipeline DSL: typed step records (PPipeline, PIndexScan, etc.)
     that annotate entity-group ops with execution dispatch info.

   The logical IR is built by logical.cljc and lowered to physical
   plan maps by lower.cljc. Pipeline annotations are computed by
   plan.cljc/build-pipeline and consumed by execute.cljc.")

;; ============================================================================
;; Logical IR
;;
;; Represents the query as a compositional algebraic expression.
;; Each node captures WHAT to compute, not HOW.

(defrecord LScan
           [clause    ;; original pattern vector [?e :attr ?v ?tx]
            vars      ;; #{free variables}
            e a v tx  ;; individual pattern components
            source])  ;; optional source symbol ($2 etc.), nil for default

(defrecord LFilter
           [clause    ;; original predicate clause [[pred ?x ?y]]
            fn-sym    ;; predicate function symbol
            args      ;; argument list
            vars])    ;; #{free variables}

(defrecord LBind
           [clause    ;; original function clause [[fn ?x] ?result]
            fn-sym    ;; function symbol
            args      ;; argument list
            binding   ;; binding form
            vars])    ;; #{free variables}

(defrecord LEntityJoin
           [entity-var  ;; the shared entity variable (?e)
            scans       ;; vector of LScan (positive patterns on this entity)
            anti-scans  ;; vector of LScan (NOT patterns folded as anti-joins)
            vars        ;; union of all child vars
            source])    ;; optional source symbol

(defrecord LAntiJoin
           [clause      ;; original NOT/NOT-JOIN clause
            sub-nodes   ;; vector of logical IR nodes (sub-query)
            vars        ;; #{referenced variables}
            join-vars   ;; for NOT-JOIN: explicit join variable set; nil for NOT
            type])      ;; :not or :not-join

(defrecord LUnion
           [clause      ;; original OR/OR-JOIN clause
            branches    ;; vector of LogicalPlan (one per OR branch)
            vars        ;; #{referenced variables}
            join-vars   ;; for OR-JOIN: explicit join variable set; nil for OR
            type])      ;; :or or :or-join

(defrecord LFixpoint
           [clause              ;; original rule call clause
            rule-name           ;; rule name symbol
            call-args           ;; arguments to the rule call (may include constants)
            head-vars           ;; head variable symbols (free vars for recursion)
            scc-rule-names      ;; set of mutually recursive rule names
            scc-rule-plans      ;; {rule-name → {:head-vars, :base-plans, :rec-clause-versions}}
            base-plans          ;; vector of LogicalPlans (base cases)
            rec-clause-versions ;; vector of LogicalPlans (recursive delta-clause versions)
            base-scan-attr      ;; attribute from base case scan (for magic set optimization)
            vars])              ;; #{referenced variables}

(defrecord LRuleCall
           [clause      ;; original clause [rule-name ?arg1 ?arg2 ...]
            rule-name   ;; rule name symbol
            call-args   ;; argument vector
            vars])      ;; #{referenced variables}

(defrecord LRuleLookup
           [clause      ;; original clause
            rule-name   ;; rule name symbol
            call-args   ;; arguments
            mode        ;; :delta or :main (which accumulator to read from)
            vars])      ;; #{referenced variables}

(defrecord LExternalEngine
           [clause         ;; original clause
            fn-sym         ;; function symbol
            args           ;; arguments
            binding        ;; binding form
            vars           ;; #{referenced variables}
            mode           ;; :filter, :retrieval, or :solver
            idx-ident      ;; index identifier
            engine-meta    ;; engine metadata map
            estimated-card ;; cost model estimate
            accepts-entity-filter?]) ;; whether engine accepts entity filter

(defrecord LPassthrough
           [clause ;; original clause
            type   ;; clause type keyword (:unknown, etc.)
            vars]) ;; #{referenced variables}

(defrecord LogicalPlan
           [nodes          ;; vector of logical IR nodes (LEntityJoin, LFilter, LBind, etc.)
            bound-vars     ;; set of externally bound variables (from :in bindings)
            classified])   ;; vector of classified clause maps (for diagnostics/compatibility)

;; ============================================================================
;; Pipeline DSL — Compositional execution step specifications
;;
;; Each entity-group's execution is described as a sequence of typed steps.
;; Steps are specifications — they describe WHAT to execute. The executor
;; maps step sequences to fused implementations (existing path functions).
;;
;; Pipeline:  IndexScan → GroundFilter → ProbeFilter → [Merge] → Emit

(defrecord PIndexScan
           [index           ;; :eavt, :aevt, :avet
            clause          ;; pattern vector [?e :attr ?v ?tx]
            scan-attr-ground?]) ;; true when attribute position is ground (not a var)

(defrecord PGroundFilter
           [has-ground-filter?   ;; whether ground component filtering is needed
            has-pushdown?])      ;; whether pushdown predicate bounds exist

(defrecord PProbeFilter
           [probe-field])  ;; datom field index (0=entity, 2=value), or nil when no probe

(defrecord PSortedMerge
           [n-merges])     ;; number of merge attributes (all card-one, no anti)

(defrecord PPerCursorMerge
           [n-merges       ;; number of merge attributes
            has-anti?])    ;; whether any anti-merge (NOT) ops exist

(defrecord PCardManyMerge
           [n-merges])     ;; number of merge attributes (at least one card-many)

(defrecord PEmitTuple
           [n-find])       ;; number of find variables projected

(defrecord PCollect
           [datom-field     ;; datom field to collect from (0=entity, 2=value)
            merge-idx])     ;; merge index to collect from (-1 = scan)

(defrecord PPipeline
           [steps          ;; vector of step records (PIndexScan, PGroundFilter, etc.)
            fused-path     ;; keyword: :scan-only | :card-many-merge | :sorted-merge | :per-cursor-merge
            use-cursors?   ;; boolean — CLJ only, false on CLJS
            attr-refs?])   ;; boolean — attribute references mode
