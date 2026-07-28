(ns datahike.query.eqcheck
  "Plan-level EQUALITY-OBLIGATION invariant checker (dev/test only).

   Datalog semantics: every occurrence of a variable denotes the SAME value.
   The fused engine models a plan as a pipeline of producers, so each variable
   occurrence is a slot to write into and the join semantics is re-derived at
   each execution site. This namespace states the invariant once, over the
   PLAN ALONE — no query execution, no oracle:

       implied-equalities(plan) == enforced-equalities(plan)

   LEFT side  — variable co-occurrence: for each variable, the first occurrence
                (in execution order) is the PRODUCER; every later occurrence is
                a CONSUMER and implies one equality obligation against the
                producer's column.
   RIGHT side — the obligations the plan's ops actually enforce, modelled from
                the executor's real mechanisms (see `enforced-by`).

   Both inclusions matter:
     implied \\ enforced  → UNDER-constraining (silent wrong answers: extra rows)
     enforced \\ implied  → OVER-constraining  (silent wrong answers: lost rows),
                            here mostly in the form of PHANTOM COLUMNS: a var an
                            op advertises but never materialises, so downstream
                            joins run against an all-nil column.

   Usage:
     (binding [*check-equalities?* true] (d/q …))            ;; assert
     (eqcheck/check-plan plan)                                ;; inspect report

   This namespace is deliberately read-only w.r.t. the plan and has no
   dependency on execute.cljc, so it can be called from `lower` (lower →
   eqcheck → plan; plan never requires eqcheck)."
  (:require
   [clojure.set]
   [datahike.query.analyze :as analyze]
   [datahike.query.plan :as plan]))

;; ---------------------------------------------------------------------------
;; Flags

(def ^:dynamic *check-equalities?*
  "When true, `maybe-check!` validates every plan built. Dev/test only —
   mirrors the `*profile?*` precedent in datahike.query."
  false)

(def ^:dynamic *violation-handler*
  "Called with the report map when a checked plan violates the invariant.
   nil → throw. Set to a collector fn to MEASURE instead of failing."
  nil)

(def ^:dynamic *tolerate-unannotated?*
  "Incremental-adoption switch. When true, ops whose obligation model is not
   yet written (`:passthrough`, unknown op keys) are reported as :unmodelled
   instead of counted as violations."
  true)

;; ---------------------------------------------------------------------------
;; Occurrence sites

(def ^:private group-ops #{:entity-group :pattern-scan})

(defn- group-op? [op] (contains? group-ops (:op op)))

(defn- scan-op-of [op] (if (= :entity-group (:op op)) (:scan-op op) op))
(defn- merge-ops-of [op] (if (= :entity-group (:op op)) (vec (:merge-ops op)) []))

(defn- locus-rank
  "Intra-op evaluation order of a locus."
  [locus]
  (cond
    (= locus :args) -1
    (= locus :scan) 0
    (= locus :call-args) 0
    (= locus :join-vars) 0
    (= locus :branches) 0
    (and (vector? locus) (= :merge (first locus))) (inc (long (second locus)))
    (= locus :binding) 1000
    :else 500))

(defn site-rank [s] [(:op-idx s) (locus-rank (:locus s)) (or (:pos s) 0)])

(defn- clause-sites
  [op-idx locus clause role extra]
  (into []
        (keep-indexed
         (fn [pos x]
           (when (analyze/free-var? x)
             (merge {:op-idx op-idx :locus locus :pos pos :var x :role role} extra))))
        (vec clause)))

(defn- binding-vars [binding]
  (into #{} (filter analyze/free-var?) (analyze/extract-vars binding)))

(defn- or-produced-vars
  "Vars EVERY branch of an OR(-JOIN) produces — the same intersection
   plan/op-produced-vars uses."
  [op]
  (let [per-branch (mapv (comp set plan/branch-produced-vars) (:branches op))]
    (if (seq per-branch)
      (reduce clojure.set/intersection per-branch)
      #{})))

(defn op-sites
  "All variable-occurrence sites of `op` at index `op-idx`.

   :role is :producer when the site can be the first binder of the var, and
   :consumer when the site can only test an already-bound value.

   Returns [sites unmodelled?] — unmodelled? is true for op shapes whose
   obligation contract is not written yet."
  [op-idx op]
  (case (:op op)
    (:entity-group :pattern-scan)
    (let [scan (scan-op-of op)
          optional-scan? (boolean (:optional? scan))
          scan-sites (map (fn [s]
                            ;; a fused get-else does not BIND its entity var,
                            ;; it needs one bound (plan/op-required-vars)
                            (if (and optional-scan? (= 0 (:pos s)))
                              (assoc s :role :consumer)
                              s))
                          (clause-sites op-idx :scan (:clause scan) :producer nil))
          merge-sites
          (mapcat (fn [k mop]
                    (let [anti? (boolean (:anti? mop))]
                      (map (fn [s]
                             (cond
                               ;; merge is a lookupGE keyed on the scan's eid
                               (= 0 (:pos s)) (assoc s :role :consumer)
                               anti?           (assoc s :role :consumer :anti? true)
                               :else s))
                           (clause-sites op-idx [:merge k] (:clause mop) :producer
                                         (cond-> nil anti? (assoc :anti? true))))))
                  (range) (merge-ops-of op))]
      [(vec (concat scan-sites merge-sites)) false])

    :predicate
    [(mapv (fn [v] {:op-idx op-idx :locus :args :var v :role :consumer})
           (plan/args-free-vars (:args op)))
     false]

    (:function :external-engine)
    [(into (mapv (fn [v] {:op-idx op-idx :locus :args :var v :role :consumer})
                 (plan/args-free-vars (:args op)))
           (mapv (fn [v] {:op-idx op-idx :locus :binding :var v :role :producer})
                 (binding-vars (:binding op))))
     false]

    (:not :not-join)
    ;; the body is its own scope and is lowered (and checked) separately;
    ;; at THIS level the op only consumes its join vars.
    [(mapv (fn [v] {:op-idx op-idx :locus :join-vars :var v :role :consumer})
           (filter analyze/free-var? (or (:join-vars op) (:vars op))))
     false]

    (:or :or-join)
    (let [produced (or-produced-vars op)
          allv (filter analyze/free-var? (or (:join-vars op) (:vars op)))]
      [(mapv (fn [v] {:op-idx op-idx :locus :branches :var v
                      :role (if (contains? produced v) :producer :consumer)})
             allv)
       false])

    (:rule-call :recursive-rule :rule-lookup)
    [(mapv (fn [v] {:op-idx op-idx :locus :call-args :var v :role :producer})
           (filter analyze/free-var? (:call-args op)))
     false]

    ;; :passthrough and anything unknown
    [(mapv (fn [v] {:op-idx op-idx :locus :unknown :var v :role :producer})
           (filter analyze/free-var? (:vars op)))
     true]))

;; ---------------------------------------------------------------------------
;; What an op ACTUALLY materialises, vs what it DECLARES
;;
;; execute-fused-scan-rel builds the output Relation's attrs by walking the
;; scan clause then every merge clause, handing each not-yet-seen free var the
;; NEXT column index — while the tuple grows by one slot per merge that has a
;; free v (and one per free tx), including duplicates and excluding antis.
;; The two walks USED to disagree in two ways, both silent:
;;   * an ANTI merge's vars got a column index but never a slot → phantom column
;;   * a DUPLICATE v/tx var consumed a slot but no index        → every later
;;                                                                column shifted
;; Both are fixed: `merge-emit-v` / `merge-emit-tx` (execute.cljc) now record
;; per merge whether a slot is actually appended, using the SAME
;; `(not (contains? attrs …))` test the index walk uses and skipping anti
;; merges. This model must mirror that, or it reports a shift for every query
;; with a repeated merge value var — which is precisely the class the fix
;; addressed, so the checker would reject the code that repaired it.

(defn group-column-layout
  "Simulate execute-fused-scan-rel's out-attrs vs the tuple it actually builds.
   Returns {:declared {var → attrs-index}
            :actual   {var → real tuple index}
            :phantoms #{vars with an index but no slot}
            :shifted  #{vars whose declared index ≠ actual index}}"
  [op]
  (let [scan (scan-op-of op)
        merges (merge-ops-of op)
        scan-clause (vec (:clause scan))
        base (reduce (fn [m [pos x]]
                       (if (analyze/free-var? x) (assoc m x pos) m))
                     {} (map-indexed vector scan-clause))
        ;; declared: out-attrs (execute.cljc `out-attrs`)
        declared (loop [attrs base, idx 4, [mop & more] merges]
                   (if (nil? mop)
                     attrs
                     (let [[attrs idx]
                           (reduce (fn [[a i] x]
                                     (if (and (analyze/free-var? x) (not (contains? a x)))
                                       [(assoc a x (inc i)) (inc i)]
                                       [a i]))
                                   [attrs idx]
                                   (vec (:clause mop)))]
                       (recur attrs idx more))))
        ;; actual: 5 fixed slots [e a v tx added] then one slot per EMITTED field.
        ;; A field is emitted only when it is a free var the layout has not
        ;; already bound — mirroring merge-emit-v / merge-emit-tx, which thread
        ;; the growing attrs map exactly as the index walk above does.
        slots (first
               (reduce (fn [[acc seen] mop]
                         (if (:anti? mop)
                           [acc seen]
                           (let [c (vec (:clause mop))
                                 step (fn [[acc seen] x]
                                        (if (and (some? x) (symbol? x)
                                                 (analyze/free-var? x)
                                                 (not (contains? seen x)))
                                          [(conj acc x) (conj seen x)]
                                          [acc seen]))]
                             (-> [acc seen] (step (get c 2)) (step (get c 3))))))
                       [[] (into #{} (keys base))]
                       merges))
        actual (reduce (fn [m [i v]]
                         (if (contains? m v) m (assoc m v (+ 5 (long i)))))
                       base
                       (map-indexed vector slots))]
    {:declared declared
     :actual actual
     :phantoms (into #{} (remove #(contains? actual %)) (keys declared))
     :shifted (into #{} (filter #(and (contains? actual %)
                                      (not= (get declared %) (get actual %))))
                    (keys declared))}))

(defn op-actual-columns
  "Vars for which `op` actually materialises a value downstream ops can join on."
  [op]
  (case (:op op)
    (:entity-group :pattern-scan) (set (keys (:actual (group-column-layout op))))
    (:function :external-engine) (binding-vars (:binding op))
    (:rule-call :recursive-rule :rule-lookup)
    (into #{} (filter analyze/free-var?) (:call-args op))
    (:or :or-join) (or-produced-vars op)
    (:predicate :not :not-join) #{}
    ;; unknown: assume it produces whatever it declares
    (into #{} (filter analyze/free-var?) (:vars op))))

(defn op-declared-produced
  "Vars the PLANNER believes this op binds — `plan/op-produced-vars`, reached
   through its public wrapper. This is what `detect-inter-group-joins`,
   `order-plan-ops` and `structurally-fusable?` reason with; when it exceeds
   what the op actually materialises, downstream joins run against a column
   that is never written."
  [op]
  (set (plan/branch-produced-vars {:ops [op]})))

;; ---------------------------------------------------------------------------
;; Implied obligations

(defn implied-equalities
  "The complete set of equality obligations a plan's variable co-occurrence
   implies: for each var, {producer, consumer} pairs against the FIRST
   producer-capable occurrence in execution order.

   Returns {:obligations [...] :unmodelled-ops [...] :no-producer #{vars}}"
  [plan]
  (let [ops (vec (:ops plan))
        per-op (map-indexed (fn [i op] (op-sites i op)) ops)
        sites (vec (mapcat first per-op))
        unmodelled (into [] (keep-indexed (fn [i [_ u?]] (when u? i))) (vec per-op))
        by-var (group-by :var sites)]
    {:obligations
     (vec (mapcat
           (fn [[v vsites]]
             (let [sorted (sort-by site-rank vsites)
                   producer (first (filter #(= :producer (:role %)) sorted))]
               (when producer
                 (for [c sorted
                       :when (not= c producer)]
                   {:var v :producer producer :consumer c}))))
           by-var))
     :unmodelled-ops unmodelled
     :no-producer (into #{}
                        (keep (fn [[v vsites]]
                                (when-not (some #(= :producer (:role %)) vsites) v)))
                        by-var)}))

;; ---------------------------------------------------------------------------
;; Enforcement model

(defn- group-indexes*
  "{op-idx → group-idx} using the same filter detect-inter-group-joins uses."
  [ops]
  (let [pairs (keep-indexed (fn [oi op] (when (group-op? op) oi)) ops)]
    (into {} (map-indexed (fn [gi oi] [oi gi])) pairs)))

(defn- in-clause? [clause v] (boolean (some #(= v %) (vec clause))))

(defn- probe-resolvable?
  "Mirror of execute/find-probe-info: the probe var must sit in the CONSUMER's
   scan clause and somewhere in the PRODUCER's scan or merge clauses."
  [consumer-op producer-op v]
  (and (in-clause? (:clause (scan-op-of consumer-op)) v)
       (or (in-clause? (:clause (scan-op-of producer-op)) v)
           (some #(in-clause? (:clause %) v) (merge-ops-of producer-op)))))

(defn enforced-by
  "Which mechanism, if any, enforces `obligation` on each execution path.
   Returns {:rel <kw|nil> :direct <kw|nil>}.

   Mechanisms (verified against execute.cljc on this branch — see the drift note below):
     :merge-entity-key    merge lookupGE is keyed on the scan datom's eid
   THIS IS A HAND-DERIVED MODEL of execute.cljc and it WILL drift — it already
   did once, in the very PR that introduced it: the layout simulation kept
   describing the duplicate-slot column shift that the same PR fixed, which made
   it reject correct queries (18% of tx-var plans) including the PR's own
   regression test. Whenever an enforcement mechanism in execute.cljc changes,
   this model must change with it. The durable fix is for the executor to CONSUME
   declared obligations, after which this becomes a two-sided consistency check
   rather than a re-derivation.

   Mechanisms:
     :merge-eq-slot       build-common-merge-arrays → merge-eq-slots: an int
                          naming the binding slot an occurrence must equal, so
                          ANY earlier scan/merge position can be the producer
                          (superseded the merge-check-scan-v/tx booleans)
     :collapse-rels       rel/collapse-rels hash-joins on shared attrs
     :group-join-probe    the plan's :group-joins probe (direct path)
     :ctx-var-lookup      a predicate/function arg resolved by var name"
  [plan ops g-idx obligation]
  (let [{:keys [var producer consumer]} obligation
        pi (:op-idx producer)
        ci (:op-idx consumer)
        p-op (nth ops pi)
        c-op (nth ops ci)]
    (if (= pi ci)
      ;; ---------------- intra-op ----------------
      (let [pl (:locus producer) cl (:locus consumer)
            pp (:pos producer)   cp (:pos consumer)
            merge? (and (vector? cl) (= :merge (first cl)))
            m (cond
                ;; merge's entity position ≡ the scan's entity: structural
                (and merge? (= 0 cp) (= :scan pl) (= 0 pp)) :merge-entity-key
                ;; Every other intra-group value/tx equality is carried by
                ;; `merge-eq-slots`, which assigns each occurrence the slot it
                ;; must equal in one left-to-right walk over scan e/a/v/tx then
                ;; each merge's v/tx. It therefore covers merge→scan (the old
                ;; merge-check-scan-v/tx booleans) AND merge→merge, which those
                ;; booleans could not express.
                (and merge? (#{2 3} cp) (#{2 3} pp)
                     (or (= :scan pl) (and (vector? pl) (= :merge (first pl)))))
                :merge-eq-slot
                :else nil)]
        {:rel m :direct m})
      ;; ---------------- inter-op ----------------
      (let [p-cols (op-actual-columns p-op)
            has-col? (contains? p-cols var)
            rel (cond
                  (not has-col?) nil
                  (group-op? c-op)
                  (when (contains? (op-actual-columns c-op) var) :collapse-rels)
                  :else :ctx-var-lookup)
            direct (cond
                     (not has-col?) nil
                     (and (group-op? p-op) (group-op? c-op))
                     (let [gj (:group-joins plan)
                           cg (get g-idx ci)
                           pg (get g-idx pi)
                           entry (get gj cg)]
                       (when (and entry
                                  (= pg (:producer-idx entry))
                                  (= var (first (:probe-vars entry)))
                                  (probe-resolvable? c-op p-op var))
                         :group-join-probe))
                     ;; execute-plan-direct runs ALL groups first, then post-ops,
                     ;; regardless of plan order. So a function/engine bind that
                     ;; the plan orders BEFORE a group still meets the group's
                     ;; column in post-apply-fns, whose already-bound branch
                     ;; compares instead of writing (execute.cljc:2409).
                     (and (group-op? c-op)
                          (#{:function :external-engine} (:op p-op))
                          (contains? (binding-vars (:binding p-op)) var))
                     :post-apply-fns

                     (group-op? c-op) nil ;; a rule / OR producer feeding a group
                                          ;; is not expressible on the direct path
                     :else :ctx-var-lookup)]
        {:rel rel :direct direct}))))

(defn- direct-path-plausible?
  "Would this plan even be offered to execute-plan-direct? (Structural part of
   can-direct-fuse?; the runtime part — find-var coverage — is not visible from
   the plan alone.)

   Must mirror `can-direct-fuse?`, INCLUDING its requirement that every implied
   cross-group equality is actually enforced. Without that clause this reports
   plans as direct-plausible which the gate now declines to the Relation engine,
   producing :missing-direct findings for queries that are in fact correct —
   i.e. the checker's model of the executor drifts the moment the executor is
   fixed. That drift is why the executor should eventually CONSUME declared
   obligations instead of the checker re-deriving them."
  [plan]
  (let [ops (vec (:ops plan))
        g-ops (filterv group-op? ops)
        g-idx (group-indexes* ops)
        oi-of (into {} (map (fn [[oi gi]] [gi oi])) g-idx)]
    (and (if (contains? plan :structurally-fusable?)
           (:structurally-fusable? plan)
           (plan/structurally-fusable? ops))
         (not (:has-passthrough? plan))
         (seq g-ops)
         ;; …and the gate's own cross-group clause. CALL it, never re-implement
         ;; it: a private copy here would keep reporting "declined" if the real
         ;; gate were ever weakened, and the checker would silently stop
         ;; catching the very bug it exists for.
         (plan/all-group-equalities-enforced? g-ops (:group-joins plan))
         ;; can-direct-fuse? rejects a plan whose recorded probe cannot resolve
         (every? (fn [[cg {:keys [producer-idx probe-vars]}]]
                   (let [c-op (nth ops (get oi-of cg))
                         p-op (nth ops (get oi-of producer-idx))]
                     (probe-resolvable? c-op p-op (first probe-vars))))
                 (:group-joins plan)))))

;; ---------------------------------------------------------------------------
;; Over-enforcement: phantom columns and ill-formed probes

(defn over-enforcements
  "Equalities the plan enforces (or would enforce) that the query does NOT
   imply — the over-constraining direction.

   Today's only structural source is a var an op ADVERTISES (:vars /
   :output-vars, which detect-inter-group-joins and collapse-rels read) but
   never materialises: the join then runs against an absent column."
  [plan]
  (let [ops (vec (:ops plan))]
    (vec
     (mapcat
      (fn [i op]
        (let [phantom (clojure.set/difference (op-declared-produced op)
                                              (op-actual-columns op))
              layout (when (group-op? op) (group-column-layout op))
              mentioned-elsewhere?
              (fn [v] (some (fn [[j o]]
                              (and (not= i j)
                                   (contains? (into #{} (filter analyze/free-var?)
                                                    (or (:output-vars o) (:vars o)))
                                              v)))
                            (map-indexed vector ops)))]
          (concat
           ;; only a problem when some OTHER op also mentions v: then the
           ;; planner believes there is a join on a column nobody writes.
           (for [v phantom :when (mentioned-elsewhere? v)]
             {:kind :phantom-column :op-idx i :op (:op op) :var v})
           (for [v (:shifted layout)]
             {:kind :column-shift :op-idx i :op (:op op) :var v
              :declared-index (get-in layout [:declared v])
              :actual-index (get-in layout [:actual v])})
           ;; a recorded inter-group probe whose var the producer never writes
           (when (group-op? op)
             (let [g-idx (group-indexes* ops)
                   oi-of (into {} (map (fn [[oi gi]] [gi oi])) g-idx)]
               (when-let [{:keys [producer-idx probe-vars]} (get (:group-joins plan)
                                                                 (get g-idx i))]
                 (let [pv (first probe-vars)
                       p-op (nth ops (get oi-of producer-idx))]
                   (when-not (contains? (op-actual-columns p-op) pv)
                     [{:kind :probe-on-phantom :op-idx i :op (:op op) :var pv
                       :producer-idx producer-idx}]))))))))
      (range) ops))))

;; ---------------------------------------------------------------------------
;; The check

(defn check-plan
  "Compute the invariant report for a plan. Pure — no execution.

   {:ok?             both inclusions hold
    :missing-rel     obligations the Relation path does not enforce
    :missing-direct  obligations the direct/fused path does not enforce
                     (only when the plan is structurally eligible for it)
    :over            over-constraining findings (phantom columns / column shift)
    :unmodelled-ops  op indexes whose contract is not written yet
    :no-producer     vars with no producing occurrence (:in-bound, or unbindable)}"
  [plan]
  (let [ops (vec (:ops plan))
        {:keys [obligations unmodelled-ops no-producer]} (implied-equalities plan)
        g-idx (group-indexes* ops)
        unmodelled? (set unmodelled-ops)
        relevant (if *tolerate-unannotated?*
                   (remove (fn [{:keys [producer consumer]}]
                             (or (unmodelled? (:op-idx producer))
                                 (unmodelled? (:op-idx consumer))))
                           obligations)
                   obligations)
        judged (mapv (fn [o] (assoc o :by (enforced-by plan ops g-idx o))) relevant)
        direct? (direct-path-plausible? plan)
        over (over-enforcements plan)
        missing-rel (filterv #(nil? (:rel (:by %))) judged)
        missing-direct (if direct?
                         (filterv #(nil? (:direct (:by %))) judged)
                         [])]
    {:ok? (and (empty? missing-rel) (empty? missing-direct) (empty? over))
     :n-obligations (count judged)
     :missing-rel missing-rel
     :missing-direct missing-direct
     :over over
     :direct-path-plausible? direct?
     :unmodelled-ops unmodelled-ops
     :no-producer no-producer}))

(defn explain-site [s]
  (str (:var s) "@op" (:op-idx s) (:locus s)
       (when (:pos s) (str "." (:pos s)))))

(defn explain
  "One-line-per-finding rendering of a report."
  [report]
  (concat
   (for [m (:missing-rel report)]
     (str "UNDER (rel)    " (:var m) ": " (explain-site (:producer m))
          " ≠ enforced against " (explain-site (:consumer m))))
   (for [m (:missing-direct report)]
     (str "UNDER (direct) " (:var m) ": " (explain-site (:producer m))
          " ≠ enforced against " (explain-site (:consumer m))))
   (for [o (:over report)]
     (str "OVER           " (name (:kind o)) " " (:var o) " on op" (:op-idx o)
          " (" (:op o) ")"
          (when (:declared-index o)
            (str " declared=" (:declared-index o) " actual=" (:actual-index o)))))))

(defn maybe-check!
  "Call site for the lowering pass. No-op unless *check-equalities?*."
  [plan]
  (when *check-equalities?*
    (let [report (check-plan plan)]
      (when-not (:ok? report)
        (if *violation-handler*
          (*violation-handler* (assoc report :plan plan))
          (throw (ex-info "Plan violates the equality-obligation invariant"
                          (assoc report :explain (vec (explain report)))))))))
  plan)
