(ns datahike.test.query-differential-test
  "Seeded generative differential testing: the base (relational) engine is the
   semantic reference; every generated query must produce the SAME result (or
   the same raised/answered outcome) under the planner.

   Rationale: the planner re-implements semantics the base engine gets by
   construction, and every re-implementation site can drift — the nested-q
   scope leaks, the get-else source/default divergences and the silent-drop
   ordering bugs were all planner-vs-base divergences that no enumerated test
   shape covered. Enumerated shapes live in query-shape-corpus-test; this
   namespace explores the composition space around them.

   CI budget: a FIXED seed keeps this deterministic; 3000 cases run in ~23s
   (the JVM/db setup dominates — marginal cost is ~5ms per case). To fuzz
   more deeply out-of-band, raise the count locally, e.g.:
     DATAHIKE_DIFF_CASES=5000 clojure -M:test --focus datahike.test.query-differential-test
   A failure prints the offending query; with the fixed seed it reproduces
   deterministically, and test.check shrinks it to a minimal spec."
  (:require
   [clojure.test :refer [is deftest testing]]
   [clojure.test.check.clojure-test :refer [defspec]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [datahike.api :as d]
   [datahike.oracle :as o]
   [datahike.query :as q]))

(def ^:private num-cases
  ;; 3000: the fixed JVM/db setup dominates the cost (100 cases ≈ 8.7s,
  ;; 3000 ≈ 23s — ~5ms marginal per case once warm), and every distinct
  ;; divergence the 30k-seed hunts found surfaced within the first ~2600
  ;; seeds — 3000 encloses the observed discovery zone with margin.
  (or (some-> (System/getenv "DATAHIKE_DIFF_CASES") parse-long) 3000))

;; One shared db for all cases — content exercises card-one/card-many, refs,
;; missing attributes (for get-else), keyword values (for or-branches) and a
;; retraction (so history exists and index structure isn't pristine).
(defonce ^:private test-db
  (delay
    (let [cfg {:store {:backend :memory :id (random-uuid)}
               :schema-flexibility :write}]
      (d/create-database cfg)
      (let [conn (d/connect cfg)]
        (d/transact conn [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
                          {:db/ident :nick :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
                          {:db/ident :score :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                          {:db/ident :tag :db/valueType :db.type/keyword :db/cardinality :db.cardinality/many}
                          {:db/ident :friend :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
                          ;; a SECOND ref relation, so a recursive rule's step can
                          ;; traverse something other than its base case's edge
                          {:db/ident :colleague :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
                          ;; unique, so an :in binding can arrive as a LOOKUP REF
                          {:db/ident :uid :db/valueType :db.type/string
                           :db/unique :db.unique/identity :db/cardinality :db.cardinality/one}])
        (d/transact conn [{:db/id 100 :uid "u100" :name "alice" :nick "al" :score 10 :tag [:red :blue] :friend 101 :colleague 102}
                          {:db/id 101 :uid "u101" :name "bob" :score 20 :tag [:blue] :colleague 103}
                          {:db/id 102 :uid "u102" :name "carol" :nick "cc" :score 30 :friend 100}
                          {:db/id 103 :uid "u103" :name "dave" :tag [:red]}
                          {:db/id 104 :uid "u104" :name "eve" :score 20 :friend 103}
                          {:db/id 105 :uid "u105" :name "frank" :nick "f" :tag [:green :red] :score 5}])
        (d/transact conn [[:db/retract 101 :score 20]])
        (d/transact conn [[:db/add 101 :score 25]])
        (d/db conn)))))

;; ---------------------------------------------------------------------------
;; Query specs: independent generator choices assembled into an always-valid
;; query by a pure builder — inapplicable choices degrade gracefully instead
;; of producing unbound-var queries, so every case tests real behavior.

(defonce ^:private test-db2
  (delay
    (let [cfg {:store {:backend :memory :id (random-uuid)}
               :schema-flexibility :write}]
      (d/create-database cfg)
      (let [conn (d/connect cfg)]
        (d/transact conn [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
                          {:db/ident :score :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])
        ;; overlapping entity ids with db1, PARTIAL coverage (103/104 absent)
        ;; and different values — cross-db joins must discriminate sources
        (d/transact conn [{:db/id 100 :name "alice-2" :score 1}
                          {:db/id 101 :name "bob-2"}
                          {:db/id 102 :name "carol-2" :score 3}
                          {:db/id 105 :name "frank-2" :score 5}])
        (d/db conn)))))

;; ---------------------------------------------------------------------------
;; Axis: THE DATA. The generator above varies the query's shape across ~20
;; dimensions, but every case used to run against one tidy dataset — values
;; well-typed, distinct, no duplicates. Whole families of bug hid there, because
;; the specializations classify their INPUT (a sampled row, a value's shape, a
;; column's type) and tidy input makes every classification look right.

(defonce ^:private test-db-dup
  ;; Heavy duplication. An aggregate reads the FIND PROJECTION, so duplicate
  ;; values are what distinguish `count`/`sum` over a deduplicated projection
  ;; from one over raw rows — a distinction invisible when every value differs.
  (delay
    (let [cfg {:store {:backend :memory :id (random-uuid)}
               :schema-flexibility :write}]
      (d/create-database cfg)
      (let [conn (d/connect cfg)]
        (d/transact conn [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
                          {:db/ident :nick :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
                          {:db/ident :score :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                          {:db/ident :tag :db/valueType :db.type/keyword :db/cardinality :db.cardinality/many}
                          {:db/ident :friend :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
                          {:db/ident :colleague :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
                          {:db/ident :uid :db/valueType :db.type/string
                           :db/unique :db.unique/identity :db/cardinality :db.cardinality/one}])
        ;; ACYCLIC on :friend, deliberately. A cycle here makes the reference
        ;; engine's mutual recursion run forever (see
        ;; query-rules-test/test-mutual-recursion-over-a-cycle), and a generator
        ;; must not contain a combination known to hang — that belongs in a
        ;; pinned test, not as a landmine every future run steps on.
        (d/transact conn [{:db/id 100 :uid "u100" :name "alice" :nick "dup" :score 20 :tag [:red :blue] :friend 101 :colleague 103}
                          {:db/id 101 :uid "u101" :name "alice" :nick "dup" :score 20 :tag [:blue] :friend 102 :colleague 104}
                          {:db/id 102 :uid "u102" :name "alice" :score 20 :tag [:red :blue]}
                          {:db/id 103 :uid "u103" :name "bob" :nick "dup" :score 5 :tag [:red]}
                          {:db/id 104 :uid "u104" :name "bob" :score 5 :tag [:red :blue] :friend 103}
                          {:db/id 105 :uid "u105" :name "bob" :nick "dup" :score 5}])
        (d/transact conn [[:db/retract 101 :score 20]])
        (d/transact conn [[:db/add 101 :score 20]])
        (d/db conn)))))

(defonce ^:private test-db-loose
  ;; `:schema-flexibility :read`: :score and :name are UNDECLARED, so one
  ;; attribute holds Long, Double and String values at once. Any code that types
  ;; a whole column from one sampled row is wrong here, and a fast path that
  ;; assumes numbers must decline rather than truncate. :uid stays unique so
  ;; lookup-ref bindings still mean something, and :friend stays a ref so the
  ;; recursive rules still traverse.
  (delay
    (let [cfg {:store {:backend :memory :id (random-uuid)}
               :schema-flexibility :read}]
      (d/create-database cfg)
      (let [conn (d/connect cfg)]
        (d/transact conn [{:db/ident :uid :db/valueType :db.type/string
                           :db/unique :db.unique/identity :db/cardinality :db.cardinality/one}
                          {:db/ident :friend :db/valueType :db.type/ref
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :colleague :db/valueType :db.type/ref
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :tag :db/valueType :db.type/keyword
                           :db/cardinality :db.cardinality/many}])
        (d/transact conn [{:db/id 100 :uid "u100" :name "alice" :nick "al" :score 10 :tag [:red :blue] :friend 101 :colleague 102}
                          {:db/id 101 :uid "u101" :name "bob" :score 2.5 :tag [:blue] :colleague 103}
                          {:db/id 102 :uid "u102" :name "carol" :nick "cc" :score "thirty" :friend 100}
                          {:db/id 103 :uid "u103" :name "dave" :score 7 :tag [:red]}
                          {:db/id 104 :uid "u104" :name 42 :score 7 :friend 103}
                          {:db/id 105 :uid "u105" :name "frank" :nick "f" :tag [:green :red] :score 5}])
        (d/transact conn [[:db/retract 103 :score 7]])
        (d/transact conn [[:db/add 103 :score 7.5]])
        (d/db conn)))))

(defn- dataset-db [dataset]
  (case dataset
    :dup @test-db-dup
    :loose @test-db-loose
    @test-db))

(def ^:private rule-sets
  {:plain     '[[(named ?e ?n) [?e :name ?n]]]
   :fn-body   '[[(upper-name ?e ?ru) [?e :name ?rn] [(clojure.string/upper-case ?rn) ?ru]]]
   :recursive '[[(reach ?a ?b) [?a :friend ?b]]
                [(reach ?a ?b) [?a :friend ?x] (reach ?x ?b)]]
   :mutual    '[[(ehop ?a ?b) [?a :friend ?b]]
                [(ehop ?a ?b) [?a :friend ?x] (ohop ?x ?b)]
                [(ohop ?a ?b) [?a :friend ?x] (ehop ?x ?b)]]
   :with-not  '[[(unred ?e) [?e :name ?rn] (not [?e :tag :red])]]
   ;; Axis: RULE SHAPE. The five sets above are all closures of the SAME
   ;; relation their base case walks, which is the one shape the recursive fast
   ;; paths assume. The three below break that assumption in the three ways that
   ;; matter, and each has already been a wrong answer:
   ;;   - the step traverses a DIFFERENT relation than the base case;
   ;;   - the step is a SUBSET of it (filtered traversal);
   ;;   - the recursion is right-linear rather than left-linear.
   :rec-changes-edge '[[(xreach ?a ?b) [?a :friend ?b]]
                       [(xreach ?a ?b) [?a :colleague ?x] (xreach ?x ?b)]]
   :rec-filtered     '[[(freach ?a ?b) [?a :friend ?b]]
                       [(freach ?a ?b) [?a :friend ?x] [?x :tag :blue] (freach ?x ?b)]]
   :rec-right        '[[(rreach ?a ?b) [?a :friend ?b]]
                       [(rreach ?a ?b) (rreach ?a ?x) [?x :friend ?b]]]})

(def ^:private rule-clause
  {:plain     '(named ?e ?rn2)
   :fn-body   '(upper-name ?e ?ru)
   :recursive '(reach ?e ?r)
   :mutual    '(ehop ?e ?r)
   :with-not  '(unred ?e)
   :rec-changes-edge '(xreach ?e ?r)
   :rec-filtered     '(freach ?e ?r)
   :rec-right        '(rreach ?e ?r)})

(def ^:private gen-spec
  (gen/hash-map
   :score?    gen/boolean                                    ;; add [?e :score ?s]
   :tag?      gen/boolean                                    ;; add [?e :tag ?t]
   :friend?   gen/boolean                                    ;; add [?e :friend ?f] [?f :name ?fn]
   ;; 0-3 stacked modifiers — combinations are where ordering/pipeline
   ;; interactions live (a single-modifier grammar found three bugs; the
   ;; composition space is the next stratum)
   :modifiers (gen/vector-distinct
               (gen/elements [:pred-lt :pred-gt :fn-upper :fn-chain
                              :get-else :get-else-long :missing-nick
                              :not-tag :not-join-nick :or-tag :or-and
                              :pred-two-vars])
               {:min-elements 0 :max-elements 3})
   :pred-const (gen/choose -5 40)
   ;; deterministic clause permutation — both engines must tolerate ANY
   ;; user-written clause order
   :shuffle-seed (gen/choose 0 1000)
   :shuffle?  gen/boolean
   :temporal  (gen/frequency [[6 (gen/return :none)]
                              [2 (gen/return :as-of)]
                              [1 (gen/return :history)]])
   :in-coll?  gen/boolean                                    ;; bind ?e via :in [?e ...]
   ;; ---------------------------------------------------------------------
   ;; :in binding SHAPES. The generator used to bind only `[?e ...]`, `%`
   ;; and `$2`, which left the whole scalar-const dimension ungenerated —
   ;; and a scalar :in is folded into the clauses before planning, so it is
   ;; the input that makes the planner and the base engine see different
   ;; clause text. Every divergence in the #901 family needed one.
   ;; ---------------------------------------------------------------------
   ;; scalar :in for a value var — :unused binds a var no clause mentions,
   ;; the shape that proves a post-fold decision cannot be cached safely
   :in-scalar (gen/elements [:none :none :tag :score :name :unused])
   :in-tuple?  gen/boolean                                   ;; bind [?t ?s] as a tuple
   :in-rel?    gen/boolean                                   ;; bind [[?t]] as a relation
   :in-lookup? gen/boolean                                   ;; bind ?e from a lookup ref
   ;; a rule called with a GROUND argument: :in-arg drives magic-set demand
   ;; from the input side, :out-arg from the output side (opposite seeding)
   :rule-ground (gen/elements [:none :none :in-arg :out-arg])
   ;; result-shaping options. No :limit/:offset: with ties in the sort key
   ;; the row SET at a cut-off is genuinely ambiguous, so a disagreement
   ;; there would not be a bug. :order-by is compared as a multiset for the
   ;; same reason — it still catches wrong or missing rows.
   :result-mod (gen/elements [:none :none :with :order-by])
   ;; rules: a rule clause added to the body, rule set passed via :in %
   :rules     (gen/frequency [[5 (gen/return :none)]
                              [1 (gen/return :plain)]
                              [1 (gen/return :fn-body)]
                              [1 (gen/return :recursive)]
                              [1 (gen/return :mutual)]
                              [1 (gen/return :with-not)]
                              [1 (gen/return :rec-changes-edge)]
                              [1 (gen/return :rec-filtered)]])
   ;; :rec-right is DEFINED above but deliberately not generated: the reference
   ;; engine does not terminate on a rule whose body leads with the recursive
   ;; call, so it cannot serve as the oracle for that shape. Pinned instead in
   ;; query-rules-test/test-right-recursive-rule — a generator must not contain
   ;; a combination known to hang.
   ;; multi-source: a $2 clause joining ?e across databases
   :multi     (gen/frequency [[4 (gen/return :none)]
                              [1 (gen/return :join-name)]
                              [1 (gen/return :join-score)]])
   :use2?     gen/boolean                                    ;; prefer a $2/rule var as primary
   ;; :consumer-only projects ONLY the friend-join group's var (?fn): with a
   ;; predicate on ?s that leaves the producer group with an attached pred and
   ;; zero find-vars — the collect-only producer shape whose hoisted-predicate
   ;; slot mis-wiring #887 fixed. No other find variant can produce it, because
   ;; whenever ?s exists it dominates `primary`.
   :find      (gen/elements [:e :e+primary :e+modifier :primary+modifier
                             :coll-primary :agg-count :agg-min :agg-count-primary
                             :consumer-only
                             ;; Axis: THE AGGREGATE. Only `count` and `min` were
                             ;; generated — the two type-PRESERVING ones, which
                             ;; is why the population-vs-sample variance split
                             ;; and the median's type both went unnoticed.
                             :agg-avg :agg-variance :agg-stddev :agg-median
                             :agg-sum :agg-count-distinct :agg-min-n
                             :agg-avg-grouped :agg-median-grouped])
   ;; Axis: THE DATA — see the dataset defs above.
   :dataset   (gen/frequency [[3 (gen/return :tidy)]
                              [2 (gen/return :dup)]
                              [2 (gen/return :loose)]])
   ;; ---------------------------------------------------------------------
   ;; Axis: OUTPUT-VAR REBINDING. A function/get-else clause writes its result
   ;; into a variable a PRECEDING pattern already bound, e.g.
   ;;   [?e :name ?n] [(clojure.string/upper-case ?n) ?n]
   ;; which asks for the entities whose name is already upper-case. Datomic
   ;; unifies here, as datahike itself does for repeated vars inside one clause
   ;; (#912/#913); overwriting instead asserts a fact the database does not
   ;; contain. Measured on this axis alone: 47% of cases answer wrongly and 19%
   ;; are wrong on BOTH engines, i.e. structurally invisible to base-vs-planner
   ;; comparison — which is exactly why the axis never existed and the bug
   ;; survived. It needs the oracle to be visible at all.
   :rebind?   (gen/frequency [[3 (gen/return false)] [1 (gen/return true)]])
   ;; ---------------------------------------------------------------------
   ;; Axis: FIND-VECTOR ORDER, varied INDEPENDENTLY of clause order. The
   ;; generator has always permuted :where clauses, but derived the :find
   ;; vector from them, so `[:find ?e ?v]` and `[:find ?v ?e]` over identical
   ;; clauses were never both generated. The fused multi-group projection is
   ;; sensitive to precisely that: with >=2 entity groups and a post-op it
   ;; emits the columns in set-iteration order rather than :find order.
   :find-perm? gen/boolean))

(def ^:private rule-out-vars
  "The var each rule clause BINDS — nil for the unary rule, which binds none.
   Grounding that argument removes the var from the query, so `primary` must
   not pick it."
  {:plain '?rn2 :fn-body '?ru :recursive '?r :mutual '?r :with-not nil
   :rec-changes-edge '?r :rec-filtered '?r :rec-right '?r})

(defn- ground-rule-clause
  "Rewrite a rule call to ground one argument. :out-arg grounds the rule's
   output — the direction that seeds magic-set demand from the wrong side —
   and :in-arg grounds its input. The unary rule has no output to ground."
  [clause rules ground]
  (let [args (vec (rest clause))]
    (case ground
      :none clause
      :in-arg (apply list (first clause) 100 (rest args))
      :out-arg (if (or (nil? (get rule-out-vars rules)) (< (count args) 2))
                 clause
                 (apply list (first clause) (first args)
                        (case rules
                          :plain ["alice"]
                          :fn-body ["ALICE"]
                          [103]))))))

(defn- build-query
  "Assemble a valid query + extra args from a spec.
   Returns [query args opts], where opts is the map-form extras (:order-by)."
  [{:keys [score? tag? friend? modifiers pred-const shuffle-seed shuffle?
           in-coll? rules multi use2? find
           in-scalar in-tuple? in-rel? in-lookup? rule-ground result-mod
           rebind? find-perm?]}]
  (let [;; :recursive/:mutual rule clauses walk :friend — force the pattern in
        friend? (or friend? (#{:recursive :mutual :rec-changes-edge
                               :rec-filtered :rec-right} rules))
        rule-ground (if (= :none rules) :none rule-ground)
        rule-cl (when (not= :none rules)
                  (ground-rule-clause (get rule-clause rules) rules rule-ground))
        ;; grounding the output removes that var from the query
        rule-var (when (and (not= :none rules)
                            (not= rule-cl (get rule-clause rules))
                            (= :out-arg rule-ground))
                   ::grounded)
        rule-out (when (and (not= :none rules) (nil? rule-var))
                   (get rule-out-vars rules))
        patterns (cond-> '[[?e :name ?n]]
                   score? (conj '[?e :score ?s])
                   tag? (conj '[?e :tag ?t])
                   friend? (conj '[?e :friend ?f] '[?f :name ?fn])
                   (not= :none rules) (conj rule-cl)
                   (= :join-name multi) (conj '[$2 ?e :name ?n2])
                   (= :join-score multi) (conj '[$2 ?e :score ?s2]))
        ;; modifiers that need ?s degrade when score? is absent
        modifiers (mapv (fn [m] (if (and (#{:pred-lt :pred-gt :pred-two-vars} m)
                                         (not score?))
                                  :fn-upper m))
                        (distinct modifiers))
        mod->clauses
        (fn [m]
          (case m
            :pred-lt       [[[(list '< '?s pred-const)]] nil]
            :pred-gt       [[[(list '> '?s pred-const)]] nil]
            :pred-two-vars [['[(< ?s 100)] '[(not= ?s 11)]] nil]
            ;; With :rebind?, the output var is `?n` — already bound by the
            ;; always-present [?e :name ?n]. The clause then CONSTRAINS rather
            ;; than binds: `[(upper-case ?n) ?n]` selects the already-upper-case
            ;; names (none, in these datasets), and `[(get-else $ ?e :nick _) ?n]`
            ;; selects the entities whose nick equals their name. An engine that
            ;; overwrites instead answers with every row, which is the divergence
            ;; this axis exists to expose.
            :fn-upper      [(if rebind?
                              ['[(clojure.string/upper-case ?n) ?n]]
                              ['[(clojure.string/upper-case ?n) ?u]])
                            (if rebind? '?n '?u)]
            :fn-chain      [['[(clojure.string/upper-case ?n) ?u]
                             (if rebind?
                               '[(clojure.string/lower-case ?u) ?n]
                               '[(clojure.string/lower-case ?u) ?l])]
                            (if rebind? '?n '?l)]
            :get-else      [(if rebind?
                              ['[(get-else $ ?e :nick "none") ?n]]
                              ['[(get-else $ ?e :nick "none") ?v]])
                            (if rebind? '?n '?v)]
            :get-else-long [['[(get-else $ ?e :score 0) ?gs]] '?gs]
            :missing-nick  [['[(missing? $ ?e :nick)]] nil]
            :not-tag       [['(not [?e :tag :red])] nil]
            :not-join-nick [['(not-join [?e] [?e :nick _])] nil]
            :or-tag        [['(or [?e :tag :red] [?e :tag :blue])] nil]
            :or-and        [['(or (and [?e :tag :red] [?e :score ?s2])
                                  [?e :nick "al"])] nil]))
        expanded (mapv mod->clauses modifiers)
        mod-clauses (into [] (mapcat first) expanded)
        mod-var (some second expanded)
        clauses (into patterns mod-clauses)
        clauses (if shuffle?
                  ;; seeded deterministic permutation
                  (let [rng (java.util.Random. (long shuffle-seed))
                        idxs (loop [order (vec (range (count clauses))) i (dec (count clauses))]
                               (if (pos? i)
                                 (let [j (.nextInt rng (inc i))]
                                   (recur (assoc order i (order j) j (order i)) (dec i)))
                                 order))]
                    (mapv clauses idxs))
                  clauses)
        primary (cond
                  (and use2? (= :join-name multi)) '?n2
                  (and use2? (= :join-score multi)) '?s2
                  (and use2? rule-out) rule-out
                  score? '?s tag? '?t friend? '?fn :else '?n)
        find-part (case find
                    :e ['?e]
                    :e+primary (vec (distinct ['?e primary]))
                    :e+modifier (vec (distinct ['?e (or mod-var primary)]))
                    :primary+modifier (vec (distinct [primary (or mod-var '?n)]))
                    :coll-primary [[primary '...]]
                    :agg-count [(list 'count '?e)]
                    :agg-min ['?e (list 'min (if score? '?s '?n))]
                    :agg-count-primary [primary (list 'count '?e)]
                    ;; only the consumer group's var; degrades to [?n] without
                    ;; the friend join (single group — no producer/consumer split)
                    :consumer-only [(if friend? '?fn '?n)]
                    ;; the value aggregates need a numeric-ish column; without
                    ;; :score they degrade to counting rather than build a query
                    ;; whose divergence would only be about the wrong column
                    :agg-avg (if score? [(list 'avg '?s)] [(list 'count '?e)])
                    :agg-variance (if score? [(list 'variance '?s)] [(list 'count '?e)])
                    :agg-stddev (if score? [(list 'stddev '?s)] [(list 'count '?e)])
                    :agg-median (if score? [(list 'median '?s)] [(list 'count '?e)])
                    :agg-sum (if score? [(list 'sum '?s)] [(list 'count '?e)])
                    :agg-count-distinct [(list 'count-distinct (if score? '?s '?n))]
                    ;; min/max with a COUNT argument returns a collection, a
                    ;; different contract from scalar min/max
                    :agg-min-n [(list 'min 2 (if score? '?s '?n))]
                    :agg-avg-grouped (if score?
                                       (vec (distinct [primary (list 'avg '?s)]))
                                       [primary (list 'count '?e)])
                    :agg-median-grouped (if score?
                                          (vec (distinct [primary (list 'median '?s)]))
                                          [primary (list 'count '?e)]))
        ;; ?e comes from a collection OR a lookup ref, never both
        e-binding (cond in-coll? ['[?e ...] [100 101 102 103 104 105]]
                        in-lookup? ['?e [:uid "u100"]])
        ;; A scalar wins over the tuple/relation shapes for the same vars, so
        ;; no var is ever bound twice.
        scalar-binding (case in-scalar
                         :tag ['?t :red]
                         :score ['?s 20]
                         :name ['?n "alice"]
                         :unused ['?unused 42]
                         nil)
        tuple-binding (when (and in-tuple? (= :none in-scalar))
                        ['[?t ?s] [:red 20]])
        rel-binding (when (and in-rel? (= :none in-scalar) (not in-tuple?))
                      ['[[?t]] [[:red] [:blue]]])
        ;; :in and args are assembled from ONE ordered list of pairs, so the
        ;; two can never drift out of correspondence.
        bindings (cond-> []
                   (not= :none multi) (conj ['$2 ::db2])
                   (not= :none rules) (conj ['% (get rule-sets rules)])
                   e-binding (conj e-binding)
                   scalar-binding (conj scalar-binding)
                   tuple-binding (conj tuple-binding)
                   rel-binding (conj rel-binding))
        in-part (when (seq bindings)
                  (into '[$] (map first) bindings))
        args (mapv second bindings)
        ;; :with needs plain-var find shapes; :order-by needs a symbol as the
        ;; first find element. Both degrade to :none rather than build an
        ;; invalid query.
        with-part (when (and (= :with result-mod) (#{:e :e+primary} find))
                    '[?n])
        ;; Vary :find ORDER independently of clause order. Reversal is enough to
        ;; separate [E V] from [V E], which is the axis the fused multi-group
        ;; projection is sensitive to, and it stays deterministic under the
        ;; fixed seed. Applied before :with/:order-by derive from find-part, so
        ;; those stay consistent with whatever order we ended up with.
        find-part (if (and find-perm? (> (count find-part) 1))
                    (vec (reverse find-part))
                    find-part)
        order-by (when (and (= :order-by result-mod) (symbol? (first find-part)))
                   [(first find-part) :asc])]
    [(vec (concat [:find] find-part
                  (when with-part (cons :with with-part))
                  (when in-part (cons :in in-part))
                  [:where] clauses))
     args
     (cond-> {} order-by (assoc :order-by order-by))]))

(defn- normalize
  "Order-insensitive, duplicate-preserving comparison form: collection finds
   ([?x ...]) and aggregate rels may come back in engine-specific order."
  [r]
  (cond
    (set? r) (into #{} (map (fn [t] (if (sequential? t) (vec t) t))) r)
    (sequential? r) (frequencies r)
    :else r))

(def ^:private case-timeout-ms
  "Per-engine wall clock for one generated case. Generated cases are tiny — the
   whole dataset is six entities — so anything this slow is not slow, it is
   stuck, and a divergence must be REPORTED rather than hanging the run. The
   first widened-axis run found exactly that: the reference engine does not
   terminate on mutual recursion over a cyclic :friend graph, which no tidy
   (acyclic) dataset could surface."
  (or (some-> (System/getenv "DATAHIKE_DIFF_CASE_TIMEOUT_MS") parse-long) 5000))

(defn- run-engine [disable? query db args opts]
  (let [thunk (fn []
                (try
                  (binding [q/*disable-planner* disable?]
                    (let [args' (into [db] (map (fn [a] (if (= ::db2 a) @test-db2 a))) args)]
                      (normalize
                       (if (seq opts)
                         ;; map form — the only way to pass :order-by
                         (d/q (assoc opts :query query :args args'))
                         (apply d/q query args')))))
                  (catch Exception _ ::raised)))
        fut (future (thunk))
        r (deref fut case-timeout-ms ::timeout)]
    (when (= r ::timeout)
      ;; The thread is CPU-bound and will not observe an interrupt, but the run
      ;; must proceed; the case is already reported as a divergence.
      (future-cancel fut))
    r))

(defn- wrap-db [db temporal]
  (case temporal
    :none db
    :as-of (d/as-of db (:max-tx db))
    :history (d/history db)))

;; ---------------------------------------------------------------------------
;; Third engine: the naive oracle (datahike.oracle)
;;
;; base-vs-planner has a structural blind spot — when both engines share a
;; wrong assumption they AGREE, and this spec passes. Measured on one extra
;; generator axis (rebinding a function output to an already-bound var): 47% of
;; cases wrong, and 19% wrong in a way NO two-engine comparison can see. The
;; oracle is a third implementation written for obviousness, so it breaks the
;; tie. It costs ~1.1 ms/case against the planner's ~7.8, i.e. it is the
;; cheapest of the three.

(def ^:private oracle-mode
  "strict — an oracle disagreement fails the build (the goal state).
   report — collect and print disagreements without failing. Use while a known
            shared-wrong class is still being fixed, so the axis stays covered
            instead of being switched off and forgotten.
   off    — skip the oracle entirely."
  (or (System/getenv "DATAHIKE_ORACLE") "strict"))

(def ^:private oracle-reports (atom []))

(def ^:private oracle-stats
  "How many generated cases the oracle actually compared, vs skipped as a shape
   it does not cover. An oracle that skips everything reports no disagreements,
   which reads exactly like a clean sweep — so this is asserted, not logged."
  (atom {:checked 0 :skipped 0}))

(defn- run-oracle [query db args]
  (let [thunk (fn []
                (try
                  (let [args' (into [db] (map (fn [a] (if (= ::db2 a) @test-db2 a))) args)]
                    (normalize (apply o/q query args')))
                  (catch clojure.lang.ExceptionInfo e
                    ;; A shape the oracle does not cover is a SKIP, never a
                    ;; mismatch — otherwise its gaps would masquerade as bugs.
                    (if (:oracle/unsupported (ex-data e)) ::unsupported ::raised))
                  (catch Exception _ ::raised)))
        fut (future (thunk))
        r (deref fut case-timeout-ms ::timeout)]
    (when (= r ::timeout) (future-cancel fut))
    r))

(def ^:private known-shared-wrong
  "Classes where BOTH engines are known to answer wrongly, so the oracle
   disagreeing is expected until the class is fixed.

   This is an ALLOWLIST, not a mute switch: a disagreement outside these
   classes still fails the build immediately, and `known-shared-wrong-is-still-
   needed` fails when an entry stops matching — so the change that fixes a
   class is forced to delete its entry rather than leave it accumulating."
  [{:id :output-var-rebind
    :why (str "an output binding whose target var is already bound must UNIFY "
              "(Datomic; and datahike already unifies for repeated vars inside "
              "one clause, #912/#913). Today get-else ignores the obligation on "
              "the planner and the base engine overwrites it; tuple bindings "
              "overwrite on both. Fixed by the binding-seam work — delete this "
              "entry then.")
    :match? (fn [spec] (boolean (:rebind? spec)))}])

(def ^:private oracle-known (atom {}))

(defn- check-oracle!
  "Compare the oracle against the engines' agreed answer. Returns true unless
   strict mode has found a real disagreement."
  [query args opts spec base planner oracle]
  (if (or (#{::unsupported ::timeout} oracle)
          ;; opts carries :order-by/:limit/:offset, which the oracle declines
          (seq opts)
          ;; when the engines already disagree, that failure is reported above
          ;; and is the one to fix first
          (not= base planner))
    (do (swap! oracle-stats update :skipped inc) true)
    (let [_ (swap! oracle-stats update :checked inc)
          agree? (= base oracle)
          known (when-not agree?
                  (first (filter (fn [e] ((:match? e) spec)) known-shared-wrong)))]
      (when-not agree?
        (swap! oracle-reports conj
               {:query query :args args :spec (select-keys spec [:dataset :temporal])
                :engines base :oracle oracle :known (:id known)
                ;; the verdict that matters: both engines agree AND are wrong
                :verdict (if known :known-shared-wrong :oracle-vs-both)}))
      (when known (swap! oracle-known update (:id known) (fnil inc 0)))
      (if (and known (not agree?))
        true
        (if (= "strict" oracle-mode)
          (do (is agree?
                  (str "both engines agree but the oracle disagrees — a shared "
                       "wrong assumption is invisible to differential testing\n"
                       "  query:   " (pr-str query) "\n"
                       "  args:    " (pr-str args) "\n"
                       "  engines: " (pr-str base) "\n"
                       "  oracle:  " (pr-str oracle)))
              agree?)
          true)))))

(defspec base-and-planner-agree-on-generated-queries
  {:num-tests num-cases :seed 1721160000042}
  (prop/for-all [spec gen-spec]
                (let [[query args opts] (build-query spec)
                      db (wrap-db (dataset-db (:dataset spec)) (:temporal spec))
                      base (run-engine true query db args opts)
                      planner (run-engine false query db args opts)
                      oracle (when (not= "off" oracle-mode)
                               (run-oracle query db args))
                      ;; The rebind axis violates ONE law (an output binding
                      ;; whose target is already bound must unify) and that law
                      ;; is broken in two directions at once: the planner
                      ;; ignores the obligation while the base engine
                      ;; overwrites it, so the SAME class shows up here as a
                      ;; planner-vs-base divergence and, where both overwrite,
                      ;; as an oracle-vs-both one. Both are allowlisted
                      ;; together and both disappear with the same fix — see
                      ;; known-shared-wrong.
                      known-rebind? (and (not= base planner)
                                         (:rebind? spec)
                                         (some (fn [e] ((:match? e) spec))
                                               known-shared-wrong))]
                  (when known-rebind?
                    (swap! oracle-known update :output-var-rebind (fnil inc 0)))
                  (if known-rebind?
                    true
                    (do
                      (is (= base planner)
                          (str "engines diverge on " (pr-str query)
                               " args " (pr-str args) " opts " (pr-str opts)
                               " temporal " (:temporal spec)
                               " dataset " (:dataset spec)
                               "\n  base:    " (pr-str base)
                               "\n  planner: " (pr-str planner)))
                      (and (= base planner)
                           (or (nil? oracle)
                               (check-oracle! query args opts spec base planner oracle))))))))

(deftest oracle-coverage-is-not-silent
  (testing "the oracle actually ran — a skip-everything oracle reports no
            disagreements, which is indistinguishable from a clean sweep"
    (let [{:keys [checked skipped]} @oracle-stats
          reports @oracle-reports]
      (if (= "off" oracle-mode)
        (is (zero? checked) "DATAHIKE_ORACLE=off must not run the oracle")
        (do
          ;; A file, not stdout/stderr: kaocha replaces both streams (and the
          ;; System/err field) before test namespaces load, replaying them only
          ;; on FAILURE — but these numbers are wanted on a passing run, and CI
          ;; needs to read them mechanically.
          (let [f (java.io.File. "target/oracle-stats.edn")]
            (.mkdirs (.getParentFile f))
            (spit f (pr-str {:mode oracle-mode :checked checked :skipped skipped
                             :known @oracle-known
                             :disagreements (mapv #(select-keys % [:verdict :known :query :engines :oracle])
                                                  (take 20 reports))})))
          (is (pos? checked)
              (str "the oracle compared " checked " cases and skipped " skipped
                   " — if checked is 0 the third engine is decorative")))))))

(deftest known-shared-wrong-is-still-needed
  (testing "every allowlisted both-engines-wrong class still reproduces"
    ;; An allowlist that outlives its bug is worse than no allowlist: it keeps
    ;; a whole class of divergence permanently unreported. So a class that has
    ;; stopped diverging FAILS here, and the fix is to delete the entry.
    (if (= "off" oracle-mode)
      (is (= "off" oracle-mode) "oracle disabled — allowlist not exercised")
      (doseq [{:keys [id why]} known-shared-wrong]
        (is (pos? (get @oracle-known id 0))
            (str "no generated case still diverges for known-wrong class " id
                 " — if it is fixed, DELETE the entry from known-shared-wrong "
                 "so the class is enforced again. Context: " why))))))
