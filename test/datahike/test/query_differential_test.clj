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
   [clojure.test :refer [is]]
   [clojure.test.check.clojure-test :refer [defspec]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [datahike.api :as d]
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
                          ;; unique, so an :in binding can arrive as a LOOKUP REF
                          {:db/ident :uid :db/valueType :db.type/string
                           :db/unique :db.unique/identity :db/cardinality :db.cardinality/one}])
        (d/transact conn [{:db/id 100 :uid "u100" :name "alice" :nick "al" :score 10 :tag [:red :blue] :friend 101}
                          {:db/id 101 :uid "u101" :name "bob" :score 20 :tag [:blue]}
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

(def ^:private rule-sets
  {:plain     '[[(named ?e ?n) [?e :name ?n]]]
   :fn-body   '[[(upper-name ?e ?ru) [?e :name ?rn] [(clojure.string/upper-case ?rn) ?ru]]]
   :recursive '[[(reach ?a ?b) [?a :friend ?b]]
                [(reach ?a ?b) [?a :friend ?x] (reach ?x ?b)]]
   :mutual    '[[(ehop ?a ?b) [?a :friend ?b]]
                [(ehop ?a ?b) [?a :friend ?x] (ohop ?x ?b)]
                [(ohop ?a ?b) [?a :friend ?x] (ehop ?x ?b)]]
   :with-not  '[[(unred ?e) [?e :name ?rn] (not [?e :tag :red])]]})

(def ^:private rule-clause
  {:plain     '(named ?e ?rn2)
   :fn-body   '(upper-name ?e ?ru)
   :recursive '(reach ?e ?r)
   :mutual    '(ehop ?e ?r)
   :with-not  '(unred ?e)})

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
                              [1 (gen/return :with-not)]])
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
                             :consumer-only])))

(def ^:private rule-out-vars
  "The var each rule clause BINDS — nil for the unary rule, which binds none.
   Grounding that argument removes the var from the query, so `primary` must
   not pick it."
  {:plain '?rn2 :fn-body '?ru :recursive '?r :mutual '?r :with-not nil})

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
           in-scalar in-tuple? in-rel? in-lookup? rule-ground result-mod]}]
  (let [;; :recursive/:mutual rule clauses walk :friend — force the pattern in
        friend? (or friend? (#{:recursive :mutual} rules))
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
            :fn-upper      [['[(clojure.string/upper-case ?n) ?u]] '?u]
            :fn-chain      [['[(clojure.string/upper-case ?n) ?u]
                             '[(clojure.string/lower-case ?u) ?l]] '?l]
            :get-else      [['[(get-else $ ?e :nick "none") ?v]] '?v]
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
                    :consumer-only [(if friend? '?fn '?n)])
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

(defn- run-engine [disable? query db args opts]
  (try
    (binding [q/*disable-planner* disable?]
      (let [args' (into [db] (map (fn [a] (if (= ::db2 a) @test-db2 a))) args)]
        (normalize
         (if (seq opts)
           ;; map form — the only way to pass :order-by
           (d/q (assoc opts :query query :args args'))
           (apply d/q query args')))))
    (catch Exception _ ::raised)))

(defn- wrap-db [db temporal]
  (case temporal
    :none db
    :as-of (d/as-of db (:max-tx db))
    :history (d/history db)))

(defspec base-and-planner-agree-on-generated-queries
  {:num-tests num-cases :seed 1721160000042}
  (prop/for-all [spec gen-spec]
                (let [[query args opts] (build-query spec)
                      db (wrap-db @test-db (:temporal spec))
                      base (run-engine true query db args opts)
                      planner (run-engine false query db args opts)]
                  (is (= base planner)
                      (str "engines diverge on " (pr-str query)
                           " args " (pr-str args) " opts " (pr-str opts)
                           " temporal " (:temporal spec)
                           "\n  base:    " (pr-str base)
                           "\n  planner: " (pr-str planner)))
                  (= base planner))))
