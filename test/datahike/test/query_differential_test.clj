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

   CI budget: a FIXED seed and small case count keep this deterministic and
   cheap (~100 queries, each run twice against one small shared db — a few
   seconds). To fuzz more deeply out-of-band, raise the count locally, e.g.:
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
  (or (some-> (System/getenv "DATAHIKE_DIFF_CASES") parse-long) 100))

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
                          {:db/ident :friend :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}])
        (d/transact conn [{:db/id 100 :name "alice" :nick "al" :score 10 :tag [:red :blue] :friend 101}
                          {:db/id 101 :name "bob" :score 20 :tag [:blue]}
                          {:db/id 102 :name "carol" :nick "cc" :score 30 :friend 100}
                          {:db/id 103 :name "dave" :tag [:red]}
                          {:db/id 104 :name "eve" :score 20 :friend 103}
                          {:db/id 105 :name "frank" :nick "f" :tag [:green :red] :score 5}])
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
   :find      (gen/elements [:e :e+primary :e+modifier :primary+modifier
                             :coll-primary :agg-count :agg-min :agg-count-primary])))

(defn- build-query
  "Assemble a valid query + extra args from a spec. Returns [query args]."
  [{:keys [score? tag? friend? modifiers pred-const shuffle-seed shuffle?
           in-coll? rules multi use2? find]}]
  (let [;; :recursive/:mutual rule clauses walk :friend — force the pattern in
        friend? (or friend? (#{:recursive :mutual} rules))
        patterns (cond-> '[[?e :name ?n]]
                   score? (conj '[?e :score ?s])
                   tag? (conj '[?e :tag ?t])
                   friend? (conj '[?e :friend ?f] '[?f :name ?fn])
                   (not= :none rules) (conj (get rule-clause rules))
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
                  (and use2? (= :fn-body rules)) '?ru
                  (and use2? (#{:recursive :mutual} rules)) '?r
                  (and use2? (= :plain rules)) '?rn2
                  score? '?s tag? '?t friend? '?fn :else '?n)
        find-part (case find
                    :e ['?e]
                    :e+primary (vec (distinct ['?e primary]))
                    :e+modifier (vec (distinct ['?e (or mod-var primary)]))
                    :primary+modifier (vec (distinct [primary (or mod-var '?n)]))
                    :coll-primary [[primary '...]]
                    :agg-count [(list 'count '?e)]
                    :agg-min ['?e (list 'min (if score? '?s '?n))]
                    :agg-count-primary [primary (list 'count '?e)])
        ;; :in order must match arg order: $ [$2] [%] [coll]
        in-part (when (or in-coll? (not= :none rules) (not= :none multi))
                  (vec (concat '[$]
                               (when (not= :none multi) '[$2])
                               (when (not= :none rules) '[%])
                               (when in-coll? '[[?e ...]]))))
        args (vec (concat (when (not= :none multi) [::db2])
                          (when (not= :none rules) [(get rule-sets rules)])
                          (when in-coll? [[100 101 102 103 104 105]])))]
    [(vec (concat [:find] find-part
                  (when in-part (cons :in in-part))
                  [:where] clauses))
     args]))

(defn- normalize
  "Order-insensitive, duplicate-preserving comparison form: collection finds
   ([?x ...]) and aggregate rels may come back in engine-specific order."
  [r]
  (cond
    (set? r) (into #{} (map (fn [t] (if (sequential? t) (vec t) t))) r)
    (sequential? r) (frequencies r)
    :else r))

(defn- run-engine [disable? query db args]
  (try
    (binding [q/*disable-planner* disable?]
      (normalize (apply d/q query db
                        (map (fn [a] (if (= ::db2 a) @test-db2 a)) args))))
    (catch Exception _ ::raised)))

(defn- wrap-db [db temporal]
  (case temporal
    :none db
    :as-of (d/as-of db (:max-tx db))
    :history (d/history db)))

(defspec base-and-planner-agree-on-generated-queries
  {:num-tests num-cases :seed 1721160000042}
  (prop/for-all [spec gen-spec]
                (let [[query args] (build-query spec)
                      db (wrap-db @test-db (:temporal spec))
                      base (run-engine true query db args)
                      planner (run-engine false query db args)]
                  (is (= base planner)
                      (str "engines diverge on " (pr-str query)
                           " args " (pr-str args) " temporal " (:temporal spec)
                           "\n  base:    " (pr-str base)
                           "\n  planner: " (pr-str planner)))
                  (= base planner))))
