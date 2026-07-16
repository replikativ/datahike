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

(def ^:private gen-spec
  (gen/hash-map
   :score?   gen/boolean                                     ;; add [?e :score ?s]
   :tag?     gen/boolean                                     ;; add [?e :tag ?t]
   :friend?  gen/boolean                                     ;; add [?e :friend ?f] [?f :name ?fn]
   :modifier (gen/elements [:none :pred-lt :pred-gt :fn-upper
                            :get-else :not-tag :or-tag :fn-chain])
   :pred-const (gen/choose 0 35)
   :find     (gen/elements [:e :e+primary :e+modifier :primary+modifier])))

(defn- build-query
  "Assemble a valid query from a spec. Returns [query-vector]."
  [{:keys [score? tag? friend? modifier pred-const find]}]
  (let [patterns (cond-> '[[?e :name ?n]]
                   score? (conj '[?e :score ?s])
                   tag? (conj '[?e :tag ?t])
                   friend? (conj '[?e :friend ?f] '[?f :name ?fn]))
        ;; modifiers that need ?s degrade to :fn-upper when score? is absent
        modifier (if (and (#{:pred-lt :pred-gt} modifier) (not score?))
                   :fn-upper
                   modifier)
        [mod-clauses mod-var]
        (case modifier
          :none      [[] nil]
          :pred-lt   [[(list 'vector (list '< '?s pred-const))] nil]
          :pred-gt   [[(list 'vector (list '> '?s pred-const))] nil]
          :fn-upper  [['[(clojure.string/upper-case ?n) ?u]] '?u]
          :get-else  [['[(get-else $ ?e :nick "none") ?v]] '?v]
          :not-tag   [['(not [?e :tag :red])] nil]
          :or-tag    [['(or [?e :tag :red] [?e :tag :blue])] nil]
          :fn-chain  [['[(clojure.string/upper-case ?n) ?u]
                       '[(clojure.string/lower-case ?u) ?l]] '?l])
        ;; predicate clauses were built as (vector (...)) markers — realize them
        mod-clauses (mapv (fn [c] (if (and (seq? c) (= 'vector (first c)))
                                    [(second c)]
                                    c))
                          mod-clauses)
        primary (cond score? '?s tag? '?t friend? '?fn :else '?n)
        find-vars (distinct
                   (case find
                     :e ['?e]
                     :e+primary ['?e primary]
                     :e+modifier ['?e (or mod-var primary)]
                     :primary+modifier [primary (or mod-var '?n)]))]
    (vec (concat [:find] find-vars [:where] patterns mod-clauses))))

(defn- run-engine [disable? query db]
  (try
    (binding [q/*disable-planner* disable?]
      (d/q query db))
    (catch Exception _ ::raised)))

(defspec base-and-planner-agree-on-generated-queries
  {:num-tests num-cases :seed 1721160000042}
  (prop/for-all [spec gen-spec]
                (let [query (build-query spec)
                      base (run-engine true query @test-db)
                      planner (run-engine false query @test-db)]
                  (is (= base planner)
                      (str "engines diverge on " (pr-str query)
                           "\n  base:    " (pr-str base)
                           "\n  planner: " (pr-str planner)))
                  (= base planner))))
