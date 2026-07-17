(ns datahike.test.query-shape-corpus-test
  "Enumerated engine-parity corpus: every query here runs on BOTH engines and
   must agree. These shapes come from the adversarial reviews of PR #883 —
   each is here because it found a bug, nearly found one, or guards a shape
   the planner handles through a special path (or/not/rules around fn chains,
   quoted constants, named sources, temporal wrappers). The generative
   companion (query-differential-test) explores the composition space AROUND
   these; this corpus pins the exact known-tricky points deterministically.

   Adding a shape costs one line and ~2ms of CI — when a planner bug is fixed,
   its minimal repro belongs either here or (with an explanatory docstring) in
   query-engine-parity-test."
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :refer [deftest is testing]])
   [datahike.api :as d]
   [datahike.db :as ddb]
   [datahike.query :as q]
   [datahike.test.core-test]))

(def ^:private db
  ;; Store-less in-memory db: planner-eligible, runs on both platforms
  ;; synchronously; keep-history? so the temporal shapes below work.
  (delay
    (d/db-with (ddb/empty-db {:tag {:db/cardinality :db.cardinality/many}}
                             {:keep-history? true})
               [{:db/id 100 :name "alice" :nick "al" :age 30 :tag [:red :blue]}
                {:db/id 101 :name "bob" :age 40 :tag [:blue]}])))

(def ^:private rules
  '[[(named ?e ?n) [?e :name ?n]]
    [(adult ?e) [?e :age ?a] [(< 35 ?a)]]])

(def ^:private corpus
  "[label query args] — args beyond the db; :% marks the rules position."
  [;; fn/pred chains behind logical operators
   [:or-join-fn '[:find ?e ?u :where
                  (or-join [?e ?u]
                           (and [?e :name ?n] [(clojure.string/upper-case ?n) ?u])
                           (and [?e :name ?n] [(str ?n "!") ?u]))] []]
   [:or-pred '[:find ?e ?s :where [?e :age ?a] [(str ?a) ?s]
               (or [?e :tag :red] [?e :tag :blue])] []]
   [:not-then-fn '[:find ?n ?u :where [?e :name ?n] (not [?e :nick "zz"])
                   [(clojure.string/upper-case ?n) ?u]] []]
   [:not-join-then-fn '[:find ?n ?ok :where [?e :name ?n]
                        (not-join [?e] [?e :nick _])
                        [(str ?n "-ok") ?ok]] []]
   [:rule-then-fn '[:find ?u :in $ % :where (adult ?e) (named ?e ?n)
                    [(clojure.string/upper-case ?n) ?u]] [:%]]
   ;; quoted and literal constants in fn/pred args
   [:ground-quote '[:find ?t :where [(ground (quote (:a :b))) [?t ...]]] []]
   [:pred-quote-set '[:find ?n :where [?e :name ?n]
                      [(contains? (quote #{"alice"}) ?n)]] []]
   [:vec-arg '[:find ?n ?c :where [?e :name ?n] [(count [:x :y]) ?c]] []]
   ;; get-else family (see also query-engine-parity-test for the bug pins)
   [:get-else-kw '[:find ?e ?v :where [?e :name ?n]
                   [(get-else $ ?e :nick :none) ?v]] []]
   [:get-else-after-not '[:find ?e ?v :where [?e :name ?n] (not [?e :tag :green])
                          [(get-else $ ?e :nick "none") ?v]] []]
   ;; card-many interactions
   [:card-many-two-vals '[:find ?e ?t :where [?e :tag ?t] [?e :age ?a]] []]
   [:card-many-projected '[:find ?a :where [?e :tag ?t] [?e :age ?a]] []]
   ;; scalar / collection find shapes over fn outputs
   [:scalar-fn '[:find ?u . :where [100 :name ?n] [(clojure.string/upper-case ?n) ?u]] []]
   [:coll-fn '[:find [?u ...] :where [?e :name ?n] [(clojure.string/upper-case ?n) ?u]] []]])

(defn- run-both [query args]
  (let [args (map (fn [a] (if (= :% a) rules a)) args)
        run (fn [disable?]
              (try (binding [q/*disable-planner* disable?]
                     (apply d/q query @db args))
                   (catch #?(:clj Exception :cljs :default) _ ::raised)))]
    [(run true) (run false)]))

(deftest corpus-shapes-agree-across-engines
  (doseq [[label query args] corpus]
    (testing (str label)
      (let [[base planner] (run-both query args)]
        (is (= base planner)
            (str label ": engines diverge on " (pr-str query)
                 "\n  base:    " (pr-str base)
                 "\n  planner: " (pr-str planner)))))))

(deftest corpus-temporal-shapes-agree
  (testing "as-of and history wrappers around fn/get-else shapes"
    (let [asof (d/as-of @db (:max-tx @db))
          queries ['[:find ?e ?v :where [?e :name ?n] [(get-else $ ?e :nick "none") ?v]]
                   '[:find ?n ?u :where [?e :name ?n] [(clojure.string/upper-case ?n) ?u]]]]
      (doseq [query queries]
        (let [run (fn [disable? d*] (try (binding [q/*disable-planner* disable?] (d/q query d*))
                                         (catch #?(:clj Exception :cljs :default) _ ::raised)))]
          (is (= (run true asof) (run false asof))
              (str "as-of divergence on " (pr-str query))))))))
