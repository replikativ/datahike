(ns datahike.test.query-engine-parity-test
  "Pins engine-parity contracts between the planner and the base (relational)
   engine that have historically drifted apart silently.

   Contract 1 — unresolvable clauses RAISE on both engines.
   The base engine's clause loop (tools/resolve-clauses) raises \"Cannot
   resolve any more clauses\" when no clause can make progress. The planner
   executes its plan in a single linear pass: bind-by-fn's nil (retry-later)
   signal has no retry queue to land in, so it must raise the same error
   (execute/bind-by-fn-strict). History of drifting here: #814 added an
   `(or next-ctx ctx)` guard that silently DROPPED the clause; #815 removed
   it as \"structurally unreachable\" — a premise the ordering fallbacks
   (order-plan-ops greedy/:force branches, which emit unrunnable ops by
   design) never satisfied — after which the nil WIPED the whole context.
   Both states produced silent wrong results instead of an error. This test
   makes the invariant executable so the next refactor can't remove it on a
   prose argument.

   Contract 2 — replan must not leak nested-query-literal vars.
   plan/replan seeds re-ordering with the executed prefix's bound vars; using
   the recursive :vars walk would mark a subquery literal's ?p as bound in
   the outer scope (the same leak fixed in order-plan-ops/op-available-vars),
   letting a consumer of the real ?p be ordered before its producer."
  (:require
   [clojure.test :refer [deftest is testing]]
   [datahike.api :as d]
   [datahike.query :as q]
   [datahike.query.plan :as plan]))

(defn- fresh-conn []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write}]
    (d/create-database cfg)
    (d/connect cfg)))

;; The result cache is keyed by [query args db-snapshot] — the engine is NOT
;; part of the key, so a cached base-engine answer would be served straight
;; back to the planner call and every parity assertion below would compare a
;; result against itself. Both helpers bypass it.
(defn- planner [query & args]
  (binding [q/*disable-planner* false
            q/*query-result-cache?* false]
    (apply d/q query args)))

(defn- base [query & args]
  (binding [q/*disable-planner* true
            q/*query-result-cache?* false]
    (apply d/q query args)))

(defn- raised-cannot-resolve? [f]
  (try (f) false
       (catch Exception e
         (boolean (re-find #"Cannot resolve any more clauses"
                           (str (.getMessage e)))))))

(deftest unresolvable-clause-raises-on-both-engines
  (testing "a fn clause whose input var can never be bound raises identically"
    (let [conn (fresh-conn)
          _    (d/transact conn [{:db/ident :v :db/valueType :db.type/long
                                  :db/cardinality :db.cardinality/one}])
          db   (d/db conn)
          query '[:find ?n . :in $ :where [(count ?never) ?n]]]
      (is (raised-cannot-resolve? #(base query db))
          "base engine raises")
      (is (raised-cannot-resolve? #(planner query db))
          "planner raises the same error (was: silent nil via ctx wipe)"))))

(deftest unresolvable-chained-clause-raises-on-both-engines
  (testing "an unresolvable clause downstream of resolvable ones also raises"
    (let [conn (fresh-conn)
          _    (d/transact conn [{:db/ident :person/name :db/valueType :db.type/string
                                  :db/cardinality :db.cardinality/one}])
          _    (d/transact conn [{:person/name "alice"}])
          db   (d/db conn)
          query '[:find ?n . :in $ :where
                  [?e :person/name ?p]
                  [(count ?nowhere) ?n]]]
      (is (raised-cannot-resolve? #(base query db)))
      (is (raised-cannot-resolve? #(planner query db))))))

(deftest get-else-quoted-default-parity
  (testing "a (quote x) get-else default unwraps to its constant on BOTH engines"
    ;; The base path unwraps quote args in -call-fn; the planner's fused
    ;; optional-merge paths plant :default-value into tuples VERBATIM, so the
    ;; unwrap must happen at LOptionalScan construction (logical.cljc) — found
    ;; by adversarial review: base returned (:a :b) while the planner returned
    ;; the literal (quote (:a :b)).
    (let [conn (fresh-conn)
          _    (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                                  :db/cardinality :db.cardinality/one}
                                 {:db/ident :nick :db/valueType :db.type/string
                                  :db/cardinality :db.cardinality/one}])
          _    (d/transact conn [{:db/id 100 :name "alice" :nick "al"}
                                 {:db/id 101 :name "bob"}])
          db   (d/db conn)
          query '[:find ?e ?v :in $ :where
                  [?e :name ?n]
                  [(get-else $ ?e :nick (quote (:a :b))) ?v]]]
      (is (= #{[100 "al"] [101 '(:a :b)]} (base query db)))
      (is (= (base query db) (planner query db))
          "planner must plant the unwrapped constant, not the (quote …) literal"))))

(deftest get-else-named-source-parity
  (testing "get-else over a NAMED source keeps left-outer semantics (issue #884)"
    ;; Pre-fix: the source mismatch prevented the optional scan from fusing
    ;; with the $data name-scan; it then ran standalone on the plain scan path
    ;; (an inner join) and silently dropped the entity the default is FOR.
    ;; LOptionalScan now carries the source (the scans fuse again), and a
    ;; genuinely standalone optional scan routes through bind-by-fn naming
    ;; the op's actual source instead of a hardcoded '$.
    (let [conn (fresh-conn)
          _    (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                                  :db/cardinality :db.cardinality/one}
                                 {:db/ident :nick :db/valueType :db.type/string
                                  :db/cardinality :db.cardinality/one}])
          _    (d/transact conn [{:db/id 100 :name "alice" :nick "al"}
                                 {:db/id 101 :name "bob"}])
          db   (d/db conn)
          named '[:find ?e ?v :in $data :where
                  [$data ?e :name ?n]
                  [(get-else $data ?e :nick "none") ?v]]
          ;; standalone shape: e-var comes from a collection binding, so the
          ;; optional scan has no scan to fuse with on ANY source
          standalone '[:find ?e ?v :in $ [?e ...] :where
                       [(get-else $ ?e :nick "none") ?v]]]
      (is (= #{[100 "al"] [101 "none"]} (base named db)))
      (is (= (base named db) (planner named db))
          "planner must not drop the defaulted row on a named source (BUG: returned only the entity that has :nick)")
      (is (= #{[100 "al"] [101 "none"]} (base standalone db [100 101])))
      (is (= (base standalone db [100 101]) (planner standalone db [100 101]))
          "standalone optional scan keeps left-outer semantics"))))

(deftest card-many-merge-with-get-else-parity
  (testing "a card-many attribute in the same group as get-else keeps ALL its
            values, and projecting the card-many var away doesn't leave
            duplicate tuples (both found by the generative differential test;
            both pre-existing)"
    ;; Bug 1: per-cursor-merge (selected whenever a group had an optional
    ;; merge) does a single lookupGE per merge — card-ONE semantics — so the
    ;; card-many :tag merge silently yielded only its first value. Mixed
    ;; groups now route to card-many-merge, which learned optional defaults.
    ;; Bug 2: with [?e :tag ?t] as the DRIVING scan and ?t projected away,
    ;; has-card-many-dupes? missed the duplicate projections and took the
    ;; no-dedup QueryResult path → a "set" containing [e] twice.
    (let [conn (fresh-conn)
          _    (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                                  :db/cardinality :db.cardinality/one}
                                 {:db/ident :nick :db/valueType :db.type/string
                                  :db/cardinality :db.cardinality/one}
                                 {:db/ident :tag :db/valueType :db.type/keyword
                                  :db/cardinality :db.cardinality/many}])
          _    (d/transact conn [{:db/id 100 :name "alice" :nick "al" :tag [:red :blue]}
                                 {:db/id 101 :name "bob" :tag [:blue]}])
          db   (d/db conn)
          values '[:find ?e ?t :where [?e :name ?n] [?e :tag ?t]
                   [(get-else $ ?e :nick "none") ?v]]
          projected '[:find ?e :where [?e :name ?n] [?e :tag ?t]
                      [(get-else $ ?e :nick "none") ?v]]]
      (is (= #{[100 :red] [100 :blue] [101 :blue]} (base values db)))
      (is (= (base values db) (planner values db))
          "planner must keep every card-many value (BUG: lost [100 :red])")
      (is (= #{[100] [101]} (base projected db)))
      (is (= (base projected db) (planner projected db))
          "planner must dedup projected tuples (BUG: returned [100] twice)")
      (is (= 2 (count (planner projected db)))
          "no duplicate tuples inside the result set"))))

(defn- multiset
  "Aggregate results come back as vectors whose group order is engine-specific;
   compare them as multisets."
  [r]
  (if (sequential? r) (frequencies (map vec r)) r))

(deftest sorted-merge-card-many-driver-parity
  (testing "a card-many attribute promoted to DRIVING scan by clause order
            keeps all its values (sorted-merge advanced per entity and emitted
            only the first value; found by the generative differential test
            under clause-order permutation)"
    (let [conn (fresh-conn)
          _ (d/transact conn [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
                              {:db/ident :nick :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
                              {:db/ident :score :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                              {:db/ident :tag :db/valueType :db.type/keyword :db/cardinality :db.cardinality/many}
                              {:db/ident :friend :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}])
          _ (d/transact conn [{:db/id 100 :name "alice" :nick "al" :score 10 :tag [:red :blue] :friend 101}
                              {:db/id 101 :name "bob" :score 20 :tag [:blue]}
                              {:db/id 102 :name "carol" :nick "cc" :score 30 :friend 100}
                              {:db/id 103 :name "dave" :tag [:red]}
                              {:db/id 104 :name "eve" :score 20 :friend 103}
                              {:db/id 105 :name "frank" :nick "f" :tag [:green :red] :score 5}])
          db (d/db conn)
          ;; the fn/pred clause FIRST is what promotes [?e :tag ?t] to driver
          queries ['[:find ?e ?t :where [(some? ?n)] [?e :tag ?t] [?e :name ?n]]
                   '[:find ?e ?t :where [(clojure.string/upper-case ?n) ?u] [?e :tag ?t] [?e :name ?n]]
                   '[:find ?t ?u :where [(clojure.string/upper-case ?n) ?u] [?e :tag ?t] [?e :name ?n]]]]
      (doseq [query queries]
        (is (= (base query db) (planner query db))
            (str "planner must not truncate card-many driving scans: " (pr-str query)))))))

(deftest columnar-aggregate-parity
  (testing "the columnar aggregate path deduplicates the projected tuple space
            and never returns argmin row indices (both found by the generative
            differential test)"
    (let [conn (fresh-conn)
          _ (d/transact conn [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
                              {:db/ident :score :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                              {:db/ident :tag :db/valueType :db.type/keyword :db/cardinality :db.cardinality/many}])
          _ (d/transact conn [{:db/id 100 :name "alice" :score 10 :tag [:red :blue]}
                              {:db/id 101 :name "bob" :score 25 :tag [:blue]}
                              {:db/id 103 :name "dave" :tag [:red]}
                              {:db/id 105 :name "frank" :score 5 :tag [:green :red]}])
          db (d/db conn)]
      (doseq [query ['[:find (count ?e) :where [?e :name ?n] [?e :tag ?t]]
                     '[:find (count ?e) :where [?e :name ?n] (not [?e :tag :red])]
                     '[:find ?s (count ?e) :where [?e :name ?n] [?e :score ?s] [?e :tag ?t]]
                     ;; string min: the columnar engine returned ROW INDICES —
                     ;; now falls through to the (correct) relation path
                     '[:find ?e (min ?n) :where [?e :name ?n]]
                     '[:find ?s (sum ?s) :where [?e :score ?s] [?e :tag ?t]]]]
        (is (= (multiset (base query db)) (multiset (planner query db)))
            (str "aggregate divergence on " (pr-str query)))))))

(deftest replan-seed-does-not-leak-literal-vars
  (testing "replan orders a consumer of the real ?p after ?p's producer, even
            when an executed nested-q op mentions ?p inside its query literal"
    (let [conn (fresh-conn)
          _    (d/transact conn [{:db/ident :g/account :db/valueType :db.type/ref
                                  :db/cardinality :db.cardinality/one}
                                 {:db/ident :person/name :db/valueType :db.type/string
                                  :db/cardinality :db.cardinality/one}])
          _    (d/transact conn [{:person/name "alice"}])
          db   (d/db conn)
          query '{:find [?u ?n]
                  :in [$]
                  :where [[(datahike.api/q [:find ?p :in $x :where [$x ?p :g/account _]] $) ?v]
                          [(count ?v) ?n]
                          [?e :person/name ?p]
                          [(clojure.string/upper-case ?p) ?u]]}
          plan0 (#'q/get-or-create-plan db (:where query) #{} nil {})
          ;; Simulate a mid-execution replan right after the nested-q op
          ;; (index 0 in plan order: it is the only unconditionally-runnable op).
          replanned (plan/replan plan0 0 1 db)
          ops (:ops replanned)
          idx-of (fn [pred] (first (keep-indexed (fn [i op] (when (pred op) i)) ops)))
          scan-idx (idx-of (fn [op] (#{:pattern-scan :entity-group} (:op op))))
          upper-idx (idx-of (fn [op] (and (= :function (:op op))
                                          (= 'clojure.string/upper-case (:fn-sym op)))))]
      (is (some? scan-idx) "plan retains the :person/name scan")
      (is (some? upper-idx) "plan retains the upper-case fn op")
      (is (< scan-idx upper-idx)
          "consumer of the real ?p is ordered after ?p's producer (leaked
           literal ?p in the replan seed would allow the reverse)")
      ;; And end-to-end: the full query stays correct on both engines.
      (is (= (base query db) (planner query db) #{["ALICE" 0]})))))

;; ---------------------------------------------------------------------------
;; Contract 3 — a rule head var that no branch body binds takes the caller's
;; value on both engines (#897).
;;
;; Threading a caller-supplied parameter through a recursion is common:
;;
;;   [(reachable ?anchor ?eps ?n) ...base case, never mentions ?eps...]
;;   [(reachable ?anchor ?eps ?o) ... [(contains? ?eps ?ep)]
;;                                (reachable ?anchor ?eps ?s)]
;;
;; ?eps is not range-restricted — the base body cannot produce it, so its only
;; possible value is the one the call site passed. The base engine gets this for
;; free by renaming head vars to the call args. The planner renames to the
;; rule's own head vars (so a constant call-arg can be filtered after the
;; fixpoint instead of restricting the accumulator), which left ?eps bound by
;; nobody: the branch relation came back without that column and the fixpoint's
;; dedup step NPE'd on the missing :attrs entry.

(defn- triple-conn
  "Anchor/edge graph as (subject, predicate, object) triples:
   root -isa-> Anchor, root -link-> b -link-> c -link-> d -link-> b (cycle),
   c -skip-> z, and a disconnected q -isa-> Other, q -link-> r."
  []
  (let [conn (fresh-conn)]
    (d/transact conn (for [a [:triple/s :triple/p :triple/o]]
                       {:db/ident a
                        :db/valueType :db.type/string
                        :db/cardinality :db.cardinality/one}))
    (d/transact conn [{:triple/s "root" :triple/p "isa" :triple/o "Anchor"}
                      {:triple/s "root" :triple/p "link" :triple/o "b"}
                      {:triple/s "b" :triple/p "link" :triple/o "c"}
                      {:triple/s "c" :triple/p "link" :triple/o "d"}
                      {:triple/s "d" :triple/p "link" :triple/o "b"}
                      {:triple/s "c" :triple/p "skip" :triple/o "z"}
                      {:triple/s "q" :triple/p "isa" :triple/o "Other"}
                      {:triple/s "q" :triple/p "link" :triple/o "r"}])
    conn))

(def ^:private reachable-rules
  "Right-recursive reachability: anchored start, edge kinds passed in by the
   caller and never bound by a body."
  '[[(reachable ?anchor ?eps ?n)
     [?e :triple/s ?n] [?e :triple/p "isa"] [?e :triple/o ?anchor]]
    [(reachable ?anchor ?eps ?o)
     [?e :triple/s ?s] [?e :triple/p ?ep] [?e :triple/o ?o]
     [(contains? ?eps ?ep)]
     (reachable ?anchor ?eps ?s)]])

(deftest unbound-head-var-takes-caller-value-on-both-engines
  (let [conn (triple-conn)
        db (d/db conn)]
    (testing "the reported repro: ground anchor, edge set from :in"
      (let [query '[:find ?n :in $ % ?anchor ?eps :where (reachable ?anchor ?eps ?n)]]
        (is (= (base query db reachable-rules "Anchor" #{"link"})
               (planner query db reachable-rules "Anchor" #{"link"})
               #{["root"] ["b"] ["c"] ["d"]})
            "cycle terminates and every reachable node is found")))

    (testing "the call site names the parameter differently than the rule does"
      ;; The planner used to get away with this only when the two names
      ;; happened to coincide and the value sat in an outer relation.
      (let [query '[:find ?n :in $ % ?anchor ?preds :where (reachable ?anchor ?preds ?n)]]
        (is (= (base query db reachable-rules "Anchor" #{"link"})
               (planner query db reachable-rules "Anchor" #{"link"})
               #{["root"] ["b"] ["c"] ["d"]}))))

    (testing "the parameter selects which edges are followed"
      (let [query '[:find ?n :in $ % ?anchor ?eps :where (reachable ?anchor ?eps ?n)]]
        (is (= (base query db reachable-rules "Anchor" #{"link" "skip"})
               (planner query db reachable-rules "Anchor" #{"link" "skip"})
               #{["root"] ["b"] ["c"] ["d"] ["z"]})
            "adding :skip to the passed-in set pulls in z")
        (is (= (base query db reachable-rules "Anchor" #{})
               (planner query db reachable-rules "Anchor" #{})
               #{["root"]})
            "an empty set follows no edges at all")))

    (testing "the anchor is free too — one row per anchor/node pair"
      (let [query '[:find ?a ?n :in $ % ?eps :where (reachable ?a ?eps ?n)]]
        (is (= (base query db reachable-rules #{"link"})
               (planner query db reachable-rules #{"link"})
               #{["Anchor" "root"] ["Anchor" "b"] ["Anchor" "c"] ["Anchor" "d"]
                 ["Other" "q"] ["Other" "r"]}))))

    (testing "the parameter is multi-valued, bound by an outer clause"
      (let [rules '[[(reach ?anchor ?pred ?n)
                     [?e :triple/s ?n] [?e :triple/p "isa"] [?e :triple/o ?anchor]]
                    [(reach ?anchor ?pred ?o)
                     [?e :triple/s ?s] [?e :triple/p ?pred] [?e :triple/o ?o]
                     (reach ?anchor ?pred ?s)]]
            query '[:find ?n :in $ % ?anchor
                    :where [_ :triple/p ?p] [(not= ?p "isa")] (reach ?anchor ?p ?n)]]
        ;; Each ?p value is followed on its own: "link" walks root→b→c→d,
        ;; "skip" only ever reaches the anchored root (no skip edge leaves it),
        ;; so z stays out — the parameter must not be smeared across values.
        (is (= (base query db rules "Anchor")
               (planner query db rules "Anchor")
               #{["root"] ["b"] ["c"] ["d"]}))))

    (testing "the base body filters on the parameter without binding it"
      (let [rules '[[(reach ?anchor ?eps ?n)
                     [?e :triple/s ?n] [?e :triple/p "isa"] [?e :triple/o ?anchor]
                     [(contains? ?eps "link")]]
                    [(reach ?anchor ?eps ?o)
                     [?e :triple/s ?s] [?e :triple/p ?ep] [?e :triple/o ?o]
                     [(contains? ?eps ?ep)]
                     (reach ?anchor ?eps ?s)]]
            query '[:find ?n :in $ % ?anchor ?eps :where (reach ?anchor ?eps ?n)]]
        (is (= (base query db rules "Anchor" #{"link"})
               (planner query db rules "Anchor" #{"link"})
               #{["root"] ["b"] ["c"] ["d"]})
            "the predicate sees the caller's value inside the base branch")
        (is (= (base query db rules "Anchor" #{"other"})
               (planner query db rules "Anchor" #{"other"})
               #{})
            "and can reject the base case outright")))

    (testing "nothing binds the parameter on either side — the rule still runs"
      ;; No caller binding to take, so the planner hands the whole rule to the
      ;; relational engine rather than inventing a value.
      (let [rules '[[(loose ?anchor ?x ?n)
                     [?e :triple/s ?n] [?e :triple/p "isa"] [?e :triple/o ?anchor]]
                    [(loose ?anchor ?x ?o)
                     [?e :triple/s ?s] [?e :triple/p "link"] [?e :triple/o ?o]
                     (loose ?anchor ?x ?s)]]
            query '[:find ?n :in $ % ?anchor :where (loose ?anchor ?whatever ?n)]]
        (is (= (base query db rules "Anchor")
               (planner query db rules "Anchor")
               #{["root"] ["b"] ["c"] ["d"]}))))

    (d/release conn)))

(deftest unbound-head-var-in-mutual-recursion
  ;; Both rules of the SCC thread ?eps; only the called rule's base branch
  ;; leaves it unbound, so the planner can still resolve it from the call args.
  (let [conn (triple-conn)
        db (d/db conn)
        rules '[[(even-step ?anchor ?eps ?n)
                 [?e :triple/s ?n] [?e :triple/p "isa"] [?e :triple/o ?anchor]]
                [(even-step ?anchor ?eps ?o)
                 [?e :triple/s ?s] [?e :triple/p ?ep] [?e :triple/o ?o]
                 [(contains? ?eps ?ep)]
                 (odd-step ?anchor ?eps ?s)]
                [(odd-step ?anchor ?eps ?o)
                 [?e :triple/s ?s] [?e :triple/p ?ep] [?e :triple/o ?o]
                 [(contains? ?eps ?ep)]
                 (even-step ?anchor ?eps ?s)]]
        query '[:find ?n :in $ % ?anchor ?eps :where (even-step ?anchor ?eps ?n)]]
    (is (= #{["root"] ["b"] ["c"] ["d"]}
           (planner query db rules "Anchor" #{"link"})))
    (d/release conn)))
