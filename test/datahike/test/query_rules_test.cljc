(ns datahike.test.query-rules-test
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [are is deftest testing]]
      :clj  [clojure.test :as t :refer        [are is deftest testing]])
   [clojure.core.async :refer [<!]]
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.query :as dq]
   [datahike.test.utils :as du]
   [datahike.test.async #?(:clj :refer :cljs :refer-macros) [deftest-async]]))

#?(:cljs (def Throwable js/Error))

(deftest test-rules
  (let [db [[5 :follow 3]
            [1 :follow 2] [2 :follow 3] [3 :follow 4] [4 :follow 6]
            [2         :follow           4]]]
    (is (= (d/q '[:find  ?e1 ?e2
                  :in    $ %
                  :where (follow ?e1 ?e2)]
                db
                '[[(follow ?x ?y)
                   [?x :follow ?y]]])
           #{[1 2] [2 3] [3 4] [2 4] [5 3] [4 6]}))

    (testing "Joining regular clauses with rule"
      (is (= (d/q '[:find ?y ?x
                    :in $ %
                    :where [_ _ ?x]
                    (rule ?x ?y)
                    [(even? ?x)]]
                  db
                  '[[(rule ?a ?b)
                     [?a :follow ?b]]])
             #{[3 2] [6 4] [4 2]})))

    (testing "Rule context is isolated from outer context"
      (is (= (d/q '[:find ?x
                    :in $ %
                    :where [?e _ _]
                    (rule ?x)]
                  db
                  '[[(rule ?e)
                     [_ ?e _]]])
             #{[:follow]})))

    (testing "Rule with branches"
      (is (= (d/q '[:find  ?e2
                    :in    $ ?e1 %
                    :where (follow ?e1 ?e2)]
                  db
                  1
                  '[[(follow ?e2 ?e1)
                     [?e2 :follow ?e1]]
                    [(follow ?e2 ?e1)
                     [?e2 :follow ?t]
                     [?t  :follow ?e1]]])
             #{[2] [3] [4]})))

    (testing "Recursive rules"
      (is (= (d/q '[:find  ?e2
                    :in    $ ?e1 %
                    :where (follow ?e1 ?e2)]
                  db
                  1
                  '[[(follow ?e1 ?e2)
                     [?e1 :follow ?e2]]
                    [(follow ?e1 ?e2)
                     [?e1 :follow ?t]
                     (follow ?t ?e2)]])
             #{[2] [3] [4] [6]}))

      (is (= (d/q '[:find ?e1 ?e2
                    :in $ %
                    :where (follow ?e1 ?e2)]
                  [[1 :follow 2] [2 :follow 3]]
                  '[[(follow ?e1 ?e2)
                     [?e1 :follow ?e2]]
                    [(follow ?e1 ?e2)
                     (follow ?e2 ?e1)]])
             #{[1 2] [2 3] [2 1] [3 2]}))

      (is (= (d/q '[:find ?e1 ?e2
                    :in $ %
                    :where (follow ?e1 ?e2)]
                  [[1 :follow 2] [2 :follow 3] [3 :follow 1]]
                  '[[(follow ?e1 ?e2)
                     [?e1 :follow ?e2]]
                    [(follow ?e1 ?e2)
                     (follow ?e2 ?e1)]])
             #{[1 2] [2 3] [3 1] [2 1] [3 2] [1 3]})))

    (testing "Mutually recursive rules"
      (is (= (d/q '[:find  ?e1 ?e2
                    :in    $ %
                    :where (f1 ?e1 ?e2)]
                  [[0 :f1 1]
                   [1 :f2 2]
                   [2 :f1 3]
                   [3 :f2 4]
                   [4 :f1 5]
                   [5 :f2 6]]
                  '[[(f1 ?e1 ?e2)
                     [?e1 :f1 ?e2]]
                    [(f1 ?e1 ?e2)
                     [?t :f1 ?e2]
                     (f2 ?e1 ?t)]
                    [(f2 ?e1 ?e2)
                     [?e1 :f2 ?e2]]
                    [(f2 ?e1 ?e2)
                     [?t :f2 ?e2]
                     (f1 ?e1 ?t)]])
             #{[0 1] [0 3] [0 5]
               [1 3] [1 5]
               [2 3] [2 5]
               [3 5]
               [4 5]})))

    (testing "Passing ins to rule"
      (is (= (d/q '[:find ?x ?y
                    :in $ % ?even
                    :where
                    (match ?even ?x ?y)]
                  db
                  '[[(match ?pred ?e ?e2)
                     [?e :follow ?e2]
                     [(?pred ?e)]
                     [(?pred ?e2)]]]
                  even?)
             #{[4 6] [2 4]})))

    (testing "Using built-ins inside rule"
      (is (= (d/q '[:find ?x ?y
                    :in $ %
                    :where (match ?x ?y)]
                  db
                  '[[(match ?e ?e2)
                     [?e :follow ?e2]
                     [(even? ?e)]
                     [(even? ?e2)]]])
             #{[4 6] [2 4]})))
    (testing "Calling rule twice (#44)"
      (d/q '[:find ?p
             :in $ % ?fn
             :where (rule ?p ?fn "a")
             (rule ?p ?fn "b")]
           [[1 :attr "a"]]
           '[[(rule ?p ?fn ?x)
              [?p :attr ?x]
              [(?fn ?x)]]]
           (constantly true))))

  (testing "Specifying db to rule"
    (is (= (d/q '[:find ?n
                  :in   $sexes $ages %
                  :where ($sexes male ?n)
                  ($ages adult ?n)]
                [["Ivan" :male] ["Darya" :female] ["Oleg" :male] ["Igor" :male]]
                [["Ivan" 15] ["Oleg" 66] ["Darya" 32]]
                '[[(male ?x)
                   [?x :male]]
                  [(adult ?y)
                   [?y ?a]
                   [(>= ?a 18)]]])
           #{["Oleg"]}))))

(deftest test-get-else-inside-rule-body
  ;; Regression for plan-rule-op routing branch bodies through the
  ;; logical IR pipeline. Top-level queries get `[(get-else $ ?e :a v)
  ;; ?x]` promoted to an LOptionalScan that binds ?e via an attribute
  ;; scan; previously rule body planning skipped that pass and ?e was
  ;; left unbound, so the same body returned `#{[nil …]}` instead of
  ;; actual matches.
  (let [db (d/db-with (db/empty-db {:concept/id         {:db/unique :db.unique/identity}
                                    :concept/preferred  {}
                                    :concept/deprecated {}})
                      [{:db/id 1 :concept/id "c1" :concept/preferred "Alice"}
                       {:db/id 2 :concept/id "c2" :concept/preferred "Bob" :concept/deprecated true}
                       {:db/id 3 :concept/id "c3" :concept/preferred "Carol"}])]
    (testing "scalar get-else binds the entity through the optional scan"
      (is (= (d/q '{:find [?id ?dep]
                    :in [$ %]
                    :where [(maybe-dep ?id ?dep)]}
                  db
                  '[[(maybe-dep ?id ?dep)
                     [(get-else $ ?e :concept/deprecated false) ?dep]
                     [?e :concept/id ?id]]])
             #{["c1" false] ["c2" true] ["c3" false]})))

    (testing "predicate on get-else output inside a rule"
      (is (= (d/q '{:find [?id]
                    :in [$ %]
                    :where [(active? ?id)]}
                  db
                  '[[(active? ?id)
                     [?e :concept/id ?id]
                     [(get-else $ ?e :concept/deprecated false) ?dep]
                     [(not ?dep)]]])
             #{["c1"] ["c3"]})))))

;; https://github.com/tonsky/datahike/issues/218

(deftest test-false-arguments
  (let [db    (d/db-with (db/empty-db)
                         [[:db/add 1 :attr true]
                          [:db/add 2 :attr false]])
        rules '[[(is ?id ?val)
                 [?id :attr ?val]]]]
    (is (= (d/q '[:find ?id :in $ %
                  :where (is ?id true)]
                db rules)
           #{[1]}))
    (is (= (d/q '[:find ?id :in $ %
                  :where (is ?id false)] db rules)
           #{[2]}))))

;; Indexed-attr magic-set path over an :attribute-refs? true CONNECTION — runs on
;; both platforms (cljs via deftest-async + the async setup-db).
(deftest-async test-rule-arguments
  (let [cfg {:store {:backend :memory
                     :id #uuid "a0000000-0000-0000-0000-00000000000a"}
             :name "rule-test"
             :keep-history? true
             :schema-flexibility :write
             :attribute-refs? true}
        schema [{:db/ident       :name
                 :db/cardinality :db.cardinality/one
                 :db/index       true
                 :db/unique      :db.unique/identity
                 :db/valueType   :db.type/string}
                {:db/ident       :parents
                 :db/cardinality :db.cardinality/many
                 :db/valueType   :db.type/ref}
                {:db/ident       :age
                 :db/cardinality :db.cardinality/one
                 :db/valueType   :db.type/long}]
        rules '[[(parent-info ?child ?name ?age)
                 [?child :parents ?p]
                 [(ground ["Alice" "Bob"]) [?name ...]]
                 [?p :name ?name]
                 [?p :age ?age]]]
        conn (<! (du/setup-db-async cfg))]

    (<! (d/transact! conn {:tx-data schema}))
    (<! (d/transact! conn {:tx-data [{:name "Alice"
                                      :age  25}
                                     {:name "Bob"
                                      :age 30}]}))
    (<! (d/transact! conn {:tx-data [{:name    "Charlie"
                                      :age     5
                                      :parents [[:name "Alice"]
                                                [:name "Bob"]]}]}))

    (is (= #{[25]}
           (d/q {:query '{:find [?age]
                          :in [$ ?n ?pn %]
                          :where
                          [[?child :name ?n]
                           (parent-info ?child ?pn ?age)]}
                 :args [@conn "Charlie" "Alice" rules]})))

    (is (= #{[25]}
           (d/q {:query '{:find [?age]
                          :in [$ ?n [?pn ...] %]
                          :where
                          [[?child :name ?n]
                           (parent-info ?child ?pn ?age)]}
                 :args [@conn "Charlie" ["Alice"] rules]})))

    (is (= #{[25]}
           (d/q {:query '{:find [?age]
                          :in [$ ?n %]
                          :where
                          [[?child :name ?n]
                           (parent-info ?child "Alice" ?age)]}
                 :args [@conn "Charlie" rules]})))

    (is (thrown-with-msg? Throwable
                          #"Bad format for value in pattern, must be a scalar, nil or a vector of two elements."
                          (d/q {:query '{:find [?age]
                                         :in [$ ?n %]
                                         :where
                                         [[?child :name ?n]
                                          (parent-info ?child ["Alice"] ?age)]}
                                :args [@conn "Charlie" rules]})))
    (d/release conn)))

(deftest test-deep-bound-recursive-rule
  ;; A ground argument routes the recursive fixpoint through the magic-set
  ;; (demand) restriction. This regression pins the properties that restriction
  ;; must preserve:
  ;; 1. depth — closures deeper than any internal batching stay complete (an
  ;;    earlier demand-set threshold silently truncated results at 999);
  ;; 2. the demand guard drops derivations arriving from undemanded sources
  ;;    without losing any tuple of the ground row;
  ;; 3. demand stays timely when short and long paths rejoin (uneven diamond).
  (let [depth 1200
        anc-rule '[[(anc ?c ?a) [?c :parent ?a]]
                   [(anc ?c ?a) [?c :parent ?m] (anc ?m ?a)]]
        chain-db (d/db-with (db/empty-db {:parent {:db/valueType :db.type/ref
                                                   :db/cardinality :db.cardinality/one}})
                            (for [i (range 1 depth)] {:db/id i :parent (inc i)}))]
    (testing "ancestors of the head of a deep chain are complete"
      (is (= (dec depth)
             (count (d/q '[:find [?a ...] :in $ % ?h :where (anc ?h ?a)]
                         chain-db anc-rule 1)))))
    (testing "a ground argument bound deep inside the chain"
      (is (= 200
             (count (d/q '[:find [?a ...] :in $ % ?h :where (anc ?h ?a)]
                         chain-db anc-rule 1000)))))
    (testing "derivations from undemanded side entities are guarded without loss"
      (let [noisy-db (d/db-with chain-db
                                (for [i (range 1 51)]
                                  {:db/id (+ 10000 i) :parent i}))]
        (is (= (dec depth)
               (count (d/q '[:find [?a ...] :in $ % ?h :where (anc ?h ?a)]
                           noisy-db anc-rule 1))))))
    (testing "uneven diamond: short and long paths to the same ancestor"
      (let [dia-db (d/db-with (db/empty-db {:parent {:db/valueType :db.type/ref
                                                     :db/cardinality :db.cardinality/many}})
                              [{:db/id 1 :parent [2 10]}
                               {:db/id 2 :parent 3}
                               {:db/id 10 :parent 11}
                               {:db/id 11 :parent 12}
                               {:db/id 12 :parent 3}
                               {:db/id 3 :parent 4}
                               {:db/id 4 :parent 5}])]
        (is (= #{2 3 4 5 10 11 12}
               (set (d/q '[:find [?a ...] :in $ % ?h :where (anc ?h ?a)]
                         dia-db anc-rule 1))))))))

(deftest test-rule-head-var-bound-only-by-caller
  ;; ?eps is a rule parameter no branch body binds — not range-restricted, so
  ;; its only possible value is the one the call site passed. The planner used
  ;; to rename head vars to the rule's own names without carrying that value in,
  ;; leaving the branch relation without a ?eps column and NPEing in the
  ;; fixpoint's dedup step (#897).
  (let [db [[1 :s "root"] [1 :p "isa"] [1 :o "Anchor"]
            [2 :s "root"] [2 :p "link"] [2 :o "b"]
            [3 :s "b"] [3 :p "link"] [3 :o "c"]
            [4 :s "c"] [4 :p "skip"] [4 :o "z"]]
        rules '[[(reachable ?anchor ?eps ?n)
                 [?e :s ?n] [?e :p "isa"] [?e :o ?anchor]]
                [(reachable ?anchor ?eps ?o)
                 [?e :s ?s] [?e :p ?ep] [?e :o ?o]
                 [(contains? ?eps ?ep)]
                 (reachable ?anchor ?eps ?s)]]
        query '[:find ?n :in $ % ?anchor ?eps :where (reachable ?anchor ?eps ?n)]]
    (testing "the passed-in edge set reaches the whole chain"
      (is (= #{["root"] ["b"] ["c"]} (d/q query db rules "Anchor" #{"link"}))))
    (testing "widening the passed-in set follows the extra edge kind"
      (is (= #{["root"] ["b"] ["c"] ["z"]} (d/q query db rules "Anchor" #{"link" "skip"}))))
    (testing "an empty set leaves only the anchored base case"
      (is (= #{["root"]} (d/q query db rules "Anchor" #{}))))))
(deftest test-recursive-rule-ground-output-arg
  ;; A recursive rule called with the ground argument on the OUTPUT side —
  ;; "who reaches X?" rather than "what does X reach?". Magic-set seeding fed
  ;; the demand value into the EAVT ENTITY slot, which walks edges the wrong
  ;; way: it looked up the target's OUTGOING edges and, finding none, seeded
  ;; an empty relation and the fixpoint died at iteration 0. Only the general
  ;; demand-restricted base evaluation handles this direction.
  (let [rules '[[(reach ?a ?b) [?a :friend ?b]]
                [(reach ?a ?b) [?a :friend ?x] (reach ?x ?b)]]
        db (d/db-with (db/empty-db {:friend {:db/valueType :db.type/ref
                                             :db/cardinality :db.cardinality/many}})
                      [{:db/id 1 :friend [2]}
                       {:db/id 2 :friend [3 6]}
                       {:db/id 3 :friend [4]}
                       {:db/id 4 :friend [5]}
                       {:db/id 5}
                       {:db/id 6}])]
    (testing "ground output arg — including the chain end, which has no outgoing edge"
      (are [target res] (= res (set (d/q '[:find [?a ...] :in $ % ?B :where (reach ?a ?B)]
                                         db rules target)))
        2 #{1}
        3 #{1 2}
        5 #{1 2 3 4}
        6 #{1 2}))

    (testing "ground input arg keeps taking the point-lookup path"
      (are [source res] (= res (set (d/q '[:find [?b ...] :in $ % ?A :where (reach ?A ?b)]
                                         db rules source)))
        1 #{2 3 4 5 6}
        4 #{5}
        5 #{}))))

(deftest test-recursive-rule-demand-not-transitive-closure
  ;; Magic sets read the next round's demand out of the derived head tuples at
  ;; the head's other position. That is sound only when the values in that
  ;; column are the ones the recursion navigates to next — true for a linear
  ;; transitive closure, where the base case and the recursive step traverse the
  ;; SAME relation, and false in general.
  ;;
  ;; Here the base case yields city NAMES while the recursion walks :follows
  ;; between entities, so demand was seeded with strings, matched nothing, and
  ;; the fixpoint stopped with answers still underived — silently incomplete,
  ;; no error. See `lower/magic-demand-sound?`.
  (let [db (d/db-with (db/empty-db {:follows {:db/valueType :db.type/ref
                                              :db/cardinality :db.cardinality/many}
                                    :city {:db/cardinality :db.cardinality/one}})
                      [{:db/id 1 :follows [2]}
                       {:db/id 2 :follows [3] :city "berlin"}
                       {:db/id 3 :city "paris"}])
        rules '[[(sc ?a ?b) [?a :city ?b]]
                [(sc ?a ?b) [?a :follows ?t] (sc ?t ?b)]]]
    (testing "a ground input arg still reaches through the whole chain"
      ;; 1 has no :city of its own; it follows 2 (berlin), which follows 3 (paris)
      (are [source res] (= res (set (d/q '[:find [?b ...] :in $ % ?A :where (sc ?A ?b)]
                                         db rules source)))
        1 #{"berlin" "paris"}
        2 #{"berlin" "paris"}
        3 #{"paris"}))

    (testing "the ungrounded call is unaffected"
      (is (= #{[1 "berlin"] [1 "paris"] [2 "berlin"] [2 "paris"] [3 "paris"]}
             (set (d/q '[:find ?a ?b :in $ % :where (sc ?a ?b)] db rules)))))

    (testing "a rule whose recursive step walks a different edge than its base"
      ;; base links via :follows, recursion via :knows — the harvested column
      ;; holds :follows targets, which say nothing about where :knows leads.
      (let [db2 (d/db-with (db/empty-db {:follows {:db/valueType :db.type/ref
                                                   :db/cardinality :db.cardinality/many}
                                         :knows {:db/valueType :db.type/ref
                                                 :db/cardinality :db.cardinality/many}})
                           [{:db/id 1 :knows [2]}
                            {:db/id 2 :follows [3] :knows [3]}
                            {:db/id 3 :follows [4]}
                            {:db/id 4}])
            r '[[(p ?a ?b) [?a :follows ?b]]
                [(p ?a ?b) [?a :knows ?x] (p ?x ?b)]]]
        ;; 1 knows 2; 2 follows 3 -> p(1,3). 1 knows 2 knows 3; 3 follows 4 -> p(1,4).
        (is (= #{3 4} (set (d/q '[:find [?b ...] :in $ % ?A :where (p ?A ?b)] db2 r 1))))))))

(deftest test-recursive-rule-step-must-be-the-base-relation
  ;; `magic-demand-sound?`'s predecessors compared the recursive step to the
  ;; base case by renaming the propagated local to the call's threaded argument.
  ;; That rename is only meaningful under two conditions, and without them a
  ;; rule could be declared a transitive closure when it is not — deriving
  ;; tuples the rule does not entail.
  (let [db (d/db-with (db/empty-db {:e {:db/valueType :db.type/ref
                                        :db/cardinality :db.cardinality/many}})
                      [{:db/id 1 :e [2]} {:db/id 2 :e [3]} {:db/id 3 :db/ident :n3}])]
    (testing "a recursive call whose propagated var the body never constrains"
      ;; ?x is unconstrained, so the second branch adds nothing over the first:
      ;; p is exactly :e, NOT its closure. Renaming ?x away made the branch
      ;; canonicalize to the base case and claim to be a closure of it.
      (let [rules '[[(p ?a ?b) [?a :e ?b]]
                    [(p ?a ?b) [?a :e ?b] (p ?x ?b)]]]
        (is (= #{[1 2] [2 3]}
               (set (d/q '[:find ?a ?b :in $ % :where (p ?a ?b)] db rules))))
        (is (= #{2} (set (d/q '[:find [?b ...] :in $ % :where (p 1 ?b)] db rules))))))

    (testing "a rename that would merge two distinct live variables"
      ;; ?b already occurs in the body, so rewriting ?x to ?b collapses two
      ;; different variables and the result no longer denotes the step.
      (let [rules '[[(p ?a ?b) [?a :e ?b]]
                    [(p ?a ?b) [?a :e ?x] [?a :e ?b] (p ?x ?b)]]]
        (is (= #{[1 2] [2 3]}
               (set (d/q '[:find ?a ?b :in $ % :where (p ?a ?b)] db rules))))))))

(deftest test-recursive-rule-filtered-traversal
  ;; A filtered traversal — the step is a SUBSET of the base relation — is one
  ;; of the most common recursive shapes. Demand harvested from the base case is
  ;; then a superset of what the recursion needs, which is sound (a superset
  ;; only costs work), so this must keep working and keep its fast path.
  (let [db (d/db-with (db/empty-db {:e {:db/valueType :db.type/ref
                                        :db/cardinality :db.cardinality/many}
                                    :active {:db/cardinality :db.cardinality/one}})
                      [{:db/id 1 :e [2]}
                       {:db/id 2 :e [3] :active true}
                       {:db/id 3 :e [4] :active false}
                       {:db/id 4}])
        rules '[[(p ?a ?b) [?a :e ?b]]
                [(p ?a ?b) [?a :e ?x] [?x :active true] (p ?x ?b)]]]
    ;; 1->2 directly; 2 is active so 1 reaches 3; 3 is NOT active, so 4 is not
    ;; reachable from 1. 2->3 directly, and 3 inactive stops there.
    (is (= #{2 3} (set (d/q '[:find [?b ...] :in $ % :where (p 1 ?b)] db rules))))
    (is (= #{3} (set (d/q '[:find [?b ...] :in $ % :where (p 2 ?b)] db rules))))))

#?(:clj
   (deftest test-mutual-recursion-over-a-cycle
     ;; Mutual recursion over CYCLIC data. The compiled planner's semi-naive
     ;; fixpoint dedups per rule and converges; the reference engine's rule
     ;; solver does not terminate at all here, so it is asserted only on the
     ;; planner and run on a bounded thread.
     ;;
     ;; Found by widening the differential generator's dataset axis: its one
     ;; dataset was acyclic on :friend, so every mutual-recursion case
     ;; terminated and this was invisible. It matters because the reference
     ;; engine is both the differential oracle and the permanent fallback for
     ;; shapes the planner declines — a user on DATAHIKE_QUERY_PLANNER=false
     ;; hangs rather than gets a wrong answer.
     (let [db (d/db-with (db/empty-db {:friend {:db/valueType :db.type/ref
                                                :db/cardinality :db.cardinality/one}})
                         [{:db/id 100 :friend 101}
                          {:db/id 101 :friend 102}
                          {:db/id 102 :friend 100}])
           rules '[[(ehop ?a ?b) [?a :friend ?b]]
                   [(ehop ?a ?b) [?a :friend ?x] (ohop ?x ?b)]
                   [(ohop ?a ?b) [?a :friend ?x] (ehop ?x ?b)]]
           ;; Pin the engine: the base-engine CI job sets
           ;; DATAHIKE_QUERY_PLANNER=false, which would otherwise run these on
           ;; the very engine that does not terminate here.
           run (fn [q] (let [f (future (binding [dq/*disable-planner* false]
                                         (set (d/q q db rules))))
                             r (deref f 15000 ::timeout)]
                         (when (= r ::timeout) (future-cancel f))
                         r))]
       ;; every pair is reachable in a 3-cycle
       (is (= #{[100 100] [100 101] [100 102]
                [101 100] [101 101] [101 102]
                [102 100] [102 101] [102 102]}
              (run '[:find ?a ?b :in $ % :where (ehop ?a ?b)]))
           "planner terminates on mutual recursion over a cycle")
       (is (= #{100 101 102}
              (run '[:find [?b ...] :in $ % :where (ehop 100 ?b)]))
           "…and with a ground argument"))))

#?(:clj
   (deftest test-right-recursive-rule
     ;; A right-linear transitive closure — the recursive call comes FIRST in
     ;; the body:
     ;;   [(rr ?a ?b) [?a :friend ?b]]
     ;;   [(rr ?a ?b) (rr ?a ?x) [?x :friend ?b]]
     ;; The planner's semi-naive fixpoint evaluates this bottom-up and converges.
     ;; The reference engine expands the leading rule call before anything binds
     ;; it and does not terminate — on a THREE-entity ACYCLIC graph, so this is
     ;; not about cycles or size. Asserted on the planner only, on a bounded
     ;; thread.
     ;;
     ;; Found by adding a rule-shape axis to the differential generator: its five
     ;; rule sets were all left-linear, so this whole shape was untested.
     (let [db (d/db-with (db/empty-db {:friend {:db/valueType :db.type/ref
                                                :db/cardinality :db.cardinality/one}})
                         [{:db/id 1 :friend 2} {:db/id 2 :friend 3} {:db/id 3}])
           rules '[[(rr ?a ?b) [?a :friend ?b]]
                   [(rr ?a ?b) (rr ?a ?x) [?x :friend ?b]]]
           ;; Pin the engine: the base-engine CI job sets
           ;; DATAHIKE_QUERY_PLANNER=false, which would otherwise run these on
           ;; the very engine that does not terminate here.
           run (fn [q] (let [f (future (binding [dq/*disable-planner* false]
                                         (set (d/q q db rules))))
                             r (deref f 15000 ::timeout)]
                         (when (= r ::timeout) (future-cancel f))
                         r))]
       (is (= #{[1 2] [2 3] [1 3]} (run '[:find ?a ?b :in $ % :where (rr ?a ?b)]))
           "planner terminates on a right-recursive rule")
       (is (= #{2 3} (run '[:find [?b ...] :in $ % :where (rr 1 ?b)]))
           "…and with a ground argument"))))

(deftest test-recursive-rule-branch-body-with-several-clauses
  ;; A recursive rule whose BRANCH BODY has more than one clause takes the
  ;; fused scan+merge path, which emits its tuples as VECTORS (the scan
  ;; datom's five fields, extended by `conj` per merge) rather than as arrays
  ;; like a single-clause body does. The JVM projection dispatches on the
  ;; tuple's shape; the ClojureScript one used `aget` unconditionally, and
  ;; `aget` of a PersistentVector on JS is `undefined` rather than an error.
  ;; So on cljs every head var projected to nil and the rule answered
  ;; `[[nil nil]]` — a silent wrong answer on the DEFAULT engine there, since
  ;; the planner is on by default in ClojureScript.
  ;;
  ;; The clause count is what matters, not the encoding: a reified edge
  ;; (`[?e :edge/from ?x] [?e :edge/to ?y]`) and two independent clauses fail
  ;; identically, while the one-clause direct edge was always fine — which is
  ;; why this shape survived so long. Asserted on both platforms, against
  ;; hand-written closures.
  (let [db (d/db-with (db/empty-db {:sym {:db/cardinality :db.cardinality/one}
                                    :direct {:db/valueType :db.type/ref
                                             :db/cardinality :db.cardinality/many}
                                    :edge/from {:db/valueType :db.type/ref
                                                :db/cardinality :db.cardinality/one}
                                    :edge/to {:db/valueType :db.type/ref
                                              :db/cardinality :db.cardinality/one}})
                      [{:db/id 1 :sym "a" :direct [2]}
                       {:db/id 2 :sym "b" :direct [3]}
                       {:db/id 3 :sym "c" :direct [4]}
                       {:db/id 4 :sym "d"}
                       {:db/id 11 :edge/from 1 :edge/to 2}
                       {:db/id 12 :edge/from 2 :edge/to 3}
                       {:db/id 13 :edge/from 3 :edge/to 4}])
        closure #{[1 2] [1 3] [1 4] [2 3] [2 4] [3 4]}
        run (fn [q rules]
              (binding [dq/*disable-planner* false]
                (set (d/q q db rules))))]
    (testing "a two-clause (reified) branch body projects its head vars"
      (is (= closure
             (run '[:find ?a ?b :in $ % :where (r ?a ?b)]
                  '[[(r ?x ?y) [?e :edge/from ?x] [?e :edge/to ?y]]
                    [(r ?x ?y) [?e :edge/from ?x] [?e :edge/to ?m] (r ?m ?y)]]))))
    (testing "…with a ground argument, which post-filters the projected tuples"
      (is (= #{2 3 4}
             (run '[:find [?b ...] :in $ % :where (r 1 ?b)]
                  '[[(r ?x ?y) [?e :edge/from ?x] [?e :edge/to ?y]]
                    [(r ?x ?y) [?e :edge/from ?x] [?e :edge/to ?m] (r ?m ?y)]]))))
    (testing "two INDEPENDENT clauses fail the same way — it is the count, not reification"
      (is (= closure
             (run '[:find ?a ?b :in $ % :where (r ?a ?b)]
                  '[[(r ?x ?y) [?x :direct ?y] [?x :sym _]]
                    [(r ?x ?y) [?x :direct ?m] (r ?m ?y)]]))))
    (testing "the one-clause body that always worked still does"
      (is (= closure
             (run '[:find ?a ?b :in $ % :where (r ?a ?b)]
                  '[[(r ?x ?y) [?x :direct ?y]]
                    [(r ?x ?y) [?x :direct ?m] (r ?m ?y)]]))))))

(deftest test-recursive-rule-does-not-inherit-caller-relations
  ;; A rule's fixpoint computes the rule's relation INDEPENDENTLY of its call
  ;; site — the caller joins on the output vars afterwards. Letting a caller
  ;; relation into a branch body therefore does not merely restrict the search,
  ;; it restricts the ACCUMULATOR, and a right-recursive body reading that
  ;; accumulator for its second hop finds it empty and stops after one (#911).
  ;;
  ;; It is triggered by a NAME collision, but not with the call args: on the
  ;; recursive path every branch is renamed to the rule's OWN declared head vars
  ;; (`lower.cljc`, "we use the rule head vars … NOT the call-args"), so those
  ;; names are internal and ANY caller variable spelled like them is captured —
  ;; the call site need not mention it. `[?x :sym "a"] (r ?p ?q)` against
  ;; `[(r ?x ?y) …]` collides on the DECLARATION's ?x and was wrong; rename that
  ;; anchor and the same query was correct. That is why every existing rule test
  ;; missed it: they all happen to spell the caller differently, or pass a
  ;; ground argument. Both spellings are asserted below, so a name-dependent
  ;; regression cannot hide behind either one.
  ;;
  ;; One mechanism, three wrong answers — all asserted here, because a fix that
  ;; addressed only the first would look complete:
  ;;   * a right-recursive rule truncated to its first hop (a strict subset);
  ;;   * a caller relation over TWO head-var names made the rule return nothing;
  ;;   * at a second call site, the first call's result restricted the next
  ;;     rule's accumulator, degenerating `(r ?x ?y) (s ?x ?b)` into `r ⋈ s`.
  ;;
  ;; The answers are hand-written transitive closures, not another engine's
  ;; output: the wrong answer here is a strict SUBSET with no error raised, so
  ;; an oracle that shares the fault would agree with it.
  (let [db (d/db-with (db/empty-db {:sym {:db/cardinality :db.cardinality/one}
                                    :direct {:db/valueType :db.type/ref
                                             :db/cardinality :db.cardinality/many}
                                    :edge/from {:db/valueType :db.type/ref
                                                :db/cardinality :db.cardinality/one}
                                    :edge/to {:db/valueType :db.type/ref
                                              :db/cardinality :db.cardinality/one}})
                      ;; a->b->c->d, plus the same edges reified as edge entities
                      [{:db/id 1 :sym "a" :direct [2]}
                       {:db/id 2 :sym "b" :direct [3]}
                       {:db/id 3 :sym "c" :direct [4]}
                       {:db/id 4 :sym "d"}
                       {:db/id 11 :edge/from 1 :edge/to 2}
                       {:db/id 12 :edge/from 2 :edge/to 3}
                       {:db/id 13 :edge/from 3 :edge/to 4}])
        ;; right-recursive: the recursive call is LAST, so the second hop is the
        ;; one that reads the accumulator
        direct-rules  '[[(r ?x ?y) [?x :direct ?y]]
                        [(r ?x ?y) [?x :direct ?m] (r ?m ?y)]]
        reified-rules '[[(r ?x ?y) [?e :edge/from ?x] [?e :edge/to ?y]]
                        [(r ?x ?y) [?e :edge/from ?x] [?e :edge/to ?m] (r ?m ?y)]]
        ;; Pin the engine, as the two right-recursive tests above do: the
        ;; base-engine CI job sets DATAHIKE_QUERY_PLANNER=false, and the
        ;; relational engine expands a right-recursive rule call before
        ;; anything binds it, so it does not terminate on these shapes. This
        ;; fix is in the planner, so the planner is what must answer here.
        ;; Bounded on a future (clj) so a regression fails instead of hanging
        ;; the job; cljs has the planner on by default and no futures.
        syms (fn [q rules]
               #?(:clj (let [f (future (binding [dq/*disable-planner* false]
                                         (set (d/q q db rules))))
                             r (deref f 15000 ::timeout)]
                         (when (= r ::timeout) (future-cancel f))
                         r)
                  :cljs (binding [dq/*disable-planner* false]
                          (set (d/q q db rules)))))]
    ;; Both encodings are asserted on both platforms. The reified one was
    ;; JVM-only for a while: on cljs the planner answered a reified-edge
    ;; recursive rule with a single all-nil tuple, because a branch body with
    ;; more than one clause emits VECTOR tuples and the cljs projection read
    ;; them with `aget` (which is `undefined`, not an error, on a
    ;; PersistentVector). Fixed — see `execute-recursive-rule`.
    (doseq [[label rules] [["direct edge" direct-rules]
                           ["reified edge" reified-rules]]]
      (testing label
        ;; the caller's ?x collides with the rule's head var ?x
        (is (= #{"b" "c" "d"}
               (syms '[:find [?s ...] :in $ %
                       :where [?x :sym "a"] (r ?x ?y) [?y :sym ?s]]
                     rules))
            (str label " — full closure when the caller's var collides with a head var"))
        ;; …the same query with the caller's vars renamed. Both spellings must
        ;; agree; on the bug only this one was right.
        (is (= #{"b" "c" "d"}
               (syms '[:find [?s ...] :in $ %
                       :where [?p :sym "a"] (r ?p ?q) [?q :sym ?s]]
                     rules))
            (str label " — …and the variable-renamed twin agrees"))
        ;; the rule call comes FIRST, so nothing has bound ?x when it runs
        (is (= #{"b" "c" "d"}
               (syms '[:find [?s ...] :in $ %
                       :where (r ?x ?y) [?y :sym ?s] [?x :sym "a"]]
                     rules))
            (str label " — full closure when the call precedes what binds it"))
        ;; the rule's own relation, unrestricted: every reachable pair
        (is (= #{["a" "b"] ["a" "c"] ["a" "d"]
                 ["b" "c"] ["b" "d"] ["c" "d"]}
               (syms '[:find ?sx ?sy :in $ %
                       :where (r ?x ?y) [?x :sym ?sx] [?y :sym ?sy]]
                     rules))
            (str label " — the whole relation"))
        ;; THE ACTUAL TRIGGER: the anchor is spelled like the rule's DECLARED
        ;; head var ?x while the call args are ?p/?q, so the call site never
        ;; mentions the colliding variable. The renamed twin above renames the
        ;; anchor too and so does not cover this.
        (is (= #{"b" "c" "d"}
               (syms '[:find [?s ...] :in $ %
                       :where [?x :sym "a"] (r ?p ?q) [?q :sym ?s]]
                     rules))
            (str label " — a caller var colliding with a DECLARED head var,"
                 " though the call site never names it"))
        ;; a caller relation over TWO head-var names: joined against the rule's
        ;; whole relation, this returned nothing at all
        (is (= #{"b" "c" "d"}
               (syms '[:find [?s ...] :in $ %
                       :where [?x :sym ?y] (r ?a ?b) [?b :sym ?s]]
                     rules))
            (str label " — a two-column caller relation over both head-var names"))))
    (testing "a branch body is not planned believing outer variables are bound"
      ;; The plan-time half of the same capture. A body's ops were ordered
      ;; believing the OUTER scope's bindings held, and a head var spelled like
      ;; an outer var looked bound from clause zero — so `[(str ?y) ?t]` was
      ;; cost-ordered AHEAD of the pattern that binds ?y. While bodies still
      ;; inherited caller relations that belief was accidentally satisfied;
      ;; once they stopped, it raised "Cannot resolve any more clauses" at
      ;; execute time. A branch is now planned believing only the head vars the
      ;; call site actually supplies — the pass-through ones.
      (let [fn-rules '[[(r ?x ?y) [?x :direct ?y] [(str ?y) ?t] [(some? ?t)]]
                       [(r ?x ?y) [?x :direct ?m] (r ?m ?y) [(str ?y) ?t] [(some? ?t)]]]]
        (is (= #{"a" "b" "c"}
               (syms '[:find [?s ...] :in $ %
                       :where [?y :sym "d"] (r ?p ?y) [?p :sym ?s]]
                     fn-rules))
            "a function op on a head var spelled like an outer var")
        (is (= #{"a" "b" "c"}
               (syms '[:find [?s ...] :in $ %
                       :where [?w :sym "d"] (r ?p ?w) [?p :sym ?s]]
                     fn-rules))
            "…and the non-colliding spelling agrees")))
    (testing "a caller-supplied parameter still reaches the body"
      ;; The counterweight: pass-through head vars (#897) ARE bound from the
      ;; call site, so narrowing what a branch believes must not narrow them
      ;; away — `?eps` is bound by no body, and a predicate over it has to stay
      ;; placeable.
      (let [pt-rules '[[(reach ?anchor ?eps ?n)
                        [?anchor :direct ?n] [(contains? ?eps ?n)]]
                       [(reach ?anchor ?eps ?o)
                        (reach ?anchor ?eps ?s) [?s :direct ?o] [(contains? ?eps ?o)]]]]
        (is (= #{"b" "c"}
               ;; pinned for the same reason as `syms` above — the recursive
               ;; call leads this branch too
               (binding [dq/*disable-planner* false]
                 (set (d/q '[:find [?s ...] :in $ % ?eps
                             :where (reach 1 ?eps ?n) [?n :sym ?s]]
                           db pt-rules #{2 3}))))
            "a pass-through head var is still supplied by the caller")))
    (testing "a second call site does not inherit the first call's result"
      ;; `(r …)`'s result relation is itself spelled with head-var names, so it
      ;; was captured by the NEXT rule's branch bodies: `s` was restricted to
      ;; pairs already in `r` rather than computing its own relation.
      (let [two-rules '[[(r ?x ?y) [?x :direct ?y]]
                        [(r ?x ?y) [?x :direct ?m] (r ?m ?y)]
                        [(s ?x ?y) [?x :direct ?y]]
                        [(s ?x ?y) [?x :direct ?m] (s ?m ?y)]]]
        (is (= #{"b" "c" "d"}
               (syms '[:find [?s ...] :in $ %
                       :where [?x :sym "a"] (r ?x ?y) (s ?x ?b) [?b :sym ?s]]
                     two-rules))
            "the second rule computes its own relation")))))
