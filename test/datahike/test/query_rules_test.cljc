(ns datahike.test.query-rules-test
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [are is deftest testing]]
      :clj  [clojure.test :as t :refer        [are is deftest testing]])
   [clojure.core.async :refer [<!]]
   [datahike.api :as d]
   [datahike.db :as db]
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
