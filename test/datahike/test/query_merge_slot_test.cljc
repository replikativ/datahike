(ns datahike.test.query-merge-slot-test
  "Several patterns on one entity are answered by one scan and a seek per
   further pattern. The seek only knows attribute and value, so three things a
   pattern may say were lost when it was not the one driving the scan:

   - a literal transaction, `[?e :k \"a\"] [?e :t _ 536870914]`;
   - an `added` variable shared with a sibling, `[?e :a _ _ ?op] [?e :b _ _ ?op]`;
   - a variable attribute, `[?e ?a 1] [?e ?b 4]`, which has nothing to seek by.

   Every query here is answered twice, by the planner and by the reference
   engine, and must also return what the data says."
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing]])
   [clojure.walk :as walk]
   [datahike.api :as d]
   [datahike.query :as q]))

(def ^:private schema
  [{:db/ident :x/k :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :x/i :db/valueType :db.type/long :db/cardinality :db.cardinality/one :db/index true}
   {:db/ident :x/s :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :x/t :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])

(defn- setup
  "tx1: a{i 1, s 4} b{i 1} c{i 1, s 9} d{i 2, s 4}.
   tx2: c loses s 9, d's s becomes 5, a and c gain t 7."
  []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write :keep-history? true}
        _ (d/create-database cfg)
        conn (d/connect cfg)
        _ (d/transact conn schema)
        tx1 (:max-tx (:db-after (d/transact conn [{:x/k "a" :x/i 1 :x/s 4} {:x/k "b" :x/i 1}
                                                  {:x/k "c" :x/i 1 :x/s 9} {:x/k "d" :x/i 2 :x/s 4}])))
        tx2 (:max-tx (:db-after (d/transact conn [[:db/retract [:x/k "c"] :x/s 9] {:x/k "d" :x/s 5}
                                                  {:x/k "a" :x/t 7} {:x/k "c" :x/t 7}])))
        db @conn]
    {:db db :history (d/history db) :tx1 tx1 :tx2 tx2}))

(defn- both
  "The planner's answer, after checking the reference engine gives the same."
  [query & args]
  (let [planned (apply d/q query args)
        reference (binding [q/*disable-planner* true] (apply d/q query args))]
    (is (= reference planned) (str "planner and reference engine differ on " (pr-str query)))
    planned))

(defn- ks [db result]
  (into #{} (map #(:x/k (d/entity db (first %)))) result))

(defn- with-tx
  "`query` with the symbol `TX` replaced by a transaction id, as a literal."
  [query tx]
  (walk/postwalk #(if (= 'TX %) tx %) query))

(deftest a-literal-transaction-on-a-second-pattern-is-compared
  (let [{:keys [db history tx1 tx2]} (setup)]
    (doseq [[label d] [["current" db] ["history" history]]]
      (testing label
        (testing "beside a unique lookup"
          (is (= #{} (both (with-tx '[:find ?e :where [?e :x/k "a"] [?e :x/t _ TX]] tx1) d)))
          (is (= #{"a"} (ks db (both (with-tx '[:find ?e :where [?e :x/k "a"] [?e :x/t _ TX]] tx2) d)))))
        (testing "beside an indexed value"
          (is (= #{} (both (with-tx '[:find ?e :where [?e :x/i 1] [?e :x/t _ TX]] tx1) d)))
          (is (= #{"a" "c"} (ks db (both (with-tx '[:find ?e :where [?e :x/i 1] [?e :x/t _ TX]] tx2) d)))))
        (testing "under not"
          (is (= #{"a" "b" "c"}
                 (ks db (both (with-tx '[:find ?e :where [?e :x/i 1] (not [?e :x/t _ TX])] tx1) d))))
          (is (= #{"b"}
                 (ks db (both (with-tx '[:find ?e :where [?e :x/i 1] (not [?e :x/t _ TX])] tx2) d)))))
        (testing "in an or branch and in a rule body"
          (is (= #{"a" "c"}
                 (ks db (both (with-tx '[:find ?e :where [?e :x/i 1]
                                         (or [?e :x/t _ TX] [?e :x/k "zz" TX])] tx2) d))))
          (is (= #{"a" "c"}
                 (ks db (both '[:find ?e :in $ % :where [?e :x/i 1] (stamped ?e)] d
                              [(with-tx '[(stamped ?e) [?e :x/t _ TX]] tx2)])))))
        (testing "on a single pattern, and with a variable attribute"
          (is (= #{"a" "c"} (ks db (both (with-tx '[:find ?e :where [?e :x/t _ TX]] tx2) d))))
          (is (= #{} (both (with-tx '[:find ?e :where [?e :x/t _ TX]] tx1) d))))
        (testing "the transaction as an input is unchanged"
          (is (= #{} (both '[:find ?e :in $ ?tx :where [?e :x/i 1] [?e :x/t _ ?tx]] d tx1)))
          (is (= #{"a" "c"}
                 (ks db (both '[:find ?e :in $ ?tx :where [?e :x/i 1] [?e :x/t _ ?tx]] d tx2)))))))))

(deftest an-added-variable-shared-by-two-patterns-is-unified
  (let [{:keys [db history]} (setup)
        named (fn [result] (into #{} (map (fn [[e p]] [(:x/k (d/entity db e)) p])) result))]
    (testing "history: c's s was asserted and retracted, its i only asserted"
      (is (= #{["a" true] ["c" true] ["d" true]}
             (named (both '[:find ?e ?p :where [?e :x/i _ _ ?p] [?e :x/s _ _ ?p]] history))))
      (is (= #{["a" true] ["c" true] ["d" true]}
             (named (both '[:find ?e ?p :where [?e :x/k _ _ ?p] [?e :x/i _ _ ?p] [?e :x/s _ _ ?p]]
                          history))))
      (is (= #{["a" true] ["c" true] ["d" true]}
             (named (both '[:find ?e ?p :where [?e :x/i _ _ ?p]
                            [(get-else $ ?e :x/t 0) ?v] [?e :x/s _ _ ?p]] history)))))
    (testing "distinct variables still pair every combination"
      (is (= #{["a" true true] ["c" true true] ["c" true false] ["d" true true] ["d" true false]}
             (into #{} (map (fn [[e p r]] [(:x/k (d/entity db e)) p r]))
                   (both '[:find ?e ?p ?r :where [?e :x/i _ _ ?p] [?e :x/s _ _ ?r]] history)))))
    (testing "current: everything is an assertion"
      (is (= #{["a" true] ["d" true]}
             (named (both '[:find ?e ?p :where [?e :x/i _ _ ?p] [?e :x/s _ _ ?p]] db)))))))

(deftest patterns-with-a-variable-attribute-on-one-entity
  (let [{:keys [db history]} (setup)
        named (fn [result] (into #{} (map (fn [[e & more]] (into [(:x/k (d/entity db e))] more))) result))]
    (doseq [[label d] [["current" db] ["history" history]]]
      (testing label
        (is (= #{["a" :x/i :x/s]}
               (named (both '[:find ?e ?a ?b :where [?e ?a 1] [?e ?b 4] [?e :x/i 1]] d))))
        (is (= #{["a" :x/i :x/s :x/k]}
               (named (both '[:find ?e ?a ?b ?c :where [?e ?a 1] [?e ?b 4] [?e ?c "a"]] d))))
        (testing "the same attribute variable twice"
          (is (= #{} (both '[:find ?e ?a :where [?e ?a 1] [?e ?a 4]] d)))
          (is (= #{["a" :x/t] ["c" :x/t]}
                 (named (both '[:find ?e ?a :where [?e ?a 7] [?e ?a ?v] [(= ?v 7)]] d)))))
        (testing "beside a ground attribute it is unchanged"
          (is (= #{["a" :x/s]} (named (both '[:find ?e ?a :where [?e :x/i 1] [?e ?a 4]] d)))))))
    (testing "under not"
      (is (= #{"b" "c"} (ks db (both '[:find ?e :where [?e :x/i 1] (not [?e ?la 4])] db))))
      (is (= #{"b" "c"} (ks db (both '[:find ?e :where [?e :x/i 1] (not [?e ?la 4])] history))))
      (is (= #{["a" :x/i] ["b" :x/i] ["c" :x/i]}
             (named (both '[:find ?e ?a :where [?e ?a 1] (not [?e ?a 4])] db)))))))

(deftest a-negation-with-a-local-variable-on-a-history-database
  ;; The folded negation's `?local` was projected from a datom that a
  ;; satisfied negation does not have.
  (let [{:keys [db history]} (setup)
        a (d/q '[:find ?e . :where [?e :x/k "a"]] db)
        b (d/q '[:find ?e . :where [?e :x/k "b"]] db)
        c (d/q '[:find ?e . :where [?e :x/k "c"]] db)]
    (is (= #{"a" "b"}
           (ks db (both '[:find ?e :in $ [?e ...]
                          :where [?e :x/i 1] (not [?e :x/s ?local _ false])]
                        history [a b c]))))
    (is (= #{"a" "b"}
           (ks db (both '[:find ?e :where [?e :x/i 1] (not [?e :x/s ?local _ false])] history))))))

(deftest an-added-slot-on-an-as-of-or-since-view
  ;; Such a view holds what survived, all of it asserted. c's `s 9` was
  ;; retracted in tx2; the reference engine answered with it for `added` true,
  ;; having dropped the retraction before working out what survived.
  (let [{:keys [db tx1 tx2]} (setup)]
    (doseq [[label view] [["as-of" (d/as-of db tx2)] ["since" (d/since db (dec tx1))]]]
      (testing label
        (is (= #{} (both '[:find ?e :where [?e :x/s 9 _ true]] view)))
        (is (= #{} (both '[:find ?e :where [?e :x/i 1] [?e :x/s 9 _ true]] view)))
        (is (= #{"a"} (ks db (both '[:find ?e :where [?e :x/i 1] [?e :x/s 4 _ true]] view))))
        (is (= #{} (both '[:find ?e :where [?e :x/i 1] [?e :x/s 4 _ false]] view)))))
    (testing "as of tx1 the value is still there"
      (is (= #{"c"} (ks db (both '[:find ?e :where [?e :x/s 9 _ true]] (d/as-of db tx1))))))))

#?(:clj
   (deftest an-aggregate-over-a-join-honours-the-added-slot
     ;; Large enough for the index-ordered sum, which read e, a and v only.
     (let [cfg {:store {:backend :memory :id (random-uuid)}
                :schema-flexibility :write :keep-history? false}
           _ (d/create-database cfg)
           conn (d/connect cfg)]
       (d/transact conn [{:db/ident :lk :db/valueType :db.type/long
                          :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                         {:db/ident :rk :db/valueType :db.type/long
                          :db/cardinality :db.cardinality/one :db/index true}
                         {:db/ident :amount :db/valueType :db.type/long
                          :db/cardinality :db.cardinality/one}])
       (d/transact conn (vec (concat (for [i (range 600)] {:lk i})
                                     (for [i (range 600)] {:rk i :amount 1}))))
       (is (= 600 (both '[:find (sum ?v) . :with ?r
                          :where [?l :lk ?k] [?r :rk ?k] [?r :amount ?v]] @conn)))
       (is (= 600 (both '[:find (sum ?v) . :with ?r
                          :where [?l :lk ?k] [?r :rk ?k] [?r :amount ?v _ true]] @conn)))
       (is (nil? (both '[:find (sum ?v) . :with ?r
                         :where [?l :lk ?k] [?r :rk ?k] [?r :amount ?v nil false]] @conn))))))

(deftest a-negation-keeps-its-local-variables-local
  ;; `?local` occurs only inside the `not`. Restating the literal transaction
  ;; must leave the clause a plain `not`: as a `not-join` over the body's
  ;; variables it demanded a binding for `?local` from outside.
  (let [{:keys [db history tx1 tx2]} (setup)]
    (doseq [[label d] [["current" db] ["history" history]]]
      (testing label
        (is (= #{"a" "b" "c"}
               (ks db (both (with-tx '[:find ?e :where [?e :x/i 1] (not [?e :x/t ?local TX])] tx1) d))))
        (is (= #{"b"}
               (ks db (both (with-tx '[:find ?e :where [?e :x/i 1] (not [?e :x/t ?local TX])] tx2) d))))
        (is (= #{}
               (both (with-tx '[:find ?e :where [?e :x/k "a"] (not [?e :x/t ?local TX])] tx2) d)))))))

(deftest an-added-variable-shared-across-slots-and-inputs
  (let [{:keys [db history]} (setup)
        a (d/q '[:find ?e . :where [?e :x/k "a"]] db)
        c (d/q '[:find ?e . :where [?e :x/k "c"]] db)
        named (fn [result] (into #{} (map (fn [[e p]] [(:x/k (d/entity db e)) p])) result))]
    (testing "entities from a collection input"
      (is (= #{["a" true]}
             (named (both '[:find ?e ?p :in $ [?e ...] :where [?e :x/i _ _ ?p] [?e :x/s _ _ ?p]]
                          db [a c]))))
      (is (= #{["a" true] ["c" true]}
             (named (both '[:find ?e ?p :in $ [?e ...] :where [?e :x/i _ _ ?p] [?e :x/s _ _ ?p]]
                          history [a c]))))
      (is (= #{["a" true]}
             (named (both '[:find ?e ?p :in $ [?e ...] :where [?e :x/i 1] [?e :x/s _ _ ?p]]
                          db [a c])))))
    (testing "the variable is also a VALUE of a sibling pattern"
      ;; x/t 7 is no boolean, so nothing can satisfy both patterns
      (is (= #{} (both '[:find ?e :where [?e :x/t ?p] [?e :x/s _ _ ?p]] db)))
      (is (= #{} (both '[:find ?e ?p :where [?e :x/s _ _ ?p] [?e :x/t ?p]] history))))))

(deftest a-transaction-from-an-input-on-a-second-pattern-is-compared
  ;; Like the literal, in every input form. Beside a unique lookup the pattern
  ;; is never the one driving the scan.
  (let [{:keys [db history tx1 tx2]} (setup)
        a (d/q '[:find ?e . :where [?e :x/k "a"]] db)]
    (doseq [[label d] [["current" db] ["history" history]]]
      (testing label
        (is (= #{} (both '[:find ?e :in $ ?t :where [?e :x/k "a"] [?e :x/t _ ?t]] d tx1)))
        (is (= #{"a"} (ks db (both '[:find ?e :in $ ?t :where [?e :x/k "a"] [?e :x/t _ ?t]] d tx2))))
        (is (= #{} (both '[:find ?v :in $ ?e ?t :where [?e :x/i ?v] [?e :x/t _ ?t]] d a tx1)))
        (is (= #{[1]} (both '[:find ?v :in $ ?e ?t :where [?e :x/i ?v] [?e :x/t _ ?t]] d a tx2)))
        (testing "collection, tuple and relation inputs"
          (is (= #{} (both '[:find ?e :in $ [?t ...] :where [?e :x/k "a"] [?e :x/t _ ?t]] d [tx1])))
          (is (= #{"a"} (ks db (both '[:find ?e :in $ [?t ...] :where [?e :x/k "a"] [?e :x/t _ ?t]]
                                     d [tx1 tx2]))))
          (is (= #{} (both '[:find ?e :in $ [?t ?u] :where [?e :x/k "a"] [?e :x/t _ ?t]] d [tx1 0])))
          (is (= #{"a"} (ks db (both '[:find ?e :in $ [[?t ?u]] :where [?e :x/k "a"] [?e :x/t _ ?t]]
                                     d [[tx1 0] [tx2 1]])))))
        (testing "under not"
          (is (= #{"a"} (ks db (both '[:find ?e :in $ ?t :where [?e :x/k "a"] (not [?e :x/t _ ?t])]
                                     d tx1))))
          (is (= #{} (both '[:find ?e :in $ ?t :where [?e :x/k "a"] (not [?e :x/t _ ?t])] d tx2))))))))

(deftest a-negation-compares-its-own-variable-with-an-input
  ;; `?f` is bound by the negation's pattern. Counted as bound beforehand, the
  ;; predicate ran first, never held, and the negation excluded nothing.
  (let [{:keys [db history]} (setup)]
    (is (= #{"b" "c"} (ks db (both '[:find ?e :in $ ?x :where [?e :x/i 1]
                                     (not [?e :x/s ?f] [(= ?f ?x)])] db 4))))
    (is (= #{"a" "b" "c"} (ks db (both '[:find ?e :in $ ?x :where [?e :x/i 1]
                                         (not [?e :x/s ?f] [(= ?f ?x)])] db 5))))
    (is (= #{"b" "c"} (ks db (both '[:find ?e :in $ [?x ...] :where [?e :x/i 1]
                                     (not [?e :x/s ?f] [(= ?f ?x)])] db [4]))))
    (is (= #{"b"} (ks db (both '[:find ?e :in $ ?x :where [?e :x/i 1]
                                 (not [?e :x/s ?f] [(>= ?f ?x)])] history 4))))
    (testing "two negations do not share their local variables"
      (is (= #{"b"} (ks db (both '[:find ?e :in $ ?x ?y :where [?e :x/i 1]
                                   (not [?e :x/s ?f] [(= ?f ?x)])
                                   (not [?e :x/t ?f] [(= ?f ?y)])] db 4 7)))))))

(deftest a-pattern-without-an-attribute-and-get-else-with-a-variable-one
  (let [{:keys [db history]} (setup)]
    (doseq [[label d] [["current" db] ["history" history]]]
      (testing label
        (is (= (if (= label "history") #{"a" "d"} #{"a"})
               (ks db (both '[:find ?e :where [?e] [?e ?a 4]] d))))
        (is (= #{[0]} (both '[:find ?v :in $ ?a :where [?e :x/k "b"] [(get-else $ ?e ?a 0) ?v]]
                            d :x/s)))))
    (is (= #{["a" 4] ["b" 0] ["c" 0]}
           (into #{} (map (fn [[e v]] [(:x/k (d/entity db e)) v]))
                 (both '[:find ?e ?v :where [?e :x/i 1] [?x :db/ident ?a] [(= ?a :x/s)]
                         [(get-else $ ?e ?a 0) ?v]] db))))))

(deftest a-quoted-symbol-compared-with-an-indexed-value
  ;; The quote form reached the index bound as a list.
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write :keep-history? false}
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (d/transact conn [{:db/ident :y/idx :db/valueType :db.type/symbol
                       :db/cardinality :db.cardinality/one :db/index true}
                      {:db/ident :y/plain :db/valueType :db.type/symbol
                       :db/cardinality :db.cardinality/one}
                      {:db/ident :y/edge :db/valueType :db.type/ref
                       :db/cardinality :db.cardinality/one}])
    (d/transact conn [{:db/id -1 :y/idx 'foo :y/plain 'foo} {:db/id -2 :y/idx 'bar :y/plain 'bar :y/edge -1}])
    (let [db @conn
          by-sym (fn [sym] (d/q '[:find ?e . :in $ ?s :where [?e :y/plain ?s]] db sym))
          foo (by-sym 'foo)
          bar (by-sym 'bar)]
      (is (= #{[foo]} (both '[:find ?e :where [?e :y/idx ?s] [(= ?s (quote foo))]] db)))
      (is (= #{[bar]} (both '[:find ?e :where [?e :y/idx ?s] [(= (quote bar) ?s)]] db)))
      (is (= #{[foo]} (both '[:find ?e :where [?e :y/plain ?s] [(= ?s (quote foo))]] db)))
      (testing "a recursive rule over patterns that fix `added`"
        (let [rules (fn [flag] [['(r ?x ?y) ['?x :y/edge '?y '_ flag]]
                                ['(r ?x ?y) ['?x :y/edge '?z '_ flag] '(r ?z ?y)]])]
          (is (= #{[foo]} (both '[:find ?y :in $ % ?x :where (r ?x ?y)] db (rules true) bar)))
          (is (= #{} (both '[:find ?y :in $ % ?x :where (r ?x ?y)] db (rules false) bar))))))))

(deftest an-explicit-join-keeps-its-local-variables
  (let [{:keys [db tx1 tx2]} (setup)
        a (d/q '[:find ?e . :where [?e :x/k "a"]] db)]
    (testing "a tx variable the join does not declare is local, whatever :in calls by that name"
      (is (= #{} (both '[:find ?e :in $ [?t ...] :where [?e :x/k "a"]
                         (not-join [?e] [?e :x/t _ ?t])] db [tx1])))
      (is (= #{[a]} (both '[:find ?e :in $ [?t ...] :where [?e :x/k "a"]
                            (or-join [?e] [?e :x/t _ ?t])] db [tx1]))))
    (testing "declared, it is the input"
      (is (= #{[a]} (both '[:find ?e :in $ [?t ...] :where [?e :x/k "a"]
                            (not-join [?e ?t] [?e :x/t _ ?t])] db [tx1])))
      (is (= #{} (both '[:find ?e :in $ [?t ...] :where [?e :x/k "a"]
                         (not-join [?e ?t] [?e :x/t _ ?t])] db [tx2]))))
    (testing "an or-join's local does not count as bound for a sibling not"
      (is (= #{} (both '[:find ?e :in $ [?x ...] :where [?e :x/k "a"]
                         (or-join [?e] [?e :x/s ?f])
                         (not [?e :x/s ?f] [(= ?f ?x)])] db [4]))))
    (testing "a source-prefixed or orders a branch like an unprefixed one"
      (is (= #{[a]} (both '[:find ?e :in $ $b [?x ...] :where [?e :x/k "a"]
                            ($b or (and [?e :x/s ?f] [(= ?f ?x)]))] db db [4]))))
    (testing "a required variable of an or-join that is bound"
      (is (= #{[4]} (both '[:find ?v :in $ ?e :where (or-join [[?e] ?v] [?e :x/s ?v])] db a))))
    (testing "get-else with a quoted attribute"
      (is (= #{[4]} (both '[:find ?v :where [?e :x/k "a"] [(get-else $ ?e (quote :x/s) 0) ?v]] db))))))

(deftest predicates-pushed-into-an-index-range
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write :keep-history? true}
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (d/transact conn [{:db/ident :p/n :db/valueType :db.type/long
                       :db/cardinality :db.cardinality/one :db/index true}
                      {:db/ident :p/flag :db/valueType :db.type/boolean
                       :db/cardinality :db.cardinality/one :db/index true}
                      {:db/ident :p/sym :db/valueType :db.type/symbol
                       :db/cardinality :db.cardinality/one :db/index true}
                      {:db/ident :p/pair :db/valueType :db.type/tuple
                       :db/tupleTypes [:db.type/long :db.type/long]
                       :db/cardinality :db.cardinality/one}])
    (d/transact conn [{:db/id -1 :p/n 3 :p/flag false :p/sym (symbol "?p") :p/pair [4 5]}
                      {:db/id -2 :p/n 4 :p/flag true :p/sym 'foo}])
    (let [db @conn
          ;; the :p/n of every entity found
          n (fn [qq & args] (into #{} (map (fn [[e]] (:p/n (d/entity db e))))
                                  (apply both qq db args)))]
      (testing "two equalities on one value are both required"
        (is (= #{} (n '[:find ?e :where [?e :p/n ?v] [(= ?v 3)] [(= ?v 4)]])))
        (is (= #{3} (n '[:find ?e :where [?e :p/n ?v] [(= ?v 3)] [(= ?v 3)]])))
        (is (= #{} (n '[:find ?e :where [?e :p/n ?v] [(= ?v 3)] [(> ?v 3)]])))
        (is (= #{} (n '[:find ?e :where [?e :p/n ?v] [(> ?v 3)] [(= ?v 3)]])))
        (is (= #{4} (n '[:find ?e :where [?e :p/n ?v] [(>= ?v 3)] [(= ?v 4)]])))
        (is (= #{} (n '[:find ?e :in $ ?a ?b :where [?e :p/n ?v] [(= ?v ?a)] [(= ?v ?b)]] 3 4))))
      (testing "false is a bound"
        (is (= #{3} (n '[:find ?e :where [?e :p/flag ?v] [(= ?v false)]])))
        (is (= #{3} (n '[:find ?e :in $ ?b :where [?e :p/flag ?v] [(= ?v ?b)]] false)))
        (is (= #{4} (n '[:find ?e :where [?e :p/flag ?v] [(not= ?v false)]]))))
      (testing "a quoted symbol that looks like a variable is not the input of that name"
        (is (= #{3} (n '[:find ?e :in $ ?p :where [?e :p/sym ?v] [(= ?v (quote ?p))]] 7))))
      (testing "get-else destructuring a tuple value"
        (is (= #{[4 5]} (both '[:find ?x ?y :where [?e :p/n 3] [(get-else $ ?e :p/pair 0) [?x ?y]]]
                              db)))))))

#?(:clj
   (deftest count-over-a-join-with-find-variables-that-do-not-identify-the-producer
     ;; The index-ordered count emits one row per producer key. Without the
     ;; key or its entity among the :find variables those rows have to be
     ;; added up; a set had collapsed them.
     (let [cfg {:store {:backend :memory :id (random-uuid)}
                :schema-flexibility :write :keep-history? false}
           _ (d/create-database cfg)
           conn (d/connect cfg)]
       (d/transact conn [{:db/ident :j/lk :db/valueType :db.type/long
                          :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                         {:db/ident :j/grp :db/valueType :db.type/long
                          :db/cardinality :db.cardinality/one}
                         {:db/ident :j/rk :db/valueType :db.type/long
                          :db/cardinality :db.cardinality/one :db/index true}])
       (d/transact conn (vec (concat (for [i (range 1200)] {:j/lk i :j/grp (mod i 3)})
                                     (for [i (range 2400)] {:j/rk (quot i 2)}))))
       (let [db @conn
             q* (fn [qq] (let [p (d/q qq db)
                               r (binding [q/*disable-planner* true] (d/q qq db))]
                           (is (= (frequencies r) (frequencies p)))
                           (is (= (count p) (count (set p))) "no row twice")
                           (set p)))]
         (is (= #{[2400]} (q* '[:find (count ?r) :where [?l :j/lk ?k] [?r :j/rk ?k]])))
         (is (= #{[0 800] [1 800] [2 800]}
                (q* '[:find ?g (count ?r) :where [?l :j/lk ?k] [?l :j/grp ?g] [?r :j/rk ?k]])))
         (is (= (into #{} (map (fn [k] [k 2])) (range 1200))
                (q* '[:find ?k (count ?r) :where [?l :j/lk ?k] [?r :j/rk ?k]])))
         (is (= #{[0 800]}
                (q* '[:find ?g (count ?r) :where [?l :j/lk ?k] [?l :j/grp ?g]
                      [(= ?g (quote 0))] [?r :j/rk ?k]])))
         (is (= #{} (q* '[:find (count ?r) :where [?l :j/lk ?k] [?r :j/rk ?k] [(> ?k 99999)]])))))))

(deftest pushed-predicates-keep-the-query's-comparisons
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write :keep-history? true}
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (d/transact conn [{:db/ident :c/k :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                      {:db/ident :c/n :db/valueType :db.type/long
                       :db/cardinality :db.cardinality/one :db/index true}
                      {:db/ident :c/str :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one :db/index true}
                      {:db/ident :c/kw :db/valueType :db.type/keyword
                       :db/cardinality :db.cardinality/one :db/index true}
                      {:db/ident :c/inst :db/valueType :db.type/instant
                       :db/cardinality :db.cardinality/one :db/index true}
                      #?(:clj {:db/ident :c/bytes :db/valueType :db.type/bytes
                               :db/cardinality :db.cardinality/one :db/index true})])
    (d/transact conn [(merge {:c/k "a" :c/n 3 :c/str "c" :c/kw :c :c/inst #inst "2020-01-03"}
                             #?(:clj {:c/bytes (byte-array [1 2 3])}))
                      {:c/k "b" :c/n 1 :c/str "a" :c/kw :a :c/inst #inst "2020-01-01"}])
    (d/transact conn (vec (for [i (range 4000)] {:c/n (+ 10 i)})))
    (let [db @conn
          a (d/q '[:find ?e . :where [?e :c/k "a"]] db)
          b (d/q '[:find ?e . :where [?e :c/k "b"]] db)
          big (d/q '[:find ?e . :where [?e :c/n 500]] db)]
      (doseq [[label d] [["current" db] ["history" (d/history db)]]]
        (testing label
          #?(:clj (testing "an array is a value"
                    (is (= #{[a]} (both '[:find ?e :in $ ?p :where [?e :c/bytes ?v] [(= ?v ?p)]]
                                        d (byte-array [1 2 3]))))))
          (testing "`=` tells a long from a double, `==` does not"
            ;; one number type in JavaScript
            #?(:clj (is (= #{} (both '[:find ?e :where [?e :c/n ?v] [(= ?v 1.0)]] d))))
            (is (= #{[b]} (both '[:find ?e :where [?e :c/n ?v] [(== ?v 1.0)]] d))))
          (testing "the pattern's value is itself an input"
            (is (= #{[a]} (both '[:find ?e :in $ ?v :where [?e :c/n ?v] [(= ?v 3)]] d 3)))
            (is (= #{[a]} (both '[:find ?e :in $ ?v :where [?e :c/n ?v] [(> ?v 2)]] d 3)))
            (is (= #{} (both '[:find ?e :in $ ?v :where [?e :c/n ?v] [(> ?v 3)]] d 3)))
            (testing "which the predicate's bounds do not replace"
              (is (= #{} (both '[:find ?e :in $ ?v :where [?e :c/n ?v] [(= ?v 10)]] d 3)))
              (is (= #{[a]} (both '[:find ?e :in $ ?v :where [?e :c/n ?v] [(>= ?v 1)]] d 3)))
              (is (= #{[a]} (both '[:find ?e :in $ ?v :where [?e :c/n ?v] [(<= ?v 100)]] d 3)))))
          (testing "a literal nil bounds nothing"
            (is (= #{[a] [b]} (both '[:find ?e :where [?e :c/kw ?v] [(> ?v nil)]] d))))
          (testing "an inclusive bound on entities that are sought one by one"
            (is (= #{[big]} (both '[:find ?e :in $ [?e ...] :where [?e :c/n ?v] [(>= ?v 10)]]
                                  d [a b big])))
            (is (= #{[a] [b]} (both '[:find ?e :in $ [?e ...] :where [?e :c/n ?v] [(<= ?v 3)]]
                                    d [a b big]))))
          ;; the JVM tightened two bounds with a numeric max/min
          #?(:clj
             (testing "two bounds on one side, on values that are not numbers"
               (is (= #{[a]} (both '[:find ?e :where [?e :c/str ?v] [(>= ?v "a")] [(>= ?v "b")]] d)))
               (is (= #{[b]} (both '[:find ?e :where [?e :c/kw ?v] [(<= ?v :z)] [(<= ?v :b)]] d)))
               (is (= #{[a]} (both '[:find ?e :where [?e :c/inst ?v]
                                     [(> ?v #inst "2019-01-01")] [(> ?v #inst "2020-01-02")]] d))))))))))

(deftest a-required-or-join-variable-bound-by-a-later-clause
  (let [{:keys [db history]} (setup)]
    (doseq [[label d] [["current" db] ["history" history]]]
      (testing label
        (is (= #{[4]} (both '[:find ?v :where (or-join [[?e] ?v] [?e :x/s ?v]) [?e :x/k "a"]] d)))
        (is (= #{[4]} (both '[:find ?v :in $ $b :where ($b or-join [[?e] ?v] [?e :x/s ?v])
                              [?e :x/k "a"]] d d)))
        (testing "bound by nothing, or only read by another clause, it is refused"
          (doseq [qq '[[:find ?v :where (or-join [[?e] ?v] [?e :x/s ?v])]
                       [:find ?v :where (or-join [[?e] ?v] [?e :x/s ?v]) [(pos? ?e)]]
                       [:find ?v ?z :where (or-join [[?e] ?v] [?e :x/s ?v]) [(inc ?e) ?z]]]]
            (is (thrown? #?(:clj Exception :cljs js/Error) (d/q qq d)))
            (is (thrown? #?(:clj Exception :cljs js/Error)
                         (binding [q/*disable-planner* true] (d/q qq d))))))
        (testing "a function's output binds it"
          (let [a (d/q '[:find ?e . :where [?e :x/k "a"]] db)]
            (is (= #{[4]} (both '[:find ?v :in $ ?a :where (or-join [[?e] ?v] [?e :x/s ?v])
                                  [(identity ?a) ?e]] d a)))))
        (testing "a quoted nil default is the nil default"
          (is (thrown? #?(:clj Exception :cljs js/Error)
                       (d/q '[:find ?v :where [?e :x/k "b"]
                              [(get-else $ ?e :x/s (quote nil)) ?v]] d))))))))

(deftest a-collection-as-a-second-source
  ;; Beside a database, a pattern on a collection asked the collection for
  ;; its schema and threw.
  (let [{:keys [db]} (setup)
        rows [[1 :x/i 1] [2 :x/i 9]]]
    (is (= #{[1] [2]} (both '[:find ?e :in $ $b :where [$b ?e :x/i ?v]] db rows)))
    (is (= #{[2]} (both '[:find ?e :in $ $b :where [$b ?e :x/i ?v] [(>= ?v 9)]] db rows)))
    (testing "the body of a prefixed or / not is planned for that source"
      ;; :x/i is indexed in the database; the collection has no index for a
      ;; pushed predicate to land in
      (is (= #{[2]} (both '[:find ?e :in $ $b :where ($b or (and [?e :x/i ?v] [(>= ?v 9)]))]
                          db rows)))
      (is (= #{[1]} (both '[:find ?e :in $ $b [?e ...] :where ($b not [?e :x/i ?v] [(>= ?v 9)])]
                          db rows [1 2]))))))

(deftest what-the-narrower-scopes-must-not-change
  ;; Each of these answered correctly before the scoping fixes and went wrong
  ;; during them.
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :read :keep-history? false}
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (d/transact conn [{:db/ident :r/n :db/cardinality :db.cardinality/one :db/index true}
                      {:db/ident :r/m :db/valueType :db.type/long
                       :db/cardinality :db.cardinality/one :db/index true}])
    (d/transact conn [{:db/id -1 :r/wanted 7 :r/active true :r/tag 8 :r/s 1}
                      {:db/id -2 :r/n 1} {:db/id -3 :r/n "x"}])
    (d/transact conn (vec (for [i (range 200)] {:r/m i})))
    (let [db @conn
          one (d/q '[:find ?e . :where [?e :r/n 1]] db)
          m42 (d/q '[:find ?e . :where [?e :r/m 42]] db)]
      (testing "a nested negation correlated through a plain or does not fold"
        ;; ?x is 7; the entity has no :r/tag 7, so the inner not holds and the
        ;; outer one excludes it. Folded, the inner not asked for ANY tag.
        (is (= #{} (both '[:find ?e :where (or [?e :r/wanted ?x])
                           (or (not [?e :r/active true] (not [?e :r/tag ?x])))] db)))
        (is (= #{} (both '[:find ?e :in $ $b :where (or [?e :r/wanted ?x])
                           ($b or (not [?e :r/active true] (not [?e :r/tag ?x])))] db db))))
      (testing "a function fed by the or-join does not bind what the or-join requires"
        (is (thrown? #?(:clj Exception :cljs js/Error)
                     (d/q '[:find ?v :where (or-join [[?e] ?v] [?e :r/s ?v]) [(inc ?v) ?e]] db)))
        (is (thrown? #?(:clj Exception :cljs js/Error)
                     (d/q '[:find ?v :where [(inc ?v) ?e] (or-join [[?e] ?v] [?e :r/s ?v])] db))))
      (testing "an equality the range excludes"
        (is (= #{} (both '[:find ?e :where [?e :r/m ?v] [(>= ?v 1)] [(= ?v -5)]] db))))
      (testing "a transaction input on a pattern alone on its source is not restated"
        (is (= '[[?e :r/m _ ?t] [$b ?e :r/n _]]
               (:where (q/normalize-repeated-vars
                        '{:find [?e] :in [$ $b [?t ...]]
                          :where [[?e :r/m _ ?t] [$b ?e :r/n _]]})))))
      #?(:clj
         (testing "a range on an attribute holding values of two types"
           (is (= #{[one]} (both '[:find ?e :where [?e :r/n ?v] [(number? ?v)] [(>= ?v 0)]] db)))
           (is (= #{[one]} (both '[:find ?e :where [?e :r/n ?v] [(number? ?v)] [(> ?v 0)]] db)))))
      (testing "a chain of functions fed by the or-join binds nothing for it either"
        (is (thrown? #?(:clj Exception :cljs js/Error)
                     (d/q '[:find ?v :where (or-join [[?e] ?v] [?e :r/s ?v])
                            [(inc ?v) ?x] [(identity ?x) ?e]] db))))
      (testing "an equality after a range"
        (is (= #{[m42]} (both '[:find ?e :where [?e :r/m ?v] [(>= ?v 0)] [(= ?v 42)]] db)))
        (is (= #{} (both '[:find ?e :where [?e :r/m ?v] [(>= ?v 50)] [(= ?v 42)]] db)))
        (is (= #{} (both '[:find ?e :where [?e :r/m ?v] [(<= ?v 40)] [(= ?v 42)]] db)))))))

(deftest a-collection-of-transactions-on-a-pattern-of-its-own
  ;; Not restated: alone on its entity the pattern scans and joins its
  ;; transaction with the input.
  (let [{:keys [db tx1 tx2]} (setup)]
    (is (= #{"a" "c"} (ks db (both '[:find ?e :in $ [?t ...] :where [?e :x/t _ ?t]] db [tx1 tx2]))))
    (is (= #{} (both '[:find ?e :in $ [?t ...] :where [?e :x/t _ ?t]] db [tx1])))
    (is (= '[[?e :x/t _ ?t]]
           (:where (q/normalize-repeated-vars
                    '{:find [?e] :in [$ [?t ...]] :where [[?e :x/t _ ?t]]}))))))

(deftest patterns-sharing-an-input-transaction-agree-on-it
  ;; One restating variable for all of them: they compare their transactions
  ;; with each other datom by datom, not only with the input afterwards.
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write :keep-history? true}
        _ (d/create-database cfg)
        conn (d/connect cfg)
        _ (d/transact conn [{:db/ident :m/k :db/valueType :db.type/string
                             :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                            {:db/ident :m/a :db/valueType :db.type/long
                             :db/cardinality :db.cardinality/many}
                            {:db/ident :m/b :db/valueType :db.type/long
                             :db/cardinality :db.cardinality/many}])
        txs (vec (for [i (range 20)]
                   (:max-tx (:db-after (d/transact conn [{:m/k "x" :m/a i :m/b i}])))))
        db @conn]
    (doseq [[label d] [["current" db] ["history" (d/history db)]]]
      (testing label
        (is (= #{[5 5]} (both '[:find ?a ?b :in $ ?t :where [?e :m/k "x"] [?e :m/a ?a ?t] [?e :m/b ?b ?t]]
                              d (nth txs 5))))
        (is (= (into #{} (map (fn [i] [i i])) (range 20))
               (both '[:find ?a ?b :in $ [?t ...] :where [?e :m/a ?a ?t] [?e :m/b ?b ?t]] d txs)))))
    (is (= '[[?e :m/a ?a ?__tx__1] [(identity ?__tx__1) ?t] [?e :m/b ?b ?__tx__1]]
           (:where (q/normalize-repeated-vars
                    '{:find [?a ?b] :in [$ [?t ...]]
                      :where [[?e :m/a ?a ?t] [?e :m/b ?b ?t]]}))))))
