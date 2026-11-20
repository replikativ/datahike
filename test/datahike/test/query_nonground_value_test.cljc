(ns datahike.test.query-nonground-value-test
  "A value slot that is neither a value nor a `?var` - `_`, `_x`, or no slot at
   all (`[?e :a]`) - matches every value and binds nothing. The point fast paths
   read it as a value and compared stored values with the symbol `_`, so
   `[?s :stmt/item ?si] [?s :stmt/status _]` returned nothing (#1168).

   Every query here is answered twice, by the planner and by the reference
   engine, and must also return what the data says."
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing]])
   [datahike.api :as d]
   [datahike.query :as q]))

(def ^:private schema
  [{:db/ident :stmt/item :db/valueType :db.type/ref :db/cardinality :db.cardinality/one :db/index true}
   {:db/ident :stmt/status :db/valueType :db.type/keyword :db/cardinality :db.cardinality/one}
   {:db/ident :stmt/note :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :stmt/tag :db/valueType :db.type/keyword :db/cardinality :db.cardinality/many}
   {:db/ident :item/name :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}])

(defn- test-db
  "Two items. Item a has four statements: s0 status+note, s1 status only,
   s2 note only, s3 neither (but tagged). Item b has one statement with a status."
  []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write :keep-history? false}
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (d/transact conn schema)
    (d/transact conn [{:db/id -1 :item/name "a"} {:db/id -2 :item/name "b"}
                      {:stmt/item -1 :stmt/note "s0" :stmt/status :open}
                      {:stmt/item -1 :stmt/note "s1x" :stmt/status :done}
                      {:stmt/item -1 :stmt/note "s2"}
                      {:stmt/item -1 :stmt/tag [:t1 :t2]}
                      {:stmt/item -2 :stmt/note "b0" :stmt/status :open}])
    @conn))

(defn- both
  "The planner's answer, after checking the reference engine gives the same."
  [query & args]
  (let [planned (apply d/q query args)
        reference (binding [q/*disable-planner* true] (apply d/q query args))]
    (is (= reference planned) (str "planner and reference engine differ on " (pr-str query)))
    planned))

(deftest a-non-ground-value-slot-only-requires-the-attribute
  (let [db (test-db)
        a (d/q '[:find ?e . :where [?e :item/name "a"]] db)
        notes (fn [find-clause where & extra-in]
                (fn [& args]
                  (apply both (vec (concat [:find] find-clause [:in '$] extra-in [:where] where))
                         db args)))]
    (testing "after a scalar input: `_`, `_x` and a pattern without a value slot"
      (doseq [slot '[[?s :stmt/status _] [?s :stmt/status _x] [?s :stmt/status]]]
        (is (= #{["s0"] ["s1x"]}
               ((notes '[?n] (conj '[[?s :stmt/item ?si] [?s :stmt/note ?n]] slot) '?si) a))
            (pr-str slot))))
    (testing "the #1168 shape: nothing else in the query"
      (is (= 2 (count (both '[:find ?s :in $ ?si :where [?s :stmt/item ?si] [?s :stmt/status _]]
                            db a)))))
    (testing "with the value written into the query"
      (is (= 2 (count (both (into '[:find ?s :where] [['?s :stmt/item a] '[?s :stmt/status _]])
                            db)))))
    (testing "with a collection input"
      (is (= 2 (count (both '[:find ?s :in $ [?si ...] :where [?s :stmt/item ?si] [?s :stmt/status _]]
                            db [a])))))
    (testing "on a unique attribute, the lookup the fast path was built for"
      (is (= #{[a]} (both '[:find ?e :in $ ?n :where [?e :item/name ?n] [?e :item/name _]] db "a")))
      (is (= #{} (both '[:find ?e :in $ ?n :where [?e :item/name ?n] [?e :stmt/status _]] db "a"))))
    (testing "next to a compared value, a bound variable and a second input"
      (is (= #{["s0"]}
             (both '[:find ?n :in $ ?si :where
                     [?s :stmt/item ?si] [?s :stmt/status :open] [?s :stmt/note _] [?s :stmt/note ?n]]
                   db a)))
      (is (= #{["s1x"]}
             (both '[:find ?n :in $ ?si ?st :where
                     [?s :stmt/item ?si] [?s :stmt/status ?st] [?s :stmt/note _] [?s :stmt/note ?n]]
                   db a :done))))
    (testing "with get-else, whose result may itself be ignored"
      (is (= #{["s0" :open] ["s1x" :done]}
             (both '[:find ?n ?st :in $ ?si :where
                     [?s :stmt/item ?si] [?s :stmt/status _] [?s :stmt/note ?n]
                     [(get-else $ ?s :stmt/status :none) ?st]]
                   db a)))
      (is (= 4 (count (both '[:find ?s :in $ ?si :where
                              [?s :stmt/item ?si] [(get-else $ ?s :stmt/status :none) _]]
                            db a)))))
    (testing "on a card-many attribute"
      (is (= 1 (count (both '[:find ?s :in $ ?si :where [?s :stmt/item ?si] [?s :stmt/tag _]]
                            db a)))))
    (testing "an aggregate and a scalar find"
      (is (= [[2]] (vec (both '[:find (count ?s) :in $ ?si :where
                                [?s :stmt/item ?si] [?s :stmt/status _]]
                              db a))))
      (is (some? (both '[:find ?s . :in $ ?si :where [?s :stmt/item ?si] [?s :stmt/status _]]
                       db a))))))

;; ---------------------------------------------------------------------------
;; Found reviewing the fix above. Each of these the planner answered wrongly.

(defn- x-db
  "a: k \"a\", i 1, s 4, m #{2 3}, y foo.  b: k \"b\", i 1, y _x.  c: k \"c\", i 1."
  []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write :keep-history? false}
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (d/transact conn [{:db/ident :x/k :db/valueType :db.type/string :db/cardinality :db.cardinality/one
                       :db/unique :db.unique/identity}
                      {:db/ident :x/i :db/valueType :db.type/long :db/cardinality :db.cardinality/one
                       :db/index true}
                      {:db/ident :x/s :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                      {:db/ident :x/m :db/valueType :db.type/long :db/cardinality :db.cardinality/many
                       :db/index true}
                      {:db/ident :x/y :db/valueType :db.type/symbol :db/cardinality :db.cardinality/one
                       :db/index true}])
    (d/transact conn [{:x/k "a" :x/i 1 :x/s 4 :x/m [2 3] :x/y 'foo}
                      {:x/k "b" :x/i 1 :x/y '_x}
                      {:x/k "c" :x/i 1}])
    @conn))

(defn- names
  "The :x/k of every entity a query returns in its first column."
  [db result]
  (into #{} (map #(:x/k (d/entity db (first %)))) result))

(deftest a-symbol-passed-as-data-is-a-value
  ;; In a clause `_x` is a blank and `?q` a variable; as an input VALUE they are
  ;; symbols to compare with what is stored.
  (let [db (x-db)]
    (is (= #{"b"} (names db (both '[:find ?e :in $ ?s :where [?e :x/y ?s]] db '_x))))
    (doseq [sym '[_x _ ?q]]
      (is (= #{} (both '[:find ?e :in $ ?s :where [?e :x/k "a"] [?e :x/y ?s] [(identity ?e) ?z]]
                       db sym))
          (str "a stores foo, not " sym)))
    (is (= #{"a"} (names db (both '[:find ?e :in $ ?s :where
                                    [?e :x/k "a"] [?e :x/y ?s] [(identity ?e) ?z]]
                                  db 'foo))))
    (testing "in a collection and in a tuple"
      (is (= #{"b"} (names db (both '[:find ?e :in $ [?s ...] :where [?e :x/y ?s]] db ['_x]))))
      (is (= #{} (both '[:find ?e :in $ [?s ?k] :where [?e :x/k ?k] [?e :x/y ?s] [(identity ?e) ?z]]
                       db ['_x "a"]))))))

(deftest an-entity-the-caller-bound-is-a-constraint
  ;; c has :x/i 1 and no :x/s. The point program took the entity from the index
  ;; hit and answered with a, which the caller never asked about.
  (let [db (x-db)
        a (d/q '[:find ?e . :where [?e :x/k "a"]] db)
        c (d/q '[:find ?e . :where [?e :x/k "c"]] db)]
    (is (= #{} (both '[:find ?e :in $ ?e ?i :where [?e :x/i ?i] [?e :x/s _]] db c 1)))
    (is (= #{} (both '[:find ?e ?v :in $ ?e ?i :where [?e :x/i ?i] [?e :x/s ?v]] db c 1)))
    (is (= #{} (both '[:find ?e :in $ ?e ?i :where [?e :x/i ?i] [?e :x/s 4]] db c 1)))
    (is (= #{[a 4]} (both '[:find ?e ?v :in $ ?e ?i :where [?e :x/i ?i] [?e :x/s ?v]] db a 1)))))

(deftest a-fixed-added-slot-is-compared
  ;; A current database holds assertions only: `false` matches nothing.
  (let [db (x-db)
        a (d/q '[:find ?e . :where [?e :x/k "a"]] db)]
    (doseq [query '[[:find ?e :where [?e :x/i 1] [?e :x/k _ nil false]]
                    [:find ?e :where [?e :x/i 1 nil false] [?e :x/k _]]
                    [:find ?e :where [?e :x/i 1] [?e :x/k ?k nil false]]
                    [:find ?e :where [?e :x/i 1] [?e :x/k _ _ false]]
                    [:find ?e :where [?e :x/k "a" _ false]]]]
      (is (= #{} (both query db)) (pr-str query)))
    (is (= #{} (both (into '[:find ?v :where] [[a :x/s '?v '_ false]]) db)))
    (is (= #{} (both '[:find ?e :in $ ?i :where [?e :x/i ?i] [?e :x/k _ nil false]] db 1)))
    (testing "`true` and a negated `false` keep every row"
      (is (= #{"a"} (names db (both '[:find ?e :where [?e :x/i 1] [?e :x/s _ _ true]] db))))
      (is (= #{"a" "b" "c"}
             (names db (both '[:find ?e :where [?e :x/i 1] (not [?e :x/k _ _ false])] db)))))))

(deftest an-ignored-card-many-value-yields-each-entity-once
  ;; a has two :x/m values. The result is a set, so membership alone hides a
  ;; second copy of the row: count the rows.
  (let [db (x-db)]
    (doseq [query '[[:find ?e :where [?e :x/m _]]
                    [:find ?e :where [?e :x/m _x]]
                    [:find ?e :where [?e :x/m]]
                    [:find ?e :where [?e :x/i 1] [?e :x/m _]]]]
      (is (= 1 (count (vec (both query db)))) (pr-str query)))
    (testing "with a limit: the duplicate used to take the second entity's place"
      (let [db (d/db-with db [{:x/k "d" :x/m [10]}])]
        (is (= 2 (count (set (d/q {:query '[:find ?e :where [?e :x/m _]] :args [db] :limit 2}))))))
      (is (= 2 (count (vec (both '[:find ?e ?m :where [?e :x/m ?m]] db)))))
      (is (= 1 (count (vec (both '[:find ?e :where [?e :x/m 2]] db))))))))
