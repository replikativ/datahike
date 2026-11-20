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
