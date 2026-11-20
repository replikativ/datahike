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
