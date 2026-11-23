(ns datahike.test.probe-bounds-test
  "A scan that is driven by seeks on bound entities or values applies the pattern's
   pushed-down predicates. The slice a full scan takes enforces `>=`, `<=` and `=`
   by its bounds; a seek never sees them."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.query :as q]))

(def ^:private n 20000)

(defn- db-with-scores [history?]
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write
             :keep-history? history?}]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn [{:db/ident :score :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/one :db/index true}
                        {:db/ident :grade :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/one :db/index true}
                        {:db/ident :label :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one}])
      ;; many low scores, so that the pattern is large and a seek per bound key
      ;; is the cheaper plan; a few high ones to bind
      (doseq [part (partition-all 2000 (range n))]
        (d/transact conn (vec (for [i part]
                                {:score (if (< i (- n 20)) (mod i 10) (+ 100 i))
                                 ;; one value for nearly all, so that a ground
                                 ;; value still leaves a large pattern
                                 :grade (if (< i (- n 20)) 3 9)
                                 :label (str "l" (mod i 7))}))))
      [cfg conn])))

(def ^:private queries
  '[[:find ?e ?s :in $ [?e ...] :where [?e :score ?s] [(<= ?s 10)]]
    [:find ?e ?s :in $ [?e ...] :where [?e :score ?s] [(>= ?s 5)]]
    [:find ?e ?s :in $ [?e ...] :where [?e :score ?s] [(< ?s 10)]]
    [:find ?e ?s :in $ [?e ...] :where [?e :score ?s] [(> ?s 5)]]
    [:find ?e ?s :in $ [?e ...] :where [?e :score ?s] [(= ?s 3)]]
    [:find ?e ?s :in $ [?e ...] :where [?e :score ?s] [(<= ?s 10)] [(>= ?s 2)]]
    [:find ?e ?s ?l :in $ [?e ...] :where [?e :score ?s] [(<= ?s 10)] [?e :label ?l]]
    [:find ?e ?l :in $ [?e ...] :where [?e :label ?l] [?e :score ?s] [(<= ?s 10)]]
    [:find ?e :in $ [?e ...] :where [?e :score 3]]
    ;; a ground value: the planned index (AVET) would enforce it by position
    [:find ?e :in $ [?e ...] :where [?e :grade 3]]
    [:find ?e ?l :in $ [?e ...] :where [?e :grade 3] [?e :label ?l]]
    [:find ?e ?s :in $ [?e ...] :where [?e :grade 3] [?e :score ?s] [(<= ?s 10)]]])

(def ^:private value-queries
  '[[:find ?e ?s :in $ [?s ...] :where [?e :score ?s] [(<= ?s 10)]]
    [:find ?e ?s :in $ [?s ...] :where [?e :score ?s] [(>= ?s 5)]]
    [:find ?e ?s ?l :in $ [?s ...] :where [?e :score ?s] [(<= ?s 10)] [?e :label ?l]]])

(defn- both [query db arg]
  [(d/q query db arg)
   (binding [q/*disable-planner* true] (d/q query db arg))])

(deftest seeks-on-bound-keys-keep-the-pushed-down-bounds
  (doseq [history? [false true]]
    (let [[cfg conn] (db-with-scores history?)
          db @conn
          high (vec (sort (map first (d/q '[:find ?e :where [?e :score ?s] [(> ?s 50)]] db))))
          low  (vec (take 3 (sort (map first (d/q '[:find ?e :where [?e :score 3]] db)))))
          ids  (into (vec (take 2 high)) low)]
      (is (= 20 (count high)) "precondition")
      (testing (str "entities bound, history " history?)
        (doseq [query queries]
          (let [[planned reference] (both query db ids)]
            (is (= reference planned) (pr-str query)))))
      (testing "the inclusive bound alone rejects the high scores"
        (is (= #{} (d/q (first queries) db (vec (take 2 high)))))
        (is (= 3 (count (d/q (first queries) db ids))))
        (is (= #{} (d/q '[:find ?e :in $ [?e ...] :where [?e :grade 3]] db (vec (take 2 high))))
            "and the ground value rejects the entities that have another"))
      (testing (str "values bound, history " history?)
        (let [vals [3 7 (+ 100 (- n 1)) (+ 100 (- n 2))]]
          (doseq [query value-queries]
            (let [[planned reference] (both query db vals)]
              (is (= reference planned) (pr-str query))
              (is (seq reference) "precondition: the reference finds rows")))))
      (when history?
        (testing "over the history"
          (doseq [query (concat (take 6 queries) (drop 9 queries))]
            (let [h (d/history db)
                  [planned reference] (both query h ids)]
              (is (= reference planned) (pr-str query))))))
      (d/release conn)
      (d/delete-database cfg))))
