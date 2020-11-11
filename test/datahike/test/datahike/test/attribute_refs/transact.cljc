(ns datahike.test.attribute-refs.transact
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer        [is deftest testing]])
   [datahike.api :as da]
   [datahike.core :as d]))

(def config {:attribute-refs? true})

(def test-schema
  [{:db/ident       :aka
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/string}
   {:db/ident       :label
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/keyword}
   {:db/ident       :name
    :db/cardinality :db.cardinality/one
    :db/valueType   :db.type/string
    :db/unique      :db.unique/identity}
   {:db/ident       :age
    :db/cardinality :db.cardinality/one
    :db/valueType   :db.type/long}
   {:db/ident       :had-birthday
    :db/cardinality :db.cardinality/one
    :db/valueType   :db.type/boolean}
   {:db/ident       :weight
    :db/cardinality :db.cardinality/one
    :db/valueType   :db.type/long}
   {:db/ident       :friend
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/ref}])

(defn wrap-ddatoms [db offset op datoms]
  (let [irmap (:ident-ref-map db)]
    (mapv (fn [[e a v]] [op (+ offset e) (get irmap a) v])
          datoms)))

(def test-conn
  (do (da/delete-database config)
      (da/create-database config)
      (let [conn (da/connect config)
            _ (da/transact conn test-schema)
            max-eid (:max-eid @conn)]
        {:conn conn :db @conn :e0 max-eid})))

(def test-db (:db test-conn))
(def test-e0 (:e0 test-conn))

(deftest test-with
  (let [db  (d/db-with test-db
                       (wrap-ddatoms test-db test-e0 :db/add
                                     [[1 :name "Ivan"]
                                      [1 :name "Petr"]
                                      [1 :aka  "Devil"]
                                      [1 :aka  "Tupen"]]))]

    (is (= (d/q '[:find ?v
                  :in $ ?e
                  :where [?e :name ?v]]
                db (+ test-e0 1))
           #{["Petr"]}))
    (is (= (d/q '[:find ?v
                  :in $ ?e
                  :where [?e :aka ?v]]
                db (+ test-e0 1))
           #{["Devil"] ["Tupen"]}))

    (testing "Retract"
      (let [db2 (d/db-with db (wrap-ddatoms test-db test-e0 :db/retract
                                            [[1 :name "Petr"]
                                             [1 :aka "Devil"]]))]

        (is (= (d/q '[:find ?v
                      :in $ ?e
                      :where [?e :name ?v]]
                    db2 (+ test-e0 1))
               #{}))
        (is (= (d/q '[:find ?v
                      :in $ ?e
                      :where [?e :aka ?v]]
                    db2 (+ test-e0 1))
               #{["Tupen"]}))

        (is (= (into {} (d/entity db2 (+ test-e0 1)))
               {:aka #{"Tupen"}}))))

    (testing "Cannot retract what's not there"
      (let [db3 (d/db-with db (wrap-ddatoms test-db test-e0 :db/retract
                                            [[1 :name "Ivan"]]))]
        (is (= (d/q '[:find ?v
                      :in $ ?e
                      :where [?e :name ?v]]
                    db3 (+ test-e0 1))
               #{["Petr"]}))))))

(deftest test-retract-fns
  (let [db (d/db-with test-db
                      [{:db/id (+ test-e0 1), :name "Ivan", :age 15,
                        :aka ["X" "Y" "Z"], :friend (+ test-e0 2)}
                       {:db/id (+ test-e0 2), :name  "Petr", :age 37}])]
    (let [db (d/db-with db [[:db.fn/retractEntity (+ test-e0 1)]])]
      (is (= (d/q '[:find ?a ?v
                    :in $ ?e
                    :where [?e ?a ?v]]
                  db (+ test-e0 1))
             #{}))
      (is (= (d/q '[:find ?a ?v
                      :in $ ?e
                      :where [?e ?r ?v]
                             [?r :db/ident ?a]]
                    db (+ test-e0 2))
               #{[:name "Petr"] [:age 37]})))

    #_(is (= (d/db-with db [[:db.fn/retractEntity (+ test-e0 1)]]) ; TODO: Why not equal?
             (d/db-with db [[:db/retractEntity (+ test-e0 1)]])))

    (testing "Retract entity with incoming refs"
      (is (= (d/q '[:find ?e
                    :in $ ?e1
                    :where [?e1 :friend ?e]]
                  db (+ test-e0 1))
             #{[(+ test-e0 2)]}))

      (let [db (d/db-with db [[:db.fn/retractEntity (+ test-e0 2)]])]
        (is (= (d/q '[:find ?e
                      :in $ ?e1
                      :where [?e1 :friend ?e]]
                    db (+ test-e0 1))
               #{}))))

    (let [db (d/db-with db [[:db.fn/retractAttribute (+ test-e0 1) :name]])]
        (is (= (d/q '[:find ?a ?v
                      :in $ ?e1
                      :where [?e1 ?r ?v]
                             [?r :db/ident ?a]]
                    db (+ test-e0 1))
               #{[:age 15] [:aka "X"] [:aka "Y"] [:aka "Z"] [:friend (+ test-e0 2)]}))
        (is (= (d/q '[:find ?a ?v
                      :in $ ?e2
                      :where [?e2 ?r ?v]
                             [?r :db/ident ?a]]
                    db (+ test-e0 2))
               #{[:name "Petr"] [:age 37]})))

    (let [db (d/db-with db [[:db.fn/retractAttribute (+ test-e0 1) :aka]])]
        (is (= (d/q '[:find ?a ?v
                      :in $ ?e
                      :where [?e ?r ?v]
                             [?r :db/ident ?a]]
                    db (+ test-e0 1))
               #{[:name "Ivan"] [:age 15] [:friend (+ test-e0 2)]}))
        (is (= (d/q '[:find ?a ?v
                      :in $ ?e
                      :where [?e ?r ?v]
                             [?r :db/ident ?a]]
                    db (+ test-e0 2))
               #{[:name "Petr"] [:age 37]})))))

(deftest test-db-fn-cas
    (let [conn (:conn test-conn)
          weight-ref (get-in @conn [:ident-ref-map :weight])]
      (da/transact conn [[:db/cas (+ test-e0 1) weight-ref nil 100]])
      (is (= (:weight (d/entity @conn (+ test-e0 1))) 100))
      (da/transact conn [[:db/add (+ test-e0 1) weight-ref 200]])
      (da/transact conn [[:db.fn/cas (+ test-e0 1) weight-ref 200 300]])
      (is (= (:weight (d/entity @conn (+ test-e0 1))) 300))
      (da/transact conn [[:db/cas (+ test-e0 1) weight-ref 300 400]])
      (is (= (:weight (d/entity @conn (+ test-e0 1))) 400))
      #_(is (thrown-msg? (str ":db.fn/cas failed on datom [" (+ test-e0 1) " " weight-ref" 400], expected 200")
                       (da/transact conn [[:db.fn/cas (+ test-e0 1) weight-ref 200 210]]))))

    #_(let [conn (:conn test-conn)
          label-ref (get-in @conn [:ident-ref-map :label])]
        (da/transact conn [[:db/add (+ test-e0 1) label-ref :x]])
        (da/transact conn [[:db/add (+ test-e0 1) label-ref :y]])
        (da/transact conn [[:db.fn/cas (+ test-e0 1) label-ref :y :z]])
        (is (= (:label (d/entity @conn (+ test-e0 1))) #{:x :y :z}))
        #_(is (thrown-msg? (str ":db.fn/cas failed on datom [" (+ test-e0 1) " " label-ref " (:x :y :z)], expected :s")
                         (da/transact conn [[:db.fn/cas (+ test-e0 1) label-ref :s :t]]))))

    #_(let [conn (:conn test-conn)
          name-ref (get-in @conn [:ident-ref-map :name])
          age-ref (get-in @conn [:ident-ref-map :age])]
        (da/transact conn [[:db.fn/retractEntity (+ test-e0 1)]])
        (da/transact conn [[:db/add (+ test-e0 1) name-ref "Ivan"]])
        (da/transact conn [[:db.fn/cas (+ test-e0 1) age-ref nil 42]])
        (is (= (:age (d/entity @conn (+ test-e0 1))) 42))
        #_(is (thrown-msg? (str ":db.fn/cas failed on datom [" (+ test-e0 1) " " age-ref" 42], expected ni")
                         (da/transact conn [[:db.fn/cas (+ test-e0 1) age-ref nil 4711]])))))

(deftest test-db-fn
  (let [conn (:conn test-conn)
        had-birthday-ref (get-in @conn [:ident-ref-map :had-birthday])
        inc-age (fn [db name]
                  (if-let [[eid age] (first (d/q '{:find [?e ?age]
                                                   :in [$ ?name]
                                                   :where [[?e ?r ?name]
                                                           [?r :db/ident :name]
                                                           [?e ?r2 ?age]
                                                           [?r2 :db/ident :age]]}
                                                 db name))]
                    [{:db/id eid :age (inc age)} [:db/add eid had-birthday-ref true]]
                    (throw (ex-info (str "No entity with name: " name) {}))))]
    (da/transact conn [{:db/id (+ test-e0 1) :name "Ivan" :age 31}
                       {:db/id (+ test-e0 1) :name "Petr"}
                       {:db/id (+ test-e0 1) :aka "Devil"}
                       {:db/id (+ test-e0 1) :aka "Tupen"}])

    (is (= (d/q '[:find ?v ?a
                  :where [?e :name ?v]
                  [?e :age ?a]] @conn)
           #{["Petr" 31]}))
    (is (= (d/q '[:find ?v
                  :where [?e :aka ?v]] @conn)
           #{["Devil"] ["Tupen"]}))
    #_(is (thrown-msg? "No entity with name: Bob"
                       (d/transact! conn [[:db.fn/call inc-age "Bob"]])))
    #_(let [{:keys [db-after]} (d/transact! conn [[:db.fn/call inc-age "Petr"]])
            e (d/entity db-after 1)]
        (is (= (:age e) 32))
        (is (:had-birthday e)))))
