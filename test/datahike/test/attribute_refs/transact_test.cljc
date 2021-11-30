(ns datahike.test.attribute-refs.transact-test
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer        [is deftest testing use-fixtures]])
   [datahike.api :as d]
   [datahike.test.attribute-refs.utils :refer [ref-db ref-e0
                                               setup-new-connection
                                               wrap-direct-datoms]]))

(deftest test-with
  (let [datoms  [[1 :name "Ivan"]
                 [1 :name "Petr"]
                 [1 :aka  "Devil"]
                 [1 :aka  "Tupen"]]
        db  (d/db-with ref-db (wrap-direct-datoms ref-db ref-e0 :db/add datoms))]
    (is (= (d/q '[:find ?v
                  :in $ ?e
                  :where [?e :name ?v]]
                db (+ ref-e0 1))
           #{["Petr"]}))
    (is (= (d/q '[:find ?v
                  :in $ ?e
                  :where [?e :aka ?v]]
                db (+ ref-e0 1))
           #{["Devil"] ["Tupen"]}))

    (testing "Retract"
      (let [datoms2 [[1 :name "Petr"]
                     [1 :aka "Devil"]]
            db2 (d/db-with db (wrap-direct-datoms ref-db ref-e0 :db/retract datoms2))]
        (is (= (d/q '[:find ?v
                      :in $ ?e
                      :where [?e :name ?v]]
                    db2 (+ ref-e0 1))
               #{}))
        (is (= (d/q '[:find ?v
                      :in $ ?e
                      :where [?e :aka ?v]]
                    db2 (+ ref-e0 1))
               #{["Tupen"]}))

        (is (= (into {} (d/entity db2 (+ ref-e0 1)))
               {:aka #{"Tupen"}}))))

    (testing "Cannot retract what's not there"
      (let [datoms3 [[1 :name "Ivan"]]
            db3 (d/db-with db (wrap-direct-datoms ref-db ref-e0 :db/retract datoms3))]
        (is (= (d/q '[:find ?v
                      :in $ ?e
                      :where [?e :name ?v]]
                    db3 (+ ref-e0 1))
               #{["Petr"]}))))))

(deftest test-retract-fns
  (let [db (d/db-with ref-db
                      [{:db/id (+ ref-e0 1), :name "Ivan", :age 15,
                        :aka ["X" "Y" "Z"], :friend (+ ref-e0 2)}
                       {:db/id (+ ref-e0 2), :name  "Petr", :age 37}])]
    (let [db (d/db-with db [[:db.fn/retractEntity (+ ref-e0 1)]])]
      (is (= (d/q '[:find ?a ?v
                    :in $ ?e
                    :where [?e ?a ?v]]
                  db (+ ref-e0 1))
             #{}))
      (is (= (d/q '[:find ?a ?v
                    :in $ ?e
                    :where [?e ?r ?v]
                    [?r :db/ident ?a]]
                  db (+ ref-e0 2))
             #{[:name "Petr"] [:age 37]})))

    (is (= (d/db-with db [[:db.fn/retractEntity (+ ref-e0 1)]])
           (d/db-with db [[:db/retractEntity (+ ref-e0 1)]])))

    (testing "Retract entity with incoming refs"
      (is (= (d/q '[:find ?e
                    :in $ ?e1
                    :where [?e1 :friend ?e]]
                  db (+ ref-e0 1))
             #{[(+ ref-e0 2)]}))

      (let [db2 (d/db-with db [[:db.fn/retractEntity (+ ref-e0 2)]])]
        (is (= (d/q '[:find ?e
                      :in $ ?e1
                      :where [?e1 :friend ?e]]
                    db2 (+ ref-e0 1))
               #{}))))

    (let [name-ref (get-in db [:ident-ref-map :name])
          db2 (d/db-with db [[:db.fn/retractAttribute (+ ref-e0 1) name-ref]])]
      (is (= (d/q '[:find ?a ?v
                    :in $ ?e
                    :where [?e ?r ?v]
                    [?r :db/ident ?a]]
                  db2 (+ ref-e0 1))
             #{[:age 15] [:aka "X"] [:aka "Y"] [:aka "Z"] [:friend (+ ref-e0 2)]}))
      (is (= (d/q '[:find ?a ?v
                    :in $ ?e
                    :where [?e ?r ?v]
                    [?r :db/ident ?a]]
                  db2 (+ ref-e0 2))
             #{[:name "Petr"] [:age 37]})))

    (let [aka-ref (get-in db [:ident-ref-map :aka])
          db2 (d/db-with db [[:db.fn/retractAttribute (+ ref-e0 1) aka-ref]])]
      (is (= (d/q '[:find ?a ?v
                    :in $ ?e
                    :where [?e ?r ?v]
                    [?r :db/ident ?a]]
                  db2 (+ ref-e0 1))
             #{[:name "Ivan"] [:age 15] [:friend (+ ref-e0 2)]}))
      (is (= (d/q '[:find ?a ?v
                    :in $ ?e
                    :where [?e ?r ?v]
                    [?r :db/ident ?a]]
                  db2 (+ ref-e0 2))
             #{[:name "Petr"] [:age 37]})))))

(deftest test-db-fn-cas
  (let [conn (setup-new-connection)
        e0 (:max-eid @conn)
        weight-ref (get-in @conn [:ident-ref-map :weight])]
    (d/transact conn [[:db/cas (+ e0 1) weight-ref nil 100]])
    (is (= (:weight (d/entity @conn (+ e0 1))) 100))
    (d/transact conn [[:db/add (+ e0 1) weight-ref 200]])
    (d/transact conn [[:db.fn/cas (+ e0 1) weight-ref 200 300]])
    (is (= (:weight (d/entity @conn (+ e0 1))) 300))
    (d/transact conn [[:db/cas (+ e0 1) weight-ref 300 400]])
    (is (= (:weight (d/entity @conn (+ e0 1))) 400))
    (is (thrown-msg? (str ":db.fn/cas failed on datom [" (+ ref-e0 1) " " weight-ref " 400], expected 200")
                     (d/transact conn [[:db.fn/cas (+ ref-e0 1) weight-ref 200 210]]))))

  (let [conn (setup-new-connection)
        e0 (:max-eid @conn)
        label-ref (get-in @conn [:ident-ref-map :label])]
    (d/transact conn [[:db/add (+ e0 1) label-ref :x]])
    (d/transact conn [[:db/add (+ e0 1) label-ref :y]])
    (d/transact conn [[:db.fn/cas (+ e0 1) label-ref :y :z]])
    (is (= (:label (d/entity @conn (+ e0 1))) #{:x :y :z}))
    (is (thrown-msg? (str ":db.fn/cas failed on datom [" (+ ref-e0 1) " " label-ref " (:x :y :z)], expected :s")
                     (d/transact conn [[:db.fn/cas (+ ref-e0 1) label-ref :s :t]]))))

  (let [conn (setup-new-connection)
        e0 (:max-eid @conn)
        name-ref (get-in @conn [:ident-ref-map :name])
        age-ref (get-in @conn [:ident-ref-map :age])]
    (d/transact conn [[:db.fn/retractEntity (+ e0 1)]])
    (d/transact conn [[:db/add (+ e0 1) name-ref "Ivan"]])
    (d/transact conn [[:db.fn/cas (+ e0 1) age-ref nil 42]])
    (is (= (:age (d/entity @conn (+ e0 1))) 42))
    (is (thrown-msg? (str ":db.fn/cas failed on datom [" (+ ref-e0 1) " " age-ref " 42], expected nil")
                     (d/transact conn [[:db.fn/cas (+ ref-e0 1) age-ref nil 4711]])))))

(deftest test-db-fn
  (let [conn (setup-new-connection)
        e0 (:max-eid @conn)
        had-birthday-ref (get-in @conn [:ident-ref-map :had-birthday])
        inc-age (fn [db name]
                  (if-let [[eid age] (first (d/q '{:find [?e ?age]
                                                   :in [$ ?name]
                                                   :where [[?e :name ?name]
                                                           [?e :age ?age]]}
                                                 db name))]
                    [{:db/id eid :age (inc age)} [:db/add eid had-birthday-ref true]]
                    (throw (ex-info (str "No entity with name: " name) {}))))]
    (d/transact conn [{:db/id (+ e0 1) :name "Ivan" :age 31}
                      {:db/id (+ e0 1) :name "Petr"}
                      {:db/id (+ e0 1) :aka "Devil"}
                      {:db/id (+ e0 1) :aka "Tupen"}])

    (is (= (d/q '[:find ?v ?a
                  :where [?e :name ?v]
                  [?e :age ?a]] @conn)
           #{["Petr" 31]}))
    (is (= (d/q '[:find ?v
                  :where [?e :aka ?v]] @conn)
           #{["Devil"] ["Tupen"]}))
    (is (thrown-msg? "No entity with name: Bob"
                     (d/transact conn [[:db.fn/call inc-age "Bob"]])))
    (let [{:keys [db-after]} (d/transact conn [[:db.fn/call inc-age "Petr"]])
          e (d/entity db-after (+ e0 1))]
      (is (= (:age e) 32))
      (is (:had-birthday e)))))
