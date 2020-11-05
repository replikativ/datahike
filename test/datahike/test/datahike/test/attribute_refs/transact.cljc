(ns datahike.test.attribute-refs.transact
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [datahike.api :as da]
   [datahike.core :as d]))

(def config {:attribute-refs? true})


(def test-schema
  [{:db/ident       :aka
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/string}
   {:db/ident       :label
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/string}
   {:db/ident       :name
    :db/cardinality :db.cardinality/one
    :db/valueType   :db.type/string
    :db/unique      :db.unique/identity}
   {:db/ident       :age
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

       #_(is (= (into {} (d/entity db2 (+ test-e0 1))) {:aka #{"Tupen"}})) ;; TODO: adjust entity function
        ))

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
                      [{:db/id (+ test-e0 1), :name "Ivan", :age 15, :aka ["X" "Y" "Z"], :friend (+ test-e0 2)}
                       {:db/id (+ test-e0 2), :name  "Petr", :age 37}])]
    (let [db (d/db-with db [[:db.fn/retractEntity (+ test-e0 1)]])]
      (is (= (d/q '[:find ?a ?v
                    :in $ ?e
                    :where [?e ?a ?v]]
                  db (+ test-e0 1))
             #{}))
      #_(is (= (d/q '[:find ?a ?v
                    :in $ ?e
                    :where [?e ?a ?v]]
                  db (+ test-e0 2))
             #{[:name "Petr"] [:age 37]})))                 ; TODO: refs or kws?

    #_(is (= (d/db-with db [[:db.fn/retractEntity (+ test-e0 1)]]) ; TODO: Why not equal?
           (d/db-with db [[:db/retractEntity (+ test-e0 1)]])))

    (testing "Retract entitiy with incoming refs"
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

    #_(let [db (d/db-with db [[:db.fn/retractAttribute (+ test-e0 1) :name]])]
      (is (= (d/q '[:find ?a ?v
                    :in $ ?e1
                    :where [?e1 ?a ?v]]
                  db (+ test-e0 1))
             #{[:age 15] [:aka "X"] [:aka "Y"] [:aka "Z"] [:friend (+ test-e0 2)]}))
      (is (= (d/q '[:find ?a ?v
                    :in $ ?e2
                    :where [?e2 ?a ?v]]
                  db (+ test-e0 2))
             #{[:name "Petr"] [:age 37]})))                 ; TODO: return kw or ref for attribute?

    #_(let [db (d/db-with db [[:db.fn/retractAttribute (+ test-e0 1) :aka]])]
      (is (= (d/q '[:find ?a ?v
                    :in $ ?e
                    :where [?e ?a ?v]]
                  db(+ test-e0 1))
             #{[:name "Ivan"] [:age 15] [:friend (+ test-e0 2)]}))
      (is (= (d/q '[:find ?a ?v
                    :in $ ?e
                    :where [?e ?a ?v]]
                  db (+ test-e0 2))
             #{[:name "Petr"] [:age 37]})))))

#_(deftest test-db-fn-cas
  (let [conn (:conn test-conn)]
    (da/transact @conn [[:db/cas 1 :weight nil 100]])
    (is (= (:weight (d/entity @conn 1)) 100))
    (da/transact @conn [[:db/add 1 :weight 200]])
    (da/transact @conn [[:db.fn/cas 1 :weight 200 300]])
    (is (= (:weight (d/entity @conn 1)) 300))
    (da/transact @conn [[:db/cas 1 :weight 300 400]])
    (is (= (:weight (d/entity @conn 1)) 400))
    (is (thrown-msg? ":db.fn/cas failed on datom [1 :weight 400], expected 200"
                     (d/transact! conn [[:db.fn/cas 1 :weight 200 210]]))))

  #_(let [conn (:conn test-conn)]
    (d/transact! conn [[:db/add 1 :label :x]])
    (d/transact! conn [[:db/add 1 :label :y]])
    (d/transact! conn [[:db.fn/cas 1 :label :y :z]])
    (is (= (:label (d/entity @conn 1)) #{:x :y :z}))
    (is (thrown-msg? ":db.fn/cas failed on datom [1 :label (:x :y :z)], expected :s"
                     (d/transact! conn [[:db.fn/cas 1 :label :s :t]]))))

  #_(let [conn(:conn test-conn)]
    (d/transact! conn [[:db/add 1 :name "Ivan"]])
    (d/transact! conn [[:db.fn/cas 1 :age nil 42]])
    (is (= (:age (d/entity @conn 1)) 42))
    (is (thrown-msg? ":db.fn/cas failed on datom [1 :age 42], expected nil"
                     (d/transact! conn [[:db.fn/cas 1 :age nil 4711]]))))

  #_(let [conn (:conn test-conn)]
    (is (thrown-msg? "Can't use tempid in '[:db.fn/cas -1 :attr nil :val]'. Tempids are allowed in :db/add only"
                     (d/transact! conn [[:db/add    -1 :name "Ivan"]
                                        [:db.fn/cas -1 :attr nil :val]])))))

(deftest test-db-fn
  (let [conn (:conn test-conn)
        inc-age (fn [db name]
                  (if-let [[eid age] (first (d/q '{:find [?e ?age]
                                                   :in [$ ?name]
                                                   :where [[?e :name ?name]
                                                           [?e :age ?age]]}
                                                 db name))]
                    [{:db/id eid :age (inc age)} [:db/add eid :had-birthday true]]
                    (throw (ex-info (str "No entity with name: " name) {}))))]
    (da/transact @conn [{:db/id (+ test-e0 1) :name "Ivan" :age 31}
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
          e (d/entity db-after 1)]                          ;; TODO: d/entity
      (is (= (:age e) 32))
      (is (:had-birthday e)))))


