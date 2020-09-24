(ns datahike.test.transact
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [datahike.test.core :as tdc]
   [datahike.core :as d]))

(let [db (-> (d/empty-db)
             (d/db-with [(d/datom tdc/e1 :name "Oleg")
                         (d/datom tdc/e1 :age  17)
                         (d/datom tdc/e1 :name "Oleg" d/tx0 false)]))]
  (is (= [[tdc/e1 :age 17 d/tx0]]
         (map (juxt :e :a :v :tx)
              (d/datoms db :eavt)))))

(let [db  (-> (d/empty-db {:aka {:db/cardinality :db.cardinality/many}
                           :name {:db/cardinality :db.cardinality/one}})
              (d/db-with [[:db/add tdc/e1 :name "Ivan"]])
              (d/db-with [[:db/add tdc/e1 :name "Petr"]])
              (d/db-with [[:db/add tdc/e1 :aka  "Devil"]])
              (d/db-with [[:db/add tdc/e1 :aka  "Tupen"]]))
      db2 (-> db
              (d/db-with [[:db/retract tdc/e1 :name "Petr"]])
              (d/db-with [[:db/retract tdc/e1 :aka  "Devil"]]))]

  (is (= (d/q '[:find ?v
                :in $ ?e
                :where [?e :name ?v]]
              db2 tdc/e1)
         #{}))
  (is (= (d/q '[:find ?v
                :in $ ?e
                :where [?e :aka ?v]]
              db2 tdc/e1)
         #{["Tupen"]}))

  (is (= (into {} (d/entity db2 tdc/e1)) {:aka #{"Tupen"}})))

(deftest test-with
  (let [db  (-> (d/empty-db {:aka {:db/cardinality :db.cardinality/many}
                             :name {:db/cardinality :db.cardinality/one}})
                (d/db-with [[:db/add tdc/e1 :name "Ivan"]])
                (d/db-with [[:db/add tdc/e1 :name "Petr"]])
                (d/db-with [[:db/add tdc/e1 :aka  "Devil"]])
                (d/db-with [[:db/add tdc/e1 :aka  "Tupen"]]))]

    (is (= (d/q '[:find ?v
                  :in $ ?e
                  :where [?e :name ?v]]
                db tdc/e1)
           #{["Petr"]}))
    (is (= (d/q '[:find ?v
                  :in $ ?e
                  :where [?e :aka ?v]]
                db tdc/e1)
           #{["Devil"] ["Tupen"]}))

    (testing "Retract"
      (let [db2 (-> db
                    (d/db-with [[:db/retract tdc/e1 :name "Petr"]])
                    (d/db-with [[:db/retract tdc/e1 :aka  "Devil"]]))]

        (is (= (d/q '[:find ?v
                      :in $ ?e
                      :where [?e :name ?v]]
                    db2 tdc/e1)
               #{}))
        (is (= (d/q '[:find ?v
                      :in $ ?e
                      :where [?e :aka ?v]]
                    db2 tdc/e1)
               #{["Tupen"]}))

        (is (= (into {} (d/entity db2 tdc/e1)) {:aka #{"Tupen"}}))))

    (testing "Cannot retract what's not there"
      (let [db3  (-> db
                     (d/db-with [[:db/retract tdc/e1 :name "Ivan"]]))]
        (is (= (d/q '[:find ?v
                      :in $ ?e
                      :where [?e :name ?v]]
                    db3 tdc/e1)
               #{["Petr"]})))))                             ;; TODO: finds: ivan, petr
  (testing "Skipping nils in tx"
    (let [db (-> (d/empty-db)
                 (d/db-with [[:db/add tdc/e1 :attr 2]
                             nil
                             [:db/add tdc/e3 :attr 4]]))]
      (is (= [[tdc/e1 :attr 2], [tdc/e3 :attr 4]]
             (map (juxt :e :a :v) (d/datoms db :eavt)))))))

(deftest test-with-datoms
  (testing "keeps tx number"
    (let [db (-> (d/empty-db)
                 (d/db-with [(d/datom tdc/e1 :name "Oleg")
                             (d/datom tdc/e1 :age  17 (+ 1 d/tx0))
                             [:db/add tdc/e1 :aka  "x" (+ 2 d/tx0)]]))]
      (is (= [[tdc/e1 :age  17     (+ 1 d/tx0)]
              [tdc/e1 :aka  "x"    (+ 2 d/tx0)]
              [tdc/e1 :name "Oleg" d/tx0]]
             (map (juxt :e :a :v :tx)
                  (d/datoms db :eavt))))))

  (testing "retraction"
    (let [db (-> (d/empty-db)
                 (d/db-with [(d/datom tdc/e1 :name "Oleg")
                             (d/datom tdc/e1 :age  17)
                             (d/datom tdc/e1 :name "Oleg" d/tx0 false)]))]
      (is (= [[tdc/e1 :age 17 d/tx0]]
             (map (juxt :e :a :v :tx)
                  (d/datoms db :eavt)))))))

(deftest test-retract-fns
  (let [db (-> (d/empty-db {:aka    {:db/cardinality :db.cardinality/many}
                            :friend {:db/valueType :db.type/ref}})
               (d/db-with [{:db/id tdc/e1, :name  "Ivan", :age 15, :aka ["X" "Y" "Z"], :friend tdc/e2}
                           {:db/id tdc/e2, :name  "Petr", :age 37}]))]
    (let [db (d/db-with db [[:db.fn/retractEntity tdc/e1]])]
      (is (= (d/q '[:find ?a ?v
                    :in $ ?e
                    :where [?e ?a ?v]]
                  db tdc/e1)
             #{}))
      (is (= (d/q '[:find ?a ?v
                    :in $ ?e
                    :where [?e ?a ?v]]
                  db tdc/e2)
             #{[:name "Petr"] [:age 37]})))

    (is (= (d/db-with db [[:db.fn/retractEntity tdc/e1]])
           (d/db-with db [[:db/retractEntity tdc/e1]])))

    (testing "Retract entitiy with incoming refs"
      (is (= (d/q '[:find ?e
                    :in $ ?e1
                    :where [?e1 :friend ?e]]
                  db tdc/e1)
             #{[tdc/e2]}))

      (let [db (d/db-with db [[:db.fn/retractEntity tdc/e2]])]
        (is (= (d/q '[:find ?e
                      :in $ ?e1
                      :where [?e1 :friend ?e]]
                    db tdc/e1)
               #{}))))

    (let [db (d/db-with db [[:db.fn/retractAttribute tdc/e1 :name]])]
      (is (= (d/q '[:find ?a ?v
                    :in $ ?e1
                    :where [?e1 ?a ?v]]
                  db tdc/e1)
             #{[:age 15] [:aka "X"] [:aka "Y"] [:aka "Z"] [:friend tdc/e2]}))
      (is (= (d/q '[:find ?a ?v
                    :in $ ?e2
                    :where [?e2 ?a ?v]]
                  db tdc/e2)
             #{[:name "Petr"] [:age 37]})))

    (let [db (d/db-with db [[:db.fn/retractAttribute tdc/e1 :aka]])]
      (is (= (d/q '[:find ?a ?v
                    :in $ ?e
                    :where [?e ?a ?v]]
                  db tdc/e1)
             #{[:name "Ivan"] [:age 15] [:friend tdc/e2]}))
      (is (= (d/q '[:find ?a ?v
                    :in $ ?e
                    :where [?e ?a ?v]]
                  db tdc/e2)
             #{[:name "Petr"] [:age 37]})))))

(deftest test-retract-fns-not-found
  (let [db  (-> (d/empty-db {:name {:db/unique :db.unique/identity}})
                (d/db-with  [[:db/add tdc/e1 :name "Ivan"]]))
        all #(vec (d/datoms % :eavt))]
    (are [op] (= [(d/datom tdc/e1 :name "Ivan")]
                 (all (d/db-with db [op])))
      [:db/retract             tdc/e2 :name "Petr"]
      [:db.fn/retractAttribute tdc/e2 :name]
      [:db.fn/retractEntity    tdc/e2]
      [:db/retractEntity       tdc/e2]
      [:db/retract             [:name "Petr"] :name "Petr"]
      [:db.fn/retractAttribute [:name "Petr"] :name]
      [:db.fn/retractEntity    [:name "Petr"]])

    (are [op] (= [[] []]
                 [(all (d/db-with db [op]))
                  (all (d/db-with db [op op]))]) ;; idempotency
      [:db/retract             tdc/e1 :name "Ivan"]
      [:db.fn/retractAttribute tdc/e1 :name]
      [:db.fn/retractEntity    tdc/e1]
      [:db/retractEntity       tdc/e1]
      [:db/retract             [:name "Ivan"] :name "Ivan"]
      [:db.fn/retractAttribute [:name "Ivan"] :name]
      [:db.fn/retractEntity    [:name "Ivan"]])))

(deftest test-transact!
  (let [conn (d/create-conn {:aka {:db/cardinality :db.cardinality/many}})]
    (d/transact! conn [[:db/add tdc/e1 :name "Ivan"]])
    (d/transact! conn [[:db/add tdc/e1 :name "Petr"]])
    (d/transact! conn [[:db/add tdc/e1 :aka  "Devil"]])
    (d/transact! conn [[:db/add tdc/e1 :aka  "Tupen"]])

    (is (= (d/q '[:find ?v
                  :in $ ?e
                  :where [?e :name ?v]]
                @conn tdc/e1)
           #{["Petr"]}))

    (is (= (d/q '[:find ?v
                  :in $ ?e
                  :where [?e :aka ?v]]
                @conn tdc/e1)
           #{["Devil"] ["Tupen"]}))))

(deftest test-db-fn-cas
  (let [conn (d/create-conn)]
    (d/transact! conn [[:db/cas tdc/e1 :weight nil 100]])
    (is (= (:weight (d/entity @conn tdc/e1)) 100))
    (d/transact! conn [[:db/add tdc/e1 :weight 200]])
    (d/transact! conn [[:db.fn/cas tdc/e1 :weight 200 300]])
    (is (= (:weight (d/entity @conn tdc/e1)) 300))
    (d/transact! conn [[:db/cas tdc/e1 :weight 300 400]])
    (is (= (:weight (d/entity @conn tdc/e1)) 400))
    (is (thrown-msg? (str ":db.fn/cas failed on datom [" tdc/e1 " :weight 400], expected 200")
                     (d/transact! conn [[:db.fn/cas tdc/e1 :weight 200 210]]))))

  (let [conn (d/create-conn {:label {:db/cardinality :db.cardinality/many}})]
    (d/transact! conn [[:db/add tdc/e1 :label :x]])
    (d/transact! conn [[:db/add tdc/e1 :label :y]])
    (d/transact! conn [[:db.fn/cas tdc/e1 :label :y :z]])
    (is (= (:label (d/entity @conn tdc/e1)) #{:x :y :z}))
    (is (thrown-msg? (str ":db.fn/cas failed on datom [" tdc/e1 " :label (:x :y :z)], expected :s")
                     (d/transact! conn [[:db.fn/cas tdc/e1 :label :s :t]]))))

  (let [conn (d/create-conn)]
    (d/transact! conn [[:db/add tdc/e1 :name "Ivan"]])
    (d/transact! conn [[:db.fn/cas tdc/e1 :age nil 42]])
    (is (= (:age (d/entity @conn tdc/e1)) 42))
    (is (thrown-msg? (str ":db.fn/cas failed on datom [" tdc/e1 " :age 42], expected nil")
                     (d/transact! conn [[:db.fn/cas tdc/e1 :age nil 4711]]))))

  (let [conn (d/create-conn)]
    (is (thrown-msg? "Can't use tempid in '[:db.fn/cas -1 :attr nil :val]'. Tempids are allowed in :db/add only"
                     (d/transact! conn [[:db/add    -1 :name "Ivan"]
                                        [:db.fn/cas -1 :attr nil :val]])))))

(deftest test-db-fn
  (let [conn (d/create-conn {:aka {:db/cardinality :db.cardinality/many}})
        inc-age (fn [db name]
                  (if-let [[eid age] (first (d/q '{:find [?e ?age]
                                                   :in [$ ?name]
                                                   :where [[?e :name ?name]
                                                           [?e :age ?age]]}
                                                 db name))]
                    [{:db/id eid :age (inc age)} [:db/add eid :had-birthday true]]
                    (throw (ex-info (str "No entity with name: " name) {}))))]
    (d/transact! conn [{:db/id tdc/e1 :name "Ivan" :age 31}])
    (d/transact! conn [[:db/add tdc/e1 :name "Petr"]])
    (d/transact! conn [[:db/add tdc/e1 :aka  "Devil"]])
    (d/transact! conn [[:db/add tdc/e1 :aka  "Tupen"]])
    (is (= (d/q '[:find ?v ?a
                  :where [?e :name ?v]
                  [?e :age ?a]]
                @conn)
           #{["Petr" 31]}))
    (is (= (d/q '[:find ?v
                  :where [?e :aka ?v]]
                @conn)
           #{["Devil"] ["Tupen"]}))
    (is (thrown-msg? "No entity with name: Bob"
                     (d/transact! conn [[:db.fn/call inc-age "Bob"]])))
    (let [{:keys [db-after]} (d/transact! conn [[:db.fn/call inc-age "Petr"]])
          e (d/entity db-after tdc/e1)]
      (is (= (:age e) 32))
      (is (:had-birthday e)))))

#_(deftest test-db-ident-fn ;; TODO: check for :db/ident support within hhtree
    (let [conn    (d/create-conn {:name {:db/unique :db.unique/identity}})
          inc-age (fn [db name]
                    (if-some [ent (d/entity db [:name name])]
                      [{:db/id (:db/id ent)
                        :age   (inc (:age ent))}
                       [:db/add (:db/id ent) :had-birthday true]]
                      (throw (ex-info (str "No entity with name: " name) {}))))]
      (d/transact! conn [{:db/id    1
                          :name     "Petr"
                          :age      31
                          :db/ident :Petr}
                         {:db/ident :inc-age
                          :db/fn    inc-age}])
      (is (thrown-msg? "Can’t find entity for transaction fn :unknown-fn"
                       (d/transact! conn [[:unknown-fn]])))
      (is (thrown-msg? "Entity :Petr expected to have :db/fn attribute with fn? value"
                       (d/transact! conn [[:Petr]])))
      (is (thrown-msg? "No entity with name: Bob"
                       (d/transact! conn [[:inc-age "Bob"]])))
      (d/transact! conn [[:inc-age "Petr"]])
      (let [e (d/entity @conn 1)]
        (is (= (:age e) 32))
        (is (:had-birthday e)))))

(deftest test-resolve-eid
  (let [conn (d/create-conn)
        t1   (d/transact! conn [[:db/add -1 :name "Ivan"]
                                [:db/add -1 :age 19]
                                [:db/add -2 :name "Petr"]
                                [:db/add -2 :age 22]])
        t2   (d/transact! conn [[:db/add "Serg" :name "Sergey"]
                                [:db/add "Serg" :age 30]])]
    (is (= (:tempids t1) {-1 (+ d/e0 1), -2 (+ d/e0 2), :db/current-tx (+ d/tx0 1)}))
    (is (= (:tempids t2) {"Serg" (+ d/e0 3), :db/current-tx (+ d/tx0 2)}))
    (is (= #{[(+ d/e0 1) "Ivan" 19   (+ d/tx0 1)]
             [(+ d/e0 2) "Petr" 22   (+ d/tx0 1)]
             [(+ d/e0 3) "Sergey" 30 (+ d/tx0 2)]}
           (d/q '[:find  ?e ?n ?a ?t
                  :where [?e :name ?n ?t]
                  [?e :age ?a]] @conn)))))

(deftest test-resolve-eid-refs
  (let [conn (d/create-conn {:friend {:db/valueType :db.type/ref
                                      :db/cardinality :db.cardinality/many}})
        tx   (d/transact! conn [{:name "Sergey"
                                 :friend [-1 -2]}
                                [:db/add -1  :name "Ivan"]
                                [:db/add -2  :name "Petr"]
                                [:db/add "B" :name "Boris"]
                                [:db/add "B" :friend -3]
                                [:db/add -3  :name "Oleg"]
                                [:db/add -3  :friend "B"]])
        q '[:find ?fn
            :in $ ?n
            :where [?e :name ?n]
            [?e :friend ?fe]
            [?fe :name ?fn]]]
    (is (= (:tempids tx) {-1 (+ d/e0 2), -2 (+ d/e0 3), "B" (+ d/e0 4), -3 (+ d/e0 5), :db/current-tx (+ d/tx0 1)}))
    (is (= (d/q q @conn "Sergey") #{["Ivan"] ["Petr"]}))
    (is (= (d/q q @conn "Boris") #{["Oleg"]}))
    (is (= (d/q q @conn "Oleg") #{["Boris"]}))))

(deftest test-resolve-current-tx
  (doseq [tx-tempid [:db/current-tx "datomic.tx" "datahike.tx"]]
    (testing tx-tempid
      (let [conn (d/create-conn {:created-at {:db/valueType :db.type/ref}})
            tx1  (d/transact! conn [{:name "X", :created-at tx-tempid}
                                    {:db/id tx-tempid, :prop1 "prop1"}
                                    [:db/add tx-tempid :prop2 "prop2"]
                                    [:db/add -1 :name "Y"]
                                    [:db/add -1 :created-at tx-tempid]])]
        (is (= (d/q '[:find ?e ?a ?v :where [?e ?a ?v]] @conn)
               #{[(+ d/e0 1) :name "X"]
                 [(+ d/e0 1) :created-at (+ d/tx0 1)]
                 [(+ d/tx0 1) :prop1 "prop1"]
                 [(+ d/tx0 1) :prop2 "prop2"]
                 [(+ d/e0 2) :name "Y"]
                 [(+ d/e0 2) :created-at (+ d/tx0 1)]}))
        (is (= (:tempids tx1) (assoc {-1 (+ d/e0 2), :db/current-tx (+ d/tx0 1)}
                                     tx-tempid (+ d/tx0 1))))
        (let [tx2   (d/transact! conn [[:db/add tx-tempid :prop3 "prop3"]])
              tx-id (get-in tx2 [:tempids tx-tempid])]
          (is (= (into {} (d/entity @conn tx-id))
                 {:prop3 "prop3"})))
        (let [tx3   (d/transact! conn [{:db/id tx-tempid, :prop4 "prop4"}])
              tx-id (get-in tx3 [:tempids tx-tempid])]
          (is (= tx-id (+ d/tx0 3)))
          (is (= (into {} (d/entity @conn tx-id))
                 {:prop4 "prop4"})))))))
