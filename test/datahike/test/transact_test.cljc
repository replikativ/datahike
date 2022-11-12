(ns datahike.test.transact-test
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.datom :as dd]
   [datahike.constants :as const]
   [datahike.tools :as tools]
   [datahike.test.utils :as du]
   [datahike.test.core-test]
   [datahike.test.cljs-utils]
   [clojure.spec.alpha :as s]
   [clojure.spec.gen.alpha :as gen]))

(deftest test-with
  (let [db  (-> (db/empty-db {:aka {:db/cardinality :db.cardinality/many}})
                (d/db-with [[:db/add 1 :name "Ivan"]])
                (d/db-with [[:db/add 1 :name "Petr"]])
                (d/db-with [[:db/add 1 :aka  "Devil"]])
                (d/db-with [[:db/add 1 :aka  "Tupen"]]))]

    (is (= (d/q '[:find ?v
                  :where [1 :name ?v]] db)
           #{["Petr"]}))
    (is (= (d/q '[:find ?v
                  :where [1 :aka ?v]] db)
           #{["Devil"] ["Tupen"]}))

    (testing "Retract"
      (let [db  (-> db
                    (d/db-with [[:db/retract 1 :name "Petr"]])
                    (d/db-with [[:db/retract 1 :aka  "Devil"]]))]

        (is (= (d/q '[:find ?v
                      :where [1 :name ?v]] db)
               #{}))
        (is (= (d/q '[:find ?v
                      :where [1 :aka ?v]] db)
               #{["Tupen"]}))

        (is (= (into {} (d/entity db 1)) {:aka #{"Tupen"}}))))

    (testing "Cannot retract what's not there"
      (let [db  (-> db
                    (d/db-with [[:db/retract 1 :name "Ivan"]]))]
        (is (= (d/q '[:find ?v
                      :where [1 :name ?v]] db)
               #{["Petr"]})))))

  (testing "Skipping nils in tx"
    (let [db (-> (db/empty-db)
                 (d/db-with [[:db/add 1 :attr 2]
                             nil
                             [:db/add 3 :attr 4]]))]
      (is (= [[1 :attr 2], [3 :attr 4]]
             (map (juxt :e :a :v) (d/datoms db :eavt)))))))

(deftest test-with-datoms
  (testing "keeps tx number"
    (let [db (-> (db/empty-db)
                 (d/db-with [(dd/datom 1 :name "Oleg")
                             (dd/datom 1 :age  17 (+ 1 const/tx0))
                             [:db/add 1 :aka  "x" (+ 2 const/tx0)]]))]
      (is (= [[1 :age  17     (+ 1 const/tx0)]
              [1 :aka  "x"    (+ 2 const/tx0)]
              [1 :name "Oleg" const/tx0]]
             (map (juxt :e :a :v :tx)
                  (d/datoms db :eavt))))))

  (testing "retraction"
    (let [db (-> (db/empty-db)
                 (d/db-with [(dd/datom 1 :name "Oleg")
                             (dd/datom 1 :age  17)
                             (dd/datom 1 :name "Oleg" const/tx0 false)]))]
      (is (= [[1 :age 17 const/tx0]]
             (map (juxt :e :a :v :tx)
                  (d/datoms db :eavt)))))))

(deftest test-retract-fns
  (let [db (-> (db/empty-db {:aka    {:db/cardinality :db.cardinality/many}
                             :friend {:db/valueType :db.type/ref}})
               (d/db-with [{:db/id 1, :name  "Ivan", :age 15, :aka ["X" "Y" "Z"], :friend 2}
                           {:db/id 2, :name  "Petr", :age 37}]))]
    (let [db (d/db-with db [[:db.fn/retractEntity 1]])]
      (is (= (d/q '[:find ?a ?v
                    :where [1 ?a ?v]] db)
             #{}))
      (is (= (d/q '[:find ?a ?v
                    :where [2 ?a ?v]] db)
             #{[:name "Petr"] [:age 37]})))

    (is (= (d/db-with db [[:db.fn/retractEntity 1]])
           (d/db-with db [[:db/retractEntity 1]])))

    (testing "Retract entitiy with incoming refs"
      (is (= (d/q '[:find ?e :where [1 :friend ?e]] db)
             #{[2]}))

      (let [db (d/db-with db [[:db.fn/retractEntity 2]])]
        (is (= (d/q '[:find ?e :where [1 :friend ?e]] db)
               #{}))))

    (let [db (d/db-with db [[:db.fn/retractAttribute 1 :name]])]
      (is (= (d/q '[:find ?a ?v
                    :where [1 ?a ?v]] db)
             #{[:age 15] [:aka "X"] [:aka "Y"] [:aka "Z"] [:friend 2]}))
      (is (= (d/q '[:find ?a ?v
                    :where [2 ?a ?v]] db)
             #{[:name "Petr"] [:age 37]})))

    (let [db (d/db-with db [[:db.fn/retractAttribute 1 :aka]])]
      (is (= (d/q '[:find ?a ?v
                    :where [1 ?a ?v]] db)
             #{[:name "Ivan"] [:age 15] [:friend 2]}))
      (is (= (d/q '[:find ?a ?v
                    :where [2 ?a ?v]] db)
             #{[:name "Petr"] [:age 37]})))

    (is (= (d/db-with db [[:db.fn/retractAttribute 1 :name]])
           (d/db-with db [[:db/retract 1 :name]])))
    (is (= (d/db-with db [[:db.fn/retractAttribute 1 :aka]])
           (d/db-with db [[:db/retract 1 :aka]])))))

(deftest test-retract-fns-not-found
  (let [db  (-> (db/empty-db {:name {:db/unique :db.unique/identity}})
                (d/db-with  [[:db/add 1 :name "Ivan"]]))
        all #(vec (d/datoms % :eavt))
        tx0 (inc const/tx0)]
    (are [op] (= [(dd/datom 1 :name "Ivan" tx0)]
                 (all (d/db-with db [op])))
      [:db/retract             2 :name "Petr"]
      [:db.fn/retractAttribute 2 :name]
      [:db.fn/retractEntity    2]
      [:db/retractEntity       2]
      [:db/retract             [:name "Petr"] :name "Petr"]
      [:db.fn/retractAttribute [:name "Petr"] :name]
      [:db.fn/retractEntity    [:name "Petr"]])

    (are [op] (= [[] []]
                 [(all (d/db-with db [op]))
                  (all (d/db-with db [op op]))]) ;; idempotency
      [:db/retract             1 :name "Ivan"]
      [:db.fn/retractAttribute 1 :name]
      [:db.fn/retractEntity    1]
      [:db/retractEntity       1]
      [:db/retract             [:name "Ivan"] :name "Ivan"]
      [:db.fn/retractAttribute [:name "Ivan"] :name]
      [:db.fn/retractEntity    [:name "Ivan"]])))

(deftest test-retract-component
  (let [db  (-> (db/empty-db {:name {:db/unique :db.unique/identity}
                              :room {:db/valueType :db.type/ref
                                     :db/cardinality :db.cardinality/many
                                     :db/isComponent true}})
                (d/db-with  [{:name :house1 :room [{:name :kitchen} {:name :bath}]}
                             {:name :house2 :room [{:name :office} {:name :attic}]}]))
        names (fn [db]
                (set (d/q '[:find [?name ...]
                            :where [_ :name ?name]]
                          db)))]

    (is (= (names (d/db-with db [[:db.fn/retractEntity [:name :house1]]]))
           #{:house2 :office :attic}))

    (testing ":db.fn/retractAttribute retracts components"
      (let [db (d/db-with db [[:db.fn/retractAttribute [:name :house1] :room]])]
        (is (nil? (:room (d/entity db [:name :house1]))))
        (is (= (names db)
               #{:house1 :house2 :office :attic}))))

    (testing ":db/retract only retracts reference to components"
      (let [db (d/db-with db [[:db/retract [:name :house1] :room [:name :kitchen]]])]
        (is (= (map :name (:room (d/entity db [:name :house1])))
               [:bath]))
        (is (d/entity db [:name :kitchen])))

      (let [db (d/db-with db [[:db/retract [:name :house1] :room]])]
        (is (nil? (:room (d/entity db [:name :house1]))))
        (is (= (names db)
               #{:house1 :kitchen :bath :house2 :office :attic}))))))

(deftest test-transact!
  (let [conn (du/setup-db {:initial-tx [{:db/ident :aka :db/cardinality :db.cardinality/many}]})]
    (d/transact conn [[:db/add 1 :name "Ivan"]])
    (d/transact conn [[:db/add 1 :name "Petr"]])
    (d/transact conn [[:db/add 1 :aka  "Devil"]])
    (d/transact conn [[:db/add 1 :aka  "Tupen"]])

    (is (= (d/q '[:find ?v
                  :where [1 :name ?v]] @conn)
           #{["Petr"]}))
    (is (= (d/q '[:find ?v
                  :where [1 :aka ?v]] @conn)
           #{["Devil"] ["Tupen"]}))))

(deftest test-db-fn-cas
  (let [conn (du/setup-db)]
    (d/transact conn {:tx-data [[:db/cas 1 :weight nil 100]]})
    (is (= (:weight (d/entity @conn 1)) 100))
    (d/transact conn {:tx-data [[:db/add 1 :weight 200]]})
    (d/transact conn {:tx-data [[:db.fn/cas 1 :weight 200 300]]})
    (is (= (:weight (d/entity @conn 1)) 300))
    (d/transact conn {:tx-data [[:db/cas 1 :weight 300 400]]})
    (is (= (:weight (d/entity @conn 1)) 400))
    (is (thrown-msg? ":db.fn/cas failed on datom [1 :weight 400], expected 200"
                     (d/transact conn {:tx-data [[:db.fn/cas 1 :weight 200 210]]}))))

  (let [conn  (du/setup-db {:initial-tx [{:db/ident :label :db/cardinality :db.cardinality/many}]})]
    (d/transact conn {:tx-data [[:db/add 1 :label :x]]})
    (d/transact conn {:tx-data [[:db/add 1 :label :y]]})
    (d/transact conn {:tx-data [[:db.fn/cas 1 :label :y :z]]})
    (is (= (:label (d/entity @conn 1)) #{:x :y :z}))
    (is (thrown-msg? ":db.fn/cas failed on datom [1 :label (:x :y :z)], expected :s"
                     (d/transact conn {:tx-data [[:db.fn/cas 1 :label :s :t]]}))))

  (let [conn (du/setup-db)]
    (d/transact conn {:tx-data [[:db/add 1 :name "Ivan"]]})
    (d/transact conn {:tx-data [[:db.fn/cas 1 :age nil 42]]})
    (is (= (:age (d/entity @conn 1)) 42))
    (is (thrown-msg? ":db.fn/cas failed on datom [1 :age 42], expected nil"
                     (d/transact conn {:tx-data [[:db.fn/cas 1 :age nil 4711]]}))))

  (let [conn (du/setup-db)]
    (is (thrown-msg? "Can't use tempid in '[:db.fn/cas -1 :attr nil :val]'. Tempids are allowed in :db/add only"
                     (d/transact conn {:tx-data [[:db/add    -1 :name "Ivan"]
                                                 [:db.fn/cas -1 :attr nil :val]]})))))

(deftest test-db-fn
  (let [conn (du/setup-db {:initial-tx [{:db/ident :aka :db/cardinality :db.cardinality/many}]})
        inc-age (fn [db name]
                  (if-let [[eid age] (first (d/q '{:find [?e ?age]
                                                   :in [$ ?name]
                                                   :where [[?e :name ?name]
                                                           [?e :age ?age]]}
                                                 db name))]
                    [{:db/id eid :age (inc age)} [:db/add eid :had-birthday true]]
                    (throw (ex-info (str "No entity with name: " name) {}))))]
    (d/transact conn {:tx-data [{:db/id 1 :name "Ivan" :age 31}]})
    (d/transact conn {:tx-data [[:db/add 1 :name "Petr"]]})
    (d/transact conn {:tx-data [[:db/add 1 :aka  "Devil"]]})
    (d/transact conn {:tx-data [[:db/add 1 :aka  "Tupen"]]})
    (is (= (d/q '[:find ?v ?a
                  :where [?e :name ?v]
                  [?e :age ?a]] @conn)
           #{["Petr" 31]}))
    (is (= (d/q '[:find ?v
                  :where [?e :aka ?v]] @conn)
           #{["Devil"] ["Tupen"]}))
    (is (thrown-msg? "No entity with name: Bob"
                     (d/transact conn {:tx-data [[:db.fn/call inc-age "Bob"]]})))
    (let [{:keys [db-after]} (d/transact conn {:tx-data [[:db.fn/call inc-age "Petr"]]})
          e (d/entity db-after 1)]
      (is (= (:age e) 32))
      (is (:had-birthday e)))))

#_(deftest test-db-ident-fn ;; TODO: check for :db/ident support within hhtree
    (let [conn    (du/setup-db {:initial-tx [{:name {:db/unique :db.unique/identity}}]})
          inc-age (fn [db name]
                    (if-some [ent (d/entity db [:name name])]
                      [{:db/id (:db/id ent)
                        :age   (inc (:age ent))}
                       [:db/add (:db/id ent) :had-birthday true]]
                      (throw (ex-info (str "No entity with name: " name) {}))))]
      (d/transact conn {:tx-data [{:db/id    1
                                   :name     "Petr"
                                   :age      31
                                   :db/ident :Petr}
                                  {:db/ident :inc-age
                                   :db/fn    inc-age}]})
      (is (thrown-msg? "Can’t find entity for transaction fn :unknown-fn"
                       (d/transact conn {:tx-data [[:unknown-fn]]})))
      (is (thrown-msg? "Entity :Petr expected to have :db/fn attribute with fn? value"
                       (d/transact conn {:tx-data [[:Petr]]})))
      (is (thrown-msg? "No entity with name: Bob"
                       (d/transact conn {:tx-data [[:inc-age "Bob"]]})))
      (d/transact conn {:tx-data [[:inc-age "Petr"]]})
      (let [e (d/entity @conn 1)]
        (is (= (:age e) 32))
        (is (:had-birthday e)))))

(deftest test-resolve-eid
  (let [conn (du/setup-db)
        t1   (d/transact conn {:tx-data [[:db/add -1 :name "Ivan"]
                                         [:db/add -1 :age 19]
                                         [:db/add -2 :name "Petr"]
                                         [:db/add -2 :age 22]]})
        t2   (d/transact conn {:tx-data [[:db/add "Serg" :name "Sergey"]
                                         [:db/add "Serg" :age 30]]})]
    (is (= (:tempids t1) {-1 1, -2 2, :db/current-tx (+ const/tx0 1)}))
    (is (= (:tempids t2) {"Serg" 3, :db/current-tx (+ const/tx0 2)}))
    (is (= #{[1 "Ivan" 19   (+ const/tx0 1)]
             [2 "Petr" 22   (+ const/tx0 1)]
             [3 "Sergey" 30 (+ const/tx0 2)]}
           (d/q '[:find  ?e ?n ?a ?t
                  :where [?e :name ?n ?t]
                  [?e :age ?a]] @conn)))))

(deftest test-resolve-eid-refs
  (let [conn (du/setup-db {:initial-tx [{:db/ident :friend
                                         :db/valueType :db.type/ref
                                         :db/cardinality :db.cardinality/many}]})
        tx   (d/transact conn {:tx-data [{:name "Sergey"
                                          :friend [-1 -2]}
                                         [:db/add -1  :name "Ivan"]
                                         [:db/add -2  :name "Petr"]
                                         [:db/add "B" :name "Boris"]
                                         [:db/add "B" :friend -3]
                                         [:db/add -3  :name "Oleg"]
                                         [:db/add -3  :friend "B"]]})
        q '[:find ?fn
            :in $ ?n
            :where [?e :name ?n]
            [?e :friend ?fe]
            [?fe :name ?fn]]]
    (is (= (:tempids tx) {-1 3, -2 4, "B" 5, -3 6, :db/current-tx (+ const/tx0 2)}))
    (is (= (d/q {:query q :args [@conn "Sergey"]}) #{["Ivan"] ["Petr"]}))
    (is (= (d/q {:query q :args [@conn "Boris"]}) #{["Oleg"]}))
    (is (= (d/q {:query q :args [@conn "Oleg"]}) #{["Boris"]})))

  (testing "Resolve eid for unique attributes with temporary reference value"
    (let [conn (fn [] (du/setup-db {:initial-tx [{:db/ident       :foo/match
                                                  :db/valueType   :db.type/ref
                                                  :db/cardinality :db.cardinality/one
                                                  :db/unique      :db.unique/identity}]}))
          query '[:find ?e ?a ?v
                  :where [?e ?a ?v]
                  [(= ?a :foo/match)]]]
      (testing "with maps"
        (testing "temp-eid first"
          (let [report (d/transact (conn) [{:db/id 16
                                            :foo/match -1000001}
                                           {:db/id -1000001
                                            :foo/match 16}])
                id (get-in report [:tempids -1000001])]
            (is (= (d/q query (:db-after report))
                   #{[16 :foo/match id] [id :foo/match 16]}))))
        (testing "temp-vid first"
          (let [report (d/transact (conn) [{:db/id -1000001
                                            :foo/match 16}
                                           {:db/id 16
                                            :foo/match -1000001}])
                id (get-in report [:tempids -1000001])]
            (is (= (d/q query (:db-after report))
                   #{[16 :foo/match id] [id :foo/match 16]})))))
      (testing "with vectors"
        (testing "temp-eid first"
          (let [report (d/transact (conn) [[:db/add 16 :foo/match -1000001]
                                           [:db/add -1000001 :foo/match 16]])
                id (get-in report [:tempids -1000001])]
            (is (= (d/q query (:db-after report))
                   #{[16 :foo/match id] [id :foo/match 16]}))))
        (testing "temp-vid first"
          (let [report (d/transact (conn) [[:db/add -1000001 :foo/match 16]
                                           [:db/add 16 :foo/match -1000001]])
                id (get-in report [:tempids -1000001])]
            (is (= (d/q query (:db-after report))
                   #{[16 :foo/match id] [id :foo/match 16]}))))))))

(deftest test-resolve-current-tx
  (doseq [tx-tempid [:db/current-tx "datomic.tx" "datahike.tx"]]
    (testing tx-tempid
      (let [conn (du/setup-db {:keep-history? false})
            tx1  (d/transact conn {:tx-data [{:db/ident :created-at :db/valueType :db.type/ref}
                                             {:name "X", :created-at tx-tempid}
                                             {:db/id tx-tempid, :prop1 "prop1"}
                                             [:db/add tx-tempid :prop2 "prop2"]
                                             [:db/add -1 :name "Y"]
                                             [:db/add -1 :created-at tx-tempid]]})]
        (is (= #{[1 :db/ident :created-at]
                 [1 :db/valueType :db.type/ref]
                 [2 :name "X"]
                 [2 :created-at (+ const/tx0 1)]
                 [(+ const/tx0 1) :prop1 "prop1"]
                 [(+ const/tx0 1) :prop2 "prop2"]
                 [3 :name "Y"]
                 [3 :created-at (+ const/tx0 1)]}
               (d/q '[:find ?e ?a ?v :where [?e ?a ?v]] @conn)))
        (is (= (assoc {-1 3, :db/current-tx (+ const/tx0 1)}
                      tx-tempid (+ const/tx0 1))
               (:tempids tx1)))
        (let [tx2   (d/transact conn {:tx-data [[:db/add tx-tempid :prop3 "prop3"]]})
              tx-id (get-in tx2 [:tempids tx-tempid])]
          (is (= (into {} (d/entity @conn tx-id))
                 {:prop3 "prop3"})))
        (let [tx3   (d/transact conn {:tx-data [{:db/id tx-tempid, :prop4 "prop4"}]})
              tx-id (get-in tx3 [:tempids tx-tempid])]
          (is (= tx-id (+ const/tx0 3)))
          (is (= (into {} (d/entity @conn tx-id))
                 {:prop4 "prop4"})))))))

(deftest test-tx-meta
  (testing "simple test"
    (let [conn (du/setup-db)
          tx   (d/transact conn {:tx-data [{:name "Sergey"
                                            :age  5}]
                                 :tx-meta {:foo "bar"}})]
      (is (= (dissoc (:tx-meta tx) :db/txInstant)
             {:foo "bar"}))))
  (testing "generative test"
    (let [conn (du/setup-db)
          Metadata (s/map-of keyword? (s/or :int int?
                                            :str string?
                                            :inst inst?
                                            :kw keyword?
                                            :sym symbol?))
          generated (gen/generate (s/gen Metadata))
          tx-report (d/transact conn {:tx-data [{:name "Sergey"
                                                 :age  5}]
                                      :tx-meta generated})]
      (is (= (dissoc (:tx-meta tx-report) :db/txInstant)
             generated))))
  (testing "manual txInstant is the same as auto-generated"
    (let [conn (du/setup-db)
          date (tools/get-time)
          tx   (d/transact conn {:tx-data [{:name "Sergey"
                                            :age  5}]
                                 :tx-meta {:db/txInstant date}})]
      (is (= [[1 :age 5 536870913 true]
              [1 :name "Sergey" 536870913 true]
              [536870913
               :db/txInstant
               date
               536870913
               true]]
             (mapv (comp #(into [] %) seq)
                   (d/datoms @conn :eavt))))))
  (testing "missing schema definition"
    (let [schema [{:db/ident       :name
                   :db/cardinality :db.cardinality/one
                   :db/index       true
                   :db/unique      :db.unique/identity
                   :db/valueType   :db.type/string}
                  {:db/ident       :age
                   :db/cardinality :db.cardinality/one
                   :db/valueType   :db.type/long}]
          conn (du/setup-db {:initial-tx schema
                             :schema-flexibility :write})]
      (is (thrown-msg? "Bad entity attribute :foo at [:db/add 536870914 :foo :bar 536870914], not defined in current schema"
                       (d/transact conn {:tx-data [{:name "Sergey"
                                                    :age  5}]
                                         :tx-meta {:foo :bar}})))))
  (testing "meta-data is available on the indices"
    (let [conn (du/setup-db)
          tx   (d/transact conn {:tx-data [{:name "Sergey"
                                            :age  5}]
                                 :tx-meta {:foo :bar}})]
      (is (= #{[536870913 :bar]}
             (d/q '[:find ?e ?v
                    :where [?e :foo ?v]]
                  @conn)))))
  (testing "retracting metadata"
    (let [conn (du/setup-db)
          _    (d/transact conn {:tx-data [{:name "Sergey"
                                            :age  5}]
                                 :tx-meta {:foo :bar}})
          _    (d/transact conn {:tx-data [[:db/retract 536870913 :foo :bar]]})]
      (is (= #{}
             (d/q '[:find ?e ?v
                    :where [?e :foo ?v]]
                  @conn)))))
  (testing "overwrite metadata"
    (let [conn (du/setup-db)
          _    (d/transact conn {:tx-data [{:name "Sergey"
                                            :age  5}]
                                 :tx-meta {:foo :bar}})
          _    (d/transact conn {:tx-data [{:db/id 536870913 :foo :baz}]})]
      (is (= #{[536870913 :baz]}
             (d/q '[:find ?e ?v
                    :where [?e :foo ?v]]
                  @conn)))))
  (testing "metadata has txInstant"
    (let [conn (du/setup-db)
          {:keys [tempids]} (d/transact conn {:tx-data [{:name "Sergey"
                                                         :age  5}]
                                              :tx-meta {:foo :bar}})]
      (is (-> (d/pull @conn
                      '[:db/txInstant]
                      (:db/current-tx tempids))
              :db/txInstant
              inst?)))))
