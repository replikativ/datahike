(ns datahike.test.api
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer [is are deftest testing use-fixtures]])
   [datahike.test.core]
   [datahike.test.utils :as utils]
   [datahike.api :as d]))

(deftest test-transact-docs
  (let [cfg {:store {:backend :mem
                     :id "hashing"}
             :keep-history? false
             :schema-flexibility :read}
        _ (d/delete-database cfg)
        _ (d/create-database cfg)
        conn (d/connect cfg)
        dvec #(vector (:e %) (:a %) (:v %))]
    ;; add a single datom to an existing entity (1)
    (is (= [[1 :name "Ivan"]]
           (map dvec (:tx-data (d/transact conn {:tx-data [[:db/add 1 :name "Ivan"]]})))))

    ;; retract a single datom
    (is (= [[1 :name "Ivan"]]
           (map dvec (:tx-data (d/transact conn {:tx-data [[:db/retract 1 :name "Ivan"]]})))))

    ;; retract single entity attribute
    (is (= []
           (map dvec (:tx-data (d/transact conn {:tx-data [[:db.fn/retractAttribute 1 :name]]})))))

    ;; retract all entity attributes (effectively deletes entity)
    (is (= []
           (map dvec (:tx-data (d/transact conn {:tx-data [[:db.fn/retractEntity 1]]})))))

    ;; create a new entity (`-1`, as any other negative value, is a tempid
    ;; that will be replaced with DataScript to a next unused eid)
    (is (= '([2 :name "Ivan"])
           (map dvec (:tx-data (d/transact conn {:tx-data [[:db/add -1 :name "Ivan"]]})))))

    ;; check assigned id (here `*1` is a result returned from previous `transact` call)
    (is (= {-1 3, :db/current-tx 536870918}
           (:tempids (d/transact conn {:tx-data [[:db/add -1 :name "Ivan"]]}))))

    ;; check actual datoms inserted
    (is (= '([4 :name "Ivan"])
           (map dvec (:tx-data (d/transact conn {:tx-data [[:db/add -1 :name "Ivan"]]}))))) ; => [#datahike/Datom [296 :name "Ivan"]]

    ;; tempid can also be a string
    (is (= {"ivan" 5, :db/current-tx 536870920}
           (:tempids (d/transact conn {:tx-data [[:db/add "ivan" :name "Ivan"]]}))))

    ;; reference another entity (must exist)
    (is (= '([6 :friend 296])
           (map dvec (:tx-data (d/transact conn {:tx-data [[:db/add -1 :friend 296]]})))))

    ;; create an entity and set multiple attributes (in a single transaction
    ;; equal tempids will be replaced with the same unused yet entid)
    (is (= '([7 :name "Ivan"] [7 :likes "fries"] [7 :likes "pizza"] [7 :friend 296])
           (map dvec (:tx-data (d/transact conn {:tx-data [[:db/add -1 :name "Ivan"]
                                                           [:db/add -1 :likes "fries"]
                                                           [:db/add -1 :likes "pizza"]
                                                           [:db/add -1 :friend 296]]})))))

    ;; create an entity and set multiple attributes (alternative map form)
    (is (= '([8 :name "Ivan"] [8 :likes ["fries" "pizza"]] [8 :friend 296])
           (map dvec (:tx-data (d/transact conn {:tx-data [{:db/id  -1
                                                            :name   "Ivan"
                                                            :likes  ["fries" "pizza"]
                                                            :friend 296}]})))))

    ;; update an entity (alternative map form). Can’t retract attributes in
    ;; map form. For cardinality many attrs, value (fish in this example)
    ;; will be added to the list of existing values
    (is (= '([296 :name "Oleg"] [296 :likes ["fish"]])
           (map dvec (:tx-data (d/transact conn {:tx-data [{:db/id  296
                                                            :name   "Oleg"
                                                            :likes  ["fish"]}]})))))

    ;; ref attributes can be specified as nested map, that will create netsed entity as well
    (is (= '([297 :name "Oleg"] [297 :friend {:db/id -2, :name "Sergey"}])
           (map dvec (:tx-data (d/transact conn {:tx-data [{:db/id  -1
                                                            :name   "Oleg"
                                                            :friend {:db/id -2
                                                                     :name "Sergey"}}]})))))

    ;; schema is needed for using a reverse attribute
    (is (= '([298 :db/valueType :db.type/ref] [298 :db/cardinality :db.cardinality/one] [298 :db/ident :friend])
           (map dvec (:tx-data (d/transact conn {:tx-data [{:db/valueType :db.type/ref
                                                            :db/cardinality :db.cardinality/one
                                                            :db/ident :friend}]})))))

    ;; reverse attribute name can be used if you want created entity to become
    ;; a value in another entity reference
    (is (= '([299 :name "Oleg"] [296 :friend 299])
           (map dvec (:tx-data (d/transact conn {:tx-data [{:db/id  -1
                                                            :name   "Oleg"
                                                            :_friend 296}]})))))

    ;; equivalent to
    (is (= '([300 :name "Oleg"] [296 :friend 300])
           (map dvec (:tx-data (d/transact conn {:tx-data [{:db/id  -1, :name   "Oleg"}
                                                           {:db/id 296, :friend -1}]})))))

    ;; deprecated api
    (is (= '([301 :name "Oleg"] [301 :likes "pie"] [301 :likes "dates"] [301 :friend 297])
           (map dvec (:tx-data (d/transact conn [[:db/add -1 :name "Oleg"]
                                                 [:db/add -1 :likes "pie"]
                                                 [:db/add -1 :likes "dates"]
                                                 [:db/add -1 :friend 297]])))))

    ;; lazy sequence
    (is (= '([302 :name "Oleg"] [302 :likes "pie"] [302 :likes "dates"] [302 :friend 297])
           (map dvec (:tx-data (d/transact conn (take 4 [[:db/add -1 :name "Oleg"]
                                                         [:db/add -1 :likes "pie"]
                                                         [:db/add -1 :likes "dates"]
                                                         [:db/add -1 :friend 297]]))))))

    ;; incorrect arguments
    (is (thrown? clojure.lang.ExceptionInfo (d/transact conn nil)))
    (is (thrown? clojure.lang.ExceptionInfo (d/transact conn :foo)))
    (is (thrown? clojure.lang.ExceptionInfo (d/transact conn 1)))
    (is (thrown? clojure.lang.ExceptionInfo (d/transact conn {:foo "bar"})))))

(deftest test-transact!-docs
  (let [cfg {:store {:backend :mem
                     :id "hashing"}
             :keep-history? false
             :schema-flexibility :read}
        _ (d/delete-database cfg)
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    ;; add a single datom to an existing entity (1)
    (is (d/transact! conn [[:db/add 1 :name "Ivan"]]))))

    ;; retract a single datom

(deftest test-pull-docs
  (let [cfg {:store {:backend :mem
                     :id "pull"}
             :initial-tx [{:db/ident :likes
                           :db/cardinality :db.cardinality/many}
                          {:db/ident :friends
                           :db/cardinality :db.cardinality/many}]
             :keep-history? false
             :schema-flexibility :read}
        _ (d/delete-database cfg)
        _ (d/create-database cfg)
        conn (d/connect cfg)
        dvec #(vector (:e %) (:a %) (:v %))]
    (is (d/transact conn [{:db/id 1
                           :name "Ivan"
                           :likes :pizza
                           :friends 2}
                          {:db/id 2
                           :name "Oleg"}]))

    (is (= {:db/id   1,
            :name    "Ivan"
            :likes   [:pizza]
            :friends [{:db/id 2, :name "Oleg"}]}
           (d/pull @conn '{:selector [:db/id :name :likes {:friends [:db/id :name]}] :eid 1})))

    (is (= {:db/id   1,
            :name    "Ivan"
            :likes   [:pizza]
            :friends [{:db/id 2, :name "Oleg"}]}
           (d/pull @conn '[:db/id :name :likes {:friends [:db/id :name]}] 1)))))

(deftest test-pull-many-docs
  (let [cfg {:store {:backend :mem
                     :id "hashing"}
             :initial-tx [[:db/add 1 :name "Ivan"]
                          [:db/add 2 :name "Oleg"]]
             :keep-history? false
             :schema-flexibility :read}
        _ (d/delete-database cfg)
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (is (= (d/pull-many @conn [:db/id :name] [1 2])
           [{:db/id 1, :name "Ivan"}
            {:db/id 2, :name "Oleg"}]))))

(deftest test-q-docs
  (let [cfg {:store {:backend :mem
                     :id "q"}
             :initial-tx [[:db/add -1 :name "Ivan"]
                          [:db/add -1 :likes "fries"]
                          [:db/add -1 :likes "pizza"]
                          [:db/add -1 :friend 296]]
             :keep-history? false
             :schema-flexibility :read}
        _ (d/delete-database cfg)
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (is (= #{["fries"] ["candy"] ["pie"] ["pizza"]}
           (d/q '[:find ?value :where [_ :likes ?value]]
                #{[1 :likes "fries"]
                  [2 :likes "candy"]
                  [3 :likes "pie"]
                  [4 :likes "pizza"]})))

    (is (= #{["fries"] ["candy"] ["pie"] ["pizza"]}
           (d/q {:query '[:find ?value :where [_ :likes ?value]]
                 :args [#{[1 :likes "fries"]
                          [2 :likes "candy"]
                          [3 :likes "pie"]
                          [4 :likes "pizza"]}]})))

    (is (= #{["fries"]}
           (d/q {:query '[:find ?value :where [_ :likes ?value]]
                 :offset 2
                 :limit 1
                 :args [#{[1 :likes "fries"]
                          [2 :likes "candy"]
                          [3 :likes "pie"]
                          [4 :likes "pizza"]}]})))

    (is (= #{["fries"] ["pie"] ["candy"] ["pizza"]}
           (d/q {:query '[:find ?value :where [_ :likes ?value]]
                 :offset 0
                 :timeout 50
                 :args [#{[1 :likes "fries"]
                          [2 :likes "candy"]
                          [3 :likes "pie"]
                          [4 :likes "pizza"]}]})))

    (is (= #{["candy"] ["fries"]}
           (d/q {:query '[:find ?value :where [_ :likes ?value]]
                 :offset 2
                 :timeout 50
                 :args [#{[1 :likes "fries"]
                          [2 :likes "candy"]
                          [3 :likes "pie"]
                          [4 :likes "pizza"]}]})))

    (is (= #{["fries"] ["candy"] ["pie"] ["pizza"]}
           (d/q '{:find [?value] :where [[_ :likes ?value]]}
                #{[1 :likes "fries"]
                  [2 :likes "candy"]
                  [3 :likes "pie"]
                  [4 :likes "pizza"]})))

    (is (= #{["fries"] ["candy"] ["pie"] ["pizza"]}
           (d/q {:query '{:find [?value] :where [[_ :likes ?value]]}
                 :args [#{[1 :likes "fries"]
                          [2 :likes "candy"]
                          [3 :likes "pie"]
                          [4 :likes "pizza"]}]})))

    (is (= #{["fries"] ["candy"] ["pie"] ["pizza"]}
           (d/q {:query "[:find ?value :where [_ :likes ?value]]"
                 :args [#{[1 :likes "fries"]
                          [2 :likes "candy"]
                          [3 :likes "pie"]
                          [4 :likes "pizza"]}]})))

    ;; TODO better testing
    (is (= [{:db/id 1, :friend 296, :likes "pizza", :name "Ivan"}]
           (d/q '[:find [(pull ?e [*]) ...]
                  :where [?e ?a ?v]]
                @conn)))))

(deftest test-with-docs
  (let [cfg {:store {:backend :mem
                     :id "with"}
             :keep-history? false
             :schema-flexibility :read}
        _ (d/delete-database cfg)
        _ (d/create-database cfg)
        conn (d/connect cfg)
        dvec #(vector (:e %) (:a %) (:v %))]
    ;; add a single datom to an existing entity (1)
    (let [res (d/with @conn {:tx-data [[:db/add 1 :name "Ivan"]]})]
      (is (= nil
             (:tx-meta res)))
      (is (= '([1 :name "Ivan"])
             (map dvec (:tx-data res)))))
    (let [res (d/with @conn {:tx-data [[:db/add 1 :name "Ivan"]]
                             :tx-meta {:foo :bar}})]
      (is (= {:foo :bar}
             (:tx-meta res)))
      (is (= '([1 :name "Ivan"])
             (map dvec (:tx-data res)))))))

;; TODO testing properly on what?
(deftest test-db-docs
  (let [cfg {:store {:backend :mem
                     :id "db"}
             :keep-history? false
             :schema-flexibility :read}
        _ (d/delete-database cfg)
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (is (= datahike.db.DB
           (type (d/db conn))))
    (is (= datahike.db.DB
           (type @conn)))))

(deftest test-history-docs
  (let [cfg {:store {:backend :mem
                     :id "history"}
             :initial-tx [{:db/ident :name
                           :db/valueType :db.type/string
                           :db/unique :db.unique/identity
                           :db/index true
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :age
                           :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}]
             :keep-history? true
             :schema-flexibility :read}
        _ (d/delete-database cfg)
        _ (d/create-database cfg)
        conn (d/connect cfg)]

    (d/transact conn {:tx-data [{:name "Alice" :age 25} {:name "Bob" :age 30}]})

    (is (= #{["Alice" 25] ["Bob" 30]}
           (d/q {:query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
                 :args [(d/history (d/db conn))]})))

    (d/transact conn {:tx-data [{:db/id [:name "Alice"] :age 35}]})

    (is (= #{["Alice" 35] ["Bob" 30]}
           (d/q {:query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
                 :args [(d/db conn)]})))

    (is (= #{["Alice" 25] ["Alice" 35] ["Bob" 30]}
           (d/q {:query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
                 :args [(d/history (d/db conn))]})))))

(deftest test-as-of-docs
  (let [cfg {:store {:backend :mem
                     :id "as-of"}
             :initial-tx [{:db/ident :name
                           :db/valueType :db.type/string
                           :db/unique :db.unique/identity
                           :db/index true
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :age
                           :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}]
             :keep-history? true
             :schema-flexibility :read}
        _ (d/delete-database cfg)
        _ (d/create-database cfg)
        conn (d/connect cfg)]

    (d/transact conn {:tx-data [{:name "Alice" :age 25} {:name "Bob" :age 30}]})

    (Thread/sleep 100)

    (def date (java.util.Date.))

    (d/transact conn {:tx-data [{:db/id [:name "Alice"] :age 35}]})

    (is (= #{["Alice" 25] ["Bob" 30]}
           (d/q {:query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
                 :args [(d/as-of (d/db conn) date)]})))

    (is (= #{["Alice" 35] ["Bob" 30]}
           (d/q {:query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
                 :args [(d/db conn)]})))))

(deftest test-since-docs
  (let [cfg {:store {:backend :mem
                     :id "since"}
             :initial-tx [{:db/ident :name
                           :db/valueType :db.type/string
                           :db/unique :db.unique/identity
                           :db/index true
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :age
                           :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}]
             :keep-history? true
             :schema-flexibility :read}
        _ (d/delete-database cfg)
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (d/transact conn {:tx-data [{:name "Alice" :age 25} {:name "Bob" :age 30}]})

    (Thread/sleep 100)

    (def date (java.util.Date.))

    (Thread/sleep 100)

    (d/transact conn [{:db/id [:name "Alice"] :age 30}])

    (is (= #{["Alice" 30]}
           (d/q '[:find ?n ?a
                  :in $ $since
                  :where
                  [$ ?e :name ?n]
                  [$since ?e :age ?a]]
                @conn
                (d/since @conn date))))

    (is (= #{["Alice" 30] ["Bob" 30]}
           (d/q {:query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
                 :args [(d/db conn)]})))))

(deftest test-datoms-docs
  (let [cfg {:store {:backend :mem
                     :id "datoms"}
             :initial-tx [{:db/ident :name
                           :db/type :db.type/string
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :likes
                           :db/type :db.type/string
                           :db/index true
                           :db/cardinality :db.cardinality/many}
                          {:db/ident :friends
                           :db/type :db.type/ref
                           :db/cardinality :db.cardinality/many}]
             :keep-history? false
             :schema-flexibility :read}
        _ (d/delete-database cfg)
        _ (d/create-database cfg)
        db (d/connect cfg)
        _ (d/transact db [{:db/id 4 :name "Ivan"}
                          {:db/id 4 :likes "fries"}
                          {:db/id 4 :likes "pizza"}
                          {:db/id 4 :friends 5}])
        _ (d/transact db [{:db/id 5 :name "Oleg"}
                          {:db/id 5 :likes "candy"}
                          {:db/id 5 :likes "pie"}
                          {:db/id 5 :likes "pizza"}])
        dvec #(vector (:e %) (:a %) (:v %))]

    ;; find all datoms for entity id == 1 (any attrs and values)
    ;; sort by attribute, then value
    (is (= '([4 :friends 5]
             [4 :likes "fries"]
             [4 :likes "pizza"]
             [4 :name "Ivan"])
           (map dvec (d/datoms @db {:index :eavt :components [4]}))))
          ;; => (#datahike/Datom [1 :friends 2]
          ;;     #datahike/Datom [1 :likes \"fries\"]
          ;;     #datahike/Datom [1 :likes \"pizza\"]
          ;;     #datahike/Datom [1 :name \"Ivan\"])

    ;; find all datoms for entity id == 1 and attribute == :likes (any values)
    ;; sorted by value
    (is (= '([4 :likes "fries"]
             [4 :likes "pizza"])
           (map dvec (d/datoms @db {:index :eavt :components [4 :likes]}))))
    ;; => (#datahike/Datom [1 :likes \"fries\"]
    ;;     #datahike/Datom [1 :likes \"pizza\"])

    ;; find all datoms for entity id == 1, attribute == :likes and value == \"pizza\"
    (is (= '([4 :likes "pizza"])
           (map dvec (d/datoms @db {:index :eavt :components [4 :likes "pizza"]}))))
    ;; => (#datahike/Datom [1 :likes \"pizza\"])

    ;; find all datoms for attribute == :likes (any entity ids and values)
    ;; sorted by entity id, then value
    (is (= '([4 :likes "fries"]
             [4 :likes "pizza"]
             [5 :likes "candy"]
             [5 :likes "pie"]
             [5 :likes "pizza"])
           (map dvec (d/datoms @db {:index :aevt :components [:likes]}))))
      ;; => (#datahike/Datom [1 :likes \"fries\"]
      ;;     #datahike/Datom [1 :likes \"pizza\"]
      ;;     #datahike/Datom [2 :likes \"candy\"]
      ;;     #datahike/Datom [2 :likes \"pie\"]
      ;;     #datahike/Datom [2 :likes \"pizza\"])

    ;; find all datoms that have attribute == `:likes` and value == `\"pizza\"` (any entity id)
    ;; `:likes` must be a unique attr, reference or marked as `:db/index true`
    (is (= '([4 :likes "pizza"]
             [5 :likes "pizza"])
           (map dvec (d/datoms @db {:index :avet :components [:likes "pizza"]}))))
    ;; => (#datahike/Datom [1 :likes \"pizza\"]
    ;;     #datahike/Datom [2 :likes \"pizza\"])

    ;; find all datoms sorted by entity id, then attribute, then value
    (is (= '([1 :db/cardinality :db.cardinality/one] [1 :db/ident :name] [1 :db/type :db.type/string] [2 :db/cardinality :db.cardinality/many] [2 :db/ident :likes] [2 :db/index true] [2 :db/type :db.type/string] [3 :db/cardinality :db.cardinality/many] [3 :db/ident :friends] [3 :db/type :db.type/ref] [4 :friends 5] [4 :likes "fries"] [4 :likes "pizza"] [4 :name "Ivan"] [5 :likes "candy"] [5 :likes "pie"] [5 :likes "pizza"] [5 :name "Oleg"])
           (map dvec (d/datoms @db {:index :eavt})))) ; => (...)))

    ;; get all values of :db.cardinality/many attribute
    (is (= '("fries" "pizza")
           (->> (d/datoms @db {:index :eavt :components [4 :likes]})
                (map :v))))

    ;; lookup entity ids by attribute value
    (is (= '(4 5)
           (->> (d/datoms @db {:index :avet :components [:likes "pizza"]})
                (map :e))))

    ;; find all entities with a specific attribute
    (is (= '(4 5)
           (->> (d/datoms @db {:index :aevt :components [:name]})
                (map :e))))

    ;; find “singleton” entity by its attr
    (is (= 4
           (->> (d/datoms @db {:index :aevt :components [:name]})
                first :e)))

    ;; find N entities with lowest attr value (e.g. 10 earliest posts)
    #_(is (= "fail"
             (->> (d/datoms @db {:index :avet :components [:name]})
                  (take 2))))

    ;; find N entities with highest attr value (e.g. 10 latest posts)
    #_(is (= "fail"
             (->> (d/datoms @db {:index :avet :components [:name]})
                  (reverse)
                  (take 2))
             (map dvec (d/datoms @db {:index :eavt})))) ; => (...)))

    ;; get all values of :db.cardinality/many attribute
    (is (= '("fries" "pizza")
           (->> (d/datoms @db {:index :eavt :components [4 :likes]})
                (map :v))))

    ;; lookup entity ids by attribute value
    (is (= '(4 5)
           (->> (d/datoms @db {:index :avet :components [:likes "pizza"]})
                (map :e))))

    ;; find all entities with a specific attribute
    (is (= '(4 5)
           (->> (d/datoms @db {:index :aevt :components [:name]})
                (map :e))))

    ;; find “singleton” entity by its attr
    (is (= 4
           (->> (d/datoms @db {:index :aevt :components [:name]})
                first :e)))

    ;; find N entities with lowest attr value (e.g. 10 earliest posts)
    #_(is (= "fail"
             (->> (d/datoms @db {:index :avet :components [:name]})
                  (take 2))))

    ;; find N entities with highest attr value (e.g. 10 latest posts)
    #_(is (= "fail"
             (->> (d/datoms @db {:index :avet :components [:name]})
                  (reverse)
                  (take 2))))))

(deftest test-seek-datoms-doc
  (let [cfg {:store {:backend :mem
                     :id "seek-datoms"}
             :initial-tx  [{:db/ident :name
                            :db/type :db.type/string
                            :db/index true
                            :db/cardinality :db.cardinality/one}
                           {:db/ident :likes
                            :db/type :db.type/string
                            :db/index true
                            :db/cardinality :db.cardinality/many}
                           {:db/ident :friends
                            :db/type :db.type/ref
                            :db/index true
                            :db/cardinality :db.cardinality/many}]
             :keep-history? false
             :schema-flexibility :read}
        _ (d/delete-database cfg)
        _ (d/create-database cfg)
        db (d/connect cfg)
        dvec #(vector (:e %) (:a %) (:v %))
        _ (d/transact db {:tx-data [{:db/id 4 :name "Ivan"}
                                    {:db/id 4 :likes "fries"}
                                    {:db/id 4 :likes "pizza"}
                                    {:db/id 4 :friends 5}
                                    {:db/id 5 :likes "candy"}
                                    {:db/id 5 :likes "pie"}
                                    {:db/id 5 :likes "pizza"}]})]

    (is (= '([4 :friends 5]
             [4 :likes "fries"]
             [4 :likes "pizza"]
             [4 :name "Ivan"]
             [5 :likes "candy"]
             [5 :likes "pie"]
             [5 :likes "pizza"])
           (map dvec (d/seek-datoms @db {:index :eavt :components [4]}))))

    (is (= '([4 :name "Ivan"]
             [5 :likes "candy"]
             [5 :likes "pie"]
             [5 :likes "pizza"])
           (map dvec (d/seek-datoms @db {:index :eavt :components [4 :name]}))))

    (is (= '([5 :likes "candy"]
             [5 :likes "pie"]
             [5 :likes "pizza"])
           (map dvec (d/seek-datoms @db {:index :eavt :components [5]}))))

    (is (= '([5 :likes "pie"]
             [5 :likes "pizza"])
           (map dvec (d/seek-datoms @db {:index :eavt :components [5 :likes "fish"]}))))))

(deftest test-index-range-doc
  (let [cfg {:store {:backend :mem
                     :id "seek-datoms"}
             :initial-tx [{:db/ident :name
                           :db/type :db.type/string
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :likes
                           :db/index true
                           :db/type :db.type/string
                           :db/cardinality :db.cardinality/many}
                          {:db/ident :age
                           :db/unique :db.unique/identity
                           :db/type :db.type/ref
                           :db/cardinality :db.cardinality/many}]
             :keep-history? false
             :schema-flexibility :read}
        _ (d/delete-database cfg)
        _ (d/create-database cfg)
        db (d/connect cfg)
        dvec #(vector (:e %) (:a %) (:v %))
        _ (d/transact db {:tx-data [{:name "Ivan"}
                                    {:likes "fries"}
                                    {:likes "pizza"}
                                    {:age 19}
                                    {:likes "candy"}
                                    {:likes "pie"}
                                    {:likes "pizza"}]})]
    (is (= '([8 :likes "candy"] [5 :likes "fries"] [9 :likes "pie"] [6 :likes "pizza"] [10 :likes "pizza"])
           (map dvec (d/index-range @db {:attrid :likes :start "a" :end "zzzzzzzzz"}))))

    (is (= '([5 :likes "fries"] [9 :likes "pie"])
           (map dvec (d/index-range @db {:attrid :likes :start "egg" :end "pineapple"}))))))

(deftest test-database-hash
  (testing "Hashing without history"
    (let [cfg {:store {:backend :mem
                       :id "hashing"}
               :keep-history? false
               :schema-flexibility :read}
          _ (d/delete-database cfg)
          _ (d/create-database cfg)
          conn (d/connect cfg)
          hash-0 0]
      (testing "first hash equals zero"
        (is (= hash-0 (hash @conn))))
      (testing "hash remains 0 after reconnecting"
        (is (= hash-0 (-> (d/connect cfg) deref hash))))
      (testing "add entity to database"
        (let [_ (d/transact conn [{:db/id 1 :name "Max Mustermann"}])
              hash-1 (hash @conn)]
          (is (= hash-1 (-> (d/connect cfg) deref hash)))
          (testing "remove entity again"
            (let [_ (d/transact conn [[:db/retractEntity 1]])
                  hash-2 (hash @conn)]
              (is (not= hash-2 hash-1))
              (is (= hash-0 hash-2))))))))
  (testing "Hashing with history"
    (let [cfg {:store {:backend :mem
                       :id "hashing-with-history"}
               :keep-history? true
               :schema-flexibility :read}
          _ (d/delete-database cfg)
          _ (d/create-database cfg)
          conn (d/connect cfg)
          hash-0 (hash @conn)]
      (testing "first hash equals zero"
        (is (= hash-0 (hash @conn))))
      (testing "hash remains 0 after reconnecting"
        (is (= hash-0 (-> (d/connect cfg) deref hash))))
      (testing "add entity to database"
        (let [_ (d/transact conn [{:db/id 1 :name "Max Mustermann"}])
              hash-1 (hash @conn)]
          (is (= hash-1 (-> (d/connect cfg) deref hash)))
          (testing "retract entity again"
            (let [_ (d/transact conn [[:db/retractEntity 1]])
                  hash-2 (hash @conn)]
              (is (not= hash-1 hash-2))
              (is (not= hash-0 hash-2)))))))))

(deftest test-database-schema
  (letfn [(test-schema [cfg]
            (let [conn                      (utils/setup-db cfg)
                  name-schema               {:db/ident       :name
                                             :db/valueType   :db.type/string
                                             :db/cardinality :db.cardinality/one
                                             :db/unique      :db.unique/identity}
                  related-to-schema         {:db/ident       :related-to
                                             :db/valueType   :db.type/ref
                                             :db/cardinality :db.cardinality/many}
                  age-schema                {:db/ident       :age
                                             :db/valueType   :db.type/long
                                             :db/cardinality :db.cardinality/one
                                             :db/noHistory   true}
                  coerced-schema            (fn [db]
                                              (reduce-kv
                                               (fn [m k v]
                                                 (assoc m k (dissoc v :db/id)))
                                               {}
                                               (d/schema db)))
                  name-reverse-schema       {:db/ident           #{:name}
                                             :db/index           #{:name}
                                             :db.unique/identity #{:name}
                                             :db/unique          #{:name}}
                  age-reverse-schema        (-> name-reverse-schema
                                                (update :db/ident conj :age)
                                                (assoc :db/noHistory #{:age}))
                  related-to-reverse-schema (-> age-reverse-schema
                                                (update :db/ident conj :related-to)
                                                (update :db/index conj :related-to)
                                                (assoc :db.cardinality/many #{:related-to})
                                                (assoc :db.type/ref #{:related-to}))]
              (d/transact conn {:tx-data [name-schema]})
              (is (= {:name name-schema}
                     (coerced-schema @conn)))
              (is (= name-reverse-schema (d/reverse-schema @conn)))

              (d/transact conn {:tx-data [age-schema]})
              (is (= {:name name-schema
                      :age  age-schema}
                     (coerced-schema @conn)))
              (is (= age-reverse-schema
                     (d/reverse-schema @conn)))

              (d/transact conn {:tx-data [related-to-schema]})
              (is (= {:name       name-schema
                      :age        age-schema
                      :related-to related-to-schema}
                     (coerced-schema @conn)))
              (is (= related-to-reverse-schema
                     (d/reverse-schema @conn)))))]
    (let [base-cfg {:store              {:backend :mem
                                         :id      "api-db-schema-test"}
                    :keep-history?      false
                    :attribute-refs?    false
                    :schema-flexibility :write}]
      (testing "Empty database without any schema"
        (let [conn (do
                     (d/delete-database base-cfg)
                     (d/create-database base-cfg)
                     (d/connect base-cfg))]
          (is (= {}
                 (d/schema @conn)))
          (is (= {}
                 (d/reverse-schema @conn)))))
      (testing "Empty database with write flexibility and no attribute refs"
        (test-schema base-cfg))
      (testing "Empty database with write flexibility and attribute refs"
        (test-schema (assoc base-cfg :attribute-refs? true)))
      (testing "Empty database with read flexibility and no attribute refs"
        (test-schema (assoc base-cfg :schema-flexibility :read))))))

(deftest test-db-meta
  (let [cfg {:store              {:backend :mem
                                  :id      "api-db-schema-test"}
             :keep-history?      false
             :attribute-refs?    false
             :schema-flexibility :write}
        conn (utils/setup-db cfg)]
    (is (= #{:datahike/version :datahike/id :datahike/created-at :konserve/version :hitchhiker.tree/version}
           (-> @conn :meta keys set)))))
