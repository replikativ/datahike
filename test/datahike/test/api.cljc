(ns datahike.test.api
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer [is are deftest testing use-fixtures]])
   [datahike.test.core]
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
    (def report *1)
    (is (= {-1 296}
           (:tempids report))) ; => {-1 296}

    ;; check actual datoms inserted
    (is (= [296 :name "Ivan"]
           (:tx-data report))) ; => [#datahike/Datom [296 :name "Ivan"]]

    ;; tempid can also be a string
    (is (= '([3 :name "Ivan"])
           (map dvec (:tx-data (d/transact conn {:tx-data [[:db/add "ivan" :name "Ivan"]]})))))

    (is (= {"ivan" 297}
           (:tempids *1))) ; => {"ivan" 297}

    ;; reference another entity (must exist)
    (is (= '([4 :friend 296])
           (map dvec (:tx-data (d/transact conn {:tx-data [[:db/add -1 :friend 296]]})))))

    ;; create an entity and set multiple attributes (in a single transaction
    ;; equal tempids will be replaced with the same unused yet entid)
    (is (= '([5 :name "Ivan"] [5 :likes "fries"] [5 :likes "fries"] [5 :likes "pizza"] [5 :friend 296])
           (map dvec (:tx-data (d/transact conn {:tx-data [[:db/add -1 :name "Ivan"]
                                                           [:db/add -1 :likes "fries"]
                                                           [:db/add -1 :likes "pizza"]
                                                           [:db/add -1 :friend 296]]})))))

    ;; create an entity and set multiple attributes (alternative map form)
    (is (= '([6 :name "Ivan"] [6 :likes ["fries" "pizza"]] [6 :friend 296])
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
    (is (= '([300 :name "Oleg"] [296 :friend 299] [296 :friend 300])
           (map dvec (:tx-data (d/transact conn {:tx-data [{:db/id  -1, :name   "Oleg"}
                                                           {:db/id 296, :friend -1}]})))))

    ;; deprecated api
    (is (= '([301 :name "Oleg"] [301 :likes "pie"] [301 :likes "pie"] [301 :likes "dates"] [301 :friend 297])
           (map dvec (:tx-data (d/transact conn [[:db/add -1 :name "Oleg"]
                                                 [:db/add -1 :likes "pie"]
                                                 [:db/add -1 :likes "dates"]
                                                 [:db/add -1 :friend 297]])))))

    ;; lazy sequence
    (is (= '([302 :name "Oleg"] [302 :likes "pie"] [302 :likes "pie"] [302 :likes "dates"] [302 :friend 297])
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

    (is (= #{["pizza"]}
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

    (is (= #{["candy"] ["pizza"]}
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
           (d/q '{:query {:find [?value] :where [[_ :likes ?value]]}
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
           (type (d/db conn))))))

(deftest test-history-docs
  (let [cfg {:store {:backend :mem
                     :id "history"}
             :keep-history? true
             :schema-flexibility :read}
        _ (d/delete-database cfg)
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (is (= datahike.db.HistoricalDB
           (type (d/history @conn))))))

(deftest test-as-of-docs
  (let [cfg {:store {:backend :mem
                     :id "as-of"}
             :keep-history? true
             :schema-flexibility :read}
        _ (d/delete-database cfg)
        _ (d/create-database cfg)
        conn (d/connect cfg)
        date (java.util.Date.)]
    (is (= datahike.db.AsOfDB
           (type (d/as-of @conn date))))))

(deftest test-since-docs
  (let [cfg {:store {:backend :mem
                     :id "since"}
             :keep-history? true
             :schema-flexibility :read}
        _ (d/delete-database cfg)
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (is (= datahike.db.SinceDB
           (type (d/since @conn (java.util.Date.)))))))

(comment
  (def cfg {:store {:backend :mem
                             :id "datoms"}
            :initial-tx [{:db/ident :name
                          :db/type :db.type/string
                          :db/cardinality :db.cardinality/one}
                         {:db/ident :likes
                          :db/type :db.type/string
                          :db/cardinality :db.cardinality/many}
                         {:db/ident :friends
                          :db/type :db.type/ref
                          :db/cardinality :db.cardinality/many}]
            :keep-history? false
            :schema-flexibility :read})
  (d/delete-database cfg)
  (d/create-database cfg)
  (def db (d/connect cfg))
  (def dvec #(vector (:e %) (:a %) (:v %)))
  (d/transact db {:tx-data [{:db/id 4 :name "Ivan"}
                            {:db/id 4 :likes "fries"}
                            {:db/id 4 :likes "pizza"}
                            {:db/id 4 :friends 5}]})
  (d/transact db {:tx-data [{:db/id 5 :name "Oleg"}
                            {:db/id 5 :likes "candy"}
                            {:db/id 5 :likes "pie"}
                            {:db/id 5 :likes "pizza"}]})
  (d/datoms @db :avet)
  (d/datoms @db :avet :likes "pizza")
  (d/datoms @db :avet :db/ident :likes)
  (d/datoms @db :eavt))

(deftest test-datoms-docs
  (let [cfg {:store {:backend :mem
                     :id "datoms"}
             :initial-tx [{:db/ident :name
                           :db/type :db.type/string
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :likes
                           :db/type :db.type/string
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
    #_(is (= '([4 :likes "pizza"]
               [5 :likes "pizza"])
             (map dvec (d/datoms @db {:index :avet :components [:likes "pizza"]}))))
    ;; => (#datahike/Datom [1 :likes \"pizza\"]
    ;;     #datahike/Datom [2 :likes \"pizza\"])

    ;; find all datoms sorted by entity id, then attribute, then value
    (is (= '([1 :db/cardinality :db.cardinality/one] [1 :db/ident :name] [1 :db/type :db.type/string] [2 :db/cardinality :db.cardinality/many] [2 :db/ident :likes] [2 :db/type :db.type/string] [3 :db/cardinality :db.cardinality/many] [3 :db/ident :friends] [3 :db/type :db.type/ref] [4 :friends 5] [4 :likes "fries"] [4 :likes "pizza"] [4 :name "Ivan"] [5 :likes "candy"] [5 :likes "pie"] [5 :likes "pizza"] [5 :name "Oleg"])
           (map dvec (d/datoms @db {:index :eavt})))) ; => (...)))

    ;; get all values of :db.cardinality/many attribute
    (is (= '("fries" "pizza")
           (->> (d/datoms @db {:index :eavt :components [4 :likes]})
                (map :v))))

    ;; lookup entity ids by attribute value
    #_(is (= "fail"
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
             :initial-tx [{:db/ident :name
                           :db/type :db.type/string
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :likes
                           :db/type :db.type/string
                           :db/cardinality :db.cardinality/many}
                          {:db/ident :friends
                           :db/type :db.type/ref
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
                                    {:db/id 4 :friends 5}]})
        _ (d/transact db {:tx-data [{:db/id 5 :likes "candy"}
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
