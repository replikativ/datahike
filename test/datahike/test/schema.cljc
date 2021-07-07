(ns datahike.test.schema
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer [is are deftest testing use-fixtures]])
   [datahike.api :as d]
   [datahike.schema :as ds]
   [datahike.db :as dd]
   [datahike.datom :as da]
   [datahike.constants :as c])
  (:import [java.lang System]))

#?(:clj
   (defn random-uuid []
     (java.util.UUID/randomUUID)))

(def name-schema {:db/ident       :name
                  :db/valueType   :db.type/string
                  :db/cardinality :db.cardinality/one})

(def personal-id-schema {:db/ident :id
                         :db/valueType :db.type/long
                         :db/unique :db.unique/identity
                         :db/cardinality :db.cardinality/many})

(def find-name-q '[:find ?n
                   :where [_ :name ?n]])

(def find-schema-q '[:find ?n ?vt ?c
                     :where
                     [?e :db/ident ?n]
                     [?e :db/valueType ?vt]
                     [?e :db/cardinality ?c]])

(deftest test-empty-db
  (let [cfg "datahike:mem://test-empty-db"
        _ (d/delete-database cfg)
        _ (d/create-database cfg)
        conn (d/connect cfg)
        db (d/db conn)
        tx [{:name "Alice"}]]

    (is (= c/non-ref-implicit-schema (dd/-schema db)))

    (testing "transact without schema present"
      (is (thrown-msg?
           "Bad entity attribute :name at {:db/id 1, :name \"Alice\"}, not defined in current schema"
           (d/transact conn tx))))

    (testing "transacting new schema"
      (d/transact conn [name-schema])
      (is (= #{[:name :db.type/string :db.cardinality/one]}
             (d/q find-schema-q (d/db conn))))
      (is (= (merge c/non-ref-implicit-schema
                    {:name     #:db{:ident       :name
                                    :valueType   :db.type/string
                                    :cardinality :db.cardinality/one}
                     1         :name})
             (dd/-schema (d/db conn)))))

    (testing "transacting data with schema present"
      (d/transact conn tx)
      (is (= #{["Alice"]}
             (d/q find-name-q (d/db conn)))))

    (testing "insert new data with wrong data type"
      (is (thrown-msg?
           "Bad entity value 42 at [:db/add 3 :name 42], value does not match schema definition. Must be conform to: string?"
           (d/transact conn [{:name 42}]))))

    (testing "insert new data with additional attributes not in schema"
      (is (thrown-msg?
           "Bad entity attribute :age at {:db/id 3, :age 42}, not defined in current schema"
           (d/transact conn [{:name "Bob" :age 42}]))))

    (testing "insert incomplete schema :db/valueType"
      (is (thrown-msg?
           "Incomplete schema transaction attributes, expected :db/ident, :db/valueType, :db/cardinality"
           (d/transact conn [{:db/valueType :db.type/string}]))))

    (testing "insert incomplete schema :db/cardinality"
      (is (thrown-msg?
           "Incomplete schema transaction attributes, expected :db/ident, :db/valueType, :db/cardinality"
           (d/transact conn [{:db/cardinality :db.cardinality/many}]))))

    (testing "insert incomplete schema :db/cardinality, :db/ident"
      (is (thrown-msg?
           "Incomplete schema transaction attributes, expected :db/ident, :db/valueType, :db/cardinality"
           (d/transact conn [{:db/ident :phone :db/cardinality :db.cardinality/many}]))))

    (testing "insert schema with incorrect value type"
      (is (thrown-msg?
           "Bad entity value :string at [:db/add 3 :db/valueType :string], value does not match schema definition. Must be conform to: #{:db.type/number :db.type/unique :db.type/instant :db.type/cardinality :db.type/tuple :db.type/boolean :db.type/bytes :db.type/uuid :db.type/value :db.type/string :db.type/keyword :db.type/ref :db.type/bigdec :db.type.install/attribute :db.type/float :db.type/bigint :db.type/double :db.type/long :db.type/valueType :db.type/symbol}"
           (d/transact conn [{:db/ident       :phone
                              :db/cardinality :db.cardinality/one
                              :db/valueType   :string}]))))))

(deftest test-db-with-initial-schema
  (let [cfg "datahike:mem://test-db-with-initial-schema"
        _ (d/delete-database cfg)
        _ (d/create-database cfg :initial-tx [name-schema])
        conn (d/connect cfg)]

    (testing "schema existence"
      (let [db (d/db conn)]
        (is (= (merge c/non-ref-implicit-schema
                      {:name     #:db{:ident       :name
                                      :valueType   :db.type/string
                                      :cardinality :db.cardinality/one}
                       1         :name})
               (dd/-schema db)))
        (is (= #{[:name :db.type/string :db.cardinality/one]} (d/q find-schema-q db)))))

    (testing "insert new data according to schema"
      (d/transact conn [{:name "Alice"}])
      (is (= #{["Alice"]} (d/q find-name-q (d/db conn)))))

    (testing "extend schema with :age"
      (d/transact conn [{:db/ident       :age
                         :db/valueType   :db.type/long
                         :db/cardinality :db.cardinality/one}])
      (let [db (d/db conn)]
        (is (= (merge c/non-ref-implicit-schema
                      {:name     #:db{:ident       :name
                                      :valueType   :db.type/string
                                      :cardinality :db.cardinality/one}
                       1         :name
                       :age      #:db{:ident       :age
                                      :valueType   :db.type/long
                                      :cardinality :db.cardinality/one}
                       3         :age})
               (dd/-schema db)))
        (is (= #{[:name :db.type/string :db.cardinality/one] [:age :db.type/long :db.cardinality/one]}
               (d/q find-schema-q db)))))

    (testing "insert new data"
      (d/transact conn [{:name "Bob" :age 42}])
      (is (= #{["Alice"] ["Bob"]} (d/q find-name-q (d/db conn)))))

    (testing "change cardinality for :name"
      (d/transact conn [{:db/ident :name
                         :db/cardinality :db.cardinality/many}])
      (let [db (d/db conn)]
        (is (= (merge c/non-ref-implicit-schema
                      {:name     #:db{:ident       :name
                                      :valueType   :db.type/string
                                      :cardinality :db.cardinality/many}
                       1         :name
                       :age      #:db{:ident       :age
                                      :valueType   :db.type/long
                                      :cardinality :db.cardinality/one}
                       3         :age})
               (dd/-schema db)))
        (is (= #{[:name :db.type/string :db.cardinality/many] [:age :db.type/long :db.cardinality/one]}
               (d/q find-schema-q db)))))))

(defn testing-type [conn type-name tx-val tx-id wrong-val]
  (testing type-name
    (let [schema-name (keyword "value" type-name)]
      (d/transact conn [{schema-name tx-val}])
      (is (= #{[tx-val]}
             (d/q '[:find ?v :in $ ?sn :where [?e ?sn ?v]] (d/db conn) schema-name)))
      (is (thrown-msg?
           (str "Bad entity value "
                wrong-val
                " at [:db/add "
                tx-id
                " "
                schema-name
                " "
                wrong-val
                "], value does not match schema definition. Must be conform to: "
                (ds/describe-type (keyword "db.type" type-name)))
           (d/transact conn [{schema-name wrong-val}]))))))

(deftest test-schema-types
  (let [cfg "datahike:mem://test-schema-types"
        _ (d/delete-database cfg)
        schema-tx [{:db/ident       :value/bigdec
                    :db/valueType   :db.type/bigdec
                    :db/cardinality :db.cardinality/one}
                   {:db/ident       :value/bigint
                    :db/valueType   :db.type/bigint
                    :db/cardinality :db.cardinality/one}
                   {:db/ident       :value/boolean
                    :db/valueType   :db.type/boolean
                    :db/cardinality :db.cardinality/one}
                   {:db/ident       :value/double
                    :db/valueType   :db.type/double
                    :db/cardinality :db.cardinality/one}
                   {:db/ident       :value/float
                    :db/valueType   :db.type/float
                    :db/cardinality :db.cardinality/one}
                   {:db/ident       :value/instant
                    :db/valueType   :db.type/instant
                    :db/cardinality :db.cardinality/one}
                   {:db/ident       :value/keyword
                    :db/valueType   :db.type/keyword
                    :db/cardinality :db.cardinality/one}
                   {:db/ident       :value/long
                    :db/valueType   :db.type/long
                    :db/cardinality :db.cardinality/one}
                   {:db/ident       :value/string
                    :db/valueType   :db.type/string
                    :db/cardinality :db.cardinality/one}
                   {:db/ident       :value/symbol
                    :db/valueType   :db.type/symbol
                    :db/cardinality :db.cardinality/one}
                   {:db/ident       :value/uuid
                    :db/valueType   :db.type/uuid
                    :db/cardinality :db.cardinality/one}]
        _ (d/create-database cfg :initial-tx schema-tx)
        conn (d/connect cfg)]

    (testing-type conn "bigdec" (bigdec 1) 13 1)
    (testing-type conn "bigint" (biginteger 1) 14 1.0)
    (testing-type conn "boolean" true 15 0)
    (testing-type conn "double" (double 1) 16 1)
    (testing-type conn "float" (float 1) 17 1)
    (testing-type conn "instant" (java.util.Date.) 18 1)
    (testing-type conn "keyword" :one 19 1)
    (testing-type conn "long" (long 2) 20 :2)
    (testing-type conn "string" "one" 21 :one)
    (testing-type conn "symbol" 'one 22 :one)
    (testing-type conn "uuid" (random-uuid) 23 1)))

(deftest test-schema-cardinality
  (let [cfg "datahike:mem://test-schema-cardinality"
        _ (d/delete-database cfg)
        schema-tx [{:db/ident       :owner
                    :db/valueType   :db.type/string
                    :db/index       true
                    :db/unique      :db.unique/identity
                    :db/cardinality :db.cardinality/one}
                   {:db/ident       :cars
                    :db/valueType   :db.type/keyword
                    :db/cardinality :db.cardinality/many}]
        _ (d/create-database cfg :initial-tx schema-tx)
        conn (d/connect cfg)]

    (testing "insert :owner and :cars one by one"
      (d/transact conn [{:db/id -1
                         :owner "Alice"}
                        {:db/id -1
                         :cars  :audi}
                        {:db/id -1
                         :cars  :bmw}])
      (is (= #{["Alice" :audi] ["Alice" :bmw]}
             (d/q '[:find ?o ?c :where [?e :owner ?o] [?e :cars ?c]] (d/db conn)))))

    (testing "insert :cars as list"
      (d/transact conn [{:db/id -2
                         :owner "Bob"
                         :cars  [:chrysler :daimler]}])
      (is (= #{["Alice" :audi] ["Alice" :bmw] ["Bob" :chrysler] ["Bob" :daimler]}
             (d/q '[:find ?o ?c :where [?e :owner ?o] [?e :cars ?c]] (d/db conn)))))

    (testing "insert to cardinality one"
      (d/transact conn [{:db/id [:owner "Alice"]
                         :owner "Charlie"}])
      (is (= #{["Charlie" :audi] ["Charlie" :bmw] ["Bob" :chrysler] ["Bob" :daimler]}
             (d/q '[:find ?o ?c :where [?e :owner ?o] [?e :cars ?c]] (d/db conn)))))

    (testing "test cardinality change if unique is set"
      (is (thrown-msg?
           "Update not supported for these schema attributes"
           (d/transact conn [{:db/ident       :owner
                              :db/cardinality :db.cardinality/many}])))
      (is (thrown-msg?
           "Update not supported for these schema attributes"
           (d/transact conn [{:db/id [:db/ident :owner]
                              :db/cardinality :db.cardinality/many}]))))))

(deftest test-schema-persistence
  (testing "test file persistence"
    (let [os (System/getProperty "os.name")
          path (case os
                 "Windows 10"  (str (System/getProperty "java.io.tmpdir") "dh-test-persistence")
                 "/tmp/dh-test-persistence")
          cfg {:store {:backend :file
                       :path path}
               :initial-tx [name-schema]}
          _ (d/delete-database cfg)
          _ (d/create-database cfg)
          conn (d/connect cfg)]
      (testing "schema exists on creation and first connection"
        (is (= #{[:name :db.type/string :db.cardinality/one]} (d/q find-schema-q (d/db conn)))))
      (testing "reconnect with db"
        (let [new-conn (d/connect cfg)]
          (is (= #{[:name :db.type/string :db.cardinality/one]} (d/q find-schema-q (d/db new-conn))))))
      (d/delete-database cfg)))
  (testing "test mem persistence"
    (let [cfg "datahike:mem://test-schema-persistence"
          _ (d/create-database cfg :initial-tx [name-schema])
          conn (d/connect cfg)]
      (testing "schema exists on creation and first connection"
        (is (= #{[:name :db.type/string :db.cardinality/one]} (d/q find-schema-q (d/db conn)))))
      (testing "reconnect with db"
        (let [new-conn (d/connect cfg)]
          (is (= #{[:name :db.type/string :db.cardinality/one]} (d/q find-schema-q (d/db new-conn))))))
      (d/delete-database cfg))))

(deftest test-schema-on-read-db
  (testing "test database creation with schema-on-read"
    (let [cfg "datahike:mem://test-schemaless-db"
          _ (d/delete-database cfg)
          _ (d/create-database cfg :schema-on-read true)
          conn (d/connect cfg)]
      (testing "insert any data"
        (d/transact conn [{:name "Alice" :age 26} {:age "12" :car :bmw}])
        (is (= #{[1 "Alice" 26]}
               (d/q '[:find ?e ?n ?a :where [?e :name ?n] [?e :age ?a]] (d/db conn))))
        (is (= #{[2 "12" :bmw]}
               (d/q '[:find ?e ?a ?c :where [?e :age ?a] [?e :car ?c]] (d/db conn))))))))

(deftest test-ident
  (testing "use db/ident as enum"
    (let [cfg "datahike:mem://test-ident"
          schema [{:db/ident :important}
                  {:db/ident :archive}
                  {:db/ident       :message
                   :db/valueType   :db.type/string
                   :db/cardinality :db.cardinality/one}
                  {:db/ident       :tag
                   :db/valueType   :db.type/ref
                   :db/cardinality :db.cardinality/many}]
          _ (d/delete-database cfg)
          _ (d/create-database cfg :initial-tx schema)
          conn (d/connect cfg)]
      (testing "insert data with enums"
        (d/transact conn [{:message "important" :tag :important} {:message "archive" :tag [:important :archive]}])
        (is (= #{["important" :important] ["archive" :important] ["archive" :archive]}
               (d/q '[:find ?m ?t :where [?e :message ?m] [?e :tag ?te] [?te :db/ident ?t]] (d/db conn))))))))

(deftest test-remove-schema
  (let [cfg "datahike:mem://test-remove-schema"
        _ (d/delete-database cfg)
        _ (d/create-database cfg)
        conn (d/connect cfg)
        db (d/db conn)]
    (testing "non existing schema should throw exception"
      (is (thrown-msg? "Schema with attribute :name does not exist"
                       (dd/remove-schema db (da/datom 1 :db/ident :name)))))
    (testing "when upserting a non existing schema, it should not throw an exception"
      (is (d/transact conn [name-schema])))))

(deftest test-update-schema
  (let [cfg "datahike:mem://test-update-schema"
        _ (d/delete-database cfg)
        _ (d/create-database cfg :initial-schema [name-schema personal-id-schema])
        conn (d/connect cfg)
        db (d/db conn)

        update-name-attr (fn [attr new-value] (d/transact conn {:tx-data [(assoc name-schema attr new-value)]}))]
    (testing "Allow to update doc"
      (is (update-name-attr :db/doc "Some doc") "It should be allowed to add :db/doc.")
      (is (update-name-attr :db/doc "Some new doc") "It should be allowed to update :db/doc.")
      (is (d/transact conn {:tx-data [[:db/retract :name :db/doc]]}) "It should be allowed to retract :db/doc."))

    (testing "Allow to toggle noHistory"
      (is (update-name-attr :db/noHistory true) "It should be allowed to enable :db/noHistory.")
      (is (update-name-attr :db/noHistory false) "It should be allowed to disable :db/noHistory."))

    (testing "Allow to toggle isComponent"
      (is (update-name-attr :db/isComponent true) "It should be allowed to enable :db/isComponent.")
      (is (update-name-attr :db/isComponent false) "It should be allowed to disable :db/isComponent."))

    (testing "Allow to update :db/unique only if it already exists"
      (is (thrown-msg? "Update not supported for these schema attributes"
                       (d/transact conn {:tx-data [(assoc name-schema :db/unique :db.unique/value)]}))
          "It shouldn't be allowed to update :db/unique if it doesn't exist already.")
      (is (d/transact conn {:tx-data [(assoc personal-id-schema :db/unique :db.unique/value)]})
          "It should be allowed to update :db/unique if it exists already."))

    (testing "Allow to update :db/cardinality "
      (testing "if :db/unique is not set"
        (is (update-name-attr :db/cardinality :db.cardinality/many)
            "It should be allowed to update :db/cardinality to :db.cardinality/many.")
        (is (update-name-attr :db/cardinality :db.cardinality/one)
            "It should be allowed to update :db/cardinality to :db.cardinality/one."))

      (testing "if :db/unique is set"
        (is (d/transact conn {:tx-data [(assoc personal-id-schema :db/cardinality :db.cardinality/one)]})
            "It should be allowed to update :db/cardinality to :db.cardinality/one.")
        (is (thrown-msg? "Update not supported for these schema attributes"
                         (d/transact conn {:tx-data [(assoc personal-id-schema :db/cardinality :db.cardinality/many)]}))
            "It shouldn't be allowed to update :db/cardinality to :db.cardinality/many")))))
