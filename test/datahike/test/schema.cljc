(ns datahike.test.schema
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing use-fixtures]])
   [datahike.api :as d]
   [datahike.schema :as ds]
   [datahike.test.core :as tdc])
  (:import [java.io File]))

(def name-schema {:db/ident       :name
                  :db/valueType   :db.type/string
                  :db/cardinality :db.cardinality/one})

(def find-name-q '[:find ?n
                   :where [_ :name ?n]])

(def find-schema-q '[:find ?n ?vt ?c
                     :where
                     [?e :db/ident ?n]
                     [?e :db/valueType ?vt]
                     [?e :db/cardinality ?c]])

(deftest test-empty-db
  (let [test-uri "datahike:mem://test-empty-db"]
    (testing "Create empty database"
      (d/create-database test-uri))

    (let [conn (d/connect test-uri)
          db (d/db conn)
          tx [{:name "Alice"}]]

      (is (= {:db/ident {:db/unique :db.unique/identity}} (:schema db)))

      (testing "transact without schema present"
        (is (thrown-msg?
              "No schema found in db."
              (d/transact! conn tx))))

      (testing "transacting new schema"
        (d/transact! conn [name-schema])
        (is (= #{[:name :db.type/string :db.cardinality/one]}
               (d/q find-schema-q (d/db conn))))
        (is (= {:db/ident #:db{:unique :db.unique/identity}
                :name     #:db{:ident       :name
                               :valueType   :db.type/string
                               :cardinality :db.cardinality/one}
                1         :name}
               (:schema (d/db conn)))))

      (testing "transacting data with schema present"
        (d/transact! conn tx)
        (is (= #{["Alice"]}
               (d/q find-name-q (d/db conn)))))

      (testing "insert new data with wrong data type"
        (is (thrown-msg?
              "Bad entity value 42 at [:db/add 3 :name 42], value does not match schema definition. Must be conform to: string?"
              (d/transact! conn [{:name 42}]))))

      (testing "insert new data with additional attributes not in schema"
        (is (thrown-msg?
              "Bad entity attribute :age at {:db/id 3, :age 42}, not defined in current schema"
              (d/transact! conn [{:name "Bob" :age 42}]))))

      (testing "insert incomplete schema :db/ident"
        (is (thrown-msg?
              "Incomplete schema transaction attributes, expected :db/ident, :db/valueType, :db/cardinality"
              (d/transact! conn [{:db/ident :phone}]))))

      (testing "insert incomplete schema :db/valueType"
        (is (thrown-msg?
              "Incomplete schema transaction attributes, expected :db/ident, :db/valueType, :db/cardinality"
              (d/transact! conn [{:db/valueType :db.type/string}]))))

      (testing "insert incomplete schema :db/cardinality"
        (is (thrown-msg?
              "Incomplete schema transaction attributes, expected :db/ident, :db/valueType, :db/cardinality"
              (d/transact! conn [{:db/cardinality :db.cardinality/many}]))))

      (testing "insert incomplete schema :db/cardinality, :db/ident"
        (is (thrown-msg?
              "Incomplete schema transaction attributes, expected :db/ident, :db/valueType, :db/cardinality"
              (d/transact! conn [{:db/ident :phone :db/cardinality :db.cardinality/many}]))))

      (testing "insert schema with incorrect value type"
        (is (thrown-msg?
              "Bad entity value :string at [:db/add 3 :db/valueType :string], value does not match schema definition. Must be conform to: #{:db.type/instant :db.type/boolean :db.type/uuid :db.type/value :db.type/string :db.type/keyword :db.type/ref :db.type/bigdec :db.type/float :db.type/bigint :db.type/double :db.type/long :db.type/symbol}"
              (d/transact! conn [{:db/ident       :phone
                                  :db/cardinality :db.cardinality/one
                                  :db/valueType   :string}])))))

    (testing "cleanup"
      (d/delete-database test-uri))))

(deftest test-db-with-initial-schema
  (let [test-uri "datahike:mem://test-db-with-initial-schema"]

    (testing "create database with initial schema"
      (d/create-database {:uri test-uri :initial-tx [name-schema]}))

    (let [conn (d/connect test-uri)]

      (testing "schema existence"
        (let [db (d/db conn)]
          (is (= {:db/ident {:db/unique :db.unique/identity}
                  :name     #:db{:ident       :name
                                 :valueType   :db.type/string
                                 :cardinality :db.cardinality/one}
                  1         :name}
                 (:schema db)))
          (is (= #{[:name :db.type/string :db.cardinality/one]} (d/q find-schema-q db)))))

      (testing "insert new data according to schema"
        (d/transact! conn [{:name "Alice"}])
        (is (= #{["Alice"]} (d/q find-name-q (d/db conn)))))

      (testing "extend schema with :age"
        (d/transact! conn [{:db/ident       :age
                            :db/valueType   :db.type/long
                            :db/cardinality :db.cardinality/one}])
        (let [db (d/db conn)]
          (is (= {:db/ident {:db/unique :db.unique/identity}
                  :name     #:db{:ident       :name
                                 :valueType   :db.type/string
                                 :cardinality :db.cardinality/one}
                  1         :name
                  :age      #:db{:ident       :age
                                 :valueType   :db.type/long
                                 :cardinality :db.cardinality/one}
                  3         :age}
                 (:schema db)))
          (is (= #{[:name :db.type/string :db.cardinality/one] [:age :db.type/long :db.cardinality/one]}
                 (d/q find-schema-q db)))))

      (testing "insert new data"
        (d/transact! conn [{:name "Bob" :age 42}])
        (is (= #{["Alice"] ["Bob"]} (d/q find-name-q (d/db conn)))))

      (testing "change cardinality for :name"
        (d/transact! conn [{:db/id          1
                            :db/cardinality :db.cardinality/many}])
        (let [db (d/db conn)]
          (is (= {:db/ident {:db/unique :db.unique/identity}
                  :name     #:db{:ident       :name
                                 :valueType   :db.type/string
                                 :cardinality :db.cardinality/many}
                  1         :name
                  :age      #:db{:ident       :age
                                 :valueType   :db.type/long
                                 :cardinality :db.cardinality/one}
                  3         :age}
                 (:schema db)))
          (is (= #{[:name :db.type/string :db.cardinality/many] [:age :db.type/long :db.cardinality/one]}
                 (d/q find-schema-q db))))))

    (testing "cleanup"
      (d/delete-database test-uri))))

(defn testing-type [conn type-name tx-val tx-id wrong-val]
  (testing type-name
    (let [schema-name (keyword "value" type-name)]
      (d/transact! conn [{schema-name tx-val}])
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
            (d/transact! conn [{schema-name wrong-val}]))))))

(deftest test-schema-types
  (let [uri "datahike:mem://test-schema-types"
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
        _ (d/create-database {:uri uri :initial-tx schema-tx})
        conn (d/connect uri)]

    (testing-type conn "bigdec" (bigdec 1) 13 1)
    (testing-type conn "bigint" (biginteger 1) 14 1)
    (testing-type conn "boolean" true 15 0)
    (testing-type conn "double" (double 1) 16 1)
    (testing-type conn "float" (float 1) 17 1)
    (testing-type conn "instant" (java.util.Date.) 18 1)
    (testing-type conn "keyword" :one 19 1)
    (testing-type conn "long" (long 2) 20 :2)
    (testing-type conn "string" "one" 21 :one)
    (testing-type conn "symbol" 'one 22 :one)
    (testing-type conn "uuid" (java.util.UUID/randomUUID) 23 1)

    (d/delete-database uri)))

(deftest test-schema-cardinality
  (let [uri "datahike:mem://test-schema-cardinality"
        schema-tx [{:db/ident       :owner
                    :db/valueType   :db.type/string
                    :db/index       true
                    :db/unique      :db.unique/identity
                    :db/cardinality :db.cardinality/one}
                   {:db/ident       :cars
                    :db/valueType   :db.type/keyword
                    :db/cardinality :db.cardinality/many}]
        _ (d/create-database {:uri uri :initial-tx schema-tx})
        conn (d/connect uri)]

    (testing "insert :owner and :cars one by one"
      (d/transact! conn [{:db/id -1
                          :owner "Alice"}
                         {:db/id -1
                          :cars  :audi}
                         {:db/id -1
                          :cars  :bmw}])
      (is (= #{["Alice" :audi] ["Alice" :bmw]}
             (d/q '[:find ?o ?c :where [?e :owner ?o] [?e :cars ?c]] (d/db conn)))))

    (testing "insert :cars as list"
      (d/transact! conn [{:db/id -2
                          :owner "Bob"
                          :cars [:chrysler :daimler]}])
      (is (= #{["Alice" :audi] ["Alice" :bmw] ["Bob" :chrysler] ["Bob" :daimler]}
             (d/q '[:find ?o ?c :where [?e :owner ?o] [?e :cars ?c]] (d/db conn)))))

    (testing "insert to cardinality one"
      (d/transact! conn [{:db/id [:owner "Alice"]
                          :owner "Charlie"}])
      (is (= #{["Charlie" :audi] ["Charlie" :bmw] ["Bob" :chrysler] ["Bob" :daimler]}
             (d/q '[:find ?o ?c :where [?e :owner ?o] [?e :cars ?c]] (d/db conn)))))

    (testing "test  cardinality change if unique is set"
      (is (thrown-msg?
            "Update not supported for these schema attributes"
            (d/transact! conn [{:db/id          [:db/ident :owner]
                                :db/cardinality :db.cardinality/many}]))))

    (d/delete-database uri)))

;; see https://gist.github.com/edw/5128978
(defn delete-files-recursively [fname & [silently]]
  (letfn [(delete-f [^File file]
            (when (.exists file)
              (when (.isDirectory file)
                (doseq [child-file (.listFiles file)]
                  (delete-f child-file)))
              (clojure.java.io/delete-file file silently)))]
    (delete-f (clojure.java.io/file fname))))


(deftest test-schema-persistence
  (testing "test file persistence"
    (let [file-path "/tmp/dh-test-schema-persistence"
          _ (delete-files-recursively file-path)
          uri (str "datahike:file://" file-path)
          _ (d/create-database {:uri uri :initial-tx [name-schema]})
          conn (d/connect uri)]
      (testing "schema exists on creation and first connection"
        (is (= #{[:name :db.type/string :db.cardinality/one]} (d/q find-schema-q (d/db conn)))))
      (testing "reconnect with db"
        (let [new-conn (d/connect uri)]
          (is (= #{[:name :db.type/string :db.cardinality/one]} (d/q find-schema-q (d/db new-conn))))))
      (d/delete-database uri)))
  (testing "test mem persistence"
    (let [uri "datahike:mem://test-schema-persistence"
          _ (d/create-database {:uri uri :initial-tx [name-schema]})
          conn (d/connect uri)]
      (testing "schema exists on creation and first connection"
        (is (= #{[:name :db.type/string :db.cardinality/one]} (d/q find-schema-q (d/db conn)))))
      (testing "reconnect with db"
        (let [new-conn (d/connect uri)]
          (is (= #{[:name :db.type/string :db.cardinality/one]} (d/q find-schema-q (d/db new-conn))))))
      (d/delete-database uri))))

(deftest test-schema-on-read-db
  (testing "test database creation with schema-on-read"
    (let [uri "datahike:mem://test-schemaless-db"
          _ (d/create-database {:uri uri :schema-on-read true})
          conn (d/connect uri)]
      (testing "insert any data"
        (d/transact! conn [{:name "Alice" :age 26} {:age "12" :car :bmw}])
        (is (= #{[1 "Alice" 26]}
               (d/q '[:find ?e ?n ?a :where [?e :name ?n] [?e :age ?a]] (d/db conn))))
        (is (= #{[2 "12" :bmw]}
               (d/q '[:find ?e ?a ?c :where [?e :age ?a] [?e :car ?c]] (d/db conn)))))
      (d/delete-database uri))))
