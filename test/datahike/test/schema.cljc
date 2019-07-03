(ns datahike.test.schema
  (:require
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing use-fixtures]])
    [datahike.api :as d]
    [datahike.test.core :as tdc]))

(def name-schema {:db/ident :name
                  :db/valueType :db.type/string
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

      (is (= {} (:schema db)))

      (testing "transact without schema present"
        (is (thrown-msg?
             "No schema found in db."
             (d/transact! conn tx))))

      (testing "transacting new schema"
        (d/transact! conn [name-schema])
        (is (= #{[:name :db.type/string :db.cardinality/one]}
               (d/q find-schema-q (d/db conn))))
        (is (=  {:name #:db{:ident :name
                            :valueType :db.type/string
                            :cardinality :db.cardinality/one}
                 1 :name}
                (:schema (d/db conn)))))

      (testing "transacting data with schema present"
        (d/transact! conn tx)
        (is (=  #{["Alice"]}
                (d/q find-name-q (d/db conn)))))

      (testing "insert new data with wrong data type"
        (is (thrown-msg?
             "Bad entity value 42 at [:db/add 3 :name 42], value does not match schema definition. Must be of type :db.type/string"
             (d/transact! conn [{:name 42}]))))

      (testing "insert new data with additional attributes not in schema"
        (is (thrown-msg?
             "Bad entity attribute :age at {:db/id 3, :age 42}, not defined in current schema"
             (d/transact! conn [{:name "Bob" :age 42}]))))

      (testing "insert incomplete schema"
        (is (thrown-msg?
             "Incomplete schema transaction attributes, expected :db/ident, :db/valueType, :db/cardinality"
             (d/transact! conn [{:db/ident :phone}])))))

    (testing "cleanup"
      (d/delete-database test-uri))))

(deftest test-db-with-initial-schema
  (let [test-uri "datahike:mem://test-db-with-initial-schema"]

    (testing "create database with initial schema"
      (d/create-database test-uri [name-schema]))

    (let [conn (d/connect test-uri)]

      (testing "schema existence"
        (let [db (d/db conn)]
          (is (= {:name #:db{:ident :name,
                             :valueType :db.type/string
                             :cardinality :db.cardinality/one}
                  1 :name}
                 (:schema db)))
          (is (= #{[:name :db.type/string :db.cardinality/one]} (d/q find-schema-q db)))))

      (testing "insert new data according to schema"
        (d/transact! conn [{:name "Alice"}])
        (is (= #{["Alice"]} (d/q find-name-q (d/db conn)))))

      (testing "extend schema with :age"
        (d/transact! conn [{:db/ident :age
                            :db/valueType :db.type/long
                            :db/cardinality :db.cardinality/one}])
        (let [db (d/db conn)]
          (is (= {:name #:db{:ident :name,
                             :valueType :db.type/string,
                             :cardinality :db.cardinality/one},
                  1 :name,
                  :age #:db{:ident :age,
                            :valueType :db.type/long,
                            :cardinality :db.cardinality/one},
                  3 :age}
                 (:schema db)))
          (is (= #{[:name :db.type/string :db.cardinality/one] [:age :db.type/long :db.cardinality/one]}
                 (d/q find-schema-q db)))))

      (testing "insert new data"
        (d/transact! conn [{:name "Bob" :age 42}])
        (is (= #{["Alice"] ["Bob"]} (d/q find-name-q (d/db conn)))))

      (testing "change cardinality for :name"
        (d/transact! conn [{:db/id 1
                            :db/cardinality :db.cardinality/many}])
        (let [db (d/db conn)]
          (is (= {:name #:db{:ident :name,
                             :valueType :db.type/string,
                             :cardinality :db.cardinality/many},
                  1 :name,
                  :age #:db{:ident :age,
                            :valueType :db.type/long,
                            :cardinality :db.cardinality/one},
                  3 :age}
                 (:schema db)))
          (is (= #{[:name :db.type/string :db.cardinality/many] [:age :db.type/long :db.cardinality/one]}
                 (d/q find-schema-q db))))))

    (testing "cleanup"
      (d/delete-database test-uri))))

(defn testing-type [conn type-name tx-val tx-id wrong-val]
  (testing "float"
    (let [schema-name (keyword "value" type-name)]
      (d/transact! conn [{schema-name tx-val}])
      (is (= #{[tx-val]}
             (d/q '[:find ?v :in $ ?sn :where [?e ?sn  ?v]] (d/db conn) schema-name)))
      (is (thrown-msg?
           (str "Bad entity value "
                wrong-val
                " at [:db/add "
                tx-id
                " "
                schema-name
                " "
                wrong-val
                "], value does not match schema definition. Must be of type :db.type/"
                type-name)
           (d/transact! conn [{schema-name wrong-val}]))))))

(deftest test-schema-types
  (let [uri "datahike:mem://test-schema-types"
        schema-tx [{:db/ident :value/bigdec
                    :db/valueType :db.type/bigdec
                    :db/cardinality :db.cardinality/one}
                   {:db/ident :value/bigint
                    :db/valueType :db.type/bigint
                    :db/cardinality :db.cardinality/one}
                   {:db/ident :value/boolean
                    :db/valueType :db.type/boolean
                    :db/cardinality :db.cardinality/one}
                   {:db/ident :value/double
                    :db/valueType :db.type/double
                    :db/cardinality :db.cardinality/one}
                   {:db/ident :value/float
                    :db/valueType :db.type/float
                    :db/cardinality :db.cardinality/one}
                   {:db/ident :value/instant
                    :db/valueType :db.type/instant
                    :db/cardinality :db.cardinality/one}
                   {:db/ident :value/keyword
                    :db/valueType :db.type/keyword
                    :db/cardinality :db.cardinality/one}
                   {:db/ident :value/long
                    :db/valueType :db.type/long
                    :db/cardinality :db.cardinality/one}
                   {:db/ident :value/string
                    :db/valueType :db.type/string
                    :db/cardinality :db.cardinality/one}
                   {:db/ident :value/symbol
                    :db/valueType :db.type/symbol
                    :db/cardinality :db.cardinality/one}
                   {:db/ident :value/uuid
                    :db/valueType :db.type/uuid
                    :db/cardinality :db.cardinality/one}]
        _ (d/create-database uri schema-tx)
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
    (testing-type conn "symbol" 'one  22 :one)
    (testing-type conn "uuid" (java.util.UUID/randomUUID)  23 1)

    (d/delete-database uri)))
