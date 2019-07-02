(ns datahike.test.schema
  (:require
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing use-fixtures]])
    [datahike.api :as d]
    [datahike.test.core :as tdc]))

(def name-schema {:db/id #db/id [:db.part/user]
                  :db/ident :name
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
          tx [{:db/id #db/id [db.part/user] :name "Alice"}]]

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
             (d/transact! conn [{:db/id #db/id [db.part/user] :name 42}]))))

      (testing "insert new data with additional attributes not in schema"
        (is (thrown-msg?
             "Bad entity attribute :age at {:db/id 3, :age 42}, not defined in current schema"
             (d/transact! conn [{:db/id #db/id [db.part/user] :name "Bob" :age 42}])))))

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
        (d/transact! conn [{:db/id #db/id [:db.part/user] :name "Alice"}])
        (is (= #{["Alice"]} (d/q find-name-q (d/db conn)))))

      (testing "extend schema"
        (d/transact! conn [{:db/id #db/id [:db.part/db]
                            :db/ident :age
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
        (d/transact! conn [{:db/id #db/id [:db.part/user]
                            :name "Bob"
                            :age 42}])
        (is (= #{["Alice"] ["Bob"]} (d/q find-name-q (d/db conn))))))

    (testing "cleanup"
      (d/delete-database test-uri))))
