(ns examples.core
  (:require [datahike.api :as d]))

;; define base uri we can connect to
(def uri "datahike:mem://temporal-index")

;; define schema
(def schema-tx [{:db/ident :name
                 :db/valueType :db.type/string
                 :db/unique :db.unique/identity
                 :db/index true
                 :db/cardinality :db.cardinality/one}
                {:db/ident :age
                 :db/valueType :db.type/long
                 :db/cardinality :db.cardinality/one}
                {:db/ident :sibling
                 :db/valueType :db.type/ref
                 :db/cardinality :db.cardinality/many}])

(def config {:uri uri :initial-tx schema-tx})

;; create the database with default configuration w
(d/create-database config)

;; connect to the database
(def conn (d/connect uri))

;; transact some data
(d/transact! conn [{:name "Alice"
                    :age 25}
                   {:name "Bob"
                    :age 35}
                   {:name "Charlie"
                    :age 45
                    :sibling [[:name "Alice"]]}])

;; search current data without any new data
(d/q '[:find ?e ?a ?v :where [?e ?a ?v]] (d/db conn))

;; let's change something
(d/transact! conn [{:db/id [:name "Alice"] :age 30}])

;; search for current data of Alice
(d/q '[:find ?v :in $ ?e :where [?e :age ?v]] (d/db conn) [:name "Alice"])

;; now we search within historical data
(d/q '[:find ?v :in $ ?e :where [?e :age ?v]] (d/history conn) [:name "Alice"])
