(ns sandbox
  (:require [datahike.api :as d]))

(comment

  (def uri "datahike:mem://sandbox")

  (d/delete-database uri)

  (def schema [{:db/ident :name
                :db/cardinality :db.cardinality/one
                :db/index true
                :db/unique :db.unique/identity
                :db/valueType :db.type/string}
               {:db/ident :sibling
                :db/cardinality :db.cardinality/many
                :db/valueType :db.type/ref}
               {:db/ident :age
                :db/cardinality :db.cardinality/one
                :db/valueType :db.type/long}])

  (d/create-database uri :initial-tx schema)

  (def conn (d/connect uri))

  (def result (d/transact conn [{:name  "Alice", :age   25}
                                {:name  "Bob", :age   35}
                                {:name "Charlie", :age 45 :sibling [[:name "Alice"] [:name "Bob"]]}]))

  (d/q '[:find ?e ?a ?v ?t :where [?e ?a ?v ?t]] (d/history @conn))

  (d/q {:query '{:find [?e ?a] :in [$ ?n] :where [[?e :name ?n] [?e :age ?a]]} :args [@conn "Alice"]} )

  (d/q '[:find ?e ?a ?n :where [?e ?a ?n ?tx false]] (d/history @conn)))
