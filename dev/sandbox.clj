(ns sandbox
  (:require [datahike.api :as d])
  (:import [java.net URI]))

(comment

  (def uri "datahike:mem://sandbox")

  #_(def uri "datahike:pg://johto:boofar@localhost:5433/johto")

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

  (d/create-database {:uri uri :initial-tx schema})

  (def conn (d/connect uri))

  (d/transact! conn [{:name  "Alice", :age   25}
                     {:name  "Bob", :age   35}
                     {:name "Charlie", :age 45 :sibling [[:name "Alice"] [:name "Bob"]]}])

  (d/q '[:find ?e ?n ?a :where [?e :name ?n] [?e :age ?a]] (d/db conn))

  )
