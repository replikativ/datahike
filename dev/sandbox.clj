(ns sandbox
  (:require [datahike.api :as d]))

(comment

  (def uri "datahike:mem://dev")
  ;;(def path "datahike:file:///tmp/local-db-0")
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

  (d/transact! conn [{:name  "Alice", :age   27}
                     {:name  "Bob", :age   37}
                     {:name  "Charlie", :age   47}
                     {:name "Daisy", :age 24 :sibling [[:name "Alice"] [:name "Charlie"]]}])

)
