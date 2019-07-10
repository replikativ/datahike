(ns sandbox
  (:require [datahike.api :as d]
            [datahike.core :as c]))

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

  (def db (d/db conn))

  (d/transact! conn [{:name  "Alice", :age   27}
                     {:name  "Bob", :age   37}
                     {:name  "Charlie", :age   47}
                     {:name  "Daisy", :age   24 :sibling [:name "Alice"]}])

  (d/transact! conn [{:db/id [:name "Alice"]
                      :name "bob"}])


  )
