(ns sandbox
  (:require [datahike.api :as d]
            [datahike.core :as c]))

(comment

  (def path "datahike:mem://dev")
  ;;(def path "datahike:file:///tmp/local-db-0")

  (d/delete-database path)

  (d/create-database path {:name {:db.cardinality :db.cardinality/many
                                  :db/ident :name}
                           :sibling {:db/valueType :db.type/ref
                                     :db.cardinality :db.cardinality/many
                                     }})

  (def conn (d/connect path) )

  (d/transact! conn [{ :db/id 1, :name  "Alice", :age   15 }
                     { :db/id 2, :name  "Bob", :age   37 }
                     { :db/id 3, :name  "Charlie", :age   37 }
                     { :db/id 4, :age 15 }
                     { :db/id 5, :name  "Daisy", :age   22 :sibling [1]}
                     ])

  (def query '[:find ?e ?a ?tx :where [?e :name "Daisy" ?tx] [?e :age ?a]] )

  (d/q query (d/db conn))

  (:rschema (d/db conn))
  (:schema (d/db conn))

  (def db (c/empty-db))

  (d/transact! conn [{:db/id #db/id[]
                      :db/ident :name
                      :db/valueType :db.type/string}])

  (d/q '[:find ?e ?ident :where [?e :db/ident ?ident]] (d/db conn))

  (:rschema db)

  (get-in db [:schema :db.part/db])


  )
