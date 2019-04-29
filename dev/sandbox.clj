(ns sandbox
  (:require [datahike.api :as d]))

(comment

  (def path "datahike:mem://dev")
  ;;(def path "datahike:file:///tmp/local-db-0")

  (d/delete-database path)

  (d/create-database path)

  (def conn (d/connect path) )

  (d/transact! conn [{ :db/id 1, :name  "Alice", :age   15 }
                     { :db/id 2, :name  "Bob", :age   37 }
                     { :db/id 3, :name  "Charlie", :age   37 }
                     { :db/id 4, :age 15 }
                     { :db/id 5, :name  "Daisy", :age   22 }
                     ])

  (def query '[:find ?e ?a ?tx :where [?e :name "Daisy" ?tx] [?e :age ?a]] )

  (d/q query (d/db conn))


  )
