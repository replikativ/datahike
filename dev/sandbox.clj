(ns sandbox
  (:require [datahike.api :as d]
            [datahike.core :as c]))


(comment

  (def path "datahike:file:///tmp/local-db-0")

  (d/delete-database path)

  (d/create-database path)

  (def conn (d/connect path) )

  @(d/transact conn [{ :db/id 1, :name  "Ivan", :age   15 }
                     { :db/id 2, :name  "Petr", :age   37 }
                     { :db/id 3, :name  "Ivan", :age   37 }
                     { :db/id 4, :age 15 }
                     { :db/id 5, :name  "Wanja", :age   22 }
                     ])

  (def query '[:find ?e ?tx :where [?e :name "Wanja" ?tx]] )

  (c/q query @conn)

  (c/datoms @conn :eavt)

  (c/seek-datoms @conn :eavt)

  (c/rseek-datoms @conn :eavt)

  )
