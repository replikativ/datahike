(ns sandbox
  (:require [datahike.api :as d]
            [datahike.core :as c]))


(comment

  (def path "datahike:file:///tmp/local-db-0")

  (d/delete-database path)

  (d/create-database path)

  (def conn (d/connect path) )

  @(d/transact conn [{ :db/id 1, :name  "Alice" :age 30 }
                     { :db/id 2, :name  "Bob" :age 35}
                     { :db/id 3 :name "Charlie" :age 40}
                     ])

  (def query '[:find ?e ?n ?tx :where [?e :name ?n ?tx]] )

  (c/q query @conn)

  (c/datoms @conn :eavt)

  (c/seek-datoms @conn :eavt)

  (c/rseek-datoms @conn :eavt)

  )
