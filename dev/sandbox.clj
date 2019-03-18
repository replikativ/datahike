(ns sandbox
  (:require [datahike.api :as d]
            [datahike.core :as c]))


(comment

  (def path "datahike:file:///tmp/local-db-0")

  (d/delete-database path)

  (d/create-database path)

  (def conn (d/connect path))

  @(d/transact conn [{:db/id 1 :name "Alice"}
                     {:db/id 2 :name "Bob"}
                     {:db/id 3 :name "Charlie"}])

  (d/q '[:find ?n :where [?e :name ?n]] @conn)

  (c/datoms (d/db conn) :eavt)

  )
