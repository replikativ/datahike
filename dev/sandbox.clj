(ns sandbox
  (:require [datahike.api :as api]
            [datahike.impl.entity :as de]
            [hitchhiker.tree.core :as hc :refer [<??]]
            [datahike.db :as ddb]
            [datahike.constants :refer [tx0 txmax e0 emax]]
            [datahike.core :as d]))

(comment

  (def path "datahike:file:///tmp/local-db-0")

  (api/delete-database path)

  (api/create-database path)

  (def conn (api/connect path) )

  @(api/transact conn [{ :db/id 1, :name  "Ivan", :age   15 }
                     { :db/id 2, :name  "Petr", :age   37 }
                     { :db/id 3, :name  "Ivan", :age   37 }
                     { :db/id 4, :age 15 }
                     { :db/id 5, :name  "Wanja", :age   22 }
                     ])

  (def query '[:find ?e ?tx :where [?e :name "Wanja" ?tx]] )

  )
