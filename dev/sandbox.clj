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

  (api/q query @conn)

  (d/datoms @conn :eavt)

  (d/seek-datoms @conn :eavt)

  (d/rseek-datoms @conn :eavt)

  (def db (let [empty-db (d/empty-db {:aka { :db/cardinality :db.cardinality/many }})]
            (-> empty-db
                (d/db-with [{:db/id 1
                             :name  "Petr"
                             :email "petya@spb.ru"
                             :aka   ["I" "Great"]
                             :password "<SECRET>"}
                            {:db/id 2
                             :name  "Ivan"
                             :aka   ["Terrible" "IV"]
                             :password "<PROTECTED>"}
                            {:db/id 3
                             :name  "Nikolai"
                             :aka   ["II"]
                             :password "<UNKWOWN>"}
                            ]))))

  (def  remove-ivan (fn [_ datom] (not= 2 (:e datom))))

  db

  (hash (d/db-with db [[:db.fn/retractEntity 2]]))

  (hash (d/filter db remove-ivan))



  )
