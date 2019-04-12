(ns sandbox
  (:require [datahike.api :as d]
            [datahike.db :as ddb]
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

  (d/q '[:find [?v ...] :where [?e :name ?v]] @conn)

  (def db (c/db-with (c/empty-db {:name    {:db/unique :db.unique/identity}
                                  :friends {:db/valueType   :db.type/ref
                                            :db/cardinality :db.cardinality/many}})
                     [{:db/id 1 :name "Ivan" :friends [2 3]}
                      {:db/id 2 :name "Petr" :friends 3}
                      {:db/id 3 :name "Oleg"}]))

  (c/datoms db :aevt :friends nil [:name "Ivan"] [:name "Petr"])

  (c/datoms db :avet :friends [:name "Oleg"] nil [:name "Ivan"])

  #_(apply c/datoms db :aevt [:friends [:name "Ivan"]])


  (let [db (c/db-with (c/empty-db {:name  { :db/unique :db.unique/identity }
                                   :email { :db/unique :db.unique/identity }})
                      [{:db/id 1 :name "Ivan" :email "@1"}
                       {:db/id 2 :name "Petr" :email "@2"}])
        touched (fn [tx e] (into {} (c/touch (c/entity (:db-after tx) e))))
        tempids (fn [tx] (dissoc (:tempids tx) :db/current-tx))
        tx (c/with db [{:db/id "1" :name "Ivan" :age 35}
                       [:db/add "2" :name "Oleg"]
                       [:db/add "2" :email "@2"]])]
    tx)

  )
