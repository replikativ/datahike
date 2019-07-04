(ns sandbox
  (:require [datahike.api :as d]
            [datahike.core :as c]))

(comment

  (def path "datahike:mem://dev")
 ;;(def path "datahike:file:///tmp/local-db-0")
  (d/delete-database path)

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

  (d/create-database path schema)

  ;; (d/create-database path)
  ;;

  (def conn (d/connect path))

  (def db (d/db conn))

  (d/transact! conn [{:name  "Alice", :age   15}
                     {:name  "Bob", :age   37}
                     {:name  "Charlie", :age   37}
                     {:name  "Daisy", :age   22 :sibling [:name "Alice"]}])

  (def query '[:find ?e ?a ?tx :where [?e :name "Daisy" ?tx] [?e :age ?a]])

  (d/transact! conn [{:name "Enoch" :age 55 :sibling [:name "Alice"]}])
  (d/transact! conn [{:name "Freya" :age 25 :sibling [[:name "Bob" ] [:name "Charlie"]]}])

  (d/q '[:find ?n ?sn :where [?e :name ?n] [?e :sibling ?s] [?s :name ?sn]] (d/db conn))

  (d/q query db)

  (d/q query (d/db conn))

  (d/transact! conn [{:db/id [:db/ident :name]
                      :db/cardinality :db.cardinality/many
                      :db/ident :boofar}])

  (d/transact! conn [{:db/id [:db/ident :sibling]
                      :db/unique :db.unique/identity}])

  (def db (c/empty-db))

  (def path "datahike:mem://dev")
  (d/create-database path)
  (def conn (d/connect path))


  (d/transact! conn [{:db/id #db/id[db.part/db]
                      :db/valueType :db.type/string
                      :db/cardinality :db.cardinality/many
                      :db/ident :name}
                     {:db/id #db/id[db.part/db]
                      :db/valueType :db.type/long
                      :db/cardinality :db.cardinality/one
                      :db/ident :age}])

  (d/q '[:find ?e ?ident :where [?e :db/valueType ?ident]] (d/db conn))

  (d/transact! conn [{:db/id #db/id[db.part/user] :name "Alice" :age 30}])

  (d/transact! conn [{:db/id #db/id[db.part/user] :name "Alice" :age "23"}])

  (d/q '[:find ?e :where [?e :name "Alice"]] (d/db conn))

  (empty? {:a 1})

  )
