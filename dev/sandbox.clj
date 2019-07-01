(ns sandbox
  (:require [datahike.api :as d]))

(comment

  (def path "datahike:mem://dev")
 ;;(def path "datahike:file:///tmp/local-db-0")
  (d/delete-database path)

  (def schema [{:db/id #db/id[db.part/db]
                :db/ident :name
                :db/valueType :db.type/string}
               {:db/id #db/id[db.part/db]
                :db/ident :sibling
                :db/cardinality :db.cardinality/many
                :db/valueType :db.type/ref}
               {:db/id #db/id[db.part/db]
                :db/ident :age
                :db/valueType :db.type/long}])

  (d/create-database path schema)

  ;; (d/create-database path)

  (def conn (d/connect path))

  (def db (d/db conn))

  (d/transact! conn [{:db/id 1, :name  "Alice", :age   15}
                     {:db/id 2, :name  "Bob", :age   37}
                     {:db/id 3, :name  "Charlie", :age   37}
                     {:db/id 4, :age 15}
                     {:db/id 5, :name  "Daisy", :age   22 :sibling [1]}])

  (def query '[:find ?e ?a ?tx :where [?e :name "Daisy" ?tx] [?e :age ?a]])

  (d/transact! conn [{:db/id #db/id [db.part/user]
                      :email "foo"}])

  (d/q query db)

  (d/q query (d/db conn))

  (def db (c/empty-db))

  (def path "datahike:mem://dev")
  (d/create-database path)
  (def conn (d/connect path))

  (:schema (d/db conn))

  (d/transact! conn [{:db/id #db/id[db.part/db]
                      :db/valueType :db.type/string
                      :db/cardinality :db.cardinality/many
                      :db/ident :name}
                     {:db/id #db/id[db.part/db]
                      :db/valueType :db.type/long
                      :db/ident :age}])

  (d/q '[:find ?e ?ident :where [?e :db/valueType ?ident]] (d/db conn))

  (d/transact! conn [{:db/id #db/id[db.part/user] :name "Alice" :age 30}])

  (d/transact! conn [{:db/id #db/id[db.part/user] :name "Alice" :age "23"}])

  (d/q '[:find ?e :where [?e :name "Alice"]] (d/db conn))

  )
