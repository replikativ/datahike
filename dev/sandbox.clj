(ns sandbox
  (:require [datahike.api :as d]
            [datahike.schema :as s]
            [datahike.core :as c]))

(comment

  (def uri "datahike:mem://dev")
 ;;(def path "datahike:file:///tmp/local-db-0")
  (d/delete-database uri)

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


  (d/create-database {:uri uri :initial-tx schema})

  (def conn (d/connect uri))


  (d/transact! conn [{:name  "Alice", :age   27}
                     {:name  "Bob", :age   37}
                     {:name  "Charlie", :age   47}
                     {:name "Daisy", :age 24 :sibling [[:name "Alice"] [:name "Charlie"]]}])

  (d/q '[:find ?a ?v ?tx :in $ ?e :where [?e ?a ?v ?tx]] (d/db conn) 4)

  (d/pull (d/db conn) '[*] [:name "Alice"])

  (d/transact! conn [{:db/id [:name "Alice"] :age 47}])
  (d/transact! conn [{:db/id [:name "Alice"] :age 32}])
  (d/transact! conn [{:db/id [:name "Alice"] :age 27}])

  (d/transact! conn [{:db/id [:name "Charlie"] :name "Enoch"}])

  (d/pull (d/db conn) '[*] [:name "Enoch"])

  (d/q '[:find ?v :where [4 :age ?v]] (d/db conn))

  (d/q '[:find ?e ?a ?tx :where [?e ?a 27 ?tx]] (d/db conn))

  (d/q '[:find ?e ?a ?v :where [?e ?a ?v 1563193744848]] (d/db conn))

  (d/q '[:find ?e ?a ?v ?tx :where [?e ?a ?v ?tx] [(<= ?tx 1563193744848)]] (d/db conn))

  )
