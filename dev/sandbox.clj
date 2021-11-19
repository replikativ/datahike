(ns sandbox
  (:require [datahike.api :as d]))

(comment

  (def schema [{:db/ident       :name
                :db/cardinality :db.cardinality/one
                :db/index       true
                :db/unique      :db.unique/identity
                :db/valueType   :db.type/string}
               {:db/ident       :parents
                :db/cardinality :db.cardinality/many
                :db/valueType   :db.type/ref}
               {:db/ident       :age
                :db/cardinality :db.cardinality/one
                :db/valueType   :db.type/long}])

  (def cfg {:store {:backend :mem :id "sandbox"}
            :connection {:sync? true}
            :keep-history? true
            :schema-flexibility :write
            :attribute-refs? false})

  (do
    (d/delete-database cfg)
    (d/create-database cfg))

  (def conn (d/connect cfg))

  (d/transact conn schema)

  (d/transact conn [{:name "Alice"
                     :age  40}
                    {:name "Bob"
                     :age 38}])

  (d/transact conn [{:name    "Charlie"
                     :age     10
                     :parents [[:name "Alice"] [:name "Bob"]]}])

  (d/q '[:find ?e ?a ?v ?t
         :in $ ?a
         :where
         [?e :name ?v ?t]
         [?e :age ?a]]
       @conn
       40)

  (d/sync! conn)

  (d/q '[:find ?e :where [?e :name "Alice"]] @conn)

  (d/stop-sync conn)

  (d/transact conn [{:name "Daisy"
                     :age 40}])

  (d/schema @conn)

  (meta conn)

  )

