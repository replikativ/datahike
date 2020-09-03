(ns sandbox
  (:require [datahike.api :as d]))

(comment

  (def schema [{:db/ident       :name
                :db/cardinality :db.cardinality/one
                :db/index       true
                :db/unique      :db.unique/identity
                :db/valueType   :db.type/string}
               {:db/ident       :sibling
                :db/cardinality :db.cardinality/many
                :db/valueType   :db.type/ref}
               {:db/ident       :age
                :db/cardinality :db.cardinality/one
                :db/valueType   :db.type/long}])

  (def cfg {:store  {:backend :mem :id "sandbox"}
            :keep-history? true
            :schema-flexibility :write
            :initial-tx schema})

  (d/delete-database cfg)

  (d/create-database cfg)

  (def conn (d/connect cfg))

  (d/transact conn [{:name "Alice"
                     :age  25}
                    {:name "Bob"
                     :age  35}
                    {:name    "Charlie"
                     :age     45
                     :sibling [[:name "Alice"] [:name "Bob"]]}])

  (d/q '[:find ?e ?a ?v ?t
         :in $ ?a
         :where [?e :name ?v ?t] [?e :age ?a]]
       @conn
       35))
