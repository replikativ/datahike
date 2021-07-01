(ns user
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

  (def cfg {:store {:backend :mem
                    :id "sandbox"}
            :name "sandbox"
            :keep-history? true
            :schema-flexibility :write
            :attribute-refs? true})

  (d/delete-database cfg)
  (d/create-database cfg)

  (def conn (d/connect cfg))

  (d/transact conn schema)

  (d/transact conn [{:name "Alice"
                     :age  25}
                    {:name "Bob"
                     :age 30}])

  (d/transact conn [{:name    "Charlie"
                     :age     5
                     :parents [{:name "Alice"}
                               {:name "Bob"}]}])

  (d/q '[:find ?e ?a ?v ?t
         :in $ ?a
         :where
         [?e :name ?v ?t]
         [?e :age ?a]]
       @conn
       25)

  (d/q '[:find ?e ?at ?v
         :where
         [?e ?a ?v]
         [?a :db/ident ?at]]
       @conn)

  (d/q '[:find ?e :where [?e :name "Alice"]] @conn)

  (:schema @conn))
