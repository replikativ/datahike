(ns user
  (:require [datahike.api :as d]
            [datahike.db :as dd]
            [datahike.schema :as ds]))

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

  (def cfg {:store {:backend :mem :id "sandbox"}
            :keep-history? true
            :schema-flexibility :write
            :attribute-refs? false})

  (def conn (do
              (d/delete-database cfg)
              (d/create-database cfg)
              (d/connect cfg)))

  (d/transact conn schema)

  (d/datoms @conn :avet)
  (d/datoms @conn :aevt)
  (d/datoms @conn :eavt)

  (:max-eid @conn)

  (d/schema @conn)

  (d/reverse-schema @conn)

  (:store @conn)

  (d/transact conn [{:name "Alice"
                     :age  25}])

  (d/transact conn [{:name    "Charlie"
                     :age     45
                     :sibling [{:name "Alice"} {:name "Bob"}]}])

  (d/q '[:find ?e ?a ?v ?t
         :in $ ?a
         :where
         [?e :name ?v ?t]
         [?e :age ?a]]
       @conn
<<<<<<< HEAD:dev/sandbox.clj
       25)

  (d/q '[:find ?e ?at ?v
         :where
         [?e ?a ?v]
         [?a :db/ident ?at]]
       @conn)

  (d/q '[:find ?e :where [?e :name "Alice"]] @conn)

  (:schema @conn)


  )
=======
       35)

  )
>>>>>>> move sandbox to user namespace:env/dev/user.clj
