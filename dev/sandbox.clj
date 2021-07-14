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

  (def cfg {:store {:backend :mem :id "sandbox"}
            :keep-history? true
            :schema-flexibility :write
            :attribute-refs? false})

  (def cfg {:store {:backend :mem :id "sandbox"}
            :keep-history? true
            :schema-flexibility :write
            :attribute-refs? true})

  (def conn (do
              (d/delete-database cfg)
              (d/create-database cfg)
              (d/connect cfg)))

  (d/transact conn schema)

  (d/datoms @conn :avet)
  (d/datoms @conn :aevt)
  (d/datoms @conn :eavt)

  (:max-eid @conn)

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
       25)

  (d/q '[:find ?e ?at ?v
         :where
         [?e ?a ?v]
         [?a :db/ident ?at]]
       @conn)

  (d/q '[:find ?e :where [?e :name "Alice"]] @conn)

  (:schema @conn)

  (d/transact conn (vec (repeatedly 5000 (fn [] {:age (long (rand-int 1000))
                                                 :name (str (rand-int 1000))}))))

  (time
   (d/q {:query '[:find ?e ?v
                  :in $
                  :where [?e :name ?v]]
         :args [@conn]
         :offset 0
         :limit 10}))

  (time
   (do (d/q {:query '[:find ?v1 ?v2
                      :in $
                      :where [?e1 :name ?v1] [?e2 :name ?v2]]
             :args [@conn]})
       nil)))
