(ns datahike.notebooks.minimal-start
  (:require [datahike.api :as d]))

(def cfg {:store {:backend :file
                  :path "/tmp/example"}})

;; create a database at this place, per default configuration we enforce a strict
;; schema and keep all historical data

(when (d/database-exists? cfg)
  (d/delete-database cfg))

(d/create-database cfg)

(def conn (d/connect cfg))

(:config @conn)

;; the first transaction will be the schema we are using
;; you may also add this within database creation by adding :initial-tx
;; to the configuration
(d/transact conn [{:db/ident :name
                   :db/valueType :db.type/string
                   :db/cardinality :db.cardinality/one}
                  {:db/ident :age
                   :db/valueType :db.type/long
                   :db/cardinality :db.cardinality/one}])

(d/schema @conn)

;; lets add some data and wait for the transaction
(d/transact conn [{:name  "Alice", :age   20 }
                  {:name  "Bob", :age   30 }
                  {:name  "Charlie", :age   40 }
                  {:age 15}
                  {:name "Daisy"}])

;; [entity-id attribute value transaction-id added?]
;; [Entity_ID Attribute Value]
;; [3 :name "Alice"]
;; [3 :age 20]

;; search the data
(d/q '[:find ?entity-id ?name ?age
       :where
       [?entity-id :name ?name]
       [?entity-id :age ?age]]
  @conn)

;; add new entity data using a hash map
(d/transact conn {:tx-data [{:db/id 3 :age 25}]})

;; if you want to work with queries like in
;; https://grishaev.me/en/datomic-query/,
;; you may use a hashmap
(d/q {:query '{:find [?e ?n ?a ]
               :where [[?e :name ?n]
                       [?e :age ?a]]}
      :args [@conn]})

(d/q '[:find ?age
       :where
       [?entity-id :name "Alice"]
       [?entity-id :age ?age]]
     @conn)

;; query the history of the data
(d/q '[:find ?age
       :where
       [?entity-id :name "Alice"]
       [?entity-id :age ?age]]
  (d/history @conn))

;; pull API
(d/pull @conn '[*] 3)

(d/pull @conn '[:name :db/id] 3)

(d/pull @conn '[*] 7)

;; you might need to release the connection for specific stores like leveldb
(d/release conn)

;; clean up the database if it is not need any more
(d/delete-database cfg)
