;; # Datahike Crash Course
(ns datahike.notebooks.crash-course
  (:require [datahike.api :as d]))

(def cfg {:store {:backend :file
                  :path "/tmp/example"}})

;; ## Create a database
;; Per default configuration we enforce a strict
;; schema and keep all historical data.
(when (d/database-exists? cfg)
  (d/delete-database cfg))

(d/create-database cfg)

;; ## Connect to database
(def conn (d/connect cfg))

(:config @conn)

;; ## Transact data
;; The first transaction will be the schema we are using.
;; You may also add this within database creation by adding :initial-tx
;; to the configuration.
(d/transact conn [{:db/ident :name
                   :db/valueType :db.type/string
                   :db/cardinality :db.cardinality/one}
                  {:db/ident :age
                   :db/valueType :db.type/long
                   :db/cardinality :db.cardinality/one}])

;; You can take a look at your current schema.
(d/schema @conn)

;; Lets add some data.
(def tx-report (d/transact conn [{:name  "Alice", :age   20}
                                 {:name  "Bob", :age   30}
                                 {:name  "Charlie", :age   40}
                                 {:age 15}
                                 {:name "Daisy"}]))

;; The transact-fn returns a transaction-report for you to inspect.
;; It's a map and the tx-data-key returns a vector of datoms in the form
;; `[entity-id attribute value transaction-id]`.
(:tx-data tx-report)

;; ## Query the database.
(d/q '[:find ?entity-id ?name ?age
       :where
       [?entity-id :name ?name]
       [?entity-id :age ?age]]
     @conn)

;; Update the entity with the entity-id 3 using a hash-map as argument.
;; Alice is now 25 years old instead of 20
(d/transact conn {:tx-data [{:db/id 3 :age 25}]})

;; If you want to work with queries like in
;; https://grishaev.me/en/datomic-query/,
;; you can pass a hashmap to the query-fn like this.
(d/q {:query '{:find [?e ?n ?a]
               :where [[?e :name ?n]
                       [?e :age ?a]]}
      :args [@conn]})

;; Or you use the vector-format.
(d/q '[:find ?age
       :where
       [?entity-id :name "Alice"]
       [?entity-id :age ?age]]
     @conn)

;; ## Query the history of the data
;; Just pass the db derived from the history-fn to the query.
;; You then receive the actual and the old value of Alice's age.
(d/q '[:find ?age
       :where
       [?entity-id :name "Alice"]
       [?entity-id :age ?age]]
     (d/history @conn))

;; ## Pull-API
;; For querying Datahike you can also use the pull-API.
;; With the pull-API you don't need to specify what data to query for,
;; you just pull all attributes of an entity with a wildcard.
(d/pull @conn '[*] 3)

;; Or you limit your pull of an entity's data specifying the attributes.
(d/pull @conn '[:name :db/id] 3)

(d/pull @conn '[*] 7)

;; ## Cleaning up
;; You might need to release the connection for specific stores like leveldb.
(d/release conn)

;; Delete the database if it is not needed any more.
(d/delete-database cfg)
