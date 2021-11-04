(ns examples.core
  (:require [datahike.api :as d]))

(def cfg {:store {:backend :file
                  :path "/tmp/example"}})

;; create a database at this place, per default configuration we enforce a strict
;; schema and keep all historical data
(d/create-database cfg)

(def conn (d/connect cfg))

;; the first transaction will be the schema we are using
;; you may also add this within database creation by adding :initial-tx
;; to the configuration
(d/transact conn [{:db/ident :name
                   :db/valueType :db.type/string
                   :db/cardinality :db.cardinality/one }
                  {:db/ident :age
                   :db/valueType :db.type/long
                   :db/cardinality :db.cardinality/one }])

;; lets add some data and wait for the transaction
(d/transact conn [{:name  "Alice", :age   20 }
                  {:name  "Bob", :age   30 }
                  {:name  "Charlie", :age   40 }
                  {:age 15 }])

;; search the data
(d/q '[:find ?e ?n ?a
       :where
       [?e :name ?n]
       [?e :age ?a]]
  @conn)
;; => #{[3 "Alice" 20] [4 "Bob" 30] [5 "Charlie" 40]}

;; add new entity data using a hash map
(d/transact conn {:tx-data [{:db/id 3 :age 25}]})

;; if you want to work with queries like in
;; https://grishaev.me/en/datomic-query/,
;; you may use a hashmap
(d/q {:query '{:find [?e ?n ?a ]
               :where [[?e :name ?n]
                       [?e :age ?a]]}
      :args [@conn]})
;; => #{[5 "Charlie" 40] [4 "Bob" 30] [3 "Alice" 25]}

;; query the history of the data
(d/q '[:find ?a
       :where
       [?e :name "Alice"]
       [?e :age ?a]]
  (d/history @conn))
;; => #{[20] [25]}

;; you might need to release the connection for specific stores like leveldb
(d/release conn)

;; clean up the database if it is not need any more
(d/delete-database cfg)
