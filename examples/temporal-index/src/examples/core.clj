(ns examples.core
  (:require [datahike.api :as d]))

;; define base uri we can connect to
(def uri "datahike:mem://temporal-index")

;; define schema
(def schema-tx [{:db/ident :name
                 :db/valueType :db.type/string
                 :db/unique :db.unique/identity
                 :db/index true
                 :db/cardinality :db.cardinality/one}
                {:db/ident :age
                 :db/valueType :db.type/long
                 :db/cardinality :db.cardinality/one}])

;; cleanup any previous data
(d/delete-database uri)

;; create the database with default configuration and above schema
(d/create-database uri :initial-tx schema-tx)

;; connect to the database
(def conn (d/connect uri))

;; transact age and name data
(d/transact! conn [{:name "Alice" :age 25} {:name "Bob" :age 30}])

;; let's find name and age of all data
(def query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]])

;; search current data without any new data
(d/q query (d/db conn))

(def first-date (java.util.Date.))

;; let's change something
(d/transact! conn [{:db/id [:name "Alice"] :age 30}])

;; search for current data of Alice
(d/q query (d/db conn))

;; now we search within historical data
(d/q query (d/history conn))

;; let's find the dates for each attribute additions
;; :db/txInstant is an attribute of the meta entity added to each transaction
;; and can be treated just as any other data
(d/q '[:find ?a ?v ?t :where [?e ?a ?v ?tx] [?tx :db/txInstant ?t]] (d/history conn))

;; next let's get the current data of a specific time
(d/q query (d/as-of conn first-date))

;; pull is also supported
(d/pull (d/as-of conn first-date) '[*] [:name "Alice"])

;; now we want to now any additions after a specific time
(d/q query (d/since conn first-date))
;; => {}, because :name was transacted before the first date

;; let's build a query where we use the latest db to find the name and the since db to find out who's age changed
(d/q '[:find ?n ?a
       :in $ $since
       :where
       [$ ?e :name ?n]
       [$since ?e :age ?a]]
     (d/db conn)
     (d/since conn first-date))

;; let's remove Bob
(d/transact! conn [[:db/retractEntity [:name "Bob"]]])

;; Only Alice remains
(d/q query (d/db conn))

;; Let's have a look at the history, Bob should be there
(d/q query (d/history conn))

;; let's find the retracted entity and the date of the retraction
(d/q '[:find ?n ?a ?tx
       :where
       [?r :db/retracted ?e]
       [?e :name ?n]
       [?e :age ?a]
       [?r :db/txInstant ?tx]]
     (d/history conn))
