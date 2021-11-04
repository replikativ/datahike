(ns examples.schema
  (:require [datahike.api :as d]))

;; The first example assumes you know your data model in advanve,
;; so you we can use a schema-on-write approach in contrast to a schema-on-read
;; approach. Have a look at the documentation in `/doc/schema.md` for more
;; information on the different types of schema flexibility. After the first
;; example we will have a short schema-on-read example.

; first define data model
(def schema [{:db/ident :contributor/name
              :db/valueType :db.type/string
              :db/unique :db.unique/identity
              :db/index true
              :db/cardinality :db.cardinality/one
              :db/doc "a contributor's name"}
             {:db/ident :contributor/email
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/many
              :db/doc "a contributor's email"}
             {:db/ident :repository/name
              :db/valueType :db.type/string
              :db/unique :db.unique/identity
              :db/index true
              :db/cardinality :db.cardinality/one
              :db/doc "a repository's name"}
             {:db/ident :repository/contributors
              :db/valueType :db.type/ref
              :db/cardinality :db.cardinality/many
              :db/doc "the repository's contributors"}
             {:db/ident :repository/public
              :db/valueType :db.type/boolean
              :db/cardinality :db.cardinality/one
              :db/doc "toggle whether the repository is public"}
             {:db/ident :repository/tags
              :db/valueType :db.type/ref
              :db/cardinality :db.cardinality/many
              :db/doc "the repository's tags"}
             {:db/ident :language/clojure}
             {:db/ident :language/rust}])

;; define configuration
(def cfg {:store {:backend :mem
                  :id "schema-intro"}
          :schema-flexibility :write})

;; cleanup previous database
(d/delete-database cfg)

;; create the in-memory database
(d/create-database cfg)

;; connect to it
(def conn (d/connect cfg))

;; add the schema

(d/transact conn schema)

;; let's insert our first user
(d/transact conn [{:contributor/name "alice" :contributor/email "alice@exam.ple"}])

;; let's find her with a query
(def find-name-email '[:find ?e ?n ?em :where [?e :contributor/name ?n] [?e :contributor/email ?em]])

(d/q find-name-email @conn)

;; let's find her directly, as contributor/name is a unique, indexed identity
(d/pull @conn '[*] [:contributor/name "alice"])

;; add a second email, as we have a many cardinality, we can have several ones as a user
(d/transact conn [{:db/id [:contributor/name "alice"] :contributor/email "alice@test.test"}])

;; let's see both emails
(d/q find-name-email @conn)

;; try to add something completely not defined in the schema
(d/transact conn [{:something "different"}])
;; => Exception shows missing schema definition

;; try to add wrong contributor values
(d/transact conn [{:contributor/email :alice}])
;; => Exception shows what value is expected

;; add another contributor by using a the alternative transaction schema that expects a hash map with tx-data attribute
(d/transact conn {:tx-data [{:contributor/name "bob" :contributor/email "bob@ac.me"}]})

(d/q find-name-email @conn)

(d/pull @conn '[*] [:contributor/name "bob"])

;; change bob's name to bobby
(d/transact conn [{:db/id [:contributor/name "bob"] :contributor/name "bobby"}])

;; check it
(d/q find-name-email @conn)

(d/pull @conn '[*] [:contributor/name "bobby"])

;; bob is not related anymore as index
(d/pull @conn '[*] [:contributor/name "bob"])
;; will give an exception

;; create a repository, with refs from uniques, and an ident as enum
(d/transact conn [{:repository/name "top secret"
                   :repository/public false
                   :repository/contributors [[:contributor/name "bobby"] [:contributor/name "alice"]]
                   :repository/tags :language/clojure}])

;; let's search with pull inside the query
(def find-repositories '[:find (pull ?e [*]) :where [?e :repository/name ?n]])

;; looks good
(d/q find-repositories @conn)

;; let's go further and fetch the related contributor data as well
(def find-repositories-with-contributors '[:find (pull ?e [* {:repository/contributors [*] :repository/tags [*]}]) :where [?e :repository/name ?n]])

(d/q find-repositories-with-contributors @conn)

;; the schema is part of the index, so we can query them too.
;; Let's find all attribute names and their description.
(d/q '[:find ?a ?d :where [?e :db/ident ?a] [?e :db/doc ?d]] @conn)

;; cleanup the database
(d/delete-database cfg)

;; Schema On Read

;; let's create another database that can hold any arbitrary data

(def cfg {:store {:backend :mem
                  :id "schemaless"}
          :schema-flexibility :read})

(d/create-database cfg)

(def conn (d/connect cfg))

;; now we can go wild and transact anything
(d/transact conn [{:any "thing"}])

;; use simple query on this data
(d/q '[:find ?v :where [_ :any ?v]] @conn)

;; be aware: although there is no schema, you should tell the database if some
;; attributes can have specific cardinality or indices.
;; You may add that as schema transactions like before
(d/transact conn [{:db/ident :any :db/cardinality :db.cardinality/many}])

;; let's add more data to the first any entity
(def any-eid (d/q '[:find ?e . :where [?e :any "thing"]] @conn))
(d/transact conn [{:db/id any-eid :any "thing else"}])

(d/q '[:find ?v :where [_ :any ?v]] @conn)
