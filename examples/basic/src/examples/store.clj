(ns examples.store
  (:require [datahike.api :as d]
            [datahike-jdbc.core]))

(def schema [{:db/ident :name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}])

(def query '[:find ?n :where [?e :name ?n]])

;; let's cleanup, create, and connect all in one
(defn cleanup-and-create-conn [cfg]
  (d/delete-database cfg)
  (d/create-database cfg)
  (let [conn (d/connect cfg)]
    (d/transact conn schema)
    conn))

(defn transact-and-find [conn name]
  (d/transact conn [{:name name}])
  (d/q query @conn))

;; first let's have a look at the memory store which uses an atom internally to store data
;; only a simple identifier is needed, we use
(def mem-cfg {:store {:backend :mem :id "mem-example"}})

;; create it
(def mem-conn (cleanup-and-create-conn mem-cfg))

;; add and find data
(transact-and-find mem-conn "Alice");; => #{["Alice"]}

;; next we try out file based store which can be used as the simplest form of persistence
;; the datoms are serialized at `/tmp/file_example`
(def file-cfg {:store {:backend :file :path "/tmp/file_example"}})

(def file-conn (cleanup-and-create-conn file-cfg))

(transact-and-find file-conn "Bob");; => #{["Bob"]}

;; External backends
;; make sure you add `datahike-jdbc`  as dependency
;; for a more robust and remote store you can connect to a postgresql instance
;; you can create a simple instance using docker and docker-compose with `docker-compose.yml` in this project
;; See README for infos on starting
;; we connect to a postgresql instance with username datahike, password clojure, at the localhost with port 5437 and a datahike database
(def pg-cfg {:store {:backend :jdbc
                     :dbtype "postgresql"
                     :dbname "pg-example"
                     :user "datahike"
                     :password "clojure"
                     :host "localhost"
                     :port 5437}})

(def pg-conn (cleanup-and-create-conn pg-cfg))

(transact-and-find pg-conn "Charlie")

;; of course we can combine the data from all databases using queries with multiple inputs
(d/q '[:find ?mem ?file ?pg
       :in $mem-db $file-db $pg-db
       :where
       [$mem-db ?e0 :name ?mem]
       [$file-db ?e1 :name ?file]
       [$pg-db ?e3 :name ?pg]]
     (d/db mem-conn)
     (d/db file-conn)
     (d/db pg-conn));; => #{["Alice" "Bob" "Charlie"]}

;; cleanup
(do
  (d/delete-database mem-cfg)
  (d/delete-database file-cfg)
  (d/delete-database pg-cfg))

