(ns example.core
  (:require [datahike.api :as d]))

(def schema [{:db/ident :name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}])

(def query '[:find ?n :where [?e :name ?n]])

;; let's cleanup, create, and connect all in one
(defn cleanup-and-create-conn [uri]
  (d/delete-database uri)
  (d/create-database uri :initial-tx schema)
  (d/connect uri))

(defn transact-and-find [conn name]
  (d/transact! conn [{:name name}])
  (d/q query (d/db conn)))

;; first let's have a look at the memory store which uses an atom internally to store data
;; only a simple identifier is needed, we use
(def mem-uri "datahike:mem://example")

;; create it
(def mem-conn (cleanup-and-create-conn mem-uri))

;; add and find data
(transact-and-find mem-conn "Alice")

;; next we try out file based store which can be used as the simplest form of persistence
;; the datoms are serialized at `/tmp/file_example`
(def file-uri "datahike:file:///tmp/file_example")

(def file-conn (cleanup-and-create-conn file-uri))

(transact-and-find file-conn "Bob")


;; another simple alternative is using leveldb at `/tmp/level_example`
(def level-uri "datahike:level:///tmp/level_example")

(def level-conn (cleanup-and-create-conn level-uri))

(transact-and-find level-conn "Charlie")


;; for a more conservative and remote store you can connect to a postgresql instance
;; you can create a simple instance using docker and docker-compose with `docker-compose.yml` in this project
;; See README for infos on starting
;; we connect to a postgresql instance with username datahike, password clojure, at the localhost with port 5434 and a datahike database
(def pg-uri "datahike:pg://datahike:clojure@localhost:5434/datahike")

(def pg-conn (cleanup-and-create-conn pg-uri))

(transact-and-find pg-conn "Daisy")

;; of course we can combine the data from all databases using queries with multiple inputs
(d/q '[:find ?mem ?file ?level ?pg
       :in $mem-db $file-db $level-db $pg-db
       :where
       [$mem-db ?e0 :name ?mem]
       [$file-db ?e1 :name ?file]
       [$level-db ?e2 :name ?level]
       [$pg-db ?e3 :name ?pg]]
     (d/db mem-conn)
     (d/db file-conn)
     (d/db level-conn)
     (d/db pg-conn))
