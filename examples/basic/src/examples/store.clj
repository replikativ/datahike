(ns examples.store
  (:require [datahike.api :as d]
            [datahike-postgres.core]
            [datahike-leveldb.core]))

(def schema [{:db/ident :name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}])

(def query '[:find ?n :where [?e :name ?n]])

;; let's cleanup, create, and connect all in one
(defn cleanup-and-create-conn [cfg]
  (d/delete-database cfg)
  (d/create-database (assoc cfg :inital-tx cfg))
  (d/connect cfg))

(defn transact-and-find [conn name]
  (d/transact conn [{:name name}])
  (d/q query @conn))

;; first let's have a look at the memory store which uses an atom internally to store data
;; only a simple identifier is needed, we use
(def mem-cfg {:store {:backend :mem :id "mem-example"}})

;; create it
(def mem-conn (cleanup-and-create-conn mem-cfg))

;; add and find data
(transact-and-find mem-conn "Alice")

;; next we try out file based store which can be used as the simplest form of persistence
;; the datoms are serialized at `/tmp/file_example`
(def file-cfg {:store {:backend :file :path "/tmp/file_example"}})

(def file-conn (cleanup-and-create-conn file-cfg))

(transact-and-find file-conn "Bob")

;; External backends
;; make sure you add `datahike-postgres` and `datahike-leveldb` as dependency

;; another simple alternative is using leveldb at `/tmp/level_example`
(def level-cfg  {:store {:backend :level :path "/tmp/level_example"}})

(def level-conn (cleanup-and-create-conn level-cfg))

(transact-and-find level-conn "Charlie")

;; for a more conservative and remote store you can connect to a postgresql instance
;; you can create a simple instance using docker and docker-compose with `docker-compose.yml` in this project
;; See README for infos on starting
;; we connect to a postgresql instance with username datahike, password clojure, at the localhost with port 5434 and a datahike database
(def pg-cfg {:store {:backend :pg
                     :username "datahike"
                     :password "clojure"
                     :host "localhost"
                     :port 5434
                     :path "/datahike"}})

(def pg-conn (cleanup-and-create-conn pg-cfg))

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

;; clean up
;; LevelDB needs to be released
(d/release level-conn)

(do
  (d/delete-database mem-cfg)
  (d/delete-database file-cfg)
  (d/delete-database level-cfg)
  (d/delete-database pg-cfg))
