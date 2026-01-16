(ns examples.store
  (:require [datahike.api :as d]))

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
;; memory backend requires a UUID identifier for distributed tracking
(def mem-cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}})

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
;; Datahike supports additional backends via plugins:
;; - PostgreSQL (via datahike-jdbc)
;; - S3 (via datahike-s3)
;; - Redis, LevelDB, etc.
;; See https://github.com/replikativ/datahike for available backends

;; We can query across multiple databases
(d/q '[:find ?mem ?file
       :in $mem-db $file-db
       :where
       [$mem-db ?e0 :name ?mem]
       [$file-db ?e1 :name ?file]]
     (d/db mem-conn)
     (d/db file-conn)) ;; => #{["Alice" "Bob"]}

;; cleanup
(do
  (d/delete-database mem-cfg)
  (d/delete-database file-cfg))

