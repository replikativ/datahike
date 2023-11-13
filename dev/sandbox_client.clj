(ns sandbox-client
  (:require [datahike.http.client :refer :all]
            [datahike.http.server :refer [start-server stop-server]]))

(comment
  (def config {:port     3333
               :join?    false
               :dev-mode false
               :token    "securerandompassword"})

  (def server (start-server config))

  (stop-server server)

  (def new-config (create-database {:schema-flexibility :read
                                    :remote-peer {:backend :datahike-server
                                                  :url     "http://localhost:3333"
                                                  :token   "securerandompassword"
                                                  :format   :transit}}))

  (def conn (connect new-config))

  (:remote-peer conn)

  (def test-db @conn)

  (:remote-peer test-db)

  (db conn)

  (history test-db)

  (:remote-peer (history test-db))

  (type test-db)

  (transact conn [{:name "Peter" :age 42}])

  (def test-db (db conn))

  (q '[:find ?n ?a
       :in $1 $2 $3
       :where
       [$1 ?e :age ?a]
       [$1 ?e :name ?n]]
     test-db (history test-db) (entity test-db 1))

  (query-stats '[:find ?n ?a :in $ $2 :where [?e :age ?a] [?e :name ?n]] test-db test-db)

  (pull test-db '[:*] 1)

  (pull-many test-db '[:*] [1 2])

  (datoms test-db :eavt)

  (seek-datoms test-db :eavt)

  (metrics test-db)

  (schema test-db)

  (reverse-schema test-db)

  ;; no export because of function passing limitation
  (listen conn (fn [x] x))

  (filter test-db "even?") ;; returns client-db, not filtered DB

  (is-filtered (filter test-db "even?")) ;; false

  (entity test-db 1)

  (entity-db (entity test-db 1))

  (since test-db (java.util.Date.))

  (as-of test-db (java.util.Date.))

  (with test-db [{:name "Petra" :age 43}]) ;; TODO works but returns same DB as input

  (db-with test-db [{:name "Petra" :age 43}])

  (tempid test-db)

  (release conn)

  (delete-database new-config))
