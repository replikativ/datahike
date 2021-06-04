(ns sandbox-benchmarks
  (:require [benchmark.core :as b]
            [benchmark.store :as s]))

;(b/-main)                                                   ;; TIMBRE_LEVEL=':fatal' clj -M:benchmark

;(b/-main "-t" "test-id")                                    ;; TIMBRE_LEVEL=':fatal' clj -M:benchmark  -t test-id

;(b/-main "-t" "test-id" "-t" "test-id2")                    ;; TIMBRE_LEVEL=':fatal' clj -M:benchmark -t test-id -t test-id2

;(b/-main "-t" "test-id" "-u" "http://localhost:3001" "-n" "benchmarks" "-g" "test-token") ;; TIMBRE_LEVEL=':fatal' clj -M:benchmark run -o remote-db -t test-id -u http://localhost:3001 -n benchmarks -g test-token


;; docker run -d --name datahike-server -p 3001:3000 -e DATAHIKE_SERVER_TOKEN=test-token -e DATAHIKE_SCHEMA_FLEXIBILITY=write -e DATAHIKE_STORE_BACKEND=file -e DATAHIKE_NAME=benchmarks -e DATAHIKE_STORE_PATH=/opt/datahike-server/benchmarks replikativ/datahike-server:snapshot

;(def db (s/->RemoteDB "http://localhost:3001" "test-token" "benchmarks"))

;(s/list-databases db)

;(s/transact-missing-schema db)
;(s/get-datoms db)
;(s/request-data db '[:find ?e ?a ?v :where [?e ?a ?v]])
;(s/request-data db '[:find ?e ?a ?v :where [1 ?a ?v]])      ;; -> error status 500
;(s/request-data db '[:find ?e ?a ?v :where [?e :db ?v]])
;(s/request-data db '[:find ?e ?v ?t :where [?e :dh-backend ?v ?t]])
;(s/get-schema db)

