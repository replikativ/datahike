(ns datahike.test.migrate
  (:require [clojure.test :refer :all]
            [datahike.migrate :refer :all]
            [datahike.api :refer :all]))

(def tx-data [[:db/add 1 :db/cardinality :db.cardinality/one 536870913 true]
              [:db/add 1 :db/ident :name 536870913 true]
              [:db/add 1 :db/index true 536870913 true]
              [:db/add 1 :db/unique :db.unique/identity 536870913 true]
              [:db/add 1 :db/valueType :db.type/string 536870913 true]
              [:db/add 2 :db/cardinality :db.cardinality/one 536870913 true]
              [:db/add 2 :db/ident :age 536870913 true]
              [:db/add 2 :db/valueType :db.type/long 536870913 true]
              [:db/add 3 :age 25 536870913 true]
              [:db/add 3 :name "Alice" 536870913 true]
              [:db/add 4 :age 35 536870913 true]
              [:db/add 4 :name "Bob" 536870913 true]])

(deftest export-import-test
  (testing "Test a roundtrip for exporting and importing."
    (let [uri "datahike:file:///tmp/export-db1"
          _ (delete-database uri)
          _ (create-database uri)
          conn (connect uri)]
      (transact conn tx-data)

      (export-db @conn "/tmp/eavt-dump")

      (is (= (slurp "/tmp/eavt-dump")
             "#datahike/Datom [1 :db/cardinality :db.cardinality/one 536870913 true]\n#datahike/Datom [1 :db/ident :name 536870913 true]\n#datahike/Datom [1 :db/index true 536870913 true]\n#datahike/Datom [1 :db/unique :db.unique/identity 536870913 true]\n#datahike/Datom [1 :db/valueType :db.type/string 536870913 true]\n#datahike/Datom [2 :db/cardinality :db.cardinality/one 536870913 true]\n#datahike/Datom [2 :db/ident :age 536870913 true]\n#datahike/Datom [2 :db/valueType :db.type/long 536870913 true]\n#datahike/Datom [3 :age 25 536870913 true]\n#datahike/Datom [3 :name \"Alice\" 536870913 true]\n#datahike/Datom [4 :age 35 536870913 true]\n#datahike/Datom [4 :name \"Bob\" 536870913 true]\n"))

      (let [import-uri "datahike:mem:///reimport"
            _ (create-database import-uri)
            new-conn (connect import-uri)]

        (import-db new-conn "/tmp/eavt-dump")

        (is (= (q '[:find ?e
                    :where [?e :name]]
                  @new-conn)
               #{[3] [4]}))
        (delete-database uri)))))


(deftest load-entities-test
  (testing "Test migrate simple datoms"
    (let [source-datoms (->> tx-data
                             (mapv #(-> % rest vec))
                             (concat [[536870913 :db/txInstant #inst "2020-03-11T14:54:27.979-00:00" 536870913 true]]))]
      (let [cfg {:backend :mem
                 :path "/target"}
            _ (delete-database cfg)
            _ (create-database cfg)
            conn (connect cfg)]
        @(load-entities conn source-datoms)
        (is (= (into #{} source-datoms)
               (q '[:find ?e ?a ?v ?t ?op :where [?e ?a ?v ?t ?op]] @conn)))))))
