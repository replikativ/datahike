(ns datahike.test.migrate
  (:require [clojure.test :refer :all]
            [datahike.migrate :as m]
            [datahike.api :as d]))

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
    (let [os (System/getProperty "os.name")
          path (case os
                 "Windows 10" (str (System/getProperty "java.io.tmpdir") "export-db1")
                 "/tmp/export-db1")
          cfg {:store {:backend :file
                       :path path}}
          _ (d/delete-database cfg)
          _ (d/create-database cfg)
          conn (d/connect cfg)
          export-path (case os
                        "Windows 10" (str (System/getProperty "java.io.tmpdir") "eavt-dump")
                        "/tmp/eavt-dump")]
      (d/transact conn tx-data)

      (m/export-db @conn export-path)

      (is (= (slurp export-path)
             (case os
               "Windows 10"
               "#datahike/Datom [1 :db/cardinality :db.cardinality/one 536870913 true]\r\n#datahike/Datom [1 :db/ident :name 536870913 true]\r\n#datahike/Datom [1 :db/index true 536870913 true]\r\n#datahike/Datom [1 :db/unique :db.unique/identity 536870913 true]\r\n#datahike/Datom [1 :db/valueType :db.type/string 536870913 true]\r\n#datahike/Datom [2 :db/cardinality :db.cardinality/one 536870913 true]\r\n#datahike/Datom [2 :db/ident :age 536870913 true]\r\n#datahike/Datom [2 :db/valueType :db.type/long 536870913 true]\r\n#datahike/Datom [3 :age 25 536870913 true]\r\n#datahike/Datom [3 :name \"Alice\" 536870913 true]\r\n#datahike/Datom [4 :age 35 536870913 true]\r\n#datahike/Datom [4 :name \"Bob\" 536870913 true]\r\n"
               "#datahike/Datom [1 :db/cardinality :db.cardinality/one 536870913 true]\n#datahike/Datom [1 :db/ident :name 536870913 true]\n#datahike/Datom [1 :db/index true 536870913 true]\n#datahike/Datom [1 :db/unique :db.unique/identity 536870913 true]\n#datahike/Datom [1 :db/valueType :db.type/string 536870913 true]\n#datahike/Datom [2 :db/cardinality :db.cardinality/one 536870913 true]\n#datahike/Datom [2 :db/ident :age 536870913 true]\n#datahike/Datom [2 :db/valueType :db.type/long 536870913 true]\n#datahike/Datom [3 :age 25 536870913 true]\n#datahike/Datom [3 :name \"Alice\" 536870913 true]\n#datahike/Datom [4 :age 35 536870913 true]\n#datahike/Datom [4 :name \"Bob\" 536870913 true]\n")))

      (let [import-path (case os
                          "Windows 10" (str (System/getProperty "java.io.tmpdir") "reimport")
                          "/tmp/reimport")
            import-cfg {:store {:backend :file
                                :path import-path}}
            _ (d/delete-database import-cfg)
            _ (d/create-database import-cfg)
            new-conn (d/connect import-cfg)]

        (m/import-db new-conn export-path)

        (is (= (d/q '[:find ?e
                      :where [?e :name]]
                    @new-conn)
               #{[3] [4]}))
        (d/delete-database cfg)))))

(deftest load-entities-test
  (testing "Test migrate simple datoms"
    (let [source-datoms (->> tx-data
                             (mapv #(-> % rest vec))
                             (concat [[536870913 :db/txInstant #inst "2020-03-11T14:54:27.979-00:00" 536870913 true]]))]
      (let [cfg {:store {:backend :mem
                         :id "target"}}
            _ (d/delete-database cfg)
            _ (d/create-database cfg)
            conn (d/connect cfg)]
        @(d/load-entities conn source-datoms)
        (is (= (into #{} source-datoms)
               (d/q '[:find ?e ?a ?v ?t ?op :where [?e ?a ?v ?t ?op]] @conn)))))))
