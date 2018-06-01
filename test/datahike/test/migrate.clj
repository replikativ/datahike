(ns datahike.test.migrate
  (:require [clojure.test :refer :all]
            [datahike.migrate :refer :all]
            [datahike.api :refer :all]))


(deftest export-import-test
  (testing "Test a roundtrip for exporting and importing."
    (let [uri "datahike:file:///tmp/export-db1"
          _ (delete-database uri)
          _ (create-database uri)
          conn (connect uri)]
      @(transact conn [{ :db/id 1, :name  "Ivan", :age   15 }
                       { :db/id 2, :name  "Petr", :age   37 }
                       { :db/id 3, :name  "Ivan", :age   37 }
                       { :db/id 4, :age 15 }])
      (export-db @conn "/tmp/eavt-dump")
      (is (= (slurp "/tmp/eavt-dump")
             "#datahike/Datom [1 :age 15 536870913 true]\n#datahike/Datom [1 :name \"Ivan\" 536870913 true]\n#datahike/Datom [2 :age 37 536870913 true]\n#datahike/Datom [2 :name \"Petr\" 536870913 true]\n#datahike/Datom [3 :age 37 536870913 true]\n#datahike/Datom [3 :name \"Ivan\" 536870913 true]\n#datahike/Datom [4 :age 15 536870913 true]\n"))
      (let [import-uri "datahike:mem:///reimport"
            _ (create-database import-uri)
            new-conn (connect import-uri)]
        (import-db new-conn "/tmp/eavt-dump")
        (is (= (q '[:find ?e
                    :where [?e :name]]
                  @new-conn)
               #{[3] [2] [1]}))
        (delete-database uri)))))

