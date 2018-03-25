(ns datahike.test.api
  (:require [datahike.api :refer :all]
            [clojure.test :refer :all]))


(deftest datahike-api-test
  (testing "Testing the high-level API."
    (let [uri "datahike:file:///tmp/api-test"
          _ (create-database uri)
          conn (connect uri)
          tx-report @(transact conn [{ :db/id 1, :name  "Ivan", :age   15 }
                                     { :db/id 2, :name  "Petr", :age   37 }
                                     { :db/id 3, :name  "Ivan", :age   37 }
                                     { :db/id 4, :age 15 }])]
      (is (= (q '[:find ?e
                  :where [?e :name]]
                @conn)
             #{[3] [2] [1]}))
      (delete-database uri)
      )))


(comment
  (delete-database "datahike:file:///tmp/api-test"))

 
