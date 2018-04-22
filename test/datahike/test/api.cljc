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
      (release conn)
      (delete-database uri)
      )))


(deftest datahike-simple-schema-support
  (testing "Testing initial schema setup."
    (let [uri "datahike:file:///tmp/schema-api-test"
          _ (create-database-with-schema uri {:aka {:db/cardinality :db.cardinality/many}})
          conn (connect uri)]
      @(transact conn [ { :db/id -1
                         :name  "Maksim"
                         :age   45
                         :aka   ["Max Otto von Stierlitz", "Jack Ryan"] } ])
      (is (= (q '[ :find  ?n ?a ?aka
                 :where [?e :aka ?aka]
                 [?e :name ?n]
                 [?e :age  ?a] ]
                @conn)
             #{["Maksim" 45 "Max Otto von Stierlitz"] ["Maksim" 45 "Jack Ryan"]})
      (delete-database uri)))))

(comment

  (def uri #_"datahike:mem:///test"
    "datahike:file:///tmp/api-test"
    #_"datahike:level:///tmp/api-test1")

  (create-database-with-schema uri nil #_{:aka {:db/cardinality :db.cardinality/many}})

  (delete-database uri)

  (def conn (connect uri))

  (release conn)

  (:cache (:store @conn))

  (time
   (doseq [i (range 100)]
     @(transact conn [{ :db/id i, :name  "Ivan", :id i}])
     ))

  @(transact conn [ { :db/id -1
                      :name  "Maksim"
                       :age   45
                       :aka   ["Max Otto von Stierlitz", "Jack Ryan"] } ])

  (q '[ :find  ?n ?a ?aka
         :where [?e :aka ?aka]
         [?e :name ?n]
         [?e :age  ?a] ]
       @conn)

  (let [schema {:aka {:db/cardinality :db.cardinality/many}}
        conn   (d/create-conn schema)]
    
    )

  (time
   @(transact conn (for [i (range 10000)]
                     { :db/id i, :name  "Ivan", :id i}))
   )

  (dotimes [i 10]
    (time
     (q '[:find ?e
          :where [?e :id 485]]
        @conn)))



  (delete-database "datahike:file:///tmp/api-test"))

 
