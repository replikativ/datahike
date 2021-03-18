(ns demo
  (:require [datahike.api :as d]
            [datahike.impl.entity :as de]
            [clojure.core.async :as async :refer [go <!]]))



(def people-schema [{:db/ident       :name
                     :db/cardinality :db.cardinality/one
                     :db/index       true
                     :db/unique      :db.unique/identity
                     :db/valueType   :db.type/string}
                    {:db/ident       :age
                     :db/cardinality :db.cardinality/one
                     :db/valueType   :db.type/number}
                    {:db/ident       :country
                     :db/cardinality :db.cardinality/one
                     :db/valueType   :db.type/string}
                    {:db/ident       :siblings
                     :db/cardinality :db.cardinality/many
                     :db/valueType   :db.type/ref}
                    {:db/ident       :friend
                     :db/cardinality :db.cardinality/many
                     :db/valueType :db.type/ref}])


(def people-idb {:store  {:backend :indexeddb :id "people-idb"}
               :keep-history? true
               :schema-flexibility :write
               :initial-tx people-schema})


;; We will also create a national dish db with a schema on read
(def national-dish-idb {:store  {:backend :indexeddb :id "national-dish-idb"}
                        :keep-history? false
                        :schema-flexibility :read})





(comment
  ;; REPL-Driven code

  ;; Create an indexeddb store
  (go (<! (d/create-database people-idb)))

  ;; Connect to the indexeddb store
  (go (def conn-idb (<! (d/connect people-idb))))


  ;; Create a second database and connect to it. We will use this later.
  (go (<! (d/create-database national-dish-idb))
      (def conn-idb-2 (<! (d/connect national-dish-idb))))


  ;; Add people to our first database
  (go (<! (d/transact conn-idb [{:name "Alice"
                                 :age  26
                                 :country "Indonesia"}
                                {:name "Bob"
                                 :age  35
                                 :country "Germany"
                                 :_friend [{:name "Mike"
                                            :age 28
                                            :country "United Kingdom"}]}
                                {:name  "Charlie"
                                 :age   45
                                 :country "Italy"
                                 :siblings [[:name "Alice"] [:name "Bob"]]}])))

  ;; Add some dishes to our second database - we will use this later
  (go (<! (d/transact conn-idb-2 [{:country "Italy"
                                   :dishes #{"Pizza" "Pasta"}}
                                  {:country  "Indonesia"
                                   :dishes #{"Nasi goreng" "Satay" "Gado gado"}}
                                  {:country "United Kingdom"
                                   :dishes #{"Fish and Chips" "Sunday roast"}}
                                  {:country "Germany"
                                   :dishes #{"Döner kebab" "Currywurst" "Sauerbraten"}}])))


  ;; Find the :name of the person with :age of 26
  (go (println (<! (d/q '[:find ?v
                          :in $ ?a
                          :where
                          [?e :name ?v]
                          [?e :age ?a]]
                        @conn-idb
                        26))))


  ;; Use the pull API
  (go (println (<! (d/pull @conn-idb [:name, :age] 7))))
  (go (println (<! (d/pull @conn-idb '[*] 7))))
  (go (println (<! (d/pull-many @conn-idb '[:name :age] [5 6]))))


  ;; Use the Entity API
  (go (def touched-entity (<! (de/touch (<! (d/entity @conn-idb 8))))))

  (go (println (:name touched-entity)))
  (go (println (:age touched-entity)))
  (go (println (:siblings touched-entity)))

  (go (println (count touched-entity)))
  (go (println (keys touched-entity)))
  (go (println (vals touched-entity)))
  (go (println (contains? touched-entity :siblings)))



  ;; History functionality
  (go (println (<! (d/q '[:find ?a ?v
                          :in $
                          :where [?e :name ?v] [?e :age ?a]]
                        @conn-idb))))
  ;; #{[26 Alice] [35 Bob] [28 Mike] [45 Charlie]}
  ;; We get back the age and name of each entity in the database we initially transacted


  ;; Let's proceed to make changes to the database

  (d/transact conn-idb [{:name "Alice"
                         :age  20}])

  (def first-date-snapshot (js/Date.))

  (d/transact conn-idb [{:name "Alice"
                         :age  40}
                        {:name "Bob"
                         :age  20}])

  (def second-date-snapshot (js/Date.))


  (d/transact conn-idb [{:name "Alice"
                         :age  55}])

  (go (println (<! (d/q '[:find ?a ?v
                          :in $
                          :where [?e :name ?v] [?e :age ?a]]
                        @conn-idb))))
  ;; Result: #{[55 Alice] [20 Bob] [28 Mike] [45 Charlie]}
  ;; Now Alice and Bob have changed their ages


  ;; 👇 Get the full history of ages that has existed for "Alice" in the database and it's transaction
  (go (println (<! (d/q '[:find ?a ?t
                          :where
                          [?e :name "Alice"]
                          [?e :age ?a ?t]]
                        (d/history @conn-idb)))))
  ;; Result: #{[55 536870917] [20 -536870916] [20 536870915] [26 -536870915] [26 536870914] [40 -536870917] [40 536870916]}




  ;; 👇 We can also query the state of the db at a certain point in time. Using the transaction id or date.
  (go (println (<! (d/q '[:find ?n ?a
                          :where
                          [?e :name ?n]
                          [?e :age ?a]]
                        (d/as-of @conn-idb 536870915)))))
   ;; Result: #{[Mike 28] [Charlie 45] [Alice 20] [Bob 35]}
   ;; 536870915 = transaction-id when Alice was set to 20    

  (go (println (<! (d/q '[:find ?n ?a
                          :where
                          [?e :name ?n]
                          [?e :age ?a]]
                        (d/as-of @conn-idb first-date-snapshot)))))
   ;; Result: #{[Mike 28] [Charlie 45] [Alice 20] [Bob 35]}
   ;; You can also pass the date in as a parameter   

  (go (println (<! (d/q '[:find ?n ?a
                          :where
                          [?e :name ?n]
                          [?e :age ?a]]
                        (d/as-of @conn-idb second-date-snapshot)))))
   ;; Result: #{[Bob 20] [Mike 28] [Charlie 45] [Alice 40]}


  ;; 👇 All the changes since a point in time or transaction id
  (go (println (<! (d/q '[:find ?n ?a
                          :in $ $since
                          :where
                          [$ ?e :name ?n]
                          [$since ?e :age ?a]]
                        @conn-idb
                        (d/since @conn-idb first-date-snapshot)))))
  ;; Result: #{[Alice 55] [Bob 20] [Alice 40]}


  (go (println (<! (d/q '[:find ?n ?a
                          :in $ $since
                          :where
                          [$ ?e :name ?n]
                          [$since ?e :age ?a]]
                        @conn-idb
                        (d/since @conn-idb second-date-snapshot)))))
  ;#{[Alice 55]}


  ;; Now we query the second db we set up
  (go (println (<! (d/q '[:find ?n ?d
                          :in $ ?n
                          :where
                          [?e :country ?n]
                          [?e :dishes ?d]]
                        @conn-idb-2
                        "United Kingdom"))))


  ;; Let's do a cross db join to see what dishes Alice might like based on her country
  (go (println (<! (d/q '[:find ?name ?d
                          :in $1 $2 ?name
                          :where
                          [$1 ?e :name ?name]
                          [$1 ?e :country ?c]
                          [$2 ?e2 :country ?c]
                          [$2 ?e2 :dishes ?d]]
                        @conn-idb
                        @conn-idb-2
                        "Alice"))))


  ;; We can even pass a list of people to the query and get the results of what dishes they might like by using the national dish db
  (go (println (<! (d/q '[:find ?name ?d
                          :in $1 $2 [?name ...]
                          :where
                          [$1 ?e :name ?name]
                          [$1 ?e :country ?c]
                          [$2 ?e2 :country ?c]
                          [$2 ?e2 :dishes ?d]]
                        @conn-idb
                        @conn-idb-2
                        ["Alice" "Mike" "Charlie"]))))



  ;; You must release the connection before deleting the database. 
  ;; However if the database has been freshly created of you have refreshed the browser without connecting you can delete it straight away.

  (d/release conn-idb)
  (d/delete-database people-idb)

  (d/release conn-idb-2)
  (d/delete-database national-dish-idb)



  ;; format blocker
  )