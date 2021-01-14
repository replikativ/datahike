(ns api-sandbox
  (:require [datahike.api :as d]
            [datahike.impl.entity :as de]
            [clojure.core.async :as async :refer [go <!]]
            [datahike.db :as db]))


(def schema [{:db/ident       :name
              :db/cardinality :db.cardinality/one
              :db/index       true
              :db/unique      :db.unique/identity
              :db/valueType   :db.type/string}
             {:db/ident       :sibling
              :db/cardinality :db.cardinality/many
              :db/valueType   :db.type/ref}
             {:db/ident       :age
              :db/cardinality :db.cardinality/one
              :db/valueType   :db.type/number}
             {:db/ident       :friend
              :db/cardinality :db.cardinality/many
              :db/valueType :db.type/ref}])


(def cfg-idb {:store  {:backend :indexeddb :id "idb-sandbox"}
              :keep-history? true
              :schema-flexibility :write
              :initial-tx schema})


(comment

  ;; REPL-driven code


  ;; Create an indexeddb store
  (d/create-database cfg-idb)

  ;; Connect to the indexeddb store
  (go (def conn-idb (<! (d/connect cfg-idb))))


  ;; Transact some data to the store
  (d/transact conn-idb [{:name "Alice"
                         :age  26}
                        {:name "Bob"
                         :age  35
                         :_friend [{:name "Mike"
                                    :age 28}]}
                        {:name  "Charlie"
                         :age   45
                         :sibling [[:name "Alice"] [:name "Bob"]]}])

  ;; Run queries against the store
  (go (println (<! (d/q '[:find ?e ?a ?v ?t
                          :in $ ?a
                          :where [?e :name ?v ?t] [?e :age ?a]]
                        @conn-idb
                        26))))

  (go (println (<! (d/q '[:find ?e ?v
                          :in $ %
                          :where (r ?e ?v)]
                        @conn-idb
                        '[[(r ?e ?v)
                           [?e :name ?v]]]))))


  (go (println (<! (d/q '[:find ?e ?v
                          :in $
                          :where [?e :name ?v]]
                        @conn-idb))))

  (go (println (<! (d/q '[:find ?e ?v
                          :in $ %
                          :where (r ?e ?v)]
                        @conn-idb
                        '[[(r ?e ?v)
                           [?e :name ?v]]]))))


  ;; Use the Entity API
  (async/go (println (<! ((<! (d/entity @conn-idb 6)) :name))))
  (async/go (println (<! ((<! (d/entity @conn-idb 6)) :age))))
  (async/go (println (<! ((<! (d/entity @conn-idb 8)) :sibling))))
  (async/go (println (<! ((<! (d/entity @conn-idb 7)) :friend))))

  (go
    (let [e (<! (d/entity @conn-idb 6))]
      (println (<! (e :name)))))


  ;; Collection operators over a touched db
  (async/go  (println (count (<! (de/touch (<! (d/entity @conn-idb 6)))))))
  (async/go  (println (keys (<! (de/touch (<! (d/entity @conn-idb 6)))))))
  (async/go  (println (vals (<! (de/touch (<! (d/entity @conn-idb 6)))))))
  (async/go  (println (contains? (<! (de/touch (<! (d/entity @conn-idb 6)))) :name)))


  ;; Basic performance measurements. This takes up to 30 seconds.
  ;; We expect significant improvements from upserts.
  ;; Which will happen with the rebase against the development branch.
  (go (time (<! (d/transact conn-idb (vec (for [i (range 10000)]
                                            {:age i}))))))

  (go (time (println (<! (d/q '[:find (count ?e)
                                :where
                                [?e :age _]]
                              @conn-idb)))))



  ;; Only use this when you are ready to delete.
  ;; Release the connection from the store
  (d/release conn-idb)


  ;; Delete the store. 
  ;; This can be done immediately after creation.
  ;; In a fresh browser session or after releasing the connection. 
  (d/delete-database cfg-idb)


  ;; Typical indexeddb behaviour blocks the deletion of a database if there is an open connection.
  ;; When all connections are released it will delete the database.
  ;; Even if you called this before releasing the connection.
  ;; The functions below allow this behaviour. 
  ;; It can be possible to open multiple connections to an indexeddb store.
  ;; Across open tabs and also through peers. As such.
  ;; We are still thinking through connection management.

  ;; Declare the release connection function
  (defn release-connection []
    (.close (:db (:store @conn-idb))))

  ;; Release the connection
  (release-connection)

  ;;
  ;; Creating multiple databases in the browser!
  ;; It's possible to create multiple indexeddb databases.
  ;; 

  (def cfg-idb-2 {:store  {:backend :indexeddb :id "idb-example-2"}
                  :keep-history? false
                  :schema-flexibility :write
                  :initial-tx schema})

  ;; Create a second database
  (d/create-database cfg-idb-2)

  ;; Check for existence of second database
  (go (println (<! (d/database-exists? cfg-idb-2))))

  ;; Delete second database
  (d/delete-database cfg-idb-2)



  ;;
  ;; Test History functionality
  ;; This uses the first database we created. To use these queries don't delete the first database
  ;; 

  (go (println (<! (d/q '[:find ?a ?v
                          :in $
                          :where [?e :name ?v] [?e :age ?a]]
                        @conn-idb))))
  ;#{[26 Alice] [35 Bob] [28 Mike] [45 Charlie]}
  ;; We get back the age and name of each entity in the database so far

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
  ;#{[55 Alice] [20 Bob] [28 Mike] [45 Charlie]}
  ;;; Now Alice and Bob have changed their ages

  (go (println (<! (d/q '[:find ?a ?t
                          :where
                          [?e :name "Alice"]
                          [?e :age ?a ?t]]
                        (d/history @conn-idb)))))
  ;#{[55 536870917] [20 -536870916] [20 536870915] [26 -536870915] [26 536870914] [40 -536870917] [40 536870916]}
  ;Get the full history of ages that has existed for "Alice" in the database





  (go (println (<! (d/q '[:find ?n ?a
                          :where
                          [?e :name ?n]
                          [?e :age ?a]]
                        (d/as-of @conn-idb 536870915))))) ; 536870915 = transaction-id when Alice was set to 20
   ;#{[Mike 28] [Charlie 45] [Alice 20] [Bob 35]}
   ;
  (go (println (<! (d/q '[:find ?n ?a
                          :where
                          [?e :name ?n]
                          [?e :age ?a]]
                        (d/as-of @conn-idb first-date-snapshot)))))
   ;#{[Mike 28] [Charlie 45] [Alice 20] [Bob 35]}

  (go (println (<! (d/q '[:find ?n ?a
                          :where
                          [?e :name ?n]
                          [?e :age ?a]]
                        (d/as-of @conn-idb second-date-snapshot)))))
   ;#{[Bob 20] [Mike 28] [Charlie 45] [Alice 40]}


  (go (println (<! (d/q '[:find ?n ?a
                          :in $ $since
                          :where
                          [$ ?e :name ?n]
                          [$since ?e :age ?a]]
                        @conn-idb
                        (d/since @conn-idb 536870915))))) ; 536870915 = transaction-id when Alice was set to 20
  ;#{[Alice 55] [Bob 20] [Alice 20] [Alice 40]}



  (go (println (<! (d/q '[:find ?n ?a
                          :in $ $since
                          :where
                          [$ ?e :name ?n]
                          [$since ?e :age ?a]]
                        @conn-idb
                        (d/since @conn-idb first-date-snapshot)))))
  ;#{[Alice 55] [Bob 20] [Alice 40]}

  (go (println (<! (d/q '[:find ?n ?a
                          :in $ $since
                          :where
                          [$ ?e :name ?n]
                          [$since ?e :age ?a]]
                        @conn-idb
                        (d/since @conn-idb second-date-snapshot)))))
  ;#{[Alice 55]}




  ;;
  ;; Other experiments with entity API
  ;;

  ;; Creating counter - Delete later
  (def cfg-idb-3 {:store  {:backend :indexeddb :id "idb-sandbox-3"}
                  :keep-history? false
                  :schema-flexibility :read})

    ;; Create an indexeddb store
  (d/create-database cfg-idb-3)

  ;; Connect to the indexeddb store
  (go (def conn-idb-3 (<! (d/connect cfg-idb-3))))


  (d/transact conn-idb-3 [{:db/ident :counter
                           :counter/count 1}])

  (d/transact conn-idb-3 [[:db/add [:db/ident :counter] :counter/count 2]])



  (async/go (println (<! ((<! (d/entity @conn-idb-3 :counter)) :counter/count))))






  ;; Blocks auto-format from wrapping the paren at last form
  )
