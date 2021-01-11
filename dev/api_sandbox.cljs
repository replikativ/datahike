(ns api-sandbox
  (:require [datahike.api :as d]
            [datahike.impl.entity :as de]
            [clojure.core.async :as async :refer [go <!]]))


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
              :keep-history? false
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

  ;; Run a queries against the store
  (go (println (<! (d/q '[:find ?e ?a ?v ?t
                          :in $ ?a
                          :where [?e :name ?v ?t] [?e :age ?a]]
                        @conn-idb
                        35))))
  
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



  ;; It's possible to create multiple indexeddb databases.
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


  ;; Blocks formatter from wrapping the parent at last form
  )
