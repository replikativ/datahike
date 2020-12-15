(ns api-sandbox-mem
  (:require [datahike.api :as d]
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


(def cfg {:store  {:backend :mem :id "mem-sandbox"}
              :keep-history? false
              :schema-flexibility :write
              :initial-tx schema})



(comment

  ;; REPL-driven code


  ;; Create an indexeddb store
  (d/create-database cfg)

  ;; Connect to the indexeddb store
  (go (def conn (<! (d/connect cfg))))


  ;; Transact some data to the store
  (d/transact conn [{:name "Alice"
                         :age  26}
                        {:name "Bob"
                         :age  35
                         :_friend [{:name "Mike"
                                    :age 28}]}
                        {:name  "Charlie"
                         :age   45
                         :sibling [[:name "Alice"] [:name "Bob"]]}])

  ;; Run a query against the store
  (go (println (<! (d/q '[:find ?e ?a ?v ?t
                          :in $ ?a
                          :where [?e :name ?v ?t] [?e :age ?a]]
                        @conn
                        35))))


  ;; Use the Entity API
  (async/go (println (<! ((<! (d/entity @conn 6)) :name))))
  (async/go (println (<! ((<! (d/entity @conn 6)) :age))))
  (async/go (println (<! ((<! (d/entity @conn 8)) :sibling))))
  (async/go (println (<! ((<! (d/entity @conn 7)) :friend))))


  ;; Basic performance measurements. This takes up to 30 seconds.
  ;; We expect significant improvements from upserts with a rebase against master.
  (go (time (<! (d/transact conn (vec (for [i (range 10000)]
                                            {:age i}))))))

  (go (time (println (<! (d/q '[:find (count ?e)
                                :where
                                [?e :age _]]
                              @conn)))))


  ;; Delete the store. 
  (d/delete-database cfg)



  ;; Blocks formatter from wrapping the parent at last form
  )