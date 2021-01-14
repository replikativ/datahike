(ns db.compiled
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
  
  (defn run-query []
      (go (<! (d/q '[:find ?e ?a ?v ?t
                     :in $ ?a
                     :where [?e :name ?v ?t] [?e :age ?a]]
                   @conn-idb
                   35))))