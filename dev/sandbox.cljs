(ns sandbox
  (:require [datahike.query :as q]
            [datahike.db :as db]
            [clojure.core.async :as async :refer [go <!]]
            [hitchhiker.tree.utils.cljs.async :as ha]
            [datahike.impl.entity :as de]))



(async/go
  (def working-tx-dummy {:initial-report {:db-before (async/<! (db/empty-db)), :db-after (async/<! (db/empty-db)), :tx-data [], :tempids {}, :tx-meta nil}
                         :initial-es [#:db{:ident :name, :cardinality :db.cardinality/one, :index true, :unique :db.unique/identity, :valueType :db.type/string} #:db{:ident :sibling, :cardinality :db.cardinality/many, :valueType :db.type/ref} #:db{:ident :age, :cardinality :db.cardinality/one, :valueType :db.type/long}]}))


(comment
  ;; REPL-driven code

  (println "Browser connection test")


  ;;
  ;; Primary work
  ;;

  (defn with
    "Same as [[transact!]], but applies to an immutable database value. Returns transaction report (see [[transact!]])."
    ([db tx-data] (with db tx-data nil))
    ([db tx-data tx-meta]
     {:pre [(db/db? db)]}
     (db/transact-tx-data (db/map->TxReport
                           {:db-before db
                            :db-after  db
                            :tx-data   []
                            :tempids   {}
                            :tx-meta   tx-meta}) tx-data)))



  (def schema {:aka {:db/cardinality :db.cardinality/many}
               :friend {:db/valueType :db.type/ref
                        :db/cardinality :db.cardinality/many}})

  (ha/go-try (def bob-db (:db-after (ha/<? (with (ha/<? (db/empty-db schema))
                                                 [{:name "Bob"
                                                   :age 35
                                                   :aka  ["Max" "Jack"]
                                                   :_friend [{:name "Boris"
                                                              :age 28}]}])))))


  ;; Query bob-db
  (async/go (println (async/<! (q/q '[:find ?a :where
                                      [?e :name "Bob"]
                                      [?e :age ?a]]
                                    bob-db))))


  ;; Looking up entity associated with "Bob"
  (async/go (println (<! ((<! (de/entity bob-db 1)) :name))))
  (async/go (println (first (<! ((<! (de/entity bob-db 1)) :aka)))))
  (async/go (println (<! ((<! (de/entity bob-db 1)) :age))))
  (async/go (println (<! ((<! (de/entity bob-db 1)) :_friend))))
  (async/go (println (<! ((<! (de/entity bob-db 1)) :db/id))))



  ;; Looking up entity associated with "Boris"
  (async/go (println (<! ((<! (de/entity bob-db 2)) :name))))
  (async/go (println (<! ((<! (de/entity bob-db 2)) :friend))))
  (async/go (println (<! ((first (<! ((<! (de/entity bob-db 2)) :friend))) :name))))


  ;; Operators over a touched db
  (async/go  (println (count (<! (de/touch (<! (de/entity bob-db 1)))))))
  (async/go  (println (keys (<! (de/touch (<! (de/entity bob-db 1)))))))
  (async/go  (println (vals (<! (de/touch (<! (de/entity bob-db 1)))))))
  (async/go  (println (contains? (<! (de/touch (<! (de/entity bob-db 1)))) :name)))


  ;; Inspect a touched entity
  (async/go  (def touched-entity (<! (de/touch (<! (de/entity bob-db 1))))))
  @(.-cache touched-entity)
  @(.-touched touched-entity)
  @(.-db touched-entity)
  (count touched-entity)
  
  
  ;; Blocks formatter from wrapping the parent at last form
  )
