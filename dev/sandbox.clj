(ns sandbox
  (:require ;[datahike.api :as d]
            ;[datahike.query :as q]
            [datahike.db :as db]
            [clojure.core.async :as async]
            [hitchhiker.tree.utils.clojure.async :as ha]))




(def working-tx-dummy {:initial-report {:db-before (async/<!! (db/empty-db)), :db-after (async/<!! (db/empty-db)), :tx-data [], :tempids {}, :tx-meta nil}
                       :initial-es [#:db{:ident :name, :cardinality :db.cardinality/one, :index true, :unique :db.unique/identity, :valueType :db.type/string} #:db{:ident :sibling, :cardinality :db.cardinality/many, :valueType :db.type/ref} #:db{:ident :age, :cardinality :db.cardinality/one, :valueType :db.type/long}]})



(comment
  
  (require '[datahike.db :as dd])
  (require '[datahike.datom :refer [datom]])

  (let [db (dd/init-db [(datom 1 :foo "bar") (datom 2 :qux :quun)])]
    (dd/-datoms db :eavt nil))

  (async/<!! (db/empty-db))

  (async/<!! (db/init-db []))


  (ha/<?? (db/transact-tx-data (:initial-report working-tx-dummy) (:initial-es working-tx-dummy)))

  (defn with
    "Same as [[transact!]], but applies to an immutable database value. Returns transaction report (see [[transact!]])."
    ([db tx-data] (with db tx-data nil))
    ([db tx-data tx-meta]
     {:pre [(db/db? db)]}
     (async/<!! (db/transact-tx-data (db/map->TxReport
                                      {:db-before db
                                       :db-after  db
                                       :tx-data   []
                                       :tempids   {}
                                       :tx-meta   tx-meta}) tx-data))))

  (def bob-db (:db-after (with (async/<!! (db/empty-db)) [{:name "bob" :age 5}])))

  (async/<!! (q/q '[:find ?a :where
                    [?e :name "bob"]
                    [?e :age ?a]]
                  bob-db))



  ;(def tx-dummy {:initial-report #datahike.db.TxReport{:db-before #datahike/DB {:max-tx 536870912 :max-eid 0}, :db-after #datahike/DB {:max-tx 536870912 :max-eid 0}, :tx-data [], :tempids {}, :tx-meta nil}
  ;               :initial-es [#:db{:ident :name, :cardinality :db.cardinality/one, :index true, :unique :db.unique/identity, :valueType :db.type/string} #:db{:ident :sibling, :cardinality :db.cardinality/many, :valueType :db.type/ref} #:db{:ident :age, :cardinality :db.cardinality/one, :valueType :db.type/long}]})

  ;;
  )


(comment

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
                :db/valueType   :db.type/long}])

  (def cfg {:store  {:backend :mem :id "sandbox"}
            :keep-history? true
            :schema-flexibility :write
            :initial-tx schema})

  (d/delete-database cfg)

  (d/create-database cfg)

  (def conn (d/connect cfg))

  (d/transact conn [{:name "Alice"
                     :age  25}
                    {:name "Bob"
                     :age  35}
                    {:name    "Charlie"
                     :age     45
                     :sibling [[:name "Alice"] [:name "Bob"]]}])

  (d/q '[:find ?e ?a ?v ?t
         :in $ ?a
         :where [?e :name ?v ?t] [?e :age ?a]]
       @conn
       35))

