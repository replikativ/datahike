(ns sandbox
  (:require [datahike.api :as d]
            [datahike.datom :ass dd]
            [datahike.index :as di]
            [datahike.db :as ddb]))

(comment

  (def uri "datahike:mem://sandbox")

  (d/delete-database uri)

  (def schema [{:db/ident :name
                :db/cardinality :db.cardinality/one
                :db/index true
                :db/unique :db.unique/identity
                :db/valueType :db.type/string}
               {:db/ident :sibling
                :db/cardinality :db.cardinality/many
                :db/valueType :db.type/ref}
               {:db/ident :age
                :db/cardinality :db.cardinality/one
                :db/valueType :db.type/long}])

  (d/create-database uri :initial-tx schema)

  (def conn (d/connect uri))

  (def result (d/transact conn [{:name  "Alice", :age   25}
                                {:name  "Bob", :age   35}
                                {:name "Charlie", :age 45 :sibling [[:name "Alice"] [:name "Bob"]]}]))

  (d/q '[:find ?e ?v ?t :where [?e :name ?v ?t]] @conn)
  )

(comment

  (def dt-entities [[13194139534312
                     :db/txInstant
                     #inst "2019-11-13T08:38:13.555-00:00"
                     13194139534312
                     true]
                    [63 :db/ident :name 13194139534312 true]
                    [63 :db/cardinality :db.cardinality/one 13194139534312 true]
                    [63 :db/valueType :string 13194139534312 true]
                    [0 :db.install/attribute 63 13194139534312 true]
                    [17592186045420 :name "Charlie" 13194139534313 true]
                    [17592186045419 :name "Bob" 13194139534313 true]
                    [17592186045421 :name "Daisy" 13194139534313 true]
                    [13194139534313
                     :db/txInstant
                     #inst "2019-11-13T08:38:16.240-00:00"
                     13194139534313
                     true]
                    [17592186045418 :name "Alice" 13194139534313 true]
                    [13194139534318
                     :db/txInstant
                     #inst "2019-11-13T08:38:17.307-00:00"
                     13194139534318
                     true]
                    [17592186045421 :name "Daisy" 13194139534318 false]])

  (def migration-state (atom {}))

  (defn migrate-meta [db entities]
    (loop [meta-entities (->> entities
                              (filter (fn [[_ a _ _ _ :as entity]] (= a :db/txInstant)))
                              (sort-by first))
           db db]
      (if (empty? meta-entities)
        db
        (let [max-tx (inc (ddb/-max-tx db))
              [e a v t _] (first meta-entities)
              meta-entity (dd/datom max-tx a v max-tx)
              db' (cond-> db
                    true (update-in [:eavt] #(di/-insert % meta-entity :eavt))
                    true (update-in [:aevt] #(di/-insert % meta-entity :aevt))
                    true (ddb/advance-max-tid max-tx)
                    true (update :hash + (hash meta-entity)))]
          (swap! migration-state assoc-in [:tids e] max-tx)
          (recur (rest meta-entities) db')))))

  (defn migrate-data [db raw-entities]
    (loop [entities (->> raw-entities
                         (remove (fn [[_ a _ _ _]] (#{:db/txInstant :db.install/attribute} a)))
                         (sort-by first))
           db db]
      (if (empty? entities)
        db
        (let [max-eid (ddb/next-eid db)
              [e a v t added] (first entities)
              temporal-index? (ddb/-temporal-index? db)
              entity ^datahike.datom.Datom (dd/datom max-eid a v (get-in @migration-state [:tids t]) added)
              db' (if (ddb/datom-added entity)
                    (cond-> db
                      true (update-in [:eavt] #(di/-insert % entity :eavt))
                      true (update-in [:aevt] #(di/-insert % entity :aevt))
                      true (ddb/advance-max-eid (.-e entity))
                      true (update :hash + (hash entity)))
                    (if-some [removing ^datahike.datom.Datom (first (ddb/-search db [(.-e entity) (.-a entity) (.-v entity)]))]
                      (cond-> db
                        true (update-in [:eavt] #(di/-remove % removing :eavt))
                        true (update-in [:aevt] #(di/-remove % removing :aevt))
                        true (update :hash - (hash removing))
                        temporal-index? (update-in [:temporal-eavt] #(di/-insert % entity :eavt))
                        temporal-index? (update-in [:temporal-aevt] #(di/-insert % entity :aevt)))
                      db))]
          (swap! migration-state assoc-in [:eids e] max-eid)
          (recur (rest entities) db')))))

  (def new-db (-> (ddb/empty-db nil :datahike.index/hitchhiker-tree :config {:schema-on-read false
                                                                             :temporal-index true})
                  (migrate-meta dt-entities)
                  (migrate-data dt-entities))))
