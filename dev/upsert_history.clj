(ns upsert-history)

(require '[datahike.api :as d])

(def schema [{:db/ident       :name
              :db/cardinality :db.cardinality/one
              :db/index       true
              :db/unique      :db.unique/identity
              :db/valueType   :db.type/string}
             {:db/ident       :parents
              :db/cardinality :db.cardinality/many
              :db/valueType   :db.type/ref}
             {:db/ident       :age
              :db/cardinality :db.cardinality/one
              :db/valueType   :db.type/long}])

#_(def cfg {:store {:backend :mem
                  :id "sandbox"}
          :name "sandbox"
          :keep-history? true
          :schema-flexibility :write
          :attribute-refs? true})

(def cfg {:store {:backend :file :path "/tmp/upsert-history"
                  :id "sandbox"}
          :name "sandbox"
          :keep-history? true
          :schema-flexibility :write
          :attribute-refs? true
          :index :datahike.index/hitchhiker-tree})


(d/delete-database cfg)
(d/create-database cfg)

(def conn (d/connect cfg))

(d/transact conn {:tx-data schema})

(d/transact conn {:tx-data [{:name "Alice"
                             :age  25}]})

(d/datoms @conn :eavt [:name "Alice"] :age)

(d/datoms (d/history @conn) :eavt [:name "Alice"] :age)

(d/transact conn {:tx-data [{:db/id [:name "Alice"]
                             :age 25}]})

(d/transact conn {:tx-data (vec (map (fn [_]
                                       {:db/id [:name "Alice"]
                                        :age 25})
                                  (range 5000)))})



(d/datoms @conn :eavt [:name "Alice"] :age)

(d/datoms (d/history @conn) :eavt [:name "Alice"] :age)

(d/transact conn {:tx-data [{:db/id [:name "Alice"]
                             :age 25}]})

(d/datoms @conn :eavt [:name "Alice"] :age)

(d/datoms (d/history @conn) :eavt [:name "Alice"] :age)
