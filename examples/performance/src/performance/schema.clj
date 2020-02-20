(ns performance.schema)

(def schemas {"simple" [{:db/ident :name
                         :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one}]
              "complex" []})