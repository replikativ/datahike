(ns performance.schema)

(defn make-col
  ([name type] (make-col name type :db.cardinality/one))
  ([name type cardinality]
   {:db/ident       name
    :db/valueType   type
    :db/cardinality cardinality}))