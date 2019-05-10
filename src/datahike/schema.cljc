(ns datahike.schema
  (:require [clojure.spec.alpha :as s]))

(s/def :db/valueType #{:db.type/ref :db.type/string :db.type/long :db.type/boolean :db.type/double})
(s/def :db/ident keyword?)
(s/def :db/cardinality #{:db.cardinality/one :db.cardinality/many})
(s/def :db/unique #{:db.unique/identity :db.unique/value})
(s/def :db/index boolean?)
(s/def :db/id #(= (class %) java.lang.Long)
(s/def :db.install/_attribute #{:db.part/tx :db.part/db :db.part/user})

(comment


{:db/id #db/id[:db.part/db]
 :db/ident :source/user
 :db/valueType :db.type/ref
 :db/cardinality :db.cardinality/one
 :db/unique :db.unique/identity
 :db/index true
 :db.install/_attribute :db.part/db}

  )
