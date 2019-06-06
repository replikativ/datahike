(ns datahike.schema
  (:require [clojure.spec.alpha :as s]))

(s/def :db.type/id #(or (= (class %) java.lang.Long) string?))
(s/def :db.type/value #{:db.type/ref :db.type/string :db.type/long :db.type/boolean :db.type/double :db.type/keyword})
(s/def :db.type/ref :db.type/id)
(s/def :db.type/string string?)
(s/def :db.type/long #(= (class %) java.lang.Long))
(s/def :db.type/boolean boolean?)
(s/def :db.type/double double?)
(s/def :db.type/keyword keyword?)
(s/def :db.type/cardinality #{:db.cardinality/one :db.cardinality/many})
(s/def :db.type/unique #{:db.unique/identity :db.unique/value})
;; only for datomic compliance
(s/def :db.type.install/_attribute #{:db.part/tx :db.part/db :db.part/user})

(s/def ::schema-attribute #{:db/id :db/ident :db/isComponent :db/valueType :db/cardinality :db/unique :db/index :db.install/_attribute})

(s/def ::schema (s/keys :req [:db/id :db/ident :db/valueType]
                        :opt [:db/cardinality :db/unique :db/index :db.install/_attribute]))

(defn schema-attr? [attr]
  (s/valid? ::schema-attribute attr))

(defn schema-val-valid? [[e a v _] schema]
  (when (schema-attr? a)
    (s/valid? (-> schema a :db/valueType) v)))

(defn value-valid? [[_ _ a v _ :as at] schema]
  (s/valid?
   (get-in schema
           (if (schema-attr? a)
             [:db.part/db a :db/valueType]
             [a :db/valueType]))
   v))

(defn schema-valid? [schema]
  (s/valid? ::schema schema))

(comment
  
  
  
  )