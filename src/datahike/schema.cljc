(ns datahike.schema
  (:require [clojure.spec.alpha :as s]))

(s/def :db.type/id #(or (= (class %) java.lang.Long) string?))

;; db types
(s/def :db.type/bigdec #(= (class %) java.math.BigDecimal))
(s/def :db.type/bigint #(= (class %) java.math.BigInteger))
(s/def :db.type/boolean boolean?)
(s/def :db.type/double double?)
(s/def :db.type/float float?)
(s/def :db.type/instant #(= (class %) java.util.Date))
(s/def :db.type/keyword keyword?)
(s/def :db.type/long #(= (class %) java.lang.Long))
(s/def :db.type/ref :db.type/id)
(s/def :db.type/string string?)
(s/def :db.type/symbol symbol?)
(s/def :db.type/uuid #(= (class %) java.util.UUID))

(s/def :db.type/value
  #{:db.type/bigdec
    :db.type/bigint
    :db.type/boolean
    :db.type/double
    :db.type/float
    :db.type/instant
    :db.type/keyword
    :db.type/long
    :db.type/ref
    :db.type/string
    :db.type/symbol
    :db.type/uuid
    :db.type/value})

;; TODO: add tuples

(s/def :db.type/cardinality #{:db.cardinality/one :db.cardinality/many})
(s/def :db.type/unique #{:db.unique/identity :db.unique/value})

;; only for old datomic compliance, will be part of partioning in the future
(s/def :db.type.install/_attribute #{:db.part/tx :db.part/db :db.part/user})

(s/def ::schema-attribute #{:db/id :db/ident :db/isComponent :db/valueType :db/cardinality :db/unique :db/index :db.install/_attribute :db/doc})

(s/def ::schema (s/keys :req [:db/ident :db/valueType :db/cardinality ]
                        :opt [:db/id :db/unique :db/index :db.install/_attribute]))

(def required-keys #{:db/ident :db/valueType :db/cardinality})

(def ^:const implicit-schema-spec {:db/ident {:db/valueType   :db.type/keyword
                                              :db/unique      :db.unique/identity
                                              :db/cardinality :db.cardinality/one}
                                   :db/valueType {:db/valueType   :db.type/value
                                                  :db/unique      :db.unique/identity
                                                  :db/cardinality :db.cardinality/one}
                                   :db/id {:db/valueType   :db.type/id
                                           :db/unique      :db.unique/identity
                                           :db/cardinality :db.cardinality/one}
                                   :db/cardinality {:db/valueType   :db.type/cardinality
                                                    :db/unique      :db.unique/identity
                                                    :db/cardinality :db.cardinality/one}
                                   :db/index {:db/valueType   :db.type/boolean
                                              :db/unique      :db.unique/identity
                                              :db/cardinality :db.cardinality/one}
                                   :db/unique {:db/valueType   :db.type/unique
                                               :db/unique      :db.unique/identity
                                               :db/cardinality :db.cardinality/one}
                                   :db/isComponent {:db/valueType :db.type/boolean
                                                    :db/unique :db.unique/identity
                                                    :db/cardinality :db.cardinality/one}
                                   :db.install/_attribute {:db/valueType   :db.type.install/_attribute
                                                           :db/unique      :db.unique/identity
                                                           :db/cardinality :db.cardinality/one}})
(def schema-keys #{:db/ident :db/isComponent :db/valueType :db/cardinality :db/unique :db/index :db.install/_attribute})

(defn schema-attr? [attr]
  (s/valid? ::schema-attribute attr))

(defn value-valid? [[_ _ a v _] schema]
  (let [schema (if (schema-attr? a) implicit-schema-spec schema)
        value-type (get-in schema [a :db/valueType])]
    (s/valid? value-type v)))

(defn schema-entity? [entity]
  (some #(contains? entity %) schema-keys))

(defn schema? [schema]
  (s/valid? ::schema schema))

(defn describe-type [schema-type]
  (s/describe schema-type))
