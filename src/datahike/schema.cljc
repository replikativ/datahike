(ns ^:no-doc datahike.schema
  (:require [clojure.spec.alpha :as s]
            [datahike.datom])
  #?(:clj (:import [datahike.datom Datom])))

(s/def :db.type/id #?(:clj #(or (= (class %) java.lang.Long) string?)
                      :cljs #(or (and (number? %)
                                      (js/Number.isSafeInteger %))
                                 string?)))

;; db types
(s/def :db.type/bigdec #?(:cljs (complement any?) ; feels more appropriate than hiding key -pat
                          :clj decimal?))
(s/def :db.type/bigint #?(:clj integer?
                          :cljs #(or (integer? %)
                                     (= js/BigInt (type %)))))
(s/def :db.type/boolean boolean?)
(s/def :db.type/bytes #?(:clj bytes?
                         :cljs #(and (some->> (.-buffer %) (instance? js/ArrayBuffer))
                                     (some->> (.-byteLength %) (number?)))))
(s/def :db.type/double double?)
(s/def :db.type/float float?)
(s/def :db.type/number number?)
(s/def :db.type/instant #?(:clj #(= (class %) java.util.Date)
                           :cljs inst?))
(s/def :db.type/keyword keyword?)
(s/def :db.type/long #?(:clj #(= (class %) java.lang.Long)
                        :cljs #(js/Number.isSafeInteger %)))
(s/def :db.type/ref :db.type/id)
(s/def :db.type/string string?)
(s/def :db.type/symbol symbol?)
(s/def :db.type/uuid uuid?)
(s/def :db.type/tuple vector?)

(s/def :db.type/value
  #{:db.type/bigdec
    :db.type/bigint
    :db.type/boolean
    :db.type/bytes
    :db.type/double
    :db.type/float
    :db.type/number
    :db.type/instant
    :db.type/keyword
    :db.type/long
    :db.type/ref
    :db.type/string
    :db.type/symbol
    :db.type/uuid
    :db.type/value
    :db.type/tuple
    :db.type/cardinality
    :db.type.install/attribute
    :db.type/valueType
    :db.type/unique})

;; TODO: add bytes

(s/def :db.type/cardinality #{:db.cardinality/one :db.cardinality/many})
(s/def :db.type/unique #{:db.unique/identity :db.unique/value})

;; only for old datomic compliance, will be part of partioning in the future
(s/def :db.type.install/_attribute #{:db.part/tx :db.part/db :db.part/user})

(s/def ::schema-attribute #{:db/id :db/ident :db/isComponent :db/noHistory :db/valueType :db/cardinality :db/unique :db/index :db.install/_attribute :db/doc :db/tupleAttrs  :db/tupleType :db/tupleTypes})

(s/def ::entity-spec-attribute #{:db/ensure :db.entity/attrs :db.entity/preds})
(s/def ::meta-attribute #{:db/txInstant :db/retracted :db/noCommit})

(s/def ::schema (s/keys :req [:db/ident :db/valueType :db/cardinality]
                        :opt [:db/id :db/unique :db/index :db.install/_attribute :db/doc :db/noHistory :db/tupleType :db/tupleTypes]))

(s/def ::entity-spec (s/keys :opt [:db.entity/attrs :db.entity/preds]))

(s/def ::enum (s/keys :req [:db/ident]))

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
                                   :db/noHistory {:db/valueType :db.type/boolean
                                                  :db/unique :db.unique/identity
                                                  :db/cardinality :db.cardinality/one}
                                   :db/txInstant {:db/valueType :db.type/instant
                                                  :db/unique :db.unique/identity
                                                  :db/index true
                                                  :db/cardinality :db.cardinality/one}
                                   :db/noCommit  {:db/valueType :db.type/boolean
                                                  :db/unique :db.unique/identity
                                                  :db/cardinality :db.cardinality/one}
                                   :db/retracted {:db/valueType :db.type/long
                                                  :db/unique :db.unique/identity
                                                  :db/cardinality :db.cardinality/many}
                                   :db/ensure {:db/valueType :db.type/keyword
                                               :db/cardinality :db.cardinality/one}
                                   :db.entity/attrs {:db/valueType :db.type/keyword
                                                     :db/cardinality :db.cardinality/many}
                                   :db.entity/preds {:db/valueType :db.type/symbol
                                                     :db/cardinality :db.cardinality/many}
                                   :db/doc {:db/valueType :db.type/string
                                            :db/index true
                                            :db/cardinality :db.cardinality/one}
                                   :db.install/_attribute {:db/valueType   :db.type.install/_attribute
                                                           :db/unique      :db.unique/identity
                                                           :db/cardinality :db.cardinality/one}
                                   :db/tupleType {:db/valueType :db.type/value
                                                  :db/cardinality :db.cardinality/one}
                                   :db/tupleTypes {:db/valueType :db.type/tuple
                                                   :db/cardinality :db.cardinality/one}
                                   :db/tupleAttrs {:db/valueType :db.type/tuple
                                                   :db/cardinality :db.cardinality/one}})
(s/def :db/helpers #{:db.install/attribute :db})
(s/def :db.part/types #{:db.part/tx :db.part/sys :db.part/user})

(s/def :db.meta/attributes #{:db/txInstant})

(s/def ::sys-idents (s/or :value :db.type/value
                          :cardinality :db.type/cardinality
                          :parts :db.part/types
                          :helpers :db/helpers
                          :meta :db.meta/attributes
                          :unique :db.type/unique))

(def schema-keys #{:db/ident :db/isComponent :db/noHistory :db/valueType :db/cardinality :db/unique :db/index :db.install/_attribute :db/doc :db/tupleType :db/tupleTypes :db/tupleAttrs})

(s/def ::old-schema-val (s/keys :req [:db/valueType :db/cardinality]
                                :opt [:db/ident :db/unique :db/index :db.install/_attribute :db/doc :db/noHistory]))

(s/def ::old-schema-key keyword?)

(s/def ::old-schema (s/nilable (s/map-of ::old-schema-key ::old-schema-val)))

(defn old-schema-valid? [schema]
  (s/valid? ::old-schema schema))

(defn explain-old-schema [schema]
  (s/explain-data ::old-schema schema))

(defn meta-attr? [a-ident]
  (s/valid? ::meta-attribute a-ident))

(defn schema-attr? [a-ident]
  (s/valid? ::schema-attribute a-ident))

(defn sys-ident? [a-ident]
  (s/valid? ::sys-idents a-ident))

(defn entity-spec-attr? [a-ident]
  (s/valid? ::entity-spec-attribute a-ident))

(defn value-valid? [a-ident v-ident schema]
  (let [schema (if (or (meta-attr? a-ident) (schema-attr? a-ident) (entity-spec-attr? a-ident))
                 implicit-schema-spec
                 schema)
        value-type (get-in schema [a-ident :db/valueType])]
    (s/valid? value-type v-ident)))

(defn instant? [db ^Datom datom schema]
  (let [a-ident (if (:attribute-refs? (:config db))
                  ((:ref-ident-map db) (.-a datom))
                  (.-a datom))
        schema (if (or (meta-attr? a-ident) (schema-attr? a-ident))
                 implicit-schema-spec
                 schema)]
    (= (get-in schema [a-ident :db/valueType]) :db.type/instant)))

(defn schema-entity? [entity]
  (some #(contains? entity %) schema-keys))

(defn schema? [schema]
  (s/valid? ::schema schema))

(defn describe-type [schema-type]
  (s/describe schema-type))

(defn find-invalid-schema-updates [entity attr-schema]
  (reduce-kv
   (fn [m attr-def new-value]
     (let [old-value (get-in attr-schema [attr-def])]
       (when (not= old-value new-value)
         (case attr-def
           :db/cardinality
           ;; Prohibit update from :db.cardinality/one to :db.cardinality/many, if there is a :db/unique constraint.
           (when (and (= new-value :db.cardinality/many)
                      (#{:db.unique/value :db.unique/identity} (:db/unique attr-schema)))
             (assoc m attr-def [old-value new-value]))

           :db/unique
           (when (or (not (:db/unique attr-schema))
                     (not= :db.cardinality/one (:db/cardinality attr-schema)))
             (assoc m attr-def [old-value new-value]))

           ;; Always allow these attributes to be updated. 
           :db/doc nil
           :db/noHistory nil
           :db/isComponent nil

           (assoc m attr-def [old-value new-value])))))
   {}
   (dissoc entity :db/id)))

(defn is-system-keyword? [value]
  (and (or (keyword? value) (string? value))
       (if-let [ns (namespace (keyword value))]
         (= "db" (first (clojure.string/split ns #"\.")))
         false)))

(defn get-user-schema [{:keys [schema] :as db}]
  (into {} (filter #(not (is-system-keyword? (key %))) schema)))
