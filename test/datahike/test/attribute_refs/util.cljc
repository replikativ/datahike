(ns datahike.test.attribute-refs.util
  (:require [datahike.api :as d]
            [datahike.db :as db]))

(def ref-config {:store {:backend :mem
                         :id "attr-refs-test.util"}
                 :attribute-refs? true
                 :keep-history? false
                 :schema-flexibility :write})

(def test-schema
  [{:db/ident       :name
    :db/cardinality :db.cardinality/one
    :db/valueType   :db.type/string
    :db/unique      :db.unique/identity}
   {:db/ident       :mname
    :db/cardinality :db.cardinality/one
    :db/valueType   :db.type/string}
   {:db/ident       :aka
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/string}
   {:db/ident       :age
    :db/cardinality :db.cardinality/one
    :db/valueType   :db.type/long}
   {:db/ident       :sex
    :db/cardinality :db.cardinality/one
    :db/valueType   :db.type/string}
   {:db/ident       :huh?
    :db/cardinality :db.cardinality/one
    :db/valueType   :db.type/boolean}
   {:db/ident       :father
    :db/cardinality :db.cardinality/one
    :db/valueType   :db.type/ref}
   {:db/ident       :parent
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/ref}
   {:db/ident       :children
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/ref}
   {:db/ident       :had-birthday
    :db/cardinality :db.cardinality/one
    :db/valueType   :db.type/boolean}
   {:db/ident       :weight
    :db/cardinality :db.cardinality/one
    :db/valueType   :db.type/long}
   {:db/ident       :height
    :db/cardinality :db.cardinality/one
    :db/valueType   :db.type/long}
   {:db/ident       :child
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/ref}
   {:db/ident       :friend
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/ref}
   {:db/ident       :enemy
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/ref}
   {:db/ident       :label
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/keyword}
   {:db/ident       :part
    :db/valueType   :db.type/ref
    :db/isComponent true
    :db/cardinality :db.cardinality/many}
   {:db/ident       :spec
    :db/valueType   :db.type/ref
    :db/isComponent true
    :db/cardinality :db.cardinality/one}
   {:db/ident       :attr
    :db/cardinality :db.cardinality/one
    :db/valueType   :db.type/keyword}
   {:db/ident       :follow
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/ref}
   {:db/ident       :value
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/long}
   {:db/ident       :f1
    :db/cardinality :db.cardinality/one
    :db/valueType   :db.type/ref}
   {:db/ident       :f2
    :db/cardinality :db.cardinality/one
    :db/valueType   :db.type/ref}])

(defn setup-new-connection []
  (d/delete-database ref-config)
  (d/create-database ref-config)
  (let [conn (d/connect ref-config)]
    (d/transact conn test-schema)
    conn))

(def test-setup
  (let [conn (setup-new-connection)
        max-eid (:max-eid @conn)]
    {:conn conn :db @conn :e0 max-eid}))

(def ref-db (:db test-setup))
(def ref-conn (:conn test-setup))
(def ref-e0 (:e0 test-setup))

(defn wrap-ref-datoms [db offset op datoms]
  (mapv (fn [[e a v]] [op (+ offset e) (db/-ref-for db a) (+ offset v)])
        datoms))

(defn wrap-direct-datoms [db offset op datoms]
  (mapv (fn [[e a v]] [op (+ offset e) (db/-ref-for db a) v])
        datoms))

(defn shift-entities [offset entities]
  (mapv (fn [entity] (if (:db/id entity)
                       (update entity :db/id (partial + offset))
                       entity))
        entities))

(defn shift-in [coll-of-coll indices offset]
  (into (empty coll-of-coll)
        (map (fn [item] (reduce (fn [acc i] (update acc i + offset))
                                item
                                indices))
             coll-of-coll)))

(defn shift [coll offset]
  (into (empty coll)
        (map (partial + offset)
             coll)))
