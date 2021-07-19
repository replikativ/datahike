(ns ^:no-doc datahike.constants)

(def ^:const e0 0x00000000)
(def ^:const tx0 0x20000000)
(def ^:const emax 0x7FFFFFFF)
(def ^:const txmax 0x7FFFFFFF)

(def ^:const system-schema
  [{:db/id        tx0
    :db/txInstant #inst"1970-01-01T00:00:00.000-00:00"}
   {:db/id 1
    :db/ident :db/ident
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc "An attribute's or specification's identifier"
    :db/unique :db.unique/value}
   {:db/id 2
    :db/ident :db/valueType
    :db/valueType :db.type/valueType
    :db/cardinality :db.cardinality/one
    :db/doc "An attribute's value type"}
   {:db/id 3
    :db/ident :db/cardinality
    :db/valueType :db.type/cardinality
    :db/cardinality :db.cardinality/one
    :db/doc "An attribute's cardinality"}
   {:db/id 4
    :db/ident :db/doc
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc "An attribute's documentation"}
   {:db/id 5
    :db/ident :db/index
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db/doc "An attribute's index selection"}
   {:db/id 6
    :db/ident :db/unique
    :db/valueType :db.type/unique
    :db/cardinality :db.cardinality/one
    :db/doc "An attribute's unique selection"}
   {:db/id 7
    :db/ident :db/noHistory
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db/doc "An attribute's history selection"}
   {:db/id 8
    :db/ident :db.install/attribute
    :db/valueType :db.type.install/attribute
    :db/cardinality :db.cardinality/one
    :db/doc "Only for interoperability with Datomic"}
   {:db/id 9
    :db/ident :db/txInstant
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/doc "A transaction's time-point"
    :db/noHistory true
    :db/index true}
   {:db/id 10
    :db/ident :db.cardinality/many}
   {:db/id 11
    :db/ident :db.cardinality/one}
   {:db/id 12
    :db/ident :db.part/sys}
   {:db/id 13
    :db/ident :db.part/tx}
   {:db/id 14
    :db/ident :db.part/user}
   {:db/id 15
    :db/ident :db.type/bigdec}
   {:db/id 16
    :db/ident :db.type/bigint}
   {:db/id 17
    :db/ident :db.type/boolean}
   {:db/id 18
    :db/ident :db.type/double}
   {:db/id 19
    :db/ident :db.type/cardinality}
   {:db/id 20
    :db/ident :db.type/float}
   {:db/id 21
    :db/ident :db.type/number}
   {:db/id 22
    :db/ident :db.type/instant}
   {:db/id 23
    :db/ident :db.type/keyword}
   {:db/id 24
    :db/ident :db.type/long}
   {:db/id 25
    :db/ident :db.type/ref}
   {:db/id 26
    :db/ident :db.type/string}
   {:db/id 27
    :db/ident :db.type/symbol}
   {:db/id 28
    :db/ident :db.type/unique}
   {:db/id 29
    :db/ident :db.type/uuid}
   {:db/id 30
    :db/ident :db.type/valueType}
   {:db/id 31
    :db/ident :db.type.install/attribute}
   {:db/id 32
    :db/ident :db.unique/identity}
   {:db/id 33
    :db/ident :db.unique/value}
   {:db/id 34
    :db/ident :db/isComponent}
   {:db/id 35
    :db/ident :db/tupleType}
   {:db/id 36
    :db/ident :db/tupleTypes}
   {:db/id 37
    :db/ident :db/tupleAttrs}
   {:db/id 38
    :db/ident :db.type/tuple}])

(def ^:const system-entities
  "Holds the entity IDs of system attributes"
  (set (reduce
        (fn [m {:keys [db/ident db/id] :as attr}]
          (when ident (conj m id)))
        []
        system-schema)))

(def ^:const non-ref-implicit-schema
  {:db/ident {:db/unique :db.unique/identity}
   :db/txInstant {:db/noHistory true}
   :db.entity/attrs {:db/cardinality :db.cardinality/many}
   :db.entity/preds {:db/cardinality :db.cardinality/many}})

(def ^:const ref-implicit-schema
  "Maps attribute names to the attribute's specification"
  (reduce
   (fn [m {:keys [db/ident] :as attr}]
     (when ident
       (assoc m ident (dissoc attr :db/ident))))
   {}
   system-schema))

(def ^:const ue0 (transduce (comp (map :db/id) (remove #{tx0})) max 0 system-schema))
(def ^:const utx0 tx0)

(def ^:const default-index-b-factor 17)
(def ^:const default-index-data-node-size 300)
(def ^:const default-index-log-size (- 300 17))
