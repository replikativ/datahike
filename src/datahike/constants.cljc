(ns datahike.constants)

(def ^:const e0-sys 0)
(def ^:const tx0-sys 0x20000000)
(def ^:const emax 0x7FFFFFFF)
(def ^:const txmax 0x7FFFFFFF)

(def ^:const complete-implicit-schema
  [{:db/ident       :db/ident,
    :db/valueType   :db.type/keyword,
    :db/cardinality :db.cardinality/one,
    :db/doc         "An attribute's or specification's identifier",
    :db/unique      :db.unique/identity}
   {:db/ident       :db/valueType,
    :db/valueType   :db.type/valueType,
    :db/cardinality :db.cardinality/one,
    :db/doc         "An attribute's value type"}
   {:db/ident       :db/cardinality,
    :db/valueType   :db.type/cardinality,
    :db/cardinality :db.cardinality/one,
    :db/doc         "An attribute's cardinality"}
   {:db/ident       :db/doc,
    :db/valueType   :db.type/string,
    :db/cardinality :db.cardinality/one,
    :db/doc         "An attribute's documentation"}
   {:db/ident       :db/index,
    :db/valueType   :db.type/boolean,
    :db/cardinality :db.cardinality/one,
    :db/doc         "An attribute's index selection"}
   {:db/ident       :db/unique,
    :db/valueType   :db.type/unique,
    :db/cardinality :db.cardinality/one,
    :db/doc         "An attribute's unique selection"}
   {:db/ident       :db/noHistory,
    :db/valueType   :db.type/boolean,
    :db/cardinality :db.cardinality/one,
    :db/doc         "An attribute's history selection"}
   {:db/ident       :db.install/attribute,
    :db/valueType   :db.type.install/attribute,
    :db/cardinality :db.cardinality/one,
    :db/doc         "Only for interoperability with Datomic"}
   {:db/ident       :db/txInstant,
    :db/valueType   :db.type/instant,
    :db/cardinality :db.cardinality/one,
    :db/doc         "A transaction's time-point",
    :db/unique      :db.unique/identity,
    :db/index       true}])


(def ^:const system-schema
  (conj (vec (apply concat (map-indexed (fn [eid entity]
                                      (map (fn [[a v]] [(inc eid) a v tx0-sys true]) entity)) ; inc necessary?
                                        complete-implicit-schema)))
        [tx0-sys :db/txInstant #inst "1970-01-01T00:00:00.000-00:00" tx0-sys true]))


(def ^:const e0 (inc (+ e0-sys (count complete-implicit-schema))))
(def ^:const tx0 (inc tx0-sys))

;; for refs: add enums, then replacements

(def ^:const system-entities (set (range (inc (count complete-implicit-schema)))))
