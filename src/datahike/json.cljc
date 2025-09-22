(ns ^:no-doc datahike.json
  "JSON related translations."
  (:require [clojure.edn :as edn]
            [clojure.walk :as walk]
            [datahike.store :refer [store-identity]]
            [datahike.readers :as readers]
            [datahike.connector]
            [datahike.datom :as dd]
            [datahike.schema :as s]
            [jsonista.core :as j]
            [jsonista.tagged :as jt])
  #?(:clj
     (:import [datahike.datom Datom]
              [datahike.impl.entity Entity]
              [com.fasterxml.jackson.core JsonGenerator]
              [datahike.db HistoricalDB AsOfDB SinceDB])))

(defn config->store-id [config]
  [(store-identity (:store config))
   (:branch config)])

(defn db->map [db]
  (let [{:keys [config meta max-eid max-tx]} db]
    {:store-id  (config->store-id config)
     :commit-id (:datahike/commit-id meta)
     :max-eid   max-eid
     :max-tx    max-tx}))

(declare mapper)

(defn write-to-generator [f]
  (fn [x ^JsonGenerator gen]
    (.writeRawValue gen (j/write-value-as-string (f x) mapper))))

(def json-base-handlers
  {clojure.lang.Keyword
   {:tag    "!kw"
    :encode jt/encode-keyword
    :decode keyword}

   clojure.lang.Symbol
   {:tag    "!sym"
    :encode jt/encode-str
    :decode symbol}

   clojure.lang.PersistentHashSet
   {:tag    "!set"
    :encode jt/encode-collection
    :decode set}

   java.util.UUID
   {:tag    "!uuid"
    :encode jt/encode-str
    :decode #(java.util.UUID/fromString %)}

   java.util.Date
   {:tag    "!date"
    :encode #(jt/encode-str (.getTime ^java.util.Date %1) %2)
    :decode #(java.util.Date. (Long/parseLong %))}})

(def mapper-opts
  {:encode-key-fn true
   :decode-key-fn true
   :modules       [(jt/module
                    {:handlers
                     (merge
                      json-base-handlers
                      {datahike.connector.Connection
                       {:tag    "!datahike/Connection"
                        :encode (write-to-generator #(config->store-id (:config @(:wrapped-atom %))))
                        :decode datahike.readers/connection-from-reader}

                       datahike.datom.Datom
                       {:tag    "!datahike/Datom"
                        :encode (write-to-generator
                                 (fn [^Datom d]
                                   [(.-e d) (.-a d) (.-v d) (dd/datom-tx d) (dd/datom-added d)]))
                        :decode datahike.readers/datom-from-reader}

                       datahike.db.TxReport
                       {:tag    "!datahike/TxReport"
                        :encode (write-to-generator #(into {} %))
                        :decode datahike.db/map->TxReport}

                       datahike.db.DB
                       {:tag    "!datahike/DB"
                        :encode (write-to-generator db->map)
                        :decode datahike.readers/db-from-reader}

                       datahike.db.HistoricalDB
                       {:tag    "!datahike/HistoricalDB"
                        :encode (write-to-generator
                                 (fn [{:keys [origin-db]}]
                                   {:origin origin-db}))
                        :decode datahike.readers/history-from-reader}

                       datahike.db.SinceDB
                       {:tag    "!datahike/SinceDB"
                        :encode (write-to-generator
                                 (fn [{:keys [origin-db time-point]}]
                                   {:origin     origin-db
                                    :time-point time-point}))
                        :decode datahike.readers/since-from-reader}

                       datahike.db.AsOfDB
                       {:tag    "!datahike/AsOfDB"
                        :encode (write-to-generator
                                 (fn [{:keys [origin-db time-point]}]
                                   {:origin     origin-db
                                    :time-point time-point}))
                        :decode datahike.readers/as-of-from-reader}

                       datahike.impl.entity.Entity
                       {:tag    "!datahike/Entity"
                        :encode (write-to-generator
                                 (fn [^Entity e]
                                   (assoc (into {} e)
                                          :db (.-db e)
                                          :eid (.-eid e))))
                        :decode datahike.readers/entity-from-reader}})})]})

(def mapper (j/object-mapper mapper-opts))

;; import from datahike-server for JSON transact support with this code you can
;; pass normal JSON maps or arrays of Datoms and strings will be automatically
;; converted to keywords depending on their position

(def number-re #"\d+(\.\d+)?")
(def number-format-instance (java.text.NumberFormat/getInstance))

(defn- filter-value-type-attrs [valtypes schema]
  (into #{} (filter #(-> % schema :db/valueType valtypes) (keys schema))))

(def ^:private filter-kw-attrs
  (partial filter-value-type-attrs #{:db.type/keyword :db.type/value :db.type/cardinality :db.type/unique}))

(def keyword-valued-schema-attrs (filter-kw-attrs s/implicit-schema-spec))

(defn- xf-val [f v]
  (if (vector? v) (map f v) (f v)))

(declare handle-id-or-av-pair)

(defn- xf-ref-val [v valtype-attrs-map db]
  (if (vector? v)
    (walk/prewalk #(handle-id-or-av-pair % valtype-attrs-map db) v)
    v))

(defn keywordize-string [s]
  (if (string? s) (keyword s) s))

(defn ident-for [^datahike.db.DB db a]
  (if (and (number? a) (some? db)) (.-ident-for db a) a))

(defn cond-xf-val
  [a-ident v {:keys [ref-attrs long-attrs keyword-attrs symbol-attrs] :as valtype-attrs-map} db]
  (cond
    (contains? ref-attrs a-ident) (xf-ref-val v valtype-attrs-map db)
    (contains? long-attrs a-ident) (xf-val long v)
    (contains? keyword-attrs a-ident) (xf-val keyword v)
    (contains? symbol-attrs a-ident) (xf-val symbol v)
    :else v))

(defn handle-id-or-av-pair
  ([v valtype-attrs-map]
   (handle-id-or-av-pair v valtype-attrs-map nil))
  ([v valtype-attrs-map db]
   (if (and (vector? v) (= (count v) 2))
     (let [a (keywordize-string (first v))]
       [a (cond-xf-val (ident-for db a) (nth v 1) valtype-attrs-map db)])
     v)))

(defn- xf-tx-data-map [m valtype-attrs-map db]
  (into {}
        (map (fn [[a v]]
               [a (if (= :db/id a)
                    (handle-id-or-av-pair [a v] valtype-attrs-map db)
                    (cond-xf-val a v valtype-attrs-map db))])
             m)))

(defn- xf-tx-data-vec [tx-vec valtype-attrs-map db]
  (let [op (first tx-vec)
        [e a v] (rest tx-vec)
        a (keywordize-string a)]
    (vec (filter some? (list (keyword op)
                             (handle-id-or-av-pair e valtype-attrs-map db)
                             a
                             (cond-xf-val (ident-for db a) v valtype-attrs-map db))))))

(defn get-valtype-attrs-map [schema]
  (let [ref-valued-attrs (filter-value-type-attrs #{:db.type/ref} schema)
        long-valued-attrs (filter-value-type-attrs #{:db.type/long} schema)
        kw-valued-attrs (clojure.set/union keyword-valued-schema-attrs (filter-kw-attrs schema))
        sym-valued-attrs (filter-value-type-attrs #{:db.type/symbol} schema)]
    {:ref-attrs ref-valued-attrs
     :long-attrs long-valued-attrs
     :keyword-attrs kw-valued-attrs
     :symbol-attrs sym-valued-attrs}))

(defn xf-data-for-tx [tx-data db]
  (let [valtype-attrs-map (get-valtype-attrs-map (:schema db))]
    (map #(let [xf-fn (cond (map? %) xf-tx-data-map
                            (vector? %) xf-tx-data-vec
                            ; Q: Is this error appropriate?
                            :else (throw (ex-info "Only maps and vectors allowed in :tx-data and :tx-meta"
                                                  {:event :handlers/transact :data tx-data})))]
            (xf-fn % valtype-attrs-map db))
         tx-data)))

