(ns datahike.impl.libdatahike
  (:require [clojure.walk :as walk]
            [cheshire.core :as ch]
            [clojure.edn :as edn]
            [datahike.schema :as s]
            [datahike.api :as d]
            [taoensso.timbre :as timbre])
  (:gen-class
   :methods [^{:static true} [parseJSON [String] Object]
             ^{:static true} [parseEdn [String] Object]
             ^{:static true} [JSONAsTxData [String Object] Iterable]
             ^{:static true} [toJSONString [Object] String]
             ^{:static true} [transact [Object Iterable] Object]]))

(timbre/set-level! :warn)

(defn- filter-value-type-attrs [valtypes schema]
  (into #{} (filter #(-> % schema :db/valueType valtypes) (keys schema))))

(def ^:private filter-kw-attrs
  (partial filter-value-type-attrs #{:db.type/keyword :db.type/value :db.type/cardinality :db.type/unique}))

(def keyword-valued-schema-attrs (filter-kw-attrs s/implicit-schema-spec))

(defn- int-obj-to-long [i]
  (if (some? i)
    (long i)
    (throw (ex-info "Cannot store nil as a value"))))

(defn- xf-val [f v]
  (if (vector? v) (map f v) (f v)))

(declare handle-id-or-av-pair)

(defn- xf-ref-val [v valtype-attrs-map db]
  (if (vector? v)
    (walk/prewalk #(handle-id-or-av-pair % valtype-attrs-map db) v)
    v))

(defn keywordize-string [s]
  (if (string? s) (keyword s) s))

(defn ident-for [db a]
  (if (and (number? a) (some? db)) (.-ident-for db a) a))

(defn cond-xf-val
  [a-ident v {:keys [ref-attrs long-attrs keyword-attrs symbol-attrs] :as valtype-attrs-map} db]
  (cond
    (contains? ref-attrs a-ident) (xf-ref-val v valtype-attrs-map db)
    (contains? long-attrs a-ident) (xf-val int-obj-to-long v)
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

;; ============= exported helpers =============

(defn -parseJSON [s]
  (ch/parse-string s keyword))

(defn -parseEdn [s]
  (edn/read-string s))

(defn -JSONAsTxData [tx-data db]
  (xf-data-for-tx (ch/parse-string tx-data keyword) db))

(defn -toJSONString [edn]
  (ch/generate-string edn))

(defn -transact [conn tx-data]
  (:tx-meta (d/transact conn tx-data)))

(comment

  (-JSONAsTxData "[{\":age\": 42}]" nil)

  )
