(ns datahike.impl.libdatahike
  (:require [clojure.edn :as edn]
            [jsonista.core :as j]
            [datahike.json :as json]
            [clj-cbor.core :as cbor]
            [taoensso.timbre :as timbre])
  (:import [datahike.datom Datom])
  (:gen-class
   :methods [^{:static true} [parseJSON [String] Object]
             ^{:static true} [parseEdn [String] Object]
             ^{:static true} [parseCBOR [bytes] Object]
             ^{:static true} [toEdnString [Object] String]
             ^{:static true} [toJSONString [Object] String]
             ^{:static true} [toCBOR [Object] bytes]
             ^{:static true} [datomsToVecs [Iterable] Iterable]
             ^{:static true} [intoMap [Object] Object]]))

(timbre/set-level! :warn)

(defn -parseJSON [s]
  (j/read-value s json/mapper))

(defn -toJSONString [obj]
  (j/write-value-as-string obj json/mapper))

(defn -parseEdn [s]
  (edn/read-string s))

(defn -toEdnString [obj]
  (pr-str obj))

(defn -parseCBOR [^bytes b]
  (cbor/decode b))

(defn ^bytes -toCBOR [edn]
  (cbor/encode edn))

(defn -datomsToVecs [datoms]
  (mapv #(vec (seq ^Datom %)) datoms))

(defn -intoMap [edn]
  (into {} edn))
