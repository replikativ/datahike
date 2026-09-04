(ns datahike.cbor.elements
  "The Datom on the wire: the one CBOR handler both codecs share, kept apart
   from `datahike.cbor` so that a peer holding no store (the thin HTTP
   client) can read and write datoms without loading the index code."
  (:require [boring.core :as boring]
            [datahike.datom :as dd])
  #?(:clj (:import [datahike.datom Datom])))

(def ^:const datom-name "datahike.datom/Datom")

(defn install-element-handlers
  "Datom write + read. Returns a NEW registry.

  `register-tag 27` supplies the WRITE side (a Datom is a deftype, so boring
  does not emit it natively the way it does a defrecord); `register-record`
  supplies the READ side, keyed by the same name. The `nil` read-fn on
  `register-tag` is deliberate — passing one would replace boring's built-in
  tag-27 dispatch wholesale, and that dispatch is what looks the name up."
  [reg]
  (-> reg
      (boring/register-tag 27 #?(:clj Datom :cljs dd/Datom)
                           (fn [d] [datom-name (vec (seq d))])
                           nil)
      (boring/register-record datom-name dd/datom-from-reader)))
