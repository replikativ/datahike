(ns ^:no-doc datahike.value-type.fressian
  "Fressian adapters for registered Datahike scalar value types.

   The public registry is codec-neutral.  This namespace snapshots it while a
   store connection is assembled and turns each descriptor into the handler
   shape expected by konserve's Fressian serializer."
  (:require [datahike.value-type :as vt]
            #?(:cljs [fress.api :as fress]))
  #?(:clj (:import [org.fressian.handlers ReadHandler WriteHandler])))

(def ^:private tag-prefix "datahike.value/")

(defn- fressian-tag [wire-name]
  (str tag-prefix wire-name))

(defn read-handlers
  "Build Fressian read handlers from the current value-type registry.

   The wire body is one standard-data value, `[version payload]`.  Version
   dispatch deliberately belongs to the registering extension's decoder so it
   can retain every historical representation it has emitted."
  []
  (into {}
        (map (fn [[_ {:keys [wire] :as descriptor}]]
               (let [{:keys [name]} wire]
                 [(fressian-tag name)
                  #?(:clj
                     (reify ReadHandler
                       (read [_ rdr _ _]
                         (let [[version payload] (.readObject rdr)]
                           (vt/decode-value descriptor version payload))))
                     :cljs
                     (fn [rdr _ _]
                       (let [[version payload] (fress/read-object rdr)]
                         (vt/decode-value descriptor version payload))))])))
        (vt/descriptors)))

(defn write-handlers
  "Build Fressian write handlers from the current value-type registry."
  []
  (into {}
        (map (fn [[_ {:keys [type wire]}]]
               (let [{:keys [name version encode]} wire
                     tag (fressian-tag name)]
                 [type
                  #?(:clj
                     {tag
                      (reify WriteHandler
                        (write [_ writer value]
                          (.writeTag writer tag 1)
                          (.writeObject writer [version (encode value)])))}
                     :cljs
                     (fn [writer value]
                       (fress/write-tag writer tag 1)
                       (fress/write-object writer [version (encode value)])))])))
        (vt/descriptors)))
