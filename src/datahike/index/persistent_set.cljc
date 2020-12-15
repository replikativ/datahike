(ns datahike.index.persistent-set
  ;(:refer-clojure :exlude [seq count flush persistent!])
  (:refer-clojure :exclude [-persistent! -flush -seq -count])
  (:require [me.tonsky.persistent-sorted-set :as set]
            [me.tonsky.persistent-sorted-set.arrays :as arrays]
            [datahike.datom :as dd])

  #?(:clj (:import [datahike.datom Datom])))

(def -slice set/slice)

(def -seq seq)

(def -count count)

(def -all identity)

(def -flush identity)

(def -transient transient)

(def -persistent! persistent!)

(defn index-type->cmp [index-type]
  (case index-type
    :aevt dd/cmp-datoms-aevt
    :avet dd/cmp-datoms-avet
    dd/cmp-datoms-eavt))

(defn index-type->cmp-quick [index-type]
  (case index-type
    :aevt dd/cmp-datoms-aevt-quick
    :avet dd/cmp-datoms-avet-quick
    dd/cmp-datoms-eavt-quick))

(defn empty-set [index-type]
  (set/sorted-set-by (index-type->cmp index-type)))

(defn init-set [datoms indexed index-type]
  (let [arr (if (= index-type :avet)
              (let [avet-datoms (filter (fn [^Datom d] (contains? indexed (.-a d))) datoms)]
                (to-array avet-datoms))
              (cond-> datoms
                (not (arrays/array? datoms))
                (arrays/into-array)))
        _ (arrays/asort arr (index-type->cmp-quick index-type))]
    (set/from-sorted-array (index-type->cmp index-type) arr)))

(defn -insert [set datom index-type]
  (set/conj set datom (index-type->cmp-quick index-type)))

(defn -remove [set datom index-type]
  (set/disj set datom (index-type->cmp-quick index-type)))
