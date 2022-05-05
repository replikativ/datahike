(ns ^:no-doc datahike.index.persistent-set
  (:require [me.tonsky.persistent-sorted-set :as set]
            [me.tonsky.persistent-sorted-set.arrays :as arrays]
            [datahike.index.utils :as diu]
            [datahike.datom :as dd]
            [datahike.constants :refer [e0 tx0 emax txmax]])
  #?(:clj (:import [datahike.datom Datom])))

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

(defn index-type->cmp-quick
  ([index-type] (index-type->cmp-quick index-type true))
  ([index-type abs-txid?] (if abs-txid?
                            (case index-type
                              :aevt dd/cmp-datoms-aevt-quick
                              :avet dd/cmp-datoms-avet-quick
                              dd/cmp-datoms-eavt-quick)
                            (case index-type
                              :aevt dd/cmp-datoms-aevt-quick-raw-txid
                              :avet dd/cmp-datoms-avet-quick-raw-txid
                              dd/cmp-datoms-eavt-quick-raw-txid))))

;; Functions defined in multimethods in index.cljc

(defn empty-set [index-type]
  (set/sorted-set-by (index-type->cmp index-type)))

(defn init-set [datoms index-type indexed]
  (let [arr (if (= index-type :avet)
              (let [avet-datoms (filter (fn [^Datom d] (contains? indexed (.-a d))) datoms)]
                (to-array avet-datoms))
              (cond-> datoms
                (not (arrays/array? datoms))
                (arrays/into-array)))
        _ (arrays/asort arr (index-type->cmp-quick index-type))]
    (set/from-sorted-array (index-type->cmp index-type) arr)))

(defn -remove [set datom index-type]
  (set/disj set datom (index-type->cmp-quick index-type)))

(defn -slice [set from to index-type]
  (let [cmp (diu/prefix-scan compare (diu/datom-to-vec to index-type false))]
    (->> (take-while (fn [^Datom d] (cmp (diu/datom-to-vec d index-type false)))
                     (set/slice set from nil))
         seq)))

(defn -insert [set datom index-type]
  (if (-slice set
              (dd/datom (.-e datom) (.-a datom) (.-v datom) tx0)
              (dd/datom (.-e datom) (.-a datom) (.-v datom) txmax)
              index-type)
    set
    (set/conj set datom (index-type->cmp-quick index-type))))

(defn -temporal-insert [set datom index-type]
  (set/conj set datom (index-type->cmp-quick index-type)))

(defn -upsert [set datom index-type]
  (-> (or (when-let [old (first (-slice set
                                        (dd/datom (.-e datom) (.-a datom) nil tx0)
                                        (dd/datom (.-e datom) (.-a datom) nil txmax)
                                        index-type))]
            (-remove set old index-type))
          set)
      (set/conj datom (index-type->cmp-quick index-type))))

(defn -temporal-upsert [set datom index-type]
  (if-let [old (first (-slice set
                              (dd/datom (.-e datom) (.-a datom) nil tx0)
                              (dd/datom (.-e datom) (.-a datom) nil txmax)
                              index-type))]
    (if (diu/equals-on-indices? datom old [0 1 2])
      set
      (-> (set/conj set (dd/datom (.-e old) (.-a old) (.-v old) (.-tx old) false)
                    (index-type->cmp-quick index-type false))
          (set/conj datom (index-type->cmp-quick index-type))))
    (set/conj set datom (index-type->cmp-quick index-type))))
