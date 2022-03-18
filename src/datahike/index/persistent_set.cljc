(ns ^:no-doc datahike.index.persistent-set
  (:require [me.tonsky.persistent-sorted-set :as set]
            [me.tonsky.persistent-sorted-set.arrays :as arrays]
            [datahike.datom :as dd]
            [datahike.constants :refer [e0 tx0 emax txmax]]
            [datahike.index.interface :as i :refer [IIndex]])
  #?(:clj (:import [datahike.datom Datom]
                   [me.tonsky.persistent_sorted_set PersistentSortedSet])))

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

(extend-type PersistentSortedSet
  IIndex
  (-all [eavt-set]
    (identity eavt-set))
  (-seq [eavt-set]
    (seq eavt-set))
  (-count [eavt-set]
    (count eavt-set))
  (-insert [set datom index-type _op-count]
    #_(set/conj set datom (index-type->cmp-quick index-type)) ;; old version? in last version this was overwritten by function below
    (-> (or (when-let [old (first (set/slice set
                                             (dd/datom (.-e datom) (.-a datom) (.-v datom) tx0)
                                             (dd/datom (.-e datom) (.-a datom) (.-v datom) txmax)))]
              (set/disj set old (index-type->cmp-quick index-type)))
            set)
        (set/conj datom (index-type->cmp-quick index-type))))
  (-temporal-insert [set datom index-type _op-count]
    (set/conj set datom (index-type->cmp-quick index-type)))
  (-upsert [set datom index-type _op-count]
    (-> (or (when-let [old (first (set/slice set
                                             (dd/datom (.-e datom) (.-a datom) nil tx0)
                                             (dd/datom (.-e datom) (.-a datom) nil txmax)))]
              (set/disj set old (index-type->cmp-quick index-type)))
            set)
        (set/conj datom (index-type->cmp-quick index-type))))
  (-temporal-upsert [set datom index-type _op-count]
    (-> (or (when-let [old (first (set/slice set
                                             (dd/datom (.-e datom) (.-a datom) nil tx0)
                                             (dd/datom (.-e datom) (.-a datom) nil txmax)))]
              (set/conj set (dd/datom (.-e old) (.-a old) (.-v old) (.-tx old) false)
                        (index-type->cmp-quick index-type)))
            set)
        (set/conj datom (index-type->cmp-quick index-type))))
  (-remove [set datom index-type _op-count]
    (set/disj set datom (index-type->cmp-quick index-type)))
  (-slice [set from to _]
    (set/slice set from to))
  (-flush [set _]
    (identity set))
  (-transient [set]
    (transient set))
  (-persistent! [set]
    (persistent! set)))

(defmethod i/empty-index :datahike.index/persistent-set [_ index-type _]
  (set/sorted-set-by (index-type->cmp index-type)))

(defmethod i/init-index :datahike.index/persistent-set [_ datoms index-type _ {:keys [indexed]}]
  (let [arr (if (= index-type :avet)
              (let [avet-datoms (filter (fn [^Datom d] (contains? indexed (.-a d))) datoms)]
                (to-array avet-datoms))
              (cond-> datoms
                (not (arrays/array? datoms))
                (arrays/into-array)))
        _ (arrays/asort arr (index-type->cmp-quick index-type))]
    (set/from-sorted-array (index-type->cmp index-type) arr)))
