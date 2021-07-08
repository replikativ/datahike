(ns ^:no-doc datahike.index
  #?(:cljs (:refer-clojure :exclude [-seq -count -remove -flush -transient -persistent!]))
  (:require [datahike.index.hitchhiker-tree :as dih]
            [datahike.index.persistent-set :as dip]
            [hitchhiker.tree]
            [me.tonsky.persistent-sorted-set]))

;; TODO add doc to each function
(defprotocol IIndex
  (-all [index])
  (-seq [index])
  (-count [index])
  (-insert [index datom index-type op-count])
  (-upsert [index datom index-type op-count])
  (-temporal-upsert [index datom index-type op-count])
  (-remove [index datom index-type op-count])
  (-slice [index from to index-type])
  (-flush [index backend])
  (-transient [index])
  (-persistent! [index]))

(extend-type hitchhiker.tree.DataNode
  IIndex
  (-all [eavt-tree]
    (dih/-all eavt-tree :eavt))
  (-seq [eavt-tree]
    (dih/-seq eavt-tree :eavt))
  (-count [eavt-tree]
    (dih/-count eavt-tree :eavt))
  (-insert [tree datom index-type op-count]
    (dih/-insert tree datom index-type op-count))
  (-upsert [tree datom index-type op-count]
    (dih/-upsert tree datom index-type op-count))
  (-temporal-upsert [tree datom index-type op-count]
    (dih/-temporal-upsert tree datom index-type op-count))
  (-remove [tree datom index-type op-count]
    (dih/-remove tree datom index-type op-count))
  (-slice [tree from to index-type]
    (dih/-slice tree from to index-type))
  (-flush [tree backend]
    (dih/-flush tree backend))
  (-transient [tree]
    (dih/-transient tree))
  (-persistent! [tree]
    (dih/-persistent! tree)))

(extend-type hitchhiker.tree.IndexNode
  IIndex
  (-all [eavt-tree]
    (dih/-all eavt-tree :eavt))
  (-seq [eavt-tree]
    (dih/-seq eavt-tree :eavt))
  (-count [eavt-tree]
    (dih/-count eavt-tree :eavt))
  (-insert [tree datom index-type op-count]
    (dih/-insert tree datom index-type op-count))
  (-upsert [tree datom index-type op-count]
    (dih/-upsert tree datom index-type op-count))
  (-temporal-upsert [tree datom index-type op-count]
    (dih/-temporal-upsert tree datom index-type op-count))
  (-remove [tree datom index-type op-count]
    (dih/-remove tree datom index-type op-count))
  (-slice [tree from to index-type]
    (dih/-slice tree from to index-type))
  (-flush [tree backend]
    (dih/-flush tree backend))
  (-transient [tree]
    (dih/-transient tree))
  (-persistent! [tree]
    (dih/-persistent! tree)))

(extend-type #?(:clj me.tonsky.persistent_sorted_set.PersistentSortedSet :cljs me.tonsky.persistent-sorted-set.BTSet)
  IIndex
  (-all [eavt-set]
    (dip/-all eavt-set))
  (-seq [eavt-set]
    (dip/-seq eavt-set))
  (-count [eavt-set]
    (dip/-count eavt-set))
  (-insert [set datom index-type op-count]
    (dip/-insert set datom index-type))
  (-upsert [set datom index-type op-count]
    (dip/-upsert set datom index-type))
  (-temporal-upsert [set datom index-type op-count]
    (dip/-temporal-upsert set datom index-type))
  (-remove [set datom index-type op-count]
    (dip/-remove set datom index-type))
  (-slice [set from to _]
    (dip/-slice set from to))
  (-flush [set _]
    (dip/-flush set))
  (-transient [set]
    (dip/-transient set))
  (-persistent! [set]
    (dip/-persistent! set)))

(defmulti empty-index
  "Creates empty index"
  {:arglists '([index index-type])}
  (fn [index index-type & opts] index))

(defmethod empty-index ::hitchhiker-tree [_ _]
  (dih/empty-tree))

(defmethod empty-index ::persistent-set [_ index-type]
  (dip/empty-set index-type))

(defmulti init-index
  "Initialize index with datoms"
  {:arglists '([index datoms indexed index-type op-count])}
  (fn [index datoms indexed index-type op-count] index))

(defmethod init-index ::hitchhiker-tree [_ datoms _ index-type op-count]
  (dih/init-tree datoms index-type op-count))

(defmethod init-index ::persistent-set [_ datoms indexed index-type _]
  (dip/init-set datoms indexed index-type))
