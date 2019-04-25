(ns datahike.index
  (:require [datahike.index.hitchhiker-tree :as dih]
            [datahike.datom :as dd]
            [datahike.index.persistent-set :as dip])
  #?(:clj (:import [hitchhiker.tree.core DataNode IndexNode]
                   [me.tonsky.persistent_sorted_set PersistentSortedSet])))

(defmulti -slice
  "Slices index between from and to"
  {:arglists '([index from to index-type])}
  (fn [index from to index-type]
    (class index)))

(defmethod -slice DataNode [tree from to index-type]
  (dih/-slice tree from to index-type))

(defmethod -slice IndexNode [tree from to index-type]
  (dih/-slice tree from to index-type))

(defmethod -slice PersistentSortedSet [set from to _]
  (dip/-slice set from to))



(defmulti -seq
  "Retrieves sequence of eavt datoms from index"
  {:arglists '([index index-type])}
  (fn [index index-type]
    (class index)))

(defmethod -seq DataNode [tree index-type]
  (dih/-seq tree index-type))

(defmethod -seq IndexNode [tree index-type]
  (dih/-seq tree index-type))

(defmethod -seq PersistentSortedSet [set _]
  (dip/-seq set))



(defmulti -count
  "Counts datoms from eavt index"
  {:arglists '([index index-type])}
  (fn [index index-type]
    (class index)))

(defmethod -count DataNode [tree index-type]
  (dih/-count tree index-type))

(defmethod -count IndexNode [tree index-type]
  (dih/-count tree index-type))

(defmethod -count PersistentSortedSet [set _]
  (dip/-count set))



(defmulti -all
  "Retrieve all datoms from index"
  {:arglists '([index])}
  (fn [index] (class index)))

(defmethod -all DataNode [eavt-tree]
  (dih/-all eavt-tree :eavt))

(defmethod -all IndexNode [eavt-tree]
  (dih/-all eavt-tree :eavt))

(defmethod -all PersistentSortedSet [eavt-set]
  (dip/-all eavt-set))



(defmulti empty-index
  "Creates empty index"
  {:arglists '([backend-type index-type])}
  (fn [backend-type index-type & opts] backend-type))

(defmethod empty-index ::hitchhiker-tree [_ _]
  (dih/empty-tree))

(defmethod empty-index ::persistent-set [_ index-type]
  (dip/empty-set index-type))



(defmulti init-index
  "Initialize index with datoms"
  {:arglists '([backend-type datoms indexed index-type])}
  (fn [backend-type datoms indexed index-type] backend-type))

(defmethod init-index ::hitchhiker-tree [_ datoms _ index-type]
  (dih/init-tree datoms index-type))

(defmethod init-index ::persistent-set [_ datoms indexed index-type]
  (dip/init-set datoms indexed index-type))



(defmulti -insert
  "Insert new datom to index"
  {:arglists '([index datom index-type])}
  (fn [index datom index-type] (class index)))

(defmethod -insert DataNode [tree datom index-type]
  (dih/-insert tree datom index-type))

(defmethod -insert IndexNode [tree datom index-type]
  (dih/-insert tree datom index-type))

(defmethod -insert PersistentSortedSet [set datom index-type]
  (dip/-insert set datom index-type))



(defmulti -remove
  "Remove datom from index"
  {:arglists '([index datom index-type])}
  (fn [index datom index-type] (class index)))

(defmethod -remove DataNode [tree datom index-type]
  (dih/-remove tree datom index-type))

(defmethod -remove IndexNode [tree datom index-type]
  (dih/-remove tree datom index-type))

(defmethod -remove PersistentSortedSet [set datom index-type]
  (dip/-remove set datom index-type))
