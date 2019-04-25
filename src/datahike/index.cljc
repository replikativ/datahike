(ns datahike.index
  (:require [datahike.index.hitchhiker-tree :as dih])
  #?(:clj (:import [hitchhiker.tree.core DataNode IndexNode])))

(defmulti -slice "Slices index between from and to"
          {:arglists '([index from to index-type])}
          (fn [index from to index-type]
            (class index)))

(defmethod -slice DataNode [tree from to index-type]
  (dih/-slice tree from to index-type))

(defmethod -slice IndexNode [tree from to index-type]
  (dih/-slice tree from to index-type))

(defmulti -seq "Retrieves sequence of eavt datoms from index"
          {:arglists '([index index-type])}
           (fn [index index-type]
             (class index)))

(defmethod -seq DataNode [tree index-type]
  (dih/-seq tree index-type))

(defmethod -seq IndexNode [tree index-type]
  (dih/-seq tree index-type))

(defmulti -count "Retrieves sequence of eavt datoms from index"
          {:arglists '([index index-type])}
          (fn [index index-type]
            (class index)))

(defmethod -count DataNode [tree index-type]
  (dih/-count tree index-type))

(defmethod -seq IndexNode [tree index-type]
  (dih/-seq tree index-type))
