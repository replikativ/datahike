(ns ^:no-doc datahike.index.hitchhiker-tree
  (:require [datahike.index.hitchhiker-tree.upsert :as ups]
            [datahike.index.hitchhiker-tree.insert :as ins]
            [datahike.index.utils :as diu]
            [hitchhiker.tree.bootstrap.konserve :as hk]
            [hitchhiker.tree.utils.async :as async]
            [hitchhiker.tree.messaging :as hmsg]
            [hitchhiker.tree.key-compare :as kc]
            [hitchhiker.tree :as tree]
            [datahike.array :refer [compare-arrays]]
            [datahike.datom :as dd]
            [datahike.constants :refer [e0 tx0 emax txmax]]
            [clojure.spec.alpha :as s]
            [hasch.core :as h]
            [datahike.index.interface :as di :refer [IIndex]])
  #?(:clj (:import [clojure.lang AMapEntry]
                   [hitchhiker.tree DataNode IndexNode]
                   [datahike.datom Datom])))

(def ^:const default-index-b-factor 17)
(def ^:const default-index-data-node-size 300)
(def ^:const default-index-log-size (- 300 17))

(extend-protocol kc/IKeyCompare
  clojure.lang.PersistentVector
  (-compare [key1 key2]
    (if-not (= (class key2) clojure.lang.PersistentVector)
      (if (nil? key2)
        +1    ;; Case for tuples. E.g. (compare [100 200] nil)
        -1)   ;; HACK for nil
      (let [[a b c d] key1
            [e f g h] key2]
        (dd/combine-cmp
         (kc/-compare a e)
         (kc/-compare b f)
         (kc/-compare c g)
         (kc/-compare d h)))))
  java.lang.String
  (-compare [key1 key2]
    (compare key1 key2))
  clojure.lang.Keyword
  (-compare [key1 key2]
    (compare key1 key2))
  nil
  (-compare [key1 key2]
    (if (nil? key2)
      0 -1)))

(extend-protocol kc/IKeyCompare
  (Class/forName "[B")
  (-compare [key1 key2]
    (compare-arrays key1 key2)))

(defn- index-type->datom-fn [index-type]
  (case index-type
    :aevt (fn [a e v tx] (dd/datom e a v tx true))
    :avet (fn [a v e tx] (dd/datom e a v tx true))
    (fn [e a v tx] (dd/datom e a v tx true))))

(defn slice [tree from to index-type]
  (let [[a b c d] (diu/datom-to-vec from index-type true)
        cmp (diu/prefix-scan kc/-compare (diu/datom-to-vec to index-type false))
        xf (comp
            (take-while (fn [^AMapEntry kv] (cmp (.key kv))))
            (map (fn [kv]
                   (let [[a b c d] (.key ^AMapEntry kv)]
                     ((index-type->datom-fn index-type) a b c d)))))]
    (->> (sequence xf (hmsg/lookup-fwd-iter tree [a b c d]))
         seq)))

(defn datom-seq [tree index-type]
  (slice tree (dd/datom e0 nil nil tx0) (dd/datom emax nil nil txmax) index-type))

(defn datom-count [tree index-type]
  (count (datom-seq tree index-type)))

(defn all-datoms [tree index-type]
  (map
   #(apply
     (index-type->datom-fn index-type)
     (first %))
   (hmsg/lookup-fwd-iter tree [])))

(defn- datom->node [^Datom datom index-type]
  (case index-type
    :aevt [(.-a datom) (.-e datom) (.-v datom) (.-tx datom)]
    :avet [(.-a datom) (.-v datom) (.-e datom) (.-tx datom)]
    :eavt [(.-e datom) (.-a datom) (.-v datom) (.-tx datom)]
    (throw (IllegalArgumentException. (str "Unknown index-type: " index-type)))))

(defn- index-type->indices [index-type]
  (case index-type
    :eavt [0 1]
    :aevt [0 1]
    :avet [0 2]
    (throw (UnsupportedOperationException. "Unknown index type: " index-type))))

(defn insert [tree ^Datom datom index-type op-count]
  (let [datom-as-vec (datom->node datom index-type)]
    (async/<?? (hmsg/enqueue tree [(assoc (ins/new-InsertOp datom-as-vec op-count)
                                          :tag (h/uuid))]))))

(defn temporal-insert [tree ^Datom datom index-type op-count]
  (let [datom-as-vec (datom->node datom index-type)]
    (async/<?? (hmsg/enqueue tree [(assoc (ins/new-temporal-InsertOp datom-as-vec op-count)
                                          :tag (h/uuid))]))))

(defn upsert [tree ^Datom datom index-type op-count]
  (let [datom-as-vec (datom->node datom index-type)]
    (async/<?? (hmsg/enqueue tree [(assoc (ups/new-UpsertOp datom-as-vec op-count (index-type->indices index-type))
                                          :tag (h/uuid))]))))

(defn temporal-upsert [tree ^Datom datom index-type op-count]
  (let [datom-as-vec (datom->node datom index-type)]
    (async/<?? (hmsg/enqueue tree [(assoc (ups/new-temporal-UpsertOp datom-as-vec op-count (index-type->indices index-type))
                                          :tag (h/uuid))]))))

(defn remove-datom [tree ^Datom datom index-type op-count]
  (async/<?? (hmsg/delete tree (datom->node datom index-type) op-count)))

(defn flush-tree [tree backend]
  (:tree (async/<?? (tree/flush-tree-without-root tree backend))))

(extend-type DataNode
  IIndex
  (-all [eavt-tree]
    (all-datoms eavt-tree :eavt))
  (-seq [eavt-tree]
    (datom-seq eavt-tree :eavt))
  (-count [eavt-tree]
    (datom-count eavt-tree :eavt))
  (-insert [tree datom index-type op-count]
    (insert tree datom index-type op-count))
  (-temporal-insert [tree datom index-type op-count]
    (temporal-insert tree datom index-type op-count))
  (-upsert [tree datom index-type op-count]
    (upsert tree datom index-type op-count))
  (-temporal-upsert [tree datom index-type op-count]
    (temporal-upsert tree datom index-type op-count))
  (-remove [tree datom index-type op-count]
    (remove-datom tree datom index-type op-count))
  (-slice [tree from to index-type]
    (slice tree from to index-type))
  (-flush [tree backend]
    (flush-tree tree backend))
  (-transient [tree]
    (identity tree))
  (-persistent! [tree]
    (identity tree)))

(extend-type IndexNode
  IIndex
  (-all [eavt-tree]
    (all-datoms eavt-tree :eavt))
  (-seq [eavt-tree]
    (datom-seq eavt-tree :eavt))
  (-count [eavt-tree]
    (datom-count eavt-tree :eavt))
  (-insert [tree datom index-type op-count]
    (insert tree datom index-type op-count))
  (-temporal-insert [index datom index-type op-count]
    (temporal-insert index datom index-type op-count))
  (-upsert [tree datom index-type op-count]
    (upsert tree datom index-type op-count))
  (-temporal-upsert [tree datom index-type op-count]
    (temporal-upsert tree datom index-type op-count))
  (-remove [tree datom index-type op-count]
    (remove-datom tree datom index-type op-count))
  (-slice [tree from to index-type]
    (slice tree from to index-type))
  (-flush [tree backend]
    (flush-tree tree backend))
  (-transient [tree]
    (identity tree))
  (-persistent! [tree]
    (identity tree)))

(defn empty-tree [b-factor data-node-size log-size]
  (async/<?? (tree/b-tree (tree/->Config b-factor data-node-size log-size))))

(defmethod di/empty-index :datahike.index/hitchhiker-tree
  [_index-name _store _index-type {:keys [index-b-factor index-data-node-size index-log-size]}]
  (empty-tree index-b-factor index-data-node-size index-log-size))

(defmethod di/init-index :datahike.index/hitchhiker-tree
  [_index-name _store datoms index-type op-count {:keys [index-b-factor index-data-node-size index-log-size]}]
  (async/<??
   (async/reduce<
    (fn [tree [idx datom]]
      (insert tree datom index-type (+ idx op-count)))
    (empty-tree index-b-factor index-data-node-size index-log-size)
    (map-indexed (fn [idx datom] [idx datom]) (seq datoms)))))

(defmethod di/add-konserve-handlers :datahike.index/hitchhiker-tree [_index-name store]
  (ups/add-upsert-handler
   (ins/add-insert-handler
    (hk/add-hitchhiker-tree-handlers
     store))))

(defmethod di/konserve-backend :datahike.index/hitchhiker-tree [_index-name store]
  (hk/->KonserveBackend store))


(s/def ::index-b-factor long)
(s/def ::index-log-size long)
(s/def ::index-data-node-size long)

(defmethod di/default-index-config :datahike.index/hitchhiker-tree [_index-name]
  {:index-b-factor       default-index-b-factor
   :index-log-size       default-index-log-size
   :index-data-node-size default-index-data-node-size})
