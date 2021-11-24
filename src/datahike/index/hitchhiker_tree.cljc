(ns ^:no-doc datahike.index.hitchhiker-tree
  (:require [datahike.index.hitchhiker-tree.upsert :as ups]
            [datahike.index.hitchhiker-tree.insert :as ins]
            [hitchhiker.tree.utils.async :as async]
            [hitchhiker.tree.messaging :as hmsg]
            [hitchhiker.tree.key-compare :as kc]
            [hitchhiker.tree :as tree]
            [datahike.array :refer [compare-arrays]]
            [datahike.datom :as dd]
            [datahike.constants :refer [e0 tx0 emax txmax]]
            [hasch.core :as h])
  #?(:clj (:import [clojure.lang AMapEntry]
                   [datahike.datom Datom])))

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

(defn- from-datom [^Datom datom index-type start?]
  (let [e (fn [datom] (when-not (or (and start? (= e0 (.-e datom)))
                                    (and (not start?) (= emax (.-e datom))))
                        (.-e datom)))
        tx (fn [datom] (when-not (or (and start? (= tx0 (.-tx datom)))
                                     (and (not start?) (= txmax (.-tx datom))))
                         (.-tx datom)))
        datom-seq (case index-type
                    :aevt (list (.-a datom) (e datom) (.-v datom) (tx datom))
                    :avet (list (.-a datom) (.-v datom) (e datom) (tx datom))
                    (list (e datom) (.-a datom) (.-v datom) (tx datom)))]
    (->> datom-seq
         (take-while some?)
         vec)))

(defn -slice
  [tree from to index-type]
  (let [create-datom (index-type->datom-fn index-type)
        [a b c d] (from-datom from index-type true)
        [e f g h] (from-datom to index-type false)
        xf (comp
            (take-while (fn [^AMapEntry kv]
                           ;; prefix scan
                          (let [key (.key kv)
                                [i j k l] key
                                new (not (cond (and e f g h)
                                               (or (> (kc/-compare i e) 0)
                                                   (> (kc/-compare j f) 0)
                                                   (> (kc/-compare k g) 0)
                                                   (> (kc/-compare l h) 0))

                                               (and e f g)
                                               (or (> (kc/-compare i e) 0)
                                                   (> (kc/-compare j f) 0)
                                                   (> (kc/-compare k g) 0))

                                               (and e f)
                                               (or (> (kc/-compare i e) 0)
                                                   (> (kc/-compare j f) 0))

                                               e
                                               (> (kc/-compare i e) 0)

                                               :else false))]
                            new)))
            (map (fn [kv]
                   (let [[a b c d] (.key ^AMapEntry kv)]
                     (create-datom a b c d)))))
        new (->> (sequence xf (hmsg/lookup-fwd-iter tree [a b c d]))
                 seq)]
    new))

(defn -seq [tree index-type]
  (-slice tree (dd/datom e0 nil nil tx0) (dd/datom emax nil nil txmax) index-type))

(defn -count [tree index-type]
  (count (-seq tree index-type)))

(defn -all [tree index-type]
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

(defn -insert [tree ^Datom datom index-type op-count]
  (let [datom-as-vec (datom->node datom index-type)]
    (async/<?? (hmsg/enqueue tree [(assoc (ins/new-InsertOp datom-as-vec op-count)
                                          :tag (h/uuid))]))))

(defn -temporal-insert [tree ^Datom datom index-type op-count]
  (let [datom-as-vec (datom->node datom index-type)]
    (async/<?? (hmsg/enqueue tree [(assoc (ins/new-temporal-InsertOp datom-as-vec op-count)
                                          :tag (h/uuid))]))))

(defn -upsert [tree ^Datom datom index-type op-count]
  (let [datom-as-vec (datom->node datom index-type)]
    (async/<?? (hmsg/enqueue tree [(assoc (ups/new-UpsertOp datom-as-vec op-count (index-type->indices index-type))
                                          :tag (h/uuid))]))))

(defn -temporal-upsert [tree ^Datom datom index-type op-count]
  (let [datom-as-vec (datom->node datom index-type)]
    (async/<?? (hmsg/enqueue tree [(assoc (ups/new-temporal-UpsertOp datom-as-vec op-count (index-type->indices index-type))
                                          :tag (h/uuid))]))))

(defn -remove [tree ^Datom datom index-type op-count]
  (async/<?? (hmsg/delete tree (datom->node datom index-type) op-count)))

(defn -flush [tree backend]
  (:tree (async/<?? (tree/flush-tree-without-root tree backend))))

(def -persistent! identity)

(def -transient identity)

;; Functions used in multimethods defined in index.cljc

(defn empty-tree
  "Create empty hitchhiker tree"
  [b-factor data-node-size log-size]
  (async/<?? (tree/b-tree (tree/->Config b-factor data-node-size log-size))))

(defn init-tree
  "Create tree with datoms"
  [datoms index-type op-count b-factor data-node-size log-size]
  (async/<??
   (async/reduce<
    (fn [tree [idx datom]]
      (-insert tree datom index-type (+ idx op-count)))
    (empty-tree b-factor data-node-size log-size)
    (map-indexed (fn [idx datom] [idx datom]) (seq datoms)))))
