(ns hitchhiker.tree.codec.nippy
  (:require
   [hitchhiker.tree :as tree]
   [hitchhiker.tree.node :as n]
   #?(:clj [taoensso.nippy :as nippy])))


;; TODO share with konserve
(declare encode)

(defn nilify
  [m ks]
  (reduce (fn [m k] (assoc m k nil))
          m
          ks))

(defn encode-index-node
  [node]
  (-> node
      (nilify [:storage-addr :*last-key-cache])
      (assoc :children (mapv encode (:children node)))))

(defn encode-data-node
  [node]
  (nilify node
          [:storage-addr
           :*last-key-cache]))

(defn encode-address
  [node]
  (nilify node
          [:store
           :storage-addr]))

(defn encode
  [node]
  (cond
    (tree/index-node? node) (encode-index-node node)
    (tree/data-node? node) (encode-data-node node)
    (n/address? node) (encode-address node)
    :else node))


(defonce install*
  (delay
   #?@(:clj [(nippy/extend-freeze hitchhiker.tree.IndexNode :b-tree/index-node
                                  [node data-output]
                                  (nippy/freeze-to-out! data-output (into {} (encode node))))

             (nippy/extend-thaw :b-tree/index-node
                                [data-input]
                                (tree/map->IndexNode (nippy/thaw-from-in! data-input)))

             (nippy/extend-freeze hitchhiker.tree.DataNode :b-tree/data-node
                                  [node data-output]
                                  (nippy/freeze-to-out! data-output (into {} (encode node))))

             (nippy/extend-thaw :b-tree/data-node
                                [data-input]
                                (tree/map->DataNode (nippy/thaw-from-in! data-input)))])))

(defn ensure-installed!
  []
  @install*)
