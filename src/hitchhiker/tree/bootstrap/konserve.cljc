(ns hitchhiker.tree.bootstrap.konserve
  (:refer-clojure :exclude [subvec])
  (:require
   [konserve.cache :as k]
   [hasch.core :as h]
   [hitchhiker.tree.messaging :as msg]
   [hitchhiker.tree :as tree]
   [hitchhiker.tree.node :as n]
   [hitchhiker.tree.backend :as b]
   [hitchhiker.tree.key-compare :as c]
   [hitchhiker.tree.utils.async :as ha]
   #?@(:clj [[clojure.core.async :as async]
             [clojure.core.cache :as cache]]
       :cljs [[cljs.core.async :include-macros true :as async]
              [cljs.cache :as cache]])))

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

(defn synthesize-storage-address
  "Given a key, returns a promise containing that key for use as a storage-addr"
  [key]
  (ha/promise-chan key))

(defrecord KonserveAddr [store last-key konserve-key storage-addr]
  n/INode
  (-last-key [_] last-key)

  n/IAddress
  (-dirty? [_] false)
  (-dirty! [this] this)

  (-resolve-chan [this]
    (ha/go-try
     (let [cache (:cache store)]
       (if-let [v (cache/lookup @cache konserve-key)]
         (do
           (swap! cache cache/hit konserve-key)
           (assoc v :storage-addr (synthesize-storage-address konserve-key)))
         (let [ch (k/get-in store [konserve-key])]
           (assoc (ha/if-async?
                   (ha/<? ch)
                   (async/<!! ch))
                  :storage-addr (synthesize-storage-address konserve-key))))))))

(defn konserve-addr
  [store last-key konserve-key]
  (->KonserveAddr store
                  last-key
                  konserve-key
                  (ha/promise-chan konserve-key)))

(defrecord KonserveBackend [store]
  b/IBackend
  (-new-session [_] (atom {:writes 0 :deletes 0}))
  (-anchor-root [_ {:keys [konserve-key] :as node}]
    node)
  (-write-node [_ node session]
    (ha/go-try
     (swap! session update-in [:writes] inc)
     (let [pnode (encode node)
           id (h/uuid pnode)
           ch (k/assoc-in store [id] node)]
       (ha/<? ch)
       (konserve-addr store
                      (n/-last-key node)
                      id))))
  (-delete-addr [_ addr session]
    (swap! session update :deletes inc)))

(defn get-root-key
  [tree]
  (or
   (-> tree :storage-addr (async/poll!) :konserve-key)
   (-> tree :storage-addr (async/poll!))))

(defn create-tree-from-root-key
  [store root-key]
  (ha/go-try
   (let [ch (k/get-in store [root-key])
         val (ha/if-async?
              (ha/<? ch)
              (async/<!! ch))
         ;; need last key to bootstrap
            last-key (n/-last-key (assoc val :storage-addr (synthesize-storage-address root-key)))]
        (ha/<? (n/-resolve-chan (konserve-addr store
                                               last-key
                                               root-key))))))

(defn add-hitchhiker-tree-handlers [store]
  ;; TODO check whether store is using nippy in the future and load on the fly:
  #_[hitchhiker.tree.codec.nippy :as nippy]
  #_(nippy/ensure-installed!)
  (swap! (:read-handlers store)
         merge
         {'hitchhiker.tree.bootstrap.konserve.KonserveAddr
          (fn [{:keys [last-key konserve-key]}]
            (konserve-addr store
                           last-key
                           konserve-key))
          'hitchhiker.tree.DataNode
          (fn [{:keys [children cfg] :as d}]
            (tree/data-node (into (sorted-map-by c/-compare)
                                  children)
                            cfg))
          'hitchhiker.tree.IndexNode
          (fn [{:keys [children cfg op-buf]}]
            (tree/index-node (vec children)
                             (vec op-buf)
                             cfg))
          'hitchhiker.tree.messaging.InsertOp
          msg/map->InsertOp
          'hitchhiker.tree.messaging.DeleteOp
          msg/map->DeleteOp
          'hitchhiker.tree.Config
          tree/map->Config

          ;; support pre-refactoring 0.1.5 hitchhiker-tree record names
          'hitchhiker.konserve.KonserveAddr
          (fn [{:keys [last-key konserve-key]}]
            (konserve-addr store
                           last-key
                           konserve-key))
          'hitchhiker.tree.core.DataNode
          (fn [{:keys [children cfg] :as d}]
            (tree/data-node (into (sorted-map-by c/-compare)
                                  children)
                            cfg))
          'hitchhiker.tree.core.IndexNode
          (fn [{:keys [children cfg op-buf]}]
            (tree/index-node (vec children)
                             (vec op-buf)
                             cfg))
          'hitchhiker.tree.core.Config
          tree/map->Config

          })
  (swap! (:write-handlers store)
         merge
         {'hitchhiker.tree.bootstrap.konserve.KonserveAddr
          encode-address
          'hitchhiker.tree.DataNode
          encode-data-node
          'hitchhiker.tree.IndexNode
          encode-index-node})
  store)
