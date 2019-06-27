(ns datahike.store
  (:require [hitchhiker.konserve :as kons]
            [konserve.filestore :as fs]
            [konserve-leveldb.core :as kl]
            [konserve.memory :as mem]
            [superv.async :refer [<?? S]]))

(defmulti empty-store
  "Creates an empty store"
  {:arglists '([store-scheme path])}
  (fn [store-scheme path] store-scheme))

(defmethod empty-store :default [store-scheme _]
  (throw (IllegalArgumentException. (str "Can't create a store with scheme: " store-scheme))))

(defmulti delete-store
  "Deletes an existing store"
  {:arglists '([store-scheme path])}
  (fn [store-scheme path] store-scheme))

(defmethod delete-store :default [store-scheme _]
  (throw (IllegalArgumentException. (str "Can't delete a store with scheme: " store-scheme))))

(defmulti connect-store
  "Makes a connection to an existing store"
  {:arglists '([store-scheme path])}
  (fn [store-scheme store] store-scheme))

(defmethod connect-store :default [store-scheme _]
  (throw (IllegalArgumentException. (str "Can't connect to store with scheme: " store-scheme))))

(defmulti release-store
  "Releases the connection to an existing store (optional)."
  {:arglists '([store-scheme store])}
  (fn [store-scheme store] store-scheme))

(defmethod release-store :default [_ _]
  nil)

(defmulti scheme->index
  "Returns the index type to use for this store"
  {:arglists '([store-scheme])}
  (fn [store-scheme] store-scheme))

;; memory

(def memory (atom {}))

(defmethod empty-store "mem" [_ uri]
  (let [store (<?? S (mem/new-mem-store))]
    (swap! memory assoc uri store)
    store))

(defmethod delete-store "mem" [_ uri]
  (swap! memory dissoc uri))

(defmethod connect-store "mem" [_ uri]
  (@memory uri))

(defmethod scheme->index "mem" [_]
  :datahike.index/persistent-set)

;; file

(defmethod empty-store "file" [_ uri]
  (kons/add-hitchhiker-tree-handlers
    (<?? S (fs/new-fs-store uri))))

(defmethod delete-store  "file" [_ uri]
  (fs/delete-store uri))

(defmethod connect-store "file" [_ uri]
  (<?? S (fs/new-fs-store uri)))

(defmethod scheme->index "file" [_]
  :datahike.index/hitchhiker-tree)

;; level

(defmethod empty-store "level" [_ uri]
  (kons/add-hitchhiker-tree-handlers
    (<?? S (kl/new-leveldb-store uri))))

(defmethod delete-store "level" [_ uri]
  (kl/delete-store uri))

(defmethod connect-store "level" [_ uri]
  (<?? S (kl/new-leveldb-store uri)))

(defmethod release-store "level" [_ store]
  (kl/release store))

(defmethod scheme->index "level" [_]
  :datahike.index/hitchhiker-tree)
