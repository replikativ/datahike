(ns datahike.store
  (:require [hitchhiker.konserve :as kons]
            [konserve.filestore :as fs]
            [konserve-leveldb.core :as kl]
            [konserve-pg.core :as kp]
            [konserve.memory :as mem]
            [superv.async :refer [<?? S]]
            [konserve.core :as k]))

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

(defmethod empty-store "mem" [_ path]
  (let [store (<?? S (mem/new-mem-store))]
    (swap! memory assoc path store)
    store))

(defmethod delete-store "mem" [_ path]
  (swap! memory dissoc path))

(defmethod connect-store "mem" [_ path]
  (@memory path))

(defmethod scheme->index "mem" [_]
  :datahike.index/persistent-set)

;; file

(defmethod empty-store "file" [_ path]
  (kons/add-hitchhiker-tree-handlers
    (<?? S (fs/new-fs-store path))))

(defmethod delete-store  "file" [_ path]
  (fs/delete-store path))

(defmethod connect-store "file" [_ path]
  (<?? S (fs/new-fs-store path)))

(defmethod scheme->index "file" [_]
  :datahike.index/hitchhiker-tree)

;; postgresql

(defn pg-path [path]
  (clojure.string/join  ["postgres:" path]))

(defmethod empty-store "pg" [_ path]
  (kons/add-hitchhiker-tree-handlers
   (<?? S (kp/new-pg-store (pg-path path)))))

(defmethod delete-store  "pg" [_ path]
  (kp/delete-store (pg-path path)))

(defmethod connect-store "pg" [_ path]
  (<?? S (kp/new-pg-store (pg-path path))))

(defmethod scheme->index "pg" [_]
  :datahike.index/hitchhiker-tree)


;; level

(defmethod empty-store "level" [_ path]
  (kons/add-hitchhiker-tree-handlers
    (<?? S (kl/new-leveldb-store path))))

(defmethod delete-store "level" [_ path]
  (kl/delete-store path))

(defmethod connect-store "level" [_ path]
  (<?? S (kl/new-leveldb-store path)))

(defmethod release-store "level" [_ store]
  (kl/release store))

(defmethod scheme->index "level" [_]
  :datahike.index/hitchhiker-tree)

