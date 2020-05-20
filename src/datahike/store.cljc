(ns datahike.store
  (:require [hitchhiker.tree.bootstrap.konserve :as kons]
            [konserve.filestore :as fs]
            [konserve.memory :as mem]
            [superv.async :refer [<?? S]]))

(defmulti empty-store
          "Creates an empty store"
          {:arglists '([config])}
          :backend)

(defmethod empty-store :default [{:keys [backend]}]
  (throw (IllegalArgumentException. (str "Can't create a store with scheme: " backend))))

(defmulti delete-store
          "Deletes an existing store"
          {:arglists '([config])}
          :backend)

(defmethod delete-store :default [{:keys [backend]}]
  (throw (IllegalArgumentException. (str "Can't delete a store with scheme: " backend))))

(defmulti connect-store
          "Makes a connection to an existing store"
          {:arglists '([config])}
          :backend)

(defmethod connect-store :default [{:keys [backend]}]
  (throw (IllegalArgumentException. (str "Can't connect to store with scheme: " backend))))

(defmulti release-store
          "Releases the connection to an existing store (optional)."
          {:arglists '([config store])}
          (fn [{:keys [backend]} store]
            backend))

(defmethod release-store :default [_ _]
  nil)

(defmulti scheme->index
          "Returns the index type to use for this store"
          {:arglists '([config])}
          :backend)

;; memory

(def memory (atom {}))

(defmethod empty-store :mem [{:keys [id]}]
  (let [store (<?? S (mem/new-mem-store))]
    (swap! memory assoc id store)
    store))

(defmethod delete-store :mem [{:keys [id]}]
  (swap! memory dissoc id))

(defmethod connect-store :mem [{:keys [id]}]
  (@memory id))

(defmethod scheme->index :mem [_]
  :datahike.index/hitchhiker-tree)

;; file

(defmethod empty-store :file [{:keys [path]}]
  (kons/add-hitchhiker-tree-handlers
    (<?? S (fs/new-fs-store path))))

(defmethod delete-store :file [{:keys [path]}]
  (fs/delete-store path))

(defmethod connect-store :file [{:keys [path]}]
  (<?? S (fs/new-fs-store path)))

(defmethod scheme->index :file [_]
  :datahike.index/hitchhiker-tree)

