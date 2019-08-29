(ns datahike.store
  (:require [hitchhiker.konserve :as kons]
            [konserve.filestore :as fs]
            [konserve-leveldb.core :as kl]
            [konserve-pg.core :as kp]
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

(defmethod empty-store :mem [{:keys [host]}]
  (let [store (<?? S (mem/new-mem-store))]
    (swap! memory assoc host store)
    store))

(defmethod delete-store :mem [{:keys [host]}]
  (swap! memory dissoc host))

(defmethod connect-store :mem [{:keys [host]}]
  (@memory host))

(defmethod scheme->index :mem [_]
  :datahike.index/persistent-set)

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

;; postgresql

(defn pg-path [{:keys [username password host port path]}]
  (clojure.string/join ["postgres://" username ":" password "@" host ":" port path]))

(defmethod empty-store :pg [config]
  (kons/add-hitchhiker-tree-handlers
    (<?? S (kp/new-pg-store (pg-path config)))))

(defmethod delete-store :pg [config]
  (kp/delete-store (pg-path config)))

(defmethod connect-store :pg [config]
  (<?? S (kp/new-pg-store (pg-path config))))

(defmethod scheme->index :pg [_]
  :datahike.index/hitchhiker-tree)

;; level

(defmethod empty-store :level [{:keys [path]}]
  (kons/add-hitchhiker-tree-handlers
    (<?? S (kl/new-leveldb-store path))))

(defmethod delete-store :level [{:keys [path]}]
  (kl/delete-store path))

(defmethod connect-store :level [{:keys [path]}]
  (<?? S (kl/new-leveldb-store path)))

(defmethod release-store :level [_ store]
  (kl/release store))

(defmethod scheme->index :level [_]
  :datahike.index/hitchhiker-tree)

