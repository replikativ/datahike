(ns datahike.store
  (:require [hitchhiker.tree.bootstrap.konserve :as kons]
            [konserve.filestore :as fs]
            [konserve.memory :as mem]
            [konserve-carmine.core :as rs]
            [superv.async :refer [<?? S]]))

(doseq [s ['empty-store 'delete-store 'connect-store]]
  (ns-unmap (symbol (str  *ns*)) s))


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

;; redis

(defmethod empty-store :redis [{:keys [password host port] :as opts}]
  (kons/add-hitchhiker-tree-handlers
   (<?? S (rs/new-carmine-store {:pool {} :spec {:password password :host host :port port}}))))

(defmethod delete-store :redis [{:keys [password host port] :as opts}]
  (throw (Exception. "Not implemented")))

(defmethod connect-store :redis [{:keys [password host port] :as opts}]
  (<?? S (rs/new-carmine-store {:pool {} :spec {:password password :host host :port port}})))

(defmethod scheme->index :redis [_]
  :datahike.index/hitchhiker-tree)

