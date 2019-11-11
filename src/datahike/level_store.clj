(ns datahike.level-store
  (:require [datahike.store :refer [empty-store delete-store connect-store release-store scheme->index]]
            [hitchhiker.tree.bootstrap.konserve :as kons]
            [konserve-leveldb.core :as kl]
            [superv.async :refer [<?? S]]))

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

