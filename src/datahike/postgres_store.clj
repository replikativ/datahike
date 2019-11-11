(ns datahike.postgres-store
  (:require [datahike.store :refer [empty-store delete-store connect-store scheme->index]]
            [hitchhiker.tree.bootstrap.konserve :as kons]
            [konserve-pg.core :as kp]
            [superv.async :refer [<?? S]]))


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

