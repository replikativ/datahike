(ns datahike.upsert
  (:require [datahike.index.hitchhiker-tree :as hh]))

(defn add-upsert-handler [store]
  (swap! (:read-handlers store)
         merge
         {'datahike.index.hitchhiker_tree.UpsertOp
          (fn [{:keys [key value]}]
            (hh/map->UpsertOp {:key key :value value}))})
  store)
