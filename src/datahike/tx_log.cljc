(ns datahike.tx-log
  (:require
   [datahike.index.hitchhiker-tree :as dih]
   [datahike.datom :as datom]
   [hitchhiker.tree.utils.async :as async]
   [hitchhiker.tree.messaging :as msg]))

(defn insert-log [tree current-tx tx-data op-count]
  (msg/insert tree current-tx tx-data op-count))

(defn init-log [current-tx datoms op-count]
  (let [new-tree (dih/empty-tree)
        result (insert-log new-tree current-tx datoms op-count)]
    (async/<?? result)))

(defn empty-log []
  (dih/empty-tree))

(defn to-datom [[e a v t added]]
  (datom/datom e a v t added))

(defn -slice
  ([tree start]
   (-slice tree start nil))
  ([tree start end]
   (let [iter (msg/lookup-fwd-iter tree start)
         result (if (some? end)
                  (take-while
                   (fn [[tx _]] (<= tx end))
                   iter)
                  iter)]
     (map
      (fn [[tx data]]
        [tx (mapv (partial apply datom/datom) data)])
      result))))

(defn -get [tree tx]
  (mapv
   (partial apply datom/datom)
   (msg/lookup tree tx)))
