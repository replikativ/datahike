(ns datahike.tx-log
  (:require
   [datahike.index.hitchhiker-tree :as dih]
   [hitchhiker.tree.messaging :as msg]))

(defn insert-log [tree current-tx tx-data  op-count]
  (msg/insert tree current-tx tx-data op-count))

(defn init-log [current-tx datoms op-count]
  (insert-log
   (dih/empty-tree)
   current-tx
   datoms
   op-count))

(defn empty-log []
  (dih/empty-tree))

(defn -slice
  ([tree start]
   (-slice tree start nil))
  ([tree start end]
   (let [iter (msg/lookup-fwd-iter tree start)]
     (if (some? end)
       (take-while
        (fn [[tx _]] (<= tx end))
        iter)
       iter))))

(defn -get [tree tx]
  (msg/lookup tree tx))

(comment

  (take-while
   #(< % 10)
   [1 2 3 4 5 6 9 10 11])
  )
