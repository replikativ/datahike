(ns datahike.tx-log
  (:require
   [datahike.datom :as datom]
   [datahike.constants :as c]
   [hitchhiker.tree :as tree]
   [hitchhiker.tree.utils.async :as async]
   [hitchhiker.tree.messaging :as msg]))

(defn insert-log [tree current-tx tx-data op-count]
  (async/<?? (msg/insert tree current-tx tx-data op-count)))

(defn empty-log [{:keys [index-b-factor index-data-node-size index-log-size]}]
  (async/<?? (tree/b-tree (tree/->Config index-b-factor index-data-node-size index-log-size))))

(defn init-log [current-tx datoms index-config op-count]
  (let [new-tree (empty-log index-config)
        result (insert-log new-tree current-tx datoms op-count)]
    (async/<?? result)))

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

(defn -purge [tree datoms op-count]
  (->> datoms
       (group-by :tx)
       (map
        (fn [[tx datoms]]
          (let [old-datoms (-get tree tx)]
            [tx (remove (set datoms) old-datoms)])))
       (reduce
        (fn [t [tx datoms]]
          (-> t
              (msg/delete tx op-count)
              (msg/insert tx datoms op-count)))
        tree)))

(comment

  (def log-tree (empty-log {:index-b-factor       c/default-index-b-factor
                            :index-log-size       c/default-index-log-size
                            :index-data-node-size c/default-index-data-node-size}))

  (insert-log log-tree 513 [[513 :db/txInstant "1232" 513]] 1)

  (async/<?? (msg/insert log-tree 1 [:c :d] 1)))
