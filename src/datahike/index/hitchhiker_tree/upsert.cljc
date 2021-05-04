(ns ^:no-doc datahike.index.hitchhiker-tree.upsert
  (:require [hitchhiker.tree :as tree]
            [hitchhiker.tree.op :as op]))

(defn old-key
  "Returns the old version of the given 'new' key if it exists in 'old-keys'.
  If there are multiple old versions, the one with the biggest transaction time is returned."
  [old-keys new indices]
  (let [;;new [1 2 3 4 5]
        ;;pos-vec [2 3]
        mask (reduce (fn [mask pos]
;;                         (println mask "--- " pos)
                         (assoc mask pos (nth new pos)))
                       (vec (take (count  new) (repeat nil)))
                       indices)
        ;;[a _ b _] new
        ]

    (when (seq old-keys)
      (when-let [candidates (subseq old-keys >= mask #_[a nil nil nil])]
        (->> candidates
             (map first)
             (filter #(reduce (fn [acc pos]
                                (and acc (= (nth % pos) (nth new pos))))
                        true
                        indices) #_[[1 2 3 4 5] [0 2 3 7 8] [0 7 0 4 5]])
;;             (filter #(and (= a (first %)) (= b (nth % 2))))
          ;;(take-while #(and (= a (first %)) (= b (nth % 2))))
             reverse
             first)))))

(defn remove-old
  "Removes old key from the 'kvs' map using 'remove-fn' function if 'new' and 'old' keys' first two entries match."
  [kvs new remove-fn indices]
  (when-let [old (old-key kvs new indices)]
    (remove-fn old)))

(defrecord UpsertOp [key ts indices]
  op/IOperation
  (-insertion-ts [_] ts)
  (-affects-key [_] key)
  (-apply-op-to-coll [_ kvs]
    (-> (or (remove-old kvs key (partial dissoc kvs) indices) kvs)
        (assoc key nil)))
  (-apply-op-to-tree [_ tree]
    (let [children  (cond
                      (tree/data-node? tree) (:children tree)
                      :else (:children (peek (tree/lookup-path tree key))))]
      (-> (or (remove-old children key (partial tree/delete tree) indices) tree)
          (tree/insert key nil)))))

(defn old-retracted
  "Returns a new datom to insert in the tree to signal the retraction of the old datom."
  [kvs key indices]
  (when-let [old (old-key kvs key indices)]
    (let [[a b c _] old
          [_ _ _ nt] key]
      ;; '-' means it is retracted and 'nt' is the current transaction time.
      [a b c (- nt)])))

(defrecord temporal-UpsertOp [key ts indices]
  op/IOperation
  (-insertion-ts [_] ts)
  (-affects-key [_] key)
  (-apply-op-to-coll [_ kvs]
    (let [old-retracted  (old-retracted kvs key indices)]
      (-> (if old-retracted
            (assoc kvs old-retracted nil)
            kvs)
          (assoc key nil))))
  (-apply-op-to-tree [_ tree]
    (let [children  (cond
                      (tree/data-node? tree) (:children tree)
                      :else (:children (peek (tree/lookup-path tree key))))
          old-retracted  (old-retracted children key indices)]
      (-> (if old-retracted
            (tree/insert tree old-retracted nil)
            tree)
          (tree/insert key nil)))))

(defn new-UpsertOp [key op-count indices]
  (UpsertOp. key op-count indices))

(defn new-temporal-UpsertOp [key op-count indices]
  (temporal-UpsertOp. key op-count indices))

(defn add-upsert-handler
  "Tells the store how to deserialize upsert related operations"
  [store]
  (swap! (:read-handlers store)
         merge
         {'datahike.index.hitchhiker_tree.upsert.UpsertOp
          (fn [{:keys [key value]}]
            (map->UpsertOp {:key key :value value}))

          'datahike.index.hitchhiker_tree.upsert.temporal-UpsertOp
          (fn [{:keys [key value]}]
            (map->temporal-UpsertOp {:key key :value value}))})
  store)
