(ns ^:no-doc datahike.index.hitchhiker-tree.upsert
  (:require [hitchhiker.tree :as tree]
            [hitchhiker.tree.op :as op]))

(defn- max-t
  "Returns the key with the max 't' component"
  [ks]
  (when (seq ks)
    (apply max-key #(nth % 3) ks)))

(defn old-key
  "Returns the old version of the given 'new' key if it exists in 'old-keys'.
  If there are multiple old versions, the one with the biggest transaction time is returned.
  'indices' is a vector of integer indicating which positions in keys are significant,
  e.g., [0 2] means that the first and third entry in the key are used for filtering."
  [old-keys new indices]
  (let [mask (reduce (fn [mask pos]
                       (assoc mask pos (nth new pos)))
                     [nil nil nil nil]
                     indices)]
    (when (seq old-keys)
      (when-let [candidates (subseq old-keys >= mask)]
        (->> candidates
             (map first)
             ((if (= [0 2] indices) filter take-while)
              #(reduce (fn [bool i]
                         (and bool (= (nth % i) (nth new i))))
                       true
                       indices))
             max-t)))))

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
