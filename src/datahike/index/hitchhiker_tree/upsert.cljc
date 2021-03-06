(ns ^:no-doc datahike.index.hitchhiker-tree.upsert
  (:require [hitchhiker.tree :as tree]
            [hitchhiker.tree.op :as op]))

(defn- increase-by-one?
  "Returns true if elements in vector 'indices' form a prefix of the vector indices.
  This is equivalent to check whether the elements increase by one and contains 0.
  E.g., [0 1 2] => true, [0 2 3] => false, [1 2] => false."
  [indices]
  (if (.contains indices 0)
    (let [m (apply max indices)
          s (apply + indices)]
      (= s (/ (* m (+ 1 m)) 2)))
    false))

(def prefix? (memoize increase-by-one?))

(defn- max-t
  "Returns the key with max 't' component."
  [ks]
  (when (seq ks)
    (apply max-key #(nth % 3) ks)))

(defn mask [new indices]
  (reduce (fn [mask pos]
            (assoc mask pos (nth new pos)))
          [nil nil nil nil]
          indices))

(defn old-key
  "Returns the old version of the given 'new' key if it exists in 'old-keys'.
  If there are multiple old versions, the one with the biggest transaction time is returned.
  'indices' is a vector of integer indicating which positions in keys are significant,
  i.e., [0 2] means that the first and third entry in the key are used for filtering."
  [old-keys new indices]
  (when (seq old-keys)
    (let [mask (mask new indices)]
      (when-let [candidates (subseq old-keys >= mask)]
        (->> candidates
             (map first)
             ((if (prefix? indices) take-while filter)
              #(reduce (fn [_ i]
                         (if (= (nth % i) (nth new i))
                           true
                           (reduced false)))
                       true
                       indices))
             max-t)))))

(defn remove-old
  "Removes old key from the 'kvs' map using 'remove-fn' function if 'new' and 'old' keys' first two entries match."
  [kvs new remove-fn indices]
  (when-let [old (old-key kvs new indices)]
    (remove-fn old)))

(defrecord UpsertOp [key op-count indices]
  op/IOperation
  (-insertion-ts [_] op-count)
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

(defrecord temporal-UpsertOp [key op-count indices]
  op/IOperation
  (-insertion-ts [_] op-count)
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
          ;; TODO Remove ts when Wanderung is available.
          (fn [{:keys [key value op-count ts indices]}]
            (map->UpsertOp {:key key :value value :op-count (or op-count ts) :indices (or indices [0 1])}))

          'datahike.index.hitchhiker_tree.upsert.temporal-UpsertOp
          (fn [{:keys [key value op-count ts indices]}]
            (map->temporal-UpsertOp {:key key :value value :op-count (or op-count ts) :indices (or indices [0 1])}))})
  store)
