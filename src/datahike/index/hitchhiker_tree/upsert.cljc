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

(defn mask [new indices]
  (reduce (fn [mask pos]
            (assoc mask pos (nth new pos)))
          [nil nil nil nil]
          indices))

(defn equals-on-indices?
  "Returns true if 'k1' and 'k2' have the same
   value at positions indicated by 'indices'"
  [k1, k2, indices]
  (reduce (fn [_ i]
            (if (= (nth k1 i) (nth k2 i))
              true
              (reduced false)))
          true
          indices))

(defn old-key
  "Returns the old version of the given 'new' key if it exists in 'old-keys'.
  'indices' is a vector of integer indicating which positions in keys are significant,
  i.e., [0 2] means that the first and third entry in the key are used for filtering."
  [old-keys new indices]
  (when (seq old-keys)
    (let [mask (mask new indices)]
      (when-let [candidates (subseq old-keys >= mask)]
        (when (or (not (prefix? indices))
                  (equals-on-indices? new (-> candidates first first) indices))
          (let [res (->> candidates
                         (map first)
                      ;; Returns the key which has not been retracted.
                      ;; There will at most be one such key.
                      ;; Because of the ordering in keys, we know that
                      ;; when two successive keys have a positive
                      ;; :t value, then the second key is our answer,
                      ;; the one that has not been retracted."
                         (reduce (fn [prev-pos? k]
                                   (let [curr-pos? (pos? (nth k 3))]
                                     (if (and curr-pos?
                                              prev-pos?
                                              (equals-on-indices? new k indices))
                                       (reduced k)
                                       curr-pos?)))
                                 true))]
            (if (boolean? res) nil res)))))))

(defn remove-old
  "Removes old key from the 'kvs' map using 'remove-fn' function."
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
  "Returns a new datom to be inserted in the tree to signal the retraction of
  its corresponding 'old' datom."
  [kvs key old indices]
  (let [[a b c _] old
        [_ _ _ nt] key]
    ;; '-' means it is retracted and 'nt' is the current transaction time.
    [a b c (- nt)]))

(defrecord temporal-UpsertOp [key op-count indices]
  op/IOperation
  (-insertion-ts [_] op-count)
  (-affects-key [_] key)
  (-apply-op-to-coll [_ kvs]
    (if-let [old (old-key kvs key indices)]
      (if (equals-on-indices? key old [0 1 2])
        kvs
        (let [old-retracted (old-retracted kvs key old indices)]
          (-> (assoc kvs old-retracted nil)
              (assoc key nil))))
      (assoc kvs key nil)))
  (-apply-op-to-tree [_ tree]
    (let [children (cond
                     (tree/data-node? tree) (:children tree)
                     :else (:children (peek (tree/lookup-path tree key))))
          old (old-key children key indices)]
      (if old
        (if (equals-on-indices? key old [0 1 2])
          tree
          (let [old-retracted (old-retracted children key old indices)]
            (-> (tree/insert tree old-retracted nil)
                (tree/insert key nil))))
        (tree/insert tree key nil)))))

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
