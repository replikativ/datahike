(ns ^:no-doc datahike.index.hitchhiker-tree.insert
  (:require [hitchhiker.tree :as tree]
            [hitchhiker.tree.op :as op]))


(defn mask [new indices]
  (reduce (fn [mask pos]
            (assoc mask pos (nth new pos)))
          [nil nil nil nil]
          indices))

(defn exists-old?
  "Returns the old version of the given 'new' key if it exists in 'old-keys'.
  If there are multiple old versions, the one with the biggest transaction time is returned.
  'indices' is a vector of integer indicating which positions in keys are significant,
  i.e., [0 2] means that the first and third entry in the key are used for filtering."
  [old-keys new]
  (when (seq old-keys)
    (let [indices [0 1 2]
          mask (mask new indices)]
      false
      #_(when-let [candidates (subseq old-keys >= mask)]
        (->> candidates
             (map first)
             first
             #(reduce (fn [_ i]
                        (if (= (nth % i) (nth new i))
                          true
                          (reduced false)))
                true
                indices))))))

(defrecord InsertOp [key op-count]
  op/IOperation
  (-insertion-ts [_] op-count)
  (-affects-key [_] key)
  (-apply-op-to-coll [_ kvs]
    (when-not (exists-old? kvs key)
      (prn "------ inserting op.col " key "---- into " kvs)
      (assoc kvs key nil)))
  (-apply-op-to-tree [_ tree]
    (let [children  (cond
                      (tree/data-node? tree) (:children tree)
                      :else (:children (peek (tree/lookup-path tree key))))]
      (when-not (exists-old? children key)
        (prn "------ inserting TREE " key "---- into " children)
        (tree/insert tree key nil)))))

(defn new-InsertOp [key op-count]
  (InsertOp. key op-count))
