(ns performance.hitchhiker
  (:require [hitchhiker.tree.utils.async :as async]
            [hitchhiker.tree.messaging :as hmsg]
            [hitchhiker.tree :as tree]))



;; Hitchhiker functions

(def ^:const br 300)                                        ;; same as in datahike
(def ^:const br-sqrt (long (Math/sqrt br)))                 ;; same as in datahike

(def memory (atom {}))

(defn connect [uri]
  {:tree (get @memory uri) :config uri})

(defn create [uri]
  (swap! memory assoc uri (async/<?? (tree/b-tree (tree/->Config br-sqrt br (- br br-sqrt))))))

(defn delete [uri]
  (swap! memory dissoc uri))

(defn insert-many [tree values] ;; measure
  (async/<??
    (async/reduce<
      (fn [tree val] (hmsg/insert tree val nil))
      tree
      values)))

(defn entities->datoms [entities]
  (apply concat (map #(map (fn [[k v]] (vector 0 k v :db/add)) %) entities)))

(defn entities->values [entities]
  (apply concat (map #(map second %) entities)) )

(defn transact [conn tx]
  (let [values (if (= (:config conn) "values")
                 (entities->values tx)
                 (entities->datoms tx)) ;; tx to datoms
        new-tree (insert-many (:tree conn) values)]
    (assoc conn :tree new-tree)))
