(ns performance.db.hitchhiker
  (:require [hitchhiker.tree.utils.async :as async]
            [hitchhiker.tree.messaging :as hmsg]
            [hitchhiker.tree :as tree]
            [performance.db.interface :as db]))


;; Hitchhiker functions

(def ^:const br 300)                                        ;; same as in datahike
(def ^:const br-sqrt (long (Math/sqrt br)))                 ;; same as in datahike

(def memory (atom {}))

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

(defn entities->nodes [conn entities]
  (if (= (:config conn) "values")
    (entities->values entities)
    (entities->datoms entities)))


;; Multimethods

(defmethod db/connect :hitchhiker [_ uri]
  {:tree (get @memory uri) :config uri})

(defmethod db/release :hitchhiker [_ _] nil)

(defmethod db/transact :hitchhiker [_ conn tx]
  (let [new-tree (insert-many (:tree conn) (entities->nodes conn tx))]
    (assoc conn :tree new-tree)))

(defmethod db/init :hitchhiker [_ uri _]
  (delete uri)
  (create uri))



