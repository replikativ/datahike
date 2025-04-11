(ns datahike.query-stats
  (:require [clojure.set :as set]
            [datahike.tools :as dt]))

(defn round [precision x]
  #?(:clj (with-precision ^long precision ^java.math.BigDecimal x)
     :cljs (let [y (Math/pow 10 precision)]
             (/ (.round js/Math (* y x)) y))))

(defn compatible-abs [^long n]
  #?(:clj  (Math/abs n)
     :cljs (js/Math.abs n)))

(defn get-stats [context]
  {:rels (mapv (fn [rel] {:rows (count (:tuples rel))
                          :bound (set (keys (:attrs rel)))})
               (:rels context))})

(defn update-ctx-with-stats
  "update-fn must expect [context] as argument"
  [context clause update-fn]
  (if (:stats context)
    (let [{:keys [res t]} (dt/timed #(update-fn context))
          clause-stats (merge (get-stats res)
                              {:clause clause
                               :t t}
                              (:tmp-stats res))]
      (-> res
          (update :stats conj clause-stats)
          (dissoc :tmp-stats)))
    (update-fn context)))

(defn extend-stat
  "Adds summarized row counts, bindings and clause time for convenience"
  [{:keys [branches rels] :as stat}]
  (assoc stat
         :id (subs (str (compatible-abs (hash stat))) 0 6)
         :t-branches (reduce #(+ %1 (:t %2)) 0.0 branches)
         :rel-count (count rels)
         :rows (reduce #(+ %1 (:rows %2)) 0 rels)
         :bound (reduce #(set/union %1 (:bound %2)) #{} rels)
         :branches (mapv extend-stat branches)))

(defn get-stat-diffs [parent-node prev-node {:keys [t t-branches rows bound] :as node}]
  (let [extend-and-branches (fn [branches]
                              (mapv (partial get-stat-diffs node)
                                    (cons prev-node branches)
                                    branches))
        extend-or-branches (fn [branches]
                             (mapv (partial get-stat-diffs node prev-node) branches))]
    (-> node
        (dissoc :rows :bound)
        (assoc :prev (:id prev-node)
               :parent (:id parent-node)
               :t-diff (round 6 (- t t-branches))
               :rows-in (:rows prev-node)
               :rows-out rows
               :rows-diff (- rows (:rows prev-node))
               :bound-in (:bound prev-node)
               :bound-out bound
               :bound-diff (set/difference bound (:bound prev-node)))
        (update :branches (case (:type node)
                            :and extend-and-branches
                            :or extend-or-branches
                            :or-join extend-or-branches
                            :rule extend-or-branches
                            :solve extend-and-branches
                            identity)))))

(defn extend-stats [stats]
  (let [extended (map extend-stat stats)
        root {:rows 0 :bound (set []) :id "_root_"}]
    (mapv (partial get-stat-diffs root)
          (cons root extended)
          extended)))

(defn stats-table
  ([stats]
   (stats-table stats [:id :prev :parent :clause :type
                       :rows-in :rows-diff :bound-in :bound-diff
                       :t :t-diff :clauses]))
  ([stats cols]
   (->> stats
        extend-stats
        (hash-map :branches)
        (tree-seq map? :branches)
        rest
        (map (apply juxt cols)))))
