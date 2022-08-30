(ns datahike.index.utils
  (:require [datahike.constants :refer [e0 tx0 emax txmax]]
            [datahike.datom :as dd])
  #?(:clj (:import [datahike.datom Datom])))

#_(defn datom-to-vec [^Datom datom index-type start?]
  (let [e (fn [datom] (when-not (or (and start? (= e0 (.-e datom))) ;; those transformations can be avoided by adjusting search-indices
                                    (and (not start?) (= emax (.-e datom))))
                        (.-e datom)))
        tx (fn [datom] (when-not (or (and start? (= tx0 (.-tx datom)))
                                     (and (not start?) (= txmax (.-tx datom))))
                         (.-tx datom)))
        datom-seq (case index-type
                    :aevt (list (.-a datom) (e datom) (.-v datom) (tx datom))
                    :avet (list (.-a datom) (.-v datom) (e datom) (tx datom))
                    (list (e datom) (.-a datom) (.-v datom) (tx datom)))]
    (->> datom-seq
         (take-while some?)
         vec)))

(defn datom-to-vec
  ([^Datom datom index-type start?] (datom-to-vec datom index-type start? false))
  ([^Datom datom index-type start? incl-added?]
   (let [e (fn [datom] (when-not (or (and start? (= e0 (.-e datom)))
                                     (and (not start?) (= emax (.-e datom))))
                         (.-e datom)))
         tx (fn [datom] (when-not (or (and start? (= tx0 (.-tx datom)))
                                      (and (not start?) (= txmax (.-tx datom))))
                          (if incl-added? (dd/datom-tx datom) (.-tx datom))))
         datom-seq (cond-> (case index-type
                             :aevt (list (.-a datom) (e datom) (.-v datom) (tx datom))
                             :avet (list (.-a datom) (.-v datom) (e datom) (tx datom))
                             (list (e datom) (.-a datom) (.-v datom) (tx datom)))
                           incl-added? (concat (list (dd/datom-added datom))))]
     (->> datom-seq
          (take-while some?)
          vec))))

(defn- slice-datom-compare [cmp]
  (fn [v1 v2] (-> (filter #(not (= % 0)) (map cmp v1 v2))
                  first
                  (or 0))))

(defn prefix-scan [cmp [e f g h a]]
  (let [datom-vec-compare (slice-datom-compare cmp)]
    (fn [[i j k l b]]
      (< (cond  ;; can be more efficient with nested if-statements
           ;; 'some?' neccessary for v at 3rd pos for :eavt and :aevt index and at 2nd pos for :avet index
           (and e (some? f) (some? g) h a)  (datom-vec-compare [i j k l a] [e f g h b])
           (and e (some? f) (some? g) h)  (datom-vec-compare [i j k l] [e f g h])
           (and e (some? f) (some? g))    (datom-vec-compare [i j k] [e f g])
           (and e (some? f))              (datom-vec-compare [i j] [e f])
           e                              (cmp i e)
           :else                          0)
         1))))

(defn equals-on-indices?
  "Returns true if 'k1' and 'k2' have the same value at positions indicated by 'indices'"
  [k1, k2, indices]
  (reduce (fn [_ i]
            (if (= (nth k1 i) (nth k2 i))
              true
              (reduced false)))
          true
          indices))
