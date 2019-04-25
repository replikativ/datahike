(ns datahike.index.hitchhiker-tree
  (:require [hitchhiker.tree.core :as hc :refer [<??]]
            [hitchhiker.tree.messaging :as hmsg]
            [datahike.constants :refer [e0 tx0 emax txmax]]
            [datahike.datom :refer [datom]])
  #?(:clj (:import [clojure.lang AMapEntry]
                   [datahike.datom Datom])))

(defn- index-type->datom-fn [index-type]
  (case index-type
    :aevt (fn [a e v tx] (datom e a v tx true))
    :avet (fn [a v e tx] (datom e a v tx true))
    (fn [e a v tx] (datom e a v tx true))))

(defn from-datom [^Datom datom index-type]
  (let [datom-seq (case index-type
                    :aevt (list  (.-a datom) (.-e datom) (.-v datom) (.-tx datom))
                    :avet (list(.-a datom) (.-v datom)  (.-e datom) (.-tx datom))
                    (list (.-e datom) (.-a datom) (.-v datom) (.-tx datom)))]
    (->> datom-seq
         (remove #{e0 tx0 emax txmax})
         (remove nil?)
         vec)))

(defn -slice
  [tree from to index-type]
  (let [create-datom (index-type->datom-fn index-type)
        [a b c d] (from-datom from index-type)
        [e f g h] (from-datom to index-type)
        xf (comp
             (take-while (fn [^AMapEntry kv]
                           ;; prefix scan
                           (let [key (.key kv)
                                 [i j k l] key
                                 new (not (cond (and e f g h)
                                                (or (> (hc/compare i e) 0)
                                                    (> (hc/compare j f) 0)
                                                    (> (hc/compare k g) 0)
                                                    (> (hc/compare l h) 0))

                                                (and e f g)
                                                (or (> (hc/compare i e) 0)
                                                    (> (hc/compare j f) 0)
                                                    (> (hc/compare k g) 0))

                                                (and e f)
                                                (or (> (hc/compare i e) 0)
                                                    (> (hc/compare j f) 0))

                                                e
                                                (> (hc/compare i e) 0)

                                                :else false))]
                             new)))
             (map (fn [kv]
                    (let [[a b c d] (.key ^AMapEntry kv)]
                      (create-datom a b c d)))))
        new (->> (sequence xf (hmsg/lookup-fwd-iter tree [a b c d]))
                 seq)]
    new))

(defn -seq [tree index-type]
  (-slice tree (datom e0 nil nil tx0) (datom emax nil nil txmax) index-type))

(defn -count [tree index-type]
  (count (-seq tree index-type)))