(ns datahike.index
  (:require [datahike.constants :refer [e0 tx0 emax txmax implicit-schema]]
            [hitchhiker.tree.core :as hc :refer [<??]]
            [hitchhiker.tree.messaging :as hmsg]
            [hitchhiker.konserve :as kons]
            [datahike.datom :refer [datom]])
  #?(:clj (:import [datahike.datom Datom]
                   [clojure.lang AMapEntry])))

(defprotocol IIndex
  (-coll [index])
  (-slice [index from to] [index from])
  (-update-coll! [index coll])
  (-set-transient [index])
  (-set-persistent! [index]))

(defn from-datom [^Datom datom]
  (let [datom-seq (list (.-e datom) (.-a datom) (.-v datom) (.-tx datom))]
    (->> datom-seq
         (remove #{e0 tx0 emax txmax})
         (remove nil?)
         vec)))

(defn hitchhiker-slice [index from to create-datom]
  (let [[a b c d] from
          [e f g h] to
          xf (comp
              (take-while (fn [^AMapEntry kv]
                       ;; prefix scan
                            (let [key (.key kv)
                                  [i j k l] key]
                              (not (cond (and e f g h)
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

                                         :else false)))))
              (map (fn [kv]
                     (let [[a b c d] (.key ^AMapEntry kv)]
                       (create-datom a b c d)))))
          new (->> (sequence xf (hmsg/lookup-fwd-iter index [a b c d]))
                   seq)]
      new))

(deftype HitchhikerTree [^{:volatile-mutable true} tree create-datom]
  IIndex
  (-coll [index] tree)
  (-slice [index from]
    (hitchhiker-slice tree (from-datom from) (from-datom from) create-datom))
  (-slice [index from to]
    (hitchhiker-slice tree (from-datom from) (from-datom to) create-datom))
  (-update-coll! [index update-fn]
    (set! tree (update-fn tree)))
  (-set-transient [index])
  (-set-persistent! [index]))

(defn hitchhiker-tree [tree index-type]
  (let [create-datom (case index-type
                       :eavt
                       (fn [e a v t] (datom e a v t true))
                       :aevt
                       (fn [a e v t] (datom e a v t true))
                       :avet
                       (fn [a v e t] (datom e a v t true))
                       (fn [e a v t] (datom e a v t true)))]
    (HitchhikerTree. tree create-datom)))
