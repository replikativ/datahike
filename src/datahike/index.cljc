(ns datahike.index
  (:require [datahike.constants :refer [e0 tx0 emax txmax implicit-schema br br-sqrt]]
            [hitchhiker.tree.core :as hc :refer [<??]]
            [hitchhiker.tree.messaging :as hmsg]
            [hitchhiker.konserve :as kons]
            [me.tonsky.persistent-sorted-set :as set]
            [datahike.datom :as dd])
  #?(:clj (:import [datahike.datom Datom]
                   [clojure.lang AMapEntry])))

(defprotocol IIndex
  (-iterator [index])
  (-set-iterator! [index iterator])
  (-seq [index])
  (-count [index])
  (-slice [index from to] [index from])
  (-insert! [index datom])
  (-retract! [index datom])
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

(deftype HitchhikerTree [^{:volatile-mutable true} tree create-datom insert delete]
  IIndex
  (-iterator [index]
    tree)

  (-set-iterator! [index iterator]
    (set! tree iterator))

  (-seq [index]
    (hitchhiker-slice tree [e0 nil nil tx0] [nil nil nil nil] create-datom))

  (-count [index]
    (count (hitchhiker-slice tree [e0 nil nil tx0] [nil nil nil nil] create-datom)))

  (-slice [index from]
    (hitchhiker-slice tree (from-datom from) (from-datom from) create-datom))

  (-slice [index from to]
    (hitchhiker-slice tree (from-datom from) (from-datom to) create-datom))

  (-insert! [index datom]
    (set! tree (insert tree datom)))

  (-retract! [index datom]
    (set! tree (delete tree datom)))

  (-set-transient [index])

  (-set-persistent! [index]))

(defn hitchhiker-tree
  ([]
   (hitchhiker-tree :eavt (<?? (hc/b-tree (hc/->Config br-sqrt br (- br br-sqrt))))))
  ([index-type]
   (hitchhiker-tree index-type (<?? (hc/b-tree (hc/->Config br-sqrt br (- br br-sqrt))))))
  ([index-type tree]
   (let [create-datom (case index-type
                       :aevt
                       (fn [a e v t] (dd/datom e a v t true))
                       :avet
                       (fn [a v e t] (dd/datom e a v t true))
                       (fn [e a v t] (dd/datom e a v t true)))
        insert (fn [tree ^Datom datom]
                    (<??
                     (hmsg/insert
                      tree
                      (case index-type
                        :aevt [(.-a datom) (.-e datom) (.-v datom) (.-tx datom)]
                        :avet [(.-a datom) (.-v datom) (.-e datom) (.-tx datom)]
                        [(.-e datom) (.-a datom) (.-v datom) (.-tx datom)])
                      nil)))
        delete (fn [tree ^Datom removing]
                    (<??
                     (hmsg/delete
                      tree
                      (case index-type
                        :aevt [(.-a removing) (.-e removing) (.-v removing) (.-tx removing)]
                        :avet [(.-a removing) (.-v removing) (.-e removing) (.-tx removing)]
                        [(.-e removing) (.-a removing) (.-v removing) (.-tx removing)]))))]
    (HitchhikerTree. tree create-datom insert delete))))


(deftype PersistentSortedSet [^{:volatile-mutable true} sorted-set cmp-quick]
  IIndex
  (-iterator [index]
    sorted-set)

  (-set-iterator! [index iterator]
    (set! sorted-set iterator))

  (-count [index] (count sorted-set))

  (-seq [index] (seq sorted-set))

  (-slice [index from]
    (set/slice sorted-set from from))

  (-slice [index from to]
    (set/slice sorted-set from to))

  (-insert! [index datom]
    (set! sorted-set (set/conj sorted-set datom cmp-quick)))

  (-retract! [index removing]
    (set! sorted-set (set/disj sorted-set removing cmp-quick)))

  )

(defn sorted-set [set index-type]
  (let [cmp (case index-type
              :aevt dd/cmp-datoms-aevt
              :avet dd/cmp-datoms-avet
              dd/cmp-datoms-eavt)
        cmp-quick (case index-type
              :aevt dd/cmp-datoms-aevt-quick
              :avet dd/cmp-datoms-avet-quick
              dd/cmp-datoms-eavt-quick)]
    (PersistentSortedSet. (set/sorted-set-by cmp) cmp-quick)))
