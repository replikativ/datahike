(ns ^:no-doc datahike.index.persistent-set
  (:require [me.tonsky.persistent-sorted-set :as set]
            [me.tonsky.persistent-sorted-set.arrays :as arrays]
            [clojure.core.async :as async]
            [datahike.datom :as dd]
            [datahike.constants :refer [tx0 txmax]]
            [datahike.index.interface :as di :refer [IIndex]]
            [datahike.index.utils :as diu]
            [konserve.core :as k]
            [konserve.serializers :refer [fressian-serializer]]
            [hasch.core :refer [uuid]])
  #?(:clj (:import [datahike.datom Datom]
                   [org.fressian.handlers WriteHandler ReadHandler]
                   [me.tonsky.persistent_sorted_set PersistentSortedSet Loader Leaf])))

(defn index-type->cmp [index-type]
  (case index-type
    :aevt dd/cmp-datoms-aevt
    :avet dd/cmp-datoms-avet
    dd/cmp-datoms-eavt))

(defn index-type->cmp-quick
  ([index-type] (index-type->cmp-quick index-type true))
  ([index-type abs-txid?] (if abs-txid?
                            (case index-type
                              :aevt dd/cmp-datoms-aevt-quick
                              :avet dd/cmp-datoms-avet-quick
                              dd/cmp-datoms-eavt-quick)
                            (case index-type
                              :aevt dd/cmp-datoms-aevt-quick-raw-txid
                              :avet dd/cmp-datoms-avet-quick-raw-txid
                              dd/cmp-datoms-eavt-quick-raw-txid))))

(defn slice [pset from to index-type]
  (let [cmp (diu/prefix-scan compare (diu/datom-to-vec to index-type false))]
    (->> (set/slice pset from nil)
         (take-while (fn [^Datom d] (cmp (diu/datom-to-vec d index-type false))))
         seq)))

(defn remove-datom [set datom index-type]
  (set/disj set datom (index-type->cmp-quick index-type)))

(defn insert [pset datom index-type]
  (if (slice pset
             (dd/datom (.-e datom) (.-a datom) (.-v datom) tx0)
             (dd/datom (.-e datom) (.-a datom) (.-v datom) txmax)
             index-type)
    set
    (set/conj pset datom (index-type->cmp-quick index-type))))

(defn temporal-insert [pset datom index-type]
  (set/conj pset datom (index-type->cmp-quick index-type)))

(defn upsert [pset datom index-type]
  (-> (or (when-let [old (first (slice pset
                                       (dd/datom (.-e datom) (.-a datom) nil tx0)
                                       (dd/datom (.-e datom) (.-a datom) nil txmax)
                                       index-type))]
            (remove-datom pset old index-type))
          pset)
      (set/conj datom (index-type->cmp-quick index-type))))

(defn temporal-upsert [pset datom index-type]
  (if-let [old (first (slice pset
                             (dd/datom (.-e datom) (.-a datom) nil tx0)
                             (dd/datom (.-e datom) (.-a datom) nil txmax)
                             index-type))]
    (if (diu/equals-on-indices? datom old [0 1 2])
      pset
      (-> pset
          (set/conj (dd/datom (.-e old) (.-a old) (.-v old) (.-tx old) false)
                    (index-type->cmp-quick index-type false))
          (set/conj datom
                    (index-type->cmp-quick index-type))))
    (set/conj pset datom (index-type->cmp-quick index-type))))

(extend-type PersistentSortedSet
  IIndex
  (-slice [pset from to index-type]
    (slice pset from to index-type))
  (-all [pset]
    (identity pset))
  (-seq [pset]
    (seq pset))
  (-count [pset]
    (count pset))
  (-insert [pset datom index-type _op-count]
    (insert pset datom index-type))
  (-temporal-insert [pset datom index-type _op-count]
    (set/conj pset datom (index-type->cmp-quick index-type)))
  (-upsert [pset datom index-type _op-count]
    (upsert pset datom index-type))
  (-temporal-upsert [pset datom index-type _op-count]
    (temporal-upsert pset datom index-type))
  (-remove [pset datom index-type _op-count]
    (remove-datom pset datom index-type))
  (-flush [pset _]
    (set! (.-_root pset)
          (set/-flush (.-_root pset)))
    pset)
  (-transient [pset]
    (transient pset))
  (-persistent! [pset]
    (persistent! pset)))

(defn get-loader [konserve-store]
  (proxy [Loader] []
    (load [address]
      (let [children-as-maps (async/<!! (k/get konserve-store address))] ;; TODO: use synchronous calls as soon as available
        (->> children-as-maps
             (map (fn [m] (set/map->node this (update m :keys (fn [keys] (mapv #(when-let [datom-seq (seq %)]
                                                                                  (dd/datom-from-reader datom-seq))
                                                                               keys))))))
             (into-array Leaf))))
    (store [children]
      (let [children-as-maps (mapv (fn [node] (-> node
                                                  set/-to-map
                                                  (update :keys (fn [keys] (mapv (comp vec seq) keys)))))
                                   children)
            address (uuid)]
        (async/<!! (k/assoc konserve-store address children-as-maps))
        address))))

(defmethod di/empty-index :datahike.index/persistent-set [_index-name store index-type _]
  (with-meta (set/sorted-set-by (index-type->cmp index-type) (get-loader store))
    {:index-type index-type}))

(defmethod di/init-index :datahike.index/persistent-set [_index-name store datoms index-type _ {:keys [indexed]}]
  (let [arr (if (= index-type :avet)
              (->> datoms
                   (filter #(contains? indexed (.-a ^Datom %)))
                   to-array)
              (cond-> datoms
                (not (arrays/array? datoms))
                (arrays/into-array)))]
    (arrays/asort arr (index-type->cmp-quick index-type))
    (with-meta (set/from-sorted-array (index-type->cmp index-type) arr (get-loader store))
      {:index-type index-type})))

(defmethod di/add-konserve-handlers :datahike.index/persistent-set [_index-name store]
  (assoc store :serializers {:FressianSerializer (fressian-serializer
                                                  {"datahike.index.PersistentSortedSet" (reify ReadHandler
                                                                                          (read [_ reader _tag _component-count]
                                                                                            (let [{:keys [index-type root-node]} (.readObject reader)
                                                                                                  loader (get-loader store)
                                                                                                  pset (with-meta (set/sorted-set-by (index-type->cmp index-type) loader)
                                                                                                         {:index-type index-type})]
                                                                                              (set! (._root pset)
                                                                                                    (set/map->node loader root-node))
                                                                                              pset)))
                                                   "datahike.datom.Datom" (reify ReadHandler
                                                                            (read [_ reader _tag _component-count]
                                                                              (dd/datom-from-reader (.readObject reader))))}
                                                  {PersistentSortedSet
                                                   {"datahike.index.PersistentSortedSet" (reify WriteHandler
                                                                                           (write [_ writer pset]
                                                                                             (.writeTag    writer "datahike.index.PersistentSortedSet" 1)
                                                                                             (.writeObject writer {:root-node (set/-to-map (.-_root pset))
                                                                                                                   :index-type (:index-type (meta pset))})))}
                                                   Datom
                                                   {"datahike.datom.Datom" (reify WriteHandler
                                                                             (write [_ writer datom]
                                                                               (.writeTag    writer "datahike.datom.Datom" 1)
                                                                               (.writeObject writer (vec (seq datom)))))}})}))

(defmethod di/konserve-backend :datahike.index/persistent-set [_index-name store]
  store)
