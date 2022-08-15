(ns ^:no-doc datahike.index.persistent-set
  (:require [me.tonsky.persistent-sorted-set :as set]
            [me.tonsky.persistent-sorted-set.arrays :as arrays]
            [clojure.core.async :as async]
            [clojure.core.cache.wrapped :as wrapped]
            [clojure.data :as data]
            [datahike.datom :as dd]
            [datahike.constants :refer [tx0 txmax]]
            [datahike.index.interface :as di :refer [IIndex]]
            [konserve.core :as k]
            [konserve.serializers :refer [fressian-serializer]]
            [hasch.core :refer [uuid]])
  #?(:clj (:import [datahike.datom Datom]
                   [org.fressian.handlers WriteHandler ReadHandler]
                   [me.tonsky.persistent_sorted_set PersistentSortedSet StorageBackend Leaf Node Edit]
                   [java.util UUID])))

(defn index-type->cmp
  ([index-type] (index-type->cmp index-type true))
  ([index-type current?] (if current?
                           (case index-type
                             :aevt dd/cmp-datoms-aevt
                             :avet dd/cmp-datoms-avet
                             dd/cmp-datoms-eavt)
                           (case index-type
                             :aevt dd/cmp-temporal-datoms-aevt
                             :avet dd/cmp-temporal-datoms-avet
                             dd/cmp-temporal-datoms-eavt))))

(defn index-type->cmp-quick
  ([index-type] (index-type->cmp-quick index-type true))
  ([index-type current?] (if current?
                           (case index-type
                             :aevt dd/cmp-datoms-aevt-quick
                             :avet dd/cmp-datoms-avet-quick
                             dd/cmp-datoms-eavt-quick)
                           (case index-type
                             :aevt dd/cmp-temporal-datoms-aevt-quick
                             :avet dd/cmp-temporal-datoms-avet-quick
                             dd/cmp-temporal-datoms-eavt-quick))))

(defn index-type->slice-cmp
  [index-type from to]
  (let [cmps (->> (case index-type
                    :eavt [:e :a :v :tx :added]
                    :aevt [:a :e :v :tx :added]
                    :avet [:a :v :e :tx :added])
                  (take-while #(not (and (nil? (% from))
                                         (nil? (% to)))))
                  (mapv dd/cmp-val))]
    (fn [d1 d2]
      (reduce (fn [old cmp] (let [res (cmp d1 d2)]
                              (if (nil? res)
                                (reduced old)
                                (if (not= 0 res)
                                  (reduced res)
                                  old))))
              0
              cmps))))

(defn slice [pset from to index-type]
  (set/slice pset from to (index-type->slice-cmp index-type from to)))

(defn remove-datom [pset datom index-type]
  (set/disj pset datom (index-type->cmp-quick index-type false)))

(defn insert [pset datom index-type]
  (if (first (set/slice pset
                        (dd/datom (.-e datom) (.-a datom) (.-v datom) tx0)
                        (dd/datom (.-e datom) (.-a datom) (.-v datom) txmax)
                        (index-type->cmp-quick index-type)))
    pset
    (set/conj pset datom (index-type->cmp-quick index-type))))

(defn temporal-insert [pset datom index-type]
  (set/conj pset datom (index-type->cmp-quick index-type false)))

(defn upsert [pset datom index-type]
  (-> (if-let [old (first (cond->> (slice pset
                                          (dd/datom (.-e datom) (.-a datom) nil tx0)
                                          (dd/datom (.-e datom) (.-a datom) nil txmax)
                                          index-type)
                            (= :avet index-type)
                            (filter #(= (.-e datom) (.-e %)))))]
        (remove-datom pset old index-type)
        pset)
      (set/conj datom (index-type->cmp-quick index-type))))

(defn temporal-upsert [pset datom index-type old-val]
  (let [{:keys [e a v tx added]} datom]
    (if added
      (if old-val
        (if (= v old-val)
          pset
          (-> pset
              (set/conj (dd/datom e a old-val tx false)
                        (index-type->cmp-quick index-type false))
              (set/conj datom
                        (index-type->cmp-quick index-type false))))
        (set/conj pset datom (index-type->cmp-quick index-type false)))
      (if old-val
        (set/conj pset
                  (dd/datom e a old-val tx false)
                  (index-type->cmp-quick index-type false))
        pset))))

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
  (-temporal-upsert [pset datom index-type _op-count old-val]
    (temporal-upsert pset datom index-type old-val))
  (-remove [pset datom index-type _op-count]
    (remove-datom pset datom index-type))
  (-flush [pset _]
    (set! (.-_root pset)
          (set/-flush (.-_root pset)))
    pset)
  (-transient [pset]
    ;(transient pset)
    pset)
  (-persistent! [pset]
    ;(persistent! pset)
    pset))

(def c (atom 0))

(defn get-storage [konserve-store]
  ;; TODO use konserve cache? ideally this cache should be shared per database
  (let [cache (wrapped/lru-cache-factory {} :threshold 1000)]
    (add-watch cache :free-memory
               (fn [_ _ old new]
                 (let [[delta _ _] (data/diff (set (keys old)) (set (keys new)))]
                   (doseq [node delta]
                     #_(println "freeing" (.-_address node))
                     (locking node
                       (set! (.-_isLoaded node) false)
                       (set! (.-_children node) nil))))))
    (proxy [StorageBackend] []
      (hitCache [node]
        #_(println "hitting" (.-_address node))
        (wrapped/hit cache node)
        nil)
      (load [node]
        (let [address (.-_address node)]
          (wrapped/lookup-or-miss cache node
                                  (fn [_]
                                    ;; TODO: use synchronous calls as soon as available
                                    #_(println "loading" (.-_address node))
                                    (->> (async/<!! (k/get konserve-store address))
                                         (map (fn [m]
                                                (set/map->node
                                                 this
                                                 (update m :keys (fn [keys] (mapv #(when-let [datom-seq (seq %)]
                                                                                     (dd/datom-from-reader datom-seq))
                                                                                  keys))))))
                                         (into-array Leaf))))))
      (store [node children]
        (let [children-as-maps (mapv (fn [node] (-> node
                                                    set/-to-map
                                                    (update :keys (fn [keys] (mapv (comp vec seq) keys)))))
                                     children)
              address (uuid children-as-maps)]
          #_(println "storing" address)
          (wrapped/miss cache node children)
          (async/<!! (k/assoc konserve-store address children-as-maps))
          address)))))

(defmethod di/empty-index :datahike.index/persistent-set [_index-name store index-type _]
  (with-meta (set/sorted-set-by (index-type->cmp-quick index-type false) (get-storage store))
    {:index-type index-type}))

(defmethod di/init-index :datahike.index/persistent-set [_index-name store datoms index-type _ {:keys [indexed]}]
  (let [arr (if (= index-type :avet)
              (->> datoms
                   (filter #(contains? indexed (.-a ^Datom %)))
                   to-array)
              (cond-> datoms
                (not (arrays/array? datoms))
                (arrays/into-array)))]
    (arrays/asort arr (index-type->cmp-quick index-type false))
    (with-meta (set/from-sorted-array (index-type->cmp-quick index-type false) arr (get-storage store))
      {:index-type index-type})))

(defmethod di/add-konserve-handlers :datahike.index/persistent-set [_index-name store]
  (assoc store :serializers {:FressianSerializer (fressian-serializer
                                                  {"datahike.index.PersistentSortedSet"
                                                   (reify ReadHandler
                                                     (read [_ reader _tag _component-count]
                                                       (let [{:keys [meta root count]} (.readObject reader)
                                                             storage (get-storage store)
                                                             ;        _ (println "read index type" (:index-type meta))
                                                             cmp (index-type->cmp-quick (:index-type meta) false)]
                                                          ;; The following fields are reset as they cannot be accessed from outside:
                                                          ;; - 'edit' is set to false, i.e. the set is assumed to be persistent, not transient
                                                          ;; - 'version' is set back to 0
                                                         (PersistentSortedSet. meta cmp root count (Edit. false) 0 storage))))
                                                   "datahike.index.PersistentSortedSet.Leaf"
                                                   (reify ReadHandler
                                                     (read [_ reader _tag _component-count]
                                                       (let [{:keys [keys len dirty?]} (.readObject reader)
                                                             storage (get-storage store)
                                                             leaf (Leaf. storage (into-array Object keys) len (Edit. false))]
                                                         (set! (._isDirty leaf)
                                                               dirty?)
                                                         leaf)))
                                                   "datahike.index.PersistentSortedSet.Node"
                                                   (reify ReadHandler
                                                     (read [_ reader _tag _component-count]
                                                       (let [{:keys [keys len loaded? address children]} (.readObject reader)
                                                             storage (get-storage store)]
                                                         (if loaded?
                                                           (Node. ^StorageBackend storage (into-array Object keys) (into-array Leaf children) ^int len ^Edit (Edit. false))
                                                           (Node. ^StorageBackend storage (into-array Object keys) ^int len ^Edit (Edit. false) ^UUID (UUID/fromString address))))))
                                                   "datahike.datom.Datom"
                                                   (reify ReadHandler
                                                     (read [_ reader _tag _component-count]
                                                       (dd/datom-from-reader (.readObject reader))))}
                                                  {me.tonsky.persistent_sorted_set.PersistentSortedSet
                                                   {"datahike.index.PersistentSortedSet"
                                                    (reify WriteHandler
                                                      (write [_ writer pset]
                                                        (.writeTag writer "datahike.index.PersistentSortedSet" 1)
                                                        (.writeObject writer {:meta    (meta pset)
                                                                              :root    (._root pset)
                                                                              :count   (count pset)})))}
                                                   me.tonsky.persistent_sorted_set.Leaf
                                                   {"datahike.index.PersistentSortedSet.Leaf"
                                                    (reify WriteHandler
                                                      (write [_ writer leaf]
                                                        (.writeTag writer "datahike.index.PersistentSortedSet.Leaf" 1)
                                                        (.writeObject writer {:keys    (vec (.-_keys leaf))
                                                                              :len     (._len leaf)
                                                                              :dirty?  (._isDirty leaf)})))}
                                                   me.tonsky.persistent_sorted_set.Node
                                                   {"datahike.index.PersistentSortedSet.Node"
                                                    (reify WriteHandler
                                                      (write [_ writer node]
                                                        (.writeTag writer "datahike.index.PersistentSortedSet.Node" 1)
                                                        (.writeObject writer {:keys     (vec (.-_keys node))
                                                                              :len      (._len node)
                                                                              :children (vec (._children node))
                                                                              :loaded?  (._isLoaded node)
                                                                              :address  (.toString (._address node))})))}
                                                   datahike.datom.Datom
                                                   {"datahike.datom.Datom"
                                                    (reify WriteHandler
                                                      (write [_ writer datom]
                                                        (.writeTag writer "datahike.datom.Datom" 1)
                                                        (.writeObject writer (vec (seq datom)))))}})}))

(defmethod di/konserve-backend :datahike.index/persistent-set [_index-name store]
  store)

(defmethod di/default-index-config :datahike.index/persistent-set [_index-name]
  {})
