(ns ^:no-doc datahike.index.persistent-set
  (:require [me.tonsky.persistent-sorted-set :as psset]
            [me.tonsky.persistent-sorted-set.arrays :as arrays]
            [clojure.core.async :as async]
            [clojure.core.cache :as cache]
            [clojure.core.cache.wrapped :as wrapped]
            [datahike.datom :as dd]
            [datahike.constants :refer [tx0 txmax]]
            [datahike.index.interface :as di :refer [IIndex]]
            [konserve.core :as k]
            [konserve.serializers :refer [fressian-serializer]]
            [superv.async :refer [<?? S]]
            [hasch.core :refer [uuid]]
            [taoensso.timbre :refer [warn debug]])
  #?(:clj (:import [datahike.datom Datom]
                   [org.fressian.handlers WriteHandler ReadHandler]
                   [me.tonsky.persistent_sorted_set PersistentSortedSet IStorage Leaf Branch ANode]
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

(defn multi-cmp [cmps d1 d2]
  (reduce (fn [old cmp] (let [res (cmp d1 d2)]
                          (if (nil? res)
                            (reduced old)
                            (if (not= 0 res)
                              (reduced res)
                              old))))
          0
          cmps))

(def index-type->kwseq
  {:eavt [:e :a :v :tx :added]
   :aevt [:a :e :v :tx :added]
   :avet [:a :v :e :tx :added]})

(defn index-type->slice-cmp [index-type from to]
  (let [cmps (->> (index-type index-type->kwseq)
                  (take-while #(not (and (nil? (% from))
                                         (nil? (% to)))))
                  (mapv dd/cmp-val))]
    (partial multi-cmp cmps)))

(defn slice [pset from to index-type]
  (psset/slice pset from to (index-type->slice-cmp index-type from to)))

(defn remove-datom [pset ^Datom datom index-type]
  (psset/disj pset datom (index-type->cmp-quick index-type false)))

(defn insert [pset ^Datom datom index-type]
  (if (first (psset/slice pset
                          (dd/datom (.-e datom) (.-a datom) (.-v datom) tx0)
                          (dd/datom (.-e datom) (.-a datom) (.-v datom) txmax)
                          (index-type->cmp-quick index-type)))
    pset
    (psset/conj pset datom (index-type->cmp-quick index-type))))

(defn temporal-insert [pset ^Datom datom index-type]
  (psset/conj pset datom (index-type->cmp-quick index-type false)))

(defn upsert [pset ^Datom datom index-type old-datom]
  (psset/conj (if old-datom
                (remove-datom pset old-datom index-type)
                pset)
              datom (index-type->cmp-quick index-type)))

(defn temporal-upsert [pset ^Datom datom index-type {old-val :v}]
  (let [{:keys [e a v tx added]} datom]
    (if added
      (if old-val
        (if (= v old-val)
          pset
          (-> pset
              (psset/conj (dd/datom e a old-val tx false)
                          (index-type->cmp-quick index-type false))
              (psset/conj datom
                          (index-type->cmp-quick index-type false))))
        (psset/conj pset datom (index-type->cmp-quick index-type false)))
      (if old-val
        (psset/conj pset
                    (dd/datom e a old-val tx false)
                    (index-type->cmp-quick index-type false))
        pset))))

(extend-type PersistentSortedSet
  IIndex
  (-slice [^PersistentSortedSet pset from to index-type]
    (slice pset from to index-type))
  (-all [pset]
    (identity pset))
  (-seq [^PersistentSortedSet pset]
    (seq pset))
  (-count [^PersistentSortedSet pset]
    (count pset))
  (-insert [^PersistentSortedSet pset datom index-type _op-count]
    (insert pset datom index-type))
  (-temporal-insert [^PersistentSortedSet pset datom index-type _op-count]
    (psset/conj pset datom (index-type->cmp-quick index-type)))
  (-upsert [^PersistentSortedSet pset datom index-type _op-count old-datom]
    (upsert pset datom index-type old-datom))
  (-temporal-upsert [^PersistentSortedSet pset datom index-type _op-count old-val]
    (temporal-upsert pset datom index-type old-val))
  (-remove [^PersistentSortedSet pset datom index-type _op-count]
    (remove-datom pset datom index-type))
  (-flush [^PersistentSortedSet pset _]
    (psset/store pset)
    pset)
  (-transient [^PersistentSortedSet pset]
    (transient pset))
  (-persistent! [^PersistentSortedSet pset]
    (persistent! pset)))

(defn- gen-address [^ANode node crypto-hash?]
  (if crypto-hash?
    (if (instance? Branch node)
      (uuid (vec (.addresses ^Branch node)))
      (uuid (mapv (comp vec seq) (.keys node))))
    (uuid)))

(defrecord CachedStorage [store config cache stats]
  IStorage
  (store [_ node]
    (swap! stats update :writes inc)
    (let [address (gen-address node (:crypto-hash? config))
          _ (debug "writing storage: " address " crypto: " (:crypto-hash? config))]
      (k/assoc store address node {:sync? true})
      (wrapped/miss cache address node)
      address))
  (accessed [_ address]
    (debug "accessing storage: " address)
    (swap! stats update :accessed inc)
    (wrapped/hit cache address)
    nil)
  (restore [_ address]
    (debug "reading: " address)
    ;; persistent sorted set accesses the restored node immediately again (above)
    ;; so we avoid double counting
    #_(wrapped/miss cache address node)
    (swap! stats update :reads inc)
    (k/get store address nil {:sync? true})))

(def init-stats {:writes   0
                 :reads    0
                 :accessed 0})

(defn create-storage [store config]
  (CachedStorage. store config
                  (atom (cache/lru-cache-factory {} :threshold (:store-cache-size config)))
                  (atom init-stats)))

(def ^:const BRANCHING_FACTOR 512)

(defmethod di/empty-index :datahike.index/persistent-set [_index-name store index-type _]
  (psset/set-branching-factor! BRANCHING_FACTOR)
  (let [pset (psset/sorted-set-by (index-type->cmp-quick index-type false))]
    (set! (.-_storage pset) (:storage store))
    (with-meta pset
      {:index-type index-type})))

(defmethod di/init-index :datahike.index/persistent-set [_index-name store datoms index-type _ {:keys [indexed]}]
  (psset/set-branching-factor! BRANCHING_FACTOR)
  (let [arr (if (= index-type :avet)
              (->> datoms
                   (filter #(contains? indexed (.-a ^Datom %)))
                   to-array)
              (cond-> datoms
                (not (arrays/array? datoms))
                (arrays/into-array)))
        _ (arrays/asort arr (index-type->cmp-quick index-type false))
        pset (psset/from-sorted-array (index-type->cmp-quick index-type false) arr)]
    (set! (.-_storage pset) (:storage store))
    (with-meta pset
      {:index-type index-type})))

(defmethod di/add-konserve-handlers :datahike.index/persistent-set [config store]
  ;; deal with circular reference between storage and store
  (let [storage (atom nil)
        store
        (assoc store
               :serializers {:FressianSerializer (fressian-serializer
                                                  {"datahike.index.PersistentSortedSet"
                                                   (reify ReadHandler
                                                     (read [_ reader _tag _component-count]
                                                       (let [{:keys [meta address count]} (.readObject reader)
                                                             cmp                          (index-type->cmp-quick (:index-type meta) false)]
                                                         ;; The following fields are reset as they cannot be accessed from outside:
                                                         ;; - 'edit' is set to false, i.e. the set is assumed to be persistent, not transient
                                                         ;; - 'version' is set back to 0
                                                         (PersistentSortedSet. meta cmp address @storage nil count nil 0))))
                                                   "datahike.index.PersistentSortedSet.Leaf"
                                                   (reify ReadHandler
                                                     (read [_ reader _tag _component-count]
                                                       (let [{:keys [keys level]} (.readObject reader)]
                                                         (Leaf. keys))))
                                                   "datahike.index.PersistentSortedSet.Branch"
                                                   (reify ReadHandler
                                                     (read [_ reader _tag _component-count]
                                                       (let [{:keys [keys level addresses]} (.readObject reader)]
                                                         (Branch. (int level) keys (seq addresses)))))
                                                   "datahike.datom.Datom"
                                                   (reify ReadHandler
                                                     (read [_ reader _tag _component-count]
                                                       (dd/datom-from-reader (.readObject reader))))}
                                                  {me.tonsky.persistent_sorted_set.PersistentSortedSet
                                                   {"datahike.index.PersistentSortedSet"
                                                    (reify WriteHandler
                                                      (write [_ writer  pset]
                                                        (assert (not (nil? (.-_address  ^PersistentSortedSet pset)))
                                                                "Must be flushed.")
                                                        (.writeTag writer "datahike.index.PersistentSortedSet" 1)
                                                        (.writeObject writer {:meta    (meta pset)
                                                                              :address (.-_address  ^PersistentSortedSet pset)
                                                                              :count   (count pset)})))}
                                                   me.tonsky.persistent_sorted_set.Leaf
                                                   {"datahike.index.PersistentSortedSet.Leaf"
                                                    (reify WriteHandler
                                                      (write [_ writer leaf]
                                                        (.writeTag writer "datahike.index.PersistentSortedSet.Leaf" 1)
                                                        (.writeObject writer {:level (.level ^Leaf leaf)
                                                                              :keys  (.keys ^Leaf leaf)})))}
                                                   me.tonsky.persistent_sorted_set.Branch
                                                   {"datahike.index.PersistentSortedSet.Branch"
                                                    (reify WriteHandler
                                                      (write [_ writer node]
                                                        (.writeTag writer "datahike.index.PersistentSortedSet.Branch" 1)
                                                        (.writeObject writer {:level     (.level ^Branch node)
                                                                              :keys      (.keys ^Branch node)
                                                                              :addresses (.addresses ^Branch node)})))}
                                                   datahike.datom.Datom
                                                   {"datahike.datom.Datom"
                                                    (reify WriteHandler
                                                      (write [_ writer datom]
                                                        (.writeTag writer "datahike.datom.Datom" 1)
                                                        (.writeObject writer (vec (seq ^Datom datom)))))}})})]
    (reset! storage (or (:storage store)
                        (create-storage store config)))
    (assoc store :storage @storage)))

(defmethod di/konserve-backend :datahike.index/persistent-set [_index-name store]
  store)

(defmethod di/default-index-config :datahike.index/persistent-set [_index-name]
  {})
