(ns ^:no-doc datahike.lru
  (:require [#?(:clj clojure.core.cache :cljs cljs.cache) :refer [defcache CacheProtocol]]
            #?(:clj clojure.data.priority-map
               :cljs tailrecursion.priority-map)))

(declare assoc-lru cleanup-lru)

#?(:cljs
   (deftype LRU [key-value gen-key key-gen gen limit]
     IAssociative
     (-assoc [this k v] (assoc-lru this k v))
     (-contains-key? [_ k] (-contains-key? key-value k))
     ILookup
     (-lookup [_ k]    (-lookup key-value k nil))
     (-lookup [_ k nf] (-lookup key-value k nf))
     IPrintWithWriter
     (-pr-writer [_ writer opts]
       (-pr-writer (persistent! key-value) writer opts)))
   :clj
   (deftype LRU [^clojure.lang.Associative key-value gen-key key-gen gen limit]
     clojure.lang.ILookup
     (valAt [_ k]           (.valAt key-value k))
     (valAt [_ k not-found] (.valAt key-value k not-found))
     clojure.lang.Associative
     (containsKey [_ k] (.containsKey key-value k))
     (entryAt [_ k]     (.entryAt key-value k))
     (assoc [this k v]  (assoc-lru this k v))))

(defn assoc-lru [^LRU lru k v]
  (let [key-value (.-key-value lru)
        gen-key   (.-gen-key lru)
        key-gen   (.-key-gen lru)
        gen       (.-gen lru)
        limit     (.-limit lru)]
    (if-let [g (key-gen k nil)]
      (->LRU (assoc key-value k v)
             (-> gen-key
                 (dissoc g)
                 (assoc gen k))
             (assoc key-gen k gen)
             (inc gen)
             limit)
      (cleanup-lru
       (->LRU (assoc key-value k v)
              (assoc gen-key gen k)
              (assoc key-gen k gen)
              (inc gen)
              limit)))))

(defn cleanup-lru [^LRU lru]
  (if (> (count (.-key-value lru)) (.-limit lru))
    (let [key-value (.-key-value lru)
          gen-key   (.-gen-key lru)
          key-gen   (.-key-gen lru)
          gen       (.-gen lru)
          limit     (.-limit lru)
          [g k]     (first gen-key)]
      (->LRU (dissoc key-value k)
             (dissoc gen-key g)
             (dissoc key-gen k)
             gen
             limit))
    lru))

(defn lru [limit]
  (->LRU {} (sorted-map) {} 0 limit))

(defn- empty-weighted-state [limit weight-limit weigh]
  {:key-value    {}
   :gen-key      (sorted-map)
   :key-gen      {}
   :gen          0
   :weights      {}
   :total-weight 0
   :limit        limit
   :weight-limit weight-limit
   :weigh        weigh})

(defn- forget-generation [{:keys [gen-key key-gen] :as state} k]
  (if-let [g (get key-gen k)]
    (assoc state :gen-key (dissoc gen-key g))
    state))

(defn- record-generation [{:keys [gen-key key-gen gen] :as state} k]
  (assoc state
         :gen-key (assoc gen-key gen k)
         :key-gen (assoc key-gen k gen)
         :gen     (inc gen)))

(defn- reweigh [{:keys [weights total-weight] :as state} k w]
  (assoc state
         :weights      (assoc weights k w)
         :total-weight (+ (- total-weight (get weights k 0)) w)))

(defn- store-value [state k v]
  (update state :key-value assoc k v))

(defn- oldest-key [{:keys [gen-key]}]
  (val (first gen-key)))

(defn- evict-key [{:keys [weights] :as state} k]
  (-> state
      (forget-generation k)
      (update :key-value dissoc k)
      (update :key-gen dissoc k)
      (update :weights dissoc k)
      (update :total-weight - (get weights k 0))))

(defn- over-budget? [{:keys [key-value total-weight limit weight-limit]}]
  (and (> (count key-value) 1)
       (or (> (count key-value) limit)
           (and (pos? weight-limit)
                (> total-weight weight-limit)))))

(defn- shrink-to-budget [state]
  (if (over-budget? state)
    (recur (evict-key state (oldest-key state)))
    state))

(defn- entry-weight [{:keys [weigh weight-limit]} v]
  (if (pos? weight-limit) (weigh v) 0))

(defn- put-entry [state k v]
  (-> state
      (forget-generation k)
      (record-generation k)
      (reweigh k (entry-weight state v))
      (store-value k v)
      shrink-to-budget))

#?(:cljs
   (deftype WeightedLRU [state]
     IAssociative
     (-assoc [_ k v] (WeightedLRU. (put-entry state k v)))
     (-contains-key? [_ k] (contains? (:key-value state) k))
     ILookup
     (-lookup [_ k]    (get (:key-value state) k))
     (-lookup [_ k nf] (get (:key-value state) k nf))
     IPrintWithWriter
     (-pr-writer [_ writer opts] (-pr-writer (:key-value state) writer opts)))
   :clj
   (deftype WeightedLRU [state]
     clojure.lang.ILookup
     (valAt [_ k]           (get (:key-value state) k))
     (valAt [_ k not-found] (get (:key-value state) k not-found))
     clojure.lang.Associative
     (containsKey [_ k] (contains? (:key-value state) k))
     (entryAt [_ k]     (find (:key-value state) k))
     (assoc [_ k v]     (WeightedLRU. (put-entry state k v)))))

(defn weighted-lru
  "LRU bounded by BOTH an entry-count `limit` and a cumulative `weight-limit`,
   where `weigh` maps a stored value to its non-negative weight. The
   most-recently-used entry is always retained; a `weight-limit` of 0 disables
   the weight budget, leaving a plain count-bounded LRU."
  ([limit weight-limit] (weighted-lru limit weight-limit count))
  ([limit weight-limit weigh]
   (->WeightedLRU (empty-weighted-state limit weight-limit weigh))))

(defcache LRUDatomCache [cache lru counts n-total-datoms tick datom-limit]
  CacheProtocol
  (lookup [_ item]
          (get cache item))
  (lookup [_ item not-found]
          (get cache item not-found))
  (has? [_ item]
        (contains? cache item))
  (hit [_ item]
       (let [tick+ (inc tick)]
         (LRUDatomCache. cache
                         (if (contains? cache item)
                           (assoc lru item tick+)
                           lru)
                         counts
                         n-total-datoms
                         tick+
                         datom-limit)))
  (miss [this item result]
        (let [tick+ (inc tick)
              n-new-datoms (count result)
              new-size (+ n-total-datoms n-new-datoms)
              [c l n s] (if (contains? lru item)
                          [(dissoc cache item)
                           (dissoc lru item)
                           (dissoc counts item)
                           (- new-size (get counts item))]
                          [cache lru counts new-size])
              [c l n s] (loop [c c l l n n s s]
                          (if (> s datom-limit)
                            (let [k (first (peek lru))]
                              (if-let [x (get n k)]
                                (recur (dissoc c k)
                                       (dissoc l k)
                                       (dissoc n k)
                                       (- s x))
                                [c l n s]))
                            [c l n s]))]
          (LRUDatomCache. (assoc c item result)
                          (assoc l item tick+)
                          (assoc n item n-new-datoms)
                          s
                          tick+
                          datom-limit)))
  (evict [this key]
         (if (contains? cache key)
           (LRUDatomCache. (dissoc cache key)
                           (dissoc lru key)
                           (dissoc counts key)
                           (- n-total-datoms (get counts key))
                           (inc tick)
                           datom-limit)
           this))
  (seed [_ base]
        (LRUDatomCache. base
                        (into #?(:clj (clojure.data.priority-map/priority-map)
                                 :cljs (tailrecursion.priority-map/priority-map))
                              (map #(vector % 0)
                                   (keys base)))
                        (into {}
                              (map #(vector % (count (get base %)))
                                   (keys base)))
                        0
                        0
                        datom-limit))
  Object
  (toString [_]
            (str cache \, \space lru \, \space counts \, \space n-total-datoms \, \space tick \, \space datom-limit)))

(defn lru-datom-cache-factory
  "Returns an LRU cache with the cache and usage-table initialied to `base` --
   each entry is initialized with the same usage value.
   This function takes an optional `:threshold` argument that defines the maximum number
   of elements in the cache before the LRU semantics apply (default is 32)."
  [base & {threshold :threshold :or {threshold 32}}]
  {:pre [(number? threshold) (< 0 threshold)
         (map? base)]}
  #?(:clj (atom (clojure.core.cache/seed (LRUDatomCache. {} (clojure.data.priority-map/priority-map) {} 0 0 threshold) base))
     :cljs (atom (cljs.cache/seed (LRUDatomCache. {} (tailrecursion.priority-map/priority-map) {} 0 0 threshold) base))))
