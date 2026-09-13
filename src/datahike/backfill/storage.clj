(ns ^:no-doc datahike.backfill.storage
  "Private JVM PSS node storage. Caller must hold the durable build sweep
   deferral, source pin and local GC guard until publication or cancellation.
   This primitive does not establish those protections or enable online AVET."
  (:require [datahike.datom :as dd]
            [datahike.backfill.control :as control]
            [datahike.index.interface :as di]
            [datahike.index.persistent-set]
            [konserve.core :as k]
            [org.replikativ.persistent-sorted-set :as pss]
            [org.replikativ.persistent-sorted-set.impl.nodes :as nodes])
  (:import [java.util UUID Date]
           [java.net URI]
           [java.math BigInteger BigDecimal]
           [java.lang.reflect Array]
           [org.replikativ.persistent_sorted_set IStorage ANode Branch Branch$NodeState PersistentSortedSet Settings]))

(def ^:private defaults
  {:max-node-keys 4096 :max-node-weight 8388608
   :pending-node-limit 64 :pending-weight-limit 16777216
   :cache-node-limit 64 :cache-weight-limit 16777216})

(defn- fail! [type message data]
  (throw (ex-info message (assoc data :type type))))

(defn- options! [config options]
  (when-not (and (= :datahike.index/persistent-set (:index config))
                 (false? (:crypto-hash? config)))
    (fail! :backfill.storage/unsupported
           "Private builds require JVM persistent-set with crypto-hash? false."
           (select-keys config [:index :crypto-hash?])))
  (when-not (or (nil? options) (map? options))
    (fail! :backfill.storage/invalid-options "Storage options must be a map." {}))
  (when (some #(not (contains? defaults %)) (keys options))
    (fail! :backfill.storage/invalid-options "Unknown storage option." {}))
  (let [options (merge defaults options)]
    (doseq [[key value] options]
      (when-not (and (integer? value) (pos? value) (<= value Integer/MAX_VALUE))
        (fail! :backfill.storage/invalid-options "Storage limits must be positive bounded integers."
               {key value})))
    (when (> (:max-node-weight options) (:pending-weight-limit options))
      (fail! :backfill.storage/invalid-options "A node must fit in the pending weight limit." {}))
    options))

(defn validate-options!
  "Validate only private storage budgets without creating storage or doing I/O.
   create! additionally checks the actual database/backend capabilities."
  [options]
  (options! {:index :datahike.index/persistent-set :crypto-hash? false} options))

(defn- weight!
  "Conservative payload accounting units, NOT measured JVM retained heap.
   Charge each occurrence (including sharing) and bound traversal/depth."
  [value limit]
  (let [total (volatile! 0)]
    (letfn [(charge! [n]
              (when (> (vswap! total + n) limit)
                (fail! :backfill.storage/node-too-large "Node exceeds its weight limit." {:limit limit})))
            (visit! [x depth]
              (when (> depth 64)
                (fail! :backfill.storage/node-too-large "Node exceeds its nesting limit." {}))
              (charge! 64)
              (when-let [metadata (meta x)] (visit! metadata (inc depth)))
              (cond
                (nil? x) nil
                (string? x) (charge! (* 2 (.length ^String x)))
                (or (keyword? x) (symbol? x))
                (do (visit! (name x) (inc depth)) (visit! (namespace x) (inc depth)))
                (instance? BigInteger x) (charge! (+ 32 (quot (+ 7 (.bitLength ^BigInteger x)) 8)))
                (instance? clojure.lang.BigInt x) (visit! (.toBigInteger ^clojure.lang.BigInt x) (inc depth))
                (instance? BigDecimal x) (visit! (.unscaledValue ^BigDecimal x) (inc depth))
                (instance? clojure.lang.Ratio x)
                (do (visit! (numerator x) (inc depth)) (visit! (denominator x) (inc depth)))
                (or (instance? Long x) (instance? Integer x) (instance? Short x)
                    (instance? Byte x) (instance? Double x) (instance? Float x)
                    (boolean? x) (char? x) (instance? UUID x) (instance? Date x)) nil
                (instance? URI x) (visit! (str x) (inc depth))
                (instance? datahike.datom.Datom x)
                (doseq [v [(.-e ^datahike.datom.Datom x) (.-a ^datahike.datom.Datom x)
                           (.-v ^datahike.datom.Datom x)
                           (dd/datom-tx x) (dd/datom-added x)]]
                  (visit! v (inc depth)))
                (.isArray (class x))
                (let [n (Array/getLength x)]
                  (charge! (* 8 n))
                  (when-not (.isPrimitive (.getComponentType (class x)))
                    (dotimes [i n] (visit! (Array/get x i) (inc depth)))))
                (map? x) (doseq [[key value] x] (visit! key (inc depth)) (visit! value (inc depth)))
                (or (sequential? x) (set? x)) (doseq [v x] (visit! v (inc depth)))
                :else (fail! :backfill.storage/unsupported-value
                             "Unsupported private node value." {:class (str (class x))})))]
      (visit! value 0)
      @total)))

(defn- blob! [node options]
  (when-not (instance? ANode node)
    (fail! :backfill.storage/unsupported-value "Expected a persistent-set node." {}))
  (let [^ANode node node]
    (when (> (alength (.-_keys node)) (:max-node-keys options))
      (fail! :backfill.storage/node-too-large "Node exceeds its key capacity limit." {}))
    ;; Slot projection can recursively materialize buffered descendants before
    ;; weight accounting. Refuse it rather than claiming that allocation bounded.
    (when (pos? (.diffBufSize (.-_settings node)))
      (fail! :backfill.storage/unsupported "Private storage does not support diff buffers." {}))
    (let [blob (nodes/node->blob node)]
      [blob (weight! blob (:max-node-weight options))])))

(defn- live! [state]
  (when (:closed? @state)
    (fail! :backfill.storage/closed "Private storage is closed." {}))
  (when (:failed? @state)
    (fail! :backfill.storage/failed "Private storage failed; discard the build." {})))

(defn- node-from-blob
  ;; reader-context memoizes settings without a size limit. Keep its lifetime
  ;; local so heterogeneous historical node settings cannot grow a hidden cache.
  ([blob] (node-from-blob blob false))
  ([blob private-read?]
   (let [context (nodes/reader-context (when private-read? {:ref-type :weak}))]
     (if (contains? blob :level)
       (nodes/blob->branch context blob)
       (nodes/blob->leaf context blob)))))

(defn- cache! [state options address blob weight]
  (when (<= weight (:cache-weight-limit options))
    (swap! state
           (fn [s]
             (let [old-weight (get-in s [:cache address :weight] 0)
                   tick (inc (:tick s))]
               (loop [s (-> s
                            (assoc :tick tick)
                            (assoc-in [:cache address] {:blob blob :weight weight :tick tick})
                            (update :cache-weight + (- weight old-weight)))]
                 (if (or (> (count (:cache s)) (:cache-node-limit options))
                         (> (:cache-weight s) (:cache-weight-limit options)))
                   (let [[key entry] (apply min-key (comp :tick val) (:cache s))]
                     (recur (-> s (update :cache dissoc key) (update :cache-weight - (:weight entry)))))
                   s)))))))

(defn- flush-state! [store state]
  (live! state)
  (try
    (doseq [[address blob _weight] (:pending @state)]
      (control/check!)
      ;; Same immutable synchronous write discipline as writing/write-pending-kvs!,
      ;; without introducing a writing -> storage -> writing namespace cycle.
      (k/assoc store address (node-from-blob blob) {:immutable? true} {:sync? true})
      (swap! state update-in [:stats :persisted] inc))
    (swap! state assoc :pending [] :pending-weight 0)
    nil
    (catch Throwable e
      (swap! state assoc :failed? true)
      (throw e))))

(defrecord PrivateStorage [store options state]
  IStorage
  (store [_ node]
    (control/check!)
    (locking state
      (live! state)
      (let [[blob weight] (blob! node options)
            address (UUID/randomUUID)]
        (when (or (>= (count (:pending @state)) (:pending-node-limit options))
                  (> (+ (:pending-weight @state) weight) (:pending-weight-limit options)))
          (flush-state! store state))
        (swap! state (fn [s] (-> s
                                 (update :pending conj [address blob weight])
                                 (update :pending-weight + weight)
                                 (update-in [:stats :writes] inc))))
        (cache! state options address blob weight)
        address)))
  (restore [_ address]
    (control/check!)
    (locking state
      (live! state)
      (let [[blob weight]
            (or (when-let [entry (get-in @state [:cache address])]
                  [(:blob entry) (:weight entry)])
                (some (fn [[key blob weight]] (when (= key address) [blob weight])) (:pending @state))
                (let [node (k/get store address nil {:sync? true})]
                  (when-not node
                    (fail! :backfill.storage/node-not-found "Private build node not found." {:address address}))
                  (swap! state update-in [:stats :reads] inc)
                  (blob! node options)))]
        (cache! state options address blob weight)
        ;; Never cache returned mutable nodes/child pointers: only detached blobs.
        (node-from-blob blob true))))
  (accessed [_ _address]
    (locking state (live! state) (swap! state update-in [:stats :accessed] inc))
    nil)
  (markFreed [_ _address] nil)
  (isFreed [_ _address] false)
  (freedInfo [_ _address] nil)
  java.io.Closeable
  (close [_]
    (locking state
      (swap! state assoc :closed? true :pending [] :pending-weight 0 :cache {} :cache-weight 0))))

(defn create!
  "Create private storage without I/O. Store must already carry PSS serializers.
   Caller owns GC protection. No freelist, source cache or pending buffers are
   inherited. Weight limits bound accounted payload, not exact JVM heap or the
   backend's own caching. One candidate node/projection can coexist with the
   bounded buffers during admission; backend decoding and the caller's tree
   frontier are outside those budgets. Node topology is isolated, but stored
   values must not be mutated. Diff-buffer nodes are explicitly unsupported."
  ([store config] (create! store config nil))
  ([store config options]
   (let [options (options! config options)]
     (when (pos? (get store :datahike/diff-buf-size 0))
       (fail! :backfill.storage/unsupported "Private storage does not support diff buffers." {}))
     (->PrivateStorage (dissoc store :storage di/node-cache-key) options
                       (atom {:pending [] :pending-weight 0 :cache {} :cache-weight 0 :tick 0
                              :closed? false :failed? false
                              :stats {:writes 0 :reads 0 :accessed 0 :persisted 0}})))))

(defn build-store
  "Store view for init-index-sorted. It does not expose live storage buffers."
  [storage]
  (assoc (:store storage) :storage storage))

(defn- resident-datoms
  "Read existing topology without child(), which fills shared branch caches.
   Missing addressed children load privately. Retention is the traversal frontier
   plus the caller's already-resident immutable source tree."
  [storage ^ANode node]
  (lazy-seq
   (control/check!)
   (blob! node (:options storage))
   (if (instance? Branch node)
     (let [^Branch$NodeState snapshot (.-_state ^Branch node)
           children (.-children snapshot)
           addresses (.-addresses snapshot)]
       (mapcat (fn [i]
                 (let [resident (when children
                                  (.readReference (.-_settings node) (aget ^objects children i)))
                       address (when addresses (aget ^objects addresses i))
                       child (or resident
                                 (when address (.restore ^IStorage storage address))
                                 (fail! :backfill.storage/invalid-source "Source child has no node or address." {}))]
                   (resident-datoms storage child)))
               (range (.-_len node))))
     (map #(aget (.-_keys node) %) (range (.-_len node))))))

(defn copy-index!
  "Bind a durable source tree to private read storage without sharing mutable
   node topology or touching live storage/cache. A resident (including fused)
   root is projected/copied under the node admission limit; a cold root loads
   privately. Never flushes the source. Nonempty unflushed trees are refused
   unless :resident-source? opts into a streaming private materialization; the
   coordinator may enable that for committed memory-backend snapshots only.
   Existing node decoding/projection and each copied root are additional bounded
   candidates outside pending/cache weights. Stored domain values remain shared
   immutable values. Weak child references prevent hidden strong subtree caches."
  ([storage index] (copy-index! storage index nil))
  ([storage index options]
   (when-not (and (or (nil? options) (map? options))
                  (every? #{:resident-source?} (keys options))
                  (or (not (contains? options :resident-source?))
                      (boolean? (:resident-source? options))))
     (fail! :backfill.storage/invalid-options "Invalid source copy options." {}))
   (when-not (instance? PersistentSortedSet index)
     (fail! :backfill.storage/unsupported "Expected a persistent-set source tree." {}))
   (locking (:state storage)
     (live! (:state storage))
     (let [^PersistentSortedSet index index
          ;; Match PSS's acquire ordering for a concurrently warmed source root.
           root-ref (.-_root index)
           ^Settings settings (.-_settings index)
           resident (.readReference settings root-ref)
           address (.-_address index)
           family (:index-type (meta index))]
       (when-not (contains? #{:eavt :aevt :avet} family)
         (fail! :backfill.storage/unsupported "Source tree lacks index-family metadata." {}))
       (when (and (nil? address) (not (zero? (.-_count index))) (not (:resident-source? options)))
         (fail! :backfill.storage/unflushed-source "Source must be durably flushed before copying." {}))
       (when (pos? (.diffBufSize settings))
         (fail! :backfill.storage/unsupported "Private source copies do not support diff buffers." {}))
       (if (and (nil? address) (not (zero? (.-_count index))))
         (do
           (when-not resident (fail! :backfill.storage/invalid-source "Resident source root is missing." {}))
           (let [tree (pss/from-sorted-seq
                       (dd/index-type->cmp-quick family false)
                       (resident-datoms storage resident)
                       {:storage storage :meta (meta index) :ref-type :weak
                        :branching-factor (.branchingFactor settings) :diff-buf-size 0})]
             (flush-state! (:store storage) (:state storage))
             tree))
         (let [root (when resident
                      (node-from-blob (first (blob! resident (:options storage))) true))
               weak-settings (nodes/settings-for (.branchingFactor settings) 0 nil :weak
                                                 (.descriptor (.boundary settings)))]
           (PersistentSortedSet. (meta index) (dd/index-type->cmp-quick family false)
                                 address storage root (.-_count index) weak-settings (.-_version index))))))))

(defn flush!
  "Synchronously persist private pending nodes. A failed flush poisons the build."
  [storage]
  (locking (:state storage)
    (flush-state! (:store storage) (:state storage))))

(defn resources
  "Bounded counters; does not expose or retain node payloads."
  [storage]
  (let [s @(:state storage)]
    (merge (select-keys s [:pending-weight :cache-weight :closed? :failed?])
           (:stats s)
           {:pending-nodes (count (:pending s)) :cached-nodes (count (:cache s))})))
