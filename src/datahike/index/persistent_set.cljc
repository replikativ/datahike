(ns ^:no-doc datahike.index.persistent-set
  (:require [clojure.string]
            [org.replikativ.persistent-sorted-set :as psset]
            #?(:cljs [org.replikativ.persistent-sorted-set.btset :refer [BTSet]])
            #?(:cljs [org.replikativ.persistent-sorted-set.branch :refer [Branch] :as branch])
            #?(:cljs [org.replikativ.persistent-sorted-set.leaf :refer [Leaf]])
            #?(:cljs [org.replikativ.persistent-sorted-set.impl.storage :refer [IStorage]])
            [org.replikativ.persistent-sorted-set.arrays :as arrays]
            #?@(:clj  [[clojure.core.cache :as cache]
                       [clojure.core.cache.wrapped :as wrapped]]
                :cljs [[cljs.cache :as cache]
                       [cljs.cache.wrapped :as wrapped]])
            [datahike.datom :as dd :refer [index-type->cmp-quick]]
            [datahike.constants :refer [tx0 txmax]]
            [datahike.index.audit :as audit :refer [IAuditable]]
            [datahike.index.interface :as di :refer [IIndex]]
            [datahike.tools :as dt]
            [konserve.core :as k]
            [konserve.serializers :refer [fressian-serializer]]
            #?(:cljs [fress.api :as fress])
            [hasch.core :refer [uuid squuid]]
            [replikativ.logging :as log])
  #?(:cljs (:require-macros [datahike.index.persistent-set :refer [generate-slice-comparator-constructor]]))
  #?(:clj (:import [datahike.datom Datom]
                   [org.fressian.handlers WriteHandler ReadHandler]
                   [org.replikativ.persistent_sorted_set PersistentSortedSet IStorage Leaf Branch ANode Settings Slot]
                   [java.util List])))

;; OP_BUF_V5 write-optimization knob (JVM only). A non-zero op-buf-size makes a commit
;; buffer content-only child diffs into the rewritten ancestor instead of rewriting the
;; whole spine — ~1 PUT/commit for small commits. Primary source is the persisted index
;; config key `:op-buf-size` (so it round-trips with the store and the consistency check
;; guards it); the `pss.opBufSize` JVM sysprop is a fallback for ad-hoc experiments only.
;; 0 ⇒ baseline (off) — the default, protecting existing persistent-sorted-set stores.
(defn op-buf-size ^long [index-config]
  (long (or (:op-buf-size index-config)
            ;; JVM-only sysprop fallback for ad-hoc experiments; cljs has no sysprops.
            #?(:clj  (try (Long/parseLong (System/getProperty "pss.opBufSize" "0")) (catch Exception _ 0))
               :cljs 0))))

(def index-type->kwseq
  {:eavt [:e :a :v :tx :added]
   :aevt [:a :e :v :tx :added]
   :avet [:a :v :e :tx :added]})

(defn slice-from-to-tree
  "This function generates code for deciding which datom elements that need to be compared based on which elements in the slice bounds are nil, as well as the index. Once all datom elements have been considered, `leaf-fn` is called with a vector containing the keywords of the actual elements to compare."
  [from-sym to-sym index-spec acc leaf-fn]
  (if (empty? index-spec)

    ;; When there is nothing left to compare,
    ;; return the correct comparator.
    (leaf-fn acc)

    (let [[findex & index-spec] index-spec]
      `(if (and (nil? (~findex ~from-sym))
                (nil? (~findex ~to-sym)))

         ;; Whenever both slice bounds are nil, there is nothing more to compare
         ;; and we know what comparator to return.
         ~(leaf-fn acc)

         ;; Otherwise, if at least one slie bound is non-nil, we need a comparator for
         ;; the remaining datom elements.
         ~(slice-from-to-tree from-sym to-sym index-spec (conj acc findex) leaf-fn)))))

(defn cmp-for-kwseq-sub
  "This function generates the actual body of the comparator"
  [datom0 datom1 kwseq]
  (let [result (gensym)]
    (if (empty? kwseq)
      0
      (let [[k & kwseq] kwseq]
        `(let [;; Compare the datoms at the element with key `k`
               ~result ~(dd/cmp-val-expr k datom0 datom1)]
           (cond
             ;; If it is nil, typically return 0
             (nil? ~result) 0

             ;; If it is zero, we need to proceed with the next datom element to compare.
             (zero? ~result) ~(cmp-for-kwseq-sub datom0 datom1 kwseq)

             ;; If it is non-zero, it means that this is the result of the comparison.
             :else ~result))))))

(defn cmp-for-kwseq
  "Given a sequence of keywords for datom elements to compare, generate the code for a function that performs the comparison."
  [kwseq]
  (let [datom0 (dd/type-hint-datom (gensym))
        datom1 (dd/type-hint-datom (gensym))]
    `(fn [~datom0 ~datom1] ~(cmp-for-kwseq-sub datom0 datom1 kwseq))))

(defmacro generate-slice-comparator-constructor []
  (let [index-sym (gensym)
        from-sym (gensym)
        to-sym (gensym)

        ;; List keyword sequences referring to datom elements for
        ;; all combinations of indexes and leftmost slices of the
        ;; corresponding datom elements.
        all-kwseqs (set (for [[_ kwseq] index-type->kwseq
                              limit (range 6)]
                          (vec (take limit kwseq))))
        kwseq-sym-map (zipmap all-kwseqs (repeatedly gensym))]
    `(let [;; Pre-build comparators for every sequence
           ;; of keywords referring to datom elements.
           ;; A comparator is a function taking two datoms
           ;; as arguments.
           ~@(mapcat (fn [[kwseq sym]]
                       [sym (cmp-for-kwseq kwseq)])
                     kwseq-sym-map)]

;; This is the function generated by this macro
       ;; and it is called by the `-slice` method.
       (fn [~index-sym ~from-sym ~to-sym]

         ;; First branch based on which index to use ...
         (case ~index-sym
           ~@(mapcat
              (fn [[index-key index-spec]]
                [index-key

                 ;; ... then branch based on what elements
                 ;; are non-nil in the slice bound datoms ...
                 (slice-from-to-tree
                  from-sym to-sym
                  index-spec
                  []

                  (fn [acc]
                    {:post [(symbol? %)]}

                    ;; ... and eventually return a precomputed comparator
                    ;; that will be used by `psset/slice`. The generated
                    ;; code is the symbol that is bound to a comparator.
                    (get kwseq-sym-map acc)))])
              index-type->kwseq))))))

(def slice-comparator-constructor (generate-slice-comparator-constructor))

(defn remove-datom [pset ^Datom datom index-type]
  (psset/disj pset datom (index-type->cmp-quick index-type false)))

(defn insert [pset ^Datom datom index-type]
  ;; Use lookup with prefix comparator - O(log n) with zero allocations
  ;; Prefix comparator checks only (e,a,v) to find if ANY datom exists with same triple
  (if #?(:clj (.lookup ^PersistentSortedSet pset datom (dd/index-type->cmp-prefix index-type))
         :cljs (psset/lookup pset datom (dd/index-type->cmp-prefix index-type)))
    pset
    (psset/conj pset datom (index-type->cmp-quick index-type))))

(defn temporal-insert [pset ^Datom datom index-type]
  (psset/conj pset datom (index-type->cmp-quick index-type false)))

(defn upsert [pset ^Datom datom index-type old-datom]
  (if old-datom
    (if (= index-type :avet)
      (-> pset
          (psset/disj old-datom (index-type->cmp-quick index-type))
          (psset/conj datom (index-type->cmp-quick index-type)))
      #?(:clj (.replace ^PersistentSortedSet pset old-datom datom (dd/index-type->cmp-replace index-type))
         :cljs (psset/replace pset old-datom datom (dd/index-type->cmp-replace index-type))))
    (psset/conj pset datom (index-type->cmp-quick index-type))))

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

(defn mark [^PersistentSortedSet pset]
  (when-not (.-_address pset)
    (throw (ex-info "Index needs to be properly flushed before marking."
                    {:type :flush-before-marking})))
  (let [addresses (atom #{})]
    (psset/walk-addresses pset (fn [address] (swap! addresses conj address)))
    @addresses))

(extend-type #?(:clj PersistentSortedSet :cljs BTSet)
  IIndex
  (-slice [^PersistentSortedSet pset from to index-type]
    (psset/slice pset from to (slice-comparator-constructor index-type from to)))
  (-lookup [^PersistentSortedSet pset key cmp]
    #?(:clj  (.lookup pset key cmp)
       :cljs (psset/lookup pset key cmp)))
  (-count-slice [^PersistentSortedSet pset from to cmp]
    (psset/count-slice pset from to cmp))
  (-has-subtree-counts? [^PersistentSortedSet pset]
    #?(:clj  (psset/has-subtree-counts? pset)
       :cljs true))
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
    (persistent! pset))
  (-mark [^PersistentSortedSet pset]
    (mark pset))
  (-root-node [^PersistentSortedSet pset]
    ;; In-memory top node; populated after -flush set _root/_address.
    #?(:clj  (.root pset)
       :cljs (.-root pset)))
  (-seed-root! [^PersistentSortedSet pset root-node]
    ;; Install an inlined (fused) root so root() returns it without a
    ;; storage round-trip; deeper children stay lazy via the set's storage.
    ;; clj only — root fusion is a JVM feature for now.
    #?(:clj (set! (.-_root pset) root-node))
    pset))

;; Normalize a value for content hashing: Datoms → vectors (mirrors the leaf hash, and
;; makes the hash independent of the Datom type's identity), maps/seqs recursed. Used so a
;; slot's diff hashes the same whether it's a live PersistentTreeMap (store) or a plain
;; deserialized map (restore) — hasch already canonicalizes map key order.
#?(:clj
   (defn- canon [x]
     (cond
       (instance? Datom x)   (vec (seq x))
       (map? x)              (persistent! (reduce-kv (fn [m k v] (assoc! m (canon k) (canon v))) (transient {}) x))
       (sequential? x)       (mapv canon x)
       :else                 x)))

;; OP_BUF_V5 crypto address of a Branch. Baseline (no slots) hashes the child addresses —
;; UNCHANGED, so existing crypto stores keep their hashes. With op-buf the buffered diff
;; lives in the slots (not reflected in the anchor child-addresses), so fold the slots in:
;; the address then reflects the durable representation (anchors + diff) and the audit
;; recomputes the same from the stored node. (Within-store integrity; consistent with the
;; baseline merkle already being shape/representation-dependent.)
#?(:clj
   (defn- branch-crypto-uuid [^Branch node]
     (let [slots (.slotsForStorage node)]
       (if slots
         (uuid (canon [(vec (.addresses node)) slots]))
         (uuid (vec (.addresses node)))))))

(defn- gen-address [^ANode node crypto-hash?]
  (if crypto-hash?
    (if (instance? Branch node)
      #?(:clj (branch-crypto-uuid ^Branch node) :cljs (uuid (vec (.addresses ^Branch node))))
      (uuid (mapv (comp vec seq) (.keys node))))
    (squuid)))  ;; Sequential UUID for better index locality

#?(:clj
   (defn- walk-pss-address!
     "Read the node at `address` directly from konserve, recompute its
      content-addressed UUID, and confirm it matches `address`. Recurses
      into Branch children, accumulating any anomalies into the
      `errors` atom (instead of throwing).

      Each error has shape:
        {:type :audit/merkle-mismatch | :audit/node-missing | :audit/unknown-node-class
         :address <expected-address>
         :recomputed <uuid?>           ;; only for :merkle-mismatch
         :node-class <class-name?>}

      `verified` holds addresses already proven good in this pass; the
      walker prunes their subtrees. Within a single tree this is a
      no-op (B-tree nodes have one parent) but it keeps the function
      composable across multiple calls sharing the same atom.

      Reads go through `k/get store` directly, bypassing the live
      `CachedStorage` LRU; otherwise a hot in-memory copy could mask a
      tampered on-disk blob."
     [store address verified errors]
     (when-not (contains? @verified address)
       (let [node (k/get store address nil {:sync? true})]
         (cond
           (nil? node)
           (swap! errors conj {:type :audit/node-missing :address address})

           :else
           (let [recomputed (cond
                              (instance? Branch node)
                              (branch-crypto-uuid ^Branch node)
                              (instance? Leaf node)
                              (uuid (mapv (comp vec seq) (.keys ^Leaf node))))]
             (cond
               (nil? recomputed)
               (swap! errors conj {:type :audit/unknown-node-class
                                   :address address
                                   :node-class (some-> node class .getName)})

               (not= address recomputed)
               (swap! errors conj {:type :audit/merkle-mismatch
                                   :address address
                                   :expected address
                                   :recomputed recomputed
                                   :node-class (some-> node class .getName)})

               :else
               (do
                 (when (instance? Branch node)
                   (doseq [child-addr (.addresses ^Branch node)]
                     (walk-pss-address! store child-addr verified errors)))
                 (swap! verified conj address)))))))))

#?(:clj
   (defn- walk-pss-node!
     "Like walk-pss-address! but for a node already in hand — used for a FUSED root, which
      is inlined in the db-record and therefore not a separate konserve object. Recomputes
      the node's content UUID, confirms it equals `address`, and recurses into its children
      (which ARE separate objects) via walk-pss-address!."
     [store ^ANode node address verified errors]
     (when-not (contains? @verified address)
       (let [recomputed (cond
                          (instance? Branch node) (branch-crypto-uuid ^Branch node)
                          (instance? Leaf node)   (uuid (mapv (comp vec seq) (.keys ^Leaf node))))]
         (cond
           (nil? recomputed)
           (swap! errors conj {:type :audit/unknown-node-class :address address
                               :node-class (some-> node class .getName)})
           (not= address recomputed)
           (swap! errors conj {:type :audit/merkle-mismatch :address address :expected address
                               :recomputed recomputed :node-class (some-> node class .getName)})
           :else
           (do (when (instance? Branch node)
                 (doseq [child-addr (.addresses ^Branch node)]
                   (walk-pss-address! store child-addr verified errors)))
               (swap! verified conj address)))))))

(extend-type #?(:clj PersistentSortedSet :cljs BTSet)
  IAuditable
  (-merkle-root [^PersistentSortedSet pset]
    ;; gen-address (below) makes every node UUID a recursive content
    ;; hash of its datoms under :crypto-hash?, so the root _address
    ;; captures the whole tree. Set by psset/store during -flush.
    ;; Returns nil when unflushed; never throws.
    (.-_address pset))
  (-recompute-merkle-root [^PersistentSortedSet pset]
    ;; Walk the tree from konserve, deserialize each node, and confirm
    ;; its bytes hash back to its address. Konserve does NOT verify
    ;; content on read, so without this walk a tampered .ksv file would
    ;; round-trip undetected — only the in-memory `_address` would still
    ;; look correct. Returns a result map; never throws on mismatch.
    #?(:clj
       (let [address (.-_address pset)
             storage (.-_storage pset)
             store   (some-> storage :store)]
         (cond
           (nil? address)
           {:status :unsupported :reason :unflushed}
           (nil? store)
           {:status :unsupported :reason :no-store}
           :else
           (let [verified  (atom #{})
                 errors    (atom [])
                 ;; Fused root: inlined in the db-record, not a separate object. Detect by a
                 ;; direct store read; when absent, verify the seeded in-memory root instead
                 ;; (recomputing its content hash still detects db-record tampering of the
                 ;; root), then recurse children (separate objects) as usual.
                 root-node (k/get store address nil {:sync? true})]
             (if (nil? root-node)
               (walk-pss-node! store (.root ^PersistentSortedSet pset) address verified errors)
               (walk-pss-node! store root-node address verified errors))
             (if (seq @errors)
               {:status :mismatch :root nil :errors @errors}
               {:status :ok :root address}))))
       :cljs
       {:status :unsupported :reason :cljs-not-implemented})))

(defn- freelist-pop!
  "Atomically pop an address from the freelist. Returns nil if empty."
  [freelist-atom]
  (loop []
    (let [current @freelist-atom]
      (if (empty? current)
        nil
        (let [addr (peek current)
              new-list (pop current)]
          (if (compare-and-set! freelist-atom current new-list)
            addr
            (recur)))))))

(defrecord CachedStorage [store config cache stats pending-writes freed-addresses freed-set freelist cost-center-fn cmp]
  IStorage
  (comparator [_] cmp)   ;; OP_BUF_V5: per-index comparator for buffered-leaf projection
  (store [_ node #?(:cljs opts)]
    (@cost-center-fn :store)
    (swap! stats update :writes inc)
    (let [;; Only reuse addresses when not using crypto-hash (content-addressed storage
          ;; requires the address to match the content)
          reused (when-not (:crypto-hash? config)
                   (freelist-pop! freelist))
          address (or reused (gen-address node (:crypto-hash? config)))
          _ (log/trace :datahike/index-write {:address address :reused (boolean reused) :crypto-hash (:crypto-hash? config)})]
      ;; Evict old cached value when reusing an address
      (when reused
        (wrapped/evict cache address))
      (swap! pending-writes conj [address node])
      (wrapped/miss cache address node)
      address))
  (accessed [_ address]
    (@cost-center-fn :accessed)
    (log/trace :datahike/index-access {:address address})
    (swap! stats update :accessed inc)
    (wrapped/hit cache address)
    nil)
  (restore [_ address #?(:cljs opts)]
    (@cost-center-fn :restore)
    (log/trace :datahike/index-read {:address address})
    (if-let [cached (wrapped/lookup cache address)]
      cached
      (let [node (k/get store address nil {:sync? true})]
        (when (nil? node)
          (log/raise "Node not found in storage." {:type :node-not-found
                                                   :address address
                                                   :store store}))
        (swap! stats update :reads inc)
        (wrapped/miss cache address node)
        node)))
  (markFreed [_ address]
    (when address
      (let [now #?(:clj (java.util.Date.) :cljs (js/Date.))]
        (log/trace :datahike/index-freed {:address address})
        (swap! freed-set conj address)
        (swap! freed-addresses conj [address now]))))
  (isFreed [_ address]
    (contains? @freed-set address))
  (freedInfo [_ address]
    (when (contains? @freed-set address)
      "Address has been marked as freed")))

(def init-stats {:writes   0
                 :reads    0
                 :accessed 0})

(defn create-storage [store config]
  (CachedStorage. store config
                  (atom (cache/lru-cache-factory {} :threshold (:store-cache-size config)))
                  (atom init-stats)
                  (atom [])
                  (atom [])  ;; freed-addresses: vector of [address timestamp] pairs
                  (atom #{}) ;; freed-set: HashSet for O(1) isFreed lookups
                  (atom [])  ;; freelist: vector of reusable addresses (used as stack via peek/pop)
                  (atom (fn [_] nil))
                  nil))      ;; cmp: per-index comparator, set via (with-comparator storage cmp)

;; Per-index view of the (shared) storage carrying the index comparator. Returns a new
;; CachedStorage sharing all atoms (cache/pending-writes/stats/freed/freelist) — only the
;; cmp field differs — so OP_BUF_V5 projection can read storage.comparator() per index
;; while writes/cache stay unified across indexes.
(defn with-comparator [storage cmp]
  (if (instance? CachedStorage storage)   ;; pass through nil / non-CachedStorage (e.g. mem backend) unchanged
    (assoc storage :cmp cmp)
    storage))

(def ^:const DEFAULT_BRANCHING_FACTOR 512)

;; Branching factor is create-time-fixed: a tree built at one bf must never be mutated
;; at another (mixed node sizes break the min/max invariants). Sourced from the persisted
;; index-config (default 512 ⇒ existing stores, built at 512, are unaffected). Must reach
;; BOTH fresh-set creation AND the deserialization Settings, else a non-512 store would be
;; mutated at 512 on restore. The consistency check guards against accidental change.
(defn- branching-factor ^long [index-config]
  (long (or (:branching-factor index-config) DEFAULT_BRANCHING_FACTOR)))

(defmethod di/empty-index :datahike.index/persistent-set [_index-name store index-type index-config]
  (let [cmp (index-type->cmp-quick index-type false)
        ^PersistentSortedSet pset (psset/sorted-set* {:comparator cmp
                                                      :storage (with-comparator (:storage store) cmp)
                                                      :branching-factor (branching-factor index-config)
                                                      :op-buf-size (op-buf-size index-config)})]
    (with-meta pset
      {:index-type index-type})))

(defmethod di/init-index :datahike.index/persistent-set [_index-name store datoms index-type _ {:keys [indexed] :as index-config}]
  (let [arr (if (= index-type :avet)
              (->> datoms
                   (filter #(contains? indexed (.-a ^Datom %)))
                   to-array)
              (cond-> datoms
                (not (arrays/array? datoms))
                (arrays/into-array)))
        _ (arrays/asort arr (index-type->cmp-quick index-type false))
        cmp (index-type->cmp-quick index-type false)
        ^PersistentSortedSet pset (psset/from-sorted-array cmp
                                                           arr
                                                           (arrays/alength arr)
                                                           {:branching-factor (branching-factor index-config)
                                                            :op-buf-size (op-buf-size index-config)})]
    (set! (.-_storage pset) (with-comparator (:storage store) cmp))
    (with-meta pset
      {:index-type index-type})))

;; temporary import from psset until public
(defn- map->settings ^Settings [m]
  #?(:cljs m
     ;; 5-arg normalizing ctor (bf, refType, measure, leaf-processor, opBufSize): defaults
     ;; refType to SOFT when nil. OP_BUF_V5: deserialized nodes need opBufSize>0 to project.
     :clj (Settings.
           (int (or (:branching-factor m) 0))
           nil nil nil
           (int (or (:op-buf-size m) 0)))))

(defmethod di/add-konserve-handlers :datahike.index/persistent-set [config store]
  ;; Check if store has pre-configured handlers (e.g., LMDB with buffer encoder).
  ;; If so, the store will have :storage-atom that handlers close over.
  (if-let [storage-atom (:storage-atom store)]
    ;; Non-fressian store - handlers already configured, just create storage
    (let [storage (or (:storage store)
                      (create-storage store config))]
      (reset! storage-atom storage)
      (assoc store :storage storage))

    ;; Standard fressian store - set up serializers
    ;; deal with circular reference between storage and store
    (let [settings (map->settings {:branching-factor (branching-factor (:index-config config))
                                   :op-buf-size (op-buf-size (:index-config config))})
          storage (atom nil)
          store
          (k/assoc-serializers
           store
           {:FressianSerializer (fressian-serializer

                               ;; read handlers
                                 {"datahike.index.PersistentSortedSet"
                                  #?(:clj
                                     (reify ReadHandler
                                       (read [_ reader _tag _component-count]
                                         (let [{:keys [meta address count]} (.readObject reader)
                                               cmp                          (index-type->cmp-quick (:index-type meta) false)]
                                         ;; The following fields are reset as they cannot be accessed from outside:
                                         ;; - 'edit' is set to false, i.e. the set is assumed to be persistent, not transient
                                         ;; - 'version' is set back to 0
                                         ;; OP_BUF_V5: give the set a storage view carrying its index comparator
                                         ;; so buffered-leaf projection (Branch.child) can route by value on restore.
                                           (PersistentSortedSet. meta cmp address (with-comparator @storage cmp) nil count settings 0))))
                                     :cljs
                                     (fn [reader _tag _component-count]
                                       (let [{:keys [meta address count]} (fress/read-object reader)
                                             cmp                          (index-type->cmp-quick (:index-type meta) false)]
                                       ;; CLJS BTSet deftype: [root cnt comparator meta _hash storage address settings]
                                       ;; OP_BUF_V5: give the set a storage view carrying its index comparator so
                                       ;; buffered-leaf projection (Branch.child) can route by value on restore.
                                         (BTSet. nil count cmp meta nil (with-comparator @storage cmp) address settings))))
                                  "datahike.index.PersistentSortedSet.Leaf"
                                  #?(:clj
                                     (reify ReadHandler
                                       (read [_ reader _tag _component-count]
                                         (let [{:keys [keys _level]} (.readObject reader)]
                                           (Leaf. ^List keys settings))))
                                     :cljs
                                     (fn [reader _tag _component-count]
                                       (let [{:keys [keys _level]} (fress/read-object reader)]
                                       ;; CLJS Leaf deftype: [keys settings _measure]
                                         (Leaf. (clj->js keys) settings nil))))
                                  "datahike.index.PersistentSortedSet.Branch"
                                  #?(:clj
                                     (reify ReadHandler
                                       (read [_ reader _tag _component-count]
                                         (let [{:keys [keys level addresses subtree-count slots]} (.readObject reader)
                                               addr-vec (vec addresses)
                                               ^Branch b (Branch. (int level) (count keys) (into-array Object keys) (into-array Object (seq addresses)) nil (long (or subtree-count -1)) settings)]
                                           ;; OP_BUF_V5: reconstruct per-child buffered diffs (anchor = the child's
                                           ;; durable address). Branch.child projects them on descent. Absent ⇒ baseline.
                                           (when slots
                                             (let [arr (object-array (count keys))]
                                               (doseq [[idx entry] slots]
                                                 (aset arr (int idx)
                                                       (Slot. (:diff entry) (long (:count entry)) (:measure entry) (nth addr-vec (int idx)))))
                                               (set! (.-_slots b) arr)))
                                           b)))
                                     :cljs
                                     (fn [reader _tag _component-count]
                                       (let [{:keys [keys level addresses subtree-count slots]} (fress/read-object reader)
                                             addr-arr (clj->js addresses)
                                             ;; CLJS Branch deftype: [level keys children addresses subtree-count _measure settings _slots _rebalanced]
                                             b (Branch. (int level) (clj->js keys) nil addr-arr (or subtree-count -1) nil settings nil false)]
                                         ;; OP_BUF_V5: reconstruct per-child buffered diffs (anchor = the child's
                                         ;; durable address). Branch.child projects them on descent. Absent ⇒ baseline.
                                         (when slots
                                           (let [arr (make-array (count keys))]
                                             (doseq [[idx entry] slots]
                                               (aset arr (int idx)
                                                     {:diff    (:diff entry)
                                                      :count   (long (:count entry))
                                                      :measure (:measure entry)
                                                      :anchor  (aget addr-arr (int idx))}))
                                             (set! (.-_slots b) arr)))
                                         b)))
                                  "datahike.datom.Datom"
                                  #?(:clj
                                     (reify ReadHandler
                                       (read [_ reader _tag _component-count]
                                         (dd/datom-from-reader (.readObject reader))))
                                     :cljs
                                     (fn [reader _tag _component-count]
                                       (dd/datom-from-reader (fress/read-object reader))))}

                               ;; write handlers
                               ;; CLJ format: nested {Type {"tag" handler}} for clojure.data.fressian
                               ;; CLJS format: flat {Type handler-fn} for fress library
                                 #?(:clj
                                    {org.replikativ.persistent_sorted_set.PersistentSortedSet
                                     {"datahike.index.PersistentSortedSet"
                                      (reify WriteHandler
                                        (write [_ writer pset]
                                          (when (nil? (.-_address ^PersistentSortedSet pset))
                                            (log/raise "Must be flushed." {:type :must-be-flushed
                                                                           :pset pset}))
                                          (.writeTag writer "datahike.index.PersistentSortedSet" 1)
                                          (.writeObject writer {:meta    (meta pset)
                                                                :address (.-_address ^PersistentSortedSet pset)
                                                                :count   (count pset)})))}

                                     org.replikativ.persistent_sorted_set.Leaf
                                     {"datahike.index.PersistentSortedSet.Leaf"
                                      (reify WriteHandler
                                        (write [_ writer leaf]
                                          (.writeTag writer "datahike.index.PersistentSortedSet.Leaf" 1)
                                          (.writeObject writer {:level (.level ^Leaf leaf)
                                                                :keys  (.keys ^Leaf leaf)})))}

                                     org.replikativ.persistent_sorted_set.Branch
                                     {"datahike.index.PersistentSortedSet.Branch"
                                      (reify WriteHandler
                                        (write [_ writer node]
                                          (.writeTag writer "datahike.index.PersistentSortedSet.Branch" 1)
                                          ;; OP_BUF_V5: emit :slots only when present (nil ⇒ byte-identical to
                                          ;; the pre-op-buf format, so opBufSize=0 / legacy DBs are unaffected).
                                          (let [slots (.slotsForStorage ^Branch node)]
                                            (.writeObject writer (cond-> {:level     (.level ^Branch node)
                                                                          :keys      (.keys ^Branch node)
                                                                          :addresses (.addresses ^Branch node)
                                                                          :subtree-count (.subtreeCount ^Branch node)}
                                                                   slots (assoc :slots slots))))))}

                                     datahike.datom.Datom
                                     {"datahike.datom.Datom"
                                      (reify WriteHandler
                                        (write [_ writer datom]
                                          (.writeTag writer "datahike.datom.Datom" 1)
                                          (.writeObject writer (vec (seq ^Datom datom)))))}}

                                    :cljs
                                    {BTSet
                                     (fn [writer pset]
                                       (when (nil? (.-address ^BTSet pset))
                                         (log/raise "Must be flushed." {:type :must-be-flushed
                                                                        :pset pset}))
                                       (fress/write-tag writer "datahike.index.PersistentSortedSet" 1)
                                       (fress/write-object writer {:meta    (meta pset)
                                                                   :address (.-address ^BTSet pset)
                                                                   :count   (count pset)}))

                                     Leaf
                                     (fn [writer leaf]
                                       (fress/write-tag writer "datahike.index.PersistentSortedSet.Leaf" 1)
                                       (fress/write-object writer {:level 0 ;; not supported in cljs
                                                                   :keys  (vec (.-keys ^Leaf leaf))}))

                                     Branch
                                     (fn [writer node]
                                       (fress/write-tag writer "datahike.index.PersistentSortedSet.Branch" 1)
                                       ;; OP_BUF_V5: emit :slots only when present (nil ⇒ byte-identical to
                                       ;; the pre-op-buf format, so op-buf-size=0 / legacy DBs are unaffected).
                                       (let [slots (branch/slots-for-storage ^Branch node)]
                                         (fress/write-object writer (cond-> {:level     (.-level ^Branch node)
                                                                             :keys      (vec (.-keys ^Branch node))
                                                                             :addresses (vec (.-addresses ^Branch node))
                                                                             :subtree-count (.-subtree-count ^Branch node)}
                                                                      slots (assoc :slots slots)))))

                                     datahike.datom.Datom
                                     (fn [writer datom]
                                       (fress/write-tag writer "datahike.datom.Datom" 1)
                                       (fress/write-object writer (vec (seq ^Datom datom))))}))})]
      (reset! storage (or (:storage store)
                          (create-storage store config)))
      (assoc store :storage @storage))))

(defmethod di/konserve-backend :datahike.index/persistent-set [_index-name store]
  store)

(defmethod di/default-index-config :datahike.index/persistent-set [_index-name]
  {})
