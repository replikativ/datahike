(ns datahike.index.interface
  "All the functions in this namespace must be implemented for each index type"
  #?(:cljs (:refer-clojure :exclude [-seq -count -persistent! -flush -lookup]))
  (:require
   #?(:clj [clojure.core.cache :as cache]
      :cljs [cljs.cache :as cache])
   ;; Only for `warm-result` below — `async+sync` and `go-try-` are MACROS, so
   ;; ClojureScript needs :refer-macros. Two whole libspecs rather than a reader
   ;; conditional on the option key; see the note in datahike.gc.
   #?(:clj  [konserve.utils :refer [async+sync *default-sync-translation*]]
      :cljs [konserve.utils
             :refer [*default-sync-translation*]
             :refer-macros [async+sync]])
   #?(:clj  [superv.async :refer [go-try-]]
      :cljs [superv.async :refer-macros [go-try-]])
   ;; Required on both runtimes: `go-try-` expands into `clojure.core.async/go`,
   ;; whose state machine names `clojure.core.async` vars — :require-macros
   ;; alone leaves them undeclared in a cljs build.
   [clojure.core.async])
  #?(:cljs (:require-macros [clojure.core.async :refer [go]])))

(def node-cache-key
  "Key under which a connection hands its storage the node cache it reserved.
   Set by `datahike.connector` from `datahike.connections/acquire-node-cache!`;
   read by the index implementation when it builds storage. Absent on the paths
   that have no connection behind them, which then get a private cache."
  ::node-cache)

(defn make-node-cache
  "A fresh, empty node cache bounded by `threshold` entries. Pure: safe to call
   inside a `swap!` that may retry and discard the result."
  [threshold]
  (atom (cache/lru-cache-factory {} :threshold threshold)))

(defprotocol IIndex
  (-all [index] "Returns a sequence of all datoms in the index")
  (-seq [index] "Returns a sequence of all datoms in the index")
  (-count [index] "Returns the number of datoms in the index")
  (-insert [index datom index-type op-count] "Inserts a datom into the index")
  (-temporal-insert [index datom index-type op-count] "Inserts a datom in a history index")
  (-upsert [index datom index-type op-count old-datom] "Inserts or updates a datom into the index")
  (-temporal-upsert [index datom index-type op-count old-datom] "Inserts or updates a datom in a history index")
  (-remove [index datom index-type op-count] "Removes a datom from the index")
  (-slice [index from to index-type] "Returns a slice of the index")
  (-rslice [index from to index-type] "Returns a REVERSE slice of the index: a lazy backwards iterator over datoms d with to <= d <= from, starting at `from` and descending. Mirrors persistent-sorted-set's rslice argument order (from = upper bound).")
  (-lookup [index key cmp] "Look up a single key with custom comparator. Returns the stored element or nil.")
  (-count-slice [index from to cmp] "O(log n) count of elements in [from, to] range using the given comparator.")
  (-has-subtree-counts? [index] "Returns true if count-slice is O(log n). False means counts are missing and count-slice would degrade to O(n).")
  (-flush [index backend] "Saves the changes to the index to the given konserve backend")
  (-transient [index] "Returns a transient version of the index")
  (-persistent! [index] "Returns a persistent version of the index")
  (-mark [index] "Return konserve addresses that should be whitelisted for mark and sweep gc.")
  (-root-node [index] "Returns the in-memory root node of a flushed index, for root fusion (inlining the root into the db-record).")
  (-warm! [index opts] "EXPERIMENTAL. Prefetch this index's upper levels into whatever node cache it keeps, breadth-first, so a cold reader does not discover them one blocking round trip at a time. See `datahike.index.persistent-set.warm` for the rationale and the option map, and `datahike.warm` for the db-level entry points.

   An index type that has no node cache, or no way to learn a level's addresses before fetching it, implements this as a NO-OP returning `(zero-warm-report opts)` — the point of the protocol is that such a type stays usable through `datahike.api/warm-db` rather than having to be special-cased there. Clojure protocols have no true defaults, so every implementation must say which it is; a type that omits the method entirely will throw, exactly as it does for `-root-node` and `-has-subtree-counts?`.")
  (-seed-root! [index root-node] "Seeds the in-memory root node after restoring a db-record that inlined it (root fusion). MUTATES the index — call it only on an OWNED, unpublished copy (e.g. the with-storage copy made at attach), never on a stored record's index: records may be shared through the store's cache by every reader of that key. Returns the index."))

(def default-warm-budget
  "EXPERIMENTAL. Nodes a warm may fetch before it stops. See
   `datahike.index.persistent-set.warm` on sizing: the interior of a B-tree is
   ~2/branching-factor of the whole tree, measured, not 1/branching-factor."
  2000)

(defn zero-warm-report
  "EXPERIMENTAL. The report of a warm that had nothing to do — the shape every
   `-warm!` implementation returns, so a caller never has to branch on whether
   its index type prefetches at all."
  [opts]
  {:fetched 0 :by-level [] :rounds 0 :height 0 :by-index {}
   :budget-left (:budget opts default-warm-budget)
   :budget-exhausted? false :budget-clamped? false :ms 0.0})

(defn warm-result
  "EXPERIMENTAL. `report` in the shape the caller's `:sync?` asked for — the
   value itself when synchronous, a channel carrying it otherwise. `-warm!` is
   written `async+sync` like the rest of datahike's storage-touching API, so an
   implementation that has nothing to await still has to answer in both shapes;
   this is that one line."
  [report opts]
  (async+sync (:sync? opts #?(:clj true :cljs false)) *default-sync-translation*
              (go-try- report)))

(defmulti empty-index
  "Creates an empty index"
  (fn [index-name _store _index-type _index-config]
    index-name))

(defmulti init-index
  "Creates an index with datoms"
  (fn [index-name _store _datoms _index-type _op-count _index-config]
    index-name))

(defmulti init-index-sorted
  "Creates an index from datoms ALREADY SORTED in `index-type` order, streaming
   them into the tree rather than materialising them.

   `init-index` takes any order and sorts in memory (`arrays/asort` over the whole
   array), which makes it O(n) in heap — fine when a database fits in memory,
   which is exactly the case a bulk restore is not. This variant is the import
   path's entry: the caller has already produced the right order with an external
   merge sort, so the index build never needs more than one node per level.

   The caller OWNS the ordering guarantee. Passing a wrongly ordered seq does not
   raise here; it produces a tree whose invariants are quietly false. The
   underlying builder checks, so the failure is loud, but it belongs to the
   caller's contract rather than this one's."
  (fn [index-name _store _sorted-datoms _index-type _op-count _index-config]
    index-name))

(defmulti add-konserve-handlers
  "Adds read and write handlers for the index data types."
  (fn [config _store] (:index config)))

(defmulti konserve-backend
  "Returns a konserve store capable of handling the index. Used for flushing."
  (fn [index-name _store] index-name))

(defmulti default-index-config
  "Returns the default index configuration."
  (fn [index-name] index-name))

(defmulti with-storage
  "Return `index` bound to `storage` as a shallow copy sharing the
   (immutable) node tree. Storage is connection-scoped context, not part
   of the index value: bind an index to the live connection's storage
   when materializing it from a store, and detach it (storage nil) before
   writing it into a store, so a stored value never carries a foreign
   storage handle — even through identity-preserving stores that skip
   serialization (e.g. a tiered memory frontend). Never mutates the
   input; returns the index unchanged for index types without embedded
   storage and for nil."
  (fn [index-name _index _storage] index-name))

(defmethod with-storage :default [_index-name index _storage] index)

;; Default handlers for missing index implementations

(defn- hitchhiker-tree-missing-error []
  (ex-info
   "Hitchhiker-tree index requires explicit setup:
   1. Add io.replikativ/hitchhiker-tree to your deps.edn
   2. Require datahike.index.hitchhiker-tree in your namespace
   Or use the default :datahike.index/persistent-set index."
   {:type :missing-index-implementation
    :index :datahike.index/hitchhiker-tree
    :available-indexes (disj (set (keys (methods empty-index))) :default)}))

(defmethod empty-index :default [index-name _ _ _]
  (if (= index-name :datahike.index/hitchhiker-tree)
    (throw (hitchhiker-tree-missing-error))
    (throw (ex-info (str "Unknown index type: " index-name)
                    {:type :unknown-index-type
                     :index index-name
                     :available-indexes (disj (set (keys (methods empty-index))) :default)}))))

(defmethod init-index :default [index-name _ _ _ _ _]
  (if (= index-name :datahike.index/hitchhiker-tree)
    (throw (hitchhiker-tree-missing-error))
    (throw (ex-info (str "Unknown index type: " index-name)
                    {:type :unknown-index-type
                     :index index-name
                     :available-indexes (disj (set (keys (methods init-index))) :default)}))))

(defmethod add-konserve-handlers :default [config _]
  (let [index-name (:index config)]
    (if (= index-name :datahike.index/hitchhiker-tree)
      (throw (hitchhiker-tree-missing-error))
      (throw (ex-info (str "Unknown index type: " index-name)
                      {:type :unknown-index-type
                       :index index-name
                       :available-indexes (disj (set (keys (methods add-konserve-handlers))) :default)})))))

(defmethod konserve-backend :default [index-name _]
  (if (= index-name :datahike.index/hitchhiker-tree)
    (throw (hitchhiker-tree-missing-error))
    (throw (ex-info (str "Unknown index type: " index-name)
                    {:type :unknown-index-type
                     :index index-name
                     :available-indexes (disj (set (keys (methods konserve-backend))) :default)}))))

(defmethod default-index-config :default [index-name]
  (if (= index-name :datahike.index/hitchhiker-tree)
    (throw (hitchhiker-tree-missing-error))
    (throw (ex-info (str "Unknown index type: " index-name)
                    {:type :unknown-index-type
                     :index index-name
                     :available-indexes (disj (set (keys (methods default-index-config))) :default)}))))
