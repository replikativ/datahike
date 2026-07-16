(ns datahike.kabel.walker
  "konserve-sync reachability walker for datahike stores — the set of konserve keys
   a subscriber must receive to hold a datahike replica.

   It discovers every BTSet index-node address reachable from a branch's `:db` root
   (so sync ships only the reachable index, not ALL keys) AND the blob keys named by
   `:db.type/store-ref` datom VALUES (`datahike.gc/record-store-refs`). The latter is
   the point of this living in datahike: a plain index-tree walk never looks inside a
   datom's value, so a blob named only by a store-ref would never replicate and a
   subscriber would hold a live datom pointing at an object that never arrived — the
   same blind spot `gc-storage!` closes for collection, closed here for sync.

   This walker owns datahike's stored-db record format (index-root keys, fusion,
   schema-meta, store-refs), so it lives with datahike and versions with it, rather
   than in konserve-sync where it would chase datahike-internal changes across a repo
   boundary. konserve-sync only sees the walk-fn VALUE, via `register-store!` /
   `perform-walk-sync`.

   Usage:
   - Server: pass `datahike-walk-fn` to `konserve-sync`'s `register-store!` `:walk-fn`.
   - Client: pass it (or `make-tiered-walk-fn`) to `perform-walk-sync`."
  (:require [datahike.gc :as gc]
            [konserve.core :as k]
            [org.replikativ.persistent-sorted-set.arrays :as arrays]
            #?@(:clj [[superv.async :refer [go-try- <?-]]]
                :cljs [[clojure.core.async :refer [<!]]]))
  #?(:cljs (:require-macros [clojure.core.async :refer [go]]
                            [superv.async :refer [go-try- <?-]]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set PersistentSortedSet Branch])))

;; ============================================================================
;; BTSet Address Collection (Recursive)
;; ============================================================================

#?(:clj
   (def ^:private branch->addresses
     "Child-address accessor for the persistent-sorted-set `Branch` on the
      classpath, resolved ONCE. Two incompatible layouts are in the wild:
      newer versions pack node state in `_state` and expose a public
      `.addressArray` method; older versions hold a private `_addresses` field.
      konserve-sync is consumed at both, so we detect which is present and bind
      the right accessor. Resolved eagerly (a `delay`) rather than per node so
      the hot walk path does no reflection, and a genuinely absent accessor
      throws loudly at first use instead of being swallowed as a leaf — the
      silent-`catch` version of this masked a full sync-truncation regression."
     (delay
       (if (some #(= "addressArray" (.getName ^java.lang.reflect.Method %))
                 (.getMethods Branch))
         (fn [node] (.addressArray ^Branch node))
         (let [f (doto (.getDeclaredField Branch "_addresses") (.setAccessible true))]
           (fn [node] (.get f node)))))))

(defn- get-node-addresses
  "Get the child-address array from a BTSet BRANCH node; nil for leaves.
   Branch/leaf is an explicit `instance?` test (no catch); the branch accessor
   is version-resolved by `branch->addresses`."
  [node]
  #?(:clj (when (instance? Branch node)
            (@branch->addresses node))
     :cljs (.-addresses node)))

(defn- walk-node-async
  "Recursively walk a BTSet node and collect child addresses.
   Fetches each node from the store to discover its children's addresses."
  [store node collected]
  (go-try-
   (when node
      ;; Branch nodes have addresses array pointing to children
     (when-let [addresses (get-node-addresses node)]
       (when (pos? (arrays/alength addresses))
         (loop [i 0]
           (when (< i (arrays/alength addresses))
             (when-let [addr (arrays/aget addresses i)]
               (swap! collected conj addr)
                ;; Recursively walk child node
               (let [child (<?- (k/get store addr))]
                 (<?- (walk-node-async store child collected))))
             (recur (inc i)))))))))

(defn- get-btset-address
  "Extract root address from a BTSet or deferred index format.
   Handles both actual PersistentSortedSet/BTSet instances AND
   deferred format maps {:deferred-type :persistent-sorted-set :address ...}
   returned by Fressian handlers."
  [btset]
  (cond
    ;; Deferred format from Fressian deserialization
    (and (map? btset) (= (:deferred-type btset) :persistent-sorted-set))
    (:address btset)

    ;; Actual PersistentSortedSet (CLJ) or BTSet (CLJS)
    #?(:clj (instance? PersistentSortedSet btset)
       :cljs true)
    #?(:clj (.-_address ^PersistentSortedSet btset)
       :cljs (.-address btset))

    :else nil))

(defn- collect-btset-addresses-async
  "Collect all addresses from a BTSet by walking the tree.
   Fetches nodes from the store to discover all nested addresses.

   `fused-root` is the index's root NODE inlined in the db record, present iff the
   store runs datahike's `:fuse-index-roots?`. It is load-bearing: under fusion the
   root is inlined and NEVER written as its own konserve object, so `(k/get store
   root-addr)` returns nil, `walk-node-async` walks nothing, and the walk dead-ends
   at the root — shipping a root address that does not exist in the store while
   discovering none of the subtree. (That is a silent under-report: a fresh
   subscriber then syncs ~nothing and read-throughs to the backend forever.)

   So when the root is fused we walk the INLINED node directly and do not emit its
   address — there is no object to sync. The root's CHILDREN are still ordinary
   content-addressed objects, so the rest of the tree is discovered as usual.
   Datahike's GC does exactly this (it seeds the inlined root before marking); the
   sync walker has to as well or the two disagree about what is reachable."
  [store btset fused-root]
  (go-try-
   (let [collected (atom #{})
         root-addr (get-btset-address btset)]
     (cond
       fused-root (<?- (walk-node-async store fused-root collected))

       root-addr  (do (swap! collected conj root-addr)
                      (let [root-node (<?- (k/get store root-addr))]
                        (<?- (walk-node-async store root-node collected)))))
     @collected)))

;; ============================================================================
;; Main Walker Function
;; ============================================================================

(defn- walk-stored-db-async
  "Collect all BTSet node addresses reachable from a single stored-db value.
   Returns a channel that delivers a set of konserve keys (BTSet addresses
   + the schema-meta key, if any). Used by `datahike-walk-fn` to traverse
   one branch at a time."
  [store stored-db]
  (go-try-
   (let [collected (atom #{})]
     (when stored-db
       ;; Main indices. The `*-root` keys carry the INLINED root node under
       ;; datahike's :fuse-index-roots? (absent otherwise) — see
       ;; collect-btset-addresses-async for why passing it is not optional.
       (doseq [[idx-key root-key] [[:eavt-key :eavt-root]
                                   [:aevt-key :aevt-root]
                                   [:avet-key :avet-root]]]
         (when-let [btset (get stored-db idx-key)]
           (let [addrs (<?- (collect-btset-addresses-async
                             store btset (get stored-db root-key)))]
             (swap! collected into addrs))))
       ;; Temporal indices (when :keep-history?)
       (doseq [[idx-key root-key] [[:temporal-eavt-key :temporal-eavt-root]
                                   [:temporal-aevt-key :temporal-aevt-root]
                                   [:temporal-avet-key :temporal-avet-root]]]
         (when-let [btset (get stored-db idx-key)]
           (let [addrs (<?- (collect-btset-addresses-async
                             store btset (get stored-db root-key)))]
             (swap! collected into addrs))))
       ;; Schema meta
       (when-let [schema-key (get stored-db :schema-meta-key)]
         (swap! collected conj schema-key))
       ;; Blob keys named by :db.type/store-ref VALUES. NOT discoverable by walking
       ;; the index trees above — the walk sees node addresses, never a datom's
       ;; value — so a blob would silently never replicate. These are content-
       ;; addressed immutable objects, so they belong in the NODE portion (unioned
       ;; here), ahead of the mutable pointer cells `datahike-walk-fn` appends last.
       (swap! collected into (<?- (gc/record-store-refs store stored-db))))
     @collected)))

(defn datahike-walk-fn
  "Walker function for konserve-sync that discovers the keys reachable from a
   Datahike store's branches.

   Arguments:
   - store: The konserve store containing Datahike data
   - opts:  Options map. `:branches` selects which branches to walk NODES for:
       • absent / `:all` (default) — every branch in the store's `:branches`
         set (forks included). Heavier initial sync, but a subscriber can
         `branch-as-db` any branch locally — instant branch-switch. This is
         what fork-centric peers (dvergr distributed context) want.
       • `:trunk` — only `:db`. Lean sync of the trunk replica.
       • a branch keyword (e.g. `:db-foo`) or a coll of them — only those
         (intersected with the store's actual branches). A lean replica of the
         active branch; switching to an un-synced branch needs a fetch.

   Returns:
   - Channel yielding an ORDERED, deduped vector of reachable keys:
     the index NODES first, then the MUTABLE pointer cells LAST
     (`:branches`, then each in-scope branch HEAD).

   That order is the whole contract. Nodes are content-addressed and UNREACHABLE
   until a head points at them, so their order among themselves is irrelevant —
   but a head applied BEFORE its nodes would leave a subscriber that PERSISTS the
   sync (IndexedDB, LMDB) holding a pointer into values that never arrived, if the
   handshake were interrupted. Emitting the pointers last makes that impossible by
   construction, and konserve-sync preserves walk order — so nothing downstream has
   to infer the order back from the SHAPE of a key (the old `:key-sort-fn`
   \"keywords are roots, sort them last\" heuristic, which is silently wrong for any
   store whose keys don't fit the guess).

   The returned vector always includes:
   - `:branches` (the set of branch names) — so a subscriber knows every branch
     EXISTS via `(d/branches conn)` even when it didn't sync that branch's nodes.
   And for each IN-SCOPE branch:
   - the branch HEAD key (e.g. `:db`, `:db-foo` …),
   - all BTSet node addresses reachable from its eavt/aevt/avet indices
     (live + temporal), its `:schema-meta-key`, and the blob keys named by any
     `:db.type/store-ref` datom values (so referenced objects replicate too).

   Note on scoping: walking ALL branches was added so `(d/branch-as-db conn
   :db-foo)` resolves locally on a subscriber. Scoping narrows that back to the
   chosen branches on purpose — an out-of-scope branch's HEAD/nodes are not
   shipped, so `branch-as-db` on it returns nil until fetched. Use `:all` to
   keep every fork local; scope it when the subscriber only views one branch.

   Usage with register-store!:
     (sync/register-store! ctx store config {:walk-fn datahike-walk-fn})
     ;; scoped — wrap to inject opts:
     {:walk-fn (fn [store opts] (datahike-walk-fn store (assoc opts :branches :trunk)))}

   Usage with perform-walk-sync (client):
     (tiered/perform-walk-sync frontend backend [:db]
       (fn [store root-values opts]
         (datahike-walk-fn store opts))
       opts)"
  [store opts]
  (go-try-
   (let [;; Read the branch set, falling back to {:db} if absent so a
         ;; fresh store still walks trunk before `:branches` has ever
         ;; been initialized.
         all-branches (or (<?- (k/get store :branches))
                          #{:db})
         scope (:branches opts)
         branches (cond
                    (or (nil? scope) (= :all scope)) all-branches
                    (= :trunk scope)                 #{:db}
                    (keyword? scope)                 (filter (set all-branches) #{scope})
                    (coll? scope)                    (filter (set all-branches) scope)
                    :else                            all-branches)
         ;; NODES only here — the mutable pointer cells are appended LAST, below.
         nodes (atom [])]
     (loop [bs (seq branches)]
       (when bs
         (let [branch-key (first bs)]
           (when-let [stored-db (<?- (k/get store branch-key))]
             (let [addrs (<?- (walk-stored-db-async store stored-db))]
               (swap! nodes into addrs)))
           (recur (next bs)))))
     ;; Reachable index nodes FIRST, mutable pointer cells LAST: `:branches` (the
     ;; branch-name set, so a subscriber learns every branch EXISTS even when it
     ;; didn't sync that branch's nodes) and then each in-scope branch HEAD. A head
     ;; is only ever applied once every node it references has been — see the
     ;; docstring.
     (-> (vec (distinct @nodes))
         (conj :branches)
         (into branches)))))

;; ============================================================================
;; Convenience wrapper for tiered store walk-sync
;; ============================================================================

(defn make-tiered-walk-fn
  "Create a walk function suitable for tiered/perform-walk-sync.

   The tiered walk-sync expects: (fn [backend-store root-values opts] -> channel)
   This wrapper adapts datahike-walk-fn to that signature.

   Usage:
     (tiered/perform-walk-sync frontend backend [:db]
       (walkers/make-tiered-walk-fn)
       {:sync? false})"
  []
  (fn [backend-store _root-values opts]
    ;; root-values already has :db, but we re-fetch to ensure we walk
    ;; the BTSet structure stored in backend
    (datahike-walk-fn backend-store opts)))
