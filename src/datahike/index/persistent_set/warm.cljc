(ns ^:no-doc datahike.index.persistent-set.warm
  "Budget-bounded breadth-first index warm for the persistent-set index — pull a
   database's upper levels into the node cache in WAVES instead of discovering
   them one blocking round trip at a time.

   > ⚠️ **EXPERIMENTAL.** Names and the option map may still move. The public
   > entry points are `datahike.api/warm-index`, `warm-datoms` and
   > `warm-db`; this namespace is the persistent-set walk behind them.

   ## Why

   A cold reader's wall time is `misses x RTT`, with nothing overlapping: a scan
   asks for a node, blocks on the GET, and only then learns the next address.
   Measured against object storage at +20ms injected latency, the marginal cost
   of one more node was 24.96 ms against a ~25 ms round trip — one node, one
   round trip, zero overlap.

   It does not have to be that way, and the fix needs no prediction. A `Branch`
   holds `addresses()` — EVERY child address — the moment it is materialized. So
   the addresses of a whole level are known one level in advance. This walks
   breadth-first and fetches each level concurrently. Measured: the same 197 keys
   at width 64 is 16.4x faster than serial at +20ms.

   ## The two bounds, and why there are two

   `:depth` bounds the SHAPE, `:budget` bounds the COST, and whichever binds
   first wins.

     :depth :interior     expand while (>= level 2) — children are branches, so
                          this stops exactly at the leaf boundary. Exact, not a
                          heuristic: leaves are level 0 and the root knows the
                          tree height.
     :depth :with-leaves  expand while (>= level 1) — everything.
     :depth <integer>     at most that many levels below the start node.

   Having both is what keeps this free of latency cliffs. There is no `preload
   everything` mode and no `warm the interior` mode to switch between — a small
   database with a large budget runs out of frontier and has fetched itself
   entirely; a large one hits the budget and stops. Same code, same config,
   continuous in database size. A mode switch is a cliff, and tenants grow.

   Measured (400 issues, +20ms): `:with-leaves` budget 8 warms 8 nodes and takes
   the query from 21 GETs to 12 — a partial warm buying a partial saving,
   landing between naive and fully-warmed with no step anywhere. The same walk
   reaches `query costs 0 GETs` in 37 fetches, where `ready-store :tiered`'s
   konserve `populate-missing-strategy` preload needs 392 fetches and 11.8s: the
   walk touches only REACHABLE nodes and never enumerates the store.

   ## Sizing a budget

   In a B-tree the interior is a geometric series, so `interior/total` is a
   constant fraction independent of database size — but size it from the MEASURED
   fill, not the branching factor. Nodes run about half full, so the effective
   fanout is ~bf/2 and the interior is ~`2/bf` of the tree, twice the naive
   estimate. Measured at bf 32: 93 interior nodes of 1504 total (6.2%, against
   1/32 = 3.1%).

   The budget is also CLAMPED to the node cache. `:store-cache-size` is
   ENTRY-counted, so warming past it fetches nodes only to evict them; 0.8x
   leaves room for the query that follows to bring in its own leaves without
   evicting the spine.

   ## Selective warming

   `:from`/`:to` scope the walk to a key range: at every level only the child
   indices covering the range are expanded, so cost is proportional to the RANGE
   rather than to the database. That is how warming stays affordable on a store
   too large to warm whole.

   They are an INTERNAL option, and the only public caller that sets them is
   `datahike.warm/warm-datoms!`, which derives them from datahike's own
   `components->pattern` (with `:unbounded? true` giving `seek-datoms`'
   end-of-index upper bound). No public entry point takes raw datom bounds: they
   have to be in the index's own key order, which `components->pattern` reaches
   by PERMUTING the components per index, and a bound built by hand in the wrong
   permutation warms a valid-but-different subtree with no error and no wrong
   answer — just a warm that misses.

   ## What it cannot do

   This bounds DEPTH. It does nothing for a caller that issues many independent
   scans one after another — that breadth lives above datahike, and no amount of
   prefetching below can see it. Such a caller has to issue its seeks
   concurrently (safe: nodes are immutable, the node cache is atom-based).

   ## On the ClojureScript arm

   Deliberately NOT implemented — see the `TODO(cljs)` markers on `child-bounds`,
   `fetch-wave!` and `tree-entry`. The walk itself is shared and the shape is
   written `async+sync`, so the seam is where it belongs; but the three platform
   primitives it needs (a `searchFirst` over a node's keys, a bounded parallel
   restore, and materializing an address-rooted root asynchronously) have no
   ready ClojureScript counterpart today, and shipping them unexercised would be
   worse than saying so.

   The port is not a port: cljs has no threads but does not need them — bounded
   `Promise.all` per level is simpler than this thread pool, and the BFS shape is
   natively async. Two things genuinely differ: `:width` cannot share a default
   (a browser gives ~6 connections per origin on HTTP/1.1, so 64 merely queues),
   and the value proposition inverts — datahike's cljs read path runs a sync
   query engine over async storage, so a complete warm is what makes synchronous
   querying FEASIBLE rather than merely fast."
  (:require
   ;; `async+sync` and the superv operators are MACROS: ClojureScript needs
   ;; :refer-macros for them. Written as two whole libspecs rather than with a
   ;; reader conditional on the OPTION key, because `#?(:clj :refer :cljs
   ;; :refer-macros)` beside a second `:refer` expands to a duplicate `:refer`
   ;; on the JVM. Mirrors datahike.gc.
   #?(:clj  [konserve.utils :refer [async+sync *default-sync-translation*]]
      :cljs [konserve.utils
             :refer [*default-sync-translation*]
             :refer-macros [async+sync]])
   #?(:clj  [superv.async :refer [go-try- <?-]]
      :cljs [superv.async :refer-macros [go-try- <?-]])
   ;; Required on BOTH runtimes even though only the JVM arm names `async/…`:
   ;; `go-try-` expands into `clojure.core.async/go`, whose state machine emits
   ;; `clojure.core.async/<!` and friends, so a cljs build without the namespace
   ;; (only :require-macros below) compiles to undeclared vars. Same shape as
   ;; datahike.gc.
   [clojure.core.async :as async]
   #?(:cljs [org.replikativ.persistent-sorted-set.branch :as branch :refer [Branch]])
   [datahike.index.interface :as di]
   [replikativ.logging :as log])
  ;; go-try- expands into clojure.core.async/go, which cljs needs as a macro.
  #?(:cljs (:require-macros [clojure.core.async :refer [go]]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set PersistentSortedSet ANode Branch IStorage]
                   [java.util Comparator]
                   [java.util.concurrent Executors ExecutorService Callable Future TimeUnit])))

(def default-width
  "Concurrent in-flight restores.

   64 measured optimal against local MinIO (16.4x over serial at +20ms); 128
   REGRESSED there. A real bucket tolerates far more — a starting point to
   measure from, not a constant to inherit. ClojureScript defaults to 6 because
   a browser gives ~6 connections per origin on HTTP/1.1, so anything above that
   merely queues."
  #?(:clj 64 :cljs 6))

;; The budget default and the empty report live on the protocol's namespace, not
;; here: an index type that no-ops `-warm!` must be able to answer in the same
;; shape without depending on the persistent-set walk.
(def default-budget di/default-warm-budget)

;; ---------------------------------------------------------------------------
;; Platform primitives
;;
;; Everything below the walk that touches a node or the storage lives here, so
;; the ClojureScript arm is three named holes rather than a rewrite.

#?(:cljs
   (defn- unsupported! [what]
     (throw (ex-info (str "warm: " what " is not implemented on ClojureScript yet")
                     {:error :warm/cljs-not-implemented :missing what}))))

(defn- branch? [n]
  (instance? Branch n))

(defn- node-level [n]
  #?(:clj  (.level ^ANode n)
     :cljs (.-level n)))

(defn- child-bounds
  "Inclusive child-index bounds of `node`, intersected with [from to] (nil =
   unbounded). `_keys[i]` is the MAX key of child i, so `searchFirst` — the first
   index whose key is >= the probe — names the child that could contain it, for
   both ends of the range.

   TODO(cljs): persistent-sorted-set's ClojureScript arm keeps its equivalent
   (`util/binary-search-l`) private, so this needs either that made public or a
   ~12-line reimplementation. Not written blind."
  [node cmp from to]
  #?(:clj
     (let [^ANode node node
           ^Comparator cmp cmp
           lst (dec (.len node))]
       [(if from (min (max 0 (.searchFirst node from cmp)) lst) 0)
        (if to   (min (max 0 (.searchFirst node to cmp)) lst) lst)])
     :cljs (unsupported! "child-bounds")))

(defn- child-address [node i]
  #?(:clj  (.address ^Branch node (int i))
     :cljs (branch/address node i)))

#?(:clj
   (defn- fetch-wave-blocking!
     "Restore `reqs` ({:addr :storage ..}) with at most `width` in flight.

      Concurrent `restore` is safe: PSS nodes are immutable and content/uuid-
      keyed, and datahike's CachedStorage caches through
      `clojure.core.cache.wrapped` (atom `swap!`). Two threads racing the same
      address duplicate a fetch — wasted work, never a wrong answer.

      A per-call pool rather than a shared one: nothing to own, nothing to shut
      down on a failure path, and a warm is not hot enough for the churn to
      matter."
     [reqs width]
     (let [^ExecutorService pool (Executors/newFixedThreadPool (int (max 1 width)))]
       (try
         (->> reqs
              (mapv (fn [{:keys [^IStorage storage addr]}]
                      (reify Callable (call [_] (.restore storage addr)))))
              (.invokeAll pool)
              (mapv (fn [^Future f] (.get f))))
         (finally
           (.shutdown pool)
           (.awaitTermination pool 120 TimeUnit/SECONDS))))))

(defn- fetch-wave!
  "One level's restores, concurrently. Returns the restored nodes under
   `sync?`, a channel carrying them (or the exception, as a value) otherwise.

   HOISTED OUT of the `async+sync` body on purpose. `async+sync` postwalks ONE
   form into both arms, so any construct without a synchronous counterpart —
   a thread pool, `async/merge`, `pipeline` — cannot appear inside it. (This is
   the same constraint that made `datahike.gc` drop its `async/merge` fan-out
   across branches.) The fan-out is the whole point of this feature, so it lives
   here, on the far side of a `<?-`, where each runtime may implement it its own
   way.

   TODO(cljs): bounded `Promise.all` per level. persistent-sorted-set's cljs
   `IStorage/restore` under `{:sync? false}` returns a partial-cps expression,
   so the shape is: kick off `width` of them at once (each starts its IO
   immediately — JS is single-threaded, so awaiting them in order still
   pipelines), adapt each to a promise-chan the way
   `datahike.index.persistent-set/chan->async-expr` does in the other direction,
   and `<!` them in order."
  [reqs width sync?]
  #?(:clj
     (if sync?
       (fetch-wave-blocking! reqs width)
       ;; Errors travel as VALUES on the channel — the `go-try-`/`<?-`
       ;; convention. `thread` keeps the blocking gather off the go dispatch
       ;; pool; the fan-out itself is the executor above.
       (async/thread
         (try (fetch-wave-blocking! reqs width)
              (catch Exception e e))))
     :cljs (unsupported! "fetch-wave!")))

(defn tree-entry
  "A walk root for one index, or nil when there is nothing to walk.

   TODO(cljs): `.root` restores from the stored address if it is not already in
   hand — free under `:fuse-index-roots?`, where the root rides in the db record.
   The cljs `BTSet` keeps `root` as a raw FIELD that is nil for an address-rooted
   set, and its materializing counterpart (`btset/-root`) is private and async."
  [index-key pset {:keys [from to]}]
  #?(:clj
     (let [^PersistentSortedSet pset pset
           storage (.-_storage pset)
           root    (.root pset)]
       (when (and root storage)
         {:index index-key :node root :storage storage :cmp (.comparator pset)
          :from from :to to}))
     :cljs (unsupported! "tree-entry")))

(defn- now-ns []
  #?(:clj (System/nanoTime) :cljs (* 1e6 (js/Date.now))))

(defn- elapsed-ms [t0]
  (/ (- (now-ns) t0) 1e6))

;; ---------------------------------------------------------------------------
;; The walk

(defn- expand?
  "Should this node's children be fetched? A leaf never has children, so it
   always terminates the walk regardless of policy — which is what makes
   `:interior` fall out of the loop rather than needing to be enforced."
  [node depth round]
  (let [lvl (node-level node)]
    (cond
      (< lvl 1)              false
      (= :interior depth)    (>= lvl 2)
      (= :with-leaves depth) true
      (integer? depth)       (< round depth)
      :else                  false)))

(defn- round-robin
  "Fair interleave of unequal-length colls. Used to share one budget across
   indices: without it, whichever index is enumerated first eats the budget and a
   query against a later index gets nothing warmed."
  [colls]
  (lazy-seq
   (let [colls (remove empty? colls)]
     (when (seq colls)
       (concat (map first colls) (round-robin (map next colls)))))))

(defn- clamp-budget
  "Budget capped to 0.8x the ENTRY-counted node cache. Warming past the cache
   fetches nodes only to evict them, so a budget above it is not a bigger warm —
   it is the same warm plus wasted GETs. The 0.8 leaves room for the query that
   follows to bring in its own leaves without evicting the spine.

   Warns only when the caller ASKED for a budget the cache cannot hold. The
   default budget (2000) already exceeds the default cache (1000), so warning
   unconditionally would fire on every warm of an untuned database and say
   nothing about that database. `:budget-clamped?` reports it either way."
  [budget explicit? store-cache-size]
  (if (and store-cache-size (> budget (* 0.8 store-cache-size)))
    (let [capped (long (* 0.8 store-cache-size))]
      (when explicit?
        (log/warn :warm/budget-clamped
                  {:requested budget :capped capped
                   :store-cache-size store-cache-size
                   :msg "budget exceeded 0.8x the entry-counted node cache; raise :store-cache-size to warm more"}))
      capped)
    budget))

(defn warm-trees!
  "One breadth-first walk across SEVERAL trees, sharing one budget.

   Interleaved rather than sequential: at each round every tree contributes its
   next level, and the budget is spent round-robin across them. Warming eavt to
   exhaustion while avet gets nothing is the wrong answer for a query that reads
   avet, and which index a query needs is not knowable here.

   `entries` is a seq of {:index k :node root :storage s :cmp c :from d :to d}.
   Returns the report under `:sync?`, a channel carrying it otherwise."
  [entries {:keys [depth budget width sync? store-cache-size] :as opts
            :or   {depth :interior budget default-budget width default-width
                   sync? #?(:clj true :cljs false)}}]
  (let [capped   (clamp-budget budget (contains? opts :budget) store-cache-size)
        clamped? (< capped budget)]
    (async+sync
     sync? *default-sync-translation*
     (go-try-
      (let [t0     (now-ns)
            height (reduce max 0 (map (comp node-level :node) entries))]
        (loop [frontier (vec entries)
               round    0
               left     (long capped)
               fetched  0
               by-level []
               per-idx  {}]
          (let [groups (->> frontier
                            (filter #(and (branch? (:node %))
                                          (expand? (:node %) depth round)))
                            (group-by :index))
                reqs   (when (pos? left)
                         (vec (round-robin
                               (for [[_ es] groups]
                                 (for [{:keys [node from to cmp] :as e} es
                                       :let  [[lo hi] (child-bounds node cmp from to)]
                                       i     (range lo (inc hi))
                                       :let  [a (child-address node i)]
                                       ;; nil address = an in-memory child never
                                       ;; stored. Cannot happen on a freshly-
                                       ;; connected cold tree; skipped rather
                                       ;; than trusted.
                                       :when (some? a)]
                                   (assoc e :addr a :node nil))))))]
            (if (empty? reqs)
              {:fetched fetched :by-level by-level :rounds round :by-index per-idx
               :height height :budget-left left :budget-exhausted? false
               :budget-clamped? clamped? :ms (elapsed-ms t0)}
              (let [take-n  (min (count reqs) left)
                    batch   (subvec reqs 0 take-n)
                    nodes   (<?- (fetch-wave! batch width sync?))
                    next-f  (mapv (fn [n r] (assoc r :node n)) nodes batch)
                    left'   (- left take-n)
                    per-idx (reduce (fn [m {:keys [index]}] (update m index (fnil inc 0)))
                                    per-idx batch)]
                (if (zero? left')
                  {:fetched (+ fetched take-n) :by-level (conj by-level take-n)
                   :rounds (inc round) :by-index per-idx :height height
                   :budget-left 0 :budget-exhausted? true
                   :budget-clamped? clamped? :ms (elapsed-ms t0)}
                  (recur next-f (inc round) left' (+ fetched take-n)
                         (conj by-level take-n) per-idx)))))))))))

(defn warm!
  "Breadth-first warm of a persistent-set index into its node cache.

   Options: `:depth` (`:interior` | `:with-leaves` | integer), `:budget`,
   `:width`, `:from`/`:to`, `:store-cache-size`, `:sync?`, `:index-key` (the
   label this index gets in `:by-index`) and `:siblings`.

   `:siblings` is a seq of `[index-key index]` pairs — OTHER indices of the same
   database that share this call's single budget, round-robin, so no index can
   spend it all before another gets any. Which index a query will read is not
   knowable at warm time, and `datahike.warm/warm-db!` is the caller that needs
   this. Passing none warms just `pset`.

   Returns {:fetched :by-level :rounds :height :by-index :budget-left
   :budget-exhausted? :budget-clamped? :ms} — a channel carrying it under
   `:sync? false`. `:by-level` and `:budget-exhausted?` are the point of the
   report: they make a decaying warm visible as a metric before it is visible
   in p99."
  [pset {:keys [index-key siblings] :or {index-key :index} :as opts}]
  #?(:cljs
     ;; The walk short-circuits on ClojureScript rather than throwing: three of
     ;; its platform primitives are unimplemented (see the TODO(cljs) markers
     ;; above) and a "not implemented" raised from inside an OPTIONAL prefetch
     ;; would turn a missing optimisation into a failed read. `:unsupported`
     ;; says so in the report instead of pretending the tree was already warm.
     (di/warm-result (assoc (di/zero-warm-report opts) :unsupported :cljs) opts)
     :clj
     (let [entries (keep (fn [[k idx]] (tree-entry k idx opts))
                         (cons [index-key pset] siblings))]
       (if (seq entries)
         (warm-trees! entries opts)
         (di/warm-result (di/zero-warm-report opts) opts)))))
