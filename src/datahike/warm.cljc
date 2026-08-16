(ns datahike.warm
  "EXPERIMENTAL. Budget-bounded breadth-first index warming — the db-level entry
   points behind `datahike.api/warm-index!`, `warm-datoms!` and `warm-db!`.

   > ⚠️ **EXPERIMENTAL.** The names and the option map may still change. Nothing
   > here affects results: a warm only moves nodes into the index's node cache
   > earlier and more concurrently than a scan would. Skipping it, or having it
   > run out of budget, costs round trips and never correctness.

   A cold reader's wall time is `misses x RTT` with NOTHING overlapping: a scan
   asks for a node, blocks on the GET, and only then learns the next address.
   Measured at +20ms injected latency, the marginal cost of one more node was
   24.96 ms against a ~25 ms round trip. The fix needs no prediction — a branch
   node holds every child address the moment it is materialized, so a whole
   level's addresses are known one level in advance. These functions walk
   breadth-first and fetch each level concurrently.

   See `datahike.index.persistent-set.warm` for the walk itself, the two bounds
   (`:depth` bounds shape, `:budget` bounds cost, whichever binds first wins),
   and why there is deliberately no `preload everything` mode.

   ## Which entry point

   Three, and they differ only in WHAT gets warmed — the option map is the same
   everywhere.

   `warm-db!` is the connect-time shape: every index at once, sharing one budget
   round-robin so `eavt` cannot eat it before `avet` gets any. Its `:indices`
   option narrows that to a chosen few.

   `warm-datoms!` is the scan-scoped one: it takes the same `[e a v tx]`
   components as `d/datoms` (and, with `:unbounded? true`, as `d/seek-datoms`)
   and warms exactly the subtree that scan will read.

   `warm-index!` warms one whole index, bounded only by `:depth`/`:budget`.

   ## Why range scoping has exactly ONE entry point

   Scoping a warm to a key range means building datom bounds in the index's own
   key order, and that is a step a caller cannot check. `components->pattern`
   PERMUTES components per index (`:avet` reads `[a v e tx]` and produces
   `datom(v, a, e, tx)`) and resolves idents and lookup refs on the way. Hand a
   warm the wrong permutation and it warms a valid-but-different subtree: no
   error, no wrong answer, just a warm that misses and a query that quietly pays
   full price. Nothing short of a coverage check catches it.

   So `warm-datoms!` is the only way to scope a warm to a range, and it builds
   its bounds with datahike's OWN `components->pattern` — the same call
   `datahike.db/contextual-datoms` makes — so a warm and its scan agree by
   construction. `warm-index!` deliberately takes no `:from`/`:to`: shipping the
   trap next to the fix is worse than shipping only the fix."
  (:require [datahike.index :as di]
            [datahike.db.utils :as dbu]
            [datahike.constants :as const]
            [datahike.datom :refer [datom]])
  #?(:cljs (:require-macros [datahike.datom :refer [datom]])))

(def index-keys
  "The indices `warm-db!` considers, in order. The temporal twins exist only
   under `:keep-history?`; absent ones are skipped."
  [:eavt :aevt :avet :temporal-eavt :temporal-aevt :temporal-avet])

(defn- base-index-type
  "`:temporal-avet` -> `:avet`. `components->pattern` knows only the three
   primary index orders, and a temporal index sorts in the SAME order as its
   primary twin — `datahike.db.utils/temporal-datoms` builds one pattern from
   the primary type and slices both trees with it."
  [index-type]
  (case index-type
    (:temporal-eavt) :eavt
    (:temporal-aevt) :aevt
    (:temporal-avet) :avet
    index-type))

(defn- present
  "The index value at `index-type`, or nil. A wrapped db (historical, as-of,
   filtered) has no index fields of its own, so this is nil there and every
   entry point degrades to a no-op — warm the underlying db instead."
  [db index-type]
  (get db index-type))

(defn- base-opts
  "The option map the walk gets, with `:store-cache-size` read off the db.

   `:from`/`:to` are DROPPED here, and this is the only place they can be. The
   walk supports key bounds and `warm-datoms!` puts them back immediately below;
   what must not exist is a public entry point that takes them from a caller —
   see this namespace's docstring on why a hand-built bound is a silent miss
   rather than an error."
  [db opts]
  (-> opts
      (dissoc :from :to)
      (assoc :store-cache-size (get-in db [:config :store-cache-size]))))

(defn warm-index!
  "EXPERIMENTAL. Breadth-first warm of ONE whole index into its node cache,
   bounded by `:depth` and `:budget`.

   Options:
     :depth   `:interior` (default) | `:with-leaves` | integer
     :budget  nodes to fetch at most (default 2000), clamped to 0.8x
              `:store-cache-size` — that cache is ENTRY-counted, so warming past
              it fetches nodes only to evict them
     :width   concurrent in-flight restores (default 64 on the JVM, 6 on cljs)
     :sync?   return the report (default true on the JVM) or a channel

   Deliberately takes NO key range. Range scoping has exactly one entry point,
   `warm-datoms!`, and it is the safe one: bounds must be datoms in the index's
   own key order, which `components->pattern` reaches by PERMUTING the
   components per index (`:avet` reads `[a v e tx]` and produces
   `datom(v, a, e, tx)`) and resolving idents. A hand-built bound that gets that
   wrong warms a valid-but-different subtree — no error, no wrong answer, just a
   warm that misses — and nothing short of a coverage check notices. Shipping
   the trap beside the fix is worse than shipping only the fix.

   Returns {:fetched :by-level :rounds :height :by-index :budget-left
   :budget-exhausted? :budget-clamped? :ms}. `:by-level` and `:budget-exhausted?`
   are the point of the report: they make a decaying warm visible as a metric
   before it is visible in p99."
  ([db index-type] (warm-index! db index-type {}))
  ([db index-type opts]
   (if-let [idx (present db index-type)]
     (di/-warm! idx (assoc (base-opts db opts) :index-key index-type))
     (di/warm-result (di/zero-warm-report opts) opts))))

(defn warm-datoms!
  "EXPERIMENTAL. Warm exactly the subtree a components-scoped scan will read.

   Mirrors the two datahike scans that take `[e a v tx]` components:

     (warm-datoms! db idx cs)                    <-> (d/datoms db idx & cs)
     (warm-datoms! db idx cs {:unbounded? true}) <-> (d/seek-datoms db idx & cs)

   (Looking for a `warm-seek!`? It is the second line. It was folded in here
   rather than kept as a fourth entry point.)

   `components` is that same prefix, in the index's own component order
   (`:avet` -> `[a v e tx]`), and may be shorter or empty.

   The bounds come from `datahike.db.utils/components->pattern` — the SAME call
   `datahike.db/contextual-datoms` makes to build its `-slice` arguments, with
   the same `e0`/`tx0` and `emax`/`txmax` fills. That is the point of this
   function: warm and scan derive their range from one function, so they agree
   by construction rather than by the caller getting a permutation right. It is
   also why this is the ONLY entry point that scopes a warm to a range.

   Options are `warm-index!`'s, plus:

     :unbounded?  false (default) — a bounded range, `d/datoms` semantics: the
                  upper bound is the components pattern, so the work is
                  proportional to the RANGE.
                  true — READAHEAD, `d/seek-datoms` semantics: the upper bound
                  is the END of the index (`datahike.db/contextual-seek-datoms`
                  is asymmetric that way), so there is no range to be
                  proportional to and `:budget` is the only thing bounding the
                  work. That is the right shape for a cursor about to consume an
                  unknown amount and the wrong shape for one reading a single
                  head — size the budget to what you expect to consume.

   Returns `warm-index!`'s report."
  ([db index-type components] (warm-datoms! db index-type components {}))
  ([db index-type components {:keys [unbounded?] :as opts}]
   (if-let [idx (present db index-type)]
     (let [bt (base-index-type index-type)]
       (di/-warm! idx (assoc (base-opts db opts)
                             :index-key index-type
                             :from (dbu/components->pattern db bt components
                                                            const/e0 const/tx0)
                             :to   (if unbounded?
                                     (datom const/emax nil nil const/txmax)
                                     (dbu/components->pattern db bt components
                                                              const/emax const/txmax)))))
     (di/warm-result (di/zero-warm-report opts) opts))))

(defn warm-db!
  "EXPERIMENTAL. Warm every present index of `db`, sharing ONE budget
   round-robin across them. The common case, and the connect-time shape.

   Round-robin rather than one index at a time: warming `eavt` to exhaustion
   while `avet` gets nothing is the wrong answer for a query that reads `avet`,
   and which index a query needs is not knowable here. The budget is therefore
   interleaved at the level of individual fetches, so a budget too small for all
   the indices is SPLIT rather than eaten by whichever is enumerated first.

   Takes `warm-index!`'s options plus `:indices` — the index keys to consider,
   defaulting to `index-keys`. A single-element `:indices` covers the one-index
   case too.

   `:by-index` in the report says where the budget actually went."
  ([db] (warm-db! db {}))
  ([db {:keys [indices] :as opts}]
   (let [ks     (filter #(present db %) (or indices index-keys))
         opts   (base-opts db opts)]
     (if (seq ks)
       ;; The walk is one call, not one per index — a shared budget cannot be
       ;; split before the walk knows how much frontier each tree has. The first
       ;; index dispatches the protocol (all of a db's indices are the same
       ;; type); the rest ride along as `:siblings`.
       (di/-warm! (present db (first ks))
                  (assoc opts
                         :index-key (first ks)
                         :siblings (mapv (fn [k] [k (present db k)]) (rest ks))))
       (di/warm-result (di/zero-warm-report opts) opts)))))
