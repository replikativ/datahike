(ns ^:no-doc datahike.migrate.bulk
  "Building datahike's index trees directly from a dump, without transacting.

   ## NOT WIRED UP — read this first

   Nothing calls this namespace. `import-db` always replays a dump through
   `load-entities`, and there is no `:bulk?` option; `build-family!` and
   `sort-family!` have no callers outside the tests that exercise them directly.
   Treat it as a working component with a measured payoff, not as a path any
   user reaches today.

   A caller that wires it up owes two things the tests do NOT demonstrate:

     1. Hold the GC guard across the WHOLE sequence — `(guard/writing! store-id)`
        before the first build, `(guard/done! …)` after the commit that publishes
        the root. Bulk-built nodes are written before anything references them,
        and `datahike.gc-guard` explains what a concurrent mark does to values in
        that window. `di/init-index-sorted` deliberately does not drain
        `pending-writes` on its own for exactly this reason — pass
        `:flush-fn (datahike.writing/bulk-flush-fn store sync?)` to bound memory,
        having taken the guard.
     2. Accept that an ABORTED build is now destructive under
        `:crypto-hash? false`. Flushed nodes may reuse freelist addresses, so a
        build that fails midway has already overwritten them.

   ## Why it is worth wiring

   `import-db` inserts datom by datom, which pays B-tree insertion costs for data
   that is already known in full — the case PostgreSQL's `nbtsort.c` exists to
   avoid, and the reason `from-sorted-seq` was added to persistent-sorted-set.

   Measured on a FILE-backed store, entity ids shuffled so index keys scatter
   (transact vs external sort + three streaming builds):

     datoms   transact   bulk    speedup   node-write amplification
       0.6M     26.0 s    6.6 s    3.9x       16.8x
       1.2M     75.7 s   16.0 s    4.7x       32.6x
       2.0M    191.6 s   14.6 s   13.1x       51.4x

   The bulk build is order-INDEPENDENT; incremental insertion is not, and its
   write amplification grows with the database. With ASCENDING ids — every insert
   landing at the right edge of the tree, which is the best case for incremental
   and roughly what a monotonic id remapping produces for `eavt` — the same
   comparison at 0.6M datoms is only 1.4x. So the payoff is governed by key
   scatter and scale together, not by scale alone, and `aevt`/`avet` are the
   families that scatter.

   ## Three sorts, six trees

   A `:keep-history? true` database keeps six trees, and the obvious reading is
   six sorts. It is three, because the temporal comparator is a REFINEMENT of the
   current one:

     current  eavt   [e a v tx]
     temporal eavt   [e a v tx added]

   Same prefix. So a stream sorted temporally is already sorted for the current
   index — verified, not assumed — and one sorted file feeds both trees of a
   family: the whole stream builds the temporal tree, and the subset surviving
   `history/current-from-eavt-sorted` builds the current one.

   The `added` tie-break is also exactly what the currentness fold needs. The
   temporal comparator orders retraction before assertion (-1); the current one
   treats them as equal (0), so it could not be used for the fold even though it
   sorts the same records.

   ## Memory

   Bounded, with one exception that is not this namespace's to fix: the
   O(entities) id map from `migrate.ids`, which `estimate-import-memory` already
   calls \"the dominant, unavoidable term\". Everything else streams — the sort
   spills to disk, the currentness fold holds one run, and `from-sorted-seq`
   holds one node per level.

   ## Scope

   JVM only, and only for an EMPTY target. `from-sorted-seq` has no ClojureScript
   counterpart yet, and building indexes directly cannot honour the upsert
   semantics `load-entities` applies when datoms meet an existing database.

   `.clj` and not `.cljc` deliberately. It used to be `.cljc` by mistake, caught
   by checking the extension against what the file actually requires — at the
   time `migrate.sort` was `.clj` (`java.io.File`, `PriorityQueue`) and a cljs
   compile would have failed on `No such namespace`.

   The sort is portable now (`sort.cljc`), so only ONE thing still pins this
   file to the JVM: `di/init-index-sorted`, whose `from-sorted-seq` has no
   ClojureScript counterpart. When that lands, this becomes `.cljc` by deleting
   nothing — which is the point of having moved the sort first."
  (:require [datahike.datom :as dd]
            [datahike.index.interface :as di]
            [datahike.migrate.history :as mh]
            [datahike.migrate.sort :as msort]))

(def index-families
  "The three sorts. Each yields a `[current temporal]` pair of trees."
  [:eavt :aevt :avet])

(defn record->datom
  "`[e a v t added]` -> Datom. The comparators are over Datoms, so records become
   datoms before sorting rather than after."
  [record]
  (dd/datom (nth record 0) (nth record 1) (nth record 2)
            (nth record 3) (nth record 4)))

(defn datom->tuple
  "Datom -> `[e a v t added]`. The inverse of `record->datom` for `t > 0`.

   NOT `datom->record`, which is what it used to be called and which collides
   with `manifest/datom->record` — a different operation on the same-shaped
   data: that one resolves attribute idents and translates system-entity refs
   into `#datahike/sysref`, this one is a raw field read."
  [d]
  [(.-e ^datahike.datom.Datom d) (.-a ^datahike.datom.Datom d) (.-v ^datahike.datom.Datom d)
   (dd/datom-tx d) (dd/datom-added d)])

(defn sort-family!
  "External-sort `records` into `index-type`'s TEMPORAL order and return the file.

   Temporal rather than current order on purpose: it is a refinement, so the same
   file serves both trees, and its `added` tie-break is what the currentness fold
   requires. Caller owns the file."
  [records index-type run-size tmp-dir]
  (let [cmp (dd/index-type->cmp-quick index-type false)]
    (msort/external-sort-to-file
     records run-size tmp-dir
     (fn [a b] (cmp (record->datom a) (record->datom b))))))

(defn build-family!
  "Build the `[current temporal]` trees for one index family from a sorted file.

   Reads the file TWICE — once whole for the temporal tree, once through the
   currentness fold for the current tree. Two streams, one sort.

   `:avet` holds only indexed attributes; that filter is applied HERE, before the
   builder sees the stream, so nothing indexes a datom it will discard."
  [store index-name index-type sorted-file
   {:keys [indexed no-history] :as index-config}]
  (let [avet? (= index-type :avet)
        keep? (if avet?
                (fn [r] (contains? indexed (nth r 1)))
                (constantly true))
        ;; `:db/noHistory` attributes are present in the CURRENT trees and absent
        ;; from the temporal ones. Verified rather than assumed: a database with
        ;; one plain attribute showed :db/txInstant twice in `eavt` and not at all
        ;; in `temporal-eavt`, and `:db/txInstant` carries `:db/noHistory true`.
        ;;
        ;; This is why `(d/datoms (d/history db) :eavt)` is the UNION of the two
        ;; rather than the temporal tree alone, and why an earlier version of this
        ;; build produced temporal trees larger than the source's — it fed them
        ;; the whole history, noHistory attributes included.
        historic? (fn [r] (not (contains? no-history (nth r 1))))
        temporal (->> (msort/read-sorted-file sorted-file)
                      (filter keep?)
                      (filter historic?)
                      (map record->datom))
        current (->> (msort/read-sorted-file sorted-file)
                     (filter keep?)
                     mh/current-from-eavt-sorted
                     (map record->datom))]
    {:temporal (di/init-index-sorted index-name store temporal index-type 0 index-config)
     :current (di/init-index-sorted index-name store current index-type 0 index-config)}))
