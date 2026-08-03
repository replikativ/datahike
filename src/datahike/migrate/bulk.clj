(ns ^:no-doc datahike.migrate.bulk
  "Building datahike's index trees directly from a dump, without transacting.

   ## NOT WIRED UP — read this first

   Nothing in `src` calls this namespace. `import-db` always replays a dump
   through `load-entities`, and there is no `:bulk?` option. It is exercised by
   `datahike.test.migrate-bulk-build-test`, which checks both trees of all three
   families against the database they must reproduce — so it is a tested
   component awaiting a caller, not an untested one.

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

   The direction is not in doubt: the bulk build is order-INDEPENDENT and writes
   each node once, while incremental insertion is order-dependent and its write
   amplification grows with the database.

   NO TRUSTWORTHY MAGNITUDE HAS BEEN MEASURED YET, and the figures this docstring
   used to quote (3.9x / 4.7x / 13.1x, amplification 16.8x / 32.6x / 51.4x) are
   withdrawn. They came from a component harness with four defects: it compared
   against `d/transact` rather than `import-db`; it wrapped the sorted stream in
   a full in-memory `sort` INSIDE the timed region, which both inflates the time
   and defeats the streaming property being measured; it passed no `:flush-fn`,
   so the memory bound was not exercised; and the bulk arm never committed while
   the transact arm committed ten times. It also ran `:keep-history? false`, so
   the temporal trees and the currentness fold — the expensive half — were never
   in the measurement at all.

   Re-measure end to end, import against import, before quoting a number.

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

   Only for an EMPTY target: building indexes directly cannot honour the upsert
   semantics `load-entities` applies when datoms meet an existing database. That
   is a real limit, not a temporary one — bulk and `:merge?` are complements,
   and the condition for taking this path is *empty target AND no `:merge?` AND
   persistent-set*.

   Still `.clj`, and now only by inertia. It was `.cljc` by mistake once, caught
   by checking the extension against what the file actually requires: at the time
   `migrate.sort` was `.clj` (`java.io.File`, `PriorityQueue`) and a cljs compile
   would have failed on `No such namespace`.

   Both of the things that pinned it to the JVM are gone — the sort became
   portable (`sort.cljc`), and `from-sorted-seq` gained its ClojureScript
   counterpart, which `di/init-index-sorted` already dispatches to. Nothing here
   is JVM-specific any more except the `^datahike.datom.Datom` hints in
   `datom->tuple`. Making it `.cljc` is a rename plus a reader conditional, and
   belongs with wiring the caller rather than ahead of it."
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

   Reads the file TWICE — once through the temporal filter, once through the
   currentness fold. Two streams, one sort.

   `rschema` is the SOURCE database's reverse schema, and it is taken whole
   rather than as a pre-extracted `{:keys [no-history multival]}` on purpose.
   The previous signature read `:no-history` out of `index-config`, which
   NOTHING in datahike ever puts there — `db.cljc`'s two index-config
   constructors do not — so the filter it guarded was a permanent no-op and the
   temporal trees were built from every record. Taking the map that actually
   holds the data makes that failure unrepresentable rather than merely fixed.

   `:avet` holds only indexed attributes, but that filter is NOT applied here:
   `di/init-index-sorted` already applies it from `index-config`'s `:indexed`,
   and applying it twice was redundant.

   Both folds want a stream sorted by `[e a v t]`, and this is fed `:aevt` and
   `:avet` order too. That is sound, and it is worth saying why rather than
   leaving it to be rediscovered: all three family keys are PERMUTATIONS of
   `[e a v]` followed by `t`, so every record for one `[e a v]` is adjacent and
   `t`-ascending in all three. The folds only ever ask about one run at a time,
   so a permuted run ORDER is irrelevant to them."
  [store index-name index-type sorted-file index-config rschema]
  (let [no-history (set (:db/noHistory rschema))
        multival   (set (:db.cardinality/many rschema))
        ;; the two classes a live datom of which never reaches temporal
        excluded?  (fn [a] (or (contains? no-history a) (contains? multival a)))
        temporal (->> (msort/read-sorted-file sorted-file)
                      (mh/temporal-from-eavt-sorted excluded?)
                      (map record->datom))
        current (->> (msort/read-sorted-file sorted-file)
                     mh/current-from-eavt-sorted
                     (map record->datom))]
    {:temporal (di/init-index-sorted index-name store temporal index-type 0 index-config)
     :current (di/init-index-sorted index-name store current index-type 0 index-config)}))
