(ns ^:no-doc datahike.migrate.init
  "Building datahike's index trees directly from a dump, without transacting.

   ## Who calls this

   `datahike.migrate/import-db` under `:build-indexes? true` — an OPT-IN path, refused
   for anything it cannot reproduce exactly (see `migrate/build-indexes-refusal`). The
   default import still replays the dump through `load-entities`.

   Two properties the caller owes, which the component tests do NOT demonstrate:

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

   ## What a DB record owes beyond six trees

   `assemble-fields` is the answer, and every field in it was a way to get this
   wrong. The two that are not mechanical:

     :hash    is NOT the sum over the current index once history is kept. It is
              an incrementally maintained additive sum, and `with-datom` never
              subtracts a value that a card-one upsert superseded — the datom
              moves to the temporal index and stays counted. `hash-of-records`
              states the rule that falls out: sum over the ASSERTED records,
              which reproduces it in both history modes because the dump's
              records are exactly (current ∪ temporal).
     :schema  is a stored artifact, not something derived at load time
              (`stored->db` reads it from `schema-meta-key`), so a bulk build has
              to produce it. `schema-from-records` folds the dump's own schema
              datoms through datahike's `update-schema`/`remove-schema` rather
              than reconstructing the map by hand — the same code the transact
              path runs, so the two cannot drift.

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
   is a real limit, not a temporary one — index-building and `:merge?` are complements,
   and the condition for taking this path is *empty target AND no `:merge?` AND
   persistent-set*.

   ## Portable

   `.cljc`, and everything it stands on is: `migrate.sort` (the external merge
   sort), `migrate.fs`, `migrate.cbor` and `migrate.compress` are all `.cljc`,
   and `di/init-index-sorted` dispatches to persistent-sorted-set's ClojureScript
   `from-sorted-seq` on that runtime. The only JVM-specific thing left is the
   `^Datom` hint below, which is a reader-conditional import.

   It was `.clj` for a real reason once, and then for none: at the time
   `migrate.sort` was `.clj` (`java.io.File`, `PriorityQueue`) and a cljs compile
   would have failed on `No such namespace`.

   Both of the things that pinned it to the JVM are gone — the sort became
   portable (`sort.cljc`), and `from-sorted-seq` gained its ClojureScript
   counterpart, which `di/init-index-sorted` already dispatches to.

   What is NOT the same on the two runtimes is the SHAPE of a build.
   `di/init-index-sorted` returns a set on the JVM and, under `:sync? false`, a
   partial-cps expression on ClojureScript — so `build-family!` and
   `build-indexes!` are `async+sync` and `build-index!` absorbs the difference.
   The sorted files themselves are read with synchronous `fs` primitives on both
   runtimes, which is why the sorts and the folds can stay lazy seqs: only the
   tree build ever parks."
  (:require [datahike.datom :as dd]
            [datahike.db.transaction :as dbt]
            [datahike.index.interface :as di]
            [datahike.migrate.cbor :as mcbor]
            [datahike.migrate.compress :as mz]
            [datahike.migrate.fs :as fs]
            [datahike.migrate.history :as mh]
            [datahike.migrate.sort :as msort]
            [datahike.schema :as ds]
            [clojure.core.async :as async]
            #?(:clj  [konserve.utils :refer [async+sync *default-sync-translation*]]
               :cljs [konserve.utils :refer [*default-sync-translation*]
                      :refer-macros [async+sync]])
            #?(:clj  [superv.async :refer [go-try- <?-]]
               :cljs [superv.async :refer-macros [go-try- <?-]])
            ;; load-bearing on ClojureScript for the same reason it is in
            ;; `datahike.migrate`: `go-try-` expands into `clojure.core.async/go`,
            ;; and without this refer the compiler picks the CLOJURE macro, whose
            ;; `go-impl` walks `&env` expecting symbol keys.
            #?(:cljs [clojure.core.async :refer-macros [go]]))
  #?(:clj (:import [datahike.datom Datom])))

(def index-families
  "The three sorts. Each yields a `[current temporal]` pair of trees."
  [:eavt :aevt :avet])

(defn- check-family!
  "Refuse an index family this namespace does not build.

   `dd/index-type->cmp-quick`'s `case` has no default clause — anything that is
   not `:aevt` or `:avet` falls through to the `:eavt` comparator. That is fine
   for its 25 callers inside datahike, which pass a family they already hold, and
   wrong here: a typo would sort by `:eavt`, build a tree that is internally
   consistent, and store it under the name of a DIFFERENT index. Nothing
   downstream would notice, because every tree involved is a valid tree.

   Guarded at this boundary rather than in `datom.cljc`, where the fallthrough is
   long-standing and reached dynamically from across the query engine."
  [index-type]
  (when-not (some #{index-type} index-families)
    (throw (ex-info (str "Unknown index family for bulk build: " (pr-str index-type))
                    {:error :init/unknown-index-family
                     :index-type index-type
                     :supported index-families}))))

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
  [(.-e ^Datom d) (.-a ^Datom d) (.-v ^Datom d)
   (dd/datom-tx d) (dd/datom-added d)])

(defn sort-family!
  "External-sort `records` into `index-type`'s TEMPORAL order and return the file.

   Temporal rather than current order on purpose: it is a refinement, so the same
   file serves both trees, and its `added` tie-break is what the currentness fold
   requires. Caller owns the file."
  [records index-type run-size tmp-dir]
  (check-family! index-type)
  (let [cmp (dd/index-type->cmp-quick index-type false)]
    (msort/external-sort-to-file
     records run-size tmp-dir
     (fn [a b] (cmp (record->datom a) (record->datom b))))))

#?(:cljs
   (defn- pcps->chan
     "A partial-cps expression `(fn [resolve reject])` as a core.async channel.

      The REVERSE adapter of `datahike.writing/as-awaitable`, and that function's
      docstring is the explanation for both: two async worlds meet at this seam.
      Everything in datahike's write path is core.async, while
      persistent-sorted-set's ClojureScript builder is partial-cps — so a flush
      handed INTO the builder has to become a continuation (`as-awaitable`), and
      the builder's result handed BACK OUT has to become a channel (this), or the
      `go` block driving the import has nothing it can park on.

      Errors are delivered as VALUES on the channel rather than thrown, which is
      what `<?-` expects and what konserve does everywhere else. A non-Error
      rejection is wrapped, because `<?-` recognises failure by type."
     [pcps]
     (let [ch (async/promise-chan)]
       (pcps (fn [v] (async/put! ch v))
             (fn [e] (async/put! ch (if (instance? js/Error e)
                                      e
                                      (ex-info (str "Bulk index build failed: " (pr-str e))
                                               {:error :init/build-failed :cause e})))))
       ch)))

(defn- build-index!
  "One `di/init-index-sorted`, in the shape `async+sync` expects: the set itself
   in sync mode, a channel of it otherwise.

   ## Two `sync?`s, and conflating them is a bug

   `sync?` is this function's OWN shape — what the caller will do with the
   result. The BUILDER's mode is `(:sync? index-config)`, which is what
   `di/init-index-sorted` reads, and the two are not the same flag: the JVM
   builder is synchronous whatever it is told (it hands back a
   `PersistentSortedSet`), while a JVM import under `:sync? false` still wants a
   channel back. Passing one flag for both produced exactly that mismatch —
   `<?-` on a `PersistentArrayMap`, \"No implementation of method: :take! of
   protocol: ReadPort\".

   On ClojureScript the builder hands back a partial-cps expression whenever its
   own `:sync?` is false (the `:cljs` branch of `di/init-index-sorted`, whose
   default is false there), and this is the one place that is turned back into
   something a `go` block can park on.

   Note the JVM's async arm runs a SYNCHRONOUS build inside a go block, holding a
   dispatch thread for its duration. That is what the rest of the JVM async
   import path already does — every file read in the merge sort is blocking
   too — and making it otherwise would mean an async tree builder the JVM does
   not have."
  [index-name store records index-type index-config sync?]
  (async+sync
   sync? *default-sync-translation*
   (go-try-
    (let [res (di/init-index-sorted index-name store records index-type 0 index-config)]
      #?(:clj res
         :cljs (if (:sync? index-config) res (<?- (pcps->chan res))))))))

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
   so a permuted run ORDER is irrelevant to them.

   `temporal?` false builds ONLY the current tree, and the result has no
   `:temporal` key at all. That is the shape `:keep-history? false` needs:
   `writing/db->stored` omits the temporal keys rather than storing empty ones,
   so building them would write nodes that nothing will ever reference — garbage
   from the moment the commit lands.

   `async+sync` on `sync?` — which is the CALLER's shape, not the builder's; see
   `build-index!` on why those are two flags. The sorted file is read with `fs`
   synchronous primitives on BOTH runtimes (`readSync` on Node), so the reads
   stay a lazy seq; only the tree BUILD is ever asynchronous."
  ([store index-name index-type sorted-file index-config rschema]
   (build-family! store index-name index-type sorted-file index-config rschema true true))
  ([store index-name index-type sorted-file index-config rschema temporal?]
   (build-family! store index-name index-type sorted-file index-config rschema temporal? true))
  ([store index-name index-type sorted-file index-config rschema temporal? sync?]
   (check-family! index-type)
   (async+sync
    sync? *default-sync-translation*
    (go-try-
     (let [no-history (set (:db/noHistory rschema))
           multival   (set (:db.cardinality/many rschema))
          ;; the two classes a live datom of which never reaches temporal
           excluded?  (fn [a] (or (contains? no-history a) (contains? multival a)))
           current-recs (->> (msort/read-sorted-file sorted-file)
                             mh/current-from-eavt-sorted
                             (map record->datom))
           ;; SEQUENTIAL, and not merely for tidiness: both builds write nodes
           ;; through the same `pending-writes` buffer and the same `:flush-fn`,
           ;; which is not written to be entered twice concurrently.
           current (<?- (build-index! index-name store current-recs index-type
                                      index-config sync?))
           temporal (when temporal?
                      (<?- (build-index! index-name store
                                         (->> (msort/read-sorted-file sorted-file)
                                              (mh/temporal-from-eavt-sorted excluded?)
                                              (map record->datom))
                                         index-type index-config sync?)))]
       (cond-> {:current current}
         temporal? (assoc :temporal temporal)))))))

;; ---------------------------------------------------------------------------
;; from six trees to a DB record

;; ---------------------------------------------------------------------------
;; the scratch spool

(def default-spool-chunk-size
  "Records per spool file. Bounds the writer's buffer and the reader's resident
   set, and it is the same order as the dump's own `:chunk-size` for the same
   reason."
  100000)

(defn open-spool!
  "A COMPRESSED, chunked scratch file for the normalised record stream.

   Returns `{:add! (fn [record]) :close! (fn [] -> [file …])}`.

   ## Why chunked files rather than one stream

   gzip is a stream format, but neither runtime gives us a BOUNDED streaming
   reader for one: the JVM would want a `GZIPInputStream` and ClojureScript a
   `zlib` transform stream, and `migrate.fs` deliberately exposes neither —
   `mz/compress-bytes` compresses a BLOCK. Writing fixed-size chunks turns that
   block API into a bounded stream in both directions, with no new framing
   format and no per-runtime code: exactly what the dump's own chunks already
   do, which is why this reuses `mz` and `mcbor` unchanged.

   ## Why compress scratch at all

   The normalised spool holds the whole database, and it used to be raw CBOR
   while the dump it came from was gzipped — so a restore needed several times
   the dump's size in scratch, which is a sizing constraint an operator has to
   discover rather than be told.

   The codec choice is not the dump's choice. A dump is gzip because gzip is
   readable by every tool on every machine, and that portability is worth its
   weak ratio. NOTHING outside this process ever opens a spool file, so
   portability buys nothing here and speed is the only axis that matters —
   which is what `level` is for: a fast level trades a little ratio for less CPU
   on a file that is written once and read three times."
  ([dir] (open-spool! dir mz/default-codec default-spool-chunk-size))
  ([dir codec chunk-size]
   (let [files (volatile! [])
         buf   (volatile! [])
         flush! (fn []
                  (when (seq @buf)
                    (let [f (fs/temp-file! dir "dh-spool-"
                                           (str ".cbor" (mz/extension codec)))
                          sink (fs/open-sink f)]
                      (try
                        (fs/write! sink (mz/compress-bytes codec (mcbor/concat-records @buf)))
                        (finally (fs/close-sink! sink)))
                      (vswap! files conj f)
                      (vreset! buf []))))]
     ;; The buffer holds ENCODINGS, not records. `concat-records` joins per-record
     ;; encodings — it is the same function `write-chunk-stream!` and
     ;; `store/write-chunks!` feed — and handing it records instead was a defect
     ;; that failed differently on each runtime: the JVM raised "No matching
     ;; method alength found taking 1 args" from `concat-records`, while
     ;; ClojureScript read `(.-length record)` as `undefined`, sized a
     ;; `Uint8Array` from it, silently wrote a 20-byte gzip of NOTHING, and
     ;; produced six empty index trees with no error anywhere. Encoding at `add!`
     ;; also makes the buffer's bound a byte count rather than a record count.
     {:add!   (fn [r]
                (vswap! buf conj (mcbor/encode-record r))
                (when (>= (count @buf) (long chunk-size)) (flush!)))
      :close! (fn [] (flush!) @files)})))

(defn spool-records
  "Lazy records from `files`, ONE FILE RESIDENT AT A TIME.

   Written as an explicit `lazy-seq` and emphatically not as
   `(mapcat read files)`: `mapcat` is `(apply concat (map f coll))`, and `map`
   over a chunked collection — a vector of file paths is one — evaluates `f` for
   a whole 32-element block at once. A spool has far fewer than 32 files for any
   database that fits on a laptop, so that spelling decodes EVERY file before the
   consumer sees one record. Measured on the dump reader this replaced: pulling
   one record decoded 14 of 14 chunks, and the import then died in a 256 MB heap
   that the lazy version completes in.

   `decode-records-FROM`, because a decompressed chunk is BYTES. `decode-records`
   takes whatever `fs/reader` hands back for the runtime — an `InputStream` on
   the JVM, a pull FUNCTION on ClojureScript — and boring's JVM reader happens to
   accept a byte array where the ClojureScript one calls its source, so the wrong
   spelling worked on the JVM and failed on Node with `pull.call is not a
   function`. The kind of defect a `.cljc` file cannot have until something
   actually runs it on the second runtime."
  [files codec]
  ((fn step [fs]
     (lazy-seq
      (when-let [f (first fs)]
        (concat (mcbor/decode-records-from
                 (mz/decompress-bytes codec (fs/read-bytes f) {:file (str f)}))
                (step (rest fs))))))
   (seq files)))

(def ^:private temporal-key
  {:eavt :temporal-eavt :aevt :temporal-aevt :avet :temporal-avet})

(defn build-indexes!
  "All six (or three) trees, from a source that can be read repeatedly.

   `records-fn` is a THUNK returning a fresh record seq, not a seq: it is called
   once per family, and a seq held across all three would pin the whole dump in
   memory — the one thing the external sort exists to avoid. Each call may
   re-read the dump from disk; that is three streaming reads, which is the price
   of three sort orders.

   Returns a map keyed exactly as a DB record's index fields are, so the caller
   merges it rather than translating it.

   ## `:avet` is filtered BEFORE its sort, not after

   `:avet` holds only indexed attributes, and `di/init-index-sorted` already
   drops the rest — but it does so downstream of the sort, so the sort was
   ordering every record in the database to build an index that keeps a fraction
   of them. On a schema where one attribute in four is indexed that is most of a
   whole sort wasted.

   Filtering first is safe, and for a reason worth stating rather than trusting:
   both folds in `build-family!` work on runs of equal `[e a v]`, and every
   record in such a run shares its `a`. So removing a non-indexed attribute
   removes WHOLE runs — it can never split one, and neither fold can observe the
   difference.

   The predicate is `(:indexed index-config)`, the same set `init-index-sorted`
   filters by, taken from the same map. The two cannot drift into disagreeing,
   which is what would make this an index that silently omits datoms. The
   downstream filter stays: it is idempotent here, and `init-index-sorted` has
   other callers.

   ## An explicit `loop`, not a `reduce`

   The build is AWAITED on ClojureScript, and a `go` state machine does not enter
   a fn literal — so a reducing function that built a family inside itself could
   not park. The families are walked by an explicit `loop`/`recur` instead, which
   puts the await at a statement position: the same rule that shaped
   `run-import`'s chunk loop and `collect-apply!`'s retries.

   `async+sync` on `sync?` — the CALLER's shape. The builder's own mode travels
   in `index-config`; see `build-index!` on why those are two flags."
  ([store index-name index-config rschema keep-history? records-fn run-size tmp-dir]
   (build-indexes! store index-name index-config rschema keep-history? records-fn
                   run-size tmp-dir true))
  ([store index-name index-config rschema keep-history? records-fn run-size tmp-dir sync?]
   (async+sync
    sync? *default-sync-translation*
    (go-try-
     (let [indexed (:indexed index-config)]
       (loop [families (seq index-families) acc {}]
         (if (nil? families)
           acc
           (let [family (first families)
                 ;; The record seq is built INSIDE the sort-family! call and never
                 ;; bound. `sort-family!` drains it synchronously, but binding it
                 ;; would still pin it: `build-family!` below parks, which
                 ;; decomposes this region, and every binding in a decomposed
                 ;; region becomes a state-machine local that is never cleared.
                 ;; Measured (400k records): bind -> synchronous consume -> park
                 ;; retains the seq exactly as bind -> park -> consume does. `rs`
                 ;; here is the WHOLE database, so this held the entire spool per
                 ;; family — contradicting this fn's own docstring, which says a
                 ;; seq held across all three would pin the dump. It held it for
                 ;; one at a time. See `migrate/sorted-record-seq` for the rule.
                 f (sort-family! (if (= family :avet)
                                   (filter #(contains? indexed (nth % 1)) (records-fn))
                                   (records-fn))
                                 family run-size tmp-dir)
                 {:keys [current temporal]}
                 (<?- (build-family! store index-name family f index-config rschema
                                     (boolean keep-history?) sync?))]
             (recur (next families)
                    (cond-> (assoc acc family current)
                      keep-history? (assoc (temporal-key family) temporal)))))))))))

(defn hash-of-records
  "The `:hash` a transacted database would hold for this dump.

   `:hash` is maintained incrementally by `with-datom`, and the rule it follows
   is not \"the sum over the current index\":

     assert                     += h
     retract, history kept      -= h then += h   (net zero — `hash-datom` covers
                                                  [e a v], so the retraction
                                                  datom hashes the same as the
                                                  datom it retracts)
     retract, no history        -= h
     card-one upsert, history   += h only — the superseded value is NOT
                                  subtracted; it moves to the temporal index and
                                  stays in the sum

   A dump's records are exactly (current ∪ temporal), so summing the ASSERTED
   ones reproduces that: every assertion that ever happened appears once, every
   retraction contributes nothing, and a value forgotten under `:db/noHistory`
   or under `:keep-history? false` is absent from the dump altogether — which is
   the same as having been subtracted.

   Verified against the transact path in both history modes over the adversarial
   database of `migrate-fidelity-test`, which is where the shapes that make this
   non-obvious live: card-one overwrite, retract-then-reassert, `:db/noHistory`
   overwrite, and a retracted entity."
  [records]
  (reduce (fn [acc record]
            (if (nth record 4)
              (+ acc (hash (dd/datom (nth record 0) (nth record 1) (nth record 2))))
              acc))
          0 records))

(defn schema-from-records
  "Fold the dump's schema datoms into `base-db` and return the schema-derived
   fields: `{:schema :rschema :ident-ref-map :ref-ident-map}`.

   `base-db` is the TARGET's empty database value, so the system schema it was
   created with is the starting point and only the dump's own attributes are
   added.

   Folded through `dbt/update-schema` and `dbt/remove-schema` rather than
   reconstructed: those are what `with-datom` calls, so a schema attribute that
   datahike treats specially — many-valued entity-spec attrs, the `:db/ident`
   double-entry into `[:schema e]` and `[:schema ident]`, `key-bearing-misuse`
   validation — is handled by the same code, once. A hand-rolled reconstruction
   is a second implementation of the same rules and would drift.

   Order is transaction-ascending with retractions BEFORE assertions inside a
   transaction, which is the order the temporal comparator already imposes and
   the order a card-one schema change needs: remove the old value, then add the
   new one."
  [base-db records]
  (let [schema? (fn [record]
                  (let [a (nth record 1)]
                    (or (ds/schema-attr? a) (ds/entity-spec-attr? a))))
        ordered (sort-by (fn [record] [(nth record 3) (if (nth record 4) 1 0)])
                         (filter schema? records))
        db (reduce (fn [db [e a v t op]]
                     (let [dt (dd/datom e a v t op)]
                       (dbt/update-rschema
                        (if op
                          (dbt/update-schema db dt)
                          (dbt/remove-schema db dt)))))
                   base-db ordered)]
    (select-keys db [:schema :rschema :ident-ref-map :ref-ident-map])))
