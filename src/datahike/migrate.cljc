(ns datahike.migrate
  "Robust, type-exact, verifiable export/import for datahike databases.

   The 3-arity `export-db`/`import-db` produce and consume a *type-exact* CBOR
   dump (see `doc/import-export-design.md` and `datahike.migrate.cbor`) that
   round-trips full history, every builtin value type (fixing #633), schema
   ordering (#262), the tx log (#377), and attribute-refs databases (#508)
   without re-inserting system datoms (#531).

   The dump is a CBOR sequence (RFC 8742): one datom per top-level item, no
   delimiter, encoded with boring's `:archival` profile — sorted map keys so two
   exports are byte-identical, and fixed-width floats so a `:db.type/double` does
   not come back a `Float`. An earlier iteration used EDN-lines, which existed
   only to work around clj-cbor narrowing three double values; boring removed the
   reason, and with it the base64 wrapper every binary value used to need.

   Old single-file CBOR dumps — the format released datahike wrote — are still
   READ on import; a dump is chosen by the shape of the source, not by an arity.
   Import runs through `load-entities`, which remaps entity/tx ids while
   preserving `[e a v t op]` structure — a restored db is semantically
   equivalent, never id-identical.

   NEITHER export nor import is resumable. The import's id-remap (`:migration`)
   is memory-only and dropped on reconnect, so a partial import must be recreated
   and restarted; a partial export leaves a directory with no manifest, since the
   manifest is written last as the commit marker, and is restarted the same way.
   (This used to claim export was resumable because completed chunks were
   content-addressed. They are not — they are numbered `datoms-NNNNNN`, and
   `write-chunked!` always restarts at 1.)

   A `:history? true` export resurrects retracted data — see the data-protection
   note in `import-db`/the backup guide."
  (:require [datahike.api :as api]
            [datahike.writer :as dwriter]
            [datahike.constants :as c]
            [datahike.datom :as d]
            [datahike.db.interface :as dbi]
            [datahike.db.utils :as dbu]
            [datahike.db.transaction :as dbt]
            [datahike.schema :as ds]
            [datahike.tools :as dt]
            [datahike.gc-guard :as guard]
            [datahike.writing :as dwriting]
            [datahike.migrate.cbor :as mcbor]
            [datahike.migrate.ids :as ids]
            [datahike.migrate.init :as init]
            [datahike.migrate.digest :as dig]
            [datahike.migrate.compress :as mz]
            [datahike.migrate.fs :as fs]
            ;; JVM-only by nature: a legacy single-file dump can only have been
            ;; written by an old JVM datahike.
            #?(:clj [datahike.migrate.legacy :as mlegacy])
            [datahike.migrate.schema :as msch]
            [datahike.migrate.manifest :as mman
             :refer [a-ident attribute-refs? build-manifest
                     chunk-re codec-of config-must-match datom->record
                     assert-sync-supported! assert-jvm-only! assert-sizes-positive!
                     assert-codec-supported!
                     default-batch-size default-chunk-size
                     default-sync? estimate-from-manifest
                     export-records export-records-streaming
                     ident-schema manifest-key norm-val read-manifest-map
                     system-idents bytes->human]]
            [datahike.migrate.sort :as msort]
            [datahike.migrate.store :as mstore]
            [datahike.migrate.blobs :as mblobs]
            [clojure.edn :as edn]
            [clojure.core.async :as async]
            [konserve.core :as k]
            [konserve.binary :as kb]
            #?(:clj  [konserve.utils :refer [async+sync *default-sync-translation*]]
               :cljs [konserve.utils :refer [*default-sync-translation*]
                      :refer-macros [async+sync]])
            #?(:clj  [superv.async :refer [go-try- <?-]]
               :cljs [superv.async :refer-macros [go-try- <?-]])
            ;; load-bearing on ClojureScript: `go-try-` expands into
            ;; `clojure.core.async/go`, and without this refer the compiler picks
            ;; the CLOJURE macro, whose `go-impl` walks `&env` expecting symbol
            ;; keys — cljs `&env` is the compiler map, with keyword keys.
            #?(:cljs [clojure.core.async :refer-macros [go]])))

(def ^:dynamic *import-batch-size*
  "Transactions per batch when `import-db` reads a LEGACY single-file dump.
   Has no effect on the manifest-and-chunks format, which takes `:batch-size`
   as an option instead.

   Defined here rather than beside the legacy reader it governs, and that is
   deliberate. The CHANGELOG announced this var by its fully-qualified name —
   `datahike.migrate/*import-batch-size*` ([#845]) — so this is its published
   home, and a `binding` written against that announcement must keep working.

   It cannot simply be `def`-ed in `datahike.migrate.legacy` and referred to
   here: `datahike.migrate` already requires that namespace, so the reverse
   require would be a cycle. Nor can it be aliased — an alias is a NEW var, so
   `(binding [datahike.migrate/*import-batch-size* 5] …)` would set something
   nothing reads, turning today's loud compile error into a silent no-op. So
   the var lives here and its value is passed to `import-db-legacy` as an
   argument, which is read at the call site and therefore inside the caller's
   binding scope."
  10000)

;; Public names that used to be defined here and now live in
;; `datahike.migrate.manifest`. Re-exported rather than left to `:refer`, which
;; maps a symbol into this namespace WITHOUT interning a var — so `m/format-version`
;; and `m/check-capabilities!` would have stopped resolving for anyone outside.
;; Defined below, next to `user-datom-count` which it uses; needed by the two
;; manifest-building call sites above it.
(declare with-source-count refuse-incomplete-dump!)

(def format-version mman/format-version)
(def check-capabilities! mman/check-capabilities!)

(defn- warn!
  "Operator-facing warning on stderr.

   One helper rather than two hand-written `(binding [*out* *err*] (println
   (format …)))` forms, because neither `*err*` nor `format` exists in
   ClojureScript — `console.warn` is the counterpart, and a warning that throws
   is worse than no warning."
  [msg]
  #?(:clj (binding [*out* *err*] (println msg))
     :cljs (js/console.warn msg)))

(defn- write-chunk-stream!
  "Write up to `limit` records from the (lazy) seq `records` to `f` as a CBOR
   sequence, updating the semantic-digest accumulator `dacc` and computing the
   chunk SHA-256 incrementally — memory bounded by one compression block. Returns
   [remaining-records count sha256-hex dacc' raw-bytes].

   No delimiter is written: consecutive top-level CBOR items ARE an RFC 8742
   sequence, so the framing is a property of the encoding rather than something
   this loop maintains. The same bytes feed the file, the chunk hash and the
   semantic digest, so all three agree by construction."
  [p records limit dacc codec]
  (let [md (dig/sha256-accumulator)
        sink (fs/open-sink p)
        ;; The digest is fed the RAW record bytes, before compression — see
        ;; `migrate.compress`. Records accumulate into a block and the block is
        ;; compressed as a unit, so memory is bounded by the block rather than
        ;; by the chunk, on both runtimes and with one implementation.
        flush-block! (fn [block n]
                       (when (pos? (long n))
                         (fs/write! sink (mz/compress-bytes codec (mcbor/concat-records block)))))]
    (try
      (loop [rs (seq records) c 0 da dacc block [] bn 0 raw 0]
        (if (and rs (< c limit))
          (let [bs (mcbor/encode-record (first rs))
                len (alength ^bytes bs)
                bn' (+ (long bn) (long len))
                block' (conj block bs)]
            (dig/sha256-update! md bs)
            (if (>= bn' mz/default-block-size)
              (do (flush-block! block' bn')
                  (recur (next rs) (inc c) (dig/add-record da bs) [] 0 (+ (long raw) (long len))))
              (recur (next rs) (inc c) (dig/add-record da bs) block' bn' (+ (long raw) (long len)))))
          (do (flush-block! block bn)
              [rs c (dig/sha256-finalize md) da raw])))
      (finally (fs/close-sink! sink)))))

(defn- write-chunked! [db opts dir sorted-records chunk-size progress]
  (fs/mkdirs! dir)
  (when-not (fs/directory? dir)
    (throw (ex-info (str "Could not create the dump directory " dir)
                    {:error :export/mkdir-failed :dir (str dir)})))
  (fs/restrict-perms! dir true)
  (let [codec (get opts :compression mz/default-codec)]
    (loop [ls (seq sorted-records) n 1 chunks [] dacc (dig/accumulator)]
      (if (nil? ls)
      ;; `:compression` is stamped by the WRITER, so the codec recorded is the
      ;; one that was used. See `migrate.store/write-chunks!` for why that
      ;; matters: a manifest that disagrees with the bytes reports corruption on
      ;; an intact dump.
        (let [manifest (assoc (build-manifest db (with-source-count db opts) (dig/finalize dacc) chunks)
                              :compression codec)]
          (fs/spit-text! (fs/join dir "manifest.edn") (pr-str manifest))
          (fs/restrict-perms! (fs/join dir "manifest.edn") false)
          (progress {:phase :done :datoms (:count (dig/finalize dacc))})
          manifest)
        (let [fname (mstore/chunk-name n codec)
              tmp   (fs/join dir (str fname ".tmp"))
              final (fs/join dir fname)
              [rem cnt sha dacc' raw] (write-chunk-stream! tmp ls chunk-size dacc codec)]
          (when-not (fs/rename! tmp final)
            (throw (ex-info (str "Could not move the finished chunk into place: "
                                 tmp " -> " final ". The manifest would name a file "
                                 "that is not there.")
                            {:error :export/rename-failed :from (str tmp) :to (str final)})))
          (fs/restrict-perms! final false)
          (progress {:phase :chunk :datoms cnt})
          (recur (seq rem) (inc n)
                 (conj chunks (mstore/chunk-descriptor
                               fname cnt (fs/file-size final) raw sha))
                 dacc'))))))

(defn- blob-dir
  "The directory carried blobs live in for a filesystem dump."
  [target]
  (fs/join target mblobs/dir-name))

(defn- with-blob-writer
  "Call `f` with `(fn [id bytes opts])` writing one blob into `target`'s blob
   area, releasing the medium afterwards.

   Takes a callback rather than returning the function because a store target
   has to be OPENED, and the previous shape — return a closure over an open
   store — had nowhere to close it. `blob-writer`, `blob-reader` and
   `verify-blobs` each leaked one connection per call, so an export plus an
   import plus a verify against a `{:backend ...}` config opened three stores and
   released none.

   `opts` carries `:sync?`, and it is load-bearing twice over. This used to call
   the ONE-arity `mstore/open`/`close`, which force `{:sync? true}` — so against
   a `{:backend ...}` target it could not connect on Node at all, and worse, the
   release sat in an ordinary `finally`. `f` returns a CHANNEL in async mode, so
   that `finally` fired the moment the channel was handed back, releasing the
   store before a single blob had been written. That is the same use-after-release
   `import-db` documents twenty lines below; it simply had a second home here.
   Hence the explicit close on both the success and failure path, INSIDE the go
   block, since core.async cannot park in a `finally`."
  [target opts f]
  (async+sync
   (:sync? opts) *default-sync-translation*
   (go-try-
    (if (mstore/store-target? target)
      (let [m (<?- (mstore/open target opts))
            res (try
                  ;; `async+sync` rather than `<??`: this closure is called from
                  ;; `mblobs/copy-out!`, which is itself async+sync, so a blocking
                  ;; take here would have pinned the whole blob path to the JVM
                  ;; even though `migrate.blobs` is portable — portability
                  ;; stopping one layer short of where it is used.
                  (<?- (f (fn [id bytes o]
                            (async+sync (:sync? o) *default-sync-translation*
                                        (go-try-
                                         (<?- (k/bassoc (:store m) (mstore/blob-key (:prefix m) id)
                                                        bytes o)))))))
                  (catch #?(:clj Exception :cljs :default) e e))]
        (<?- (mstore/close m opts))
        (if (instance? #?(:clj Throwable :cljs js/Error) res) (throw res) res))
      (let [dir (blob-dir target)]
        (when-not (fs/mkdirs! dir)
          (when-not (fs/directory? dir)
            (throw (ex-info (str "Could not create the blob directory " dir)
                            {:error :export/blob-dir-failed :dir (str dir)}))))
        ;; The filesystem writes are synchronous on both runtimes, but the shape
        ;; still has to match what `copy-out!` awaits — value in sync mode, channel
        ;; otherwise — or the async branch would await a plain value.
        (<?- (f (fn [id bytes o]
                  (async+sync (:sync? o) *default-sync-translation*
                              (go-try-
                               (let [sink (fs/open-sink (fs/join dir (str id)))]
                                 (try (fs/write! sink bytes)
                                      (finally (fs/close-sink! sink))))))))))))))

(defn- with-blob-reader
  "Call `f` with `(fn [id opts]) -> bytes-or-nil` reading blobs out of a dump,
   releasing the medium afterwards. See `with-blob-writer`, including why `opts`
   and the explicit close are load-bearing rather than tidiness."
  [source opts f]
  (async+sync
   (:sync? opts) *default-sync-translation*
   (go-try-
    (if (mstore/store-target? source)
      (let [m (<?- (mstore/open source opts))
            res (try
                  ;; `to-bytes` rather than a hand-rolled callback: `bget`'s handle
                  ;; has four different shapes across backends and platforms, and
                  ;; that knowledge now lives in konserve
                  ;; (replikativ/konserve#162) instead of being re-derived at every
                  ;; call site — this was one of them.
                  (<?- (f (fn [id o]
                            (async+sync (:sync? o) *default-sync-translation*
                                        (go-try-
                                         (<?- (k/bget (:store m) (mstore/blob-key (:prefix m) id)
                                                      (kb/to-bytes o) o)))))))
                  (catch #?(:clj Exception :cljs :default) e e))]
        (<?- (mstore/close m opts))
        (if (instance? #?(:clj Throwable :cljs js/Error) res) (throw res) res))
      (<?- (f (fn [id o]
                (async+sync (:sync? o) *default-sync-translation*
                            (go-try-
                             (let [f' (fs/join (blob-dir source) (str id))]
                               (when (fs/exists? f') (fs/read-bytes f'))))))))))))

(defn- tx-aligned-chunks
  "LAZY seq of chunks of `records`, each at least `n` long, never splitting a
   transaction.

   `n` is a MINIMUM: a chunk takes whole `t` runs until it reaches `n`, so it
   overshoots rather than cutting one in half. A split transaction would hand a
   sink a fragment to transact, and hand the importer's batcher — which flushes
   on `t` changing — a group it cannot recognise as complete.

   Lazy on purpose. Realizing every chunk would materialize the whole database,
   which is what `:chunk-size` exists to avoid.

   BOUNDED BY ONE TRANSACTION, NOT BY `n`. `partition-by` streams, so this holds
   one chunk at a time only while `t` VARIES. A stream whose `t` never changes —
   a database built by one large `transact`, or by `load-entities` — is a single
   `partition-by` group, and `into` materializes all of it. Measured: 3M records
   at a constant `t` OOM at -Xmx256m even when the result is consumed lazily;
   the same 3M with `t` changing every 1000 records is fine.

   Note `:max-pending` does NOT cover this. That backstop lives in the importer's
   batcher, downstream of here, so a caller who reads `import-source` rule 3 and
   sets it is not protected against this.

   Never-splitting and bounding a single transaction are in tension: one
   transaction is the unit a sink can commit, so cutting it would hand a sink a
   fragment. The bound is therefore the largest transaction, and that is the
   contract rather than an oversight."
  [records n]
  (letfn [(step [groups]
            (lazy-seq
             (when (seq groups)
               (loop [acc [] cnt 0 gs groups]
                 (if (and (seq gs) (< cnt (long n)))
                   (let [g (first gs)]
                     (recur (into acc g) (+ cnt (count g)) (next gs)))
                   (cons acc (step gs)))))))]
    (step (partition-by (fn [r] (nth r 3)) records))))

(defn- export-record-seq
  "The record stream an export writes, before it is sorted or chunked.

   Shared by `export-db` and `export-to-sink` deliberately: they differ only in
   WHERE the records go, and letting each build its own stream is how the two
   would come to disagree about `:xform` or about which builder `:sort?` selects.

   `:sort? true` uses `export-records` (unsorted; the external sort imposes the
   dump's order); `:sort? false` uses `export-records-streaming`, which is
   already in a load-safe order and needs no scratch space.

   `:xform` is applied HERE and ONLY here — before the sort and before the
   digest, so the manifest describes what the dump actually holds and counts,
   semantic digest and `:stats` all fall out of the transformed stream rather
   than needing adjustment afterwards.

   ONE instance for the whole export, so a stateful transducer sees one stream.
   It must be PURE: `:sort? false` makes two passes over `:eavt`.

   That \"one instance\" is why the application lives in one place. `export-db`
   also applied it at its call site after this helper was extracted, so every
   dump was transformed TWICE: a source of 1,2 with an incrementing `:xform`
   exported as 3,4, and the manifest, digest and `verify` all agreed with the
   wrong values because each is derived from the doubly-transformed stream. No
   test caught it because every export-`:xform` test used an idempotent
   transducer — a `take` or a `filter`.

   The motivating case is splitting one database into per-tenant dumps. Two
   things such a filter must get right, neither enforced because both are
   legitimate choices: KEEP THE SCHEMA DATOMS (a dump of data with no schema
   imports into a database that declares nothing), and expect refs OUT of the
   retained set to dangle — `import-db`'s `:check-refs?` reports those."
  [db opts]
  (cond->> (if (:sort? opts)
             (export-records db opts)
             (export-records-streaming db opts))
    (:xform opts) (sequence (:xform opts))))

(defn- sorted-record-seq
  "The record stream an export writes, sorted if `:sort?` — CREATED AT THE POINT
   OF USE, never bound to a name.

   That is not style. core.async only decomposes subforms that CONTAIN a park,
   but once a park anywhere in the enclosing form forces decomposition, EVERY
   `let`/`loop` binding in that region becomes a local inside the state machine's
   `try`, where Clojure's locals-clearing does not apply — and one read from a
   second block goes into the state array, which is never nulled. So a named
   record stream is retained for the whole go block: the entire database.

   Measured, 400k records, used heap sampled inside the block (baseline 15 MB):

       bind -> park -> consume                     63 MB
       bind -> consume SYNCHRONOUSLY -> park       63 MB   <- no park in between
       bind -> if(parking arm NOT taken) -> use    63 MB   <- untaken arm still leaks
       inlined into the arm                        15 MB
       loop parking every iteration, seq unnamed   14 MB

   Two rules follow, and both are easy to undo by accident:

     * REORDERING DOES NOT HELP. Moving the park earlier leaves the binding in
       the same decomposed region. Only having no name works.
     * A CALLEE MAY REFERENCE THE SEQ PARAMETER ONCE. `mstore/write-chunks!` and
       `write-chunked!` each mention it exactly once, in a loop init that then
       advances — safe. A second mention (a `(when (seq records) …)` guard, say)
       reintroduces the leak with no other symptom.

   `async+sync` compiles the same source to a plain `let` under `:sync? true`, so
   none of this shows on the JVM default; `default-sync?` is FALSE on
   ClojureScript, so async is the only mode there.

   ## `:xform` is applied here, and therefore once PER CALL

   Because this is called at the point of use, \"one instance for the whole
   export\" is no longer structural — it holds because only one arm of a
   `store-target?` fork runs. Do not call this twice on one export. That exact
   defect already shipped once (`:xform` applied twice, dumps transformed twice,
   invisible because every test used an idempotent transducer), which is why
   `migrate-export-xform-test` drives a STATEFUL one through every path."
  [db opts tmp-dir]
  (let [rs (export-record-seq db opts)]
    (if (:sort? opts)
      (msort/external-sort rs (:sort-buffer opts) tmp-dir)
      rs)))

(declare export-db*)

(defn export-db
  "Export a database SNAPSHOT to `target`. Pass `@conn`, not `conn` — a
   connection is refused by name rather than derefed, so which snapshot is being
   read stays visible at the call site. `d/history`, `d/as-of` and `d/since`
   views are snapshots too and are accepted.

   Writes a DIRECTORY: `manifest.edn`, `datoms-NNNNNN.cbor`, and `store-refs/`
   when the database has `:db.type/store-ref` blobs. Opts:
     :history?     false        include full history (asserts+retracts+tx entities)
     :compression  :gzip         :gzip or :none. gzip is ~7x on a real dump and
                                needs no dependency on any runtime; see
                                `datahike.migrate.compress` for why not zstd.
     :chunk-size   100000       datoms per chunk file, and therefore the import's
                                per-chunk memory (~2.7 MB of records). Same for
                                both media — see `manifest/default-chunk-size`
                                for why they were unified.
     :sort-buffer  1000000      datoms per in-memory external-sort run. The sort
                                decorates each resident record with its sort key
                                (`migrate.sort/export-order`), so a run costs the
                                records PLUS the keys — measured at 147 bytes per
                                record for string values, 194 for longs and 241
                                for `:db.type/bytes`, where the key carries the
                                value's hash as a string. At this default that is
                                ~150-240 MB on top of the window itself; lower it
                                if the export runs in a small heap. The keys are
                                freed as soon as the run is sorted, so this is the
                                sort phase's live set, not the export's.
     :sort?        true         false ⇒ no-scratch streaming order (see below)
     :progress-fn  nil          (fn [{:keys [phase datoms]}]). `:datoms` means one
                                of two things, and `:phase` says which: a
                                PER-UNIT phase (`:chunk`, `:sink-chunk`,
                                `:batch`) reports the datoms in that unit, so
                                they sum to the total; a MILESTONE phase
                                (`:normalise`, `:build-indexes`,
                                `:sink-complete`, `:done`) reports the running
                                total so far. Summing the milestones would
                                double-count.
     :xform        nil          a TRANSDUCER over `[e a v t op]` records, the
                                mirror of `import-db`'s. Applied BEFORE the sort
                                and before the digest, so the manifest describes
                                what the dump actually holds — count, semantic
                                digest and `:stats` all fall out of the
                                transformed stream rather than being adjusted
                                afterwards.

                                The motivating case is splitting one database
                                into per-tenant dumps. Two things such a filter
                                must get right, neither enforced because both are
                                legitimate choices: KEEP THE SCHEMA DATOMS (a
                                dump of data with no schema imports into a
                                database that declares nothing), and expect refs
                                OUT of the retained set to dangle —
                                `import-db`'s `:check-refs?` reports those.

                                One instance for the whole export, so a stateful
                                transducer sees one stream. It must be PURE:
                                `:sort? false` makes two passes over `:eavt`.
   `target` may be a filesystem path/dir OR a konserve store (S3 / S3-compatible /
   JDBC / mem). Returns the manifest map.

   By default ordering uses an external merge sort, so peak memory is bounded by
   :sort-buffer, independent of database size — but it spills sorted runs to local
   temp files. With `:sort? false` the export uses **no scratch at all** (for hard
   read-only / diskless targets): it streams schema/tx-entity datoms then data in
   `:eavt` order straight to the target. This relaxes the global tx ordering — it is
   safe for the common case (load-entities remaps ids and allocates forward refs),
   but does not preserve a *same-transaction* card-one replacement; keep the default
   sort if your history has those.

   The manifest is written LAST as the commit marker.

   NOTE: with :history? true this dump contains every value ever asserted,
   including retracted (\"deleted\") data — treat it as sensitive."
  ([db target] (export-db* db target {} "export-db"))
  ([db target opts]
   ;; `:xform` is NOT an option here. See `export-transformed`: the transform is
   ;; positional there precisely so that forgetting it cannot yield a complete
   ;; dump that every integrity signal then certifies as correct.
   (when (contains? opts :xform)
     (throw (ex-info (str "export-db does not take :xform — it produces a FAITHFUL dump. "
                          "Use `export-transformed`, which takes the transducer as a "
                          "required positional argument, so omitting it is an error "
                          "rather than a silent full export.")
                     {:error :migrate/xform-not-an-option :fn "export-db"})))
   (export-db* db target opts "export-db")))

(defn export-transformed
  "Export a database SNAPSHOT that is deliberately NOT a faithful copy —
   filtered to a subset, or with values redacted — through `xform`, a transducer
   over `[e a v t op]` records.

   Same as `export-db` in every other respect, and it sets `:transformed? true`
   in the manifest, which is the manifest's existing word for \"a smaller dump is
   expected here\".

   ## Why the transducer is positional, and required

   `export-db` and this function are the same machinery doing two jobs whose
   SAFE DEFAULTS POINT IN OPPOSITE DIRECTIONS:

     * a backup wants everything. More data than expected is fine; missing data
       is the failure.
     * a subset or a redaction wants less. More data than expected is a
       DISCLOSURE; that is the failure.

   As an option, the transform was omissible, and omitting it means \"no
   transform\" — so for the second job the failure mode of forgetting it was
   identical to the default behaviour. `{:xfrom …}` produced a successful
   export, a manifest saying `:transformed? false`, and `verify` saying
   `:ok? true`, over a dump holding every tenant. Every signal agreed, because
   the dump WAS a correct full backup. It just was not what was asked for.

   A positional argument cannot be misspelled, cannot be dropped by a map merge,
   and omitting it is an arity error rather than a full dump. That is the whole
   of the reason for a second entry point.

   It does NOT make a wrong transducer safe — one that keeps more than intended
   still keeps more than intended. It removes only the \"forgot it entirely\"
   path, which is the one that fails silently.

   ## Which transforms belong here, and which belong on import

   Here: anything whose omission would let data OUT that should not — filtering
   to a tenant, redacting a value in place, replacing it with a tombstone.
   The export side is the disclosure boundary; doing it on import is too late,
   because the dump already exists with the data in it.

   On `import-db`'s `:xform` instead: reshaping that should happen when data
   LANDS — renaming an attribute for a schema change, rewriting a value
   representation, splitting one record into several. Omitting those yields
   visibly un-migrated data rather than silent over-disclosure, which is why
   they do not need this treatment; and keeping them on import means the dump
   stays a faithful artifact you can re-run the transform against without
   re-exporting, with the original still intact if the transform was wrong.

   Nothing prevents a rewrite here if you want one — this takes any transducer.
   The doctrine is about where each kind is best placed, not a restriction."
  ([db target xform] (export-transformed db target xform {}))
  ([db target xform opts]
   (when-not (ifn? xform)
     (throw (ex-info (str "export-transformed needs a transducer as its third argument; got "
                          (pr-str (type xform)) ". For a faithful dump use `export-db`.")
                     {:error :migrate/xform-required :fn "export-transformed"})))
   (when (contains? opts :xform)
     (throw (ex-info "export-transformed takes the transducer positionally; drop :xform from opts."
                     {:error :migrate/xform-not-an-option :fn "export-transformed"})))
   (export-db* db target (assoc opts :xform xform) "export-transformed")))

(defn- export-db*
  "The shared implementation of `export-db` and `export-transformed`. `who` is
   the public name to blame in errors."
  ([db target opts who]
   (assert-sync-supported! opts)
   (assert-sizes-positive! opts)
   (msch/validate-opts! msch/ExportOpts opts who)
   (let [db       (mman/ensure-db db who)
         opts     (merge {:history? (boolean (:keep-history? (dbi/-config db)))
                          :chunk-size default-chunk-size
                          :sort-buffer 1000000
                          :compression mz/default-codec
                          :sync? default-sync?
                          :sort? true}
                         opts)
         progress (or (:progress-fn opts) (constantly nil))
         _        (assert-codec-supported! opts)]
     (async+sync
      (:sync? opts) *default-sync-translation*
      (go-try-
       (let [;; Only walk for blobs when the schema can actually have them. Two
             ;; reasons, and the second is not an optimisation:
             ;;   * `reachable-store-refs` is a full reachability mark — pointless
             ;;     when no attribute is a `:db.type/store-ref`;
             ;;   * it walks index ADDRESSES, so it requires a flushed index and
             ;;     raises "Index needs to be properly flushed before marking" on
             ;;     an unflushed in-memory db. Plenty of legitimate exports are of
             ;;     such a db (`db-with`, a `:memory` store mid-test), and those
             ;;     cannot hold in-store blobs anyway.
             blob-plan (when (mblobs/schema-has-store-refs? db)
                         (<?- (mblobs/plan db (:store db) opts)))
             opts (cond-> opts
                    (seq (:carried blob-plan)) (assoc mman/blob-plan-key blob-plan)
                    (seq (:external blob-plan)) (assoc mman/blob-plan-key blob-plan))]
         ;; Blob carriage. Planned BEFORE anything is written so the manifest can
         ;; declare it, and the bytes are written before the manifest — which is
         ;; the commit marker — so a dump that has a manifest has its blobs. Same
         ;; ordering the konserve-sync walker needs when it ships blobs ahead of
         ;; the branch head: nothing may name an object that is not there yet.
         (when-let [plan (get opts mman/blob-plan-key)]
           (<?- (with-blob-writer target opts #(mblobs/copy-out! (:store db) plan % opts))))
         ;; The records to write. `:sort? true` spills sorted runs to local temp
         ;; files and k-way merges them; `:sort? false` needs no scratch at all
         ;; but cannot order a same-transaction card-one replacement, because it
         ;; makes two passes over `:eavt` instead of sorting.
         ;;
         ;; Both are portable now. The sort was JVM-only while it spoke
         ;; `java.io.File` and `java.util.PriorityQueue`; the seq-over-open-files
         ;; shape it was ALSO blamed for was never the obstacle, since every read
         ;; in the merge is a synchronous local file read and no channel op ever
         ;; occurs inside it.
         (let [tmp-dir (when (:sort? opts) (fs/temp-dir! "dh-export"))]
           (try
             ;; NOT a `write-to!` closure any more. It held the konserve write,
             ;; and a closure is exactly what the `go` state machine cannot enter
             ;; — the same rule that reshaped the importer. Inlined so the awaits
             ;; sit at statement positions.
             ;; No `sorted` binding, and no `records` binding above: BOTH arms
             ;; build their own. The store arm's park decomposes this `if`, which
             ;; promotes a shared binding for the FILESYSTEM arm too — measured,
             ;; a seq consumed in the non-parking arm is retained just the same.
             ;; Fixing only the store arm would have left write-chunked! holding
             ;; the whole dump, with nothing in the suite to notice.
             (do
               (if (mstore/store-target? target)
                 (let [m (<?- (mstore/open target opts))]
                   (try
                     (<?- (mstore/write-chunks! m (sorted-record-seq db opts tmp-dir)
                                                (:chunk-size opts)
                                                (fn [digest chunks]
                                                  (build-manifest db (with-source-count db opts) digest chunks))
                                                progress opts
                                                (get opts :compression mz/default-codec)))
                     (finally (<?- (mstore/close m opts)))))
                 (write-chunked! db opts target (sorted-record-seq db opts tmp-dir)
                                 (:chunk-size opts) progress)))
             (finally
               (when tmp-dir
                 (doseq [n (or (fs/list-names tmp-dir) [])]
                   (fs/delete! (fs/join tmp-dir n)))
                 (fs/delete! tmp-dir)))))))))))

(defn export-to-sink
  "Stream a database's records into a caller-supplied SINK. **Experimental.**

   The mirror of `import-source`. Where that lets a caller supply records from
   anywhere, this lets a caller receive them anywhere — another database, a
   custom file format, a socket. `export-db` remains the way to produce a
   datahike DUMP; this is for everything that is not one.

   `sink` is

     {:open  (fn [opts]          -> ctx)
      :write (fn [ctx records]   -> ctx)
      :close (fn [ctx]           -> result)}

   `export-to-sink` returns whatever `:close` returns.

   ## What a sink gets, and what it does NOT get

   It gets the same `[e a v t op]` records `export-db` writes, in the same
   `(t, txInstant-first, e, a)` order, transformed by the same `:xform`.

   It does NOT get what makes a dump a dump: no manifest, no per-chunk SHA-256,
   no semantic digest, nothing `verify` can later read back. Those are properties
   of BYTES AT REST, and a sink writing into a live database has none of them to
   offer. Do not read a successful `export-to-sink` as a verified backup — it is
   a transfer, and the target defines what fidelity means. `export-db` is the
   verifiable artefact.

   ## Chunks are transaction-aligned, and `:sort? false` is REFUSED

   A chunk holds at least `:chunk-size` records and then grows to the next change
   of `t`, so a transaction is never split across two `:write` calls. That costs
   a little chunk-size overshoot and buys the property every live-target sink
   needs: a sink that transacts can transact what it is handed. `export-db`'s own
   chunking does not do this — a dump chunk is a byte-range and its reader
   reassembles the stream — so this is a property of THIS seam, not of the dump.

   `:sort? false` cannot provide it and is refused rather than silently breaking
   it. That mode uses `export-records-streaming`, which walks `:eavt`: `t`
   interleaves arbitrarily, so `partition-by` yields runs of about one record and
   a transaction lands in many chunks. Measured on a database where entity order
   and transaction order disagree, `:chunk-size 40`: `:sort? true` split nothing
   across 9 chunks, while `:sort? false` put TWO transactions in all nine. It is
   not the two-pass meta/data split alone — merging those would not fix it.

   The cost is real and worth naming: `:sort? false` exists for hard read-only
   and diskless targets, and this is the only diskless route to a non-dump
   target. A sink that does not transact per chunk — a socket, a columnar
   writer — does not need the alignment and is nonetheless refused. If that
   becomes a real need, the shape is a sink declaring `:tx-aligned? false`
   rather than dropping the guarantee for everyone.

   ## Async

   `:open`, `:write` and `:close` may all do IO and are each awaited, so under
   `:sync? false` each must return a channel. `default-sync?` is FALSE on
   ClojureScript, so that is the default there.

   `:close` is called on the FAILURE path too, and deliberately not from a
   `finally`: in async mode this function returns a channel, and a `finally`
   would fire the moment it is handed back — before a single record had been
   written. `import-db` carries the same scar, where that mistake read every
   chunk against a released store and still reported `:verified? true`.

   The three run in ONE go block, so under `:sync? false` a sink written to
   return plain values will not work and one written to return channels will not
   work under `:sync? true`. To write a sink that serves both, wrap each body the
   way this namespace does — `(async+sync (:sync? opts) *default-sync-translation*
   (go-try- …))` — and note `:open` and `:write` are handed the OPTS, so `:sync?`
   is available to them.

   `:close` is called EXACTLY ONCE, on the success and the failure path alike,
   and is handed the LATEST context — the one the last successful `:write`
   returned, not the one `:open` produced. A sink that rotates a resource through
   its context can therefore close the right one. If `:close` itself throws while
   an earlier failure is already in flight, the EARLIER failure is what surfaces;
   a broken close must not hide the reason it was reached.

   ## `:db.type/store-ref` blobs

   Warned about, not refused. The records carry blob REFERENCES and their bytes
   are not written here, because a record sink has nowhere to put them. That is
   a limitation the caller may well have already handled — copying the objects
   separately (`datahike.gc/reachable-store-refs` gives the live set), or
   targeting something where the reference is the point. `export-db` is the path
   that carries the bytes for you.

   Opts are `export-db`'s: `:history?` `:xform` `:chunk-size` `:sort-buffer`
   `:sync?` `:progress-fn`. `:compression` is ignored — nothing here writes
   bytes — and `:sort? false` is refused, see above."
  ([db sink] (export-to-sink db sink {}))
  ([db sink opts]
   (assert-sync-supported! opts)
   (assert-sizes-positive! opts)
   (when-not (and (ifn? (:open sink)) (ifn? (:write sink)) (ifn? (:close sink)))
     (throw (ex-info "A sink must supply :open, :write and :close functions."
                     {:error :export/malformed-sink :got (keys sink)})))
   ;; Before anything is read. `:sort? false` cannot keep a transaction in one
   ;; chunk — see the docstring for the measurement — and this seam's whole
   ;; contract is that a sink can transact what it is handed.
   (when (false? (:sort? opts))
     (throw (ex-info (str "export-to-sink cannot honour {:sort? false}: without the sort, "
                          "records arrive in :eavt order and a transaction is split across "
                          "many chunks, which breaks the transaction alignment a sink relies "
                          "on. Use export-db for a no-scratch export.")
                     {:error :export/sort-required})))
   (msch/validate-opts! msch/SinkOpts opts "export-to-sink")
   (let [db       (mman/ensure-db db "export-to-sink")
         opts     (merge {:history? (boolean (:keep-history? (dbi/-config db)))
                          :chunk-size default-chunk-size
                          :sort-buffer 1000000
                          :sync? default-sync?
                          :sort? true}
                         opts)
         progress (or (:progress-fn opts) (constantly nil))]
     (when (mblobs/schema-has-store-refs? db)
       (warn! (str "[datahike.migrate] this database has :db.type/store-ref attributes. The "
                   "records carry blob REFERENCES; their BYTES are not written here, because a "
                   "record sink has nowhere for them to go. Copy them yourself (see "
                   "datahike.gc/reachable-store-refs) or use export-db, which carries them "
                   "alongside the dump.")))
     (async+sync
      (:sync? opts) *default-sync-translation*
      (go-try-
       ;; `tmp-dir` stays out here so the `finally` below can clean it. It is a
       ;; path, not a stream, so naming it costs nothing.
       (let [tmp-dir (when (:sort? opts) (fs/temp-dir! "dh-export-sink"))]
         (try
           (let [ctx0   (<?- ((:open sink) opts))
                 ;; The LATEST ctx, not `ctx0`. `:write` returns a new context,
                 ;; so a sink that rotates a resource through it — a file handle,
                 ;; an open transaction — would otherwise be handed a stale one to
                 ;; close. A `loop` binding is invisible to the code that runs
                 ;; after it, hence the volatile.
                 latest (volatile! ctx0)
                 ;; A loop over tx-aligned chunks rather than a reduce: the state
                 ;; machine cannot enter a reducing closure, the same constraint
                 ;; that shaped the importer. The error comes back as a VALUE
                 ;; rather than being handled in the `catch`, because nothing may
                 ;; park there — the shape `import-db` uses for the same reason.
                 err (try
                       ;; The stream is built HERE, in the loop init, and never
                       ;; bound to a name — see `sorted-record-seq`. Creating it
                       ;; "after the park" would not be enough: the binding would
                       ;; still live in the region this go block decomposes.
                       (loop [cs  (seq (tx-aligned-chunks
                                        (sorted-record-seq db opts tmp-dir)
                                        (:chunk-size opts)))
                              ctx ctx0
                              n   0]
                         (if (nil? cs)
                           (do (progress {:phase :sink-complete :datoms n}) nil)
                           (let [chunk (first cs)
                                 ctx'  (<?- ((:write sink) ctx chunk))]
                             (vreset! latest ctx')
                             ;; THIS chunk's count, not the running total. Every
                             ;; other per-unit event (`:chunk`, `:batch`) reports
                             ;; the unit, and a caller cannot tell which it is
                             ;; getting from a bare number — one that summed
                             ;; these would have counted a 3-chunk dump six
                             ;; times. The running total is still available: it
                             ;; is what `:sink-complete` carries.
                             (progress {:phase :sink-chunk :datoms (count chunk)})
                             (recur (next cs) ctx' (+ n (count chunk))))))
                       (catch #?(:clj Exception :cljs :default) e e))
                 ;; ONCE, on both paths. Closing inside the loop's success branch
                 ;; AND in a catch would double-close whenever `:close` itself
                 ;; threw: the catch would see its own exception and run again.
                 closed (try (<?- ((:close sink) @latest))
                             (catch #?(:clj Exception :cljs :default) e {::close-failed e}))]
             (cond
               ;; A failing close must not MASK the failure that caused it.
               (some? err) (throw err)
               (and (map? closed) (contains? closed ::close-failed))
               (throw (::close-failed closed))
               :else closed))
           (finally
             (when tmp-dir
               (doseq [n (or (fs/list-names tmp-dir) [])]
                 (fs/delete! (fs/join tmp-dir n)))
               (fs/delete! tmp-dir))))))))))

;; ---------------------------------------------------------------------------
;; reading dumps

(defn- chunk-bytes
  "The UNCOMPRESSED records of a chunk file.

   Bounded by one chunk, which `:chunk-size` already bounds. Streaming inside a
   chunk was possible while chunks were stored verbatim; a compressed chunk is
   read whole because neither runtime offers a streaming synchronous gunzip, and
   the bound is the same either way."
  [p codec]
  (if (= :none codec)
    (fs/read-bytes p)
    (mz/decompress-bytes codec (fs/read-bytes p) {:file (fs/file-name p)})))

(defn- sha256-of-chunk
  "Hex SHA-256 of a chunk's RECORDS — not of the file.

   The manifest's `:sha256` is over uncompressed bytes so that the codec stays a
   transport detail (see `migrate.compress`), which means verification has to
   decompress before hashing. An uncompressed chunk streams, so a dump written
   with `:compression :none` still verifies in memory bounded by one block."
  [p codec]
  (if (= :none codec)
    (let [{:keys [pull close]} (fs/puller p)]
      (try
        (loop [acc (dig/sha256-accumulator)]
          (if-let [b (pull)]
            (recur (dig/sha256-update! acc b))
            (dig/sha256-finalize acc)))
        (finally (close))))
    (dig/sha256-hex (chunk-bytes p codec))))

(defn- validate-chunk-file [dir fname]
  (when-not (re-matches chunk-re fname)
    (throw (ex-info (str "Illegal chunk file name in manifest: " fname)
                    {:error :import/bad-chunk-path :file fname})))
  (let [f (fs/join dir fname)
        canon (fs/canonical f)
        base  (fs/canonical dir)]
    (when-not (= base (fs/parent canon))
      (throw (ex-info (str "Chunk path escapes dump directory: " fname)
                      {:error :import/bad-chunk-path :file fname})))
    f))

(defn- manifest-of
  "Return {:manifest m :legacy? bool :files [path...]} WITHOUT hashing —
   reads the manifest and validates chunk paths only. Cheap enough for estimation."
  [source]
  (let [f (str source)]
    (cond
      (fs/directory? f)
      ;; A directory that is not a dump used to surface as whatever the read
      ;; happened to throw — `FileNotFoundException` on manifest.edn for an
      ;; empty or unrelated directory, `EOF while reading` from the EDN reader
      ;; for a truncated one. Neither says what was actually wrong, and `verify`
      ;; is the call an operator makes to ask exactly that.
      (let [mf (fs/join f "manifest.edn")
            manifest (try
                       (read-manifest-map (fs/slurp-text mf))
                       (catch #?(:clj Exception :cljs :default) e
                         (throw (ex-info (str "Not a datahike dump: " f
                                              ". A dump directory contains manifest.edn and "
                                              "datoms-NNNNNN.cbor; this one has no readable "
                                              "manifest.edn.")
                                         {:error :import/not-a-dump :source f}
                                         e))))
            ;; The SAME check the store medium gets from `assert-dump-manifest!`.
            ;; Present and parseable is not enough — any EDN map got this far and
            ;; was then treated as a dump with no chunks, which `verify` reported
            ;; as intact. It also refuses a dump from a NEWER datahike, which
            ;; this arm did not: `assert-dump-manifest!` is not on the filesystem
            ;; route at all, so a version-2 dump verified `:ok? true` here while
            ;; the store medium refused it.
            _ (mman/assert-format-version! manifest f)
            files    (mapv #(validate-chunk-file f (:file %)) (:chunks manifest))]
        {:manifest manifest :legacy? false :files files})

      (not (fs/exists? f))
      (throw (ex-info (str "No dump at " f ". A dump is a DIRECTORY containing "
                           "manifest.edn and datoms-NNNNNN.cbor.")
                      {:error :import/no-such-dump :source (str f)}))

      :else
      ;; An existing non-directory: the legacy single-file format released
      ;; datahike wrote. A path that does not exist at all is caught above, so
      ;; a typo no longer reaches the legacy reader and surfaces as a bare
      ;; FileNotFoundException — the one error in this namespace that had no
      ;; `:error` key.
      {:manifest {manifest-key 0 :serialization :cbor :legacy? true}
       :legacy? true :files []})))

(defn- open-dump
  "Like `manifest-of`, but for a chunked directory dump additionally validates chunk
   paths and verifies per-chunk SHA-256 by STREAMING each file (bounded memory)
   before any import touches the database.

   `:checksums` is `:require` (default) or `:skip`.

   `:require` FAILS CLOSED, and the distinction matters more than it looks. The
   loop used to carry `:when sha256`, which skips a chunk whose manifest entry
   has no hash — so deleting one key from manifest.edn turned integrity checking
   off entirely and the dump still verified clean with corrupt bytes in it.
   Nothing this exporter writes omits a hash: `chunk-descriptor` takes one as a
   required argument and both writers always compute it. An entry without one is
   an edited or foreign manifest.

   `:skip` exists because importing a modified or hand-built dump is a real
   thing to want — filtering records, or a dump produced by another tool. It is
   spelled as a deliberate value rather than a boolean flag so it cannot arrive
   by a stray `true`, and `import-db` logs a warning every time it is used."
  ([source] (open-dump source {}))
  ([source {:keys [checksums] :or {checksums :require}}]
   (let [dump   (manifest-of source)
         chunks (:chunks (:manifest dump))
         check? (and (not (:legacy? dump)) (not= :skip checksums))]
     (when check?
        ;; FAIL CLOSED. This used to be `:when sha256`, which SKIPS a chunk whose
        ;; manifest entry has no hash — so deleting one key from manifest.edn
        ;; turned integrity checking off and `verify` still reported the dump
        ;; intact, corrupt bytes and all. Nothing datahike writes omits a hash
        ;; (`chunk-descriptor` takes it as a required argument and both writers
        ;; always compute one), so an entry without one is an edited or foreign
        ;; manifest, which is exactly the case worth refusing.
       (doseq [{:keys [file sha256]} chunks]
         (when-not sha256
           (throw (ex-info (str "Chunk " file " has no :sha256 in the manifest. "
                                "Every chunk datahike writes carries one; a manifest "
                                "missing them has been edited or was not written by "
                                "this exporter, and its contents cannot be trusted.")
                           {:error :import/missing-checksum :file file})))
         (let [cf (validate-chunk-file (str source) file)]
            ;; A chunk the manifest names and the directory does not hold is the
            ;; most ordinary corruption there is — a file lost in transfer. The
            ;; store medium has said `:import/missing-chunk` for it all along
            ;; (`store/read-chunk`), and every catch set here already lists that
            ;; error; only the throw site was missing, so on a filesystem dump it
            ;; surfaced as a raw `FileNotFoundException` from `io/input-stream`
            ;; two frames down. `verify` turned that into an exception escaping
            ;; the call an operator makes to ASK whether a backup is intact,
            ;; while the identical damage on a store came back as a finding.
            ;;
            ;; Checked HERE rather than in `validate-chunk-file`, which is also
            ;; called by `manifest-of` — including from `verify`'s own catch, to
            ;; recover a manifest for the finding. Throwing there would throw out
            ;; of the handler that exists to report this.
           (when-not (fs/exists? cf)
             (throw (ex-info (str "Missing chunk file: " file
                                  ". The manifest names it, but the dump directory "
                                  "does not hold it.")
                             {:error :import/missing-chunk :file file})))
           (when (not= sha256 (sha256-of-chunk cf (codec-of (:manifest dump))))
             (throw (ex-info (str "Checksum mismatch for chunk " file)
                             {:error :import/checksum-failed :file file})))))
        ;; A manifest that lists FEWER chunks than the directory holds is also
        ;; not a dump this exporter wrote: `:chunks []` with datoms-*.cbor files
        ;; present verified clean, because the loop above had nothing to iterate.
        ;;
        ;; NOT mirrored on the store medium, and that asymmetry is deliberate
        ;; rather than the usual filesystem-first oversight. `konserve.core/keys`
        ;; yields every TOP-LEVEL key in the store, not the keys under a prefix,
        ;; so the store equivalent would enumerate a store that commonly also
        ;; holds a live database — every index node — and on S3 that is a full
        ;; bucket listing on a call an operator may run often. The motivating
        ;; case is covered there anyway, though NOT the way this note first
        ;; claimed: it said `:chunks []` leaves `:checksums :none` and `:ok?`
        ;; refuses it — and then the fix that stopped calling a legitimately
        ;; empty dump broken removed that refusal, so the note outlived its
        ;; reason for a commit. It is `:stats :datom-count` against the digest's
        ;; `:count` that catches a manifest truncated to zero chunks; see
        ;; `stats-agree?` in `verify`. What remains uncovered is a manifest that
        ;; declares a PROPER SUBSET of the chunks present and has had its stats,
        ;; counts AND digest adjusted to match — internally consistent, and so
        ;; indistinguishable from a legitimately smaller dump. That is a
        ;; completeness question, answered at export.
       (let [declared (set (map :file chunks))
             extra    (->> (fs/list-names (str source))
                           (filter #(re-matches chunk-re %))
                           (remove declared)
                           sort vec)]
         (when (seq extra)
           (throw (ex-info (str "Dump directory holds chunk files the manifest does not "
                                "list: " (pr-str extra)
                                ". The manifest is the authority on what a dump contains, "
                                "so this one is truncated or edited.")
                           {:error :import/undeclared-chunks
                            :undeclared extra})))))
     (assoc dump :chunks-verified (if check? (count chunks) 0)))))

;; ---------------------------------------------------------------------------
;; memory estimation — tell the user how much heap to give an import

(defn- manifest-total-bytes
  "Total dump bytes from the manifest's chunk `:bytes` (present in v1 dumps); falls
   back to on-disk file sizes for a filesystem dump that predates `:bytes`."
  [manifest files]
  (let [from-manifest (reduce + 0 (keep :bytes (:chunks manifest)))]
    (if (pos? from-manifest)
      from-manifest
      (reduce + 0 (map fs/file-size (or files []))))))

(defn estimate-import-memory
  "Estimate the JVM heap an `import-db` of `source` needs, so you can size `-Xmx`
   before running it. Reads only the dump's manifest (no scan, no hashing).
   `source` may be a filesystem path/dir OR a konserve store target.

   JVM-only, for two reasons: it opens the medium synchronously (so it is
   refused by name on ClojureScript, like `verify`), and `:sufficient?` /
   `:current-max-heap` have no meaning where there is no `Runtime.maxMemory` —
   they are absent from the result there rather than nil.

   Three terms. The O(entities) id-remap map the import holds for its whole
   duration dominates ABOVE about a million entities; below that the
   `:chunk-size` worth of records held while a chunk is decoded is the largest,
   which is why it is counted rather than assumed negligible. The third is one
   `:batch-size` of records plus the tx-report churn it drives. Returns e.g.
     {:datoms .. :entities .. :id-map-bytes .. :batch-bytes .. :chunk-bytes ..
      :required-heap-bytes .. :required-heap \"180 MB\"
      :recommended-heap-bytes .. :recommended-heap \"512 MB\"
      :current-max-heap-bytes .. :current-max-heap \"512 MB\" :sufficient? bool}"
  ([source] (estimate-import-memory source {}))
  ([source opts]
   (assert-jvm-only! "estimate-import-memory" opts)
   ;; Every sibling entry point validates its sizes; this one did not, so
   ;; `{:batch-size 0}` returned a confident under-estimate — and the whole
   ;; point of this function is that an operator sizes `-Xmx` from what it says.
   (msch/validate-opts! msch/EstimateOpts opts "estimate-import-memory")
   (let [batch-size (get opts :batch-size default-batch-size)]
     (if (mstore/store-target? source)
       (let [m (mstore/open source)]
         (try
           (let [manifest (mstore/read-manifest m)]
             (estimate-from-manifest manifest (manifest-total-bytes manifest nil) batch-size))
           (finally (mstore/close m))))
       (let [{:keys [manifest files]} (manifest-of source)]
         (estimate-from-manifest manifest (manifest-total-bytes manifest files) batch-size))))))

(defn- fs-read-chunk
  "The records of ONE chunk file. The filesystem counterpart to
   `migrate.store/read-chunk` — same unit, so the importer's loop does not care
   which medium it is reading."
  [manifest file opts]
  (async+sync
   (:sync? opts) *default-sync-translation*
   (go-try-
    (mcbor/decode-records-from (chunk-bytes file (codec-of manifest))))))

(defn- reduce-dump-records
  "Reduce `rf` over every record of the dump, one CHUNK at a time.

   Memory is bounded by `:chunk-size`, which is the knob for it — see
   `estimate-import-memory`, which counts this term.

   An uncompressed chunk used to stream record by record through a pull-backed
   lazy seq, bounded by one 64 KiB block instead of one chunk. That is gone, for
   three reasons that compound:

   * It only ever applied to `:compression :none`. A compressed chunk cannot
     stream without a streaming inflater, and Node has none synchronously — so
     since gzip became the default, the streaming path was the exception.
   * A lazy seq that performs IO cannot be made async. `async+sync` compiles ONE
     source into both branches, and the async branch is a `go` block whose state
     machine does not reach inside a `lazy-seq` body — the same constraint that
     turned `migrate.store`'s `reduce` into a `loop`.
   * With the chunk default now sized for it, the saving is ~2.6 MB against
     terms measured in hundreds. It was buying a second code path.

   `rf` must be pure: it is applied inside the reduction, so IO in it would be
   invisible to the go block. This is `verify`'s path; the importer uses
   `chunk-src` directly, because its per-record work writes datoms."
  [{:keys [files manifest]} rf init]
  (let [codec (codec-of manifest)]
    (reduce (fn [acc file]
              (reduce rf acc (mcbor/decode-records-from (chunk-bytes file codec))))
            init
            files)))

;; ---------------------------------------------------------------------------
;; import

(defn- resolve-sysrefs
  "Replace any SysRef value in a record with the target's system-entity eid."
  [db record]
  (let [v (nth record 2)]
    (if (mcbor/sysref? v)
      (assoc record 2 (dbi/-ref-for db (:ident v)))
      record)))

(defn- system-eid-seed
  "Identity seed {eid eid} for every target system entity, so refs resolved to a
   system entity are not re-allocated by load-entities (#508)."
  [db]
  (into {} (map (fn [e] [e e])) (dbi/-system-entities db)))

(defn- user-datom-count
  "Count user-transaction datoms (tx > tx0), i.e. everything an export writes. With
   `history?` the full temporal set is enumerated, matching a :history? true dump."
  ([db] (user-datom-count db false))
  ([db history?]
   (let [src (if history? (api/history db) db)]
     (count (filter #(> (d/datom-tx %) c/tx0) (api/datoms src :eavt))))))

(defn- with-source-count
  "Add an INDEPENDENT witness of how much the source held.

   Every other integrity field — `:datom-count`, the semantic digest — is derived
   from what was WRITTEN, so a dump that lost records agrees with itself perfectly
   and `verify` passes it. Measured: a 205-datom database exported short to 120
   produced a manifest saying 120, and `verify` returned `:ok? true`.

   `db` is an immutable value, so counting it after the write is the same count as
   before. It costs one extra index scan; `{:count-source? false}` skips it and
   records nil — \"unknown\", which an importer must treat as unverifiable rather
   than as agreement."
  [db opts]
  (cond-> opts
    (not (false? (:count-source? opts)))
    (assoc :source-datom-count (user-datom-count db (boolean (:history? opts))))))

(defn- refuse-incomplete-dump!
  "Refuse a dump that holds fewer records than its source held, with no `:xform`
   recorded to explain it.

   Called by BOTH import media. It was written for one of them first, and the
   filesystem dump sailed straight through — the same shape as the `:sha256`
   fail-closed rule, which was written for the filesystem, documented, and not
   carried across to the store. That is what a `store-target?` fork costs each
   time (see the open item on collapsing them into one medium seam)."
  [manifest opts]
  (when-let [missing (and (not (:allow-partial? opts))
                          (mman/unexplained-shortfall manifest))]
    (throw (ex-info
            (str "Dump is incomplete: it holds "
                 (get-in manifest [:stats :datom-count])
                 " records but its source held "
                 (get-in manifest [:stats :source-datom-count])
                 " — " missing " unaccounted for, and no :xform was recorded to"
                 " explain the difference. This is what a partially-written export"
                 " looks like. Re-export, or pass `:allow-partial? true` to import"
                 " it anyway.")
            {:error :import/incomplete-dump
             :missing missing
             :stats (:stats manifest)}))))

(defn- has-user-datoms?
  "Does `db` hold any user-transaction datom? Stops at the FIRST one.

   `user-datom-count` walks the entire `:eavt` index and boxes a lazy seq of
   Datoms to answer this. That is free on an empty target — which is the only
   kind `import-db` used to accept — and becomes the dominant cost of a small
   import the moment `:merge? true` allows a populated one: a full, cold-cache
   index read before a single datom is written."
  [db]
  (boolean (some #(> (d/datom-tx %) c/tx0) (api/datoms db :eavt))))

(defn- load-batch!
  "One `load-entities` call, awaited. `async+sync`: yields the tx-report in sync
   mode, a channel of it otherwise.

   Its own function because the await must sit where the `go` block can SEE it —
   a helper that awaited on the caller's behalf would be a closure, and the go
   state machine does not reach inside one.

   `api/load-entities` returns a datahike promise, which on the JVM implements
   BOTH `IDeref` and core.async's `ReadPort` (`datahike.tools/throwable-promise`)
   and on ClojureScript is a `promise-chan`. So the same object can be `@`-ed or
   parked on; the only platform difference is that cljs has no blocking deref,
   and `:sync? true` is refused there at the public entry points."
  [conn batch migration opts]
  (async+sync
   (:sync? opts) *default-sync-translation*
   (go-try-
    ;; `load-entities-migrating` rather than `api/load-entities`, because the id
    ;; mapping travels WITH the call and `load-entities` is a published 2-arity
    ;; function — widening it in place would have changed a public contract for
    ;; the sake of an internal one. The tx-report carries the UPDATED mapping
    ;; back out; see `transact-entities-directly` for why it does not live on
    ;; the database value.
    (let [p (dwriter/load-entities-migrating conn batch migration)]
      ;; ClojureScript has no blocking deref, and `:sync? true` is refused there
      ;; by `assert-sync-supported!` at the entry points.
      #?(:clj (if (:sync? opts) @p (<?- p))
         :cljs (<?- p))))))

(defn- verification-result
  "Decide, ONCE, whether an import verified — for both import paths.

   It was decided twice, and the two disagreed on the case that matters. Given a
   source declaring no record count, the streaming path threw
   `:import/verify-failed` (\"datom count mismatch\", comparing `live` against
   `(- 0 dropped)` — a mismatch that says nothing about the data), while the
   index-build path had `(or (nil? (:expected-count source-meta)) …)` and
   returned **`:verified? true`**. Same input, one fails closed with a false
   description, the other certifies. Measured on both.

   `:verified?` alone could not express any of this. It is `true` / `false` /
   `nil`, and `nil` covered THREE unrelated situations — verification was
   switched off, nothing to compare against, or `:collect` swallowing the
   result — all of which read, in a report otherwise full of successes, as
   \"fine\". So the report now also carries `:verification`, which says which:

     {:status :ok | :failed | :skipped | :unavailable
      :expected N :actual N :missing K :reason kw}

   `:verified?` keeps its old shape and meaning for callers that test it as a
   boolean — deliberately NOT widened to a keyword, since `:not-checked` would
   be TRUTHY and every `(if (:verified? rep) …)` in the wild would start reading
   an unchecked import as a verified one.

   Throws unless `:on-error :collect`, whose contract is to report rather than
   abort."
  [{:keys [verify? source-count dropped live on-error]}]
  (let [collect? (= :collect on-error)]
    (cond
      (not verify?)
      {:verified? nil :verification {:status :skipped :actual live}}

      (nil? source-count)
      (let [v {:status :unavailable :reason :no-source-count :actual live}]
        ;; Fail CLOSED, like the streaming path did — but saying the true thing.
        ;; "The source declares no count" is not "the counts disagree", and an
        ;; operator sent to inspect a dump over the latter is being sent to
        ;; inspect an artefact that may be perfectly intact.
        (when-not collect?
          (throw (ex-info (str "Cannot verify this import: the source declares no record "
                               "count, so there is nothing to compare the "
                               live " restored datoms against. Pass {:verify? false} "
                               "to import without this check.")
                          {:error :import/verify-unavailable
                           :live-count live
                           :verification v})))
        {:verified? nil :verification v})

      :else
      (let [expected (- (long source-count) (long dropped))
            ok? (= expected (long live))
            v (cond-> {:status (if ok? :ok :failed)
                       :expected expected :actual live}
                (not ok?) (assoc :missing (- expected (long live))))]
        (when (and (not ok?) (not collect?))
          (throw (ex-info (str "Post-import verification failed: the source declares "
                               source-count " record(s), " dropped " were dropped by "
                               ":xform, so " expected " datom(s) were expected and "
                               live " are present. Either the source holds less than it "
                               "declares or the import did not apply everything it read.")
                          {:error :import/verify-failed
                           :dump-count source-count
                           :dropped-by-xform dropped
                           :expected-count expected
                           :live-count live
                           :verification v})))
        {:verified? ok? :verification v}))))

#?(:clj
   (defn- legacy-import-report
     "The report map for a LEGACY single-file import, from the facts
      `import-db-legacy` hands back.

      `import-db` returns one shape whatever it was given. It used to return the
      report map for a manifest-and-chunks dump and the final `TxReport` for a
      legacy one — two types from one function, while the docstring promised the
      map unconditionally.

      IT IS NOT VERIFIED, and says so rather than manufacturing a check.

      The obvious move is to compare records decoded against datoms present
      afterwards. It does not work, and how it fails is instructive: the two are
      not the same quantity. Measured on the 44-record golden fixture, a
      faithful import leaves 45 — the legacy path replays through `api/transact`,
      which stamps its OWN `:db/txInstant`, one per batch. So a correct restore
      does not satisfy `records = datoms`, and the expectation that would
      satisfy it (`records + batches`) is derived from the import's own
      behaviour: an integrity signal computed from the write path, which is the
      defect class this seam exists to avoid.

      Nor would the count be independent. A manifest records what the EXPORTER
      counted; decoding a legacy dump says only what is in the bytes just read,
      so a truncated dump agrees with itself.

      So `:verification` is `{:status :unavailable :reason :legacy-format}` and
      `:verified?` is nil — NOT CHECKED, which is what happened.
      `verification-result`'s own `:unavailable` branch is deliberately not
      reused: it THROWS, because there a source that should declare a count did
      not, which is suspicious. Here the format never had the concept and no
      operator can supply it. `verify` is the tool that inspects a legacy dump,
      and says the same thing in the same words.

      Keys absent by construction are absent rather than nil-filled:
      `:transformed?`/`:dropped` need an `:xform` this path does not support,
      `:eid-range`/`:id-map-size` need the id machinery it does not use, and
      `:max-tx-drift` needs a source max-tx no legacy dump records."
     [conn {:keys [record-count tx-count]} opts]
     ;; The TEMPORAL set where the target has one: a legacy `export-db` wrote
     ;; `(api/history db)` whenever the source kept history, and the format
     ;; carries no flag saying whether it did, so counting only current datoms
     ;; would undercount a history dump. Conditional because `api/history`
     ;; THROWS on a database without a temporal index — "history is only allowed
     ;; on temporal indexed databases" — which an unconditional `true` turned
     ;; into an uncaught exception on every `:keep-history? false` import.
     (let [live (user-datom-count @conn (boolean (:keep-history? (:config @conn))))]
       (cond-> {:datom-count live
                :tx-count tx-count
                :max-tx (:max-tx @conn)
                :verified? nil
                :verification (if (get opts :verify? true)
                                {:status :unavailable :reason :legacy-format
                                 :actual live :records-read record-count}
                                {:status :skipped :actual live})
                :errors []}
         (:merge? opts) (assoc :merged? true)))))

(def ^:private record-fault-namespaces
  "Error namespaces that mean *datahike judged this datom*.

   `:on-error :collect`'s contract is to survive a bad RECORD and name it. That
   is only meaningful for failures a record can be responsible for, and these
   are the namespaces datahike uses to say so — `:transact/unique`,
   `:transact/schema`, `:entity-id/missing`, `:lookup-ref/syntax`,
   `:import/malformed-record`, and their siblings.

   A whitelist rather than a blacklist of infrastructure errors, because the two
   fail in opposite directions. An unrecognised INFRASTRUCTURE error under a
   blacklist would be collected — the store blamed on the data, silently, which
   is the bug this exists to stop. An unrecognised DATA error under this
   whitelist aborts the import with the original exception as the cause: noisier
   than it needs to be, and nothing is hidden or misattributed."
  #{"transact" "entity-id" "lookup-ref" "retract" "search" "schema" "import" "db"})

(defn- record-fault?
  "Whether `ex` is a failure the offending record can be held responsible for."
  [ex]
  (let [e (dt/ex-error ex)]
    (and (keyword? e) (contains? record-fault-namespaces (namespace e)))))

(defn- abort-not-a-record-fault!
  "Refuse to file a systemic failure as a bad datom.

   Measured before this existed: an `IOException` out of `writing/commit!` under
   `:on-error :collect` killed the writer, after which the narrowing retry
   attempted every remaining datom individually against a dead one and filed all
   74 as `:import/corrupt-datom` — the first of them a `:db/txInstant` the
   EXPORTER wrote. `import-db` then returned normally with 10 of 84 datoms
   restored, and the only signal was `:verified? false`.

   Two things were wrong and both are fixed by stopping here: the datoms were
   blamed for the store, and the narrowing did 74 pointless writes against a
   writer that could not accept any of them."
  [ex context]
  (throw (ex-info (str "Import aborted: the failure is not attributable to any "
                       "record, so :on-error :collect cannot survive it — "
                       (ex-message ex))
                  (merge {:error :import/apply-failed
                          :cause-error (dt/ex-error ex)}
                         context)
                  ex)))

(defn- collect-apply!
  "Apply a tx-aligned batch under :on-error :collect. Fast path applies the whole
   batch; on failure it narrows to per-transaction, then per-datom, so exactly the
   offending datoms are recorded and skipped while everything else still lands
   (id-consistency holds because the :migration id-map persists in the db value
   across load-entities calls).

   The narrowing retry is safe because a FAILED load-entities call applies
   nothing: the writer's transaction loop builds the new db value purely and only
   a successful result reaches the commit queue / `(reset! connection ...)` — on
   Exception it recurs with the old db (src/datahike/writer.cljc:105-112, commit
   at :140-143). So re-attempting a tx/datom after a failed batch cannot
   double-apply anything. Returns the errors collected.

   ## Why this is shaped the way it is

   The narrowing used to live INSIDE the `catch`: catch the batch failure, then
   `mapcat` over transactions retrying each, then `reduce` over datoms retrying
   each. Neither survives the move to async, for two independent reasons —
   core.async cannot park inside a `catch`, and `mapcat`/`reduce` bodies are
   closures the `go` state machine does not enter.

   So each attempt now records only WHETHER it succeeded, and every retry happens
   outside the handler in a `loop`. Behaviour is identical; the shape is one the
   async branch can compile."
  [conn batch migration progress opts]
  (async+sync
   (:sync? opts) *default-sync-translation*
   (go-try-
    ;; Each successful call yields an updated mapping; a FAILED one yields
    ;; none, because the writer returns the old db and allocates nothing.
    (let [r0 (try {:rep (dt/delivered! (<?- (load-batch! conn batch migration opts))
                                       {:op :load-entities :batch-size (count batch)}
                                       "a batch was applied but the writer returned no report")}
                  (catch #?(:clj Exception :cljs :default) e {:ex e}))
          rep (:rep r0)]
      ;; Checked BEFORE narrowing, not after. Narrowing a systemic failure costs
      ;; one write per datom against something that cannot accept any of them,
      ;; and then reports each as corrupt.
      (when (and (:ex r0) (not (record-fault? (:ex r0))))
        (abort-not-a-record-fault! (:ex r0) {:batch-size (count batch)}))
      (if rep
        (do (progress {:phase :batch :datoms (count batch)})
            {:errors [] :migration (:migration rep)})
        (loop [groups (seq (partition-by #(nth % 3) batch)) errs [] m migration]
          (if (nil? groups)
            {:errors errs :migration m}
            (let [tx-group (vec (first groups))
                  rt (try {:rep (<?- (load-batch! conn tx-group m opts))}
                          (catch #?(:clj Exception :cljs :default) e {:ex e}))
                  tx-rep (:rep rt)]
              (when (and (:ex rt) (not (record-fault? (:ex rt))))
                (abort-not-a-record-fault! (:ex rt) {:tx (nth (first tx-group) 3)
                                                     :tx-size (count tx-group)}))
              (if tx-rep
                (recur (next groups) errs (:migration tx-rep))
                (let [[errs' m'] (loop [ds (seq tx-group) errs errs m m]
                                   (if (nil? ds)
                                     [errs m]
                                     (let [d (first ds)
                                           r (try {:rep (<?- (load-batch! conn [d] m opts))}
                                                  (catch #?(:clj Exception :cljs :default) e {:ex e}))]
                                       (when (and (:ex r) (not (record-fault? (:ex r))))
                                         (abort-not-a-record-fault! (:ex r) {:datom d}))
                                       (recur (next ds)
                                              (if-let [ex (:ex r)]
                                                ;; `dt/ex-error`, not `(:error (ex-data ex))`:
                                                ;; the writer boundary wraps twice, so the
                                                ;; latter is nil even for a `:transact/unique`
                                                ;; that said exactly what was wrong — which is
                                                ;; how every collected error came back labelled
                                                ;; `:import/corrupt-datom`.
                                                (conj errs {:error (or (dt/ex-error ex) :import/corrupt-datom)
                                                            :datom d :message (ex-message ex)})
                                                errs)
                                              (if (:ex r) m (:migration (:rep r)))))))]
                  (recur (next groups) errs' m')))))))))))

(defn- flush-batch!
  "Apply one tx-aligned batch via load-entities. Under :on-error :abort a failure
   throws; under :collect the offending datoms are skipped and returned as errors
   (per-datom granularity, see `collect-apply!`)."
  [conn batch on-error migration progress opts]
  (async+sync
   (:sync? opts) *default-sync-translation*
   (go-try-
    (if (seq batch)
      (if (= :collect on-error)
        (<?- (collect-apply! conn batch migration progress opts))
        ;; the failure is captured and RETHROWN outside the handler: core.async
        ;; cannot park inside a `catch`, and the throw has to follow the await
        (let [r (try {:rep (<?- (load-batch! conn batch migration opts))}
                     (catch #?(:clj Exception :cljs :default) e {:ex e}))]
          (if-let [ex (:ex r)]
            ;; `:import/corrupt-datom` only when the failure IS about a record.
            ;; The old spelling was `(merge (ex-data ex) {:error :import/corrupt-datom})`,
            ;; which stamps that label over whatever the failure actually said —
            ;; and since the writer boundary leaves `(ex-data ex)` empty, that
            ;; label was ALWAYS the answer. A store outage aborted the import
            ;; with `:error :import/corrupt-datom`, sending an operator to look
            ;; at their dump.
            (throw (if (record-fault? ex)
                     (ex-info (str "Import aborted: " (ex-message ex))
                              (merge (ex-data ex) {:error :import/corrupt-datom
                                                   :cause-error (dt/ex-error ex)})
                              ex)
                     (ex-info (str "Import aborted: " (ex-message ex))
                              (merge (ex-data ex) {:error :import/apply-failed
                                                   :cause-error (dt/ex-error ex)
                                                   :batch-size (count batch)})
                              ex)))
            ;; A nil `rep` has no `:ex`, so without this the batch reports SUCCESS
            ;; — `progress` fires with its full count and `:migration` becomes
            ;; nil, which restarts the id map and leaves DANGLING REFS in the
            ;; restored database. Guarded outside the `try` so it is not
            ;; misfiled as `:import/corrupt-datom`: the datoms were fine, the
            ;; writer did not answer.
            (let [rep (dt/delivered! (:rep r)
                                     {:op :load-entities :batch-size (count batch)}
                                     "a batch was applied but the writer returned no report")]
              (progress {:phase :batch :datoms (count batch)})
              {:errors [] :migration (:migration rep)}))))
      {:errors [] :migration migration}))))

(defn- config-compat! [manifest conn]
  (let [tgt (dbi/-config @conn)
        src (:source-config manifest)]
    (doseq [k config-must-match]
      (when (and (contains? src k) (not= (get src k) (get tgt k)))
        (throw (ex-info (str "Config mismatch on " k
                             ": dump=" (get src k) " target=" (get tgt k))
                        {:error :import/config-mismatch :key k
                         :expected (get src k) :actual (get tgt k)}))))))

(defn- check-target!
  "Refuse an import this target cannot accept — BEFORE anything is written.

   Split out of `run-import`, which performed these checks under a comment
   reading \"guard rails (all before touching the db)\" while `import-db` had
   already called `restore-blobs!`. Importing a blob-carrying dump into a
   non-empty database therefore wrote every blob into the target store and only
   then refused. The objects are content-addressed and unreferenced, so the GC
   reclaims them eventually, but the refusal should come first — and on a large
   blob set that is gigabytes written to be told the target was never eligible."
  [conn manifest opts]
  (when-let [fv (get manifest manifest-key)]
    (when (> fv format-version)
      (throw (ex-info (str "Unsupported dump format-version " fv)
                      {:error :import/format-version :version fv}))))
  (config-compat! manifest conn)
  ;; ---- machine-local paths inside a portable dump ----
  ;; Secondary indexes ARE recreated automatically on import: their declaration
  ;; lives in the schema, the schema datoms are replayed like any others, and
  ;; transacting `:db.secondary/type` instantiates the index. What does NOT
  ;; travel is `:db.secondary/config`, which may name an absolute path on the
  ;; machine the dump came from — a lucene directory, a stratum root. Restoring
  ;; that onto a different host writes the index somewhere the operator did not
  ;; choose, or fails at the first write, long after the import reported success.
  ;;
  ;; A warning rather than a refusal: the path may well be right — restoring onto
  ;; the same host is the common case — and the caller has `:xform` to rewrite it
  ;; if it is not.
  (doseq [[ident entry] (or (:schema manifest) {})]
    (when-let [p (and (map? entry) (:db.secondary/type entry)
                      (get-in entry [:db.secondary/config :path]))]
      (warn! (str "[datahike.migrate] the dump's secondary index " ident
                  " carries an absolute path from the machine it was exported on: "
                  (pr-str p) ". It will be recreated pointing THERE. Rewrite it with"
                  " :xform if this database is being restored somewhere else."))))
  ;; ---- an :eids policy aimed at ids that are already taken ----
  ;; `:preserve` refuses on collision, and a caller-supplied map or function gets
  ;; the same courtesy: `transact-entities-directly` does not check, so a mapping
  ;; onto an occupied eid MERGES the dump's entity into an existing one and the
  ;; import reports success. Only checkable against a target that has data, which
  ;; is `:merge?`.
  (when (and (:merge? opts) (:eids opts)
             (not (contains? #{:allocate :offset :preserve} (:eids opts))))
    (warn! (str "[datahike.migrate] a caller-supplied :eids mapping is NOT checked for"
                " collisions with entity ids already in this database. Mapping onto an"
                " occupied id silently merges the dump's entity into the existing one."
                " :preserve refuses instead; :offset cannot collide by construction.")))
  ;; The format and config checks above are about COMPATIBILITY and apply
  ;; whatever the target holds. Only emptiness is negotiable.
  (when-not (:merge? opts)
    (when (has-user-datoms? @conn)
      (throw (ex-info (str "Target database is not empty; import is not resumable — "
                           "recreate and restart, or pass `:merge? true` to ADD this "
                           "dump to what is already there (append-only; see `import-db`).")
                      {:error :import/non-empty-target})))))

(defn- eid-policy
  "Resolve the `:eids` option into what `:migration`'s `:eids` slot should hold,
   or nil to leave the default (allocate fresh ids) alone.

   The lever is `transact-entities-directly`, which does
   `(or (migrated-eid migration-state e) max-eid)` — a pre-seeded answer wins
   over allocation. `run-import` already seeds that map for system entities;
   this generalises it to the caller's own.

     :allocate   (default) fresh target ids; source ids irrelevant.
     :offset     e -> e + delta, delta above the target's :max-eid, so the two
                 id spaces cannot overlap. For a database you know to be
                 disjoint from the dump.
     :preserve   e -> e. For a dump whose ids are already correct — an exact
                 restore. Refuses when the target has allocated past `e0`,
                 because preserving into occupied space would silently merge
                 two different entities.
     a map       {source-eid target-eid}, e.g. from `migrate.ids/build-mapping`.
     a function  (fn [source-eid] -> target-eid).

   `:offset` and `:preserve` are FUNCTIONS, so they cost O(1) rather than the
   O(entities) map the default builds — which is the memory ceiling
   `estimate-import-memory` warns about. Nothing else changes: `max-eid` still
   advances from each datom's own `e` inside `with-datom`.

   Transaction ids are NOT covered. They keep their existing allocation, which
   starts above the target's `:max-tx`; preserving source tx ids into a
   populated database is a different problem with its own collision rules."
  [eids db]
  (cond
    (nil? eids) nil
    (= :allocate eids) nil
    (= :preserve eids)
    (do (when (> (long (:max-eid db)) (long c/e0))
          ;; A WARNING, not a refusal. `:preserve` is the caller asserting that
          ;; the source ids are correct for this target, and only they can know
          ;; that. Refusing on `max-eid > e0` would reject every
          ;; `:schema-flexibility :write` database, since schema attributes are
          ;; entities and occupy ids — i.e. the common case — while still not
          ;; proving anything about whether the ranges actually overlap.
          (warn! (str "[datahike.migrate] :eids :preserve into a target that has "
                      "allocated entities already (max-eid " (:max-eid db)
                      "). Source ids are used as-is, so any that collide will "
                      "MERGE with the existing entity rather than being added. "
                      "Use :offset if the two id spaces are meant to be disjoint.")))
        identity)
    (= :offset eids)
    (let [delta (- (long (:max-eid db)) (long c/e0))]
      (fn [e] (+ (long e) delta)))
    (or (map? eids) (fn? eids)) eids
    :else (throw (ex-info (str "Unknown :eids strategy " (pr-str eids)
                               ". Expected :allocate, :offset, :preserve, a map or a function.")
                          {:error :import/bad-eid-strategy :value eids}))))

(defn- manifest->source-meta
  "The three things `run-import` needs from a dump's manifest.

   The whole of the importer's dependency on the dump FORMAT, in one place. A
   source that is not a datahike dump — a CSV reader, a triple stream,
   pre-sorted synthetic data — builds this map directly and never produces a
   manifest at all.

   Every key is optional. Without `:expected-count` the post-import count check
   has nothing to compare against (so pass `:verify? false`, or supply a count);
   without `:max-tx` there is no drift warning; `:history?` defaults to false,
   which is right for any source that has no retractions."
  [manifest]
  {:history?       (boolean (:history? manifest))
   :expected-count (:count (:semantic-digest manifest))
   :max-tx         (:max-tx (:stats manifest))})

(defn- dangling-refs
  "Ref values in `db` that name an entity holding no datoms.

   A POST-PASS over the target, not bookkeeping during the import, and that is a
   deliberate trade. Collecting \"every eid referenced\" and \"every eid asserted\"
   as records stream past is O(entities) in memory — the same term
   `estimate-import-memory` already warns about for the id map, and the reason
   the `:eids` policy went to trouble to make `:offset`/`:preserve` functions.
   This walks the ref datoms the import produced and probes each value instead:
   bounded memory, one index probe per ref.

   It is therefore OPT-IN. It costs a pass proportional to the ref count, and an
   import that does not ask should not pay for it.

   A dangling ref is NOT necessarily corruption. It is normal for a filtered
   import — an `:xform` that drops the entities being pointed at leaves the
   pointers behind, and the caller may well intend that. It is also what a wrong
   `:eids` mapping produces, which is the case worth catching. So this reports;
   it never refuses.

   `sample` bounds what is carried back, because a mapping that is wrong is
   usually wrong for everything and nobody wants a million eids in a report."
  [db sample]
  (let [ref-attrs (into #{} (keep (fn [[a m]]
                                    (when (and (keyword? a)
                                               (= :db.type/ref (:db/valueType m)))
                                      a)))
                        (:schema db))
        exists? (fn [e] (boolean (seq (dbi/datoms db :eavt [e]))))]
    (reduce (fn [acc a]
              (reduce (fn [acc dm]
                        (let [v (:v dm)]
                          (if (exists? v)
                            acc
                            (-> acc
                                (update :count inc)
                                (cond-> (< (count (:sample acc)) sample)
                                  (update :sample conj [(:e dm) a v]))))))
                      acc
                      (dbi/datoms db :aevt [a])))
            {:count 0 :sample []}
            ref-attrs)))

(defn- run-import
  "Medium-agnostic import core.
   `chunk-src` yields the dump's records a CHUNK at a time:
   `{:chunks [descriptor…] :read (fn [descriptor opts] -> records)}`, supplied by
   whichever medium the source names.
   `mem` is the memory estimate. Handles guards, attribute-refs seeding, the
   tx-aligned batcher, verification, finalization, and the report.

   ## Why chunks and not a reducer

   The old seam was `reduce-source`, `(fn [rf init] -> acc)`: the medium drove the
   fold and the caller supplied a reducing function. That works only while `rf`
   is PURE, and this one is not — it writes datoms. A reducing function is a
   closure, and the `go` state machine does not enter one, so an async import
   could never have been expressed through it. Yielding chunks instead puts the
   read AND the write at statement positions the state machine can see.

   `verify` still uses `reduce-source`, because its folds really are pure.

   ## Why `source-meta` and not the manifest

   This used to take the dump's manifest and read five keys out of it. Those
   five are the whole of its dependency on the dump FORMAT — everything else it
   needs comes from `conn` or `opts` — so it takes them directly now:

     :history?       was `(:history? manifest)`
     :expected-count was `(:count (:semantic-digest manifest))`
     :max-tx         was `(:max-tx (:stats manifest))`

   That is what makes a non-dump source possible. A CSV reader, a triple stream
   or pre-sorted synthetic data can supply `{:chunks … :read …}` plus whatever
   of those three it knows, and needs no manifest at all. Every one of them is
   optional here: absent `:expected-count` only means count verification has
   nothing to compare against, and absent `:max-tx` only means no drift warning.

   The dump-specific checks stay with the CALLER — `check-capabilities!`,
   `config-compat!` and `restore-blobs!` each already no-op on an absent
   manifest, so a non-dump caller simply does not call them."
  [conn source-meta mem chunk-src opts]
  (let [opts     (merge {:batch-size default-batch-size :verify? true
                         :on-error :abort :sync? true} opts)
        progress (or (:progress-fn opts) (constantly nil))
        xform    (:xform opts)
        batch-size (:batch-size opts)
        ;; The backstop for a stream that never reaches a transaction boundary.
        ;; A multiple of `:batch-size` rather than an absolute, so raising the
        ;; batch size raises the ceiling with it and one knob still governs
        ;; memory. `:max-pending` overrides for a caller who wants the ceiling
        ;; tighter than the flush point.
        hard-max (long (or (:max-pending opts) (* 4 batch-size)))
        on-error (:on-error opts)]
    (async+sync
     (:sync? opts) *default-sync-translation*
     (go-try-
    ;; ---- heap preflight: tell the operator how much RAM to give this ----
    ;; `false?`, not `when-not`. `:sufficient?` is ABSENT where the runtime has
    ;; no heap ceiling to report — ClojureScript, see `manifest/max-heap` — and
    ;; `(when-not nil ...)` fires, so every Node import would have printed a heap
    ;; warning full of nils through `format` and `*err*`, neither of which exists
    ;; there. Three cljs-incompatible constructs on one line, reachable only once
    ;; this file becomes .cljc: a bug placed one commit before the commit that
    ;; would have detonated it.
      (when (false? (:sufficient? mem))
        (warn! (str "[datahike.migrate] heap warning: importing " (:datoms mem)
                    " datoms (~" (:entities mem) " entities) needs about "
                    (:recommended-heap mem) "; this runtime's limit is "
                    (:current-max-heap mem)
                    ". Raise it (the id-remap map is held for the whole import) or"
                    " expect OutOfMemoryError.")))
    ;; Guard rails already ran in `check-target!`, before any blob was written.
    ;; ---- the id-mapping seed, carried WITH every load-entities call ----
    ;; Not swapped onto the connection: the writer loop carries its own db value
    ;; and refreshes only `:max-tx` from it (see the TODO in `datahike.writer`),
    ;; so a connection-level seed is not reliably visible to the transactor.
    ;; That is also why the attribute-refs system seed moved here — it had the
    ;; same exposure, silently.
      (let [db @conn
          ;; Under `:merge?` the count check has to be a DELTA: `expected` is a
          ;; property of the SOURCE, while the live count is a property of the
          ;; whole database. Taken before anything is written, and only when it
          ;; will be used — on an empty target it is 0 and the scan is free,
          ;; but on a populated one it is a full index read and paying for it
          ;; when `:verify? false` would be waste.
            live-before (if (and (:merge? opts) (:verify? opts))
                          (long (user-datom-count db (boolean (:history? source-meta))))
                          0)
            max-eid-before (:max-eid db)
          ;; system-entity identity, so refs to a system entity are TRANSLATED
          ;; rather than re-allocated (#508)
            sys (when (attribute-refs? db) (system-eid-seed db))
            policy (eid-policy (:eids opts) db)
            eids (cond
                 ;; A policy is about the caller's OWN entities. System entities
                 ;; keep their identity regardless, or `:offset` would shift the
                 ;; targets of system refs and break them.
                   (and (fn? policy) (seq sys)) (fn [e] (if (contains? sys e) e (policy e)))
                   (fn? policy) policy
                   (and policy (seq sys)) (merge sys policy)
                   policy policy
                   (seq sys) sys)
          ;; THE id mapping for this import, owned here and threaded through the
          ;; batcher. It is not on the database value and not on the connection:
          ;; an import is many `load-entities` calls and a late batch may name an
          ;; entity from an early one, so the mapping has to survive between
          ;; calls — but the db value has two holders (the connection atom and
          ;; the writer's own loop), and putting it there cost three bugs.
          ;; Owning it here also means it needs no clearing: it goes out of
          ;; scope when this function returns.
            migration0 (if eids {:eids eids} {})]
    ;; ---- stream records through a tx-aligned batcher (bounded memory) ----
    ;; System-entity idents are stable across the import, so resolve #sysref
    ;; against a captured db value.
    ;;
    ;; The batcher only flushes at a `t` boundary, so a RUN of one transaction's
    ;; records is never split — including across a chunk or file boundary, since
    ;; the batcher state carries over. That is not the same as "a transaction is
    ;; never split", which this used to claim: under `:sort? false` a
    ;; transaction's records arrive in two separate runs (schema/meta in the
    ;; first :eavt pass, data in the second) and a flush can land between them.
    ;; Harmless for content — the id-remap maps a source `t` to the same target
    ;; transaction whichever call it arrives in — but it is why the transaction
    ;; count is taken from the id map rather than from the batcher.
        (let [sref-db @conn
          ;; ONE stepper for the whole import, not one per chunk. A transducer
          ;; applied per chunk resets its state at every boundary — and chunk
          ;; boundaries are an artefact of how the dump was written, not of the
          ;; data — so `dedupe`, `partition-all` and `take` would be quietly
          ;; wrong in a way that a single-chunk test would not show. `step`
          ;; closes over that state and outlives the chunk loop.
          ;;
          ;; The transform runs at the CHUNK
          ;; boundary, after the read has been awaited and before the batching
          ;; loop, so the loop below and its parked flush are unchanged. One
          ;; input record may expand to zero or more outputs.
              step    (when xform (xform conj))
          ;; NET records removed, not "dropped": a transducer may EXPAND as well
          ;; as filter, and `expected` below is `declared - this`. One input
          ;; becoming three makes it -2, which raises the expectation, which is
          ;; right. Counted here because `prepare` is where records disappear —
          ;; the batching loop never sees them.
              removed (volatile! 0)
              ;; A VALUE, not a boolean — `:validate-records? :skip`, the way
              ;; `:checksums :skip` is spelled. An opt-out that costs 0.3% should
              ;; not be reachable by a stray `true` or a typo'd key.
              ;;
              validate? (not= :skip (:validate-records? opts))
              ;; `:collect` COLLECTS malformed records rather than aborting on
              ;; them — a bad record must surface either way, and `:collect`'s
              ;; whole contract is to survive one and name it. An earlier version
              ;; simply disabled validation here, which quietly reintroduced the
              ;; two shapes this check exists for (a non-boolean `op`, a nil `e`)
              ;; for anyone using `:collect`.
              collect? (= :collect on-error)
              bad (volatile! [])
              check (fn [rs]
                      (cond
                        (not validate?) rs
                        collect? (into [] (remove
                                           (fn [r]
                                             (try (mman/validate-record! r) false
                                                  (catch #?(:clj Exception :cljs :default) e
                                                    (vswap! bad conj
                                                            {:error (or (:error (ex-data e))
                                                                        :import/malformed-record)
                                                             :datom r
                                                             :message (ex-message e)})
                                                    true))))
                                       rs)
                        :else (do (run! mman/validate-record! rs) rs)))
              prepare (fn [records]
                        (let [rs (mapv #(resolve-sysrefs sref-db %) records)]
                          (if step
                            (let [out (into [] (mapcat (fn [r]
                                                         (let [o (step [] r)]
                                                           (if (reduced? o) @o o))))
                                            rs)]
                              (vswap! removed + (- (count rs) (count out)))
                              ;; AFTER the transducer, so an `:xform` can repair a
                              ;; malformed record before it is judged — and so a
                              ;; transducer that PRODUCES one is caught.
                              (check out))
                            (check rs))))
          ;; TWO NESTED LOOPS, not a reduce. The outer reads one chunk, the inner
          ;; batches its records and flushes — and both the read and the flush are
          ;; awaits, which must sit at statement positions the `go` state machine
          ;; can see. A reducing function would hide the flush inside a closure.
          ;;
          ;; Memory is unchanged: one chunk (bounded by `:chunk-size`) plus one
          ;; batch (bounded by `:batch-size`), exactly as before.
              final (loop [cs (seq (:chunks chunk-src))
                           acc {:batch [] :n 0 :last-t ::start :errors [] :dropped 0
                                :migration migration0}]
                      (if (nil? cs)
                        acc
                        (let [records (prepare (dt/delivered!
                                                (<?- ((:read chunk-src) (first cs) opts))
                                                {:op :read-chunk :chunk (first cs)}
                                                "a dump chunk could not be read"))]
                          (recur
                           (next cs)
                           (loop [rs (seq records) acc acc]
                             (if (nil? rs)
                               acc
                           ;; Already resolved and transformed by `prepare`. The
                           ;; xform sees a plain [e a v t op] with real ids and
                           ;; never an internal SysRef; dropping is expressed by
                           ;; emitting nothing.
                               (let [rec (first rs)]
                                 (if rec
                                   (let [t (nth rec 3)
                                         ;; Flush at a TRANSACTION boundary once the
                                         ;; batch is full — but never wait forever for
                                         ;; one. A stream whose records all carry the
                                         ;; same `t` (a database built by ONE large
                                         ;; `transact`, or by `load-entities`, exported
                                         ;; and re-imported) never satisfies
                                         ;; `(not= t last-t)`, so without the second
                                         ;; clause nothing flushes and the whole dump
                                         ;; accumulates here — defeating the
                                         ;; `:chunk-size` + `:batch-size` bound this
                                         ;; loop exists to provide, on exactly the
                                         ;; databases big enough to need it.
                                         ;;
                                         ;; Splitting a transaction across two
                                         ;; `load-entities` calls is safe: the target
                                         ;; tx id lives in `migration-state[:tids]`,
                                         ;; which is threaded across calls, so both
                                         ;; halves land on ONE transaction with order
                                         ;; and grouping intact.
                                         acc (if (or (and (>= (long (:n acc)) batch-size)
                                                          (not= t (:last-t acc)))
                                                     (>= (long (:n acc)) hard-max))
                                               (let [r (<?- (flush-batch! conn (:batch acc)
                                                                          on-error (:migration acc)
                                                                          progress opts))]
                                                 (-> acc
                                                     (update :errors into (:errors r))
                                                     (assoc :migration (:migration r))
                                                     (assoc :batch [] :n 0)))
                                               acc)]
                                     (recur (next rs)
                                            (-> acc
                                                (update :batch conj rec)
                                                (update :n inc)
                                                (assoc :last-t t))))
                                   (recur (next rs) (update acc :dropped inc))))))))))
          ;; The completion arity, once, after the last chunk. A transducer
          ;; holding a partial group — `partition-all`, or a custom one buffering
          ;; until some boundary — emits it here and nowhere else; without this
          ;; the tail of the stream is silently dropped.
          ;; The completion arity emits any partial group the transducer held.
          ;; Those are outputs with no corresponding input in this pass, so they
          ;; reduce the net-removed count.
              residue (if step (vec (step [])) [])
              _ (vswap! removed - (count residue))
              last-flush (<?- (flush-batch! conn (into (:batch final) residue)
                                            on-error (:migration final)
                                            progress opts))
              errors (-> (into (:errors final) (:errors last-flush))
                         (into @bad))
          ;; the mapping the whole import built — the `:tids` half is where the
          ;; transaction count comes from
              migration (:migration last-flush)
              hist?  (boolean (:history? source-meta))
          ;; What this import ADDED. Identical to the whole-database count for
          ;; the ordinary empty-target import, where `live-before` is 0.
              live   (- (long (user-datom-count @conn hist?)) live-before)
              dropped (long (+ (long (:dropped final)) (long @removed)))
          ;; A translator that DROPS records makes the dump's own count the wrong
          ;; expectation — the mismatch is the transformation working, not a
          ;; failure. Subtracting the drops keeps the check meaningful (records
          ;; that should have landed and did not are still caught) instead of
          ;; either failing spuriously or being switched off, which is how people
          ;; learn to ignore verification output.
              vres (verification-result {:verify? (:verify? opts)
                                         :source-count (:expected-count source-meta)
                                         :dropped dropped :live live :on-error on-error})
              verified? (:verified? vres)
          ;; The EXACT number of source transactions, taken from the id mapping's
          ;; `:tids` — one entry per distinct source `t`, already in hand, so
          ;; this costs nothing.
          ;;
          ;; The batcher's own counter cannot do this. It counts TRANSITIONS
          ;; between adjacent `t` values, which equals the transaction count only
          ;; when every transaction's records are contiguous — true for a sorted
          ;; dump and false for `:sort? false`, where `export-records-streaming`
          ;; makes two passes over :eavt and emits a transaction's schema/meta
          ;; datoms in the first and its data datoms in the second. Measured: a
          ;; 13-transaction database reported 25.
              tx-count (count (:tids migration))]
      ;; Same cljs hazards as the heap warning above — `*err*` and `format` exist
      ;; on neither path — and this one had no guard at all. Left as-is until the
      ;; move to .cljc, where both warnings become one portable helper rather
      ;; than two hand-written `binding` forms.
      ;; `max-tx` drift compares the restored database's numbering against the
      ;; source's. That is a meaningful claim about a RESTORE and a meaningless
      ;; one about a merge, where the target's numbering was never supposed to
      ;; match.
          (when-let [src-max-tx (and (not (:merge? opts)) (:max-tx source-meta))]
            (let [drift (- (long (:max-tx @conn)) (long src-max-tx))]
              (when-not (zero? drift)
                (warn! (str "[datahike.migrate] max-tx drifted by "
                            (if (pos? drift) "+" "") drift
                            " (source " src-max-tx ", restored " (:max-tx @conn)
                            "): the restored database numbers its transactions"
                            " differently, so datom `tx` components differ from the"
                            " source's and `as-of` / `since` / `tx-range` shift with"
                            " them. Entity ids, attributes and values are unaffected."
                            " A faithful restore drifts by 0.")))))
          (let [refs (when (:check-refs? opts)
                       (dangling-refs @conn (get opts :dangling-sample 10)))]
            (when (and refs (pos? (long (:count refs))))
              (warn! (str "[datahike.migrate] " (:count refs) " ref value(s) in the imported "
                          "database name an entity that holds no datoms. This is expected "
                          "when an :xform dropped the entities being pointed at; it is also "
                          "what a wrong :eids mapping produces. Sample: "
                          (pr-str (:sample refs)))))
            (cond-> {:datom-count live
       ;; "The records were transformed", which is what makes `:dropped` and any
       ;; count mismatch expected rather than a fault.
                     :transformed? (boolean xform)
                     :dropped     dropped
       ;; ZERO for a faithful restore. It used to be +1 per `load-entities`
       ;; CALL -- so +N for N batches -- because `transact-entities-directly`
       ;; bumped max-tx once per call on top of the correct bump per source
       ;; transaction. That skipped an id at every batch boundary and stretched
       ;; the numbering, which is not cosmetic: a datom carries its `tx`, so
       ;; the same dump imported at two `:batch-size` settings produced
       ;; databases whose datoms differed, and `as-of` / `since` / `tx-range`
       ;; with them. The per-call bump is gone; this stays because a non-zero
       ;; drift now means something real (a merge, or a target that was not
       ;; empty) and an operator should be told. nil when the dump records no
       ;; source max-tx.
                     :max-tx-drift (when-let [src-max-tx (and (not (:merge? opts)) (:max-tx source-meta))]
                                     (- (long (:max-tx @conn)) (long src-max-tx)))
                     :merged?     (boolean (:merge? opts))
       ;; How many entity ids the import had to REMEMBER. Zero under a function
       ;; policy (`:offset`, `:preserve`), which is the whole reason a function
       ;; is allowed: the default accumulates one entry per source entity, and
       ;; that map is what the heap warning above is about. A count rather than
       ;; the map itself, so reporting it retains nothing.
                     :id-map-size (let [m (:eids migration)] (if (map? m) (count m) 0))
       ;; Which entity ids this import created, so a caller that merged can find
       ;; what it just added without diffing the database.
                     :eid-range   (let [after (:max-eid @conn)]
                                    (when (> (long after) (long max-eid-before))
                                      [(inc (long max-eid-before)) (long after)]))
                     :tx-count    tx-count
                     :max-tx      (:max-tx @conn)
                     :verified?   verified?
                     :verification (:verification vres)
                     :recommended-heap (:recommended-heap mem)
                     :errors      errors}
              refs (assoc :dangling-refs refs)))))))))

;; ---------------------------------------------------------------------------
;; the index-build path (opt-in, `:build-indexes? true`)

(defn build-indexes-refusal
  "Why this import cannot take the index-build path, as a sentence, or nil.

   Every clause is a property the bulk builder cannot REPRODUCE, not a feature
   it merely lacks — the point of refusing rather than degrading is that an index-build
   import must be indistinguishable from the transact import it replaces, and
   the four traps below are the ways it would not be:

   `:merge?` / a non-empty target — building index trees from sorted input
     cannot apply the upsert semantics `load-entities` applies when datoms meet
     data that is already there. A complement, not a temporary gap.

   a non-persistent-set index — `di/init-index-sorted` is implemented for
     persistent-set only; hitchhiker-tree is deprecated.

   `:attribute-refs? true` — a fresh database of that shape already holds
     `ref-datoms` in its indexes and counts NONE of them in `:hash`
     (`db.cljc/empty-db`), while `export-records` filters them out of the dump.
     The bulk build would therefore have to synthesise them, and its `:hash`
     rule would need an exception. Left out rather than half-done.

   a secondary index in the dump's schema — `build-family!` builds the six
     primary trees and nothing else, and `db->stored` would then publish a db
     whose `:secondary-index-keys` are absent while its schema declares them.

   Two clauses that USED to be here are gone, and neither was a property the
   builder cannot reproduce — they were descriptions of an implementation:

   `:sync? false` said \"the sort and the tree build are blocking\". The sort is,
     on both runtimes, because every read in it is a synchronous local file read;
     the tree build is not, and `di/init-index-sorted` has had a partial-cps
     ClojureScript arm for as long as this refusal existed.

   ClojureScript said \"`datahike.migrate.init` is `.clj`\". It is `.cljc`.

   Note there is nothing left to refuse on that axis: `assert-sync-supported!`
   refuses `{:sync? true}` on ClojureScript at the public entry points, so cljs
   necessarily arrives here async, and the JVM keeps its synchronous path
   through the same `async+sync` source.

   Refusals are checked and reported TOGETHER as one message rather than
   one-at-a-time, so a caller learns everything blocking them in a single run."
  [conn manifest opts]
  (let [config (:config @conn)
        schema (or (:schema manifest) {})
        reasons
        (cond-> []
          (:merge? opts)
          (conj ":build-indexes? builds indexes from scratch and cannot merge into existing data")

          (not= :datahike.index/persistent-set (:index config))
          (conj (str ":build-indexes? needs the persistent-set index, this database uses "
                     (:index config)))

          (:attribute-refs? config)
          (conj ":build-indexes? does not support :attribute-refs? true")

          (some (fn [[_ entry]] (and (map? entry) (:db.secondary/type entry))) schema)
          (conj ":build-indexes? does not build secondary indexes, and this dump's schema declares one")

          ;; Not defensiveness for its own sake: `ids/build-mapping` decides
          ;; whether a VALUE is a ref by looking the attribute up in this schema,
          ;; and a ref value may point forward to an entity whose own datoms come
          ;; later. With no schema every ref value passes through unmapped, which
          ;; is invisible whenever the mapping happens to be the identity — the
          ;; usual case for an empty target — and silently produces dangling refs
          ;; when it is not. Every dump this exporter writes carries `:schema`.
          ;;
          ;; Keyed on the KEY's absence, not on an empty map: a
          ;; `:schema-flexibility :read` database declares no attributes, so its
          ;; `ident-schema` is legitimately `{}` — and it can hold no refs
          ;; either, since `:db.type/ref` requires a declaration. Empty is fine;
          ;; missing means the manifest is not one of ours.
          (not (contains? manifest :schema))
          (conj ":build-indexes? needs the dump's schema to remap ref values, and this manifest carries none")

          ;; `:preserve` (the default here) and `:allocate` are the two this
          ;; path can honour. `:offset` and a caller-supplied map/fn are about
          ;; fitting a dump AROUND data that is already there, which is the
          ;; `:merge?` case this path refuses outright.
          (not (contains? #{nil :preserve :allocate} (:eids opts)))
          (conj (str ":build-indexes? supports :eids :preserve (default) and :allocate; "
                     (pr-str (:eids opts)) " is for fitting a dump around existing data,"
                     " which this path does not do")))]
    (when (seq reasons)
      (apply str "cannot build indexes directly: " (interpose "; " reasons)))))

(defn- reduce-source-records
  "Reduce `rf` over the dump's sysref-resolved, `:xform`ed records, ONE CHUNK AT
   A TIME. `async+sync`: the accumulator in sync mode, a channel of it otherwise.

   ## Why a fold and not a seq

   This used to be `source-records`, returning a fresh lazy seq that each pass
   reduced over. That cannot survive `:sync? false`: `((:read chunk-src) c opts)`
   returns a CHANNEL there, and a `go` state machine does not park inside a
   `lazy-seq` body — the identical constraint `run-import` documents under \"Why
   chunks and not a reducer\", and the reason `reduce-dump-records` lost its
   streaming arm. So the read moves to a statement position in an explicit chunk
   `loop`, and the caller is handed a fold instead of a seq.

   The chunk loop is also what keeps the memory bound the seq had. Emphatically
   not

     (mapcat #((:read chunk-src) % opts) (:chunks chunk-src))

   which is what this was two revisions ago and which read the WHOLE dump into
   memory before the sort saw a record: `mapcat` is `(apply concat (map f coll))`
   and `map` over a CHUNKED collection evaluates `f` for a whole 32-element block
   at once, while a dump has far fewer than 32 chunk files — 14 for the
   1.4M-datom database this was measured on. Measured: pulling ONE record decoded
   14 of 14 chunks; one chunk at a time, the same import decoded 4 and completed
   in a 256 MB heap where the eager version died. That is also why the OOM was
   insensitive to `:sort-buffer`.

   `rf` must not PARK. It is applied inside a `reduce` here, so an await in it
   would be invisible to the go block — the same rule `reduce-dump-records`
   states for its own `rf`. Both callers only compute and write to the spool,
   which is what makes the fold seam sufficient: the spool is read back with
   synchronous `fs` primitives on both runtimes, so everything downstream of this
   function can go back to being a lazy seq.

   Called ONCE PER PASS, and each call builds a FRESH transducer instance. That
   is sound only because `:xform` is documented as pure: two instances see the
   same input in the same order, so they produce the same output.

   `counter`, when given, is a volatile incremented once per record BEFORE the
   transducer sees it — the pre-transform count, which is what makes verification
   meaningful after a dropping `:xform`.

   The transducer is stepped ONE RECORD AT A TIME from a fresh `[]`, which is
   exactly what `run-import`'s `prepare` does, rather than through `sequence`.
   Two reasons, and the first is a bug this had:

     * `sequence` SHORT-CIRCUITS. `(take 25)` stops pulling after 25 records, so
       the counter above saw 25 inputs instead of the dump's 55, `dropped` came
       out 0, and verification failed on a transducer that had worked perfectly.
       Stepping explicitly always consumes the whole input.
     * it makes `:xform` semantics identical between the two import paths by
       construction, rather than by two implementations agreeing.

   `reduced?` is unwrapped per record, and the completion arity runs once after
   the last chunk so a transducer holding a partial group flushes it."
  [sref-db chunk-src opts counter rf init]
  (async+sync
   (:sync? opts) *default-sync-translation*
   (go-try-
    (let [read-chunk (:read chunk-src)
          step (when-let [xf (:xform opts)] (xf conj))
          ;; wrapped ONCE, around `rf` — so it covers the transducer's output and
          ;; the completion arity's residue without a second call site to forget
          rf (if (or (= :skip (:validate-records? opts))
                     (= :collect (:on-error opts)))
               rf
               (fn [a r] (mman/validate-record! r) (rf a r)))]
      (loop [cs (seq (:chunks chunk-src)) acc init]
        (if (nil? cs)
          ;; The completion arity, once, at the end of the PASS — not per chunk,
          ;; where a stateful transducer would flush a group the stream had not
          ;; finished.
          (if step (reduce rf acc (step [])) acc)
          (let [records (dt/delivered! (<?- (read-chunk (first cs) opts))
                                       {:op :read-chunk :chunk (first cs)}
                                       "a dump chunk could not be read")]
            (recur (next cs)
                   (reduce (fn [a record]
                             (let [r (resolve-sysrefs sref-db record)]
                               (when counter (vswap! counter inc))
                               (if step
                                 (let [o (step [] r)]
                                   (reduce rf a (if (reduced? o) @o o)))
                                 (rf a r))))
                           acc records)))))))))

;; Distinct transaction ids, for the report's `:tx-count`.
;;
;; This exists because the obvious cheap trick does not work: a dump carries NO
;; `:db/txInstant` records at all — measured, 0 of them in a 24-record dump
;; spanning 6 transactions — so counting tx entities counts nothing.
;; `export-records` projects datoms, and datahike keeps txInstant in the tx
;; entity rather than in the datom stream this reads.
;;
;; Two implementations, and they do NOT cost the same. On the JVM it is a
;; `java.util.BitSet` offset by `tx0`: a transaction id IS `tx0 + k` for a small
;; k, so one bit per POSSIBLE transaction is exact and costs ~125 KB per million
;; transactions, where a `HashSet` of boxed Longs would cost a few hundred times
;; that. ClojureScript has no BitSet, and a hand-rolled one over a typed array is
;; not worth writing for a counter, so it is a `js/Set` of the ids themselves:
;; O(DISTINCT TRANSACTIONS) boxed numbers rather than one bit per possible
;; transaction. That is a real asymmetry, not parity — a million-transaction
;; dump costs tens of megabytes on Node against 125 KB on the JVM — and it is
;; accepted only because the term stays well under the spool and the sort's own
;; window, both of which are proportional to the whole database.
(defn- tx-counter [] #?(:clj (java.util.BitSet.) :cljs (js/Set.)))

(defn- tx-seen! [counter t]
  #?(:clj (.set ^java.util.BitSet counter (int (- (long t) (long c/tx0))))
     :cljs (.add counter t))
  nil)

(defn- tx-cardinality [counter]
  #?(:clj (.cardinality ^java.util.BitSet counter)
     :cljs (.-size counter)))

(defn- run-index-build
  "Build all six index trees from the dump directly and publish them in ONE
   commit, instead of replaying the dump through `load-entities`.

   ## Two passes over the dump, three over a normalised file

   1. `ids/build-mapping` — the COMPLETE id mapping, before anything is
      written. A bulk build needs final ids before sorting, because the sort
      order is over those ids; the incremental allocation
      `transact-entities-directly` does cannot supply them. Skipped entirely
      under `:eids :preserve`, which is the default here.
   2. Rewrite every record through that mapping and write the result to a
      temporary chunked CBOR spool. The dump is not read again: the three sorts
      read the spool, which is already sysref-resolved, transformed and remapped.
      `:hash` and the schema datoms are collected in the same pass, since the
      records are streaming past anyway.
   3. Three external sorts of the spool — one per index family — each feeding
      both trees of its family (see the namespace docstring on why six trees
      need three sorts).

   ## Where the asynchrony is, and where it is not

   `async+sync`, so one source compiles into a synchronous JVM path and a
   ClojureScript one. Only TWO things in the sequence are ever asynchronous:

     * the dump read, which is why passes 1 and 2 go through
       `reduce-source-records`' chunk loop rather than a lazy seq;
     * the tree build, which on ClojureScript is partial-cps (see
       `init/build-index!`).

   Everything between them is synchronous on both runtimes, and the SPOOL is
   what makes that true: it is written once from the async read and then read
   back with `fs` synchronous primitives (`readSync` on Node), so the three
   sorts and the currentness folds stay ordinary lazy seqs. That property is
   worth preserving — losing it would mean pushing `go` state machines through
   the merge sort.

   ## The guard, and why it is not a `finally`

   `guard/writing!` is taken BEFORE the first tree is built and released only
   after the commit that publishes the root. Everything in between is written to
   the store while the branch head still names the previous (empty) snapshot, so
   a concurrent collector would classify it as garbage. The commit runs inside
   the writer, and the writer delivers its callback only after `reset!`ing the
   connection — so awaiting the publish is exactly the right span.

   The release is explicit on both the success and the failure path, INSIDE the
   go block, and NOT a `try/finally` — for the reason `import-db` records at its
   store arm: in async mode the body hands back a channel, so a `finally` fires
   the moment it is handed back, which here would drop the guard before a single
   node had been written and delete the spool out from under the sorts. Same for
   the temp-directory cleanup.

   ## `source-schema` does NOT install the schema

   It was called `schema`, which reads as \"the schema this build installs\" and
   is not what it is. The installed schema is derived from the schema DATOMS in
   the record stream, by `init/schema-from-records` below — so a source must
   emit its own schema datoms, and this argument cannot substitute for them.

   What it IS: how to READ the records. Two uses, both about interpretation
   rather than declaration:

     * `ref?`, on every record, to decide whether a ref VALUE should raise
       `:max-eid`. An entity that is only ever pointed AT — never the `e` of any
       datom — still occupies an id, and missing it lets a later transact
       allocate that id and silently resurrect the reference.
     * `ids/build-mapping` and `ids/apply-mapping`, under `:eids :allocate`
       only, which must rewrite ref values as ids rather than leave them as
       opaque numbers.

   For a datahike dump the two agree by construction: the manifest's `:schema`
   and the schema datoms come from the same database. Nothing CHECKS that they
   agree, which does not matter yet and will as soon as a caller supplies both
   independently — see the record-source seam work, where a source declaring one
   schema and emitting another would compute `:max-eid` from the wrong ref set."
  [conn source-meta source-schema mem chunk-src opts]
  ;; The SAME defaults `run-import` applies, and for the same reason: they are
  ;; applied here rather than in `import-db` because `run-import` is also
  ;; called directly. Missing them once already cost this path its
  ;; verification — `:verify?` read as nil, so every index-build import reported
  ;; `:verified? nil` and checked nothing.
  ;; `:eids :preserve` by DEFAULT, unlike the streaming path, whose default is
  ;; `:allocate`. The target is empty, so there is nothing for a source id to
  ;; collide with, and preserving them is both the cheaper and the more
  ;; faithful answer: no O(entities) map, one less read of the dump, and a
  ;; restore that reproduces its source's ids exactly. `:allocate` remains
  ;; available for a caller who wants the dump renumbered densely.
  (let [opts       (merge {:verify? true :on-error :abort :sync? true
                           :eids :preserve}
                          opts)
        opts       (update opts :eids #(or % :preserve))
        db0        @conn
        config     (:config db0)
        keep-hist? (boolean (:keep-history? config))
        store      (:store db0)
        store-id   (:id (:store config))
        ;; 200k, not the export path's 1,000,000.
        ;;
        ;; `spill-runs` holds `run-size` records in memory, sorts them, and
        ;; spills — so this number IS the sort's peak heap, and at a million
        ;; five-element vectors it is the largest term in the whole import
        ;; now that the id map is gone under `:preserve`. Worse, `spill-runs`
        ;; does not spill at all when the input fits in one run, so a
        ;; million-record default means every database up to a million
        ;; records is sorted ENTIRELY in memory — exactly the case a bounded
        ;; heap cannot afford.
        ;;
        ;; Lower is not free: fewer records per run means more runs and a
        ;; deeper merge (`reduce-runs` handles the fan-in). 200k trades a
        ;; little merge depth for a bound an operator can reason about, and
        ;; `:sort-buffer` raises it for anyone who has the heap.
        run-size   (get opts :sort-buffer 200000)
        progress   (or (:progress-fn opts) (constantly nil))
        spool-codec (get opts :spool-codec mz/default-codec)
        ;; The index BUILDER's mode, which is NOT the import's — and this is the
        ;; one place the two must not be conflated. `psset/from-sorted-seq` is
        ;; synchronous on the JVM whatever `:sync?` says: it calls `(flush-fn)`
        ;; and ignores the result (persistent_sorted_set.clj:398). So a `false`
        ;; here under a JVM `:sync? false` import would hand the builder channels
        ;; nobody ever takes — the konserve writes would still run, out of band,
        ;; with their errors dropped on the floor and no backpressure at all.
        ;; The ClojureScript builder is partial-cps and does await the flush, and
        ;; `:sync? true` never reaches this function there
        ;; (`assert-sync-supported!` refuses it at the entry points).
        build-sync? #?(:clj true :cljs false)]
    (when (false? (:sufficient? mem))
      (warn! (str "[datahike.migrate] heap warning: importing " (:datoms mem)
                  " datoms needs about " (:recommended-heap mem)
                  "; this runtime's limit is " (:current-max-heap mem) ".")))
    (async+sync
     (:sync? opts) *default-sync-translation*
     (go-try-
      (let [tmp   (fs/temp-dir! "dh-index-build")
            token (guard/writing! store-id)
            res
            (try
              (let [;; ---- pass 1, ONLY under :allocate ----
                    ;; `:preserve` needs no mapping at all, and skipping it
                    ;; removes BOTH the O(entities) map and one of the two reads
                    ;; of the dump. That is not a micro-optimisation:
                    ;; `estimate-import-memory` calls the id map "the dominant,
                    ;; unavoidable term", and it is what would put a big restore
                    ;; over a bounded heap. It is avoidable here precisely
                    ;; because the target is empty — there is nothing for a
                    ;; source id to collide with.
                    ;;
                    ;; `build-mapping`'s body is `(reduce-records rf init)`, so
                    ;; handing it the async fold makes it return that fold's
                    ;; channel unchanged. The park happens inside
                    ;; `reduce-source-records`' own go block, not inside this
                    ;; closure — which the state machine could not enter.
                    mapping  (when (= :allocate (:eids opts))
                               (<?- (ids/build-mapping
                                     {:schema source-schema
                                      :system-entities (:system-entities db0)
                                      :max-eid (:max-eid db0)
                                      :max-tx (:max-tx db0)}
                                     (fn [rf init]
                                       (reduce-source-records db0 chunk-src opts nil rf init)))))
                    ;; ---- pass 2: normalise to the spool, collecting what we can see ----
                    ;; COMPRESSED scratch. This spool holds the whole database,
                    ;; and it used to be raw CBOR while the dump it came from was
                    ;; gzipped — several times the dump's size in scratch, which
                    ;; an operator had to discover rather than be told.
                    spool    (init/open-spool! tmp spool-codec
                                               (get opts :spool-chunk-size
                                                    init/default-spool-chunk-size))
                    raw-n    (volatile! 0)
                    schema?  (fn [record] (let [a (nth record 1)]
                                            (or (ds/schema-attr? a) (ds/entity-spec-attr? a))))
                    ref?     (fn [a] (= :db.type/ref (:db/valueType (get source-schema a))))
                    txs      (tx-counter)
                    ;; No `try/finally` around the fold to close the spool. In
                    ;; async mode the fold IS a channel, so a `finally` would
                    ;; flush the spool before a record had been read; and there
                    ;; is nothing to release on the failure path anyway — a spool
                    ;; file is a file in `tmp`, which the cleanup below removes
                    ;; whichever way this ends.
                    state    (<?- (reduce-source-records
                                   db0 chunk-src opts raw-n
                                   (fn [acc record]
                                     (let [r (if mapping
                                               (ids/apply-mapping mapping source-schema record)
                                               record)
                                           n (inc (long (:n acc)))
                                           e (nth r 0) a (nth r 1) v (nth r 2) t (nth r 3)]
                                       ((:add! spool) r)
                                       (when (>= (long t) (long c/tx0))
                                         (tx-seen! txs t))
                                       (when (zero? (rem n 100000))
                                         ;; `{:phase .. :datoms ..}` — the shape
                                         ;; every other progress callback in this
                                         ;; namespace uses, so one progress-fn
                                         ;; serves both import paths.
                                         (progress {:phase :normalise :datoms n}))
                                       (-> acc
                                           (update :n inc)
                                           (update :hash + (if (nth r 4)
                                                             (hash (d/datom e a v))
                                                             0))
                                           ;; O(1) running maxima, so `:preserve`
                                           ;; needs no mapping to derive them
                                           ;; from. `tx0` PARTITIONS the id space
                                           ;; — entity ids below it, transaction
                                           ;; ids at or above — which is what
                                           ;; makes a single comparison enough.
                                           ;; Ref VALUES count towards max-eid
                                           ;; too: a ref may name an entity that
                                           ;; holds no datoms of its own, and a
                                           ;; later allocation landing on that id
                                           ;; would silently resurrect the
                                           ;; reference.
                                           (update :max-eid max
                                                   (if (< (long e) c/tx0) (long e) 0)
                                                   (if (and (ref? a) (number? v) (< (long v) c/tx0))
                                                     (long v) 0))
                                           (update :max-tx max (long t))
                                           (cond-> (schema? r) (update :schema-recs conj r)))))
                                   {:n 0 :hash 0 :schema-recs []
                                    :max-eid (long (:max-eid db0)) :max-tx (long (:max-tx db0))}))
                    ;; flushes the tail chunk; the files are the spool
                    spool-files ((:close! spool))
                    ;; ---- the schema-derived fields, from the dump's own datoms ----
                    sfields  (init/schema-from-records db0 (:schema-recs state))
                    rschema  (:rschema sfields)
                    index-config (assoc (:index-config config)
                                        :indexed (:db/index rschema)
                                        :sync? build-sync?
                                        :flush-fn (dwriting/bulk-flush-fn store build-sync?))
                    ;; ---- three sorts, six trees ----
                    ;; Announced, because this is the long phase and it emits
                    ;; nothing while it runs: three external sorts and six tree
                    ;; builds over the whole database, with no per-record
                    ;; callback to hang progress on.
                    _        (progress {:phase :build-indexes :datoms (:n state)})
                    ;; `(:sync? opts)`, NOT `build-sync?`: the last argument is
                    ;; the SHAPE `build-indexes!` hands back, and this import
                    ;; awaits it. The builder's own mode travels in
                    ;; `index-config` above. Passing `build-sync?` here meant a
                    ;; JVM `:sync? false` import awaited a plain map — "No
                    ;; implementation of method: :take! of protocol: ReadPort".
                    indexes  (<?- (init/build-indexes! store (:index config) index-config rschema
                                                       keep-hist?
                                                       #(init/spool-records spool-files spool-codec)
                                                       run-size tmp (:sync? opts)))
                    fields   (merge sfields indexes
                                    ;; Under `:allocate` the mapping already
                                    ;; knows both maxima exactly — it allocated
                                    ;; them — and using it costs nothing. Under
                                    ;; `:preserve` the running maxima from pass 2
                                    ;; are the whole answer. Both are exact,
                                    ;; neither scans the finished trees.
                                    {:max-eid (if mapping
                                                (dec (long (:next-eid mapping)))
                                                (:max-eid state))
                                     :max-tx  (if mapping
                                                (dec (long (:next-tx mapping)))
                                                (:max-tx state))
                                     :hash    (:hash state)
                                     ;; inert for persistent-set (every index op
                                     ;; takes it as `_op-count`), and
                                     ;; hitchhiker-tree — the only index that
                                     ;; reads it — is refused above.
                                     :op-count (:n state)})
                    ;; Awaited the way `flush-batch!` awaits the writer, and for
                    ;; the same reason: `throwable-promise` is derefable AND
                    ;; parkable on the JVM, while ClojureScript has only the
                    ;; promise-chan. A bare `deref` here would not compile there.
                    p        (dwriter/publish-built-db! conn fields)
                    report   #?(:clj (if (:sync? opts) @p (<?- p))
                                :cljs (<?- p))
                    _        (when (instance? #?(:clj Throwable :cljs js/Error) report)
                               (throw report))
                    live (long (user-datom-count @conn (boolean (:history? source-meta))))
                    ;; NET removed by the transducer, exactly as `run-import`
                    ;; counts it — a transducer may expand as well as filter, so
                    ;; an input becoming three makes this negative, which RAISES
                    ;; the expectation. Subtracting it keeps the check meaningful
                    ;; (records that should have landed and did not are still
                    ;; caught) rather than either failing spuriously on a working
                    ;; transformation or being switched off.
                    dropped (- (long @raw-n) (long (:n state)))
                    vres (verification-result {:verify? (:verify? opts)
                                               :source-count (:expected-count source-meta)
                                               :dropped dropped :live live
                                               :on-error (:on-error opts)})
                    verified? (:verified? vres)
                    refs (when (:check-refs? opts)
                           (dangling-refs @conn (get opts :dangling-sample 10)))]
                (when (and refs (pos? (long (:count refs))))
                  (warn! (str "[datahike.migrate] " (:count refs) " ref value(s) in the imported "
                              "database name an entity that holds no datoms. Sample: "
                              (pr-str (:sample refs)))))
                (cond-> {:datom-count live
                         :build-indexes?       true
                         :transformed? (boolean (:xform opts))
                         :dropped     dropped
                         ;; The index-build path does not transact, so `max-tx`
                         ;; is exactly the source's — none of the +1 the
                         ;; streaming import reports, because nothing bumps it on
                         ;; the way in.
                         :max-tx-drift (when-let [src (:max-tx source-meta)]
                                         (- (long (:max-tx @conn)) (long src)))
                         :merged?     false
                         ;; ZERO under `:preserve`, which is the point of it: the
                         ;; heap warning is about this number.
                         :id-map-size (if mapping (count (:eids mapping)) 0)
                         :eid-range   (let [after (long (:max-eid @conn))
                                            before (long (:max-eid db0))]
                                        (when (> after before) [(inc before) after]))
                         :tx-count    (tx-cardinality txs)
                         :max-tx      (:max-tx @conn)
                         :verified?   verified?
                         :verification (:verification vres)
                         :recommended-heap (:recommended-heap mem)
                         :errors      []}
                  refs (assoc :dangling-refs refs)))
              (catch #?(:clj Exception :cljs :default) e e))]
        ;; The guard closes and the scratch goes away on BOTH paths, here rather
        ;; than in a `finally` — see the docstring. The guard must outlive the
        ;; publish, and it does: `publish-built-db!`'s promise resolves only after
        ;; the writer has committed and `reset!` the connection.
        (guard/done! store-id token)
        (doseq [n (or (fs/list-names tmp) [])] (fs/delete! (fs/join tmp n)))
        (fs/delete! tmp)
        (if (instance? #?(:clj Throwable :cljs js/Error) res) (throw res) res))))))

(defn- import-via
  "Pick the import path. `:build-indexes? true` REFUSES rather than falls back: silently
   taking the slow path when the caller asked for the fast one turns a
   configuration mistake into a mysterious performance report, and every reason
   in `build-indexes-refusal` is something the caller can act on."
  [conn manifest mem chunk-src opts]
  (if (:build-indexes? opts)
    (if-let [why (build-indexes-refusal conn manifest opts)]
      (throw (ex-info why {:error :import/build-indexes-refused :reason why}))
      (run-index-build conn (manifest->source-meta manifest)
                       (or (:schema manifest) {}) mem chunk-src opts))
    (run-import conn (manifest->source-meta manifest) mem chunk-src opts)))

(defn records->chunk-src
  "Wrap a seq of records as a `chunk-src`, splitting ONLY at transaction boundaries.

   For a source small enough to hold, or one that is already a lazy seq from
   somewhere cheap. A source that can address its own storage — a dump's chunk
   files, a Datomic `t` range — should build `{:chunks … :read …}` directly
   instead, because descriptors it computes itself are cheap where these are not
   (see the memory note on `import-source`).

   `n` is a MINIMUM, not a maximum: a chunk grows past it until `t` changes, so a
   transaction is never split across chunks. Splitting one would hand the
   importer a partial transaction, and the batcher's flush rule keys on `t`
   changing."
  ([records] (records->chunk-src records default-batch-size))
  ([records n]
   ;; `vec`, not the lazy seq: `:chunks` IS the descriptor list here — each chunk
   ;; is its own descriptor — and the importer holds it for the whole run.
   ;;
   ;; SO THIS HELPER MATERIALIZES THE WHOLE SOURCE, and it is the one place that
   ;; breaks `import-source`'s own rule that a descriptor be "small, data-free
   ;; metadata … never the records themselves". Measured: 3M records OOM at
   ;; -Xmx256m before the importer reads one. It is a CONVENIENCE for a source
   ;; small enough to hold, not a way to stream — a source that can address its
   ;; own storage (a file offset, a `t` range) must build cheap descriptors and
   ;; a `:read` that fetches, which is the whole point of the two-key shape.
   ;;
   ;; The tx-alignment rule lives in `tx-aligned-chunks`, shared with
   ;; `export-to-sink`, so the two cannot disagree about what a chunk is.
   (let [chunks (vec (tx-aligned-chunks records n))]
     {:chunks chunks
      ;; `:read` is `<?-`'d by the importer, so under `:sync? false` it must
      ;; return a CHANNEL — a plain collection raises
      ;; "No implementation of method: :take! of protocol: ReadPort".
      ;; `default-sync?` is FALSE on ClojureScript, so returning the vector
      ;; directly broke every Node caller by default. Measured before this.
      :read   (fn [chunk opts]
                (async+sync (get opts :sync? default-sync?)
                            *default-sync-translation*
                            (go-try- chunk)))})))

(defn import-source
  "Import records from ANY source into `conn`. **Experimental.**

   The medium-agnostic core `import-db` uses for dumps, exposed so a caller can
   supply records from somewhere that is not a datahike dump — Datomic, a
   triple stream, pre-sorted synthetic data. `import-db` is this function plus a
   dump-specific preamble (manifest, checksums, capabilities, blobs); both meet
   at the same importer, so neither can drift from the other.

   `chunk-src` is `{:chunks [descriptor …] :read (fn [descriptor opts] -> records)}`.

   ## What the source owes

   1. Records are `[e a v t op]`: `a` a keyword ident, `v` a real value (`nil` is
      not storable), `t` the source's transaction id, `op` a boolean.
   2. ORDER is `(t, txInstant-first, e, a)` — the same order `export-db` writes
      and `datahike.migrate.sort/sort-key` documents. Schema before the data that
      uses it, the tx entity before its own datoms, causally ordered.
   3. `t` MUST VARY. The batcher flushes on `(and (>= n batch-size) (not= t last-t))`,
      so a source that stamps every record with one `t` never reaches a flush and
      buffers the whole stream. `:max-pending` is the backstop, not the design.
   4. The source EMITS ITS OWN SCHEMA DATOMS. The installed schema is derived from
      the record stream; `:schema` here only feeds ref detection on the
      index-build path.
   5. `:read` must be RE-ENTRANT — `verify` and the index build read chunks more
      than once — and should return a realized, bounded collection. The importer
      `mapv`s it immediately, so laziness buys nothing and a lazy read holding a
      file handle across chunks would be a leak.

   ## Async: `:read` may park, `:xform` may not

   This is the whole reason the seam yields CHUNKS rather than taking a reducing
   function. `run-import`'s docstring has the history: a reducing function is a
   closure, the `go` state machine does not enter one, and an async import could
   never have been expressed through it. Chunks put the read and the write at
   statement positions the machine can see.

   So:

     :read    is `<?-`'d. Under `:sync? false` it MUST return a channel — a plain
              collection raises \"No implementation of method: :take! of protocol:
              ReadPort\". That is also the licence to do real IO in it: a Datomic
              log read, an HTTP fetch, a file read. `default-sync?` is FALSE on
              ClojureScript, so this is the DEFAULT path there, not the exotic one.
     :xform   runs synchronously inside the importer's `go` block and must be
              PURE — no IO, no parking. A parked take inside a transducer cannot
              be reached by the state machine.

   `records->chunk-src` handles the `:read` side for you; a hand-written source
   must do it, and `async+sync` + `go-try-` is how the rest of this namespace does.

   ## Memory

   RECORDS stream: one chunk plus one batch is live at a time. DESCRIPTORS do
   not — the importer holds `chunk-src` for the whole run, so a realized
   descriptor list stays reachable. `:chunks` may be lazy, but a descriptor must
   be small, data-free metadata (a file name, a `t` range), never the records
   themselves.

   ## Opts

   Everything `import-db` takes (`:batch-size` `:xform` `:eids` `:merge?`
   `:build-indexes?` `:on-error` `:sync?` …) plus:

     :source-meta  {:history? :expected-count :max-tx}, all three OPTIONAL.
                   Absent `:expected-count` only means the count check has
                   nothing to compare against; absent `:max-tx` only means no
                   drift warning. A source that knows none of them passes none.
     :schema       source schema map, index-build path only (ref detection).

   ## Verification is DECLINED, not defaulted away

   `:verify?` stays `true` by default here, exactly as for a dump: a source that
   declares an `:expected-count` is held to it, and one that declares none must
   pass `{:verify? false}` and thereby say so. That is the existing contract —
   see `migrate-source-test/a-source-that-knows-no-count-can-still-import` — and
   it is deliberate: a caller who simply forgot `:expected-count` should be told,
   not quietly given an unverified import.

   ## What this does NOT do

   Does NOT do what only a dump needs: no manifest, no checksums, no blob
   restore, no capability or format-version check. A non-dump caller has nothing
   to check them against."
  ([conn chunk-src] (import-source conn chunk-src {}))
  ([conn chunk-src opts]
   (mman/ensure-conn conn "import-source")
   (assert-sync-supported! opts)
   (assert-sizes-positive! opts)
   (msch/validate-opts! msch/ImportOpts opts "import-source")
   ;; `:on-error :collect` has no meaning on the index-build path, so it is
   ;; REFUSED rather than quietly ignored. `run-index-build` carries none of the
   ;; streaming path's `collect-apply!` / `record-fault?` machinery, and
   ;; `reduce-source-records` SKIPS `validate-record!` entirely under `:collect`.
   ;; Measured, one source and one set of opts through both paths: streaming gave
   ;; `[:OK 10 [{:error :import/malformed-record …}]]`, the index build gave
   ;; `[:OK 11 []]` — a nil-valued datom written into the index and zero errors
   ;; claimed. That was reachable only through a corrupt dump; making
   ;; `:build-indexes?` reachable from a record source would have made it routine.
   (when (and (:build-indexes? opts) (= :collect (:on-error opts)))
     (throw (ex-info (str "{:on-error :collect} is not supported with {:build-indexes? true}: "
                          "the index-build path neither validates records nor collects faults, "
                          "so a malformed record is written into the index and reported as no "
                          "error at all. Use the streaming import to collect faults.")
                     {:error :import/collect-unsupported-on-index-build})))
   (let [smeta (:source-meta opts)
         opts (merge {:sync? default-sync?} opts)]
     (async+sync
      (:sync? opts) *default-sync-translation*
      (go-try-
       ;; `nil` manifest throughout: every check inside degrades to a no-op on an
       ;; absent manifest, which is what makes this callable at all.
       (check-target! conn nil opts)
       (if (:build-indexes? opts)
         ;; `(select-keys opts [:schema])`, NOT `{:schema (:schema opts)}` and not
         ;; `nil`. The clause tests `(contains? manifest :schema)`, so a literal
         ;; map is ALWAYS true and turns the guard into a no-op — measured, a
         ;; source with no schema under `:eids :allocate` then produced a ref
         ;; pointing at an entity that does not exist (`#{[3 101]}` where the
         ;; schema gives `#{[3 4]}`), the exact silent dangling ref that clause
         ;; exists to prevent. `nil` was wrong the other way: always true, so this
         ;; branch was DEAD and refused by naming a manifest a record source can
         ;; never have.
         (if-let [why (build-indexes-refusal conn (select-keys opts [:schema]) opts)]
           (throw (ex-info why {:error :import/build-indexes-refused :reason why}))
           ;; `mem` is nil: it feeds only the heap warning and the report's
           ;; `:recommended-heap`, and `(false? (:sufficient? nil))` is false, so
           ;; a source with no size estimate simply gets no heap advice.
           (<?- (run-index-build conn smeta (or (:schema opts) {}) nil chunk-src opts)))
         (<?- (run-import conn smeta nil chunk-src opts))))))))

(defn- restore-blobs!
  "Put the dump's carried `:db.type/store-ref` bytes into the target store, before
   any datom that names them is loaded.

   Order matters and is the same rule the sync walker follows: a reference must
   never exist without its referent, or a reader between the two steps sees a
   dangling pointer. Blobs are content-addressed, so restoring one twice is
   idempotent and a re-run cannot corrupt anything.

   A dump that declares blobs it could NOT carry (`:external` — bytes that lived
   outside the source store, e.g. a bucket a browser PUT to directly) is refused
   unless the caller passes `:accept-external-blobs? true`. The restored database
   would name objects this import did not place, and that has to be a decision
   rather than something discovered later by a failing read."
  [conn manifest source opts]
  (async+sync
   (:sync? opts) *default-sync-translation*
   (go-try-
    (when-let [store-refs (:store-refs manifest)]
      (mblobs/check-importable store-refs opts)
      (when (seq (:carried store-refs))
        (<?- (with-blob-reader source opts #(mblobs/copy-in! (:store @conn) store-refs % opts))))))))

(defn import-db
  "Import a dump produced by `export-db` into connection `conn`.

   `source` may be a filesystem path/dir OR a konserve store target (an open store
   `{:store s :prefix ..}` or a `{:backend :s3 ..}`-style config). The target db
   SHOULD be freshly created with a config compatible with the dump's
   :source-config. 2-arity keeps the legacy surface; 3-arity opts:
     :batch-size   100000   datoms per load-entities call (tx-aligned, never split)
     :verify?      true      after import, check the datom count against the
                             manifest's and throw on mismatch. NOT the `verify`
                             function — that reads the dump; this checks the
                             database that came out of it.
     :on-error     :abort    :abort | :collect  (never silently skip)
     :xform        nil       a TRANSDUCER over records. Each record is
                             `[e a v t op]` with real ids — it runs after
                             sysref resolution, so a user function never sees an
                             internal SysRef. One input may produce zero or more
                             outputs; producing none drops it.

                             ONE instance spans the whole import, so stateful
                             transducers work across chunk boundaries: `(take n)`
                             takes n from the STREAM, not n per chunk, and the
                             completion arity runs at the end so a transducer
                             holding a partial group flushes it rather than
                             losing the tail.

                               :xform (comp (remove schema-datom?)
                                            (filter mine?)
                                            (map rewrite-value))

                             It must be PURE — no IO, no parking. It runs inside
                             the import's `go` block in async mode, where a
                             parked take in a closure cannot be reached by the
                             state machine.
                             `verify`'s tier-2 digest compares the DUMP against
                             the live db, so a transformed import will differ there
                             by design — that comparison is not meaningful after a
                             transformation.
     :merge?       false     ADD this dump to a target that already holds data,
                             instead of refusing it. APPEND-ONLY: every source
                             entity gets a fresh target entity, so importing the
                             same dump twice DUPLICATES it.
                             `transact-entities-directly` does not resolve
                             `:db.unique/identity` (unlike `transact-tx-data`),
                             so there is no upsert-by-key here and re-importing
                             is not idempotent. The report carries `:merged?
                             true` and `:eid-range [from to]` — the ids this
                             import created — so a caller can find what it just
                             added. Verification becomes a DELTA (what the
                             import added, not the whole database), and the
                             `max-tx` drift warning is suppressed because the
                             target's numbering was never meant to match the
                             source's. Refs INSIDE the dump are remapped
                             consistently; refs from the dump to entities that
                             were already in the target are not resolved — use
                             `:eids` for that.
     :eids         :allocate how source entity ids bind to target ones —
                             :allocate (fresh ids), :offset (e + delta, above the
                             target's max-eid, for a database known to be
                             disjoint), :preserve (e as-is, for a dump whose ids
                             are already correct), or a map/function of your own.
                             :offset and :preserve are FUNCTIONS and so cost
                             O(1) rather than the O(entities) map :allocate
                             builds — the memory ceiling the heap warning above
                             is about. Transaction ids are not covered; they are
                             always allocated above the target's :max-tx.
                             See `eid-policy`.
     :build-indexes? false    BETA. Create the database by building its index
                             trees directly from the dump — sort it three times
                             and construct all six trees from sorted input —
                             instead of replaying it datom by datom. One commit,
                             at the end.

                             This is the FRESH-DATABASE case, not bulk ingestion
                             in general: it refuses a non-empty target and
                             refuses `:merge?`, because building trees from
                             sorted input cannot apply the upsert semantics
                             `load-entities` applies when datoms meet existing
                             data.

                             The result is a database equal field-for-field to
                             the one the default path produces, EXCEPT that
                             `:max-tx` is the source's exactly rather than one
                             higher (nothing transacts, so nothing bumps it) and
                             `:op-count` differs, which is inert for
                             persistent-set.

                             `:eids` defaults to `:preserve` HERE, unlike the
                             streaming path: an empty target has nothing for a
                             source id to collide with, so the dump's ids are
                             kept as they are. That removes the O(entities) id
                             map — the term `estimate-import-memory` calls the
                             dominant one — and one of the two reads of the
                             dump, and makes a `:history? true` restore
                             id-identical to its source. `:eids :allocate` opts
                             back into the pre-pass and the map.

                             Runs on ClojureScript/Node too, under `:sync? false`
                             — the only mode there is. The dump read and the tree
                             build are awaited; the sorts in between read a local
                             scratch spool with synchronous primitives on both
                             runtimes.

                             Refused, with the reason, for a merge, a non-empty
                             target, a non-persistent-set index,
                             `:attribute-refs? true`, a dump whose schema
                             declares a secondary index, an `:eids` policy other
                             than `:preserve`/`:allocate`, and a manifest with no
                             schema. See `build-indexes-refusal`.

                             No speedup figure is quoted here on purpose — the
                             earlier one was withdrawn as unsound and has not
                             been re-measured end to end.
     :progress-fn  nil
   RETURNS a report map — always, whichever format `source` turned out to be.
   Always present:

     :datom-count      datoms in the target afterwards
     :tx-count         transactions the import performed
     :max-tx           the target's :max-tx afterwards
     :verified?        true | false | nil — see `verification-result`; nil
                       means NOT CHECKED, which is not the same as passing
     :verification     {:status :ok|:failed|:skipped|:unavailable ..}, which
                       says WHICH of those nil covers
     :errors           [] unless :on-error :collect collected any

   From the manifest-and-chunks path, additionally: `:transformed?`, `:dropped`
   (what an :xform removed), `:max-tx-drift`, `:merged?`, `:id-map-size`,
   `:eid-range`, `:recommended-heap`, and `:dangling-refs` when any were found.
   A LEGACY single-file dump supports none of the features those describe, so
   they are absent rather than nil-filled, and it is NOT verified: its
   `:verification` is `{:status :unavailable :reason :legacy-format}` with
   `:verified?` nil. The format carries no count to check against, and the
   records it holds are not one-for-one with the datoms a correct import
   leaves — so there is no honest comparison to make. Use `verify` to inspect
   such a dump.

   Prints a heap warning if the current -Xmx looks too small for the id-remap
   map (see `estimate-import-memory`).
   Refuses a non-empty target unless `:merge? true` (above); an interrupted
   import is not resumable either way — recreate and restart."
  ([conn source] (import-db conn source {}))
  ([conn source opts]
   (mman/ensure-conn conn "import-db")
   (assert-sync-supported! opts)
   (assert-sizes-positive! opts)
   (msch/validate-opts! msch/ImportOpts opts "import-db")
   (let [opts (merge {:sync? default-sync?} opts)
         batch-size (get opts :batch-size default-batch-size)]
     (async+sync
      (:sync? opts) *default-sync-translation*
      (go-try-
       ;; Warned for BOTH media, above the branch. It used to sit in the
       ;; filesystem arm only, so a store-medium import with `:checksums :skip`
       ;; ran silently — and the store medium is the one an operator cannot open
       ;; by hand to inspect. That is the same one-sided treatment of this option
       ;; that `store/read-chunk` records fixing on the enforcement side; this is
       ;; the reporting side of it.
       (when (= :skip (:checksums opts))
         (warn! (str "[datahike.migrate] importing WITHOUT verifying chunk "
                     "checksums (:checksums :skip). Corruption in this dump "
                     "will be loaded silently: " source)))
       (if (mstore/store-target? source)
       ;; NOT `try/finally`. `run-import` returns a CHANNEL in async mode, so a
       ;; `finally` fires the moment it is handed back — before a single chunk
       ;; has been read. Instrumented, the order was
       ;; `[:CLOSE :READ :READ :READ …]`: every chunk read against a released
       ;; store, and the import still reported `:verified? true`. It looked fine
       ;; only because `:memory` and `:file` release is near-nil; on a pooled
       ;; backend (JDBC, RocksDB, S3) that is a use-after-release.
       ;;
       ;; The close is therefore explicit on both the success and the failure
       ;; path, INSIDE the go block, since core.async cannot park in a `finally`.
         (let [m (<?- (mstore/open source opts))
               manifest (<?- (mstore/read-manifest m opts))
               mem (estimate-from-manifest manifest (manifest-total-bytes manifest nil) batch-size)
               res (try
                     ;; The SAME guard the filesystem arm gets from `open-dump`.
                     ;; Inside the `try` so a refused dump still closes the store.
                     (mman/assert-dump-manifest! manifest source opts)
                     (refuse-incomplete-dump! manifest opts)
                     (check-capabilities! manifest)
                     (check-target! conn manifest opts)
                     (<?- (restore-blobs! conn manifest source opts))
                     (<?- (import-via conn manifest mem
                                      {:chunks (:chunks manifest)
                                       ;; The `:read` seam is where a caller-supplied
                                       ;; function will run once the record source is
                                       ;; public, and on ClojureScript a throw that is
                                       ;; not an Error kills the process rather than
                                       ;; failing the import — see the helper.
                                       :read (fn [c o]
                                               (dt/call-reporting-foreign-throws
                                                #(mstore/read-chunk m manifest c o)
                                                {:op :read-chunk :chunk c}))}
                                      opts))
                     (catch #?(:clj Exception :cljs :default) e e))]
           (<?- (mstore/close m opts))
           (if (instance? #?(:clj Throwable :cljs js/Error) res) (throw res) res))
         (let [dump (open-dump source opts)]
           (if (:legacy? dump)
             ;; `*import-batch-size*` read HERE, so the value is the caller's
             ;; `binding` rather than a root value captured further in.
             #?(:clj (legacy-import-report
                      conn (mlegacy/import-db-legacy conn source *import-batch-size*) opts)
                :cljs (throw (ex-info (str "This is a legacy single-file dump, readable only on the "
                                           "JVM — it can only have been written by an old JVM datahike.")
                                      {:error :import/legacy-not-portable})))
             (let [manifest (:manifest dump)
                   mem (estimate-from-manifest manifest (manifest-total-bytes manifest (:files dump)) batch-size)]
               (refuse-incomplete-dump! manifest opts)
               (check-capabilities! manifest)
               (check-target! conn manifest opts)
             ;; AWAITED. `restore-blobs!` is `async+sync`, so under `:sync? false`
             ;; a bare call hands back a channel that nobody takes: every blob
             ;; failure is discarded and the datom import starts anyway. Since
             ;; post-import verification is count-based, `import-db` then reports
             ;; `:verified? true` for a database whose store-refs point at blobs
             ;; that were never written. The store arm above always awaited this;
             ;; only the filesystem arm did not.
               (<?- (restore-blobs! conn manifest source opts))
               (<?- (import-via conn manifest mem
                                {:chunks (:files dump)
                                 :read (fn [f o]
                                         (dt/call-reporting-foreign-throws
                                          #(fs-read-chunk manifest f o)
                                          {:op :read-chunk :chunk f}))}
                                opts)))))))))))

;; ---------------------------------------------------------------------------
;; verify

(defn- fp-init [] {:acc (dig/accumulator) :refc {} :outd {}})
(defn- fp-step [fp [e a v op] ref?]
  (if (ref? a)
    (-> fp (update-in [:refc a] (fnil inc 0)) (update-in [:outd e] (fnil inc 0)))
    ;; Hash the CBOR encoding of the normalised tuple rather than its `pr-str`.
    ;; This fingerprint compares a DUMP against a LIVE database, so it must be a
    ;; function of the values alone — and `:archival` gives that by construction,
    ;; where `pr-str` does not: it renders a Double and a Float identically and
    ;; differs between JVM and ClojureScript for several types.
    (update fp :acc dig/add-record (mcbor/encode-record [a (norm-val v) op]))))
(defn- fp-final [fp]
  {:digest (dig/finalize (:acc fp))
   :ref-counts (:refc fp)
   :out-degree (frequencies (vals (:outd fp)))})

(defn- with-source-records
  "Call `(f manifest reduce-source)` where `reduce-source` is `(fn [rf init] -> acc)`
   streaming the dump's records, for either medium (filesystem or store).
   `reduce-source` may be invoked several times (each pass re-opens/re-reads).

   `opts` reaches `open-dump` on the filesystem side. It exists for ONE caller:
   `verify` has already hashed every chunk by the time it recomputes the digest,
   and `open-dump` defaults to `:checksums :require`, so the default would hash
   the whole dump a second time and then read it a third to decode. Measured on a
   20k-record dump: 3 ms of hashing became 54 ms. Callers that have NOT already
   verified the bytes — `verify-against` — must keep the default."
  ([source f] (with-source-records source f {}))
  ([source f opts]
   (if (mstore/store-target? source)
     (let [m (mstore/open source)]
       (try (let [manifest (mstore/read-manifest m)]
              ;; Without this every tier folded over zero records and compared
              ;; empty against empty, so `verify` returned a fully green report
              ;; for a store prefix that is not a dump at all.
              (mman/assert-dump-manifest! manifest source {})
              (f manifest (fn [rf init] (mstore/reduce-records m manifest rf init))))
            (finally (mstore/close m))))
     (let [dump (open-dump source opts)]
       (f (:manifest dump) (fn [rf init] (reduce-dump-records dump rf init)))))))

(defn- verify-sample
  "Tier 3 — sampled structural diff. Pick up to `n` entities by a `:db.unique`
   attribute, reconstruct their non-ref attributes from the dump, and diff against
   `pull '[*]'` on the live db."
  [manifest reduce-source db ref? n]
  (let [schema (:schema manifest)
        uniq   (set (for [[a m] schema
                          :when (#{:db.unique/identity :db.unique/value} (:db/unique m))] a))]
    (if (empty? uniq)
      {:sampled 0 :ok? true :note "no :db.unique attrs — content covered by tiers 1–2"}
      (let [picks (reduce-source
                   (fn [acc record]
                     (let [[e a v _t op] record]
                       (if (and op (uniq a) (< (count acc) n) (not (contains? acc [a v])))
                         (assoc acc [a v] e) acc)))
                   {})
            pick-es (set (vals picks))
            ;; net *current* state per picked entity: asserts add, retracts remove,
            ;; so a fully-retracted entity nets to empty (and is not compared).
            recon (reduce-source
                   (fn [acc record]
                     (let [[e a v _t op] record]
                       (if (and (contains? pick-es e) (not (ref? a)))
                         (update-in acc [e a] (fnil (if op conj disj) #{}) (norm-val v))
                         acc)))
                   {})
            recon (into {} (for [[e am] recon
                                 :let [am (into {} (remove (comp empty? val) am))]
                                 :when (seq am)] [e am]))
            diffs (reduce
                   (fn [ds [[a v] e]]
                     (if-let [cur (get recon e)]
                       (let [live (api/q '[:find (pull ?e [*]) . :in $ ?a ?v :where [?e ?a ?v]] db a v)]
                         (if (nil? live)
                           (conj ds {:unique [a v] :error :missing-in-live})
                           ;; pull returns a vector for card-many AND for a tuple
                           ;; value — disambiguate by declared cardinality, never
                           ;; by value shape (a tuple is ONE value).
                           (let [many? (fn [la] (= :db.cardinality/many
                                                   (:db/cardinality (get schema la))))
                                 live-attrs (into {} (for [[la lv] (dissoc live :db/id)
                                                           :when (not (ref? la))]
                                                       [la (if (many? la)
                                                             (set (map norm-val lv))
                                                             #{(norm-val lv)})]))]
                             (if (= live-attrs cur) ds
                                 (conj ds {:unique [a v] :error :field-mismatch})))))
                       ds)) ;; entity net-retracted — not current, not a failure
                   [] picks)]
        {:sampled (count picks) :ok? (empty? diffs) :diffs (vec (take 5 diffs))}))))

(defn- verify-blobs*
  [store-refs read-blob]
  (let [declared (vec (:carried store-refs))
        results  (mapv (fn [id]
                         (let [bytes (read-blob id {:sync? true})]
                           (cond
                             (nil? bytes)                        [:missing id]
                             (not (mblobs/verify-blob id bytes)) [:corrupt id]
                             :else                               [:ok id])))
                       declared)
        missing  (mapv second (filter #(= :missing (first %)) results))
        corrupt  (mapv second (filter #(= :corrupt (first %)) results))]
    {:ok?             (and (empty? missing) (empty? corrupt))
     :declared        (count declared)
     :verified        (count (filter #(= :ok (first %)) results))
     :missing         missing
     :corrupt         corrupt
     :external        (count (:external store-refs))
     :self-contained? (boolean (:self-contained? store-refs))}))

(defn- verify-blobs
  "Check that a dump holds every blob it declares, and that each one's bytes hash
   to the id it is filed under.

   Blobs are verified at the SAME tier as the datom chunks, not reported
   alongside: a dump whose `store-refs/` is short is incomplete, and a
   verification that says `:ok? true` about it would be exactly the reassurance
   nobody should get. Because the file name IS the content hash, this needs
   nothing from the manifest beyond the id — and it catches a truncated or torn
   object, which a count could not.

   `:external` ids are counted, never checked: those bytes were never ours to
   carry (see `datahike.migrate.blobs`). They make a dump not self-contained,
   which the manifest states, and it is the operator who must place them."
  [manifest source]
  (if-let [store-refs (:store-refs manifest)]
    ;; `verify` is sync-only (`verify-blobs*` reads with `{:sync? true}`), so the
    ;; reader is opened in the same mode and hands back a value rather than a
    ;; channel.
    (with-blob-reader source {:sync? true} #(verify-blobs* store-refs %))
    ;; no store-refs section: either no blobs, or a dump predating blob carriage
    {:ok? true :declared 0 :verified 0 :missing [] :corrupt []
     :external 0 :self-contained? true}))

(defn- recompute-digest
  "Fold the manifest's OWN semantic digest over the dump's actual records.

   The same fold the export ran (`write-chunk!` / `write-chunks!`, both
   `(dig/add-record acc (mcbor/encode-record record))`), so a match means the
   records present are the records the manifest describes. `reduce-records`
   hands back DECODED records, so this re-encodes — safe because `:archival`
   makes a record's bytes a function of the record alone, which is the same
   property the chunk `:sha256` already rests on.

   This is the check that catches an altered VALUE. A count cannot: substituting
   one record for another of the same shape leaves every count intact."
  [source]
  (with-source-records
    source
    (fn [_manifest reduce-source]
      (dig/finalize (reduce-source
                     (fn [acc record] (dig/add-record acc (mcbor/encode-record record)))
                     (dig/accumulator))))
    ;; `verify` reaches this only AFTER every chunk's SHA-256 matched. Re-hashing
    ;; them here made one `verify` read each chunk three times and hash it twice.
    {:checksums :skip}))

(defn verify
  "Compare a dump against its own manifest (integrity) and, given a live db
   snapshot (`@conn`, not `conn`), against that database (id-independent
   semantic equivalence). Returns a tiered
   report: tier0 = checksums/paths (validated on open), tier1 = counts,
   tier2 = multiset digest over `[a v op]` + ref-topology counts, tier3 = sampled
   structural diff of unique entities. `source` may be a path or a konserve store.

   JVM-only for now: it opens the medium synchronously throughout. Unlike
   `export-db`/`import-db` it has not been through `async+sync`, so on
   ClojureScript it is refused by name rather than failing somewhere inside
   konserve's sync branch.

   NOT the same thing as `import-db`'s `:verify?` option, which is a datom-count
   delta check on the imported database. This reads the DUMP."
  ([source] (verify source {}))
  ([source opts]
   (assert-jvm-only! "verify" opts)
   (let [{:keys [manifest legacy-count chunks-verified finding recomputed]}
         (if (mstore/store-target? source)
           (let [m (mstore/open source)
                 ;; Carried out of the `try` below rather than returned from it:
                 ;; that form's value is the FINDING, where `nil` means "no
                 ;; integrity failure". A volatile keeps the two answers apart
                 ;; without a second pass over the dump.
                 recomputed (volatile! nil)]
             (try
               (let [manifest (mstore/read-manifest m)
                     ;; REPORTS rather than throws, matching the filesystem
                     ;; branch below — this is the call an operator makes to ASK
                     ;; about a dump, and "it threw" is a worse answer than "here
                     ;; is what is wrong with it". Both the manifest guard and
                     ;; `reduce-records`' per-chunk hash check are caught: before
                     ;; this, a store dump with a corrupt chunk threw out of
                     ;; `verify` while the same dump on disk came back as a
                     ;; finding.
                     finding (try
                               (mman/assert-dump-manifest! manifest source {})
                               ;; This pass ALREADY decoded every record and threw
                               ;; the lot away — `(fn [acc _] acc)`. Folding the
                               ;; digest into it costs one re-encode per record
                               ;; and no extra IO, which on a store medium is the
                               ;; difference between a free check and a second
                               ;; round trip per chunk.
                               (vreset! recomputed
                                        (dig/finalize
                                         (mstore/reduce-records
                                          m manifest
                                          (fn [acc r] (dig/add-record acc (mcbor/encode-record r)))
                                          (dig/accumulator))))
                               nil
                               (catch #?(:clj clojure.lang.ExceptionInfo :cljs :default) e
                                 (let [{:keys [error]} (ex-data e)]
                                   (if (#{:import/missing-checksum :import/checksum-failed
                                          :import/missing-chunk
                                          :import/bad-chunk-path :import/not-a-dump
                                          ;; A broken gzip member is corruption too.
                                          ;; `compress/decompress-bytes` says so
                                          ;; itself: "a hash mismatch and a broken
                                          ;; member are both corruption; they differ
                                          ;; only in which one the reader notices
                                          ;; first". It threw out of `verify` while
                                          ;; a hash mismatch beside it was a finding.
                                          :import/corrupt-chunk} error)
                                     {:error error
                                      :message #?(:clj (.getMessage e) :cljs (.-message e))
                                      :data (ex-data e)}
                                     (throw e)))))]
                 ;; Read every chunk. `mstore/reduce-records` verifies each one's
                 ;; SHA-256 as it goes, which is what makes the `:checksums :ok`
                 ;; below true.
                 ;;
                 ;; This used to read only the manifest and then report
                 ;; `:tier0 {:checksums :ok}` regardless — so a store dump with a
                 ;; corrupted chunk came back `{:ok? true}` from the very call an
                 ;; operator makes to find out whether a backup is intact. The
                 ;; same corruption on a filesystem dump threw, because
                 ;; `open-dump` hashes every chunk. Verified both ways before
                 ;; changing it.
                 {:manifest (or manifest {})
                  :chunks-verified (when (nil? finding) (count (:chunks manifest)))
                  :recomputed @recomputed
                  :finding finding})
               (finally (mstore/close m))))
           ;; REPORTS rather than enforces. `open-dump` throws on a bad or
           ;; missing checksum, which is right for an import — but this is the
           ;; call an operator makes to ASK about a dump, and "it threw" is a
           ;; worse answer than "here is what is wrong with it". So the integrity
           ;; failures become findings and `:ok?` goes false; nothing is hidden
           ;; and nothing needs an opt-out to get an answer.
           (let [[dump finding]
                 (try [(open-dump source) nil]
                      (catch #?(:clj clojure.lang.ExceptionInfo :cljs :default) e
                        (let [{:keys [error]} (ex-data e)]
                          (if (#{:import/missing-checksum :import/checksum-failed
                                 :import/missing-chunk
                                 :import/undeclared-chunks :import/corrupt-chunk} error)
                            [(manifest-of source) {:error error :message #?(:clj (.getMessage e)
                                                                            :cljs (.-message e))
                                                   :data (ex-data e)}]
                            (throw e)))))]
             (if (:legacy? dump)
               ;; Nothing above has looked at this file. `manifest-of` classifies
               ;; ANY existing non-directory as the legacy single-file format and
               ;; synthesises a manifest with no chunks, so `open-dump` has no
               ;; checksums to verify and the report below said `{:ok? true}` for
               ;; a plain text file — from the one call an operator makes to ask
               ;; whether a backup is intact. Decoding it is the only check the
               ;; format admits: it carries no manifest, no chunk list, no hashes.
               ;; `count-records` THROWS `:import/not-a-dump` here, and that is
               ;; deliberate rather than an oversight — `verify-refuses-things-
               ;; that-are-not-dumps` pins it. The findings-not-exceptions rule
               ;; above is about a DAMAGED dump, where an operator asked a
               ;; question and deserves an answer; "this file is not a dump at
               ;; all" is a different category, and the caller pointed at the
               ;; wrong thing. Now that `count-records` also checks record
               ;; SHAPE, arbitrary CBOR joins plain text in that category
               ;; instead of counting as a backup.
               {:manifest (:manifest dump)
                :legacy-count #?(:clj (mlegacy/count-records (str source))
                                 :cljs (throw (ex-info
                                               (str "This is a legacy single-file dump, readable only "
                                                    "on the JVM — it can only have been written by an "
                                                    "old JVM datahike.")
                                               {:error :import/legacy-not-portable})))}
               {:manifest (:manifest dump)
                :chunks-verified (:chunks-verified dump)
                ;; A SECOND pass, unlike the store branch. `open-dump` streams
                ;; each chunk through SHA-256 without decoding it, so there is no
                ;; existing decode to fold into — and decoding inside the hash
                ;; loop would make the cheap tier pay for the expensive one even
                ;; when the bytes are already wrong. Only reached when the hashes
                ;; passed, so the common case is one extra read of a dump already
                ;; known to be intact, on a tool that is not on any hot path.
                ;; Not gated on `(seq chunks)`. A dump with NO chunks is a real
                ;; dump — `export-transformed` with an `:xform` that filters
                ;; everything writes one — and folding over zero records gives
                ;; the same empty digest the export recorded, so it verifies
                ;; rather than being indistinguishable from a broken one.
                :recomputed (when (nil? finding) (recompute-digest source))
                :finding finding})))]
     (let [blobs (verify-blobs manifest source)
           declared (:semantic-digest manifest)
        ;; The legacy single-file format carries no manifest, no chunk list and
        ;; no hashes, so none of the checks below apply to it. `legacy-count` is
        ;; set only on that branch, and decoding the file IS the only check the
        ;; format admits.
           legacy? (some? legacy-count)
        ;; `:checksums` used to be the literal `:ok`, so "40 chunks matched" and
        ;; "there were no chunks and nothing was checked" read identically. It is
        ;; now derived, and `:chunks-verified` says how much was actually read.
           checksums (cond finding                          :failed
                           (and chunks-verified
                                (pos? chunks-verified))     :ok
                           :else                            :none)
        ;; Manifest-internal, and free: the per-chunk counts must add up to the
        ;; declared total. No IO at all, and it catches an edited chunk
        ;; descriptor before a single byte is read.
        ;; `keep`, then a check that we kept them ALL. A chunk descriptor with no
        ;; `:count` makes the sum a number that means nothing, and reporting
        ;; `:counts-add-up? false` for it says the arithmetic is wrong when the
        ;; truth is that it is unknown. The digest below is the fail-closed
        ;; check; this one is the free arithmetic beside it.
           chunk-counts (keep :count (:chunks manifest))
           chunk-sum (when (and (seq (:chunks manifest))
                                (= (count chunk-counts) (count (:chunks manifest))))
                       (reduce + 0 chunk-counts))
           counts-add-up? (when (and chunk-sum (:count declared))
                            (= (long chunk-sum) (long (:count declared))))
        ;; What the export SAYS it wrote against what it HASHED. Also free, also
        ;; manifest-internal, and it is what catches a manifest truncated to
        ;; `:chunks []` with the data still sitting beside it: the digest is then
        ;; honestly empty and agrees with itself, but `:stats` still remembers
        ;; the 103 records the export wrote. The filesystem medium catches that
        ;; forgery by listing the directory (`:import/undeclared-chunks`); the
        ;; store medium cannot afford to, because `konserve.core/keys` is
        ;; store-wide. `open-dump`'s note on that asymmetry rested on
        ;; `:chunks []` leaving `:checksums :none` and `:ok?` refusing it — and
        ;; then the fix that stopped calling a legitimately empty dump broken
        ;; removed exactly that refusal, leaving the two media disagreeing about
        ;; a two-key manifest edit. This restores it without any IO.
           stats-count (get-in manifest [:stats :datom-count])
           stats-agree? (when (and stats-count (:count declared))
                          (= (long stats-count) (long (:count declared))))
        ;; The dump against what the manifest SAYS about it. `verify` used to
        ;; hash the bytes and then echo the manifest's own numbers back as if
        ;; they had been checked — so a manifest claiming 999999 records for a
        ;; 29-record dump, or carrying a garbage `:xor`/`:sum`, came back
        ;; `:ok? true` from the one call an operator makes to ask whether a
        ;; backup is intact.
           digest-match? (when (and recomputed declared) (= recomputed declared))
        ;; FAIL CLOSED on an absent digest, for the reason `open-dump` fails
        ;; closed on an absent chunk `:sha256`: everything datahike writes
        ;; carries one, so a manifest without it has been edited. Failing OPEN
        ;; here would have made `(dissoc manifest :semantic-digest)` — a cheaper
        ;; edit than any this function now catches — defeat all of them at once,
        ;; since `declared` nil leaves every comparison nil.
           declared? (some? declared)
        ;; "Nothing was checked" is not "everything matched" — but it is also
        ;; not "this is broken". Three different things reach `:checksums :none`
        ;; and they need three answers: an empty FILE (no records, no manifest:
        ;; not a backup), a LEGACY dump (no chunks to hash by construction, and
        ;; it decoded), and a chunked dump with an empty `:chunks` (a real
        ;; manifest that declares zero records, which the digest confirms).
        ;; Keying `:ok?` off `:none` alone called all three broken, which
        ;; regressed the latter two.
           anything-verified? (if legacy?
                                (pos? (long legacy-count))
                                (and declared? (not= :failed checksums)))]
       (cond-> {:ok? (and (nil? finding)
                          (:ok? blobs)
                          anything-verified?
                          (not (false? counts-add-up?))
                          (not (false? stats-agree?))
                          (not (false? digest-match?)))
                :tier0 {:checksums checksums
                        :chunks-verified (or chunks-verified 0)
                        :format (get manifest manifest-key)}
                :tier1 (cond-> {:manifest-count (or (:count declared) legacy-count)
                                :recomputed-count (:count recomputed)
                                :digest-match? digest-match?}
                         chunk-sum   (assoc :chunk-count-sum chunk-sum
                                            :counts-add-up? counts-add-up?)
                         stats-count (assoc :stats-count stats-count
                                            :stats-agree? stats-agree?))
                :blobs blobs}
         finding (assoc :integrity finding)
         ;; The comparison keys are present-but-nil so that ONE handler works
         ;; over either function's report. They were absent here and present in
         ;; the comparison, so `(:match? (:tier1 r))` was nil half the time for
         ;; two different reasons — "not compared" and "compared, no answer".
         true    (update :tier1 merge {:live-count nil :match? nil})
         true    (merge {:tier2 nil :tier3 nil}))))))

(declare verify-against*)

(defn verify-against
  "`verify`, plus an id-independent comparison against a LIVE database snapshot
   (`@conn`, not `conn`).

   Split from `verify` rather than being its 2-arity. As one function the first
   argument changed meaning with the arity — `(verify a b)` gave a reader no way
   to tell whether `b` was the source or the opts map every sibling puts there —
   and the leading slot being taken meant `verify` could never gain opts at all.

   The two also disagreed about the same fault: a corrupt chunk was a FINDING
   from `verify` and a THROWN exception from the comparison, though `verify`'s
   own docstring argues that \"it threw\" is the wrong answer to \"is my backup
   intact?\". They now share it — see the catch below — and return the same key
   set, with the comparison keys nil when there was nothing to compare.

   Tiers: tier0 = checksums/paths, tier1 = counts, tier2 = multiset digest over
   `[a v op]` plus ref-topology counts, tier3 = sampled structural diff of unique
   entities.

   JVM-only, for the same reason `verify` is."
  ([db source] (verify-against db source {}))
  ([db source opts]
   (assert-jvm-only! "verify-against" opts)
   (let [db (mman/ensure-db db "verify-against")]
     (try
       (verify-against* db source opts)
       (catch #?(:clj clojure.lang.ExceptionInfo :cljs :default) e
         ;; A dump that fails its own integrity check cannot be meaningfully
         ;; compared, and throwing here would answer a different question than
         ;; the one asked. Fall back to `verify` for the detailed finding: it
         ;; re-reads, which costs a second pass, but only on a dump already
         ;; known to be corrupt. The success path still reads once.
         (if (#{:import/missing-checksum :import/checksum-failed
                :import/missing-chunk
                :import/undeclared-chunks :import/bad-chunk-path :import/not-a-dump
                :import/corrupt-chunk}
              (:error (ex-data e)))
           (verify source opts)
           (throw e)))))))

(defn- verify-against*
  "The comparison itself, on a dump that opened cleanly."
  ([db source opts]
   (with-source-records source
     (fn [manifest reduce-source]
       (let [schema (:schema manifest)
             ref?   (fn [a] (= :db.type/ref (:db/valueType (get schema a))))
             hist?  (boolean (:history? manifest))
               ;; tier 1 — counts
             dump-count (long (or (:count (:semantic-digest manifest)) 0))
             live-count (long (user-datom-count db hist?))
               ;; tier 2 — id-independent fingerprint, dump vs live
               ;; `:db.secondary/only` attributes compare on the HASH, because
               ;; the two sides legitimately hold different things: the dump
               ;; carries the real value (export reads it from the secondary
               ;; index — without that the backup would not contain the data at
               ;; all), while the live primary holds `project-primary`'s content
               ;; hash. Projecting the dump side is what makes them comparable;
               ;; the alternative would be reading every live secondary index
               ;; back during verification.
             sec-only? (fn [a] (dbu/secondary-only? db a))
             dump-fp (fp-final (reduce-source
                                (fn [fp record]
                                  (let [[e a v _t op] record
                                        v (if (and op (sec-only? a))
                                            (dbt/secondary-only-hash v)
                                            v)]
                                    (fp-step fp [e a v op] ref?)))
                                (fp-init)))
             src     (if hist? (api/history db) db)
             live-fp (fp-final (reduce (fn [fp dm]
                                         (if (> (d/datom-tx dm) c/tx0)
                                           (fp-step fp [(nth dm 0) (a-ident db (nth dm 1)) (nth dm 2) (nth dm 4)] ref?)
                                           fp))
                                       (fp-init) (api/datoms src :eavt)))
             t2-ok  (and (= (:digest dump-fp) (:digest live-fp))
                         (= (:ref-counts dump-fp) (:ref-counts live-fp))
                         (= (:out-degree dump-fp) (:out-degree live-fp)))
               ;; tier 3 — sampled structural diff
             t3     (verify-sample manifest reduce-source db ref? 25)
               ;; blobs — same tier as the chunks, and part of :ok?
             blobs  (verify-blobs manifest source)]
         {:ok? (and (= dump-count live-count) t2-ok (:ok? t3) (:ok? blobs))
            ;; DERIVED, not the literal `:ok` this used to be. The 1-arity was
            ;; fixed to distinguish "every chunk was hashed" from "there were no
            ;; chunks and nothing was checked"; forty lines below its comment
            ;; saying so, this arity still hardcoded `:ok` — and reported it for
            ;; a store prefix that was not a dump, with every tier folding over
            ;; zero records and comparing empty against empty.
            ;;
            ;; `with-source-records` now refuses a non-dump outright, so reaching
            ;; here means a real manifest; this says how much of it was covered.
          :tier0 {:checksums (if (pos? (count (:chunks manifest))) :ok :none)
                  :chunks-verified (count (:chunks manifest))
                  :format (get manifest manifest-key)}
          :tier1 {:manifest-count dump-count :live-count live-count :match? (= dump-count live-count)}
          :tier2 {:match? t2-ok
                  :value-digest-match? (= (:digest dump-fp) (:digest live-fp))
                  :ref-counts-match? (= (:ref-counts dump-fp) (:ref-counts live-fp))
                  :out-degree-match? (= (:out-degree dump-fp) (:out-degree live-fp))}
          :tier3 t3
          :blobs blobs})))))

;; ---------------------------------------------------------------------------
;; legacy CBOR path (backward compatibility for old dumps)

