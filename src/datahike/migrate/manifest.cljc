(ns ^:no-doc datahike.migrate.manifest
  "Everything about a dump that is a pure function of a database or a manifest:
   what a dump DECLARES, what it takes to read one, which records it contains,
   and how much heap importing it will want.

   Extracted from `datahike.migrate` because none of it touches the dump
   MEDIUM, and therefore none of it needed the async treatment the orchestration
   does. (`export-records`/`export-records-streaming` do read the database, via
   `api/datoms` — so the claim is about the filesystem and the store, not about
   IO in general.) That split is worth having on its own terms — this is the
   part with the rules in it, and it is testable without a dump — but the
   immediate reason is that it made the portable/non-portable boundary visible
   instead of implied.

   Two things here are unavoidably platform-specific and are marked as such:
   provenance metadata (`datahike.tools/meta-data` is a JVM-only macro reading
   build-time version constants) and the current heap (there is no
   `Runtime.maxMemory` in a browser). Both are diagnostics; neither changes what
   a dump contains or whether it can be read."
  (:require [datahike.api :as api]
            [datahike.constants :as c]
            [datahike.datom :as d]
            [datahike.db.interface :as dbi]
            [datahike.db.utils :as dbu]
            [datahike.index.secondary :as sec]
            [datahike.schema :as ds]
            [datahike.migrate.blobs :as mblobs]
            [datahike.migrate.cbor :as mcbor]
            [datahike.migrate.compress :as mz]
            [datahike.migrate.store :as mstore]
            [clojure.edn :as edn]
            #?(:clj [datahike.tools :as dt])))

(def format-version 1)

(def blob-plan-key
  "Where `export-db` stashes the blob plan in `opts` for `build-manifest` to read.

   Spelled out rather than written `::blob-plan`, because an auto-resolved keyword
   is relative to the namespace it is READ in. When `build-manifest` moved here
   from `datahike.migrate`, `::blob-plan` silently became
   `:datahike.migrate.manifest/blob-plan` while the writer still set
   `:datahike.migrate/blob-plan` — so the plan was never seen, the manifest
   declared no `:store-refs` and no blob capabilities, and `verify` reported
   `:ok? true` for a dump missing its blobs. 39 tests caught it. A named constant
   cannot drift that way again."
  :datahike.migrate/blob-plan)
(def manifest-key :datahike.migrate/format-version)

(def ^:private source-config-allowlist
  "Source config recorded in the manifest, for diagnostics and for the
   compatibility check below."
  #{:attribute-refs? :keep-history? :schema-flexibility :index})

(def config-must-match
  "The subset of `source-config-allowlist` an import REFUSES to cross.

   `:index` is deliberately absent, and that absence used to be silent — the
   allowlist listed four keys and the check looped over three, so a reader had
   to diff them to discover the difference was intentional. It is: a dump holds
   datoms, and `load-entities` builds whatever index the target was created
   with, so importing a persistent-set dump into a hitchhiker-tree database is
   a supported thing to do. The other three change what the datoms MEAN."
  [:attribute-refs? :keep-history? :schema-flexibility])

;; ---------------------------------------------------------------------------
;; small helpers

(defn ->db [x] (if (dbu/db? x) x @x))

(defn attribute-refs? [db] (boolean (:attribute-refs? (dbi/-config db))))

(defn a-ident
  "Attribute as a keyword ident (resolve numeric refs in attribute-refs dbs)."
  [db a]
  (if (and (number? a) (attribute-refs? db)) (dbi/-ident-for db a) a))

(defn system-idents
  "Map of source system-entity eid -> ident, for attribute-refs dumps (else {})."
  [db]
  (if (attribute-refs? db)
    (into (sorted-map)
          (keep (fn [e] (when-let [i (dbi/-ident-for db e)] [e i])))
          (dbi/-system-entities db))
    {}))

;; ---------------------------------------------------------------------------
;; the records a dump contains

(defn datom->record
  "Convert a datom to an EDN-encodable record vector [e a v t added]. `a` becomes a
   keyword ident; ref values pointing at a system entity become #datahike/sysref."
  ([db sys-ents sys-idents datom] (datom->record db sys-ents sys-idents nil datom))
  ([db sys-ents sys-idents sec-read datom]
  (let [e (nth datom 0)
        a (a-ident db (nth datom 1))
        ;; For a `:db.secondary/only` attribute the primary holds a content hash,
        ;; so the real value comes from the secondary index instead — see
        ;; `secondary-only-values`. Falls back to the primary's value, which is
        ;; correct for a RETRACTION (those already carry the stored hash) and for
        ;; an entity the scan did not cover.
        ;; Read from the SECONDARY index for a `:db.secondary/only` attribute —
        ;; the primary holds a content hash. Falls back to the primary's value,
        ;; which is right for a RETRACTION (those already carry the stored hash)
        ;; and for an entity the index has no row for.
        v (or (when (and sec-read (nth datom 4)) (sec-read a (nth datom 0)))
              (nth datom 2))
        t (nth datom 3)
        op (nth datom 4)
        sysref? (fn [val] (when (and (dbu/ref? db a) (contains? sys-ents val))
                            (get sys-idents val)))]
    [e a (mcbor/encode-value v sysref?) t op])))

(defn secondary-only-reader
  "`nil` when the database has no `:db.secondary/only` attribute — the common
   case, one rschema lookup. Otherwise a `(fn [attr eid] -> value)` that reads
   the full value from the secondary index covering `attr`.

   ## Why export needs it

   For a `:db.secondary/only` attribute the primary EAVT/AEVT/AVET hold a CONTENT
   HASH — `project-primary` substitutes it on the way in — and the real value
   goes only to the secondary index. `export-records` reads the primary indexes,
   so a dump built from them carries the hash and the value is absent from the
   backup entirely. Measured before this existed: a round trip left
   `hasch(hasch(v))` in the restored primary and `v` was unrecoverable.

   With the real value in the dump the IMPORT needs no change: the transactor
   re-projects it, so the primary lands on `hasch(v)` (correct) and the secondary
   receives `v` (correct). The double-hash was a symptom of the missing value,
   not a separate defect.

   ## Why a function and not a map

   An earlier version read every `[eid value]` pair up front and held them in
   `{attr {eid value}}` for the whole export. That is every document of a
   full-text corpus resident at once — and `:db.secondary/only` exists precisely
   for values too large to want in the primary index. Everything else in the
   export path is bounded (`:chunk-size`, `:sort-buffer`); this would have been
   the single unbounded term. One value at a time is the same bound the record
   stream already has, at the price of a random read per such datom.

   ## Why it validates eagerly

   Scannability is checked HERE, before a single record is written, so an
   unsupported index refuses the export rather than failing partway through a
   dump that is already half on disk.

   Refusing at all is the point: an index whose values cannot be read cannot be
   backed up losslessly, and writing a dump we know is incomplete means the
   operator discovers it at RESTORE time. (An export-side transform would be the
   other way out — drop the attribute deliberately — but transforms are
   import-only today; `:xform` has no export counterpart.)"
  [db]
  (let [only-attrs (->> (:schema db)
                        (keep (fn [[k v]]
                                (when (and (keyword? k) (map? v) (:db.secondary/only v)) k)))
                        seq)]
    (when only-attrs
      (let [idx-for (reduce
                     (fn [acc attr]
                       (let [idx-idents (get-in db [:rschema :db.secondary/index attr])
                             idx (some (fn [i] (get (:secondary-indices db) i)) idx-idents)]
                         (when (nil? idx)
                           (throw (ex-info (str "Cannot export " attr ": it is :db.secondary/only, "
                                                "so its values live only in a secondary index, and "
                                                "no such index is loaded on this database.")
                                           {:error :export/secondary-only-unreadable
                                            :attribute attr})))
                         (when-not (satisfies? sec/ISecondaryScannable idx)
                           (throw (ex-info (str "Cannot export " attr " losslessly: it is "
                                                ":db.secondary/only, so its values live ONLY in the "
                                                "secondary index covering it, and that index does "
                                                "not implement ISecondaryScannable. The primary "
                                                "indexes hold a content hash, so a dump written "
                                                "from them would NOT contain this attribute's data "
                                                "— you would discover that when restoring. "
                                                "Refusing to write an incomplete backup.")
                                           {:error :export/secondary-only-unreadable
                                            :attribute attr
                                            :index-idents (vec idx-idents)})))
                         (assoc acc attr idx)))
                     {} only-attrs)]
        (fn [attr eid]
          (when-let [idx (get idx-for attr)]
            (sec/-sec-value idx attr eid)))))))

(defn export-records
  "Lazy seq of encoded record vectors for `db` (UNSORTED — the external merge sort
   in `export-db` imposes the `(t, txInstant-first, e, a)` order). Emits only
   user-transaction datoms (tx > tx0): this drops the bootstrap in attribute-refs
   dbs — system entities are already present in the target, so refs to them are
   translated (#508/#531) rather than re-inserted — and is a no-op for plain dbs,
   whose bootstrap never appears in :eavt."
  [db {:keys [history?]}]
  (let [src      (if history? (api/history db) db)
        sys-ents (if (attribute-refs? db) (dbi/-system-entities db) #{})
        sidents  (system-idents db)
        sec-read (secondary-only-reader db)]
    (->> (api/datoms src :eavt)
         (filter (fn [dm] (> (d/datom-tx dm) c/tx0)))
         (map (fn [dm] (datom->record db sys-ents sidents sec-read dm))))))

(defn export-records-streaming
  "Lazy seq of encoded records for `db` in a load-safe order WITHOUT sorting (so
   export needs no scratch space — for hard read-only / diskless targets). Two lazy
   passes over `:eavt`: first schema/ident and tx-entity (`meta-attr?`) datoms — so
   schema precedes the data that uses it (#262) and every tx entity (with its
   `:db/txInstant`) exists before any data datom — then the data datoms. Emits only
   user-transaction datoms (tx > tx0).

   Load-entities tolerates the remaining EAVT ordering: cross-tx and intra-tx
   forward refs allocate on first sight, and ids remap consistently. The one shape
   this does NOT preserve is a *same-transaction* card-one replacement (retract +
   re-assert of the same [e a] in one tx), which the sorting exporter orders
   explicitly; if a history has those, use the default (sorted) export."
  [db {:keys [history?]}]
  (let [src      (if history? (api/history db) db)
        sys-ents (if (attribute-refs? db) (dbi/-system-entities db) #{})
        sidents  (system-idents db)
        user?    (fn [dm] (> (d/datom-tx dm) c/tx0))
        schema-or-meta? (fn [dm]
                          (let [a (a-ident db (nth dm 1))]
                            (or (ds/schema-attr? a) (ds/meta-attr? a))))
        sec-read (secondary-only-reader db)
        make     (fn [dm] (datom->record db sys-ents sidents sec-read dm))]
    (concat
     (->> (api/datoms src :eavt) (filter user?) (filter schema-or-meta?) (map make))
     (->> (api/datoms src :eavt) (filter user?) (remove schema-or-meta?) (map make)))))

(defn ident-schema
  "The user schema as an ident->attr-map, keyed by keyword ident only (drops the
   numeric-eid mirror that attribute-refs dbs keep)."
  [db]
  (into {} (filter (fn [[k v]] (and (keyword? k) (map? v))) (:schema db))))

;; ---------------------------------------------------------------------------
;; capabilities — what it takes to READ a dump

(def ^:private base-capabilities
  "Capabilities every dump reader is assumed to have. Anything beyond these must
   be declared, so an older reader can refuse precisely."
  #{:datahike.migrate/cbor-seq})

(defn- dump-requires
  "The capability set needed to INTERPRET this dump.

   Version alone is too blunt for a dump. `connector/version-check` refuses a
   STORE written by a newer datahike, and that is right — the on-disk index is
   not forward compatible. A dump is different: it is logical, and a v3 dump that
   happens to use no v3-only feature is perfectly readable by v2. Refusing it on
   the version stamp would work against datahike's own commitment to backwards
   compatibility, while accepting a dump whose features we cannot represent would
   silently drop data. So the dump declares what it NEEDS, and the reader
   compares that against what it HAS.

   Value-type capabilities are derived from the schema rather than hand-listed:
   a type added in a later version appears here automatically, and an older
   reader — whose `ds/builtin-value-types` does not contain it — refuses by
   construction rather than by anyone remembering to update a table."
  [db {:keys [history?]} blob-plan]
  (into base-capabilities
        cat
        [(when history? [:datahike.migrate/history])
         (when (seq (:carried blob-plan)) [:datahike.migrate/store-ref-blobs])
         (when (seq (:external blob-plan)) [:datahike.migrate/external-blobs])
         (when (:attribute-refs? (dbi/-config db)) [:datahike.migrate/attribute-refs])
         ;; every declared value type in play
         (into #{}
               (keep (fn [[_ attr]] (:db/valueType attr)))
               (dbi/-schema db))]))

(def ^:private supported-capabilities
  "What THIS version can interpret. Derived, so it tracks the schema."
  (into (conj base-capabilities
              :datahike.migrate/history
              :datahike.migrate/store-ref-blobs
              :datahike.migrate/external-blobs
              :datahike.migrate/attribute-refs)
        ds/builtin-value-types))

(defn check-capabilities!
  "Raise unless every capability the dump declares is one we can honour.

   Names the specific missing capabilities: \"this dump requires
   :db.type/double-array\" is actionable, where \"version mismatch\" is not. A
   dump with no `:requires` predates the declaration and is read as before."
  [manifest]
  (when-let [required (:requires manifest)]
    (let [missing (remove supported-capabilities required)]
      (when (seq missing)
        (throw (ex-info (str "This dump requires capabilities this version of datahike "
                             "cannot interpret: " (pr-str (vec (sort missing)))
                             ". It was written by datahike "
                             (get-in manifest [:datahike/meta :datahike/version] "?")
                             "; upgrade to import it, or re-export from a database that "
                             "does not use these features.")
                        {:error :import/unsupported-capabilities
                         :missing (vec (sort missing))
                         :required (vec (sort required))
                         :supported (vec (sort supported-capabilities))})))))
  nil)

(defn build-manifest [db {:keys [history?] :as opts} digest chunks]
  (let [cfg (dbi/-config db)]
    (array-map
     manifest-key                    format-version
     :history?                       (boolean history?)
     :serialization                  :cbor-seq
     ;; Provenance in the SAME shape the store carries (`datahike.tools/meta-data`,
     ;; which `connector/version-check` enforces), so a dump and a store can be
     ;; reasoned about with one vocabulary.
     ;;
     ;; JVM only: `meta-data` is a macro over build-time version constants read
     ;; from the classpath, which has no ClojureScript counterpart. A dump written
     ;; from a browser or Node carries no provenance — diagnostics, not something
     ;; `check-capabilities!` decides on, which is exactly why the two are
     ;; separate keys.
     :datahike/meta                  #?(:clj (dt/meta-data) :cljs nil)
     ;; …and, separately, what is needed to READ this dump. See `dump-requires`:
     ;; provenance is for diagnostics, capabilities are for the accept/reject
     ;; decision, and conflating them is what makes a version stamp too blunt.
     :requires                       (vec (sort (dump-requires db opts (get opts blob-plan-key))))
     :source-config                  (into (array-map :store-backend (get-in cfg [:store :backend]))
                                           (map (fn [k] [k (get cfg k)]))
                                           (sort source-config-allowlist))
     :schema                         (ident-schema db)
     :system-idents                  (system-idents db)
     ;; :max-eid / :max-tx let an importer estimate the O(entities) id-remap map
     ;; (and thus the heap to give the import) without scanning the dump.
     :stats                          {:datom-count (:count digest)
                                      :max-eid     (:max-eid db)
                                      :max-tx      (:max-tx db)}
     :semantic-digest                digest
     ;; Which `:db.type/store-ref` blobs this dump carries, and which it could
     ;; not. A store-ref names an object without saying where the bytes live, so
     ;; a dump is self-contained only for blobs that were IN the source store;
     ;; anything held in a raw bucket the browser PUT to never transits our JVM
     ;; and cannot be copied. Recording both halves means an import can refuse a
     ;; dump whose referents it cannot place, instead of restoring datoms that
     ;; name objects which are not there.
     :store-refs                     (when-let [p (get opts blob-plan-key)]
                                       (mblobs/manifest-entry p))
     :chunks                         (vec chunks))))

;; ---------------------------------------------------------------------------
;; sizes and names

(def default-chunk-size
  "Datoms per chunk file, and therefore the import's per-chunk memory: a chunk is
   read whole (see `migrate/reduce-dump-records`), so this is the knob that
   bounds it.

   100k datoms is roughly 2.7 MB of records. The filesystem medium used to
   default to 1,000,000 — 25.7 MB — because it streamed within a chunk and never
   held one; the store medium already used 50,000 because it always did. Now
   that both read a chunk at a time the two defaults had no reason to differ, and
   the larger one was the outlier: at 100k entities a 25.7 MB chunk was FOUR
   TIMES the id-remap map, i.e. the dominant term, in exactly the small-heap
   setting where that hurts.

   Not smaller than this: chunks are independent gzip streams, so a chunk below
   ~1 MB starts losing ratio to dictionary resets (measured at +2.4% for 64 KiB
   blocks against +0.1% for 1 MiB)."
  100000)

(def default-sync?
  "Whether an entry point runs synchronously when the caller does not say.

   True on the JVM, false on ClojureScript — the same platform split
   `datahike.api`'s versioning operations use (`branches`, `branch!` pass
   `{:sync? true}` on clj and `{:sync? false}` on cljs). Existing JVM callers
   keep the blocking behaviour they have; a browser or Node caller gets a
   channel, because there is nothing else it could get."
  #?(:clj true :cljs false))

(defn assert-sync-supported!
  "Refuse `{:sync? true}` where nothing can block.

   ClojureScript has no blocking take and no blocking deref, so a synchronous
   import cannot be implemented there — `load-batch!` would have to `@` a
   `promise-chan`. Refusing by name beats failing inside the batcher with
   something about `IDeref`, and mirrors `api/transact`, which throws
   \"Synchronous transact not supported in ClojureScript, use transact!
   instead.\""
  [opts]
  #?(:clj (do opts nil)
     :cljs (when (:sync? opts)
             (throw (ex-info (str "Synchronous export/import is not supported in "
                                  "ClojureScript — there is no blocking take. Omit "
                                  ":sync? (the default here is false) and take from "
                                  "the returned channel.")
                             {:error :migrate/sync-not-supported})))))

(defn assert-sizes-positive!
  "Refuse non-positive window sizes.

   `:sort-buffer`, `:chunk-size` and `:batch-size` all drive a
   `take`/`drop` recurrence — `spill-runs`, `write-chunks!`, the import
   batcher. At zero every pass takes nothing, drops nothing and makes no
   progress, while still creating an empty run file or writing an empty chunk
   each time round, so the loop never terminates and the output grows without
   bound. A negative value does the same.

   Checked at the public entry rather than defended at each recurrence: there
   are three of them, and a caller who passes 0 wants to hear about it before
   the export starts, not to watch a directory fill."
  [opts]
  (doseq [k [:sort-buffer :chunk-size :batch-size]]
    (when-let [v (get opts k)]
      (when-not (and (integer? v) (pos? v))
        (throw (ex-info (str k " must be a positive integer, got " (pr-str v)
                             ". A non-positive window makes no progress and would "
                             "loop forever.")
                        {:error :migrate/bad-size :option k :value v}))))))

(defn validate-record!
  "Refuse a record that is not `[e a v t op]` with the right shapes. Throws;
   returns nil. ~65 ns, i.e. 0.3% of a streaming import — which is the whole
   argument for running it always rather than behind a flag.

   Applied AFTER sysref resolution and AFTER `:xform`, so it also catches a
   transducer that emits something malformed.

   Each clause is a measured silent corruption, not a hypothetical:

   `boolean? op` — the one that matters most. `op` is consumed by TRUTHINESS
     everywhere (`(if (nth r 4) …)`, six places across three namespaces), and in
     Clojure the integer `0` is truthy. A producer in another language emitting
     CBOR `0`/`1` — the natural encoding, and the documented seam invites foreign
     producers — therefore has EVERY RETRACTION SILENTLY ASSERTED. Measured on
     the index-build path: `:verified? true` over a database whose current state
     differs from the dump's.

   `some? v` — a nil value reaches the indexes on both paths. `validate-val`
     (which would reject it) is on the transact path only; `with-datom` calls
     `validate-datom`, which checks uniqueness and not much else.

   `integer? e` — a nil `e` is worse than it looks: `(or (migrated-eid m nil)
     max-eid)` allocates ONE id for the nil key and remembers it, so every
     nil-`e` record becomes an attribute of the SAME entity.

   `t >= tx0` — `record->datom` passes `t` straight to `dd/datom`'s 5-arity,
     which encodes `added` in the SIGN of tx. So `t=0` makes an assertion into a
     retraction sitting in the current index, and `0 < t < tx0` yields a
     plausible datom at a transaction id inside the entity id space. The
     streaming path is immune (it remaps `t` through `:tids` first); the index
     build is not.

   `keyword? a` and the arity check turn two unattributable crashes — a bare
     `IndexOutOfBoundsException` from the batcher, a `ClassCastException` from
     inside `cmp_attr_quick` — into an error naming the record."
  [record]
  (letfn [(bad! [why]
            (throw (ex-info (str "Malformed dump record: " why ". Records are "
                                 "[e a v t op] with e/t integers, a a keyword ident, "
                                 "v non-nil and op a boolean.")
                            {:error :import/malformed-record
                             :reason why
                             :record record})))]
    (when-not (and (vector? record) (= 5 (count record))) (bad! "not a 5-element vector"))
    (let [[e a v t op] record]
      (when-not (integer? e) (bad! (str "e is not an integer: " (pr-str e))))
      (when-not (keyword? a) (bad! (str "a is not a keyword ident: " (pr-str a))))
      (when (nil? v) (bad! "v is nil"))
      (when-not (integer? t) (bad! (str "t is not an integer: " (pr-str t))))
      (when (< (long t) (long c/tx0))
        (bad! (str "t " t " is below tx0 (" c/tx0 "); dd/datom encodes `added` in "
                   "the sign of tx, so such a t cannot mean what it says")))
      (when-not (boolean? op)
        (bad! (str "op is not a boolean: " (pr-str op)
                   " — op is read by truthiness, so a non-boolean (CBOR 0/1 from a "
                   "foreign producer) silently asserts retractions"))))))

(defn assert-codec-supported!
  "Refuse a `:compression` this version cannot write.

   `mz/supported` was documented as the READ guard — a dump naming an unknown
   codec is refused by name rather than failing inside the decoder. Writing had
   no such check, so the value reached `mz/compress-bytes`, a `case` with no
   default clause, and surfaced as `No matching clause: :zstd` from inside the
   block compressor. By then the first chunk's `.tmp` file was already on disk.

   `nil` is rejected explicitly rather than treated as \"use the default\":
   `export-db` merges its defaults BEFORE this runs, so a caller who passed
   `{:compression nil}` meaning \"whatever you normally do\" has overridden the
   default with nil, and `No matching clause: ` — with nothing after the colon —
   is the worst possible way to find that out."
  [opts]
  (when (contains? opts :compression)
    (let [v (:compression opts)]
      (when-not (contains? mz/supported v)
        (throw (ex-info (str ":compression " (pr-str v) " is not supported for writing. "
                             "This version writes " (pr-str (sort mz/supported)) ".")
                        {:error :migrate/unsupported-codec
                         :codec v
                         :supported mz/supported}))))))

(def default-batch-size
  "Datoms per `load-entities` call. One definition — it had three, two of them
   spelled `(:batch-size (merge {:batch-size 100000} opts))`."
  100000)

(def chunk-re
  "A chunk file name. The optional suffix is the compression codec's, so a dump
   directory says what it holds — `datoms-000001.cbor.gz` is a gzip file to every
   tool on the machine.

   SIX OR MORE digits, not exactly six. `chunk-name` pads to six and then lets
   the number grow, so chunk 1000000 is `datoms-1000000.cbor` — which this
   rejected while the writer happily produced it. Export succeeded and import
   failed on its own output, with \"Illegal chunk file name in manifest\".
   Unreachable at the default `:chunk-size` (it would need 10^12 datoms) but
   `:chunk-size` is a caller option, and a small one reaches it.

   Do not sort chunk files by name past that point — `datoms-1000000` orders
   before `datoms-999999` lexicographically. Nothing does: the manifest's
   `:chunks` is an ordered vector and that is the authority."
  #"^datoms-\d{6,}\.cbor(\.gz)?$")

(defn assert-dump-manifest!
  "Guards that a manifest must pass before ANY chunk of it is read, on EVERY
   medium. Throws; returns nil.

   ## Why this is here and not in the filesystem reader

   These three checks existed only on the filesystem path, inside `manifest-of`
   and `open-dump`. The konserve-store path read its manifest with a bare `k/get`
   and proceeded, so every one of them was absent for exactly the deployment that
   most needs them — an S3 backup an operator cannot inspect by hand. Measured
   before the fix, on a store dump:

     * a chunk entry with `:sha256` deleted → tampered bytes imported,
       `{:datom-count 15, :verified? true, :errors []}`, and `verify` `:ok? true`.
       The same two edits to a filesystem dump threw `:import/missing-checksum`.
     * a MISSING manifest key → `k/get` returns nil, every guard no-ops on nil,
       `expected` and `live` are both 0, so the count check compared 0 against 0
       and passed. An import of nothing reported success. Since the manifest is
       written LAST as the commit marker, that is the normal shape of an export
       that died midway — not operator error.

   Keeping the rule in one medium-independent place is the point: the filesystem
   fix was written, documented, and then not carried across, and nothing failed.

   `:checksums :skip` is the only way past the hash requirement, and the caller
   warns when it is used."
  [manifest source opts]
  (when (nil? manifest)
    (throw (ex-info (str "Not a datahike dump: " (pr-str source)
                         " has no manifest. A dump's manifest is written LAST as its"
                         " commit marker, so a source without one is either not a dump"
                         " or an export that did not finish.")
                    {:error :import/not-a-dump :source source})))
  (when-not (contains? manifest manifest-key)
    (throw (ex-info (str "Not a datahike dump: " (pr-str source) " has a manifest with no "
                         manifest-key ", so it was not written by datahike's export.")
                    {:error :import/not-a-dump :source source
                     :manifest-keys (vec (sort (keys manifest)))})))
  (doseq [{:keys [file sha256]} (:chunks manifest)]
    (when-not (and (string? file) (re-matches chunk-re (str file)))
      (throw (ex-info (str "Dump manifest names a chunk this version will not read: "
                           (pr-str file))
                      {:error :import/bad-chunk-path :file file :source source})))
    (when (and (nil? sha256) (not= :skip (:checksums opts)))
      (throw (ex-info (str "Dump manifest has no :sha256 for chunk " (pr-str file)
                           ". Integrity checking fails CLOSED: a chunk with no declared"
                           " hash is refused rather than treated as unhashed. Pass"
                           " {:checksums :skip} to import it anyway.")
                      {:error :import/missing-checksum :file file :source source})))))

(def chunk-name
  "One spelling for both media — see `datahike.migrate.store/chunk-name`."
  mstore/chunk-name)

(defn read-manifest-map [s]
  ;; The manifest is plain EDN — no #datahike/* tags survive the move to CBOR,
  ;; but an unknown tag still degrades to a tagged-literal rather than throwing,
  ;; so a manifest written by a NEWER datahike stays readable far enough to reach
  ;; `check-capabilities!` and produce its precise refusal.
  (edn/read-string {:default (fn [t v] (tagged-literal t v))} s))

(defn codec-of
  "The compression codec a manifest declares. `:none` when absent — dumps written
   before compression existed have no such key and are stored verbatim."
  [manifest]
  (let [c (get manifest :compression :none)]
    (mz/check-supported! c)
    c))

;; ---------------------------------------------------------------------------
;; memory estimation — tell the user how much heap to give an import

(defn- round1
  "One decimal place, without `format` — which ClojureScript does not have."
  [x]
  (/ (#?(:clj Math/round :cljs js/Math.round) (* 10.0 (double x))) 10.0))

(defn- round0 [x] (#?(:clj Math/round :cljs js/Math.round) (double x)))

(defn bytes->human [b]
  (let [b (double b)]
    (cond
      (>= b 1073741824) (str (round1 (/ b 1073741824.0)) " GB")
      (>= b 1048576)    (str (round0 (/ b 1048576.0)) " MB")
      :else             (str (round0 (/ b 1024.0)) " KB"))))

(def ^:private idmap-bytes-per-entry
  "Conservative heap cost of one source->target id-map entry (boxed longs + Clojure
   persistent-map node overhead)."
  64)

(defn- max-heap
  "The runtime's heap ceiling, or nil where there is no such notion.

   JVM only, deliberately. Node reports `process.memoryUsage()` and a browser
   reports nothing comparable, and inventing a number for them would make
   `:sufficient?` a guess wearing the same name as a fact."
  []
  #?(:clj (.maxMemory (Runtime/getRuntime)) :cljs nil))

(defn estimate-from-manifest
  "Estimate from a manifest and the dump's total byte size (medium-agnostic)."
  [manifest total-bytes batch-size]
  (let [stats    (:stats manifest)
        datoms   (long (or (:datom-count stats) 0))
        entities (long (or (:max-eid stats) datoms 0))
        txs      (long (max 0 (- (long (or (:max-tx stats) c/tx0)) c/tx0)))
        ;; UNCOMPRESSED bytes per record. `total-bytes` is what is stored, which
        ;; with gzip on is ~7x smaller than what a batch or a chunk occupies in
        ;; memory; `:raw-bytes` is recorded per chunk precisely so this term does
        ;; not silently shrink when someone turns compression on. Dumps written
        ;; before `:raw-bytes` existed fall back to the stored size.
        raw-total (reduce + 0 (map (fn [c] (long (or (:raw-bytes c) (:bytes c) 0)))
                                   (:chunks manifest)))
        avg-rec  (cond
                   (and (pos? datoms) (pos? raw-total)) (/ (double raw-total) datoms)
                   (pos? datoms)                        (/ (double total-bytes) datoms)
                   :else                                64.0)
        idmap    (long (* (+ entities txs) idmap-bytes-per-entry))
        ;; a batch plus the tx-report / index-delta churn it drives (~3x)
        batch    (long (* batch-size avg-rec 3))
        ;; ...and the CHUNK held while it is decoded. `reduce-dump-records` reads
        ;; one chunk at a time, so this is a live term and it was missing: below
        ;; about a million entities it is the LARGEST of the three, which is
        ;; precisely the small-heap case the estimate exists to warn about. Taken
        ;; from the manifest's own chunk records rather than from a default,
        ;; since the dump states what it actually used. Compressed chunks also
        ;; hold the compressed copy briefly; `:bytes` covers that.
        chunk    (long (reduce max 0 (map (fn [c] (+ (long (or (:bytes c) 0))
                                                     (long (or (:raw-bytes c)
                                                               (* (or (:count c) 0) avg-rec)))))
                                          (:chunks manifest))))
        ;; What this import actually needs...
        required (long (* 1.6 (+ idmap batch chunk)))
        ;; ...and what to ASK for, which is never less than a working heap. The
        ;; floor belongs to the advice, not to the test: `:sufficient?` used to
        ;; compare the heap against `recommend`, so a three-datom import
        ;; "needed" 512 MB and any JVM below that got a stderr warning about a
        ;; dump it could import a thousand times over. A warning that fires on
        ;; trivial input is a warning people learn to ignore.
        recommend (max (* 512 1024 1024) required)
        maxheap  (max-heap)]
    (cond-> {:datoms datoms
             :entities entities
             :id-map-bytes idmap
             :batch-bytes batch
             :chunk-bytes chunk
             :required-heap-bytes required
             :required-heap (bytes->human required)
             :recommended-heap-bytes recommend
             :recommended-heap (bytes->human recommend)}
      ;; nil where the runtime has no heap ceiling to report — see `max-heap`.
      maxheap (assoc :current-max-heap-bytes maxheap
                     :current-max-heap (bytes->human maxheap)
                     :sufficient? (>= maxheap required)))))

;; ---------------------------------------------------------------------------
;; the tier-2 fingerprint: id-independent, so a dump compares to a live database

(defn norm-val
  "Stably hashable form of a value: array/bytes values compare structurally rather
   than by identity, while keeping their class distinct."
  [v]
  #?(:clj
     (cond
       (bytes? v)                              [:bytes (vec v)]
       (instance? (Class/forName "[F") v)      [:farray (vec v)]
       (instance? (Class/forName "[D") v)      [:darray (vec v)]
       :else                                   v)
     :cljs
     ;; The same three classes, spelled as the typed arrays ClojureScript uses
     ;; for them — `:db.type/bytes`, `:db.type/float-array`, `:db.type/double-array`.
     (cond
       (instance? js/Uint8Array v)             [:bytes (vec v)]
       (instance? js/Float32Array v)           [:farray (vec v)]
       (instance? js/Float64Array v)           [:darray (vec v)]
       :else                                   v)))
