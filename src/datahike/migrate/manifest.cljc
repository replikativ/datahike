(ns ^:no-doc datahike.migrate.manifest
  "Everything about a dump that is a pure function of a database or a manifest:
   what a dump DECLARES, what it takes to read one, which records it contains,
   and how much heap importing it will want.

   Extracted from `datahike.migrate` because none of it does IO, and therefore
   none of it needed the async treatment the orchestration does. That split is
   worth having on its own terms — this is the part with the rules in it, and it
   is now testable without a filesystem or a store — but the immediate reason is
   that it made the portable/non-portable boundary visible instead of implied.

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
  [db sys-ents sys-idents datom]
  (let [e (nth datom 0)
        a (a-ident db (nth datom 1))
        v (nth datom 2)
        t (nth datom 3)
        op (nth datom 4)
        sysref? (fn [val] (when (and (dbu/ref? db a) (contains? sys-ents val))
                            (get sys-idents val)))]
    [e a (mcbor/encode-value v sysref?) t op]))

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
        sidents  (system-idents db)]
    (->> (api/datoms src :eavt)
         (filter (fn [dm] (> (d/datom-tx dm) c/tx0)))
         (map (fn [dm] (datom->record db sys-ents sidents dm))))))

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
        make     (fn [dm] (datom->record db sys-ents sidents dm))]
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

(def default-batch-size
  "Datoms per `load-entities` call. One definition — it had three, two of them
   spelled `(:batch-size (merge {:batch-size 100000} opts))`."
  100000)

(def chunk-re
  "A chunk file name. The optional suffix is the compression codec's, so a dump
   directory says what it holds — `datoms-000001.cbor.gz` is a gzip file to every
   tool on the machine."
  #"^datoms-\d{6}\.cbor(\.gz)?$")

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
