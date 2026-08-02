(ns ^:no-doc datahike.migrate
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
            [datahike.constants :as c]
            [datahike.datom :as d]
            [datahike.db.interface :as dbi]
            [datahike.db.utils :as dbu]
            [datahike.schema :as ds]
            [datahike.tools :as dt]
            [datahike.migrate.cbor :as mcbor]
            [datahike.migrate.digest :as dig]
            [datahike.migrate.compress :as mz]
            [datahike.migrate.fs :as fs]
            [datahike.migrate.sort :as msort]
            [datahike.migrate.store :as mstore]
            [datahike.migrate.blobs :as mblobs]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.core.async :as async]
            [konserve.core :as k]
            [konserve.binary :as kb]
            [superv.async :refer [<?? S]])
  (:import [datahike.migrate.cbor SysRef]))

(def format-version 1)
(def ^:private manifest-key :datahike.migrate/format-version)

(def ^:private source-config-allowlist
  "Source config recorded in the manifest, for diagnostics and for the
   compatibility check below."
  #{:attribute-refs? :keep-history? :schema-flexibility :index})

(def ^:private config-must-match
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

(defn- ->db [x] (if (dbu/db? x) x @x))

(defn- attribute-refs? [db] (boolean (:attribute-refs? (dbi/-config db))))

(defn- a-ident
  "Attribute as a keyword ident (resolve numeric refs in attribute-refs dbs)."
  [db a]
  (if (and (number? a) (attribute-refs? db)) (dbi/-ident-for db a) a))

(defn- system-idents
  "Map of source system-entity eid -> ident, for attribute-refs dumps (else {})."
  [db]
  (if (attribute-refs? db)
    (into (sorted-map)
          (keep (fn [e] (when-let [i (dbi/-ident-for db e)] [e i])))
          (dbi/-system-entities db))
    {}))

;; ---------------------------------------------------------------------------
;; export

(defn- datom->record
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

(defn- export-records
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

(defn- export-records-streaming
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

(defn- ident-schema
  "The user schema as an ident->attr-map, keyed by keyword ident only (drops the
   numeric-eid mirror that attribute-refs dbs keep)."
  [db]
  (into {} (filter (fn [[k v]] (and (keyword? k) (map? v))) (:schema db))))

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

(defn- build-manifest [db {:keys [history?] :as opts} digest chunks]
  (let [cfg (dbi/-config db)]
    (array-map
     manifest-key                    format-version
     :history?                       (boolean history?)
     :serialization                  :cbor-seq
     ;; Provenance in the SAME shape the store carries (`datahike.tools/meta-data`,
     ;; which `connector/version-check` enforces), so a dump and a store can be
     ;; reasoned about with one vocabulary.
     :datahike/meta                  (dt/meta-data)
     ;; …and, separately, what is needed to READ this dump. See `dump-requires`:
     ;; provenance is for diagnostics, capabilities are for the accept/reject
     ;; decision, and conflating them is what makes a version stamp too blunt.
     :requires                       (vec (sort (dump-requires db opts (::blob-plan opts))))
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
     :store-refs                     (when-let [p (::blob-plan opts)]
                                       (mblobs/manifest-entry p))
     :chunks                         (vec chunks))))

(def ^:private default-batch-size
  "Datoms per `load-entities` call. One definition — it had three, two of them
   spelled `(:batch-size (merge {:batch-size 100000} opts))`."
  100000)

(def ^:private chunk-re
  "A chunk file name. The optional suffix is the compression codec's, so a dump
   directory says what it holds — `datoms-000001.cbor.gz` is a gzip file to every
   tool on the machine."
  #"^datoms-\d{6}\.cbor(\.gz)?$")

(def ^:private chunk-name
  "One spelling for both media — see `datahike.migrate.store/chunk-name`."
  mstore/chunk-name)

(defn- write-chunk-stream!
  "Write up to `limit` records from the (lazy) seq `records` to `f` as a CBOR
   sequence, updating the semantic-digest accumulator `dacc` and computing the
   chunk SHA-256 incrementally — O(1) memory regardless of chunk size. Returns
   [remaining-records count sha256-hex dacc'].

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
      (loop [rs (seq records) c 0 da dacc block [] bn 0]
        (if (and rs (< c limit))
          (let [bs (mcbor/encode-record (first rs))
                len (alength ^bytes bs)
                bn' (+ (long bn) (long len))
                block' (conj block bs)]
            (dig/sha256-update! md bs)
            (if (>= bn' mz/default-block-size)
              (do (flush-block! block' bn')
                  (recur (next rs) (inc c) (dig/add-record da bs) [] 0))
              (recur (next rs) (inc c) (dig/add-record da bs) block' bn')))
          (do (flush-block! block bn)
              [rs c (dig/sha256-finalize md) da])))
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
      (let [manifest (assoc (build-manifest db opts (dig/finalize dacc) chunks)
                            :compression codec)]
        (fs/spit-text! (fs/join dir "manifest.edn") (pr-str manifest))
        (fs/restrict-perms! (fs/join dir "manifest.edn") false)
        (progress {:phase :done :datoms (:count (dig/finalize dacc))})
        manifest)
      (let [fname (str (chunk-name n) (mz/extension codec))
            tmp   (fs/join dir (str fname ".tmp"))
            final (fs/join dir fname)
            [rem cnt sha dacc'] (write-chunk-stream! tmp ls chunk-size dacc codec)]
        (when-not (fs/rename! tmp final)
          (throw (ex-info (str "Could not move the finished chunk into place: "
                               tmp " -> " final ". The manifest would name a file "
                               "that is not there.")
                          {:error :export/rename-failed :from (str tmp) :to (str final)})))
        (fs/restrict-perms! final false)
        (progress {:phase :chunk :datoms cnt})
        (recur (seq rem) (inc n)
               (conj chunks {:file fname :count cnt :bytes (fs/file-size final) :sha256 sha})
               dacc'))))))

(defn- blob-dir
  "The directory carried blobs live in for a filesystem dump."
  [target]
  (fs/join target mblobs/dir-name))

(defn- with-blob-writer
  "Call `f` with `(fn [id bytes])` writing one blob into `target`'s blob area,
   releasing the medium afterwards.

   Takes a callback rather than returning the function because a store target
   has to be OPENED, and the previous shape — return a closure over an open
   store — had nowhere to close it. `blob-writer`, `blob-reader` and
   `verify-blobs` each leaked one connection per call, so an export plus an
   import plus a verify against a `{:backend ...}` config opened three stores and
   released none."
  [target f]
  (if (mstore/store-target? target)
    (let [m (mstore/open target)]
      (try
        (f (fn [id bytes]
             (<?? S (k/bassoc (:store m) (mstore/blob-key (:prefix m) id) bytes))))
        (finally (mstore/close m))))
    (let [dir (blob-dir target)]
      (when-not (fs/mkdirs! dir)
        (when-not (fs/directory? dir)
          (throw (ex-info (str "Could not create the blob directory " dir)
                          {:error :export/blob-dir-failed :dir (str dir)}))))
      (f (fn [id bytes]
           (let [sink (fs/open-sink (fs/join dir (str id)))]
             (try (fs/write! sink bytes) (finally (fs/close-sink! sink)))))))))

(defn- with-blob-reader
  "Call `f` with `(fn [id]) -> bytes-or-nil` reading blobs out of a dump,
   releasing the medium afterwards. See `with-blob-writer`."
  [source f]
  (if (mstore/store-target? source)
    (let [m (mstore/open source)]
      (try
        ;; `to-bytes` rather than a hand-rolled callback: `bget`'s handle has four
        ;; different shapes across backends and platforms, and that knowledge now
        ;; lives in konserve (replikativ/konserve#162) instead of being re-derived
        ;; at every call site — this was one of them.
        (f (fn [id]
             (<?? S (k/bget (:store m) (mstore/blob-key (:prefix m) id)
                            (kb/to-bytes {:sync? false})))))
        (finally (mstore/close m))))
    (f (fn [id]
         (let [f' (fs/join (blob-dir source) (str id))]
           (when (fs/exists? f') (fs/read-bytes f')))))))

(defn export-db
  "Export a database (or connection) to `target`.

   Writes a DIRECTORY: `manifest.edn`, `datoms-NNNNNN.cbor`, and `store-refs/`
   when the database has `:db.type/store-ref` blobs. Opts:
     :history?     false        include full history (asserts+retracts+tx entities)
     :compression  :gzip         :gzip or :none. gzip is ~7x on a real dump and
                                needs no dependency on any runtime; see
                                `datahike.migrate.compress` for why not zstd.
     :chunk-size   1000000      datoms per chunk file (50000 for store targets —
                                a store chunk is held in memory as one value)
     :sort-buffer  1000000      datoms per in-memory external-sort run
     :sort?        true         false ⇒ no-scratch streaming order (see below)
     :progress-fn  nil          (fn [{:keys [phase datoms]}])
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
  ([db-or-conn target] (export-db db-or-conn target {}))
  ([db-or-conn target opts]
   (let [db       (->db db-or-conn)
         opts     (merge {:history? (boolean (:keep-history? (dbi/-config db)))
                          ;; A store chunk is one konserve value, materialized as a
                          ;; single string — so the store default is much smaller
                          ;; than the filesystem default (which streams line by
                          ;; line and never holds a chunk in memory).
                          :chunk-size (if (mstore/store-target? target) 50000 1000000)
                          :sort-buffer 1000000
                          :compression mz/default-codec
                          :sort? true}
                         opts)
         ;; Only walk for blobs when the schema can actually have them. Two
         ;; reasons, and the second is not an optimisation:
         ;;   * `reachable-store-refs` is a full reachability mark — pointless
         ;;     when no attribute is a `:db.type/store-ref`;
         ;;   * it walks index ADDRESSES, so it requires a flushed index and
         ;;     raises "Index needs to be properly flushed before marking" on an
         ;;     unflushed in-memory db. Plenty of legitimate exports are of such
         ;;     a db (`db-with`, a `:memory` store mid-test), and those cannot
         ;;     hold in-store blobs anyway.
         blob-plan (when (mblobs/schema-has-store-refs? db)
                     (mblobs/plan db (:store db)))
         opts     (cond-> opts
                    (seq (:carried blob-plan)) (assoc ::blob-plan blob-plan)
                    (seq (:external blob-plan)) (assoc ::blob-plan blob-plan))
         progress (or (:progress-fn opts) (constantly nil))
         write-to! (fn [lines]
                     (cond
                       ;; external store (konserve): S3 / S3-compatible / JDBC / ...
                       (mstore/store-target? target)
                       (let [m (mstore/open target)]
                         (try
                           (mstore/write-chunks! m lines (:chunk-size opts)
                                                 (fn [digest chunks] (build-manifest db opts digest chunks))
                                                 progress {:sync? true}
                                                 (get opts :compression mz/default-codec))
                           (finally (mstore/close m))))

                       :else
                       (write-chunked! db opts target lines (:chunk-size opts) progress)))]
     ;; Blob carriage. Planned BEFORE anything is written so the manifest can
     ;; declare it, and the bytes are written before the manifest — which is the
     ;; commit marker — so a dump that has a manifest has its blobs. Same
     ;; ordering the konserve-sync walker needs when it ships blobs ahead of the
     ;; branch head: nothing may name an object that is not there yet.
     (when-let [plan (::blob-plan opts)]
       (with-blob-writer target #(mblobs/copy-out! (:store db) plan %)))
     (if (:sort? opts)
       (let [records (export-records db opts)
             tmp-dir (fs/temp-dir! "dh-export")]
         (try
           ;; `msort` still takes a java.io.File: the external sort is the one
           ;; part of the dump path that has not been made portable yet (its
           ;; k-way merge is a lazy seq over open files, which cannot pull from
           ;; async IO). A portable export therefore means `:sort? false`.
           (write-to! (msort/external-sort records (:sort-buffer opts)
                                           (clojure.java.io/file tmp-dir)))
           (finally
             (doseq [n (or (fs/list-names tmp-dir) [])]
               (fs/delete! (fs/join tmp-dir n)))
             (fs/delete! tmp-dir))))
       ;; no-scratch streaming: no temp dir, no sort
       (write-to! (export-records-streaming db opts))))))

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

(defn- codec-of
  "The compression codec a manifest declares. `:none` when absent — dumps written
   before compression existed have no such key and are stored verbatim."
  [manifest]
  (let [c (get manifest :compression :none)]
    (mz/check-supported! c)
    c))

(defn- read-manifest-map [^String s]
  ;; The manifest is plain EDN — no #datahike/* tags survive the move to CBOR,
  ;; but an unknown tag still degrades to a tagged-literal rather than throwing,
  ;; so a manifest written by a NEWER datahike stays readable far enough to reach
  ;; `check-capabilities!` and produce its precise refusal.
  (edn/read-string {:default (fn [t v] (tagged-literal t v))} s))

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
      (let [manifest (read-manifest-map (fs/slurp-text (fs/join f "manifest.edn")))
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
   before any import touches the database."
  [source]
  (let [dump (manifest-of source)]
    (when-not (:legacy? dump)
      (doseq [{:keys [file sha256]} (:chunks (:manifest dump))
              :when sha256
              :let [cf (validate-chunk-file (str source) file)]]
        (when (not= sha256 (sha256-of-chunk cf (codec-of (:manifest dump))))
          (throw (ex-info (str "Checksum mismatch for chunk " file)
                          {:error :import/checksum-failed :file file})))))
    dump))

;; ---------------------------------------------------------------------------
;; memory estimation — tell the user how much heap to give an import

(defn- bytes->human [b]
  (let [b (double b)]
    (cond
      (>= b 1073741824) (format "%.1f GB" (/ b 1073741824.0))
      (>= b 1048576)    (format "%.0f MB" (/ b 1048576.0))
      :else             (format "%.0f KB" (/ b 1024.0)))))

(def ^:private idmap-bytes-per-entry
  "Conservative heap cost of one source->target id-map entry (boxed longs + Clojure
   persistent-map node overhead)."
  64)

(defn- estimate-from-manifest
  "Estimate from a manifest and the dump's total byte size (medium-agnostic)."
  [manifest total-bytes batch-size]
  (let [stats    (:stats manifest)
        datoms   (long (or (:datom-count stats) 0))
        entities (long (or (:max-eid stats) datoms 0))
        txs      (long (max 0 (- (long (or (:max-tx stats) c/tx0)) c/tx0)))
        avg-rec  (if (pos? datoms) (/ (double total-bytes) datoms) 64.0)
        idmap    (long (* (+ entities txs) idmap-bytes-per-entry))
        ;; a batch plus the tx-report / index-delta churn it drives (~3x)
        batch    (long (* batch-size avg-rec 3))
        ;; What this import actually needs...
        required (long (* 1.6 (+ idmap batch)))
        ;; ...and what to ASK for, which is never less than a working heap. The
        ;; floor belongs to the advice, not to the test: `:sufficient?` used to
        ;; compare the heap against `recommend`, so a three-datom import
        ;; "needed" 512 MB and any JVM below that got a stderr warning about a
        ;; dump it could import a thousand times over. A warning that fires on
        ;; trivial input is a warning people learn to ignore.
        recommend (max (* 512 1024 1024) required)
        maxheap  (.maxMemory (Runtime/getRuntime))]
    {:datoms datoms
     :entities entities
     :id-map-bytes idmap
     :batch-bytes batch
     :required-heap-bytes required
     :required-heap (bytes->human required)
     :recommended-heap-bytes recommend
     :recommended-heap (bytes->human recommend)
     :current-max-heap-bytes maxheap
     :current-max-heap (bytes->human maxheap)
     :sufficient? (>= maxheap required)}))

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

   The dominant, unavoidable term is the O(entities) id-remap map that
   `load-entities` holds until `finalize-import!`; the rest is one `:batch-size`
   worth of records. Returns e.g.
     {:datoms .. :entities .. :id-map-bytes .. :batch-bytes ..
      :recommended-heap-bytes .. :recommended-heap \"2.1 GB\"
      :current-max-heap-bytes .. :current-max-heap \"512 MB\" :sufficient? bool}"
  ([source] (estimate-import-memory source {}))
  ([source opts]
   (let [batch-size (get opts :batch-size default-batch-size)]
     (if (mstore/store-target? source)
       (let [m (mstore/open source)]
         (try
           (let [manifest (mstore/read-manifest m)]
             (estimate-from-manifest manifest (manifest-total-bytes manifest nil) batch-size))
           (finally (mstore/close m))))
       (let [{:keys [manifest files]} (manifest-of source)]
         (estimate-from-manifest manifest (manifest-total-bytes manifest files) batch-size))))))

(defn- reduce-dump-records
  "Reduce `rf` over every record of the dump, with each file's stream scoped to
   its inner reduction (no lazy seq escapes an open handle). Flat dumps skip the
   manifest header line first. Returns the final accumulator."
  [{:keys [files manifest]} rf init]
  (let [codec (codec-of manifest)]
    (reduce (fn [acc file]
              (if (= :none codec)
                ;; verbatim: stream inside the chunk too
                (let [{:keys [pull close]} (fs/puller file)]
                  (try
                    (reduce rf acc (mcbor/decode-records-pulled pull))
                    (finally (close))))
                (reduce rf acc (mcbor/decode-records-from (chunk-bytes file codec)))))
            init
            files)))

;; ---------------------------------------------------------------------------
;; import

(defn- resolve-sysrefs
  "Replace any SysRef value in a record with the target's system-entity eid."
  [db record]
  (let [v (nth record 2)]
    (if (instance? SysRef v)
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
   double-apply anything. Returns the errors collected."
  [conn batch progress]
  (try
    @(api/load-entities conn batch)
    (progress {:phase :batch :datoms (count batch)})
    []
    (catch Exception _batch-ex
      (vec
       (mapcat
        (fn [tx-group]
          (let [tx-group (vec tx-group)]
            (try
              @(api/load-entities conn tx-group)
              []
              (catch Exception _tx-ex
                (reduce (fn [errs d]
                          (try @(api/load-entities conn [d]) errs
                               (catch Exception ex
                                 (conj errs {:error (or (:error (ex-data ex)) :import/corrupt-datom)
                                             :datom d :message (ex-message ex)}))))
                        [] tx-group)))))
        (partition-by #(nth % 3) batch))))))

(defn- flush-batch!
  "Apply one tx-aligned batch via load-entities. Under :on-error :abort a failure
   throws; under :collect the offending datoms are skipped and returned as errors
   (per-datom granularity, see `collect-apply!`)."
  [conn batch on-error progress]
  (if (seq batch)
    (if (= :collect on-error)
      (collect-apply! conn batch progress)
      (try
        @(api/load-entities conn batch)
        (progress {:phase :batch :datoms (count batch)})
        []
        (catch Exception ex
          (throw (ex-info (str "Import aborted: " (ex-message ex))
                          (merge (ex-data ex) {:error :import/corrupt-datom}) ex)))))
    []))

(defn- config-compat! [manifest conn]
  (let [tgt (dbi/-config @conn)
        src (:source-config manifest)]
    (doseq [k config-must-match]
      (when (and (contains? src k) (not= (get src k) (get tgt k)))
        (throw (ex-info (str "Config mismatch on " k
                             ": dump=" (get src k) " target=" (get tgt k))
                        {:error :import/config-mismatch :key k
                         :expected (get src k) :actual (get tgt k)}))))))

(declare import-db-legacy finalize-import!)

(defn- check-target!
  "Refuse an import this target cannot accept — BEFORE anything is written.

   Split out of `run-import`, which performed these checks under a comment
   reading \"guard rails (all before touching the db)\" while `import-db` had
   already called `restore-blobs!`. Importing a blob-carrying dump into a
   non-empty database therefore wrote every blob into the target store and only
   then refused. The objects are content-addressed and unreferenced, so the GC
   reclaims them eventually, but the refusal should come first — and on a large
   blob set that is gigabytes written to be told the target was never eligible."
  [conn manifest]
  (when-let [fv (get manifest manifest-key)]
    (when (> fv format-version)
      (throw (ex-info (str "Unsupported dump format-version " fv)
                      {:error :import/format-version :version fv}))))
  (config-compat! manifest conn)
  (when (pos? (user-datom-count @conn))
    (throw (ex-info "Target database is not empty; import is not resumable — recreate and restart."
                    {:error :import/non-empty-target}))))

(defn- run-import
  "Medium-agnostic import core. `reduce-lines` is (fn [rf init] -> acc) that streams
   the dump's record lines with resource scoping (filesystem or store). `mem` is the
   memory estimate. Handles guards, attribute-refs seeding, the tx-aligned batcher,
   verification, finalization, and the report."
  [conn manifest mem reduce-lines opts]
  (let [opts     (merge {:batch-size default-batch-size :verify? true
                         :on-error :abort :finalize? true} opts)
        progress (or (:progress-fn opts) (constantly nil))
        translate (:translate opts)
        batch-size (:batch-size opts)
        on-error (:on-error opts)]
    ;; ---- heap preflight: tell the operator how much RAM to give this ----
    (when-not (:sufficient? mem)
      (binding [*out* *err*]
        (println (format "[datahike.migrate] heap warning: importing %d datoms (~%d entities) needs about %s; this JVM's -Xmx is %s. Raise -Xmx (the id-remap map is held until finalize) or expect OutOfMemoryError."
                         (:datoms mem) (:entities mem)
                         (:recommended-heap mem) (:current-max-heap mem)))))
    ;; Guard rails already ran in `check-target!`, before any blob was written.
    ;; ---- attribute-refs: seed system-entity identity so refs to system
    ;; entities are translated, not re-allocated (#508) ----
    (when (attribute-refs? @conn)
      (swap! conn update :migration
             (fn [m] (update (or m {}) :eids merge (system-eid-seed @conn)))))
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
          final (reduce-lines
                 (fn [acc record]
                   ;; `:translate` runs AFTER sysref resolution, so a user
                   ;; function sees a plain [e a v t op] with real ids and never
                   ;; an internal SysRef. Returning nil DROPS the record.
                   (if-let [rec (let [r (resolve-sysrefs sref-db record)]
                                  (if translate (translate r) r))]
                     (let [t   (nth rec 3)
                           acc (if (and (>= (long (:n acc)) batch-size)
                                        (not= t (:last-t acc)))
                                 (-> acc
                                     (update :errors into
                                             (flush-batch! conn (:batch acc) on-error progress))
                                     (assoc :batch [] :n 0))
                                 acc)]
                       (-> acc
                           (update :batch conj rec)
                           (update :n inc)
                           (assoc :last-t t)))
                     (update acc :dropped inc)))
                 {:batch [] :n 0 :last-t ::start :errors [] :dropped 0})
          errors (into (:errors final)
                       (flush-batch! conn (:batch final) on-error progress))
          hist?  (boolean (:history? manifest))
          live   (long (user-datom-count @conn hist?))
          dropped (long (:dropped final))
          ;; A translator that DROPS records makes the dump's own count the wrong
          ;; expectation — the mismatch is the transformation working, not a
          ;; failure. Subtracting the drops keeps the check meaningful (records
          ;; that should have landed and did not are still caught) instead of
          ;; either failing spuriously or being switched off, which is how people
          ;; learn to ignore verification output.
          expected (- (long (or (:count (:semantic-digest manifest)) 0)) dropped)
          verified? (when (:verify? opts)
                      (let [ok? (= expected live)]
                        (when (and (not ok?) (not= :collect on-error))
                          (throw (ex-info "Post-import verification failed (datom count mismatch)"
                                          {:error :import/verify-failed
                                           :dump-count (:count (:semantic-digest manifest))
                                           :dropped-by-translate dropped
                                           :expected-count expected
                                           :live-count live})))
                        ok?))
          ;; The EXACT number of source transactions, taken from the id-remap's
          ;; `:tids` — one entry per distinct source `t`, already in memory, so
          ;; this costs nothing. Captured here because `finalize-import!` below
          ;; drops the map.
          ;;
          ;; The batcher's own counter cannot do this. It counts TRANSITIONS
          ;; between adjacent `t` values, which equals the transaction count only
          ;; when every transaction's records are contiguous — true for a sorted
          ;; dump and false for `:sort? false`, where `export-records-streaming`
          ;; makes two passes over :eavt and emits a transaction's schema/meta
          ;; datoms in the first and its data datoms in the second. Measured: a
          ;; 13-transaction database reported 25.
          tx-count (count (:tids (:migration @conn)))]
      ;; `verified?` is nil when verification was not RUN, and false when it ran
      ;; and failed. Treating nil as failure meant `:verify? false` silently
      ;; disabled `:finalize?`, leaving the O(entities) `:migration` id-map in
      ;; the db value forever — on exactly the imports big enough that someone
      ;; turned verification off to save time.
      (when (and (:finalize? opts) (not (false? verified?)))
        (finalize-import! conn))
      (when-let [src-max-tx (:max-tx (:stats manifest))]
        (let [drift (- (long (:max-tx @conn)) (long src-max-tx))]
          (when-not (zero? drift)
            (binding [*out* *err*]
              (println (format "[datahike.migrate] max-tx drifted by %+d (source %d, restored %d): the restored database numbers its next transaction differently. Datom content is unaffected."
                               drift src-max-tx (:max-tx @conn)))))))
      {:datom-count live
       :translated? (boolean translate)
       :dropped     dropped
       ;; The restored db's max-tx is one HIGHER than the source's, because the
       ;; import ends via `transact-entities-directly`, which bumps it once more.
       ;; It does not compound -- a second round trip is stable -- but the restored
       ;; database numbers its next transaction differently from the one it
       ;; replaced, and that is the kind of thing an operator should be told
       ;; rather than discover. nil when the dump records no source max-tx.
       :max-tx-drift (when-let [src-max-tx (:max-tx (:stats manifest))]
                       (- (long (:max-tx @conn)) (long src-max-tx)))
       :tx-count    tx-count
       :max-tx      (:max-tx @conn)
       :verified?   verified?
       :finalized?  (boolean (and (:finalize? opts) (not (false? verified?))))
       :recommended-heap (:recommended-heap mem)
       :errors      errors})))

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
  (when-let [store-refs (:store-refs manifest)]
    (mblobs/check-importable store-refs opts)
    (when (seq (:carried store-refs))
      (with-blob-reader source #(mblobs/copy-in! (:store @conn) store-refs %)))))

(defn import-db
  "Import a dump produced by `export-db` into connection `conn`.

   `source` may be a filesystem path/dir OR a konserve store target (an open store
   `{:store s :prefix ..}` or a `{:backend :s3 ..}`-style config). The target db
   SHOULD be freshly created with a config compatible with the dump's
   :source-config. 2-arity keeps the legacy surface; 3-arity opts:
     :batch-size   100000   datoms per load-entities call (tx-aligned, never split)
     :verify?      true      run verify after import; throw on mismatch
     :on-error     :abort    :abort | :collect  (never silently skip)
     :translate    nil       (fn [[e a v t op]] -> record | nil) applied to every
                             record on the way in. Returning nil DROPS it.

                             This is the general hook rather than a set of special
                             facilities: attribute renames, value rewrites, unit
                             conversions, redaction and filtering are all the same
                             operation, and it costs nothing in the streaming
                             pipeline because it is per record.

                             Three constraints, all forced rather than chosen:

                             - PURE and deterministic. A future resumable import
                               re-derives ids from a pre-pass over the same
                               records; a translator that is not a function makes
                               the two passes disagree.
                             - 1 -> 0 or 1 -> 1 only. Emitting several records
                               would break the manifest counts and the
                               tx-alignment the batcher depends on.
                             - It sees a plain [e a v t op] with real ids, AFTER
                               sysref resolution — no internal types leak into a
                               user function.

                             Verification stays honest: dropped records are
                             subtracted from the expected count, so a deliberate
                             drop is not reported as corruption. The report
                             carries :translated? and :dropped. Note that
                             `verify`'s tier-2 digest compares the DUMP against
                             the live db, so a translated import will differ there
                             by design — that comparison is not meaningful after a
                             transformation.
     :finalize?    true      clear the :migration id-map after a verified import
     :progress-fn  nil
   Returns {:datom-count .. :tx-count .. :max-tx .. :verified? .. :errors [..]
            :recommended-heap ..}. Prints a heap warning if the current -Xmx looks
   too small for the id-remap map (see `estimate-import-memory`).
   Refuses a non-empty target (import is not resumable — recreate and restart)."
  ([conn source] (import-db conn source {}))
  ([conn source opts]
   (let [batch-size (get opts :batch-size default-batch-size)]
     (if (mstore/store-target? source)
       (let [m (mstore/open source)]
         (try
           (let [manifest (mstore/read-manifest m)
                 mem (estimate-from-manifest manifest (manifest-total-bytes manifest nil) batch-size)]
             (check-capabilities! manifest)
             (check-target! conn manifest)
             (restore-blobs! conn manifest source opts)
             (run-import conn manifest mem
                         (fn [rf init] (mstore/reduce-records m manifest rf init)) opts))
           (finally (mstore/close m))))
       (let [dump (open-dump source)]
         (if (:legacy? dump)
           (import-db-legacy conn source)
           (let [manifest (:manifest dump)
                 mem (estimate-from-manifest manifest (manifest-total-bytes manifest (:files dump)) batch-size)]
             (check-capabilities! manifest)
             (check-target! conn manifest)
             (restore-blobs! conn manifest source opts)
             (run-import conn manifest mem
                         (fn [rf init] (reduce-dump-records dump rf init)) opts))))))))

(defn finalize-import!
  "Clear import bookkeeping (:migration id map) from the db after a successful,
   verified import. Idempotent. The map is O(entities) and rides in the db value."
  [conn]
  (swap! conn dissoc :migration)
  :finalized)

;; ---------------------------------------------------------------------------
;; verify

(defn- norm-val
  "Stably hashable form of a value: array/bytes values compare structurally rather
   than by identity, while keeping their class distinct."
  [v]
  (cond
    (bytes? v)                              [:bytes (vec v)]
    (instance? (Class/forName "[F") v)      [:farray (vec v)]
    (instance? (Class/forName "[D") v)      [:darray (vec v)]
    :else                                   v))

;; Tier-2 fingerprint: an id-independent multiset digest over non-ref [a v op]
;; tuples, plus per-attribute ref counts and an out-degree histogram for refs — so
;; two databases with the same content compare equal despite fully remapped ids.
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

(defn- with-source-lines
  "Call `(f manifest reduce-lines)` where `reduce-lines` is `(fn [rf init] -> acc)`
   streaming the dump's record lines, for either medium (filesystem or store).
   `reduce-lines` may be invoked several times (each pass re-opens/re-reads)."
  [source f]
  (if (mstore/store-target? source)
    (let [m (mstore/open source)]
      (try (let [manifest (mstore/read-manifest m)]
             (f manifest (fn [rf init] (mstore/reduce-records m manifest rf init))))
           (finally (mstore/close m))))
    (let [dump (open-dump source)]
      (f (:manifest dump) (fn [rf init] (reduce-dump-records dump rf init))))))

(defn- verify-sample
  "Tier 3 — sampled structural diff. Pick up to `n` entities by a `:db.unique`
   attribute, reconstruct their non-ref attributes from the dump, and diff against
   `pull '[*]'` on the live db."
  [manifest reduce-lines db ref? n]
  (let [schema (:schema manifest)
        uniq   (set (for [[a m] schema
                          :when (#{:db.unique/identity :db.unique/value} (:db/unique m))] a))]
    (if (empty? uniq)
      {:sampled 0 :ok? true :note "no :db.unique attrs — content covered by tiers 1–2"}
      (let [picks (reduce-lines
                   (fn [acc record]
                     (let [[e a v _t op] record]
                       (if (and op (uniq a) (< (count acc) n) (not (contains? acc [a v])))
                         (assoc acc [a v] e) acc)))
                   {})
            pick-es (set (vals picks))
            ;; net *current* state per picked entity: asserts add, retracts remove,
            ;; so a fully-retracted entity nets to empty (and is not compared).
            recon (reduce-lines
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
                         (let [bytes (read-blob id)]
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
    (with-blob-reader source #(verify-blobs* store-refs %))
    ;; no store-refs section: either no blobs, or a dump predating blob carriage
    {:ok? true :declared 0 :verified 0 :missing [] :corrupt []
     :external 0 :self-contained? true}))

(defn verify
  "Compare a dump against its own manifest (integrity) and, given a live db/conn,
   against that database (id-independent semantic equivalence). Returns a tiered
   report: tier0 = checksums/paths (validated on open), tier1 = counts,
   tier2 = multiset digest over `[a v op]` + ref-topology counts, tier3 = sampled
   structural diff of unique entities. `source` may be a path or a konserve store."
  ([source]
   (let [{:keys [manifest]}
         (if (mstore/store-target? source)
           (let [m (mstore/open source)]
             (try
               (let [manifest (mstore/read-manifest m)]
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
                 (mstore/reduce-records m manifest (fn [acc _] acc) nil)
                 {:manifest manifest})
               (finally (mstore/close m))))
           (open-dump source))]
     (let [blobs (verify-blobs manifest source)]
       {:ok? (:ok? blobs)
        :tier0 {:checksums :ok :format (get manifest manifest-key)}
        :tier1 {:manifest-count (:count (:semantic-digest manifest))}
        :blobs blobs})))
  ([conn-or-db source]
   (let [db    (->db conn-or-db)]
     (with-source-lines source
       (fn [manifest reduce-lines]
         (let [schema (:schema manifest)
               ref?   (fn [a] (= :db.type/ref (:db/valueType (get schema a))))
               hist?  (boolean (:history? manifest))
               ;; tier 1 — counts
               dump-count (long (or (:count (:semantic-digest manifest)) 0))
               live-count (long (user-datom-count db hist?))
               ;; tier 2 — id-independent fingerprint, dump vs live
               dump-fp (fp-final (reduce-lines
                                  (fn [fp record]
                                    (let [[e a v _t op] record]
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
               t3     (verify-sample manifest reduce-lines db ref? 25)
               ;; blobs — same tier as the chunks, and part of :ok?
               blobs  (verify-blobs manifest source)]
           {:ok? (and (= dump-count live-count) t2-ok (:ok? t3) (:ok? blobs))
            :tier0 {:checksums :ok :format (get manifest manifest-key)}
            :tier1 {:manifest-count dump-count :live-count live-count :match? (= dump-count live-count)}
            :tier2 {:match? t2-ok
                    :value-digest-match? (= (:digest dump-fp) (:digest live-fp))
                    :ref-counts-match? (= (:ref-counts dump-fp) (:ref-counts live-fp))
                    :out-degree-match? (= (:out-degree dump-fp) (:out-degree live-fp))}
            :tier3 t3
            :blobs blobs}))))))

;; ---------------------------------------------------------------------------
;; legacy CBOR path (backward compatibility for old dumps)

(def ^:dynamic *import-batch-size* 10000)

(defn ^:deprecated update-max-tx
  "DEPRECATED. max-tx is maintained by load-entities; retained for old dumps."
  [db datoms]
  (assoc db :max-tx (reduce #(max %1 (nth %2 3)) (:max-tx db 0) datoms)))

(defn- instance-to-date
  "Coerce a `java.time.Instant` to `java.util.Date`.

   Belt and braces, and known to be so: boring decodes CBOR tag 1 to `Date`
   already — `legacy-instant-is-normalised-test` pins exactly that — so this is
   an identity on every value it currently sees. It is kept rather than deleted
   because it guards the LEGACY reader, whose whole job is to cope with bytes
   written by software we no longer control, and one `instance?` per value is
   not a cost worth trading for the certainty."
  [v]
  (if (instance? java.time.Instant v) (java.util.Date/from v) v))

(defn- import-db-legacy
  "Legacy import of an old flat CBOR dump via api/transact (unchanged behaviour).

   Read with boring rather than clj-cbor. A legacy dump is already a CBOR
   sequence of datom vectors, so `decode-records` reads it directly, and the two
   libraries agree on every construct these dumps contain — verified against
   bytes clj-cbor actually wrote, in `migrate-legacy-test`.

   The one difference is benign and already handled: clj-cbor decodes tag 1 to
   `java.time.Instant`, boring to `java.util.Date`, and `instance-to-date` below
   normalised that even before the swap. It stays as a guard rather than being
   deleted, since it costs nothing and an Instant reaching here from anywhere
   else would still be wrong.

   What CANNOT be recovered is what clj-cbor lost on WRITE: it encoded zero, NaN
   and +-Infinity doubles as float16 and bignums that fit as plain integers, so
   those values are already narrowed in the bytes. boring reads them exactly as
   clj-cbor does; no reader can restore information the writer discarded."
  [conn path]
  (println "Preparing legacy CBOR import of" path "in batches of" *import-batch-size*)
  (let [datoms (->> (with-open [in (io/input-stream path)]
                      (doall (mcbor/decode-records in)))
                    (map #(-> (apply d/datom %) (update :v instance-to-date))))]
    (reduce (fn [_last-tx batch]
              (let [batch (vec batch)]
                (swap! conn update-max-tx batch)
                (api/transact conn batch)))
            nil
            (partition-all *import-batch-size* datoms))))
