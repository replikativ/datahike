(ns ^:no-doc datahike.migrate
  "Robust, type-exact, verifiable export/import for datahike databases.

   The 3-arity `export-db`/`import-db` produce and consume a *type-exact* EDN-lines
   dump (see `doc/import-export-design.md`) that round-trips full history, every
   builtin value type (fixing #633), schema ordering (#262), the tx log (#377), and
   attribute-refs databases (#508) without re-inserting system datoms (#531).

   The legacy 2-arity remains for backward compatibility and additionally still
   *reads* old CBOR dumps on import. New dumps are EDN. Import runs through
   `load-entities`, which remaps entity/tx ids while preserving `[e a v t op]`
   structure — a restored db is semantically equivalent, never id-identical.

   Import is NOT resumable: the id-remap (`:migration`) is memory-only and dropped
   on reconnect, so a partial import must be recreated-and-restarted rather than
   resumed. Export IS resumable (completed chunks are content-addressed). A
   `:history? true` export resurrects retracted data — see the data-protection note
   in `import-db`/the backup guide."
  (:require [datahike.api :as api]
            [datahike.constants :as c]
            [datahike.datom :as d]
            [datahike.db.interface :as dbi]
            [datahike.db.utils :as dbu]
            [datahike.schema :as ds]
            [datahike.migrate.edn :as medn]
            [datahike.migrate.digest :as dig]
            [datahike.migrate.sort :as msort]
            [datahike.migrate.store :as mstore]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clj-cbor.core :as cbor])
  (:import [datahike.migrate.edn SysRef]
           [java.io File BufferedReader]
           [java.security MessageDigest]
           [java.nio.charset StandardCharsets]
           [java.nio.file Files]
           [java.nio.file.attribute PosixFilePermissions]))

(def format-version 1)
(def ^:private manifest-key :datahike.migrate/format-version)

(def ^:private source-config-allowlist
  #{:attribute-refs? :keep-history? :schema-flexibility :index})

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

(defn- posix? []
  (-> (java.nio.file.FileSystems/getDefault) .supportedFileAttributeViews (.contains "posix")))

(defn- restrict-perms! [^File f dir?]
  (when (posix?)
    (try
      (Files/setPosixFilePermissions
       (.toPath f)
       (PosixFilePermissions/fromString (if dir? "rwx------" "rw-------")))
      (catch Throwable _ nil))))

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
    [e a (medn/encode-value v sysref?) t op]))

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

(defn- build-manifest [db {:keys [history?] :as _opts} digest chunks]
  (let [cfg (dbi/-config db)]
    (array-map
     manifest-key                    format-version
     :datahike-version               (try (System/getProperty "datahike.version") (catch Throwable _ nil))
     :history?                       (boolean history?)
     :serialization                  :edn-lines
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
     :chunks                         (vec chunks))))

(def chunk-re #"^datoms-\d{6}\.edn$")

(defn- chunk-name [n] (format "datoms-%06d.edn" n))

(defn- write-chunk-stream!
  "Write up to `limit` lines from the (lazy) seq `lines` to `f`, updating the
   semantic-digest accumulator `dacc` and computing the chunk SHA-256
   incrementally — O(1) memory regardless of chunk size. Returns
   [remaining-lines count sha256-hex dacc']."
  [^File f lines limit dacc]
  (let [md (MessageDigest/getInstance "SHA-256")
        nl (byte-array 1 (byte 10))]
    (with-open [w (io/writer f)]
      (loop [ls (seq lines) c 0 da dacc]
        (if (and ls (< c limit))
          (let [^String line (first ls)]
            (.write w line)
            (.write w "\n")
            (.update md (.getBytes line StandardCharsets/UTF_8))
            (.update md nl)
            (recur (next ls) (inc c) (dig/add-line da line)))
          [ls c (dig/hex (.digest md)) da])))))

(defn- write-flat! [db opts ^File f sorted-lines progress]
  (let [tmp (File/createTempFile "dh-flat-" ".edn"
                                 (.getAbsoluteFile (or (.getParentFile (.getAbsoluteFile f))
                                                       (io/file "."))))
        [_ cnt sha dacc] (write-chunk-stream! tmp sorted-lines Long/MAX_VALUE (dig/accumulator))
        digest   (dig/finalize dacc)
        manifest (build-manifest db opts digest
                                 [{:file (.getName f) :count cnt :bytes (.length tmp) :sha256 sha}])]
    (with-open [w (io/writer f)]
      (.write w (pr-str manifest))
      (.write w "\n")
      (with-open [r (io/reader tmp)] (io/copy r w)))
    (.delete tmp)
    (restrict-perms! f false)
    (progress {:phase :done :datoms cnt})
    manifest))

(defn- write-chunked! [db opts ^File dir sorted-lines chunk-size progress]
  (.mkdirs dir)
  (restrict-perms! dir true)
  (loop [ls (seq sorted-lines) n 1 chunks [] dacc (dig/accumulator)]
    (if (nil? ls)
      (let [manifest (build-manifest db opts (dig/finalize dacc) chunks)]
        (spit (io/file dir "manifest.edn") (pr-str manifest))
        (restrict-perms! (io/file dir "manifest.edn") false)
        (progress {:phase :done :datoms (:count (dig/finalize dacc))})
        manifest)
      (let [fname (chunk-name n)
            tmp   (io/file dir (str fname ".tmp"))
            final (io/file dir fname)
            [rem cnt sha dacc'] (write-chunk-stream! tmp ls chunk-size dacc)]
        (.renameTo tmp final)
        (restrict-perms! final false)
        (progress {:phase :chunk :datoms cnt})
        (recur (seq rem) (inc n)
               (conj chunks {:file fname :count cnt :bytes (.length final) :sha256 sha})
               dacc')))))

(defn export-db
  "Export a database (or connection) to `target`.

   2-arity keeps the legacy surface but writes the type-exact EDN format. 3-arity
   opts:
     :history?     false        include full history (asserts+retracts+tx entities)
     :format       :chunked|:flat  (:chunked when target is a directory, else :flat)
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
                          :sort? true}
                         opts)
         progress (or (:progress-fn opts) (constantly nil))
         write-to! (fn [lines]
                     (cond
                       ;; external store (konserve): S3 / S3-compatible / JDBC / ...
                       (mstore/store-target? target)
                       (let [m (mstore/open target)]
                         (try
                           (mstore/write-chunks! m lines (:chunk-size opts)
                                                 (fn [digest chunks] (build-manifest db opts digest chunks))
                                                 progress)
                           (finally (mstore/close m))))

                       :else
                       (let [fmt (cond
                                   (:format opts)                  (:format opts)
                                   (.isDirectory (io/file target)) :chunked
                                   :else                           :flat)]
                         (if (= :flat fmt)
                           (write-flat! db opts (io/file target) lines progress)
                           (write-chunked! db opts (io/file target) lines (:chunk-size opts) progress)))))]
     (if (:sort? opts)
       (let [records (export-records db opts)
             ^File tmp-dir (.toFile (Files/createTempDirectory
                                     "dh-export"
                                     (make-array java.nio.file.attribute.FileAttribute 0)))]
         (try
           (write-to! (msort/external-sort records (:sort-buffer opts) tmp-dir))
           (finally
             (doseq [^File f (.listFiles tmp-dir)] (.delete f))
             (.delete tmp-dir))))
       ;; no-scratch streaming: no temp dir, no sort
       (write-to! (map medn/write-record (export-records-streaming db opts)))))))

;; ---------------------------------------------------------------------------
;; reading dumps

(defn- read-manifest-map [^String s]
  (edn/read-string {:readers medn/readers :default (fn [t v] (tagged-literal t v))} s))

(defn- looks-like-edn-manifest? [^File f]
  (with-open [r (io/reader f)]
    (let [c (.read r)] (= (int \{) c))))

(defn- validate-chunk-file [^File dir fname]
  (when-not (re-matches chunk-re fname)
    (throw (ex-info (str "Illegal chunk file name in manifest: " fname)
                    {:error :import/bad-chunk-path :file fname})))
  (let [f (io/file dir fname)
        canon (.getCanonicalFile f)
        base  (.getCanonicalFile dir)]
    (when-not (= base (.getParentFile canon))
      (throw (ex-info (str "Chunk path escapes dump directory: " fname)
                      {:error :import/bad-chunk-path :file fname})))
    f))

(defn- manifest-of
  "Return {:manifest m :legacy? bool :flat? bool :files [File...]} WITHOUT hashing —
   reads the manifest and validates chunk paths only. Cheap enough for estimation."
  [source]
  (let [^File f (io/file source)]
    (cond
      (.isDirectory f)
      (let [manifest (read-manifest-map (slurp (io/file f "manifest.edn")))
            files    (mapv #(validate-chunk-file f (:file %)) (:chunks manifest))]
        {:manifest manifest :legacy? false :flat? false :files files})

      (looks-like-edn-manifest? f)
      (let [manifest (with-open [^BufferedReader r (io/reader f)]
                       (read-manifest-map (.readLine r)))]
        {:manifest manifest :legacy? false :flat? true :files [f]})

      :else
      {:manifest {manifest-key 0 :serialization :cbor :legacy? true}
       :legacy? true :flat? false :files []})))

(defn- open-dump
  "Like `manifest-of`, but for a chunked directory dump additionally validates chunk
   paths and verifies per-chunk SHA-256 by STREAMING each file (bounded memory)
   before any import touches the database. Flat dumps have no separate chunk files
   to validate (their single 'chunk' is the file itself)."
  [source]
  (let [dump (manifest-of source)]
    (when (and (not (:legacy? dump)) (not (:flat? dump)))
      (doseq [{:keys [file sha256]} (:chunks (:manifest dump))
              :when sha256
              :let [cf (validate-chunk-file (io/file source) file)]]
        (when (not= sha256 (dig/sha256-file-hex cf))
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
        recommend (max (* 512 1024 1024) (long (* 1.6 (+ idmap batch))))
        maxheap  (.maxMemory (Runtime/getRuntime))]
    {:datoms datoms
     :entities entities
     :id-map-bytes idmap
     :batch-bytes batch
     :recommended-heap-bytes recommend
     :recommended-heap (bytes->human recommend)
     :current-max-heap-bytes maxheap
     :current-max-heap (bytes->human maxheap)
     :sufficient? (>= maxheap recommend)}))

(defn- manifest-total-bytes
  "Total dump bytes from the manifest's chunk `:bytes` (present in v1 dumps); falls
   back to on-disk file sizes for a filesystem dump that predates `:bytes`."
  [manifest files]
  (let [from-manifest (reduce + 0 (keep :bytes (:chunks manifest)))]
    (if (pos? from-manifest)
      from-manifest
      (reduce + 0 (map #(.length ^File %) (or files []))))))

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
   (let [batch-size (:batch-size (merge {:batch-size 100000} opts))]
     (if (mstore/store-target? source)
       (let [m (mstore/open source)]
         (try
           (let [manifest (mstore/read-manifest m)]
             (estimate-from-manifest manifest (manifest-total-bytes manifest nil) batch-size))
           (finally (mstore/close m))))
       (let [{:keys [manifest files]} (manifest-of source)]
         (estimate-from-manifest manifest (manifest-total-bytes manifest files) batch-size))))))

(defn- reduce-dump-lines
  "Reduce `rf` over every record-line of the dump, with each file's reader scoped to
   its inner reduction (no lazy seq escapes an open handle). Flat dumps skip the
   manifest header line. Returns the final accumulator."
  [{:keys [files flat?]} rf init]
  (reduce (fn [acc ^File file]
            (with-open [r (io/reader file)]
              (let [lines (line-seq r)
                    lines (if flat? (rest lines) lines)]
                (reduce rf acc lines))))
          init
          files))

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
    (doseq [k [:attribute-refs? :keep-history? :schema-flexibility]]
      (when (and (contains? src k) (not= (get src k) (get tgt k)))
        (throw (ex-info (str "Config mismatch on " k
                             ": dump=" (get src k) " target=" (get tgt k))
                        {:error :import/config-mismatch :key k
                         :expected (get src k) :actual (get tgt k)}))))))

(declare import-db-legacy finalize-import!)

(defn- run-import
  "Medium-agnostic import core. `reduce-lines` is (fn [rf init] -> acc) that streams
   the dump's record lines with resource scoping (filesystem or store). `mem` is the
   memory estimate. Handles guards, attribute-refs seeding, the tx-aligned batcher,
   verification, finalization, and the report."
  [conn manifest mem reduce-lines opts]
  (let [opts     (merge {:batch-size 100000 :verify? true :on-error :abort :finalize? true} opts)
        progress (or (:progress-fn opts) (constantly nil))
        batch-size (:batch-size opts)
        on-error (:on-error opts)]
    ;; ---- heap preflight: tell the operator how much RAM to give this ----
    (when-not (:sufficient? mem)
      (binding [*out* *err*]
        (println (format "[datahike.migrate] heap warning: importing %d datoms (~%d entities) needs about %s; this JVM's -Xmx is %s. Raise -Xmx (the id-remap map is held until finalize) or expect OutOfMemoryError."
                         (:datoms mem) (:entities mem)
                         (:recommended-heap mem) (:current-max-heap mem)))))
    ;; ---- guard rails (all before touching the db) ----
    (when-let [fv (get manifest manifest-key)]
      (when (> fv format-version)
        (throw (ex-info (str "Unsupported dump format-version " fv)
                        {:error :import/format-version :version fv}))))
    (config-compat! manifest conn)
    (when (pos? (user-datom-count @conn))
      (throw (ex-info "Target database is not empty; import is not resumable — recreate and restart."
                      {:error :import/non-empty-target})))
    ;; ---- attribute-refs: seed system-entity identity so refs to system
    ;; entities are translated, not re-allocated (#508) ----
    (when (attribute-refs? @conn)
      (swap! conn update :migration
             (fn [m] (update (or m {}) :eids merge (system-eid-seed @conn)))))
    ;; ---- stream records through a tx-aligned batcher (bounded memory) ----
    ;; System-entity idents are stable across the import, so resolve #sysref against
    ;; a captured db value. A transaction is never split across a batch flush; a tx
    ;; spanning chunk boundaries is still kept whole, because the batcher state
    ;; carries across chunks/files.
    (let [sref-db @conn
          final (reduce-lines
                 (fn [acc line]
                   (let [rec (resolve-sysrefs sref-db (medn/read-record line))
                         t   (nth rec 3)
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
                         (update :tx-count + (if (= t (:last-t acc)) 0 1))
                         (assoc :last-t t))))
                 {:batch [] :n 0 :last-t ::start :tx-count 0 :errors []})
          errors (into (:errors final)
                       (flush-batch! conn (:batch final) on-error progress))
          hist?  (boolean (:history? manifest))
          live   (long (user-datom-count @conn hist?))
          verified? (when (:verify? opts)
                      (let [ok? (= (:count (:semantic-digest manifest)) live)]
                        (when (and (not ok?) (not= :collect on-error))
                          (throw (ex-info "Post-import verification failed (datom count mismatch)"
                                          {:error :import/verify-failed
                                           :dump-count (:count (:semantic-digest manifest))
                                           :live-count live})))
                        ok?))]
      (when (and (:finalize? opts) verified?)
        (finalize-import! conn))
      {:datom-count live
       :tx-count    (:tx-count final)
       :max-tx      (:max-tx @conn)
       :verified?   verified?
       :finalized?  (boolean (and (:finalize? opts) verified?))
       :recommended-heap (:recommended-heap mem)
       :errors      errors})))

(defn import-db
  "Import a dump produced by `export-db` into connection `conn`.

   `source` may be a filesystem path/dir OR a konserve store target (an open store
   `{:store s :prefix ..}` or a `{:backend :s3 ..}`-style config). The target db
   SHOULD be freshly created with a config compatible with the dump's
   :source-config. 2-arity keeps the legacy surface; 3-arity opts:
     :batch-size   100000   datoms per load-entities call (tx-aligned, never split)
     :verify?      true      run verify after import; throw on mismatch
     :on-error     :abort    :abort | :collect  (never silently skip)
     :finalize?    true      clear the :migration id-map after a verified import
     :progress-fn  nil
   Returns {:datom-count .. :tx-count .. :max-tx .. :verified? .. :errors [..]
            :recommended-heap ..}. Prints a heap warning if the current -Xmx looks
   too small for the id-remap map (see `estimate-import-memory`).
   Refuses a non-empty target (import is not resumable — recreate and restart)."
  ([conn source] (import-db conn source {}))
  ([conn source opts]
   (let [batch-size (:batch-size (merge {:batch-size 100000} opts))]
     (if (mstore/store-target? source)
       (let [m (mstore/open source)]
         (try
           (let [manifest (mstore/read-manifest m)
                 mem (estimate-from-manifest manifest (manifest-total-bytes manifest nil) batch-size)]
             (run-import conn manifest mem
                         (fn [rf init] (mstore/reduce-lines m manifest rf init)) opts))
           (finally (mstore/close m))))
       (let [dump (open-dump source)]
         (if (:legacy? dump)
           (import-db-legacy conn source)
           (let [manifest (:manifest dump)
                 mem (estimate-from-manifest manifest (manifest-total-bytes manifest (:files dump)) batch-size)]
             (run-import conn manifest mem
                         (fn [rf init] (reduce-dump-lines dump rf init)) opts))))))))

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
    (update fp :acc dig/add-line (pr-str [a (norm-val v) op]))))
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
             (f manifest (fn [rf init] (mstore/reduce-lines m manifest rf init))))
           (finally (mstore/close m))))
    (let [dump (open-dump source)]
      (f (:manifest dump) (fn [rf init] (reduce-dump-lines dump rf init))))))

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
                   (fn [acc line]
                     (let [[e a v _t op] (medn/read-record line)]
                       (if (and op (uniq a) (< (count acc) n) (not (contains? acc [a v])))
                         (assoc acc [a v] e) acc)))
                   {})
            pick-es (set (vals picks))
            ;; net *current* state per picked entity: asserts add, retracts remove,
            ;; so a fully-retracted entity nets to empty (and is not compared).
            recon (reduce-lines
                   (fn [acc line]
                     (let [[e a v _t op] (medn/read-record line)]
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

(defn verify
  "Compare a dump against its own manifest (integrity) and, given a live db/conn,
   against that database (id-independent semantic equivalence). Returns a tiered
   report: tier0 = checksums/paths (validated on open), tier1 = counts,
   tier2 = multiset digest over `[a v op]` + ref-topology counts, tier3 = sampled
   structural diff of unique entities. `source` may be a path or a konserve store."
  ([source]
   (let [{:keys [manifest]} (if (mstore/store-target? source)
                              (let [m (mstore/open source)]
                                (try {:manifest (mstore/read-manifest m)} (finally (mstore/close m))))
                              (open-dump source))]
     {:ok? true
      :tier0 {:checksums :ok :format (get manifest manifest-key)}
      :tier1 {:manifest-count (:count (:semantic-digest manifest))}}))
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
                                  (fn [fp line]
                                    (let [[e a v _t op] (medn/read-record line)]
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
               t3     (verify-sample manifest reduce-lines db ref? 25)]
           {:ok? (and (= dump-count live-count) t2-ok (:ok? t3))
            :tier0 {:checksums :ok :format (get manifest manifest-key)}
            :tier1 {:manifest-count dump-count :live-count live-count :match? (= dump-count live-count)}
            :tier2 {:match? t2-ok
                    :value-digest-match? (= (:digest dump-fp) (:digest live-fp))
                    :ref-counts-match? (= (:ref-counts dump-fp) (:ref-counts live-fp))
                    :out-degree-match? (= (:out-degree dump-fp) (:out-degree live-fp))}
            :tier3 t3}))))))

;; ---------------------------------------------------------------------------
;; legacy CBOR path (backward compatibility for old dumps)

(def ^:dynamic *import-batch-size* 10000)

(defn ^:deprecated update-max-tx
  "DEPRECATED. max-tx is maintained by load-entities; retained for old dumps."
  [db datoms]
  (assoc db :max-tx (reduce #(max %1 (nth %2 3)) (:max-tx db 0) datoms)))

(defn ^:deprecated update-max-tx-from-file
  "DEPRECATED no-op wrapper retained for backward compatibility."
  [db _file]
  db)

(defn- instance-to-date [v]
  (if (instance? java.time.Instant v) (java.util.Date/from v) v))

(defn- import-db-legacy
  "Legacy import of an old flat CBOR dump via api/transact (unchanged behaviour)."
  [conn path]
  (println "Preparing legacy CBOR import of" path "in batches of" *import-batch-size*)
  (let [datoms (->> (cbor/slurp-all path)
                    (map #(-> (apply d/datom %) (update :v instance-to-date))))]
    (reduce (fn [_last-tx batch]
              (let [batch (vec batch)]
                (swap! conn update-max-tx batch)
                (api/transact conn batch)))
            nil
            (partition-all *import-batch-size* datoms))))
