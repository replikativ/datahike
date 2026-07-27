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
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clj-cbor.core :as cbor])
  (:import [datahike.migrate.edn SysRef]
           [java.io File]
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
  "Ordered seq of encoded record vectors for `db`. Order: by tx `t`, tx-entity
   (`:db/txInstant`) datoms first within a tx, then `[e a]`, with the printed line
   as final tiebreak for determinism (§6/§12 of the design)."
  [db {:keys [history?]}]
  (let [src        (if history? (api/history db) db)
        sys-ents   (if (attribute-refs? db) (dbi/-system-entities db) #{})
        sidents    (system-idents db)]
    ;; Emit only user-transaction datoms (tx > tx0). This drops the bootstrap in
    ;; attribute-refs dbs — system entities are already present in the target, so
    ;; we translate refs to them (#508/#531) rather than re-inserting them — and is
    ;; a no-op for plain dbs, whose bootstrap never appears in :eavt.
    (->> (api/datoms src :eavt)
         (filter (fn [dm] (> (d/datom-tx dm) c/tx0)))
         (map (fn [dm] (datom->record db sys-ents sidents dm)))
         (sort-by (fn [[e a _v t]] [t (if (= a :db/txInstant) 0 1) e (str a)])))))

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
     :stats                          {:datom-count (:count digest)}
     :semantic-digest                digest
     :chunks                         (vec chunks))))

(defn- write-lines! [^File f lines]
  (with-open [w (io/writer f)]
    (doseq [^String ln lines]
      (.write w ln)
      (.write w "\n"))))

(def chunk-re #"^datoms-\d{6}\.edn$")

(defn- chunk-name [n] (format "datoms-%06d.edn" n))

(defn export-db
  "Export a database (or connection) to `target`.

   2-arity keeps the legacy behaviour but now writes the type-exact EDN flat
   format. 3-arity opts:
     :history?     false        include full history (asserts+retracts+tx entities)
     :format       :chunked|:flat  (:chunked when target is a directory, else :flat)
     :chunk-size   1000000      datoms per chunk file (chunked only)
     :progress-fn  nil          (fn [{:keys [phase datoms]}])
   Returns the manifest map. Chunks are written to temp names and renamed;
   the manifest is written LAST as the commit marker.

   NOTE: with :history? true this file contains every value ever asserted,
   including retracted (\"deleted\") data — treat the dump as sensitive."
  ([db-or-conn target] (export-db db-or-conn target {}))
  ([db-or-conn target opts]
   (let [db       (->db db-or-conn)
         opts     (merge {:history? (boolean (:keep-history? (dbi/-config db)))
                          :chunk-size 1000000}
                         opts)
         fmt      (cond
                    (:format opts)                    (:format opts)
                    (.isDirectory (io/file target))   :chunked
                    :else                             :flat)
         progress (or (:progress-fn opts) (constantly nil))
         records  (export-records db opts)
         lines    (map medn/write-record records)]
     (if (= :flat fmt)
       ;; ---- flat: header manifest line + record lines ----
       (let [f (io/file target)
             digest (dig/digest-lines lines)
             body   (clojure.string/join "\n" lines)
             manifest (build-manifest db opts digest
                                      [{:file (.getName f) :count (:count digest)
                                        :sha256 (dig/sha256-hex body)}])]
         (with-open [w (io/writer f)]
           (.write w (pr-str manifest)) (.write w "\n")
           (doseq [^String ln lines] (.write w ln) (.write w "\n")))
         (restrict-perms! f false)
         (progress {:phase :done :datoms (:count digest)})
         manifest)
       ;; ---- chunked directory ----
       (let [^File dir (io/file target)]
         (.mkdirs dir)
         (restrict-perms! dir true)
         (let [acc (loop [ls (seq lines) n 1 chunks [] dacc (dig/accumulator)]
                     (if (empty? ls)
                       {:chunks chunks :digest (dig/finalize dacc)}
                       (let [part (take (:chunk-size opts) ls)
                             fname (chunk-name n)
                             tmp   (io/file dir (str fname ".tmp"))
                             final (io/file dir fname)
                             content (str (clojure.string/join "\n" part) "\n")]
                         (spit tmp content)
                         (.renameTo tmp final)
                         (restrict-perms! final false)
                         (progress {:phase :chunk :datoms (count part)})
                         (recur (drop (:chunk-size opts) ls) (inc n)
                                (conj chunks {:file fname :count (count part)
                                              :sha256 (dig/sha256-hex content)})
                                (reduce dig/add-line dacc part)))))
               manifest (build-manifest db opts (:digest acc) (:chunks acc))]
           (spit (io/file dir "manifest.edn") (pr-str manifest))
           (restrict-perms! (io/file dir "manifest.edn") false)
           (progress {:phase :done :datoms (:count (:digest acc))})
           manifest))))))

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

(defn read-dump
  "Return {:manifest m :lines <reducible/seq of record-line strings> :legacy? bool}.
   Validates chunk paths and (for chunked) per-chunk SHA-256 before yielding lines."
  [source]
  (let [^File f (io/file source)]
    (cond
      (.isDirectory f)
      (let [manifest (read-manifest-map (slurp (io/file f "manifest.edn")))
            files    (mapv (fn [{:keys [file sha256]}]
                             (let [cf (validate-chunk-file f file)
                                   body (slurp cf)]
                               (when (and sha256 (not= sha256 (dig/sha256-hex body)))
                                 (throw (ex-info (str "Checksum mismatch for chunk " file)
                                                 {:error :import/checksum-failed :file file})))
                               cf))
                           (:chunks manifest))]
        {:manifest manifest :legacy? false
         :lines (mapcat #(line-seq (io/reader %)) files)})

      (looks-like-edn-manifest? f)
      (with-open [r (io/reader f)]
        (let [lines (line-seq r)
              manifest (read-manifest-map (first lines))]
          ;; realize into memory for the flat path (small dbs by contract)
          {:manifest manifest :legacy? false :lines (doall (rest lines))}))

      :else
      {:manifest {manifest-key 0 :serialization :cbor :legacy? true} :legacy? true})))

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

(defn- pack-tx-batches
  "Greedily pack tx-groups (records sharing a `t`) into batches of <= batch-size
   without ever splitting a transaction across batches. `records` must be t-sorted."
  [records batch-size]
  (let [groups (partition-by #(nth % 3) records)]
    (loop [gs groups cur [] batches []]
      (if (empty? gs)
        (if (seq cur) (conj batches cur) batches)
        (let [g (first gs)]
          (cond
            (and (seq cur) (> (+ (count cur) (count g)) batch-size))
            (recur gs [] (conj batches cur))
            :else
            (recur (rest gs) (into cur g) batches)))))))

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

(defn import-db
  "Import a dump produced by `export-db` into connection `conn`.

   The target SHOULD be freshly created with a config compatible with the dump's
   :source-config. 2-arity keeps the legacy surface; 3-arity opts:
     :batch-size   100000   datoms per load-entities call (tx-aligned, never split)
     :verify?      true      run verify after import; throw on mismatch
     :on-error     :abort    :abort | :collect  (never silently skip)
     :finalize?    true      clear the :migration id-map after a verified import
     :progress-fn  nil
   Returns {:datom-count .. :tx-count .. :max-tx .. :verified? .. :errors [..]}.
   Refuses a non-empty target (import is not resumable — recreate and restart)."
  ([conn source] (import-db conn source {}))
  ([conn source opts]
   (let [{:keys [manifest legacy? lines]} (read-dump source)]
     (if legacy?
       (import-db-legacy conn source)
       (let [opts (merge {:batch-size 100000 :verify? true :on-error :abort :finalize? true} opts)
             progress (or (:progress-fn opts) (constantly nil))]
         ;; ---- guard rails ----
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
         ;; ---- load in tx-aligned batches ----
         (let [records (mapv #(resolve-sysrefs @conn (medn/read-record %)) lines)
               batches (pack-tx-batches records (:batch-size opts))
               errors  (atom [])]
           (doseq [batch batches]
             (try
               @(api/load-entities conn batch)
               (progress {:phase :batch :datoms (count batch)})
               (catch Exception ex
                 (if (= :collect (:on-error opts))
                   (swap! errors conj {:error (or (:error (ex-data ex)) :import/corrupt-datom)
                                       :message (ex-message ex)})
                   (throw (ex-info (str "Import aborted: " (ex-message ex))
                                   (merge {:error :import/corrupt-datom} (ex-data ex)) ex))))))
           (let [hist?     (boolean (:history? manifest))
                 live      (long (user-datom-count @conn hist?))
                 verified? (when (:verify? opts)
                             (let [ok? (= (:count (:semantic-digest manifest)) live)]
                               (when (and (not ok?) (not= :collect (:on-error opts)))
                                 (throw (ex-info "Post-import verification failed (datom count mismatch)"
                                                 {:error :import/verify-failed
                                                  :dump-count (:count (:semantic-digest manifest))
                                                  :live-count live})))
                               ok?))]
             (when (and (:finalize? opts) verified?)
               (finalize-import! conn))
             {:datom-count live
              :tx-count    (count (partition-by #(nth % 3) records))
              :max-tx      (:max-tx @conn)
              :verified?   verified?
              :finalized?  (boolean (and (:finalize? opts) verified?))
              :errors      @errors})))))))

(defn finalize-import!
  "Clear import bookkeeping (:migration id map) from the db after a successful,
   verified import. Idempotent. The map is O(entities) and rides in the db value."
  [conn]
  (swap! conn dissoc :migration)
  :finalized)

;; ---------------------------------------------------------------------------
;; verify

(defn verify
  "Compare a dump against its own manifest (integrity) and optionally against a live
   db/connection (semantic equivalence). Returns a tiered report."
  ([source]
   (let [{:keys [manifest]} (read-dump source)]
     ;; read-dump already validated chunk paths + per-chunk sha256
     {:ok? true
      :tier0 {:checksums :ok :format (get manifest manifest-key)}
      :tier1 {:manifest-count (:count (:semantic-digest manifest))}}))
  ([conn-or-db source]
   (let [base (verify source)
         {:keys [manifest]} (read-dump source)
         db (->db conn-or-db)
         live (long (user-datom-count db (boolean (:history? manifest))))
         dump (long (:count (:semantic-digest manifest)))]
     (assoc base
            :ok? (= live dump)
            :tier1 {:manifest-count dump :live-count live :match? (= live dump)}))))

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
