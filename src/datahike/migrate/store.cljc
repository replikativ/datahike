(ns ^:no-doc datahike.migrate.store
  "Konserve-store medium for dumps, so export/import can target an external store
   (S3 / S3-compatible via konserve-s3, JDBC, Redis, node filesystem, indexeddb,
   in-memory, ...) with no local disk for the dump itself — the same storage
   abstraction datahike uses for its own data. A diskless container writes
   straight to its configured store.

   A dump target/source may be:
     - an already-open konserve store:   {:store <store> :prefix \"my-backup\"}
     - a konserve store-config map:       {:backend :s3 :bucket .. :id .. :prefix ..}
   Chunks and the manifest are stored under keys `[\"datahike.migrate\" prefix X]`;
   the manifest key is written LAST as the commit marker. Only the store transport
   differs — the format, per-chunk SHA-256, and semantic digest are identical to
   the filesystem dump.

   ## The browser's only medium

   The filesystem medium works on the JVM and on Node (see `datahike.migrate.fs`)
   but cannot work in a BROWSER, which has no directories. So this namespace is
   where a browser export or import goes, and it is `.cljc` accordingly. On Node
   either medium works, and a konserve store IS the filesystem when you want it
   to be (`konserve.node-filestore`).

   Two things had to change shape for that, and neither is cosmetic:

   * **`reduce` became `loop`.** `async+sync` is a syntactic postwalk over the
     form it is given, and the async branch is a core.async `go` block — whose
     state machine covers only code LEXICALLY inside it, not inside a nested
     `fn`. IO in a reducing function is invisible to it. The old `reduce` over
     chunks put a `bget` inside a closure, which compiles fine and works on the
     JVM (where `<?-` is rewritten to `do`) and deadlocks on ClojureScript. That
     failure mode — green on sync, broken on async — is why the loop is not
     optional.

   * **Chunk bytes come from `konserve.binary/to-bytes`.** `bget` hands its
     callback a handle with four different shapes across backends and platforms;
     see replikativ/konserve#162, which is where that knowledge now lives.

   The external sort spills to local temp files, which a browser has none of, so
   a browser export means `:sort? false`. On the JVM and on Node either works —
   `migrate.sort` is portable. Only the dump lives in the store."
  (:require [konserve.core :as k]
            [konserve.binary :as kb]
            [konserve.store :as ks]
            [konserve.utils :refer [#?(:clj async+sync) *default-sync-translation*]
             #?@(:cljs [:refer-macros [async+sync]])]
            [superv.async #?(:clj :refer :cljs :refer-macros) [go-try- <?-]]
            ;; `go-try-` expands into `clojure.core.async/go`, and on ClojureScript
            ;; that macro has to be referred HERE for the expansion to resolve to
            ;; the cljs version. Without it the Clojure `go` is used, whose
            ;; `go-impl` walks `&env` expecting symbol keys — cljs `&env` is the
            ;; compiler map, with keyword keys, so it dies at macroexpansion with
            ;; a ClassCastException that points at `go-try-` and explains nothing.
            #?(:cljs [clojure.core.async :refer-macros [go]])
            [datahike.migrate.cbor :as mcbor]
            [datahike.migrate.compress :as mz]
            [datahike.migrate.digest :as dig]
            [datahike.tools :as dt]))

(def ^:private ns-tag "datahike.migrate")

(def ^:private mblobs-dir
  "Kept in step with `datahike.migrate.blobs/dir-name`; spelled here rather than
   required to keep this namespace free of the blob machinery, which is JVM-only
   while this one is not."
  "store-refs")

(defn store-target?
  "True if `x` designates a konserve store medium (open store or backend config),
   as opposed to a filesystem path/File."
  [x]
  (and (map? x) (or (:store x) (:backend x))))

(defn- ckey [prefix x] [ns-tag prefix x])

(defn chunk-name
  "`datoms-000001.cbor` — the SAME name a filesystem dump uses.

   It used to omit the extension here, so the two media wrote different values
   into the same manifest field and `manifest/chunk-re` (which validates a chunk
   name before opening it) matched one and rejected the other. That went
   unnoticed only because the validation is never applied to a store manifest,
   and it made this namespace's claim that the format is \"identical to the
   filesystem dump\" false in the one field a reader looks at first.

   Written out rather than with `format`, which ClojureScript does not have."
  ([n] (chunk-name n :none))
  ([n codec]
   (let [s (str n)
         pad (- 6 (count s))]
     (str "datoms-" (apply str (repeat (max 0 pad) "0")) s ".cbor"
          (mz/extension codec)))))

(defn- chunk-key [prefix n codec] (ckey prefix (chunk-name n codec)))

(defn chunk-descriptor
  "One entry of the manifest's `:chunks` vector.

   ONE constructor for both media, because this map is the manifest's public
   contract and it was previously spelled out independently in
   `migrate/write-chunked!` and in `write-chunks!` below. `chunk-name`'s
   docstring records what that costs: the two media already disagreed once on
   `:file`, silently, and it went unnoticed because the validation that would
   have caught it is only applied to one of them.

   `:bytes` is what was STORED, `:raw-bytes` what it decodes to. Both are
   needed — the first sizes a transfer, the second sizes the heap — and with
   compression on they differ by ~7x, so an estimate built on `:bytes` alone
   underestimates memory by exactly the compression ratio."
  [file count bytes raw-bytes sha256]
  {:file file :count count :bytes bytes :raw-bytes raw-bytes :sha256 sha256})

(defn blob-key
  "Where a carried `:db.type/store-ref` blob lives in a store dump.

   Under the dump's own prefix, like everything else it owns. Blobs used to sit
   at `[\"store-refs\" id]` — outside the namespace tag AND outside the prefix —
   so deleting a dump's prefix orphaned its blobs, and two dumps in one store
   shared a blob area neither of them named."
  [prefix id]
  (ckey prefix (str mblobs-dir "/" id)))

(defn open
  "Open a medium from a target spec. Returns {:store :prefix :owned?}; call `close`.

   Returns a channel unless `{:sync? true}`, since connecting a store is IO like
   any other."
  ([target] (open target {:sync? true}))
  ([target opts]
   (async+sync
    (:sync? opts) *default-sync-translation*
    (go-try-
     (let [prefix (or (:prefix target) "dump")]
       (if-let [s (:store target)]
         {:store s :prefix prefix :owned? false}
         ;; The take is hoisted into the `let` rather than written inline as
         ;; `{:store (<?- ...)}`. core.async's `go` transformer cannot rewrite a
         ;; parked take inside a MAP LITERAL — it walks the form attaching
         ;; metadata, and a literal's keyword keys are not IObj, so it fails at
         ;; macroexpansion with a ClassCastException that names neither this file
         ;; nor the map. One more rule that applies only to the async branch.
         (let [config (dissoc target :prefix)
               store (<?- (ks/connect-store config opts))]
           {:store store :prefix prefix :owned? true :config config})))))))

(defn close
  ([medium] (close medium {:sync? true}))
  ([{:keys [store owned? config]} opts]
   (async+sync
    (:sync? opts) *default-sync-translation*
    (go-try-
     (when owned?
       (<?- (ks/release-store config store opts)))
     nil))))

(defn write-chunks!
  "Stream `sorted-records` into the store as chunk BINARIES of at most
   `chunk-size` records each, computing per-chunk SHA-256 and the semantic digest
   incrementally. `manifest-fn` is (fn [finalized-digest chunks] -> manifest-map).
   Writes the manifest key LAST. Returns the manifest.

   `bassoc` rather than `assoc`: a chunk is a CBOR sequence, i.e. opaque bytes.
   Storing it as a konserve VALUE would run those bytes back through konserve's
   own serializer — encoding an encoding — and would make the stored object's
   bytes differ from the file a filesystem dump writes, so the same chunk would
   hash differently depending on the medium. The manifest stays an ordinary
   value: it is EDN, small, and read before the codec is known."
  ([medium sorted-records chunk-size manifest-fn progress]
   (write-chunks! medium sorted-records chunk-size manifest-fn progress {:sync? true}))
  ([medium sorted-records chunk-size manifest-fn progress opts]
   (write-chunks! medium sorted-records chunk-size manifest-fn progress opts
                  mz/default-codec))
  ([{:keys [store prefix]} sorted-records chunk-size manifest-fn progress opts codec]
   (async+sync
    (:sync? opts) *default-sync-translation*
    (go-try-
     (loop [rs (seq sorted-records) n 1 chunks [] dacc (dig/accumulator)]
       (if (nil? rs)
         ;; The writer stamps `:compression`, not the caller's `manifest-fn`.
         ;; They are two different places that both "know" the codec, and when
         ;; they disagree the dump is unreadable: the reader decompresses
         ;; according to the manifest, so a manifest saying `:none` over gzipped
         ;; bytes fails the chunk hash and reports corruption on an intact dump.
         ;; Stamping here means the recorded codec IS the one that was used.
         (let [manifest (assoc (manifest-fn (dig/finalize dacc) chunks)
                               :compression codec)]
           (<?- (k/assoc store (ckey prefix "manifest") manifest opts))
           (progress {:phase :done :datoms (:count (dig/finalize dacc))})
           manifest)
         (let [part (into [] (take chunk-size) rs)
               encs (mapv mcbor/encode-record part)
               content (mcbor/concat-records encs)
               ;; The hash is over the RECORDS, the stored bytes are compressed —
               ;; see `migrate.compress`. `:bytes` describes what was stored, so
               ;; an operator sizing a restore reads the transfer cost.
               stored (mz/compress-bytes codec content)]
           (<?- (k/bassoc store (chunk-key prefix n codec) stored opts))
           (progress {:phase :chunk :datoms (count part)})
           (recur (seq (drop chunk-size rs)) (inc n)
                  (conj chunks (chunk-descriptor
                                (chunk-name n codec)
                                (count part)
                                #?(:clj (alength ^bytes stored) :cljs (.-length stored))
                                #?(:clj (alength ^bytes content) :cljs (.-length content))
                                (dig/sha256-hex content)))
                  (reduce dig/add-record dacc encs)))))))))

(defn read-manifest
  ([medium] (read-manifest medium {:sync? true}))
  ([{:keys [store prefix]} opts]
   (k/get store (ckey prefix "manifest") nil opts)))

(defn read-chunk
  "The records of ONE chunk, verified. `async+sync`.

   The unit both consumers work in. `reduce-records` folds a pure `rf` over the
   whole dump with it; the IMPORTER cannot use that shape, because its per-record
   work writes to the database — and IO inside a reducing function is a closure
   the `go` state machine does not enter. Handing back a chunk at a time lets the
   importer keep the read and the write at statement positions where the state
   machine sees both.

   Memory is bounded by `:chunk-size`, which is what bounds it either way."
  ([medium manifest chunk] (read-chunk medium manifest chunk {:sync? true}))
  ([{:keys [store prefix]} manifest {:keys [file sha256]} opts]
   (async+sync
    (:sync? opts) *default-sync-translation*
    (go-try-
     (let [codec (get manifest :compression :none)
           stored (<?- (k/bget store (ckey prefix file) (kb/to-bytes opts) opts))]
       (when (nil? stored)
         (throw (ex-info (str "Missing chunk in store: " file)
                         {:error :import/checksum-failed :file file})))
       ;; Decompress BEFORE hashing: `:sha256` is over the records, so that a
       ;; dump compares equal however it was stored (see `migrate.compress`).
       (let [content (mz/decompress-bytes codec stored {:file file})]
         ;; FAIL CLOSED on a missing hash. This used to read
         ;; `(and sha256 (not= sha256 ...))`, i.e. a nil `:sha256` skipped the
         ;; comparison — verbatim the fail-open that `migrate/open-dump`'s
         ;; docstring records as fixed ("deleting one key from manifest.edn
         ;; turned integrity checking off entirely"). It was fixed for the
         ;; filesystem and not here, and the store medium is the one an operator
         ;; cannot inspect by hand. Measured: tampered values plus a deleted
         ;; `:sha256` imported clean with `:verified? true`.
         ;;
         ;; `manifest/assert-dump-manifest!` now refuses such a manifest before
         ;; any chunk is read, so this is the second line of defence rather than
         ;; the only one — but a chunk whose hash went missing between that check
         ;; and this read must still not slip through.
         ;;
         ;; `:checksums :skip` is honoured HERE as well as in
         ;; `assert-dump-manifest!`, because otherwise it is not honoured at all
         ;; on this medium. Measured, same dump both ways:
         ;;
         ;;   FS    bad sha256, :checksums :skip  → imported 20
         ;;   STORE bad sha256, :checksums :skip  → REFUSED :import/checksum-failed
         ;;
         ;; A documented option that works on one medium and silently does not
         ;; on the other — the third instance of exactly this split, after the
         ;; fail-closed rule above and the incomplete-dump refusal. And it is
         ;; the wrong way round: the escape hatch exists to salvage what is left
         ;; of a damaged backup, and the store medium is the one an operator
         ;; cannot open by hand to repair.
         ;;
         ;; The DEFAULT is unchanged and still fails closed on both a missing
         ;; and a mismatched hash. Only an explicit, warned-about opt-out passes.
         (when (not= :skip (:checksums opts))
           (when (nil? sha256)
             (throw (ex-info (str "Chunk " file " has no :sha256 in the manifest.")
                             {:error :import/missing-checksum :file file})))
           (when (not= sha256 (dig/sha256-hex content))
             (throw (ex-info (str "Checksum mismatch for chunk " file)
                             {:error :import/checksum-failed :file file}))))
         (mcbor/decode-records-from content)))))))

(defn reduce-records
  "Reduce a PURE `rf` over every record of the dump, one verified chunk at a time.

   `rf` must not perform IO: it is applied inside the loop body, where the go
   block cannot see it. The importer therefore uses `read-chunk` directly; this
   is for `verify`, whose folds are pure."
  ([medium manifest rf init] (reduce-records medium manifest rf init {:sync? true}))
  ([medium manifest rf init opts]
   (async+sync
    (:sync? opts) *default-sync-translation*
    (go-try-
     (loop [cs (seq (:chunks manifest)) acc init]
       (if (nil? cs)
         acc
         (recur (next cs)
                (reduce rf acc (dt/delivered!
                                (<?- (read-chunk medium manifest (first cs) opts))
                                {:op :read-chunk :chunk (first cs)}
                                "a dump chunk could not be read")))))))))
