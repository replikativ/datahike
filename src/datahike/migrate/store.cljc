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

   The external-sort scratch is still JVM-only local temp files, so a portable
   export means `:sort? false`. Only the dump lives in the store."
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
            [datahike.migrate.digest :as dig]))

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
   into the same manifest field and `migrate/chunk-re` (which validates a chunk
   name before opening it) matched one and rejected the other. That went
   unnoticed only because the validation is never applied to a store manifest,
   and it made this namespace's claim that the format is \"identical to the
   filesystem dump\" false in the one field a reader looks at first.

   Written out rather than with `format`, which ClojureScript does not have."
  [n]
  (let [s (str n)
        pad (- 6 (count s))]
    (str "datoms-" (apply str (repeat (max 0 pad) "0")) s ".cbor")))

(defn- chunk-key [prefix n] (ckey prefix (chunk-name n)))

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
  ([{:keys [store prefix]} sorted-records chunk-size manifest-fn progress opts]
   (async+sync
    (:sync? opts) *default-sync-translation*
    (go-try-
     (loop [rs (seq sorted-records) n 1 chunks [] dacc (dig/accumulator)]
       (if (nil? rs)
         (let [manifest (manifest-fn (dig/finalize dacc) chunks)]
           (<?- (k/assoc store (ckey prefix "manifest") manifest opts))
           (progress {:phase :done :datoms (:count (dig/finalize dacc))})
           manifest)
         (let [part (into [] (take chunk-size) rs)
               encs (mapv mcbor/encode-record part)
               content (mcbor/concat-records encs)]
           (<?- (k/bassoc store (chunk-key prefix n) content opts))
           (progress {:phase :chunk :datoms (count part)})
           (recur (seq (drop chunk-size rs)) (inc n)
                  (conj chunks {:file (chunk-name n) :count (count part)
                                :bytes #?(:clj (alength ^bytes content)
                                          :cljs (.-length content))
                                :sha256 (dig/sha256-hex content)})
                  (reduce dig/add-record dacc encs)))))))))

(defn read-manifest
  ([medium] (read-manifest medium {:sync? true}))
  ([{:keys [store prefix]} opts]
   (k/get store (ckey prefix "manifest") nil opts)))

(defn reduce-records
  "Reduce `rf` over every record of the dump, verifying each chunk's SHA-256
   (a whole chunk at a time — bounded by chunk-size) before use.

   A `loop` and not a `reduce` over `(:chunks manifest)`, deliberately: see the
   namespace docstring. `rf` itself must be pure — it is applied inside the loop
   body, so any IO it performed would be invisible to the go block."
  ([medium manifest rf init] (reduce-records medium manifest rf init {:sync? true}))
  ([{:keys [store prefix]} manifest rf init opts]
   (async+sync
    (:sync? opts) *default-sync-translation*
    (go-try-
     (loop [cs (seq (:chunks manifest)) acc init]
       (if (nil? cs)
         acc
         (let [{:keys [file sha256]} (first cs)
               content (<?- (k/bget store (ckey prefix file) (kb/to-bytes opts) opts))]
           (when (nil? content)
             (throw (ex-info (str "Missing chunk in store: " file)
                             {:error :import/checksum-failed :file file})))
           (when (and sha256 (not= sha256 (dig/sha256-hex content)))
             (throw (ex-info (str "Checksum mismatch for chunk " file)
                             {:error :import/checksum-failed :file file})))
           (recur (next cs)
                  (reduce rf acc (mcbor/decode-records-from content))))))))))
