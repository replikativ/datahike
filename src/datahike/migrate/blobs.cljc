(ns datahike.migrate.blobs
  "Carrying `:db.type/store-ref` BYTES in a dump.

   A store-ref datom holds a `hasch` content id (`datahike.blob/blob-id`), which
   is both the value in the datom and the key the bytes live under. Exporting the
   datom therefore exports the *reference* and not the referent: restore it into a
   fresh store and you get datoms naming objects that are not there. A backup that
   loses the blobs is not a backup, so the dump has to carry them.

   THE PART THAT CANNOT BE ASSUMED AWAY: datahike does not always have the bytes.
   `:db.type/store-ref` deliberately says *what* an object is, never *where* it
   lives (see `datahike.gc/reachable-store-refs`). Two cases, and only the first
   is ours:

     - IN THIS STORE — written with `k/bassoc`, readable with `k/bget`. We copy
       these into the dump, and the dump is self-contained with respect to them.

     - ANYWHERE ELSE — a raw S3 prefix a browser PUT to with a presigned URL, a
       CDN, another bucket. The bytes never transit our JVM by design; that is
       the whole point of the feature. We cannot copy what we cannot read.

   So `plan` splits the live set into `:carried` and `:external`, the manifest
   records both, and a dump that has externals is honestly labelled as not
   self-contained rather than quietly appearing complete. That mirrors the
   division of labour the GC already states: datahike owns the mark (what is
   still referenced, across branches and through retained history), the operator
   owns the sweep — and here, the copy.

   ORDERING. Bytes must be present before anything names them, the same
   constraint the konserve-sync walker met by shipping blobs ahead of the branch
   head, and the same one the dump meets by writing the manifest last. On import
   we restore blobs BEFORE the datoms that reference them; on export we write
   them before the manifest, so a dump without a manifest is incomplete by
   definition and a dump with one has its blobs."
  (:require [datahike.db.interface :as dbi]
            [datahike.blob :as blob]
            [datahike.gc :as gc]
            [datahike.tools :as dt]
            [konserve.core :as k]
            ;; Fully portable now. This namespace used to refer
            ;; `superv.async/<??` — a BLOCKING take that exists only on the JVM —
            ;; so despite its .cljc extension it could not even LOAD under
            ;; ClojureScript. Every IO function is `async+sync` instead, and
            ;; `go-try-`/`<?-` park rather than block.
            [konserve.binary :as kb]
            ;; `async+sync` and the superv operators are MACROS — ClojureScript
            ;; needs :refer-macros, and a plain :refer fails with
            ;; "var konserve.utils/async+sync does not exist".
            #?(:clj  [konserve.utils :refer [async+sync *default-sync-translation*]]
               :cljs [konserve.utils :refer [*default-sync-translation*]
                      :refer-macros [async+sync]])
            #?(:clj  [superv.async :refer [go-try- <?-]]
               :cljs [superv.async :refer-macros [go-try- <?-]])
            ;; ClojureScript ONLY, and it is load-bearing despite `go-try-`
            ;; syntax-quoting `clojure.core.async/go`: without it the compiler
            ;; resolves the CLOJURE `go`, whose `go-impl` walks `&env` expecting
            ;; symbol keys, and cljs `&env` is the compiler map with keyword
            ;; keys. Measured — removing it fails the node build at the first
            ;; `go-try-`. The JVM refer that used to sit beside it really was
            ;; dead and is gone.
            #?(:cljs [clojure.core.async :refer-macros [go]])))

(def ^:const dir-name
  "Subdirectory (filesystem) / key segment (store) the blobs live under.

   Each blob is one object named by its content id, so the name IS the checksum:
   verification is recomputing `blob-id` over the bytes, identical content
   deduplicates for free, and nothing needs a side table mapping names to
   hashes."
  "store-refs")

(defn- blob-bytes
  "The bytes for `id` in `store`, or nil when this store does not hold them.

   `konserve.binary/to-bytes` rather than a hand-rolled callback: `bget` hands
   its callback a handle with four different shapes across backends and
   platforms, and that knowledge lives in konserve now (replikativ/konserve#162).

   nil is the ordinary answer for a blob that lives outside this store (see the
   ns docstring), so it must mean exactly that. No catch-all here: reporting a
   store error as \"external\" would turn a broken store into a dump that looks
   fine and carries nothing."
  [store id opts]
  (async+sync
   (:sync? opts) *default-sync-translation*
   (go-try-
    (<?- (k/bget store id (kb/to-bytes opts) opts)))))

(defn schema-has-store-refs?
  "True when any attribute in `db` is declared `:db.type/store-ref`.

   The cheap precondition for planning blobs at all: with no such attribute there
   can be no blobs, and the full reachability walk `plan` performs — which also
   requires a flushed index — would be both pointless and, on an unflushed
   in-memory db, an error."
  [db]
  (boolean
   (some (fn [[_ attr]]
           (= :db.type/store-ref (:db/valueType attr)))
         (dbi/-schema db))))

(defn plan
  "Split the database's live store-refs into what this dump can carry and what it
   cannot.

   Returns `{:carried [id …] :external [id …] :self-contained? bool}`, with ids
   sorted so the manifest stays deterministic. `:external` is not a failure — it
   is the operator's half of the contract, and it has to be visible.

   `loop` and not `group-by`: deciding whether a blob is carried means READING
   it, and a `group-by` predicate is a closure the `go` state machine does not
   enter. Same rule as `copy-out!` below."
  [db store opts]
  (async+sync
   (:sync? opts) *default-sync-translation*
   (go-try-
    (let [;; `delivered!`, because a CLOSED channel is not an empty result.
          ;; `go-try-` turns a thrown Exception into a channel value but does not
          ;; cover a channel that closes — a JVM `Error` (an `assert`, and the
          ;; :test alias runs with -ea) or a cljs throw of a non-`js/Error` — and
          ;; `<?-` then yields nil. `(sort nil)` is `()`, the loop below exits on
          ;; its first iteration, and a FAILED reachability walk returns
          ;; `{:carried [] :external [] :self-contained? true}`.
          ;;
          ;; That is not a cosmetic wrong answer. Reproduced: no blob bytes are
          ;; written, the manifest omits `:store-refs` entirely, `restore-blobs!`
          ;; becomes a no-op, and `verify` reports `:ok? true` — a dump missing
          ;; every blob, certified intact, importing into a database naming
          ;; objects that were never placed. `manifest.cljc`'s own docstring
          ;; records the identical outcome reached by a different route, and the
          ;; 39 tests that caught THAT one do not catch this, because the plan is
          ;; only wrong when the walk fails.
          ;;
          ;; nil is unambiguously failure here: `reachable-store-refs` returns a
          ;; set, `#{}` for a schema with store-refs but no blob values.
          live (sort (dt/delivered! (<?- (gc/reachable-store-refs
                                          db (#?(:clj java.util.Date. :cljs js/Date.) 0) opts))
                                    {:op :export/reachable-store-refs}))]
      (loop [ids (seq live) carried [] external []]
        (if (nil? ids)
          {:carried (vec (sort carried))
           :external (vec (sort external))
           :self-contained? (empty? external)}
          (let [id (first ids)]
            (if (some? (<?- (blob-bytes store id opts)))
              (recur (next ids) (conj carried id) external)
              (recur (next ids) carried (conj external id))))))))))

(defn manifest-entry
  "The `:store-refs` section for the manifest.

   Records the count and the ids rather than only a total: an importer must be
   able to say *which* blob is missing, and a verifier must be able to check the
   dump holds exactly these. Sizes are omitted deliberately — the id is the
   content hash, so it already pins the bytes."
  [{:keys [carried external self-contained?]}]
  (array-map :self-contained? self-contained?
             :carried-count (count carried)
             :carried (vec carried)
             :external (vec external)))

(defn verify-blob
  "True when `bytes` hash to `id`.

   The name is the checksum, so verification needs nothing from the manifest
   beyond the name itself — and it detects a torn or truncated object, which a
   count alone would not."
  [id bytes]
  (= id (blob/blob-id bytes)))

(defn copy-out!
  "Copy the carried blobs from the source store to `write-blob!`.

   `write-blob!` is `(fn [id bytes opts])` and owns the medium — a file under
   `store-refs/`, a key in a konserve target, an object in a bucket. Returns the
   number copied.

   A blob that has vanished between `plan` and here (a concurrent GC, say) is
   reported rather than skipped: the dump would otherwise be short by one object
   and still look complete.

   `loop` and not `reduce`: the body performs IO, and a reducing function is a
   closure the `go` state machine does not enter — the same rule that turned
   `migrate.store`'s reduce into a loop."
  [store {:keys [carried]} write-blob! opts]
  (async+sync
   (:sync? opts) *default-sync-translation*
   (go-try-
    (loop [ids (seq carried) n 0]
      (if (nil? ids)
        n
        (let [id (first ids)
              bytes (<?- (blob-bytes store id opts))]
          (when (nil? bytes)
            (throw (ex-info "Blob disappeared during export; the dump would be incomplete"
                            {:error :export/blob-vanished :blob-id id})))
          (<?- (write-blob! id bytes opts))
          (recur (next ids) (inc n))))))))

(defn copy-in!
  "Restore blobs into the target store, verifying each against its own name.

   `read-blob` is `(fn [id opts]) -> bytes-or-nil`. Called BEFORE the datoms are
   loaded, so nothing ever names an object that is not yet there.

   A blob whose bytes do not hash to its id is a corrupt dump and raises: writing
   it anyway would put a wrong object under a content-addressed key, where every
   later reader would trust it.

   `loop` and not `reduce`, for the reason `copy-out!` gives."
  [store {:keys [carried]} read-blob opts]
  (async+sync
   (:sync? opts) *default-sync-translation*
   (go-try-
    (loop [ids (seq carried) n 0]
      (if (nil? ids)
        n
        (let [id (first ids)
              bytes (<?- (read-blob id opts))]
          (when (nil? bytes)
            (throw (ex-info "Dump is missing a blob it declares"
                            {:error :import/blob-missing :blob-id id})))
          (when-not (verify-blob id bytes)
            (throw (ex-info "Blob content does not match its content id — dump is corrupt"
                            {:error :import/blob-corrupt :blob-id id})))
          (<?- (k/bassoc store id bytes opts))
          (recur (next ids) (inc n))))))))

(defn check-importable
  "Raise unless a dump's blobs can be honoured by this import.

   A dump carrying externals is importable only if the caller says so: the
   restored database will name objects this import did not place, and that has to
   be a decision rather than a discovery. Same rule as a partial dump — best
   effort is fine, silence is not."
  [{:keys [external] :as _store-refs} {:keys [accept-external-blobs?] :as _opts}]
  (when (and (seq external) (not accept-external-blobs?))
    (throw (ex-info (str "This dump is not self-contained: " (count external)
                         " store-ref blob(s) live outside the source store and were "
                         "not carried. Copy them to the target's blob location and "
                         "re-run with :accept-external-blobs? true, or export from a "
                         "database whose blobs are in-store.")
                    {:error :import/external-blobs :external external})))
  nil)
