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
  (:require [clojure.core.async :refer [go]]
            [datahike.db.interface :as dbi]
            #?(:clj [clojure.java.io :as io])
            [datahike.blob :as blob]
            [datahike.gc :as gc]
            [konserve.core :as k]
            ;; `<??` is a BLOCKING take and exists only on the JVM. Requiring it
            ;; unconditionally meant this namespace could not even LOAD under
            ;; ClojureScript, despite its .cljc extension — the same mislabelling
            ;; `migrate/bulk.cljc` had. The functions that use it are marked
            ;; `#?(:clj ...)` below; the pure ones (`manifest-entry`,
            ;; `verify-blob`, `schema-has-store-refs?`, `check-importable`) are
            ;; available on both platforms, which is what lets
            ;; `migrate.manifest` be portable.
            #?(:clj [superv.async :refer [<?? S]])))

(def ^:const dir-name
  "Subdirectory (filesystem) / key segment (store) the blobs live under.

   Each blob is one object named by its content id, so the name IS the checksum:
   verification is recomputing `blob-id` over the bytes, identical content
   deduplicates for free, and nothing needs a side table mapping names to
   hashes."
  "store-refs")

#?(:clj
   (defn- blob-bytes
     "The bytes for `id` in `store`, or nil when this store does not hold them.

      `k/bget` hands the callback a platform-specific handle — a wrapped
      `InputStream` on the JVM, a `Blob` in JS — and the callback IS the scope in
      which that handle is valid: konserve still owns the backing object, and a
      streaming view is only live until the callback (or the channel it returns)
      completes. So the read has to finish inside it. Called asynchronously, as
      datahike's stores are, the callback must synchronously return a CHANNEL;
      returning the bytes directly makes `bget` hand back a byte array where the
      caller expects something to take from, which is a confusing failure precisely
      because it looks like a working call.

      nil is the ordinary answer for a blob that lives outside this store (see the
      ns docstring), so it must mean exactly that. No catch-all here: reporting a
      store error as \"external\" would turn a broken store into a dump that looks
      fine and carries nothing."
     [store id]
     (<?? S (k/bget store id
                    (fn [{:keys [input-stream]}]
                      (go
                        (when input-stream
                          #?(:clj (let [bos (java.io.ByteArrayOutputStream.)]
                                    (io/copy input-stream bos)
                                    (.toByteArray bos))
                             ;; cljs receives a Blob; reading it is async and not
                             ;; needed until import runs on cljs.
                             :cljs input-stream))))))))

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

#?(:clj
   (defn plan
     "Split the database's live store-refs into what this dump can carry and what it
      cannot.

      Returns `{:carried [id …] :external [id …] :self-contained? bool}`, with ids
      sorted so the manifest stays deterministic. `:external` is not a failure — it
      is the operator's half of the contract, and it has to be visible."
     [db store]
     (let [live (sort (<?? S (gc/reachable-store-refs db)))
           {carried true external false} (group-by #(some? (blob-bytes store %)) live)]
       {:carried (vec (sort carried))
        :external (vec (sort external))
        :self-contained? (empty? external)})))

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

#?(:clj
   (defn copy-out!
     "Copy the carried blobs from the source store to `write-blob!`.

      `write-blob!` is `(fn [id bytes])` and owns the medium — a file under
      `store-refs/`, a key in a konserve target, an object in a bucket. Returns the
      number copied.

      A blob that has vanished between `plan` and here (a concurrent GC, say) is
      reported rather than skipped: the dump would otherwise be short by one object
      and still look complete."
     [store {:keys [carried]} write-blob!]
     (reduce (fn [n id]
               (if-let [bytes (blob-bytes store id)]
                 (do (write-blob! id bytes) (inc n))
                 (throw (ex-info "Blob disappeared during export; the dump would be incomplete"
                                 {:error :export/blob-vanished :blob-id id}))))
             0
             carried)))

#?(:clj
   (defn copy-in!
     "Restore blobs into the target store, verifying each against its own name.

      `read-blob` is `(fn [id]) -> bytes-or-nil`. Called BEFORE the datoms are
      loaded, so nothing ever names an object that is not yet there.

      A blob whose bytes do not hash to its id is a corrupt dump and raises: writing
      it anyway would put a wrong object under a content-addressed key, where every
      later reader would trust it."
     [store {:keys [carried]} read-blob]
     (reduce (fn [n id]
               (let [bytes (read-blob id)]
                 (when (nil? bytes)
                   (throw (ex-info "Dump is missing a blob it declares"
                                   {:error :import/blob-missing :blob-id id})))
                 (when-not (verify-blob id bytes)
                   (throw (ex-info "Blob content does not match its content id — dump is corrupt"
                                   {:error :import/blob-corrupt :blob-id id})))
                 (<?? S (k/bassoc store id bytes))
                 (inc n)))
             0
             carried)))

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
