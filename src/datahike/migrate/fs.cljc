(ns ^:no-doc datahike.migrate.fs
  "The filesystem operations a dump needs, on the JVM and on Node.

   ## Why this exists

   A dump directory is plain files — `manifest.edn`, `datoms-000001.cbor`,
   `store-refs/<id>` — and that is a format guarantee, not an implementation
   detail: anyone can read it with `head` and `cbor2`, and a dump written on one
   runtime must be byte-identical to one written on another. Routing Node
   through a konserve store instead would have produced konserve-framed blobs
   under the same name, so a Node dump could not be read by the JVM importer.

   The alternative was to declare the filesystem medium JVM-only. That was the
   right call for the BROWSER, which has no directories, and wrong for Node,
   which has `fs`. All nineteen operations `migrate` performs have a Node
   counterpart, including `chmod` and `rename` — so this is a seam, not a
   redesign.

   ## Shape

   Paths are STRINGS, not `java.io.File` and not any wrapper. The JVM side of
   `migrate` used `File` throughout; strings are the only spelling both runtimes
   share, and every operation here takes and returns them.

   Reading and writing are `sink` and `puller`:

   * a **sink** is opened, written bytes, and closed. It never holds the file.
   * a **puller** is `(fn [] -> bytes | nil)`, which is exactly the source shape
     `boring/decode-seq-from` takes on ClojureScript — so a multi-gigabyte flat
     dump streams through one handle on either runtime.

   ## Browser

   Loading this namespace in a browser is fine; CALLING it is not. `js/require`
   is resolved lazily and guarded, so a browser build that merely pulls the
   namespace in through `migrate` compiles and links. Every function then fails
   with a named error rather than a `ReferenceError` from Closure's output."
  #?(:clj (:require [clojure.java.io :as io])
     :cljs (:require [datahike.migrate.digest]))
  #?(:clj (:import [java.io File]
                   [java.nio.charset StandardCharsets]
                   [java.nio.file Files FileSystems]
                   [java.nio.file.attribute PosixFilePermissions])))

#?(:cljs
   (defn- node
     "The Node module `m`, or a named failure.

      Lazy and guarded rather than a top-level `(js/require \"fs\")`: this
      namespace is reachable from a browser build through `migrate`, where
      requiring at load time would break the bundle at link rather than at the
      call that actually needs a filesystem."
     [m]
     (if (exists? js/require)
       (js/require m)
       (throw (ex-info (str "datahike.migrate.fs needs Node's `" m "` module. "
                            "A filesystem dump has no browser counterpart — export "
                            "to a konserve store instead (`{:store <store> :prefix ...}`), "
                            "which is portable.")
                       {:error :datahike.migrate/no-filesystem :module m})))))

#?(:cljs (defn- fs [] (node "fs")))
#?(:cljs (defn- path* [] (node "path")))
#?(:cljs (defn- os* [] (node "os")))

;; ---------------------------------------------------------------------------
;; paths

(defn join
  "Path segments to one path."
  [& parts]
  #?(:clj (str (apply io/file parts))
     ;; `.apply` with the module as the RECEIVER. `(apply (.-join p) p ...)`
     ;; looks equivalent and is not: it passes the module as join's first
     ;; ARGUMENT, so Node sees a non-string path and raises ERR_INVALID_ARG_TYPE.
     :cljs (let [p (path*)]
             (.apply (.-join p) p (into-array (map str parts))))))

(defn parent
  "The containing directory, or nil at the root."
  [p]
  #?(:clj (some-> (.getParentFile (.getAbsoluteFile (io/file p))) str)
     :cljs (let [d (.dirname (path*) (str p))]
             (when-not (= d (str p)) d))))

(defn file-name [p]
  #?(:clj (.getName (io/file p))
     :cljs (.basename (path*) (str p))))

(defn canonical
  "The path with symlinks and `..` resolved. Used to check that a manifest names
   a chunk INSIDE the dump directory rather than somewhere else."
  [p]
  #?(:clj (str (.getCanonicalFile (io/file p)))
     :cljs (.resolve (path*) (str p))))

;; ---------------------------------------------------------------------------
;; interrogation

(defn exists? [p]
  #?(:clj (.exists (io/file p))
     :cljs (.existsSync (fs) (str p))))

(defn directory?
  "True for an existing directory. False — not an error — for a path that does
   not exist, which is what the export path asks when choosing :flat vs :chunked."
  [p]
  #?(:clj (.isDirectory (io/file p))
     :cljs (and (.existsSync (fs) (str p))
                (.isDirectory (.statSync (fs) (str p))))))

(defn file-size [p]
  #?(:clj (.length (io/file p))
     :cljs (.-size (.statSync (fs) (str p)))))

(defn list-names
  "The names (not paths) directly inside directory `p`, or nil if it is not one."
  [p]
  #?(:clj (some->> (.listFiles (io/file p)) (mapv #(.getName ^File %)))
     :cljs (when (directory? p) (vec (.readdirSync (fs) (str p))))))

;; ---------------------------------------------------------------------------
;; mutation

(defn mkdirs! [p]
  #?(:clj (.mkdirs (io/file p))
     :cljs (do (.mkdirSync (fs) (str p) #js {:recursive true}) true)))

(defn delete!
  "Remove `p`. Returns true if it went away; a missing path is not an error,
   since cleanup runs on paths that may never have been created."
  [p]
  #?(:clj (.delete (io/file p))
     :cljs (try (.rmSync (fs) (str p) #js {:force true}) true
                (catch :default _ false))))

(defn rename!
  "Move `from` onto `to`. The dump writes a chunk to `<name>.tmp` and renames it,
   so a reader never sees a partial chunk under a name the manifest will claim."
  [from to]
  #?(:clj (.renameTo (io/file from) (io/file to))
     :cljs (do (.renameSync (fs) (str from) (str to)) true)))

(defn temp-file!
  "Create an empty file under `dir` and return its path."
  [dir prefix suffix]
  #?(:clj (str (File/createTempFile prefix suffix (io/file dir)))
     :cljs (let [p (join dir (str prefix (.toString (js/Math.floor (* (js/Math.random) 1e12)) 36) suffix))]
             (.writeFileSync (fs) p (js/Uint8Array. 0))
             p)))

(defn temp-dir!
  "Create a fresh directory under the system temp location and return its path."
  [prefix]
  #?(:clj (str (Files/createTempDirectory
                prefix (make-array java.nio.file.attribute.FileAttribute 0)))
     :cljs (str (.mkdtempSync (fs) (join (.tmpdir (os*)) prefix)))))

(defn restrict-perms!
  "Make `p` owner-only: `rwx------` for a directory, `rw-------` for a file.

   A dump with `:history? true` contains every value ever asserted, including
   retracted ones, so it is at least as sensitive as the database. Best-effort
   on both runtimes — a filesystem without POSIX permissions (Windows, FAT) is
   not an error, it just cannot honour the request."
  [p dir?]
  #?(:clj
     (try
       (when (.contains (.supportedFileAttributeViews (FileSystems/getDefault)) "posix")
         (Files/setPosixFilePermissions
          (.toPath (io/file p))
          (PosixFilePermissions/fromString (if dir? "rwx------" "rw-------"))))
       true
       (catch Exception _ false))
     :cljs
     (try (.chmodSync (fs) (str p) (if dir? 0700 0600)) true
          (catch :default _ false))))

;; ---------------------------------------------------------------------------
;; text — the manifest, which is EDN and deliberately not in the dump's codec

(defn slurp-text [p]
  #?(:clj (slurp (io/file p) :encoding "UTF-8")
     :cljs (.readFileSync (fs) (str p) "utf8")))

(defn spit-text! [p s]
  #?(:clj (spit (io/file p) s :encoding "UTF-8")
     :cljs (.writeFileSync (fs) (str p) s "utf8")))

;; ---------------------------------------------------------------------------
;; bytes

(defn read-bytes
  "The whole file. For a CHUNK, which is bounded by `:chunk-size`; never for a
   flat dump, which is what `puller` is for."
  [p]
  #?(:clj (let [f (io/file p)
                bs (byte-array (.length f))]
            (with-open [in (io/input-stream f)]
              (loop [off 0]
                (let [n (.read in bs off (- (alength bs) off))]
                  (when (pos? n) (recur (+ off n))))))
            bs)
     :cljs (let [b (.readFileSync (fs) (str p))]
             ;; a Node Buffer IS a Uint8Array, but it is a VIEW onto a shared
             ;; pool — `.slice` on a Buffer copies nothing, so take an explicit
             ;; copy rather than hand out a window that later writes may reuse
             (js/Uint8Array. (.-buffer b) (.-byteOffset b) (.-byteLength b)))))

;; `read-header-line` is written in terms of `puller`, which is defined at the
;; end of the namespace with the rest of the streaming API.
(declare puller)

(defn first-byte
  "The first byte of `p`, or nil if it is empty.

   How a dump is identified: a flat dump begins with `{`, the opening brace of
   its EDN manifest line, and anything else is a legacy dump. One byte, so this
   stays cheap on a file that may be gigabytes and may not be a dump at all."
  [p]
  #?(:clj (with-open [in (io/input-stream (io/file p))]
            (let [b (.read in)] (when-not (neg? b) b)))
     :cljs (let [fd (.openSync (fs) (str p) "r")]
             (try
               (let [buf (js/Uint8Array. 1)
                     n (.readSync (fs) fd buf 0 1 0)]
                 (when (pos? n) (aget buf 0)))
               (finally (.closeSync (fs) fd))))))

(defn- utf8-string [bs]
  #?(:clj (String. ^bytes bs "UTF-8")
     :cljs (.decode (js/TextDecoder. "utf-8") bs)))

(defn read-header-line
  "The first line of `p` as `{:line <string> :bytes <n>}`, where `n` counts the
   line AND its newline — the byte offset the records begin at.

   Returning the offset is the point. A flat dump is one EDN manifest line
   followed by a CBOR sequence, and the importer must step over the line without
   re-reading the file or guessing its length: `(:bytes …)` goes straight into
   `puller`'s `:skip`. Counting characters would be wrong the moment a manifest
   holds a non-ASCII ident.

   `:max` (default 8 MiB) bounds the search, so pointing this at a file with no
   newline fails rather than reading it all."
  ([p] (read-header-line p {}))
  ([p {:keys [max] :or {max (* 8 1024 1024)}}]
   (let [{:keys [pull close]} (puller p {:chunk-size 65536})]
     (try
       (loop [acc [] total 0]
         (if-let [blk (pull)]
           (let [n #?(:clj (alength ^bytes blk) :cljs (.-length blk))
                 idx (loop [i 0]
                       (cond (>= i n) -1
                             (= 10 (bit-and #?(:clj (aget ^bytes blk i)
                                               :cljs (aget blk i)) 0xff)) i
                             :else (recur (inc i))))]
             (if (neg? idx)
               (if (> (+ total n) max)
                 (throw (ex-info (str "No newline in the first " max " bytes of " p
                                      " — this does not look like a flat dump.")
                                 {:error :import/bad-manifest :file (str p)}))
                 (recur (conj acc blk) (+ total n)))
               (let [head #?(:clj (java.util.Arrays/copyOf ^bytes blk idx)
                             :cljs (.slice blk 0 idx))
                     parts (conj acc head)
                     joined #?(:clj (let [tot (reduce + (map #(alength ^bytes %) parts))
                                          out (byte-array tot)]
                                      (loop [ps (seq parts) off 0]
                                        (if ps
                                          (let [^bytes b (first ps)]
                                            (System/arraycopy b 0 out off (alength b))
                                            (recur (next ps) (+ off (alength b))))
                                          out)))
                                :cljs (let [tot (reduce + (map #(.-length %) parts))
                                            out (js/Uint8Array. tot)]
                                        (loop [ps (seq parts) off 0]
                                          (if ps
                                            (let [b (first ps)]
                                              (.set out b off)
                                              (recur (next ps) (+ off (.-length b))))
                                            out))))]
                 {:line (utf8-string joined) :bytes (+ total idx 1)})))
           (throw (ex-info (str "No newline found in " p
                                " — this does not look like a flat dump.")
                           {:error :import/bad-manifest :file (str p)}))))
       (finally (close))))))

(defn open-sink
  "Open `p` for writing bytes. Use with `write!` and `close-sink!`.

   `append?` is what lets a flat dump write its EDN manifest line first and then
   stream the CBOR sequence after it, without holding either."
  ([p] (open-sink p false))
  ([p append?]
   #?(:clj {:stream (io/output-stream (io/file p) :append (boolean append?))}
      :cljs {:fd (.openSync (fs) (str p) (if append? "a" "w"))})))

(defn write!
  "Append `bs` (bytes) to an open sink."
  [sink bs]
  #?(:clj (.write ^java.io.OutputStream (:stream sink) ^bytes bs)
     :cljs (.writeSync (fs) (:fd sink) bs)))

(defn write-text!
  "Append a UTF-8 string to an open sink — the flat dump's manifest line."
  [sink s]
  #?(:clj (.write ^java.io.OutputStream (:stream sink)
                  (.getBytes ^String s StandardCharsets/UTF_8))
     :cljs (.writeSync (fs) (:fd sink) s)))

(defn close-sink! [sink]
  #?(:clj (.close ^java.io.OutputStream (:stream sink))
     :cljs (.closeSync (fs) (:fd sink))))

(defn puller
  "Open `p` and return `{:pull (fn [] -> bytes|nil) :close (fn [])}`.

   `:pull` is exactly the source `boring/decode-seq-from` wants on
   ClojureScript, and wrapping the JVM's `InputStream` in the same shape means
   `migrate` has ONE spelling for streaming a dump instead of two. `:skip` bytes
   are dropped first, which is how a flat dump's reader steps over the EDN
   manifest line without re-reading the file.

   `fs.readSync` is genuinely synchronous, so the resulting lazy seq behaves as
   it does on the JVM. That is the whole reason a file dump can be portable
   while an arbitrary async source cannot."
  ([p] (puller p {}))
  ([p {:keys [chunk-size skip] :or {chunk-size 65536 skip 0}}]
   #?(:clj
      (let [in (io/input-stream (io/file p))]
        (when (pos? (long skip))
          (loop [remaining (long skip)]
            (when (pos? remaining)
              (let [n (.skip in remaining)]
                (when (pos? n) (recur (- remaining n)))))))
        {:pull (fn []
                 (let [buf (byte-array chunk-size)
                       n (.read in buf)]
                   (when (pos? n)
                     (if (= n chunk-size) buf (java.util.Arrays/copyOf buf n)))))
         :close (fn [] (.close in))})
      :cljs
      (let [fd (.openSync (fs) (str p) "r")
            pos (atom skip)]
        {:pull (fn []
                 (let [buf (js/Uint8Array. chunk-size)
                       n (.readSync (fs) fd buf 0 chunk-size @pos)]
                   (when (pos? n)
                     (swap! pos + n)
                     (if (= n chunk-size) buf (.slice buf 0 n)))))
         :close (fn [] (.closeSync (fs) fd))}))))
