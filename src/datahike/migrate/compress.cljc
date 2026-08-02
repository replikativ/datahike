(ns ^:no-doc datahike.migrate.compress
  "Chunk compression for dumps. gzip, or nothing.

   ## Why gzip is on by default

   A dump is far more compressible than it looks. Measured on a 220k-datom
   database with history: 6.04 MB raw at 27.4 bytes per datom, 856 KB gzipped —
   **7x**. The redundancy is not inside a record, it is ACROSS records: the same
   attribute idents, sequential entity ids and shared transaction ids repeat
   millions of times.

   CBOR's own stringref extension cannot reach that. Each record is its own
   top-level item, so a per-record stringref namespace pays for its opening tag
   and never gets any reuse — measured, `:archival` (stringref off) comes out 10%
   SMALLER than `:clojure` (stringref on) at this granularity. Compression is the
   only mechanism that can exploit the dump's dominant redundancy, which is why
   it is a default rather than an option.

   ## Why gzip and not zstd

   zstd compresses this data 9-12x rather than 7x, and it is genuinely well
   supported — Python 3.14 stdlib, Node 22.15+, Go, and `zstd-jni` on the JVM
   with prebuilt natives for 20+ platforms. It was still the wrong choice here:

   * datahike builds a GraalVM native-image, and a JNI dependency there means
     JNI config, `--enable-native-access` and shipping or statically linking a
     `.so` — a recurring tax, not a one-time setup.
   * `zstd-jni` is 7.3 MB of jar for a library people embed.
   * A browser cannot decompress zstd without a WASM build. `DecompressionStream`
     handles gzip and deflate only.

   gzip is in the JDK and in Node, needs no dependency, survives native-image,
   and any language can read it. Anyone who wants the extra ratio can pipe an
   uncompressed dump through `zstd`, which is on every Linux box already.

   ## Multi-member, and why that is fine

   Neither runtime offers a STREAMING synchronous gzip — Node's `gzipSync` takes
   a whole buffer — so a chunk is compressed one block at a time and the blocks
   are concatenated. That is legal gzip: concatenated members are a single valid
   stream, and `gzip -dc`, Node's `gunzipSync` and Java's `GZIPInputStream` all
   read them transparently (verified on all three before this was built).

   The cost is measured, not assumed: at 1 MiB blocks it is **+0.1%** over
   whole-file gzip. 64 KiB blocks cost 2.4%, which is why the block is a
   megabyte.

   ## The hash covers UNCOMPRESSED bytes

   `:sha256` in the manifest is over the records, not over the stored file. That
   is deliberate. Compressed output is not stable: Node's own streaming and
   one-shot zstd APIs disagree byte for byte (nodejs/node#58392), gzip encoders
   differ by level and implementation, and JVM and Node need not agree. Hashing
   the stored bytes would make the same database produce dumps that fail each
   other's verification. Hashing the records keeps the codec a transport detail —
   a dump compares equal across runtimes, and the default can change later
   without invalidating anything."
  #?(:clj (:import [java.io ByteArrayOutputStream ByteArrayInputStream]
                   [java.util.zip GZIPOutputStream GZIPInputStream])))

(def supported
  "Codecs this version can read. A dump naming anything else is refused by name
   rather than failing somewhere inside the decoder."
  #{:none :gzip})

(def default-codec
  "gzip, for the reasons in the namespace docstring. `:none` stays one option
   away for anyone who wants a dump a foreign tool can read with no step at all."
  :gzip)

(def default-block-size
  "1 MiB. Measured: +0.1% over whole-file gzip, against 2.4% at 64 KiB."
  (* 1024 1024))

(defn extension
  "The suffix a chunk file carries under `codec`, so a dump directory says what
   it is — `datoms-000001.cbor.gz` is a gzip file to every tool on the machine."
  [codec]
  (case codec
    :gzip ".gz"
    ""))

#?(:cljs (defn- zlib [] (js/require "zlib")))

(defn compress-bytes
  "Compress one block. Not the whole chunk — see the namespace docstring."
  [codec bs]
  (case codec
    :none bs
    :gzip #?(:clj (let [bos (ByteArrayOutputStream.)]
                    (with-open [gz (GZIPOutputStream. bos)]
                      (.write gz ^bytes bs))
                    (.toByteArray bos))
             :cljs (js/Uint8Array. (.gzipSync (zlib) bs)))))

(defn- decompress-raw [codec bs]
  (case codec
    :none bs
    :gzip #?(:clj (let [bos (ByteArrayOutputStream.)
                        buf (byte-array 65536)]
                    (with-open [in (GZIPInputStream. (ByteArrayInputStream. ^bytes bs))]
                      (loop []
                        (let [n (.read in buf)]
                          (when (pos? n) (.write bos buf 0 n) (recur)))))
                    (.toByteArray bos))
             :cljs (js/Uint8Array. (.gunzipSync (zlib) bs)))))

(defn decompress-bytes
  "Decompress a whole chunk, including one made of several concatenated members.

   A chunk that will not decompress is CORRUPT, and says so. Without this the
   caller got a raw `java.util.zip.ZipException` / Node `Error` naming neither
   the dump nor the chunk — the same shape of problem as an import surfacing a
   bare `FileNotFoundException`, and worse here because it is the ordinary way a
   damaged backup presents. A hash mismatch and a broken member are both
   corruption; they differ only in which one the reader notices first."
  ([codec bs] (decompress-bytes codec bs nil))
  ([codec bs ctx]
   (try
     (decompress-raw codec bs)
     (catch #?(:clj Exception :cljs :default) e
       (throw (ex-info (str "Corrupt chunk: " (or (:file ctx) "<unknown>")
                            " could not be decompressed (" (name codec) ").")
                       (merge {:error :import/corrupt-chunk :compression codec} ctx)
                       e))))))

(defn check-supported!
  "Refuse a codec this version does not know, by name."
  [codec]
  (when-not (contains? supported codec)
    (throw (ex-info (str "This dump is compressed with " (pr-str codec)
                         ", which this version of datahike cannot read. Supported: "
                         (pr-str (vec (sort supported))) ".")
                    {:error :import/unsupported-compression :compression codec}))))
