(ns ^:no-doc datahike.migrate.digest
  "Two independent integrity mechanisms for dumps, with distinct threat models.

   * `sha256` — per-chunk cryptographic hash recorded in the manifest. This is the
     tamper-evidence control: `verify` recomputes it, and because a dump is
     deterministic it can be signed by external tooling.

   * semantic digest `{:xor :sum :count}` — an order-independent fold over the
     record lines, so a dump can be compared against a live, *id-remapped* database
     without solving id equality. It is **not** a security control (it is linear,
     hence forgeable); its only job is cheap structural comparison."
  (:require [clojure.java.io :as io])
  (:import [java.security MessageDigest]
           [java.nio.charset StandardCharsets]))

(defn sha256-bytes ^bytes [^bytes bs]
  (.digest (MessageDigest/getInstance "SHA-256") bs))

(defn ^String hex [^bytes bs]
  (let [sb (StringBuilder. (* 2 (alength bs)))]
    (dotimes [i (alength bs)]
      (let [b (bit-and (aget bs i) 0xff)]
        (when (< b 16) (.append sb \0))
        (.append sb (Integer/toHexString b))))
    (.toString sb)))

(defn sha256-hex
  "Hex SHA-256 of a UTF-8 string or a byte array."
  [x]
  (hex (sha256-bytes (if (string? x) (.getBytes ^String x StandardCharsets/UTF_8) x))))

(defn sha256-file-hex
  "Streaming hex SHA-256 of a file's bytes — bounded memory, for large chunks."
  [file]
  (let [md (MessageDigest/getInstance "SHA-256")
        buf (byte-array 65536)]
    (with-open [in (io/input-stream file)]
      (loop []
        (let [n (.read in buf)]
          (when (pos? n) (.update md buf 0 n) (recur)))))
    (hex (.digest md))))

(defn- record-hash8
  "First 8 bytes (big-endian) of SHA-256(record-bytes) as a raw 64-bit long (the
   sign bit is just the top data bit; all arithmetic below treats it as unsigned).

   Takes the ENCODED BYTES rather than a string. Under the EDN codec a record was
   a line and its identity was its UTF-8 text; under CBOR it is a byte array, and
   `:archival` makes those bytes a function of the record alone. So the digest is
   now a property of the DATA rather than of the text it happened to be rendered
   as, which is what lets a dump be compared across codecs at all."
  ^long [^bytes bs]
  (let [d (sha256-bytes bs)]
    (areduce d i acc (long 0)
             (if (< i 8) (bit-or (bit-shift-left acc 8) (bit-and (aget d i) 0xff)) acc))))

;; Semantic digest as a stateful accumulator so it composes with a streaming
;; reduction (one pass, bounded memory, order-independent). Everything is a 64-bit
;; long: two's-complement wraparound IS addition mod 2^64, and `%016x` renders a
;; long as unsigned hex.

(defn accumulator
  "Return a fresh accumulator {:xor long :sum long :count long}."
  [] {:xor 0 :sum 0 :count 0})

(defn add-record
  "Fold one record's encoded bytes into the accumulator."
  [acc ^bytes bs]
  (let [h (record-hash8 bs)]
    {:xor   (bit-xor (long (:xor acc)) h)
     :sum   (unchecked-add (long (:sum acc)) h)
     :count (unchecked-inc (long (:count acc)))}))

(defn finalize
  "Render an accumulator to the manifest shape: unsigned hex xor + hex sum + count."
  [acc]
  {:algo  :xor64+sum64
   :xor   (format "%016x" (long (:xor acc)))
   :sum   (format "%016x" (long (:sum acc)))
   :count (:count acc)})

(defn digest-records
  "Convenience: fold a seq/reducible of encoded record bytes to a finalized digest."
  [records]
  (finalize (reduce add-record (accumulator) records)))
