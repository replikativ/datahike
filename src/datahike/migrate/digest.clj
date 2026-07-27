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
  "First 8 bytes (big-endian) of SHA-256(line-bytes) as a raw 64-bit long (the sign
   bit is just the top data bit; all arithmetic below treats it as unsigned)."
  ^long [^String line]
  (let [d (sha256-bytes (.getBytes line StandardCharsets/UTF_8))]
    (areduce d i acc (long 0)
             (if (< i 8) (bit-or (bit-shift-left acc 8) (bit-and (aget d i) 0xff)) acc))))

;; Semantic digest as a stateful accumulator so it composes with a streaming
;; reduction (one pass, bounded memory, order-independent). Everything is a 64-bit
;; long: two's-complement wraparound IS addition mod 2^64, and `%016x` renders a
;; long as unsigned hex.

(defn accumulator
  "Return a fresh accumulator {:xor long :sum long :count long}."
  [] {:xor 0 :sum 0 :count 0})

(defn add-line
  "Fold one record line into the accumulator."
  [acc ^String line]
  (let [h (record-hash8 line)]
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

(defn digest-lines
  "Convenience: fold a seq/reducible of record lines to a finalized digest."
  [lines]
  (finalize (reduce add-line (accumulator) lines)))
