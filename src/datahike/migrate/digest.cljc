(ns ^:no-doc datahike.migrate.digest
  "Two independent integrity mechanisms for dumps, with distinct threat models.

   * `sha256` — per-chunk cryptographic hash recorded in the manifest. This is the
     tamper-evidence control: `verify` recomputes it, and because a dump is
     deterministic it can be signed by external tooling.

   * semantic digest `{:xor :sum :count}` — an order-independent fold over the
     record lines, so a dump can be compared against a live, *id-remapped* database
     without solving id equality. It is **not** a security control (it is linear,
     hence forgeable); its only job is cheap structural comparison.

   ## Why this is `.cljc`, and what was hard about it

   A dump written on the JVM must verify on node and vice versa, so every number
   here has to come out bit-identical on both platforms. Two separate problems,
   and only one of them was the obvious one:

   * **SHA-256.** `goog.crypt.Sha256` is synchronous and incremental
     (`.update` repeatedly, then `.digest`), which is the same shape as
     `MessageDigest`. It ships in the Closure library datahike already depends
     on, works in node AND the browser, and is the precedent hasch set with
     `goog.crypt.Sha512`. Web Crypto's `SubtleCrypto` was the alternative and is
     unusable here: it is Promise-returning and one-shot, with no incremental
     digest object anywhere in the standard.

   * **64-bit arithmetic — the real problem.** The semantic digest is
     `xor64+sum64`, and ClojureScript has no 64-bit integer: numbers are doubles
     and bitwise operators truncate to 32 bits. A naive port would have produced
     a DIFFERENT digest on node for the same dump, silently, which is precisely
     the comparison this exists to make. `goog.math.Long` gives exact 64-bit xor
     and wrapping add, and rendering from its two 32-bit halves reproduces the
     JVM's `%016x` exactly — `format` prints a long as unsigned 64-bit hex, and
     two unsigned 8-digit halves concatenate to the same string.

   `datahike.test.migrate-digest-test` pins the digests of a fixed corpus as
   literal strings, so the two platforms are checked against the same constants
   rather than against each other."
  #?(:clj (:require [clojure.java.io :as io])
     :cljs (:require [goog.crypt :as gcrypt]
                     [goog.crypt.Sha256]
                     [goog.math.Long :as glong]))
  #?(:clj (:import [java.security MessageDigest]
                   [java.nio.charset StandardCharsets])
     :cljs (:import [goog.crypt Sha256]
                    [goog.math Long])))

(defn sha256-bytes
  "SHA-256 of `bs`. Returns platform-native bytes: `byte[]` on the JVM (signed),
   a JS number array on ClojureScript (unsigned). `hex` masks either to 0-255, so
   the difference does not escape this namespace."
  [bs]
  #?(:clj  (.digest (MessageDigest/getInstance "SHA-256") ^bytes bs)
     :cljs (let [h (Sha256.)]
             (.update h bs)
             (.digest h))))

(defn hex
  "Lowercase hex of a byte sequence, two digits per byte."
  [bs]
  #?(:clj
     (let [^bytes bs bs
           sb (StringBuilder. (* 2 (alength bs)))]
       (dotimes [i (alength bs)]
         (let [b (bit-and (aget bs i) 0xff)]
           (when (< b 16) (.append sb \0))
           (.append sb (Integer/toHexString b))))
       (.toString sb))
     :cljs
     ;; A JS array joined at the end, rather than repeated `str`: a chunk hash
     ;; runs this over every byte of a chunk, and string concatenation in a loop
     ;; is the one shape that turns an O(n) hash into an O(n^2) one.
     (let [n (alength bs)
           out (array)]
       (dotimes [i n]
         (let [b (bit-and (aget bs i) 0xff)]
           (.push out (if (< b 16) (str "0" (.toString b 16)) (.toString b 16)))))
       (.join out ""))))

(defn sha256-hex
  "Hex SHA-256 of a UTF-8 string or a byte array."
  [x]
  (hex (sha256-bytes
        (if (string? x)
          #?(:clj (.getBytes ^String x StandardCharsets/UTF_8)
             :cljs (gcrypt/stringToUtf8ByteArray x))
          x))))

#?(:clj
   (defn sha256-file-hex
     "Streaming hex SHA-256 of a file's bytes — bounded memory, for large chunks.

      JVM only, and it is the filesystem dump medium that needs it. The portable
      medium is a konserve store, where a chunk arrives as one value already
      bounded by `:chunk-size`, so `sha256-hex` over those bytes is the whole job."
     [file]
     (let [md (MessageDigest/getInstance "SHA-256")
           buf (byte-array 65536)]
       (with-open [in (io/input-stream file)]
         (loop []
           (let [n (.read in buf)]
             (when (pos? n) (.update md buf 0 n) (recur)))))
       (hex (.digest md)))))

;; ---------------------------------------------------------------------------
;; incremental SHA-256
;;
;; The chunk hash is computed WHILE records stream past, never by re-reading
;; what was written: a flat dump is one file of unbounded size, and hashing it
;; afterwards would mean either holding it or reading it twice. Both platforms
;; have a native incremental digest — `MessageDigest` and `goog.crypt.Sha256` —
;; with the same update/digest shape, so this is a rename rather than a port.
;;
;; Stateful and mutable, unlike the semantic `accumulator` above, which is a
;; value. That asymmetry is the underlying objects', not a choice: neither
;; platform offers a persistent digest, and pretending otherwise by copying the
;; state on every record would cost more than the hash.

(defn sha256-accumulator
  "A fresh incremental SHA-256. Feed it with `sha256-update!`, finish with
   `sha256-finalize`. NOT a value — `sha256-update!` mutates and returns it."
  []
  #?(:clj (MessageDigest/getInstance "SHA-256")
     :cljs (Sha256.)))

(defn sha256-update!
  "Fold `bs` into the digest and return it."
  [acc bs]
  #?(:clj (doto ^MessageDigest acc (.update ^bytes bs))
     :cljs (doto acc (.update bs))))

(defn sha256-finalize
  "The hex digest. The accumulator must not be used afterwards — on the JVM
   `.digest` resets it, and relying on that would be relying on a detail."
  [acc]
  #?(:clj (hex (.digest ^MessageDigest acc))
     :cljs (hex (.digest acc))))

;; ---------------------------------------------------------------------------
;; 64-bit arithmetic
;;
;; On the JVM these are primitive longs and the operators are the language's. On
;; ClojureScript they are `goog.math.Long`, which is exact where a double is not:
;; `Number.MAX_SAFE_INTEGER` is 2^53, so a plain JS number cannot even hold the
;; 64-bit sum this digest accumulates, let alone wrap it correctly.

;; The ClojureScript branches are hinted `^Long` throughout — that is
;; `goog.math.Long`, not `java.lang.Long`, since this file's `:import` only takes
;; effect on that side. Without the hints `:advanced` cannot infer the target and
;; `:infer-externs` emits an extern for every method, which then blocks renaming
;; of goog's own code.

(defn- zero64 []
  #?(:clj 0 :cljs (.getZero Long)))

(defn- xor64 [a b]
  #?(:clj (bit-xor (long a) (long b)) :cljs (.xor ^Long a ^Long b)))

(defn- add64
  "Wrapping 64-bit add. Two's-complement wraparound IS addition mod 2^64 on both
   platforms — `unchecked-add` on the JVM, and `goog.math.Long.add` by
   construction."
  [a b]
  #?(:clj (unchecked-add (long a) (long b)) :cljs (.add ^Long a ^Long b)))

(defn- hex64
  "Unsigned 16-digit hex, matching the JVM's `(format \"%016x\" some-long)`.

   The ClojureScript branch renders the two 32-bit halves separately rather than
   calling `.toString 16` on the Long: that method is SIGNED, so a negative value
   comes back with a leading minus rather than as the unsigned 64-bit pattern the
   manifest records."
  [x]
  #?(:clj (format "%016x" (long x))
     :cljs (let [pad8 (fn [n]
                        (let [s (.toString (unsigned-bit-shift-right n 0) 16)]
                          (str (.repeat "0" (- 8 (.-length s))) s)))]
             (str (pad8 (.getHighBits ^Long x)) (pad8 (.getLowBits ^Long x))))))

(defn- record-hash8
  "First 8 bytes (big-endian) of SHA-256(record-bytes) as a raw 64-bit value (the
   sign bit is just the top data bit; all arithmetic treats it as unsigned).

   Takes the ENCODED BYTES rather than a string. Under the EDN codec a record was
   a line and its identity was its UTF-8 text; under CBOR it is a byte array, and
   `:archival` makes those bytes a function of the record alone. So the digest is
   now a property of the DATA rather than of the text it happened to be rendered
   as, which is what lets a dump be compared across codecs at all."
  [bs]
  (let [d (sha256-bytes bs)]
    #?(:clj
       (areduce ^bytes d i acc (long 0)
                (if (< i 8) (bit-or (bit-shift-left acc 8) (bit-and (aget ^bytes d i) 0xff)) acc))
       :cljs
       ;; `bit-shift-left` is 32-bit here, so each half is built independently and
       ;; may come out negative — which is exactly what `fromBits` wants, since it
       ;; takes signed 32-bit words.
       (let [word (fn [o] (bit-or (bit-shift-left (bit-and (aget d o) 0xff) 24)
                                  (bit-shift-left (bit-and (aget d (+ o 1)) 0xff) 16)
                                  (bit-shift-left (bit-and (aget d (+ o 2)) 0xff) 8)
                                  (bit-and (aget d (+ o 3)) 0xff)))]
         (.fromBits Long (word 4) (word 0))))))

;; Semantic digest as a stateful accumulator so it composes with a streaming
;; reduction (one pass, bounded memory, order-independent).

(defn accumulator
  "Return a fresh accumulator {:xor :sum :count}."
  [] {:xor (zero64) :sum (zero64) :count 0})

(defn add-record
  "Fold one record's encoded bytes into the accumulator."
  [acc bs]
  (let [h (record-hash8 bs)]
    {:xor   (xor64 (:xor acc) h)
     :sum   (add64 (:sum acc) h)
     :count (inc (long (:count acc)))}))

(defn finalize
  "Render an accumulator to the manifest shape: unsigned hex xor + hex sum + count."
  [acc]
  {:algo  :xor64+sum64
   :xor   (hex64 (:xor acc))
   :sum   (hex64 (:sum acc))
   :count (:count acc)})

(defn digest-records
  "Convenience: fold a seq/reducible of encoded record bytes to a finalized digest."
  [records]
  (finalize (reduce add-record (accumulator) records)))
