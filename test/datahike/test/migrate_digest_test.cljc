(ns datahike.test.migrate-digest-test
  "The dump digests must be bit-identical on every platform, or the guarantee they
   exist to provide is void: a dump written on the JVM is verified on node, and a
   `verify` that recomputes a different number reports corruption on an intact
   dump.

   So the expected values here are LITERAL CONSTANTS, not a JVM run compared
   against a ClojureScript run. Both platforms are checked against the same
   strings, which means neither can drift by agreeing with the other, and a
   failure names which platform moved.

   The SHA-256 cases are the published RFC 6234 / FIPS 180-4 vectors, so they
   check the implementation against the standard rather than against itself.

   The semantic-digest cases are the ones that actually needed the work:
   `xor64+sum64` is 64-bit arithmetic, and ClojureScript has no 64-bit integer —
   numbers are doubles (exact only to 2^53) and bitwise operators truncate to 32
   bits. A plain port would have produced a different, silently wrong digest on
   node."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.migrate.digest :as dig]))

(defn- ba
  "Portable byte container. `sha256-bytes` and `hex` accept either a `byte[]` or a
   `Uint8Array`; nothing outside this namespace has to care which."
  [xs]
  #?(:clj  (byte-array (map unchecked-byte xs))
     :cljs (js/Uint8Array.from (into-array xs))))

;; ---------------------------------------------------------------------------
;; SHA-256 against the published vectors

(deftest sha256-matches-the-published-vectors
  (testing "FIPS 180-4 / RFC 6234 test vectors, so this checks the implementation
            against the STANDARD rather than against the other platform. The JVM
            uses MessageDigest and ClojureScript uses goog.crypt.Sha256; both have
            to land on these exact strings."
    (is (= "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
           (dig/sha256-hex (ba [])))
        "the empty input")
    (is (= "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
           (dig/sha256-hex "abc"))
        "\"abc\" — also exercises the UTF-8 string path, which is
         String.getBytes on the JVM and goog.crypt/stringToUtf8ByteArray here")
    (is (= "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
           (dig/sha256-hex (ba [0x61 0x62 0x63])))
        "the same input as raw bytes hashes the same as the string")))

(deftest hex-pads-every-byte-to-two-digits
  (testing "a byte below 0x10 must render as \"0f\", not \"f\".

            Worth its own test because the two platforms disagree about what a
            byte IS — the JVM's are signed (-1 for 0xff) and ClojureScript's are
            not — so both branches mask to 0-255 and both must pad."
    (is (= "000102030f107f80ff" (dig/hex (ba [0 1 2 3 15 16 127 128 255]))))))

;; ---------------------------------------------------------------------------
;; the semantic digest — where 64 bits actually matter

(def ^:private corpus
  [(ba [])
   (ba [0])
   (ba [255])
   (ba [1 2 3 4 5 6 7 8 9])
   (ba [0xde 0xad 0xbe 0xef])
   (ba (range 0 100))])

(deftest semantic-digest-is-pinned-across-platforms
  (testing "the exact xor/sum/count a dump manifest records.

            These constants are what makes the digest portable. If ClojureScript
            computed the sum in doubles it would agree for small inputs and
            diverge past 2^53 — which is most of them, since each record
            contributes a full 64-bit hash — so pinning the literal is the only
            check that catches it."
    (is (= {:algo :xor64+sum64
            :xor "81e847840458c0b9"
            :sum "5e333c6975c234b5"
            :count 6}
           (dig/digest-records corpus)))))

(deftest semantic-digest-is-order-independent
  (testing "the property the whole mechanism rests on: a dump is compared against
            a live, id-remapped database, where record order cannot be relied on.
            xor and wrapping add are both commutative, so reversing the corpus
            must change nothing."
    (is (= (dig/digest-records corpus)
           (dig/digest-records (reverse corpus))))
    (is (= (dig/digest-records corpus)
           (dig/digest-records (shuffle corpus))))))

(deftest single-record-digest-has-xor-equal-to-sum
  (testing "with one record, xor and sum are both just that record's hash — so
            this pins `record-hash8` itself, independently of the fold. It is the
            case that would catch a big-endian/little-endian mistake in the
            ClojureScript word assembly, where the two 32-bit halves are built
            separately."
    (let [d (dig/digest-records [(ba [255])])]
      (is (= "a8100ae6aa1940d0" (:xor d)))
      (is (= "a8100ae6aa1940d0" (:sum d)))
      (is (= 1 (:count d))))))

(deftest accumulator-composes-with-a-streaming-reduction
  (testing "folding one record at a time equals digesting the whole seq — the
            property `write-chunks!` depends on, since it accumulates while
            streaming and never holds the corpus."
    (is (= (dig/digest-records corpus)
           (dig/finalize (reduce dig/add-record (dig/accumulator) corpus))))))

(deftest incremental-sha256-equals-the-one-shot
  (testing "the chunk hash is computed while records stream past, never by
            re-reading the file — so the incremental digest has to agree with
            `sha256-hex` over the same bytes concatenated, on both platforms.

            Fed in uneven pieces on purpose: a hasher that only worked when
            updates were block-aligned would pass a single-update test."
    (let [pieces [(ba [1]) (ba (range 0 100)) (ba []) (ba [255 254 253])
                  (ba (range 0 200))]
          joined (ba (mapcat #?(:clj seq :cljs array-seq) pieces))
          acc (reduce dig/sha256-update! (dig/sha256-accumulator) pieces)]
      (is (= (dig/sha256-hex joined) (dig/sha256-finalize acc))))))

(deftest incremental-sha256-of-nothing-is-the-empty-digest
  (testing "a zero-record chunk still gets a well-formed hash, and it is the
            published empty-input vector rather than anything special-cased."
    (is (= "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
           (dig/sha256-finalize (dig/sha256-accumulator))))))

(deftest empty-digest-is-zero-not-nil
  (testing "a dump with no records still gets a well-formed digest, so `verify`
            compares numbers rather than nils."
    (is (= {:algo :xor64+sum64
            :xor "0000000000000000"
            :sum "0000000000000000"
            :count 0}
           (dig/digest-records [])))))
