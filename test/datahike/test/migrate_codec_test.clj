(ns datahike.test.migrate-codec-test
  "Byte-level conformance for the dump's value encoding.

   WHY BYTES AND NOT ROUND-TRIPS. A round-trip test proves the library agrees
   with itself; it cannot tell you whether another language could read the dump,
   which is the entire reason for choosing a standard codec. So the assertions
   here pin the exact octets, against the tag numbers RFC 8949 registers. Any
   conformant decoder — `cbor2`, `serde_cbor`, `cbor-java`, a future rune — must
   read these, and the vectors are the contract a codec swap has to satisfy.

   Verified once out-of-band with Python `cbor2` on these very bytes: `bigdec`
   arrives as `Decimal('1.50')` with its scale, `instant` as a tz-aware
   `datetime`, `uuid` as a `UUID`, `bytes` as `bytes`, and the bignum exactly.
   That is the property that justifies CBOR over an EDN-tagged encoding, where
   every value would need a Clojure reader.

   NOTE the dump currently encodes values as EDN-lines; CBOR is the open
   alternative (see `doc/import-export-design.md` §5.3) and the legacy reader for
   pre-existing dumps. These vectors are therefore the CONTRACT a move to CBOR
   would have to satisfy, and the evidence for making that move — not a
   description of today's dump."
  (:require [clojure.test :refer [deftest testing is]]
            [clj-cbor.core :as cbor]))

(defn- hex [x]
  (apply str (map #(format "%02x" %) (cbor/encode x))))

(deftest float-width-is-preserved-test
  ;; #633 at root: a `:db.type/double` must not come back as a Float. The trap is
  ;; an encoder that picks the shortest float that round-trips — RFC 8949's own
  ;; deterministic profile prescribes exactly that, and it would silently narrow
  ;; every double whose value happens to fit in f32.
  ;;
  ;; clj-cbor encodes by CLASS instead (`codec.clj`: `(instance? Float n)` →
  ;; writeFloat, else writeDouble), so the distinction survives. Pinned here
  ;; because it is a policy, not a format guarantee: a codec that "optimises"
  ;; floats reintroduces #633.
  (testing "a double encodes as f64 (0xfb) even when exactly f32-representable"
    (is (= "fb3ff8000000000000" (hex (double 1.5))))
    (is (= "fb4000000000000000" (hex (double 2.0)))
        "2.0 fits f32 exactly — shortest-form encoding would narrow it here"))
  (testing "a float encodes as f32 (0xfa)"
    (is (= "fa3fc00000" (hex (float 1.5)))))
  (testing "and the class survives a round trip"
    (is (instance? Double (cbor/decode (cbor/encode (double 2.0)))))
    (is (instance? Float (cbor/decode (cbor/encode (float 1.5)))))))

(deftest zero-and-special-floats-lose-their-width-test
  ;; KNOWN GAP, asserted so it cannot drift unnoticed. clj-cbor encodes zero,
  ;; NaN and ±Infinity as f16 (0xf9) regardless of class, and f16 decodes to
  ;; Float — so a `Double` 0.0 comes back a `Float`. That is #633 surviving for
  ;; exactly three values.
  ;;
  ;; It does NOT affect the dump as it stands: the dump encodes values as
  ;; EDN-lines, and a full export/import of `:db.type/double` values 0.0, 1.5 and
  ;; 2.0 restores all three as `Double` (measured). So this is a property of the
  ;; CBOR codec, recorded here because the codec choice is still open — if the
  ;; dump moves to CBOR, this is the one case that needs a width-preserving float
  ;; policy rather than clj-cbor's default, and a codec swap must decide it
  ;; consciously instead of inheriting it.
  (testing "zero collapses to f16 for both classes"
    (is (= "f90000" (hex (double 0.0))))
    (is (= "f90000" (hex (float 0.0))))
    (is (instance? Float (cbor/decode (cbor/encode (double 0.0))))
        "a Double zero decodes as Float — the residue"))
  (testing "NaN and infinity likewise"
    (is (instance? Float (cbor/decode (cbor/encode (double ##NaN)))))
    (is (instance? Float (cbor/decode (cbor/encode (double ##Inf)))))))

(deftest standard-tags-are-used-test
  ;; The reason a dump is readable elsewhere. Each of these is a tag REGISTERED
  ;; with IANA, so a foreign decoder produces a native value without knowing
  ;; anything about datahike. An EDN-tagged encoding (`#datahike/bytes "base64"`)
  ;; is portable in principle and Clojure-only in practice.
  (testing "tag 2 — positive bignum, arbitrary precision preserved"
    (is (= "c24d018ee90ff6c373e0ee4e3f0ad2"
           (hex (bigint 123456789012345678901234567890N))))
    (is (= 123456789012345678901234567890N
           (cbor/decode (cbor/encode (bigint 123456789012345678901234567890N))))))
  (testing "tag 4 — decimal fraction, and SCALE is part of the value"
    ;; 1.50M and 1.5M are different values for us; the encoding must distinguish
    ;; them, or a restored bigdec silently changes precision.
    (is (= "c482211896" (hex 1.50M)) "[-2 150]")
    (is (= "c482200f" (hex 1.5M)) "[-1 15]")
    (is (not= (hex 1.50M) (hex 1.5M)))
    (is (= 1.50M (cbor/decode (cbor/encode 1.50M))))
    (is (= 2 (.scale ^java.math.BigDecimal (cbor/decode (cbor/encode 1.50M))))))
  (testing "tag 1 — epoch instant"
    (is (= "c11a6955b900" (hex #inst "2026-01-01T00:00:00.000-00:00"))))
  (testing "tag 37 — uuid"
    (is (= "d8255000000000000000000000000000000001"
           (hex #uuid "00000000-0000-0000-0000-000000000001"))))
  (testing "major type 2 — byte strings are native, no base64 wrapper"
    (is (= "4400017fff" (hex (byte-array [0 1 127 -1]))))
    (is (= [0 1 127 -1] (vec (cbor/decode (cbor/encode (byte-array [0 1 127 -1]))))))))

(deftest integers-and-scalars-test
  (testing "small integers are compact"
    (is (= "182a" (hex (long 42)))))
  (testing "scalars survive"
    (is (= "text" (cbor/decode (cbor/encode "text"))))
    (is (= true (cbor/decode (cbor/encode true))))
    (is (nil? (cbor/decode (cbor/encode nil))))))

(deftest encoding-is-deterministic-test
  ;; Same input, identical bytes — what makes a dump signable and a re-export
  ;; diffable. Asserted for the whole fixture at once, since determinism is a
  ;; property of the encoder as a whole and not of any single value.
  (let [fixture {"d" (double 1.5) "f" (float 1.5) "n" (long 42)
                 "bi" (bigint 123456789012345678901234567890N) "bd" 1.50M
                 "i" #inst "2026-01-01T00:00:00.000-00:00"
                 "u" #uuid "00000000-0000-0000-0000-000000000001"
                 "s" "text" "b" true}]
    (is (= (hex fixture) (hex fixture)))
    (is (= (seq (cbor/encode fixture)) (seq (cbor/encode fixture))))))
