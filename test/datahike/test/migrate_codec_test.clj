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

   These vectors were originally the CONTRACT a move to CBOR would have to
   satisfy, measured against clj-cbor while the codec choice was open. The dump
   now IS CBOR — `datahike.migrate.cbor` over boring's `:archival` profile — so
   they are asserted against the real codec and describe today's dump.

   Almost every octet is unchanged from when these were measured against
   clj-cbor — the point of pinning a format rather than a library. Two moved:

     - the three float widths clj-cbor got WRONG (`float-width-survives-test`)
     - instants, from tag 1 (epoch) to tag 0 (RFC 3339 string), which is a
       deliberate choice the requirements permit rather than a defect"
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.migrate.cbor :as mcbor]
            [boring.core :as boring]))

(defn- enc ^bytes [x] (boring/encode x mcbor/opts))
(defn- dec* [^bytes bs] (boring/decode bs mcbor/opts))

(defn- hex [x]
  (apply str (map #(format "%02x" (bit-and % 0xff)) (enc x))))

(deftest float-width-is-preserved-test
  ;; #633 at root: a `:db.type/double` must not come back as a Float. The trap is
  ;; an encoder that picks the shortest float that round-trips — RFC 8949's own
  ;; deterministic profile prescribes exactly that, and it would silently narrow
  ;; every double whose value happens to fit in f32.
  ;;
  ;; boring's `:float-policy :preserve-width` encodes by CLASS, so the
  ;; distinction survives. Pinned here because it is a POLICY, not a format
  ;; guarantee: boring's own `:canonical` profile narrows these, which is exactly
  ;; why the dump uses `:archival` instead.
  (testing "a double encodes as f64 (0xfb) even when exactly f32-representable"
    (is (= "fb3ff8000000000000" (hex (double 1.5))))
    (is (= "fb4000000000000000" (hex (double 2.0)))
        "2.0 fits f32 exactly — shortest-form encoding would narrow it here"))
  (testing "a float encodes as f32 (0xfa)"
    (is (= "fa3fc00000" (hex (float 1.5)))))
  (testing "and the class survives a round trip"
    (is (instance? Double (dec* (enc (double 2.0)))))
    (is (instance? Float (dec* (enc (float 1.5)))))))

(deftest float-width-survives-test
  ;; This test used to be `zero-and-special-floats-lose-their-width-test`, and it
  ;; asserted the OPPOSITE: under clj-cbor, zero/NaN/±Infinity encoded as f16
  ;; (0xf9) regardless of class and decoded back as `Float`, so a `Double` 0.0
  ;; came back a `Float`. That was #633 surviving for exactly three values, and
  ;; it was the single measured reason the dump could not use clj-cbor.
  ;;
  ;; boring does not have it. The three values encode as f64 like any other
  ;; double, so the gap the old test guarded is closed rather than tracked, and
  ;; the assertion is inverted rather than deleted — a codec that reintroduces
  ;; the narrowing now fails here.
  (testing "zero keeps its width in both classes"
    (is (= "fb0000000000000000" (hex (double 0.0))) "f64, not f16")
    (is (= "fa00000000" (hex (float 0.0))) "f32, not f16")
    (is (instance? Double (dec* (enc (double 0.0))))
        "a Double zero decodes as Double — #633 closed"))
  (testing "NaN and infinity likewise"
    (is (instance? Double (dec* (enc (double ##NaN)))))
    (is (instance? Double (dec* (enc (double ##Inf)))))
    (is (instance? Double (dec* (enc (double ##-Inf)))))))

(deftest standard-tags-are-used-test
  ;; The reason a dump is readable elsewhere. Each of these is a tag REGISTERED
  ;; with IANA, so a foreign decoder produces a native value without knowing
  ;; anything about datahike. An EDN-tagged encoding (`#datahike/bytes "base64"`)
  ;; is portable in principle and Clojure-only in practice.
  (testing "tag 2 — positive bignum, arbitrary precision preserved"
    (is (= "c24d018ee90ff6c373e0ee4e3f0ad2"
           (hex (bigint 123456789012345678901234567890N))))
    (is (= 123456789012345678901234567890N
           (dec* (enc (bigint 123456789012345678901234567890N))))))
  (testing "tag 4 — decimal fraction, and SCALE is part of the value"
    ;; 1.50M and 1.5M are different values for us; the encoding must distinguish
    ;; them, or a restored bigdec silently changes precision.
    (is (= "c482211896" (hex 1.50M)) "[-2 150]")
    (is (= "c482200f" (hex 1.5M)) "[-1 15]")
    (is (not= (hex 1.50M) (hex 1.5M)))
    (is (= 1.50M (dec* (enc 1.50M))))
    (is (= 2 (.scale ^java.math.BigDecimal (dec* (enc 1.50M))))))
  (testing "tag 0 — RFC 3339 instant"
    ;; boring emits tag 0 (a date-time STRING) where clj-cbor emitted tag 1 (an
    ;; epoch integer). Both are registered and both are correct; DATAHIKE-
    ;; REQUIREMENTS §2 explicitly allows either — "0 is friendlier to non-Clojure
    ;; readers; 1 is more compact — either, but pick one and document it".
    ;;
    ;; This is the one vector the codec swap moved, and it moved by choice rather
    ;; than by defect. The cost is real and worth stating: 22 bytes against 6, on
    ;; every :db/txInstant, i.e. once per transaction in the dump.
    (is (= "c074323032362d30312d30315430303a30303a30305a"
           (hex #inst "2026-01-01T00:00:00.000-00:00")))
    (is (instance? java.util.Date (dec* (enc #inst "2026-01-01T00:00:00.000-00:00")))
        "and it still decodes to the class :db.type/instant uses"))
  (testing "tag 37 — uuid"
    (is (= "d8255000000000000000000000000000000001"
           (hex #uuid "00000000-0000-0000-0000-000000000001"))))
  (testing "major type 2 — byte strings are native, no base64 wrapper"
    (is (= "4400017fff" (hex (byte-array [0 1 127 -1]))))
    (is (= [0 1 127 -1] (vec (dec* (enc (byte-array [0 1 127 -1]))))))))

(deftest integers-and-scalars-test
  (testing "small integers are compact"
    (is (= "182a" (hex (long 42)))))
  (testing "scalars survive"
    (is (= "text" (dec* (enc "text"))))
    (is (= true (dec* (enc true))))
    (is (nil? (dec* (enc nil))))))

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
    (is (= (seq (enc fixture)) (seq (enc fixture))))
    ;; Two maps built in opposite orders encode identically — that is what
    ;; :archival's sorted keys buy. NOT compared against a sorted-map: boring
    ;; carries sortedness deliberately, so a sorted-map is a different value that
    ;; restores with its comparator rather than silently becoming a hash map.
    (is (= (hex (into {} (reverse (vec fixture))))
           (hex (into {} (vec fixture))))
        "insertion order does not change the bytes")))
