(ns datahike.test.blob-identity-test
  "`blob-id` must be the SAME uuid for the same bytes on every platform.

   This is not a nicety. A blob's content id is simultaneously three things: the
   `:db.type/store-ref` datom VALUE, the konserve KEY the bytes live under, and
   the FILENAME in a dump's `store-refs/` directory. If the JVM and ClojureScript
   disagree by one bit then a blob written on one is unfindable from the other,
   `verify-blob` reports corruption on intact bytes, and a dump exported from a
   browser cannot be imported by a server.

   hasch's ClojureScript side converts typed arrays big-endian, explicitly
   \"matching the JVM's ByteBuffer\", so the intent is right. Intent is not the
   test. The `xor64+sum64` semantic digest had exactly the same obviously-correct
   intent and was silently wrong on ClojureScript, because numbers there are
   doubles and bitwise operators truncate to 32 bits — so the constants below are
   literals both platforms are checked against, never one platform compared to
   the other."
  (:require #?(:clj [clojure.test :refer [deftest testing is]]
               :cljs [cljs.test :refer [deftest testing is]])
            [datahike.blob :as blob]))

(defn- bs
  "The same byte sequence on both platforms."
  [xs]
  #?(:clj (byte-array (map unchecked-byte xs))
     :cljs (js/Uint8Array.from (into-array xs))))

(deftest blob-ids-are-identical-across-platforms
  (testing "pinned literals, measured on the JVM and asserted on both."
    (is (= "3a6d9b14-4373-57ae-86b6-df0791db25ff"
           (str (blob/blob-id (bs (range 10)))))
        "bytes 0..9")
    (is (= "038a9bfd-7f08-52c4-8965-3040ffddd9e3"
           (str (blob/blob-id (bs [104 101 108 108 111]))))
        "the UTF-8 bytes of \"hello\"")))

(deftest blob-ids-are-content-addressed
  (testing "the properties the whole store-ref design rests on: identical bytes
            give one id (so re-uploading is idempotent and two entities sharing
            content share one object), and different bytes do not collide."
    (is (= (blob/blob-id (bs [1 2 3])) (blob/blob-id (bs [1 2 3])))
        "same bytes, same id — twice over, not memoised by identity")
    (is (not= (blob/blob-id (bs [1 2 3])) (blob/blob-id (bs [1 2 4]))))
    (is (not= (blob/blob-id (bs [1 2 3])) (blob/blob-id (bs [3 2 1])))
        "order matters — a multiset hash would be wrong here")
    (is (not= (blob/blob-id (bs [])) (blob/blob-id (bs [0])))
        "empty is not the same as a zero byte")))

(deftest high-bytes-survive-the-sign-boundary
  (testing "the JVM's bytes are SIGNED and ClojureScript's are not, so anything
            above 0x7f is where a conversion mistake would show — and it would
            show as two platforms disagreeing about a filename, which is the
            failure this namespace exists to prevent."
    (let [a (blob/blob-id (bs [0x00 0x7f 0x80 0xff]))
          b (blob/blob-id (bs [0x00 0x7f 0x80 0xfe]))]
      (is (not= a b) "0xff and 0xfe are distinguished")
      (is (= 36 (count (str a))) "and it is a well-formed uuid"))))
