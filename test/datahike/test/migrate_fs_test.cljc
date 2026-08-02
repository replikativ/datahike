(ns datahike.test.migrate-fs-test
  "The filesystem seam, run identically on the JVM and on Node.

   The point of the seam is that a dump directory is a FORMAT — plain files
   anyone can read, byte-identical whoever wrote them. So these tests assert on
   bytes and on directory contents, not on whether a call returned without
   throwing."
  (:require #?(:clj [clojure.test :refer [deftest testing is]]
               :cljs [cljs.test :refer [deftest testing is]])
            [datahike.migrate.fs :as fs]
            [datahike.migrate.digest :as dig]))

(defn- ->bytes [xs]
  #?(:clj (byte-array (map unchecked-byte xs))
     :cljs (js/Uint8Array.from (into-array xs))))

(defn- bytes->vec [bs]
  (mapv #(bit-and % 0xff) #?(:clj (seq bs) :cljs (array-seq bs))))

(defn- with-tmp
  "Run `f` with a fresh directory, removing it afterwards even on failure."
  [f]
  (let [d (fs/temp-dir! "dh-fs-test-")]
    (try (f d)
         (finally
           (doseq [n (or (fs/list-names d) [])]
             (let [p (fs/join d n)]
               (doseq [n2 (or (fs/list-names p) [])] (fs/delete! (fs/join p n2)))
               (fs/delete! p)))
           (fs/delete! d)))))

;; ---------------------------------------------------------------------------

(deftest a-directory-round-trips
  (with-tmp
    (fn [d]
      (testing "the operations the chunked writer performs, in the order it
                performs them: make a directory, write a temp chunk, rename it
                into place, and record its size."
        (is (fs/directory? d))
        (is (empty? (fs/list-names d)))
        (let [tmp (fs/join d "datoms-000001.cbor.tmp")
              final (fs/join d "datoms-000001.cbor")
              payload (->bytes (range 256))
              sink (fs/open-sink tmp)]
          (fs/write! sink payload)
          (fs/close-sink! sink)
          (is (fs/exists? tmp))
          (is (fs/rename! tmp final))
          (is (not (fs/exists? tmp)) "the temp name is gone after the rename")
          (is (fs/exists? final))
          (is (= 256 (fs/file-size final)))
          (is (= ["datoms-000001.cbor"] (fs/list-names d)))
          (is (= (bytes->vec payload) (bytes->vec (fs/read-bytes final)))))))))

(deftest text-round-trips-as-utf8
  (with-tmp
    (fn [d]
      (testing "the manifest is EDN and may hold non-ASCII — an attribute ident
                or a database name. Both runtimes must agree it is UTF-8."
        (let [p (fs/join d "manifest.edn")
              s "{:name \"Ünïcodé — ✓\" :n 42}"]
          (fs/spit-text! p s)
          (is (= s (fs/slurp-text p))))))))

;; ---------------------------------------------------------------------------
;; streaming, which is the reason the seam exists rather than a `read-bytes`

(deftest the-puller-streams-a-file-in-bounded-pieces
  (with-tmp
    (fn [d]
      (testing "`puller` hands back at most `:chunk-size` bytes at a time and
                nil at the end — the source shape `boring/decode-seq-from` takes,
                so a chunk larger than memory reads through one handle."
        (let [p (fs/join d "big.bin")
              payload (->bytes (map #(mod % 256) (range 1000)))
              sink (fs/open-sink p)]
          (fs/write! sink payload)
          (fs/close-sink! sink)
          (let [{:keys [pull close]} (fs/puller p {:chunk-size 128})
                blocks (loop [acc []]
                         (if-let [b (pull)] (recur (conj acc b)) acc))]
            (close)
            (is (= 8 (count blocks)) "1000 bytes in 128-byte pieces is 8 reads")
            (is (every? #(<= (count (bytes->vec %)) 128) blocks)
                "no piece exceeds the requested size")
            (is (= (bytes->vec payload) (vec (mapcat bytes->vec blocks)))
                "and reassembling them gives the file back exactly")))))))

(deftest the-puller-of-an-empty-file-yields-nothing
  (with-tmp
    (fn [d]
      (testing "a dump with no records must read as an empty sequence rather
                than raising — the same case `decode-seq-from` handles."
        (let [p (fs/join d "empty.bin")
              sink (fs/open-sink p)]
          (fs/close-sink! sink)
          (let [{:keys [pull close]} (fs/puller p)]
            (is (nil? (pull)))
            (close)))))))

(deftest streamed-bytes-hash-the-same-as-the-whole-file
  (with-tmp
    (fn [d]
      (testing "hashing while writing must equal hashing what was written.

                This is the property the chunk SHA-256 rests on: `write-chunk-stream!`
                never re-reads the file, so if the incremental digest and the
                file disagreed, every manifest would record a hash that
                verification could not reproduce."
        (let [p (fs/join d "hashed.bin")
              pieces [(->bytes (range 100)) (->bytes [255]) (->bytes (range 50 200))]
              sink (fs/open-sink p)
              acc (reduce (fn [a bs] (fs/write! sink bs) (dig/sha256-update! a bs))
                          (dig/sha256-accumulator)
                          pieces)]
          (fs/close-sink! sink)
          (is (= (dig/sha256-hex (fs/read-bytes p))
                 (dig/sha256-finalize acc))))))))

;; ---------------------------------------------------------------------------

(deftest paths-and-interrogation
  (with-tmp
    (fn [d]
      (testing "the small operations the import path uses to decide what a dump
                IS, and to refuse a manifest naming a chunk outside it."
        (let [sub (fs/join d "store-refs")]
          (fs/mkdirs! sub)
          (is (fs/directory? sub))
          (is (= "store-refs" (fs/file-name sub)))
          (is (= (fs/canonical d) (fs/canonical (fs/parent sub))))
          (is (not (fs/exists? (fs/join d "nope"))))
          (is (false? (boolean (fs/directory? (fs/join d "nope"))))
              "a missing path is not a directory, and asking is not an error")
          (testing "canonical resolves .. — the escape a hostile manifest would use"
            (is (= (fs/canonical d)
                   (fs/canonical (fs/join sub ".."))))))))))

(deftest restricting-permissions-is-best-effort-not-fatal
  (with-tmp
    (fn [d]
      (testing "a dump with :history? true holds every value ever asserted, so it
                is made owner-only. A filesystem that cannot express that is not
                an error — the call reports what happened and the export goes on."
        (let [p (fs/join d "secret.bin")
              sink (fs/open-sink p)]
          (fs/close-sink! sink)
          (is (contains? #{true false} (fs/restrict-perms! p false)))
          (is (contains? #{true false} (fs/restrict-perms! d true)))
          (is (fs/exists? p) "and the file is still there either way"))))))

(deftest deleting-a-missing-path-is-not-an-error
  (with-tmp
    (fn [d]
      (testing "cleanup runs over paths that may never have been created — a
                temp file for a chunk that was never written, for instance."
        (is (contains? #{true false} (fs/delete! (fs/join d "never-existed")))))))
  )
