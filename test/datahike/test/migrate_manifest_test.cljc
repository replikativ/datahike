(ns datahike.test.migrate-manifest-test
  "The parts of a dump that are a pure function of a manifest — capability
   checking, the memory estimate, value normalisation, codec resolution.

   These run on BOTH platforms deliberately. When `datahike.migrate.manifest` was
   extracted it acquired ClojureScript branches — typed arrays instead of
   `Class/forName \"[F\"`, no provenance macro, no heap ceiling — and nothing on
   cljs required the namespace, so none of them were compiled, let alone run. A
   portable namespace nobody loads on the other platform is untested portability
   wearing the word `.cljc`.

   Everything here works on a manifest MAP rather than a database, which is what
   makes it runnable without a store, a filesystem, or an async writer."
  (:require #?(:clj [clojure.test :refer [deftest testing is]]
               :cljs [cljs.test :refer [deftest testing is]])
            [datahike.migrate.manifest :as mman]
            [hasch.core :as hasch]))

;; ---------------------------------------------------------------------------

(deftest blob-plan-key-is-namespace-independent
  (testing "the constant that replaced an auto-resolved `::blob-plan`.

            `build-manifest` moved namespaces and `::blob-plan` silently became
            `:datahike.migrate.manifest/blob-plan` while the writer still set
            `:datahike.migrate/blob-plan`. The plan stopped being seen, the
            manifest declared no `:store-refs` and no blob capabilities, and
            `verify` reported `:ok? true` for a dump missing its blobs. Pinning
            the literal keeps it from drifting on the next move."
    (is (= :datahike.migrate/blob-plan mman/blob-plan-key))))

(deftest capabilities-are-checked-by-name
  (testing "a dump declaring something we cannot interpret is refused, and the
            error names WHICH capability — actionable where a version mismatch
            is not."
    (is (nil? (mman/check-capabilities! {})) "no :requires — a dump predating them")
    (is (nil? (mman/check-capabilities!
               {:requires [:datahike.migrate/cbor-seq :datahike.migrate/history]}))
        "capabilities we have")
    (let [e (try (mman/check-capabilities!
                  {:requires [:datahike.migrate/cbor-seq :datahike.migrate/warp-drive]})
                 nil
                 (catch #?(:clj clojure.lang.ExceptionInfo :cljs js/Error) e e))]
      (is (some? e))
      (is (= :import/unsupported-capabilities (:error (ex-data e))))
      (is (= [:datahike.migrate/warp-drive] (:missing (ex-data e)))
          "only the capability we lack, not the whole list"))))

(deftest value-types-are-capabilities
  (testing "value-type capabilities are derived from `ds/builtin-value-types`, so
            a type this version knows is accepted without anyone maintaining a
            table — and one it does not know is refused by construction."
    (is (nil? (mman/check-capabilities! {:requires [:db.type/string :db.type/ref]})))
    (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                 (mman/check-capabilities! {:requires [:db.type/quaternion]})))))

;; ---------------------------------------------------------------------------

(deftest byte-sizes-render-without-format
  (testing "`format` does not exist in ClojureScript, so this is hand-rolled —
            which means it needs pinning on both platforms rather than trusting
            a format string."
    (is (= "512 KB" (mman/bytes->human (* 512 1024))))
    (is (= "1 MB" (mman/bytes->human (* 1024 1024))))
    (is (= "512 MB" (mman/bytes->human (* 512 1024 1024))))
    (is (= "1.5 GB" (mman/bytes->human (long (* 1.5 1024 1024 1024)))))
    (is (= "0 KB" (mman/bytes->human 0)))))

(deftest the-estimate-counts-the-chunk-and-uses-uncompressed-sizes
  (testing "the two bugs the estimate had.

            It counted the id-map and the batch but not the CHUNK held while
            decoding — below about a million entities the largest of the three.
            And it derived bytes-per-record from the STORED size, so turning gzip
            on silently shrank the estimate by the compression ratio. Chunks now
            carry `:raw-bytes` beside `:bytes`."
    (let [compressed {:stats {:datom-count 1000 :max-eid 500 :max-tx 536870920}
                      :chunks [{:count 1000 :bytes 1000 :raw-bytes 7000}]}
          verbatim   {:stats {:datom-count 1000 :max-eid 500 :max-tx 536870920}
                      :chunks [{:count 1000 :bytes 7000 :raw-bytes 7000}]}
          ec (mman/estimate-from-manifest compressed 1000 100)
          ev (mman/estimate-from-manifest verbatim 7000 100)]
      (is (pos? (:chunk-bytes ec)) "the chunk is a counted term")
      (is (= (:batch-bytes ec) (:batch-bytes ev))
          "compression does not change the heap a batch needs — it is the same
           records either way, and deriving from :bytes made it look 7x smaller")
      (is (= (:id-map-bytes ec) (:id-map-bytes ev)))
      (testing "a dump predating :raw-bytes falls back to the stored size"
        (let [old {:stats {:datom-count 1000 :max-eid 500}
                   :chunks [{:count 1000 :bytes 7000}]}]
          (is (pos? (:chunk-bytes (mman/estimate-from-manifest old 7000 100)))))))))

(deftest the-heap-ceiling-is-absent-where-there-is-none
  (testing "`Runtime.maxMemory` has no browser counterpart, and Node measures
            something else. Rather than invent a number, the estimate simply does
            not carry `:sufficient?` off the JVM — a diagnostic that guesses
            under the same key name as one that measures is worse than no
            diagnostic."
    (let [e (mman/estimate-from-manifest
             {:stats {:datom-count 10 :max-eid 5} :chunks [{:count 10 :bytes 100 :raw-bytes 100}]}
             100 10)]
      (is (pos? (:required-heap-bytes e)) "the requirement is computed everywhere")
      (is (string? (:required-heap e)))
      #?(:clj (is (contains? e :sufficient?) "the JVM can answer this")
         :cljs (is (not (contains? e :sufficient?)) "and ClojureScript does not pretend to")))))

;; ---------------------------------------------------------------------------

(deftest norm-val-distinguishes-array-kinds
  (testing "array values must compare structurally, not by identity, and keep
            their kind distinct — `[:farray …]` and `[:darray …]` must not
            collide. On the JVM these are `Class/forName \"[F\"`; here they are
            the typed arrays ClojureScript uses for the same value types."
    (is (= 42 (mman/norm-val 42)) "a scalar is itself")
    (is (= "x" (mman/norm-val "x")))
    (let [bs #?(:clj (byte-array [1 2 3]) :cljs (js/Uint8Array.from #js [1 2 3]))
          fs #?(:clj (float-array [1.0 2.0]) :cljs (js/Float32Array.from #js [1.0 2.0]))
          ds #?(:clj (double-array [1.0 2.0]) :cljs (js/Float64Array.from #js [1.0 2.0]))
          hi #?(:clj (byte-array [(unchecked-byte 0xFF) (unchecked-byte 0x80)])
                :cljs (js/Uint8Array.from #js [255 128]))]
      ;; GOLDEN, and asserted in a .cljc so BOTH runtimes must produce them.
      ;;
      ;; This used to read `[:bytes [1 2 3]]` — the element vector — which is
      ;; where the runtimes diverged: `vec` reads the platform's element type,
      ;; and the JVM's is SIGNED where ClojureScript's is not. The same three
      ;; bytes gave `[-1 -128 1]` on the JVM and `[255 128 1]` on Node, so
      ;; `verify-against` reported a mismatch on CORRECT data for any
      ;; `:db.type/bytes` value holding a byte >= 0x80, and the two runtimes
      ;; sorted such records differently. The fixture below only used values
      ;; under 0x80, where signed and unsigned agree — which is why it passed.
      ;;
      ;; The content now comes from hasch, which hashes the JVM array and the
      ;; typed array to the same value by construction (replikativ/hasch#31).
      ;; If a hasch upgrade changes its encoding these move: update them in the
      ;; same commit for both runtimes, never one.
      (is (= [:bytes  #uuid "1ef76d68-d43a-545c-9811-f0a49ffaaa01"] (mman/norm-val bs)))
      (is (= [:farray #uuid "27020a21-bbfe-52c8-87fc-47cf10542659"] (mman/norm-val fs)))
      (is (= [:darray #uuid "24755b8e-b622-5df5-bfec-f57cc71a9ddb"] (mman/norm-val ds)))
      (is (= [:bytes  #uuid "0280444f-2bc8-5f70-b77c-73f15752ba50"] (mman/norm-val hi))
          "the high-byte case the old element-vector form got wrong")
      (is (not= (mman/norm-val fs) (mman/norm-val ds))
          "same numbers, different kinds — they must not hash alike")
      (is (= (mman/norm-val bs)
             (mman/norm-val #?(:clj (byte-array [1 2 3]) :cljs (js/Uint8Array.from #js [1 2 3]))))
          "content-addressed: equal arrays normalise equally"))))

(deftest norm-val-reaches-inside-a-tuple
  (testing "a `:db.type/tuple` whose `:db/tupleTypes` names one of the three
            array types arrives as a VECTOR holding an array, so a check on the
            top-level class alone walks straight past it — which is what the
            first version of `norm-val` did.

            Both callers were then wrong for tuples in the way each was wrong
            for a bare array: `sort.cljc` stringified the identity hash, so
            chunk boundaries varied between JVM runs; and `verify-against`
            tier 3 puts the result in a SET, where two `byte[]` are never `=`,
            so an INTACT dump reported `:field-mismatch` on every sampled
            entity."
    (let [bs  #?(:clj (byte-array [1 2 3]) :cljs (js/Uint8Array.from #js [1 2 3]))
          bs' #?(:clj (byte-array [1 2 3]) :cljs (js/Uint8Array.from #js [1 2 3]))
          other #?(:clj (byte-array [9 9 9]) :cljs (js/Uint8Array.from #js [9 9 9]))]
      (is (= (mman/norm-val [bs "a"]) (mman/norm-val [bs' "a"]))
          "two distinct arrays with the same bytes: one tuple value")
      (is (= #{(mman/norm-val [bs "a"])} #{(mman/norm-val [bs' "a"])})
          "and equal as SET members, which is the form tier 3 compares in")
      (is (not= (mman/norm-val [bs "a"]) (mman/norm-val [other "a"]))
          "different content still differs, so the sort order stays total")

      (testing "a value holding no array is returned UNCHANGED — this is what
                keeps existing dumps from moving"
        (is (= [1 2 "x"] (mman/norm-val [1 2 "x"])))
        (is (= {:a 1} (mman/norm-val {:a 1})))
        (is (= [] (mman/norm-val []))))

      (testing "rewritten containers are marked, so the mapping stays injective:
                under `:schema-flexibility :read` a caller may store the
                normalised shape literally, and it must not share a sort key
                with the tuple that normalises to it"
        (is (not= (mman/norm-val [bs "a"])
                  (mman/norm-val [[:bytes (hasch/uuid bs)] "a"]))))

      (testing "nesting is not limited to one level, since free schema permits
                arbitrary values"
        (is (= (mman/norm-val {:k [#{bs}]}) (mman/norm-val {:k [#{bs'}]})))
        (is (not= (mman/norm-val {:k [#{bs}]}) (mman/norm-val {:k [#{other}]})))))))

(deftest codec-resolution-and-refusal
  (testing "a manifest with no `:compression` predates it and is stored verbatim;
            one naming a codec we lack is refused by name rather than failing
            inside a decoder."
    (is (= :none (mman/codec-of {})))
    (is (= :gzip (mman/codec-of {:compression :gzip})))
    (is (= :none (mman/codec-of {:compression :none})))
    (is (= :import/unsupported-compression
           (try (mman/codec-of {:compression :brotli}) nil
                (catch #?(:clj clojure.lang.ExceptionInfo :cljs js/Error) e
                  (:error (ex-data e))))))))

(deftest chunk-names-carry-their-codec
  (testing "one spelling for both media, and the suffix says what the file is —
            `datoms-000001.cbor.gz` is a gzip file to every tool on the machine."
    (is (= "datoms-000001.cbor" (mman/chunk-name 1 :none)))
    (is (= "datoms-000001.cbor.gz" (mman/chunk-name 1 :gzip)))
    (is (= "datoms-123456.cbor.gz" (mman/chunk-name 123456 :gzip)))
    (is (re-matches mman/chunk-re (mman/chunk-name 1 :gzip)))
    (is (re-matches mman/chunk-re (mman/chunk-name 1 :none)))
    (is (nil? (re-matches mman/chunk-re "../evil.cbor"))
        "and the pattern is what stops a manifest naming a path outside the dump")))

(deftest a-manifest-with-an-unknown-tag-still-reads
  (testing "a manifest written by a NEWER datahike must stay readable far enough
            to reach `check-capabilities!` and produce its precise refusal, so an
            unknown reader tag degrades to a tagged-literal instead of throwing."
    (let [m (mman/read-manifest-map "{:a 1 :b #some/future-tag [1 2]}")]
      (is (= 1 (:a m)))
      (is (tagged-literal? (:b m))))))

(deftest chunk-names-past-999999-are-still-legal
  (testing "`chunk-name` pads to six digits and then lets the number GROW, so
            chunk 1000000 is `datoms-1000000.cbor`. `chunk-re` demanded exactly
            six, and it is the read-side guard — so export produced a dump whose
            own file names its importer refused with \"Illegal chunk file name
            in manifest\". Silent on write, fatal on read.

            Out of reach at the default `:chunk-size` (10^12 datoms) but
            `:chunk-size` is a caller option and a small one gets there."
    (doseq [n [1 999999 1000000 1234567 99999999]
            codec [:none :gzip]]
      (let [nm (mman/chunk-name n codec)]
        (is (some? (re-matches mman/chunk-re nm))
            (str "chunk " n " (" codec ") -> " nm " must be accepted by chunk-re")))))
  (testing "and the guard still rejects what it is for — short names, wrong
            extensions, and anything that could escape the dump directory"
    (doseq [bad ["datoms-1.cbor" "datoms-00001.cbor" "datoms-000001.txt"
                 "datoms-abcdef.cbor" "../etc/passwd" "datoms-000001.cbor.zst"
                 "/abs/datoms-000001.cbor" "datoms-000001.cbor.gz.gz"]]
      (is (nil? (re-matches mman/chunk-re bad))
          (str bad " must be rejected")))))
