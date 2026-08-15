(ns datahike.test.migrate-sort-test
  "The external merge sort, on both runtimes.

   It had no direct tests at all — only end-to-end coverage through
   `migrate_test`, which exercises it with one run and therefore never runs the
   k-way merge it exists for. That was tolerable while the merge was a
   `java.util.PriorityQueue`; it is not now that the merge is a `sorted-set-by`,
   because the two fail differently.

   A priority queue holding two equal elements keeps both. A SORTED SET does
   not: `conj` on an element that compares equal to one already present is a
   no-op, so a cursor whose head ties another cursor's head would be silently
   discarded — taking every record behind it with it. The output would still be
   perfectly sorted, just short. That is why the merge's comparator breaks ties
   on run index, and why the tests below care so much about duplicates and about
   COUNT rather than order alone."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.migrate.fs :as fs]
            [datahike.migrate.sort :as msort]))

(defn- cleanup! [dir]
  (doseq [n (or (fs/list-names dir) [])]
    (fs/delete! (fs/join dir n)))
  (fs/delete! dir))

(defn- with-tmp [f]
  (let [dir (fs/temp-dir! "dh-sort-test")]
    (try (f dir) (finally (cleanup! dir)))))

(defn- records
  "`n` records spread over several transactions, deliberately NOT in sort order."
  [n]
  (vec (for [i (range n)]
         [(+ 100 (rem i 7))                      ; e — repeats
          (if (zero? (rem i 5)) :db/txInstant :name)
          (str "v" (rem i 11))                   ; v — repeats
          (+ 1000 (rem i 13))                    ; t — repeats, so runs interleave
          (even? i)])))

(deftest external-sort-matches-an-in-memory-sort
  (testing "the only definition of correct that cannot drift: for the same
            records, the external sort must produce exactly what `sort` does."
    (with-tmp
      (fn [dir]
        (let [rs (records 500)
              expected (sort msort/by-sort-key rs)]
          (is (= expected (vec (msort/external-sort rs 1000 dir)))
              "one run — no merge at all")
          (is (= expected (vec (msort/external-sort rs 50 dir)))
              "ten runs — a single k-way merge")
          (is (= expected (vec (msort/external-sort rs 1 dir)))
              "500 runs — more than max-fanin, so runs are merged in PASSES"))))))

(deftest no-record-is-lost-when-records-tie
  (testing "the failure mode a sorted set has and a priority queue does not.

            Every record here is one of three distinct values, so cursors tie
            constantly and a comparator without the run-index tie-break would
            drop whole cursors. Sorted output proves nothing about this — only
            the count does."
    (with-tmp
      (fn [dir]
        (let [rs (vec (repeatedly 300 #(rand-nth [[1 :name "x" 100 true]
                                                  [1 :name "x" 100 false]
                                                  [2 :name "x" 100 true]])))]
          (doseq [run-size [1 2 7 100]]
            (let [out (vec (msort/external-sort rs run-size dir))]
              (is (= (count rs) (count out))
                  (str "run-size " run-size ": every record survives the merge"))
              (is (= (sort msort/by-sort-key rs) out)
                  (str "run-size " run-size ": and in the right order"))
              (is (= (frequencies rs) (frequencies out))
                  (str "run-size " run-size ": with each value's multiplicity intact")))))))))

(deftest identical-records-survive-in-full
  (testing "the degenerate case of the above: 200 records that are all the same
            record. Under a tie-break that is not total, this collapses to
            however many runs there are."
    (with-tmp
      (fn [dir]
        (let [rs (vec (repeat 200 [1 :name "x" 100 true]))]
          (is (= 200 (count (msort/external-sort rs 3 dir)))))))))

(deftest an-input-that-fits-never-touches-the-filesystem
  (testing "PostgreSQL only calls `inittapes()` once memory is actually
            exhausted. Without the same distinction a ten-record sort writes a
            temp file and reads it straight back, and — because `:sort-buffer`
            defaults to a million — that was the NORMAL case for any database
            smaller than the buffer, not a corner."
    (with-tmp
      (fn [dir]
        (let [rs (records 50)]
          (is (= (sort msort/by-sort-key rs) (vec (msort/external-sort rs 1000 dir)))
              "still correctly sorted")
          (is (empty? (or (fs/list-names dir) []))
              "and no run file was created"))))))

(deftest a-single-run-is-not-copied
  (testing "`external-sort-to-file` promises one sorted CBOR-sequence file. When
            the input spills to exactly one run, that run already IS one —
            decoding and re-encoding it produces byte-identical output at the
            cost of a full pass over the data, three times over in a bulk build."
    (with-tmp
      (fn [dir]
        (let [rs (records 300)
              expected (sort msort/by-sort-key rs)
              f (msort/external-sort-to-file rs 100000 dir)]
          (is (= 1 (count (fs/list-names dir)))
              "exactly one file exists — the run, not a copy of it")
          (is (= expected (vec (msort/read-sorted-file f))) "and it reads back")
          (is (= expected (vec (msort/read-sorted-file f))) "repeatedly"))))))

(deftest run-files-are-reclaimed-as-the-merge-drains-them
  (testing "a merge reads each run exactly once and has no use for it after.
            Holding them all to the end doubles peak scratch space, which for a
            large dump is an operational limit rather than untidiness."
    (with-tmp
      (fn [dir]
        (let [rs (records 400)
              out (vec (msort/external-sort rs 20 dir))]
          (is (= (sort msort/by-sort-key rs) out) "still correct")
          (is (empty? (or (fs/list-names dir) []))
              "every run file went away as its cursor drained"))))))

(deftest many-more-runs-than-the-fan-in-cap
  (testing "the multi-pass path. `(partition-all 64)` on 65 runs gave [64 1] — a
            full 64-way merge plus a single-file copy, one gratuitous pass over
            everything. Sizes are chosen around the cap of 64."
    (with-tmp
      (fn [dir]
        (doseq [n [65 70 130 300]]
          (let [rs (records n)]
            (is (= (sort msort/by-sort-key rs) (vec (msort/external-sort rs 1 dir)))
                (str n " runs of one record each"))))))))

(deftest a-custom-comparator-is-honoured
  (testing "the sort takes a comparator because a bulk index build passes an
            index comparator over the same records rather than the export order."
    (with-tmp
      (fn [dir]
        (let [rs (records 120)
              by-e (fn [a b] (compare [(nth a 0) (str (nth a 2))]
                                      [(nth b 0) (str (nth b 2))]))]
          (is (= (sort by-e rs) (vec (msort/external-sort rs 10 dir by-e)))))))))

(deftest the-sorted-file-can-be-read-more-than-once
  (testing "`external-sort` returns a seq backed by run files that close on
            exhaustion, so it is consumable ONCE. A bulk index build needs the
            same order twice — temporal and current — which is the whole reason
            `external-sort-to-file` exists."
    (with-tmp
      (fn [dir]
        (let [rs (records 300)
              expected (sort msort/by-sort-key rs)
              f (msort/external-sort-to-file rs 20 dir)]
          (is (= expected (vec (msort/read-sorted-file f))) "first read")
          (is (= expected (vec (msort/read-sorted-file f))) "second read")
          (is (= expected (vec (msort/read-sorted-file f))) "and again"))))))

(deftest an-empty-input-sorts-to-nothing
  (testing "not an error and not nil — a database with no datoms is a database."
    (with-tmp
      (fn [dir]
        (is (= [] (vec (msort/external-sort [] 10 dir))))
        (is (= [] (vec (msort/read-sorted-file
                        (msort/external-sort-to-file [] 10 dir)))))))))

(deftest the-export-order-puts-txinstant-first-and-retraction-before-assertion
  (testing "`sort-key`'s two deliberate choices, pinned because both are format
            guarantees a consumer reads the log by.

            The retraction cases are chosen so that the VALUES' string order
            OPPOSES the required order in one of them. An earlier version of this
            test used 1 and 10 in the one arrangement where `\"1\" < \"10\"` made
            it pass while `sort-key` compared `v` before `op` — it verified a
            collation accident and called it a format guarantee. Both
            arrangements are asserted now, so no ordering of the values can make
            this pass by luck."
    (let [tx-instant [5 :db/txInstant "t" 100 true]
          other      [1 :name "a" 100 true]]
      (is (= [tx-instant other] (sort msort/by-sort-key [other tx-instant]))
          ":db/txInstant comes first within its transaction")

      (testing "old value sorts BEFORE the new one as a string"
        (let [retract [1 :score 1 100 false]
              assert* [1 :score 10 100 true]]
          (is (= [retract assert*] (sort msort/by-sort-key [assert* retract]))
              "retracts before it asserts")))

      (testing "old value sorts AFTER the new one as a string — the case that
                fails whenever `v` is compared before `op`"
        (let [retract [1 :score 10 100 false]
              assert* [1 :score 1 100 true]]
          (is (= [retract assert*] (sort msort/by-sort-key [assert* retract]))
              "retracts before it asserts, regardless of the values"))))))

(deftest records-differing-only-in-value-do-not-tie
  (testing "`v` stays in the key, last, so the order is total. Two records alike
            but for their value must not compare equal — a tie would leave the
            merge to order them by whichever run surfaced first, which is exactly
            the instability the key exists to remove."
    (let [a [1 :score 1 100 true]
          b [1 :score 2 100 true]]
      (is (not= 0 (msort/by-sort-key a b)))
      (is (= (sort msort/by-sort-key [a b]) (sort msort/by-sort-key [b a]))
          "and the order does not depend on the input order"))))

(deftest binary-values-sort-by-content-not-identity
  (testing "`sort-key` ended `(str v)`, and `(str some-byte-array)` is
            `\"[B@1a2b3c4d\"` — the JVM IDENTITY hash. So two records differing
            only in a binary value sorted in an order that varied between JVM
            RUNS. Measured before the fix: six exports of one stored database
            from six fresh JVMs produced five distinct chunk SHA-256s, which
            falsifies the byte-identical-export claim for exactly the value
            types this branch added.

            Two further consequences the identity hash had: it is 32 bits, so it
            can collide and break the totality the docstring promises; and
            ClojureScript stringifies a Uint8Array by CONTENT, so JVM and Node
            ordered the same database differently."
    (let [k #(msort/sort-key [1 :a % 5 true])]
      (testing "equal content sorts equal, whatever the identity"
        (is (= (k #?(:clj (byte-array [1 2 3]) :cljs (js/Uint8Array. #js [1 2 3])))
               (k #?(:clj (byte-array [1 2 3]) :cljs (js/Uint8Array. #js [1 2 3]))))
            "two distinct objects with the same bytes"))
      (testing "different content still differs, so the order stays total"
        (is (not= (k #?(:clj (byte-array [1 2 3]) :cljs (js/Uint8Array. #js [1 2 3])))
                  (k #?(:clj (byte-array [9]) :cljs (js/Uint8Array. #js [9]))))))
      (testing "the three array types stay distinct from each other and from a
                plain vector of the same numbers — `norm-val` tags the class"
        (let [ks [(k #?(:clj (byte-array [1 2]) :cljs (js/Uint8Array. #js [1 2])))
                  (k #?(:clj (float-array [1 2]) :cljs (js/Float32Array. #js [1 2])))
                  (k #?(:clj (double-array [1 2]) :cljs (js/Float64Array. #js [1 2])))
                  (k [1 2])]]
          (is (= 4 (count (set ks))))))
      (testing "and a non-binary value's key is untouched, so existing dumps do
                not move — this is the whole reason `norm-val` passes them through"
        (is (= "hello" (last (msort/sort-key [1 :a "hello" 5 true]))))
        (is (= "42" (last (msort/sort-key [1 :a 42 5 true]))))))))

;; ---------------------------------------------------------------------------

(deftest decorated-order-equals-the-bare-comparator
  (testing "`export-order` precomputes each record's sort key instead of
            rebuilding both keys on every comparison. The output must be
            IDENTICAL to the bare comparator's, and the way this could go wrong
            is the reason the two halves are bundled in one value rather than
            passed as two arguments: if the comparator and the key function
            disagreed, every run would be sorted by one order and merged by the
            other. Each run would be internally sorted, the merge internally
            consistent, and the global result simply wrong — which no assertion
            about a single run would catch.

            So this compares KEY SEQUENCES through the spilling path, where the
            merge is what orders the output. Comparing records directly does not
            work: records come back DECODED from run files, and `=` on a
            `byte[]` is identity, so a `:db.type/bytes` value would report a
            divergence that is the test's own artifact."
    (let [mk (fn [i]
               (case (mod i 4)
                 0 [(mod i 13) :a #?(:clj (byte-array [(unchecked-byte i) 2])
                                     :cljs (js/Uint8Array. #js [1 2]))
                    (+ 536870913 (mod i 3)) true]
                 1 [(mod i 13) :b (str "v" (mod i 7)) (+ 536870913 (mod i 3)) false]
                 2 [(mod i 13) :c (mod i 5) (+ 536870913 (mod i 3)) true]
                 3 [7 :d "tie" 536870913 true]))
          records (mapv mk (range 400))
          keys-of #(mapv msort/sort-key %)]
      (doseq [run-size [10000 100 7 1 399 401]]
        (let [d1 (fs/temp-dir! "dh-ord-a-")
              d2 (fs/temp-dir! "dh-ord-b-")]
          (try
            (let [decorated (vec (msort/external-sort records run-size d1))
                  bare      (vec (msort/external-sort records run-size d2
                                                      msort/by-sort-key))
                  kd (keys-of decorated)]
              (is (= kd (keys-of bare))
                  (str "same order at run-size " run-size))
              (is (= kd (sort kd))
                  (str "and it is actually sorted, at run-size " run-size))
              (is (= (frequencies kd) (frequencies (keys-of records)))
                  (str "no record gained or lost, at run-size " run-size)))
            (finally (cleanup! d1) (cleanup! d2)))))

      (testing "a run count above `max-fanin` (64), so `reduce-runs` performs
                real merge passes rather than one final merge"
        (let [d1 (fs/temp-dir! "dh-ord-c-")
              d2 (fs/temp-dir! "dh-ord-d-")]
          (try
            (is (= (keys-of (msort/external-sort records 2 d1))
                   (keys-of (msort/external-sort records 2 d2 msort/by-sort-key)))
                "200 runs, merged in passes")
            (finally (cleanup! d1) (cleanup! d2)))))

      (testing "`external-sort-to-file` is a separate entry point with its own
                default, and takes the spill path even for a small input"
        (let [d1 (fs/temp-dir! "dh-ord-e-")
              d2 (fs/temp-dir! "dh-ord-f-")]
          (try
            (is (= (keys-of (msort/read-sorted-file
                             (msort/external-sort-to-file records 50 d1)))
                   (keys-of (msort/read-sorted-file
                             (msort/external-sort-to-file records 50 d2
                                                          msort/by-sort-key)))))
            (finally (cleanup! d1) (cleanup! d2))))))))

(deftest a-bare-comparator-still-takes-the-undecorated-path
  (testing "not every order has a Comparable key. `init/sort-family!` sorts by
            `datom/index-type->cmp-quick`, an INDEX order over `[e a v t]` where
            `v` is heterogeneous — `(compare [1 :a \"x\" 5] [1 :a 7 5])` throws,
            which is why `sort.cljc` takes a comparator at all. Passing a
            function must therefore keep working unchanged, and must NOT be
            decorated with a key it has no key function for."
    (let [records [[3 :a 1 5 true] [1 :a 2 5 true] [2 :a 3 5 true]]
          ;; by entity id descending — nothing to do with the export order
          by-e-desc (fn [a b] (compare (nth b 0) (nth a 0)))
          d (fs/temp-dir! "dh-bare-")]
      (try
        (is (= [3 2 1] (mapv #(nth % 0) (msort/external-sort records 2 d by-e-desc)))
            "the caller's comparator decides, through the spilling path")
        (finally (cleanup! d))))))

(deftest a-map-order-without-a-key-function-is-refused
  (testing "`as-order` accepts either a keyed order or a bare comparator, and a
            map that is neither falls through to `(sort nil records)` — natural
            ordering on RECORDS, which throws on a heterogeneous `v` and, worse,
            SUCCEEDS on a homogeneous one and writes a dump in the wrong order.

            A `throw` rather than an `assert`: `shadow-cljs.edn` sets
            `:elide-asserts false` on the dev and test builds but not on
            `:browser-release` or `:npm-release`, so an assertion would exist
            everywhere except the artifacts we publish. Neither the old
            assertion nor the new refusal was pinned by anything until now."
    (let [records [[1 :a 1 5 true] [2 :a 2 5 true] [3 :a 3 5 true]]
          refused? (fn [order run-size]
                     (let [d (fs/temp-dir! "dh-ord-")]
                       (try
                         (= :migrate/invalid-sort-order
                            (try (doall (msort/external-sort records run-size d order))
                                 ::no-throw
                                 (catch #?(:clj clojure.lang.ExceptionInfo :cljs :default) e
                                   (:error (ex-data e)))))
                         (finally (cleanup! d)))))]
      (is (refused? {} 100) "in memory, one window")
      (is (refused? {} 2) "and through the spill and merge path")
      (is (refused? {:cmp compare} 100)
          "an unnamespaced :cmp is not this namespace's key — a near miss is
           still a miss, and silently sorting by natural order would be worse
           than refusing")

      (testing "the two legitimate shapes are untouched"
        (let [d (fs/temp-dir! "dh-ord-ok-")]
          (try
            (is (= 3 (count (msort/external-sort records 2 d))))
            (is (= 3 (count (msort/external-sort records 2 d msort/by-sort-key))))
            (finally (cleanup! d))))))))
