(ns ^:no-doc datahike.migrate.sort
  "External merge sort for the export path, so ordering a dump uses memory bounded
   by `:sort-buffer` rather than by the database size. `(api/datoms db :eavt)` is
   lazy over the index but ordered by `e`, not `t`; a dump must be ordered by
   transaction, so we spill sorted runs to temp files and k-way merge them.

   Records are `[e a v t op]` vectors; run files are CBOR sequences (RFC 8742),
   one top-level item per record.

   The merge carries DECODED records rather than encoded bytes. Under the EDN
   codec it carried line strings and re-parsed each one to get its sort key,
   which meant every record was decoded twice on every merge pass; a record is
   no larger in memory than the string it came from, so carrying it is both
   simpler and strictly less work.

   ## Portability

   This runs on Node as well as the JVM, which it did not when it was `sort.clj`.
   Nothing about the ALGORITHM was the obstacle, and the comments claiming a lazy
   seq made it JVM-only were wrong about the reason: the hazard those comments
   describe — a lazy seq performing IO cannot live inside a core.async go block —
   is about CHANNEL IO, and there is none here. Every read is a synchronous local
   file read (`.read` on the JVM, `fs.readSync` on Node), so the seq realises on
   the calling stack exactly as it always did, and the sort never touches
   konserve. The actual obstacles were `java.io.File` and `java.util.PriorityQueue`,
   and both have portable spellings."
  (:require [datahike.migrate.cbor :as mcbor]
            [datahike.migrate.fs :as fs]))

(defn sort-key
  "EXPORT ordering: transaction first (schema/refs before use, causal history),
   `:db/txInstant` first within a tx (tx entity established before its datoms),
   then `e`,`a` for a deterministic total order.

   This is the dump's order and a format guarantee, not an implementation
   detail — a consumer reading the log benefits from knowing it is causally
   ordered. It is NOT the order any index wants, which is why the sort takes a
   COMPARATOR as a parameter: a bulk index build re-sorts the same records into
   eavt/aevt/avet order, and that is a different order over the same data rather
   than a different dump.

   Note `(str …)` for the attribute AND the value: a stable total order without
   requiring them to be mutually Comparable. `v` is heterogeneous, so
   `(compare 3 \"x\")` would throw.

   `op` comes BEFORE `v`, retraction before assertion, and the order of those two
   keys is the whole guarantee. It used to be the other way round, and that made
   the guarantee a coincidence: a card-one overwrite's two records differ in `v`,
   so `v` decided and `op` was never consulted. Retraction-first then held only
   when the old value's string happened to sort before the new one's. Measured:
   `[1 :score 1 100 false]` / `[1 :score 10 100 true]` came out retract-first
   because `\"1\" < \"10\"`, while the same pair with the values swapped came out
   ASSERT-FIRST. The test that was supposed to pin this used 1 and 10 and so
   verified the collation accident rather than the rule.

   Retraction first is the meaningful order as well as a deterministic one: it is
   how datahike itself emits an overwrite, so a reader folding the log sees the
   old value removed and then the new one asserted.

   `v` stays in the key, last, purely to keep the order total: two records
   differing only in value must not tie, or the merge would order them by
   whichever run reached the cursor first.

   `norm-val` before `str`, and that is not cosmetic. `(str some-byte-array)` is
   `\"[B@1a2b3c4d\"` — the JVM IDENTITY hash — so two records differing only in a
   binary value sorted in an order that varied between JVM runs. Measured: six
   exports of one stored database from six fresh JVMs produced FIVE distinct
   chunk SHA-256s, which falsifies this namespace's own byte-identity claim for
   exactly the value types the branch added (`:db.type/bytes`, float-array,
   double-array — and tuples containing one). Two further consequences: a 32-bit
   identity hash can COLLIDE, so the totality promised just above also failed
   probabilistically; and ClojureScript stringifies a `Uint8Array` by CONTENT
   (`\"1,2,3\"`), so the JVM and Node ordered the same database differently.

   `norm-val` is content-based, class-distinct across the three array types, and
   already what the semantic digest uses (`fp-step`), so the two agree. It
   returns non-array values unchanged, so this key is byte-for-byte the old one
   for every other type and existing dumps do not move.

   It lives in `datahike.migrate.cbor` rather than `…/manifest` on purpose:
   manifest requires `datahike.api`, and pulling that into this leaf namespace
   to compute a sort key would be a large dependency for a small function."
  [record]
  [(nth record 3)
   (if (= (nth record 1) :db/txInstant) 0 1)
   (nth record 0)
   (str (nth record 1))
   (if (nth record 4) 1 0)
   (str (mcbor/norm-val (nth record 2)))])

(def by-sort-key
  "Comparator over RECORDS implementing the export order.

   A comparator rather than a key function, because index orders cannot be
   expressed as a Comparable key: an eavt key would be `[e a v t]`, and `v` is
   heterogeneous, so `(compare [1 :a \"x\" 5] [1 :a 7 5])` throws. datahike's own
   index comparators (`datom/index-type->cmp-quick`) exist for exactly that
   reason, and a bulk index build will pass one of those here — over Datoms —
   rather than a key function.

   The export path keeps a precomputed key because `sort-key` IS Comparable and
   precomputing is cheaper than recomputing per comparison."
  (fn [a b] (compare (sort-key a) (sort-key b))))

(defn- spill-window!
  "Sort one window and write it to a fresh run file under `tmp-dir`."
  [window tmp-dir cmp]
  (let [f (fs/temp-file! tmp-dir "dh-run-" ".cbor")
        sink (fs/open-sink f)]
    (try
      (doseq [r (sort cmp window)]
        (fs/write! sink (mcbor/encode-record r)))
      (finally (fs/close-sink! sink)))
    f))

(defn spill-runs
  "Consume a (lazy) seq of records in windows of `run-size`, sort each window with
   `cmp`, and write it to a temp run file under `tmp-dir`. Returns the vector of
   run paths. Memory is bounded by one window."
  ([records run-size tmp-dir] (spill-runs records run-size tmp-dir by-sort-key))
  ([records run-size tmp-dir cmp]
   (loop [rs (seq records) files []]
     (if (nil? rs)
       files
       (recur (seq (drop run-size rs))
              (conj files (spill-window! (into [] (take run-size) rs) tmp-dir cmp)))))))

(defn- record-seq-closing
  "Lazy seq of records from a CBOR-sequence run file, closing the source when
   exhausted. `decode-records` is bounded by the largest single item, so a run
   file larger than the heap still merges.

   `delete?` also removes the file once it is drained. A merge pass reads each
   run exactly once and has no use for it afterwards, so freeing it there caps
   scratch usage at roughly the data volume instead of twice it — PostgreSQL
   builds `logtape.c` entirely around reclaiming this space, and while we cannot
   recycle mid-file, dropping a whole exhausted run costs nothing.
   `read-sorted-file` must NOT delete: its file is read repeatedly, once per
   index family."
  ([p] (record-seq-closing p false))
  ([p delete?]
   (let [{:keys [source close]} (fs/reader p)]
     ((fn step [rs]
        (lazy-seq
         (if-let [s (seq rs)]
           (cons (first s) (step (rest s)))
           (do (close)
               (when delete? (fs/delete! p))
               nil))))
      (mcbor/decode-records source)))))

(defn merge-runs
  "K-way merge of sorted run files into a single lazy seq of RECORDS, globally
   ordered by `cmp`. Memory is bounded by the number of runs.

   A `sorted-set-by` over cursors rather than a priority queue: it is the
   portable spelling, it is O(log k) like the queue it replaces, and — because a
   sorted set must totally order its elements — it forces the tie-break the
   `PriorityQueue` version silently lacked. Ties now break by RUN INDEX, so two
   records comparing equal under `cmp` come out in a deterministic order instead
   of whichever cursor the heap happened to surface. That is the instability
   `sort-key` documents as a defect, fixed rather than worked around.

   The index is also what keeps the set from swallowing cursors: without it two
   cursors holding equal records would compare equal, and `conj` would discard
   one along with everything behind it. Exactly one entry per cursor is resident
   at a time, so the index is unique across the set."
  ([run-files] (merge-runs run-files by-sort-key false))
  ([run-files cmp] (merge-runs run-files cmp false))
  ([run-files cmp consume?]
   (let [cursor-cmp (fn [a b]
                      (let [c (cmp (:record a) (:record b))]
                        (if (zero? c) (compare (:idx a) (:idx b)) c)))
         cursors (keep-indexed (fn [i f]
                                 (when-let [rs (seq (record-seq-closing f consume?))]
                                   {:record (first rs) :rest (rest rs) :idx i}))
                               run-files)]
     ((fn step [pq]
        (lazy-seq
         (when-let [c (first pq)]
           (let [{:keys [record rest idx]} c
                 pq' (disj pq c)
                 pq' (if-let [nr (first rest)]
                       (conj pq' {:record nr :rest (next rest) :idx idx})
                       pq')]
             (cons record (step pq'))))))
      (into (sorted-set-by cursor-cmp) cursors)))))

(def ^:private max-fanin
  "Maximum run files merged at once, so a run never opens more file descriptors than
   a conservative OS limit — bounding fan-in rather than assuming a high `ulimit`."
  64)

(defn- merge-into-file
  "Merge a group of sorted run files into one new sorted run file, deleting the
   inputs as they drain. Opens at most (count run-files) descriptors.

   A group of ONE is returned untouched. Merging a single run means decoding and
   re-encoding an entire run file to produce a byte-identical one, which is a
   full pass over that data for no change in content."
  [run-files tmp-dir cmp]
  (if (= 1 (count run-files))
    (first run-files)
    (let [out (fs/temp-file! tmp-dir "dh-merge-" ".cbor")
          sink (fs/open-sink out)]
      (try
        (doseq [r (merge-runs run-files cmp true)]
          (fs/write! sink (mcbor/encode-record r)))
        (finally (fs/close-sink! sink)))
      ;; belt and braces: a run whose cursor never drained (an aborted merge)
      ;; still goes away here
      (doseq [f run-files] (fs/delete! f))
      out)))

(defn- reduce-runs
  "Merge `runs` down to at most `max-fanin` files, so the caller's final merge is
   a single streaming pass.

   Each round merges only as many runs as it must, rather than
   `(partition-all max-fanin)`. That partition has a sharp corner: 65 runs
   splits into [64 1], performing a full 64-way merge AND a pointless
   single-file copy, i.e. one gratuitous pass over the entire dataset. The
   textbook remedy is to make the FIRST pass partial — with 65 runs this merges
   2 and leaves the other 63 alone.

   `k` is clamped to `max-fanin` so no round opens more descriptors than the cap,
   and to at least 2 so a round always makes progress."
  [runs tmp-dir cmp]
  (loop [runs runs]
    (if (<= (count runs) max-fanin)
      runs
      (let [r (count runs)
            k (min max-fanin (max 2 (inc (- r max-fanin))))]
        (recur (cons (merge-into-file (take k runs) tmp-dir cmp)
                     (drop k runs)))))))

(defn external-sort
  "Given a lazy seq of records, return a lazy seq of RECORDS globally
   ordered by `cmp`, using external merge sort bounded by `run-size`. When the
   number of runs exceeds `max-fanin`, they are merged in passes so no single merge
   opens more than `max-fanin` files. Spill files are created under `tmp-dir`; the
   caller cleans up `tmp-dir` after the returned seq is fully consumed.

   An input that fits in ONE window never touches the filesystem: it is sorted in
   memory and handed back. PostgreSQL makes the same distinction — `tuplesort`
   only calls `inittapes()` once memory is actually exhausted — and without it a
   ten-record sort creates a temp file, writes it, and reads it back."
  ([records run-size tmp-dir] (external-sort records run-size tmp-dir by-sort-key))
  ([records run-size tmp-dir cmp]
   (let [rs (seq records)
         window (into [] (take run-size) rs)
         more (seq (drop run-size rs))]
     (if (nil? more)
       (sort cmp window)
       (-> (into [(spill-window! window tmp-dir cmp)]
                 (spill-runs more run-size tmp-dir cmp))
           (reduce-runs tmp-dir cmp)
           (merge-runs cmp true))))))

(defn external-sort-to-file
  "As `external-sort`, but collapse the result to ONE sorted CBOR-sequence file
   and return its path. The caller owns the file and should delete it.

   `external-sort` returns a lazy seq backed by open run files, which
   `record-seq-closing` closes on exhaustion — so it can be consumed exactly
   once. A bulk index build needs the same sorted order TWICE: once for the
   temporal tree, which takes every record, and once for the current tree, which
   takes the subset surviving `history/current-from-eavt-sorted`. Sorting twice
   would double the most expensive step for no reason.

   Reading the returned file with `read-sorted-file` is a fresh source each time,
   still bounded by one record.

   When the input spills to exactly ONE run, that run already IS a sorted
   CBOR-sequence file and is returned as-is. Decoding and re-encoding it would be
   a full pass over the whole dataset producing byte-identical output — and since
   `:sort-buffer` defaults to a million records, that was the common case rather
   than a corner: a bulk build of a database smaller than the buffer paid three
   entirely wasted serialization passes, one per index family."
  ([records run-size tmp-dir] (external-sort-to-file records run-size tmp-dir by-sort-key))
  ([records run-size tmp-dir cmp]
   (let [runs (spill-runs records run-size tmp-dir cmp)]
     (if (= 1 (count runs))
       (first runs)
       (let [out (fs/temp-file! tmp-dir "dh-sorted-" ".cbor")
             sink (fs/open-sink out)]
         (try
           (doseq [r (-> runs (reduce-runs tmp-dir cmp) (merge-runs cmp true))]
             (fs/write! sink (mcbor/encode-record r)))
           (finally (fs/close-sink! sink)))
         out)))))

(defn read-sorted-file
  "Lazy seq of records from a file written by `external-sort-to-file`, closing the
   source when exhausted. Bounded by one record; callable repeatedly."
  [p]
  (record-seq-closing p))
