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
   simpler and strictly less work."
  (:require [clojure.java.io :as io]
            [datahike.migrate.cbor :as mcbor])
  (:import [java.io File]
           [java.util PriorityQueue Comparator]))

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

   `op` is the LAST key, retraction before assertion, and it is not decoration.
   Without `v` and `op` a card-one overwrite's two records TIE — verified:
   `[1 :score 1 100 false]` and `[1 :score 10 100 true]` compared equal — and
   `merge-runs` breaks ties by PriorityQueue order, which is not stable. So the
   dump's within-transaction order was arbitrary run to run, contradicting the
   'deterministic total order' this docstring claims, and leaving a consumer
   unable to tell whether the retraction or the assertion came first.

   Retraction first is the meaningful order as well as a deterministic one: it is
   how datahike itself emits an overwrite, so a reader folding the log sees the
   old value removed and then the new one asserted."
  [record]
  [(nth record 3)
   (if (= (nth record 1) :db/txInstant) 0 1)
   (nth record 0)
   (str (nth record 1))
   (str (nth record 2))
   (if (nth record 4) 1 0)])

(def by-sort-key
  "Comparator over RECORDS implementing the export order.

   A comparator rather than a key function, because index orders cannot be
   expressed as a Comparable key: an eavt key would be `[e a v t]`, and `v` is
   heterogeneous, so `(compare [1 :a \"x\" 5] [1 :a 7 5])` throws
   ClassCastException. datahike's own index comparators (`datom/index-type->cmp-quick`)
   exist for exactly that reason, and a bulk index build will pass one of those
   here — over Datoms — rather than a key function.

   The export path keeps a precomputed key because `sort-key` IS Comparable and
   precomputing is cheaper than recomputing per comparison."
  (fn [a b] (compare (sort-key a) (sort-key b))))

(defn spill-runs
  "Consume a (lazy) seq of records in windows of `run-size`, sort each window with
   `cmp`, and write it to a temp run file under `tmp-dir`. Returns the vector of
   run `File`s. Memory is bounded by one window."
  ([records run-size ^File tmp-dir] (spill-runs records run-size tmp-dir by-sort-key))
  ([records run-size ^File tmp-dir cmp]
   (loop [rs (seq records) files []]
     (if (nil? rs)
       files
       (let [window (into [] (take run-size) rs)
             f (File/createTempFile "dh-run-" ".cbor" tmp-dir)]
         (with-open [out (io/output-stream f)]
           (doseq [r (sort cmp window)]
             (.write out ^bytes (mcbor/encode-record r))))
         (recur (seq (drop run-size rs)) (conj files f)))))))

(defn- record-seq-closing
  "Lazy seq of records from a CBOR-sequence run file, closing the stream when
   exhausted. `decode-seq-from` is bounded by the largest single item, so a run
   file larger than the heap still merges."
  [^File f]
  (let [in (io/input-stream f)]
    ((fn step [rs]
       (lazy-seq
        (if-let [s (seq rs)]
          (cons (first s) (step (rest s)))
          (do (.close in) nil))))
     (mcbor/decode-records in))))

(defn merge-runs
  "K-way merge of sorted run files into a single lazy seq of RECORDS, globally
   ordered by `key-fn`. Memory is bounded by the number of runs."
  ([run-files] (merge-runs run-files by-sort-key))
  ([run-files cmp]
   (let [^Comparator pq-cmp (reify Comparator
                              (compare [_ a b] (cmp (:record a) (:record b))))
         cursors (keep (fn [f]
                         (let [rs (seq (record-seq-closing f))]
                           (when rs
                             {:record (first rs)
                              :rest (rest rs)})))
                       run-files)
         pq (PriorityQueue. (max 1 (count run-files)) pq-cmp)]
     (doseq [c cursors] (.add pq c))
     ((fn step []
        (lazy-seq
         (when-not (.isEmpty pq)
           (let [{:keys [record rest]} (.poll pq)]
             (when-let [nr (first rest)]
               (.add pq {:record nr :rest (next rest)}))
             (cons record (step))))))))))

(def ^:private max-fanin
  "Maximum run files merged at once, so a run never opens more file descriptors than
   a conservative OS limit — bounding fan-in rather than assuming a high `ulimit`."
  64)

(defn- merge-into-file
  "Merge a group of sorted run files into one new sorted run file, deleting the
   inputs. Opens at most (count run-files) descriptors."
  [run-files ^File tmp-dir cmp]
  (let [out (File/createTempFile "dh-merge-" ".cbor" tmp-dir)]
    (with-open [os (io/output-stream out)]
      (doseq [r (merge-runs run-files cmp)]
        (.write os ^bytes (mcbor/encode-record r))))
    (doseq [^File f run-files] (.delete f))
    out))

(defn external-sort
  "Given a lazy seq of records, return a lazy seq of RECORDS globally
   ordered by `sort-key`, using external merge sort bounded by `run-size`. When the
   number of runs exceeds `max-fanin`, they are merged in passes so no single merge
   opens more than `max-fanin` files. Spill files are created under `tmp-dir`; the
   caller cleans up `tmp-dir` after the returned seq is fully consumed."
  ([records run-size ^File tmp-dir] (external-sort records run-size tmp-dir by-sort-key))
  ([records run-size ^File tmp-dir cmp]
   (loop [runs (spill-runs records run-size tmp-dir cmp)]
     (if (<= (count runs) max-fanin)
       (merge-runs runs cmp)
       (recur (mapv #(merge-into-file % tmp-dir cmp) (partition-all max-fanin runs)))))))
