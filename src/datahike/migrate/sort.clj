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
  "Ordering key: transaction first (schema/refs before use, causal history),
   `:db/txInstant` first within a tx (tx entity established before its datoms),
   then `e`,`a` for a deterministic total order."
  [record]
  [(nth record 3) (if (= (nth record 1) :db/txInstant) 0 1) (nth record 0) (str (nth record 1))])

(defn spill-runs
  "Consume a (lazy) seq of records in windows of `run-size`, sort each window by
   `sort-key`, and write it to a temp run file under `tmp-dir`. Returns the vector
   of run `File`s. Memory is bounded by one window."
  [records run-size ^File tmp-dir]
  (loop [rs (seq records) files []]
    (if (nil? rs)
      files
      (let [window (into [] (take run-size) rs)
            f (File/createTempFile "dh-run-" ".cbor" tmp-dir)]
        (with-open [out (io/output-stream f)]
          (doseq [r (sort-by sort-key window)]
            (.write out ^bytes (mcbor/encode-record r))))
        (recur (seq (drop run-size rs)) (conj files f))))))

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
   ordered by `sort-key`. Memory is bounded by the number of runs."
  [run-files]
  (let [cmp (reify Comparator
              (compare [_ a b] (compare (:key a) (:key b))))
        cursors (keep (fn [f]
                        (let [rs (seq (record-seq-closing f))]
                          (when rs
                            {:key (sort-key (first rs))
                             :record (first rs)
                             :rest (rest rs)})))
                      run-files)
        pq (PriorityQueue. (max 1 (count run-files)) cmp)]
    (doseq [c cursors] (.add pq c))
    ((fn step []
       (lazy-seq
        (when-not (.isEmpty pq)
          (let [{:keys [record rest]} (.poll pq)]
            (when-let [nr (first rest)]
              (.add pq {:key (sort-key nr)
                        :record nr
                        :rest (next rest)}))
            (cons record (step)))))))))

(def ^:private max-fanin
  "Maximum run files merged at once, so a run never opens more file descriptors than
   a conservative OS limit — bounding fan-in rather than assuming a high `ulimit`."
  64)

(defn- merge-into-file
  "Merge a group of sorted run files into one new sorted run file, deleting the
   inputs. Opens at most (count run-files) descriptors."
  [run-files ^File tmp-dir]
  (let [out (File/createTempFile "dh-merge-" ".cbor" tmp-dir)]
    (with-open [os (io/output-stream out)]
      (doseq [r (merge-runs run-files)]
        (.write os ^bytes (mcbor/encode-record r))))
    (doseq [^File f run-files] (.delete f))
    out))

(defn external-sort
  "Given a lazy seq of records, return a lazy seq of RECORDS globally
   ordered by `sort-key`, using external merge sort bounded by `run-size`. When the
   number of runs exceeds `max-fanin`, they are merged in passes so no single merge
   opens more than `max-fanin` files. Spill files are created under `tmp-dir`; the
   caller cleans up `tmp-dir` after the returned seq is fully consumed."
  [records run-size ^File tmp-dir]
  (loop [runs (spill-runs records run-size tmp-dir)]
    (if (<= (count runs) max-fanin)
      (merge-runs runs)
      (recur (mapv #(merge-into-file % tmp-dir) (partition-all max-fanin runs))))))
