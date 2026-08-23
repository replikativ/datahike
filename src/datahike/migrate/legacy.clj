(ns ^:no-doc datahike.migrate.legacy
  "Reading the single-file dump format released datahike wrote, before the
   manifest-and-chunks design existed.

   JVM-only, and unlike `bulk.clj` or the old `blobs.cljc` that is not an
   oversight: a legacy dump can only exist on a machine that ran an old JVM
   datahike, so a Node-only runtime can never encounter one. Porting it would
   mean inventing ClojureScript behaviour with no fixture to test it against.

   Kept as its own namespace so what remains in `datahike.migrate` is only code
   that is being made portable — the boundary is visible in the file listing
   rather than implied by a reader conditional."
  (:require [datahike.api :as api]
            [datahike.datom :as d]
            [datahike.migrate.cbor :as mcbor]
            [clojure.java.io :as io]))

;; `*import-batch-size*` used to live here. It is defined in `datahike.migrate`
;; instead — that is the name the CHANGELOG published ([#845]) — and its value
;; arrives as the `batch-size` argument below. See the var's docstring.

(defn ^:deprecated update-max-tx
  "DEPRECATED. max-tx is maintained by load-entities; retained for old dumps."
  [db datoms]
  (assoc db :max-tx (reduce #(max %1 (nth %2 3)) (:max-tx db 0) datoms)))

(defn- instance-to-date
  "Coerce a `java.time.Instant` to `java.util.Date`.

   Belt and braces, and known to be so: boring decodes CBOR tag 1 to `Date`
   already — `legacy-instant-is-normalised-test` pins exactly that — so this is
   an identity on every value it currently sees. It is kept rather than deleted
   because it guards the LEGACY reader, whose whole job is to cope with bytes
   written by software we no longer control, and one `instance?` per value is
   not a cost worth trading for the certainty."
  [v]
  (if (instance? java.time.Instant v) (java.util.Date/from v) v))

(defn count-records
  "Number of records in a legacy single-file dump, by decoding it.

   Throws if `path` is not one — which is the point. A legacy dump has no
   manifest, no chunk list and no checksums, so `manifest-of` classifies ANY
   existing non-directory as one and synthesises a manifest with zero chunks.
   Nothing downstream then has anything to verify, and `verify` reported
   `{:ok? true}` for a plain text file. Reading the records is the only
   integrity check the format admits."
  [path]
  (try
    (with-open [in (io/input-stream path)]
      ;; Record SHAPE, not merely "these bytes decode". `decode-records` is
      ;; happy with any CBOR sequence, so a five-byte file holding the integers
      ;; 1 2 3 4 5 counted as five records — and once `verify` learned to accept
      ;; a legacy dump that decodes (it had briefly rejected every one of them),
      ;; that file reported `{:ok? true}`. A legacy dump is a sequence of
      ;; `[e a v t op]` vectors and nothing else, so checking the shape is what
      ;; the count was always standing in for.
      (reduce (fn [n r]
                (if (and (vector? r) (= 5 (count r)) (boolean? (nth r 4)))
                  (inc n)
                  (throw (ex-info "not a datom record" {:record r}))))
              0
              (mcbor/decode-records in)))
    (catch Exception e
      ;; The decoder's complaint is about bytes ("declared count 16 needs at
      ;; least 16 bytes but only 9 remain"), which answers a question nobody
      ;; asked. The caller asked whether this is a dump.
      (throw (ex-info (str "Not a datahike dump: " path
                           ". A dump is a DIRECTORY containing manifest.edn and "
                           "datoms-NNNNNN.cbor; a single FILE is only read as the "
                           "legacy format, and this one does not decode as one.")
                      {:error :import/not-a-dump :source (str path)}
                      e)))))

(defn import-db-legacy
  "Legacy import of an old flat CBOR dump via api/transact (unchanged behaviour).

   Read with boring rather than clj-cbor. A legacy dump is already a CBOR
   sequence of datom vectors, so `decode-records` reads it directly, and the two
   libraries agree on every construct these dumps contain — verified against
   bytes clj-cbor actually wrote, in `migrate-legacy-test`.

   The one difference is benign and already handled: clj-cbor decodes tag 1 to
   `java.time.Instant`, boring to `java.util.Date`, and `instance-to-date` below
   normalised that even before the swap. It stays as a guard rather than being
   deleted, since it costs nothing and an Instant reaching here from anywhere
   else would still be wrong.

   What CANNOT be recovered is what clj-cbor lost on WRITE: it encoded zero, NaN
   and +-Infinity doubles as float16 and bignums that fit as plain integers, so
   those values are already narrowed in the bytes. boring reads them exactly as
   clj-cbor does; no reader can restore information the writer discarded.

   Returns `{:record-count n :tx-count n}` — FACTS, not a report. `import-db`
   assembles the report map, because the report needs `decide-verification` and
   friends from `datahike.migrate`, which already requires THIS namespace: a
   require back would be a cycle. Same shape as `*import-batch-size*` arriving
   as an argument rather than being read here."
  [conn path batch-size]
  (println "Preparing legacy CBOR import of" path "in batches of" batch-size)
  (let [datoms (->> (with-open [in (io/input-stream path)]
                      (doall (mcbor/decode-records in)))
                    (map #(-> (apply d/datom %) (update :v instance-to-date))))
        batches (partition-all batch-size datoms)
        n-tx (reduce (fn [n batch]
                       (let [batch (vec batch)]
                         (swap! conn update-max-tx batch)
                         (api/transact conn batch)
                         (inc n)))
                     0
                     batches)]
    ;; `datoms` is already fully realised above, so counting it costs nothing
    ;; and holds nothing new.
    {:record-count (count datoms) :tx-count n-tx}))
