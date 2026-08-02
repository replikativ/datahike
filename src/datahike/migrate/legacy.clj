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

(def ^:dynamic *import-batch-size* 10000)

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
   clj-cbor does; no reader can restore information the writer discarded."
  [conn path]
  (println "Preparing legacy CBOR import of" path "in batches of" *import-batch-size*)
  (let [datoms (->> (with-open [in (io/input-stream path)]
                      (doall (mcbor/decode-records in)))
                    (map #(-> (apply d/datom %) (update :v instance-to-date))))]
    (reduce (fn [_last-tx batch]
              (let [batch (vec batch)]
                (swap! conn update-max-tx batch)
                (api/transact conn batch)))
            nil
            (partition-all *import-batch-size* datoms))))
