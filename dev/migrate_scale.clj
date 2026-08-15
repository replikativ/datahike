(ns migrate-scale
  "Manual scale harness for datahike.migrate: build a file-backed database larger
   than the JVM heap, then export and re-import it under that same capped heap to
   demonstrate that export/import memory is bounded by the buffers, not the db size.

   Run with a small heap so the target is reachable quickly, e.g.:

     clojure -J-Xmx140m -M:dev -e \"(load-file \\\"dev/migrate_scale.clj\\\") (migrate-scale/run)\"

   `run` grows the db until the (estimated) EDN dump is at least `:heap-multiple`x
   the max heap (default 2x), exports (chunked), and imports into a fresh store —
   printing sizes and timings. NOT a unit test; it writes GBs to /tmp."
  (:require [datahike.api :as d]
            [datahike.migrate :as m]
            [clojure.java.io :as io]))

(defn dir-size ^long [path]
  (->> (file-seq (io/file path)) (filter #(.isFile ^java.io.File %))
       (map #(.length ^java.io.File %)) (reduce + 0)))

(defn- mb [b] (quot (long b) 1048576))

(defn run
  "Options:
     :heap-multiple  2      dump must be >= this many x the max heap
     :value-chars    1000   size of the padding string per entity (bigger = fewer
                            datoms to reach the target = faster build)
     :store-cache    32     :store-cache-size (small: the node cache is count-bounded)
     :sort-buffer    5000   export run size
     :chunk-size     25000  datoms per chunk file
     :batch-size     10000  import batch"
  ([] (run {}))
  ([{:keys [heap-multiple value-chars store-cache sort-buffer chunk-size batch-size]
     :or   {heap-multiple 2 value-chars 1000 store-cache 32
            sort-buffer 5000 chunk-size 25000 batch-size 10000}}]
   (let [maxheap (.maxMemory (Runtime/getRuntime))
         target  (* heap-multiple maxheap)
         stamp   (System/currentTimeMillis)
         src-path (str "/tmp/dh-scale-src-" stamp)
         cfg {:store {:backend :file :path src-path :id (java.util.UUID/randomUUID)}
              :keep-history? true :schema-flexibility :write :attribute-refs? false
              :store-cache-size store-cache :search-cache-size 0}
         pad (apply str (repeat value-chars \x))
         per-entity-bytes (+ value-chars 95)
         need (long (/ target per-entity-bytes))
         gen-batch 5000]
     (println (format "heap max = %d MB ; target DUMP >= %d MB (%dx heap)"
                      (mb maxheap) (mb target) heap-multiple))
     (d/create-database cfg)
     (let [conn (d/connect cfg)]
       (d/transact conn [{:db/ident :k :db/valueType :db.type/long :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                         {:db/ident :s :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])
       (println "building file-backed db ...")
       (loop [i 0]
         (when (< i need)
           (when (zero? (mod i 50000))
             (println (format "  %d / %d entities (store %d MB)" i need (mb (dir-size src-path)))))
           (d/transact conn (mapv (fn [j] {:k (+ i j) :s (str pad "-" (+ i j))}) (range gen-batch)))
           (recur (+ i gen-batch))))
       (println (format "BUILT: %d MB store (%.1fx heap)"
                        (mb (dir-size src-path)) (double (/ (dir-size src-path) maxheap))))
       (let [dump (str "/tmp/dh-scale-dump-" stamp)
             t0  (System/currentTimeMillis)
             man (m/export-db conn dump {:format :chunked :history? true
                                         :sort-buffer sort-buffer :chunk-size chunk-size})
             t1  (System/currentTimeMillis)]
         (println (format "EXPORT: %d datoms, %d chunks, DUMP = %d MB (%.2fx heap), %.1fs"
                          (:count (:semantic-digest man)) (count (:chunks man))
                          (mb (dir-size dump)) (double (/ (dir-size dump) maxheap))
                          (/ (- t1 t0) 1000.0)))
         (let [tcfg (-> cfg
                        (assoc-in [:store :path] (str dump "-tgt"))
                        (assoc-in [:store :id] (java.util.UUID/randomUUID)))]
           (d/create-database tcfg)
           (let [tconn (d/connect tcfg)
                 t2  (System/currentTimeMillis)
                 rep (m/import-db tconn dump {:batch-size batch-size})
                 t3  (System/currentTimeMillis)]
             (println (format "IMPORT: %.1fs %s" (/ (- t3 t2) 1000.0) (pr-str (dissoc rep :errors))))
             (d/release conn) (d/release tconn)
             (assoc rep :dump-mb (mb (dir-size dump)) :store-mb (mb (dir-size src-path))))))))))
