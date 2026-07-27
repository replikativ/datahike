(ns ^:no-doc datahike.migrate.store
  "Konserve-store medium for dumps, so export/import can target an external store
   (S3 / S3-compatible via konserve-s3, JDBC, Redis, in-memory, ...) with no local
   disk for the dump itself — the same storage abstraction datahike uses for its
   own data. A diskless container writes straight to its configured store.

   A dump target/source may be:
     - an already-open konserve store:   {:store <store> :prefix \"my-backup\"}
     - a konserve store-config map:       {:backend :s3 :bucket .. :id .. :prefix ..}
   Chunks and the manifest are stored under keys `[\"datahike.migrate\" prefix X]`;
   the manifest key is written LAST as the commit marker. Only the store transport
   differs — the format, per-chunk SHA-256, and semantic digest are identical to
   the filesystem dump.

   The external-sort scratch still uses local temp files (ephemeral, fine in a
   normal container); only the dump lives in the store."
  (:require [konserve.core :as k]
            [konserve.store :as ks]
            [datahike.migrate.digest :as dig]
            [clojure.string :as str])
  (:import [java.nio.charset StandardCharsets]))

(def ^:private ns-tag "datahike.migrate")

(defn store-target?
  "True if `x` designates a konserve store medium (open store or backend config),
   as opposed to a filesystem path/File."
  [x]
  (and (map? x) (or (:store x) (:backend x))))

(defn open
  "Open a medium from a target spec. Returns {:store :prefix :owned?}; call `close`."
  [target]
  (let [prefix (or (:prefix target) "dump")]
    (if-let [s (:store target)]
      {:store s :prefix prefix :owned? false}
      {:store (ks/connect-store (dissoc target :prefix) {:sync? true})
       :prefix prefix :owned? true :config (dissoc target :prefix)})))

(defn close [{:keys [store owned? config]}]
  (when owned?
    (try (ks/release-store config store {:sync? true}) (catch Throwable _ nil))))

(defn- ckey [prefix x] [ns-tag prefix x])

(defn- chunk-key [prefix n] (ckey prefix (format "datoms-%06d" n)))

(defn write-chunks!
  "Stream `sorted-lines` into the store as chunk values of at most `chunk-size`
   lines each, computing per-chunk SHA-256 and the semantic digest incrementally.
   `manifest-fn` is (fn [finalized-digest chunks] -> manifest-map). Writes the
   manifest key LAST. Returns the manifest."
  [{:keys [store prefix]} sorted-lines chunk-size manifest-fn progress]
  (loop [ls (seq sorted-lines) n 1 chunks [] dacc (dig/accumulator)]
    (if (nil? ls)
      (let [manifest (manifest-fn (dig/finalize dacc) chunks)]
        (k/assoc store (ckey prefix "manifest") manifest {:sync? true})
        (progress {:phase :done :datoms (:count (dig/finalize dacc))})
        manifest)
      (let [part    (into [] (take chunk-size) ls)
            content (str (str/join "\n" part) "\n")
            bytes   (alength (.getBytes content StandardCharsets/UTF_8))]
        (k/assoc store (chunk-key prefix n) content {:sync? true})
        (progress {:phase :chunk :datoms (count part)})
        (recur (seq (drop chunk-size ls)) (inc n)
               (conj chunks {:file (format "datoms-%06d" n) :count (count part)
                             :bytes bytes :sha256 (dig/sha256-hex content)})
               (reduce dig/add-line dacc part))))))

(defn read-manifest [{:keys [store prefix]}]
  (k/get store (ckey prefix "manifest") nil {:sync? true}))

(defn reduce-lines
  "Reduce `rf` over every record line of the dump, verifying each chunk's SHA-256
   (streaming a whole chunk value at a time — bounded by chunk-size) before use."
  [{:keys [store prefix]} manifest rf init]
  (reduce (fn [acc {:keys [file sha256]}]
            (let [content (k/get store (ckey prefix file) nil {:sync? true})]
              (when (nil? content)
                (throw (ex-info (str "Missing chunk in store: " file)
                                {:error :import/checksum-failed :file file})))
              (when (and sha256 (not= sha256 (dig/sha256-hex content)))
                (throw (ex-info (str "Checksum mismatch for chunk " file)
                                {:error :import/checksum-failed :file file})))
              (reduce rf acc (str/split-lines content))))
          init
          (:chunks manifest)))
