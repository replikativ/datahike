(ns ^:no-doc datahike.migrate.sort
  "Migration ordering and CBOR-sequence IO for the shared external sorter.
   Windows are record-count bounded; individual migration records have no byte
   cap. File IO is synchronous on both JVM and Node."
  (:require [clojure.string :as str]
            [datahike.sort :as sorter]
            [datahike.migrate.cbor :as mcbor]
            [datahike.migrate.fs :as fs]))

(defn sort-key
  "Export order: transaction, txInstant first, entity, attribute, retract before
   assert, then value. Arrays are normalized by content and type on both runtimes."
  [record]
  [(nth record 3)
   (if (= (nth record 1) :db/txInstant) 0 1)
   (nth record 0)
   (str (nth record 1))
   (if (nth record 4) 1 0)
   (str (mcbor/norm-val (nth record 2)))])

(def by-sort-key
  "Comparator implementing export order. Prefer export-order to cache keys."
  (fn [a b] (compare (sort-key a) (sort-key b))))

(def export-order
  "Export order with keys cached once per admitted record per merge pass.
   Index sorts may instead pass a comparator for heterogeneous datom values."
  {::key-fn sort-key})

(defn- as-order [x]
  (if (map? x)
    (if (contains? x ::key-fn)
      {:key-fn (::key-fn x)}
      (throw (ex-info "A map sort order must carry ::key-fn; otherwise pass a comparator."
                      {:error :migrate/invalid-sort-order :order x})))
    {:cmp x}))

(defn- options [run-size]
  (when-not (and (integer? run-size) (pos? run-size))
    (throw (ex-info "Sort run size must be a positive integer."
                    {:error :migrate/invalid-sort-buffer :sort-buffer run-size})))
  {:window-records run-size :fan-in 64})

(defn- delete-file! [path]
  (when (and (fs/exists? path) (not (fs/delete! path)))
    (throw (ex-info "Cannot remove sort run." {:error :migrate/sort-cleanup :path path}))))

(defn- suppress! [error cleanup-error]
  #?(:clj (when-not (identical? error cleanup-error)
            (.addSuppressed ^Throwable error ^Throwable cleanup-error))
     :cljs nil))

(defn- close-after-error! [close! error]
  (try (close!)
       (catch #?(:clj Throwable :cljs :default) cleanup-error
         (suppress! error cleanup-error)))
  (throw error))

(defn- reader! [path]
  (let [{:keys [source close]} (fs/reader path)
        remaining (volatile! nil)
        closed? (volatile! false)
        close! (fn []
                 (when-not @closed?
                   (vreset! closed? true)
                   (vreset! remaining nil)
                   (close)))]
    (try
      (vreset! remaining (mcbor/decode-records source))
      {:next! (fn []
                (when-not @closed?
                  (if-let [s (seq @remaining)]
                    (let [record (first s)]
                      (vreset! remaining (rest s))
                      {:record record})
                    (do (close!) nil))))
       :close! close!}
      (catch #?(:clj Throwable :cljs :default) e
        (close-after-error! close! e)))))

(defn- backend [path-of]
  {:prepare (fn [record] {:record record})
   :open-reader #(reader! (path-of %))
   :open-writer (fn [id]
                  (let [sink (fs/open-sink (path-of id))]
                    {:write! #(fs/write! sink (mcbor/encode-record (:record %)))
                     :close! #(fs/close-sink! sink)}))
   :delete! #(delete-file! (path-of %))
   :move! (fn [from to]
            (when-not (fs/rename! (path-of from) (path-of to))
              (throw (ex-info "Cannot move sort run." {:error :migrate/sort-move}))))})

(defn- scratch [tmp-dir]
  (let [prefix (str (fs/file-name (fs/temp-file! tmp-dir "dh-sort-" "")) "-")
        path-of (fn [[pass run]] (fs/join tmp-dir (str prefix pass "-" run ".cbor")))]
    {:backend (backend path-of)
     :path-of path-of
     :cleanup! (fn []
                 (let [failure (volatile! nil)]
                   (fs/reduce-names tmp-dir
                                    (fn [_ name]
                                      (when (str/starts-with? name prefix)
                                        (try (delete-file! (fs/join tmp-dir name))
                                             (catch #?(:clj Throwable :cljs :default) e
                                               (if-let [original @failure]
                                                 (suppress! original e)
                                                 (vreset! failure e)))))) nil)
                   (when-let [error @failure] (throw error))))}))

(defn- scoped-resource [{:keys [records close!]} cleanup!]
  (let [closed? (volatile! false)
        close-all! (fn []
                     (when-not @closed?
                       (vreset! closed? true)
                       (try (close!)
                            (catch #?(:clj Throwable :cljs :default) e
                              (close-after-error! cleanup! e)))
                       (cleanup!)))]
    {:close! close-all!
     :records ((fn step [rs]
                 (lazy-seq
                  (when-not @closed?
                    (try
                      (if-let [s (seq rs)]
                        (cons (first s) (step (rest s)))
                        (do (close-all!) nil))
                      (catch #?(:clj Throwable :cljs :default) e
                        (close-after-error! close-all! e))))))
               records)}))

(defn spill-runs
  "Write sorted runs and return their paths. This compatibility helper retains
   one path per run; external-sort uses bounded run bookkeeping instead."
  ([records run-size tmp-dir] (spill-runs records run-size tmp-dir export-order))
  ([records run-size tmp-dir cmp]
   (let [order (as-order cmp) opts (options run-size)
         {:keys [backend path-of cleanup!]} (scratch tmp-dir)]
     (try
       (let [n (sorter/initial-runs! records order backend opts)]
         (mapv #(path-of [0 %]) (range n)))
       (catch #?(:clj Throwable :cljs :default) e
         (close-after-error! cleanup! e))))))

(defn merge-runs
  "Merge sorted files lazily, closing readers on exhaustion or error. With
   consume? true, delete exhausted inputs. Fully consume the result.
   Memory scales with run count."
  ([run-files] (merge-runs run-files export-order false))
  ([run-files cmp] (merge-runs run-files cmp false))
  ([run-files cmp consume?]
   (:records (sorter/merge-runs-resource! run-files (as-order cmp) (backend identity) consume?))))

(defn open-sorted-records!
  "Return {:records seq :close! fn}. Close after use, including early termination.
   The close function does not retain the result's head. An input fitting one
   window is sorted in memory without creating files."
  ([records run-size tmp-dir] (open-sorted-records! records run-size tmp-dir export-order))
  ([records run-size tmp-dir cmp]
   (let [order (as-order cmp) opts (options run-size)
         rs (seq records)
         window (into [] (take run-size) rs)
         more (seq (drop run-size rs))]
     (if (nil? more)
       {:records (sorter/sort-window window order) :close! (fn [])}
       (let [{:keys [backend cleanup!]} (scratch tmp-dir)]
         (try
           (let [n (sorter/initial-runs! (concat window more) order backend opts)
                 runs (sorter/reduce-runs! n order backend opts)]
             (scoped-resource (sorter/merge-runs-resource! runs order backend true) cleanup!))
           (catch #?(:clj Throwable :cljs :default) e
             (close-after-error! cleanup! e))))))))

(defn external-sort
  "Return sorted records, closing and deleting owned runs on exhaustion/error.
   Fully consume the result. For early termination use with-sorted-records! or
   open-sorted-records!. Files use synchronous local IO on both JVM and Node."
  ([records run-size tmp-dir] (external-sort records run-size tmp-dir export-order))
  ([records run-size tmp-dir cmp]
   (:records (open-sorted-records! records run-size tmp-dir cmp))))

(defn with-sorted-records!
  "Call consume with a sorted sequence valid only within this synchronous call.
   Do not return a lazy result or a channel whose work still needs the sequence."
  [records run-size tmp-dir cmp consume]
  (let [{:keys [records close!]} (open-sorted-records! records run-size tmp-dir cmp)]
    (try
      (let [result (consume records)] (close!) result)
      (catch #?(:clj Throwable :cljs :default) e
        (close-after-error! close! e)))))

(defn external-sort-to-file
  "Return one sorted CBOR-sequence file, reusable with read-sorted-file.
   The caller owns the file. A single run is returned without reencoding."
  ([records run-size tmp-dir] (external-sort-to-file records run-size tmp-dir export-order))
  ([records run-size tmp-dir cmp]
   (let [order (as-order cmp) opts (options run-size)
         {:keys [backend path-of cleanup!]} (scratch tmp-dir)]
     (try
       (if-let [id (sorter/sorted-file! records order backend opts)]
         (path-of id)
         (let [writer ((:open-writer backend) [0 0])]
           ((:close! writer))
           (path-of [0 0])))
       (catch #?(:clj Throwable :cljs :default) e
         (close-after-error! cleanup! e))))))

(defn read-sorted-file
  "Read a sorted file afresh. Fully consume the sequence to close its reader;
   the file remains available for another pass."
  [p]
  ;; A single run needs no comparisons between records. In particular, do not
  ;; compute export keys while rereading a file sorted in an index's order.
  (:records (sorter/merge-runs-resource! [p] {:cmp (fn [_ _] 0)} (backend identity) false)))
