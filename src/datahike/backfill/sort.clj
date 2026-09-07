(ns ^:no-doc datahike.backfill.sort
  "Scoped JVM external sorting with bounded windows, records and merge fan-in.
   Limits account for retained record data, not exact JVM object sizes."
  (:require [boring.core :as boring]
            [datahike.migrate.cbor :as cbor])
  (:import [java.io ByteArrayOutputStream DataInputStream DataOutputStream
            BufferedInputStream BufferedOutputStream FileInputStream FileOutputStream OutputStream EOFException]
           [java.nio.file Files Path CopyOption]
           [java.nio.file.attribute FileAttribute]))

(defn- fail! [kind message data]
  (throw (ex-info message (assoc data :type kind))))

(defn validate-options! [options]
  (when-not (or (nil? options) (map? options))
    (fail! ::invalid-options "Sort options must be a map." {}))
  (let [defaults {:window-bytes 16777216 :window-records 65536
                  :max-record-bytes 1048576 :fan-in 16 :max-bytes 4294967296}
        allowed (conj (set (keys defaults)) :directory)
        opts (merge defaults options)]
    (when (some #(not (contains? allowed %)) (keys options))
      (fail! ::invalid-options "Unknown sort option." {}))
    (doseq [k (keys defaults)]
      (when-not (and (integer? (get opts k)) (pos? (get opts k))
                     (<= (get opts k) Long/MAX_VALUE))
        (fail! ::invalid-options "Sort limits must be positive bounded integers." {:option k})))
    (when (or (< (:fan-in opts) 2) (> (:fan-in opts) 128)
              (> (:max-record-bytes opts) Integer/MAX_VALUE))
      (fail! ::invalid-options "Invalid fan-in or record limit." {}))
    (when-not (or (nil? (:directory opts)) (instance? Path (:directory opts))
                  (and (string? (:directory opts)) (seq (:directory opts))))
      (fail! ::invalid-options "Directory must be a nonempty path." {}))
    opts))

(defn- record-charge!
  "Bound codec scalar allocations and traversal before encoding. Charges count
   collection elements as well as payload, so tiny scalar encodings cannot make
   an arbitrarily large retained object graph fit a window."
  [record limit]
  (let [charge (volatile! 0)]
    (letfn [(add! [n]
              (vswap! charge + n)
              (when (> @charge limit)
                (fail! ::record-too-large "Record exceeds structural budget." {})))
            (visit! [x depth]
              (when (> depth 128)
                (fail! ::record-too-large "Record nesting exceeds limit." {}))
              (add! 32)
              (cond
                (string? x) (add! (* 3 (long (.length ^String x))))
                (or (keyword? x) (symbol? x))
                (do (visit! (name x) (inc depth))
                    (when-let [n (namespace x)] (visit! n (inc depth))))
                (instance? java.math.BigInteger x) (add! (inc (quot (.bitLength ^java.math.BigInteger x) 8)))
                (instance? clojure.lang.BigInt x) (visit! (.toBigInteger ^clojure.lang.BigInt x) (inc depth))
                (instance? java.math.BigDecimal x) (visit! (.unscaledValue ^java.math.BigDecimal x) (inc depth))
                (ratio? x) (do (visit! (numerator x) (inc depth))
                               (visit! (denominator x) (inc depth)))
                (map? x) (doseq [[k v] x] (visit! k (inc depth)) (visit! v (inc depth)))
                (or (sequential? x) (set? x)) (doseq [v x] (visit! v (inc depth)))
                (and x (.isArray (class x)))
                (let [component (.getComponentType (class x))
                      n (java.lang.reflect.Array/getLength x)]
                  (if (.isPrimitive component)
                    (add! (* (long n) (cond
                                        (or (= component Byte/TYPE) (= component Boolean/TYPE)) 1
                                        (or (= component Short/TYPE) (= component Character/TYPE)) 2
                                        (or (= component Float/TYPE) (= component Integer/TYPE)) 4
                                        :else 8)))
                    (doseq [v (seq x)] (visit! v (inc depth)))))
                (or (nil? x) (boolean? x) (number? x) (char? x)
                    (instance? java.util.Date x) (instance? java.util.UUID x)) nil
                :else (fail! ::unsupported-value "Unsupported sort value." {:class (str (class x))})))]
      (visit! record 0)
      @charge)))

(defn- encode! [record {:keys [max-record-bytes window-bytes]}]
  (let [charge (record-charge! record (min window-bytes max-record-bytes))
        buffer (ByteArrayOutputStream. (int (min 8192 max-record-bytes)))
        count-bytes (volatile! 0)
        reserve! (fn [n]
                   (when (> (+ @count-bytes n) max-record-bytes)
                     (fail! ::record-too-large "Encoded record exceeds byte limit." {}))
                   (vswap! count-bytes + n))
        out (proxy [OutputStream] []
              (write
                ([v] (if (number? v)
                       (do (reserve! 1) (.write buffer (int v)))
                       (let [n (alength ^bytes v)]
                         (reserve! n) (.write buffer ^bytes v 0 n))))
                ([bs offset n] (reserve! n) (.write buffer ^bytes bs (int offset) (int n)))))]
    (boring/write-to! (boring/writer (min 8192 max-record-bytes) cbor/opts) record out)
    (let [bytes (.toByteArray buffer)
          total (+ charge (alength bytes) 64)]
      (when (> total window-bytes)
        (fail! ::record-too-large "Record cannot fit the sort window." {}))
      {:record record :bytes bytes :charge total})))

(defn- run-path ^Path [^Path directory pass run]
  (.resolve directory (str "p" pass "-r" run ".cbor")))

(defn- write-frame! [^DataOutputStream out usage limit ^bytes bytes]
  (let [n (+ 4 (long (alength bytes)))]
    (when (> (+ @usage n) limit)
      (fail! ::quota-exceeded "Sort exceeds scratch quota." {}))
    (.writeInt out (alength bytes))
    (.write out bytes)
    (vswap! usage + n)))

(defn- open-reader ^DataInputStream [^Path path]
  (DataInputStream. (BufferedInputStream. (FileInputStream. (.toFile path)))))

(defn- read-frame! [^DataInputStream in limit]
  (let [first-byte (.read in)]
    (when (not= -1 first-byte)
      (try
        (let [n (bit-or (bit-shift-left first-byte 24)
                        (bit-shift-left (.readUnsignedByte in) 16)
                        (bit-shift-left (.readUnsignedByte in) 8)
                        (.readUnsignedByte in))]
          (when-not (<= 1 n limit)
            (fail! ::corrupt "Invalid sort frame length." {}))
          (let [bytes (byte-array n)]
            (.readFully in bytes)
            {:bytes bytes :record (cbor/decode-record bytes)}))
        (catch EOFException e
          (throw (ex-info "Incomplete sort frame." {:type ::corrupt} e)))))))

(defn- with-output! [^Path path f]
  (with-open [out (DataOutputStream. (BufferedOutputStream. (FileOutputStream. (.toFile path))))]
    (f out)))

(defn- spill! [directory run window cmp usage opts]
  (with-output! (run-path directory 0 run)
    (fn [out]
      (doseq [entry (sort (fn [a b] (cmp (:record a) (:record b))) window)]
        (write-frame! out usage (:max-bytes opts) (:bytes entry))))))

(defn- initial-runs! [records directory cmp usage opts]
  (let [{:keys [window-bytes window-records]} opts
        {:keys [run window]}
        (reduce (fn [{:keys [run window charge]} record]
                  (let [entry (encode! record opts)]
                    (if (and (seq window)
                             (or (= (count window) window-records)
                                 (> (+ charge (:charge entry)) window-bytes)))
                      (do (spill! directory run window cmp usage opts)
                          {:run (inc run) :window [entry] :charge (:charge entry)})
                      {:run run :window (conj window entry) :charge (+ charge (:charge entry))})))
                {:run 0 :window [] :charge 0} records)]
    (when (seq window) (spill! directory run window cmp usage opts))
    (+ run (if (seq window) 1 0))))

(defn- merge-group! [directory pass start end target cmp usage opts]
  (let [readers (volatile! [])
        primary (volatile! nil)]
    (try
      (doseq [i (range start end)]
        (vswap! readers conj (open-reader (run-path directory pass i))))
      (let [entry-cmp (fn [a b]
                        (let [c (cmp (:record a) (:record b))]
                          (if (zero? c) (compare (:reader a) (:reader b)) c)))]
        (with-output! (run-path directory (inc pass) target)
          (fn [out]
            (loop [q (reduce-kv (fn [q i in]
                                  (if-let [entry (read-frame! in (:max-record-bytes opts))]
                                    (conj q (assoc entry :reader i)) q))
                                (sorted-set-by entry-cmp) @readers)]
              (when-let [entry (first q)]
                (write-frame! out usage (:max-bytes opts) (:bytes entry))
                (let [q (disj q entry)
                      next-entry (read-frame! (nth @readers (:reader entry)) (:max-record-bytes opts))]
                  (recur (if next-entry (conj q (assoc next-entry :reader (:reader entry))) q))))))))
      (catch Throwable e (vreset! primary e) (throw e))
      (finally
        (let [failure (volatile! nil)]
          (doseq [^DataInputStream in @readers]
            (try (.close in) (catch Throwable e (when-not @failure (vreset! failure e)))))
          (when-let [e @failure]
            (if-let [^Throwable original @primary]
              (.addSuppressed original e)
              (throw e)))))))
  (doseq [i (range start end)]
    (let [path (run-path directory pass i)
          size (Files/size path)]
      (Files/delete path)
      (vswap! usage - size))))

(defn- sorted-file! [records directory cmp usage opts]
  (loop [pass 0 runs (initial-runs! records directory cmp usage opts)]
    (cond
      (zero? runs) nil
      (= 1 runs) (run-path directory pass 0)
      :else
      (let [fan-in (:fan-in opts)
            next-runs (quot (+ runs (dec fan-in)) fan-in)]
        (doseq [group (range next-runs)]
          (let [start (* group fan-in)
                end (min runs (+ start fan-in))]
            (if (= (inc start) end)
              (Files/move (run-path directory pass start) (run-path directory (inc pass) group)
                          (make-array CopyOption 0))
              (merge-group! directory pass start end group cmp usage opts))))
        (recur (inc pass) next-runs)))))

(defn- cleanup! [^Path directory]
  (let [failure (volatile! nil)]
    (with-open [entries (Files/newDirectoryStream directory)]
      (doseq [^Path path entries]
        (try (Files/deleteIfExists path)
             (catch Throwable e (when-not @failure (vreset! failure e))))))
    (try (Files/deleteIfExists directory)
         (catch Throwable e (when-not @failure (vreset! failure e))))
    (when-let [e @failure] (throw e))))

(defn with-sorted-records!
  "Call consume with a sorted sequence valid only during that call. The caller
   must not return a lazy result referring to it. Closes readers and removes
   owned scratch on success, early return or exception. Comparator receives
   migration records; its allocations and the caller's retained input/output
   are outside the budget. Windows are charged for encoded data plus a
   conservative structural estimate. Merge retains at most fan-in records.
   Scratch quota includes simultaneous old and new runs during a merge."
  [records cmp options consume]
  (let [opts (validate-options! options)
        attrs (make-array FileAttribute 0)
        directory (if-let [parent (:directory opts)]
                    (Files/createTempDirectory (Path/of (str parent) (make-array String 0)) "datahike-sort-" attrs)
                    (Files/createTempDirectory "datahike-sort-" attrs))
        primary (volatile! nil)]
    (try
      (if-let [path (sorted-file! records directory cmp (volatile! 0) opts)]
        (with-open [in (open-reader path)]
          (letfn [(step []
                    (lazy-seq
                     (when-let [entry (read-frame! in (:max-record-bytes opts))]
                       (cons (:record entry) (step)))))]
            (consume (step))))
        (consume nil))
      (catch Throwable e (vreset! primary e) (throw e))
      (finally
        (try (cleanup! directory)
             (catch Throwable e
               (if-let [^Throwable original @primary]
                 (.addSuppressed original e)
                 (throw e))))))))

(defn reduce-sorted!
  "Reduce sorted records inside the scratch scope; honors reduced."
  [records cmp options f init]
  (with-sorted-records! records cmp options #(reduce f init %)))
