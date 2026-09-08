(ns ^:no-doc datahike.backfill.journal.file
  "Single-writer disk scratch for online builds. Descriptors name immutable
   prefixes, not resumable logs. A process restart must rebuild from its source."
  (:require [boring.core :as boring]
            [datahike.cbor.elements :as elements])
  (:import [java.io RandomAccessFile OutputStream]
           [java.nio.file Files Path]
           [java.nio.file.attribute FileAttribute]
           [java.util UUID]))

(def ^:private owners (atom {}))
(defn descriptor-id [descriptor] (:id descriptor))

(def ^:private codec {:profile :archival
                      :registry (elements/install-element-handlers (boring/tag-registry))})

(defn- fail! [type message data]
  (throw (ex-info message (assoc data :type type))))

(defn validate-options!
  "Validate without I/O and return options with defaults. Nil means defaults.
   Directory existence/access is checked only when a journal is created."
  [options]
  (when-not (or (nil? options) (map? options))
    (fail! :backfill.journal/invalid-options "Journal options must be a map." {}))
  (when (some #(not (contains? #{:directory :max-frame-bytes :max-bytes} %)) (keys options))
    (fail! :backfill.journal/invalid-options "Unknown journal option." {:keys (set (keys options))}))
  (let [{:keys [directory max-frame-bytes max-bytes] :as options}
        (merge {:max-frame-bytes 1048576 :max-bytes 268435456} options)]
    (when-not (or (nil? directory) (instance? Path directory)
                  (and (string? directory) (not (empty? directory))))
      (fail! :backfill.journal/invalid-options "Journal directory must be a nonempty path string or Path." {}))
    (doseq [[k v] [[:max-frame-bytes max-frame-bytes] [:max-bytes max-bytes]]]
      (when-not (and (integer? v) (pos? v)
                     (<= v (if (= k :max-frame-bytes) Integer/MAX_VALUE Long/MAX_VALUE)))
        (fail! :backfill.journal/invalid-options "Journal limits must be positive bounded integers." {k v})))
    options))

(defn create!
  "Create an owned scratch file. Limits include frame payload bytes and total
   file bytes respectively; each notification has an eight-byte length header."
  [options]
  (let [{:keys [directory max-frame-bytes max-bytes]} (validate-options! options)
        attrs (make-array FileAttribute 0)
        path (if directory
               (Files/createTempFile (Path/of (str directory) (make-array String 0))
                                     "datahike-build-" ".journal" attrs)
               (Files/createTempFile "datahike-build-" ".journal" attrs))
        descriptor {:id (UUID/randomUUID) :path (str path) :end 0
                    :max-frame-bytes max-frame-bytes :max-bytes max-bytes}]
    (swap! owners assoc (:id descriptor) (dissoc descriptor :end))
    descriptor))

(defn- checked-path ^Path [descriptor]
  (when-not (and (= (get @owners (:id descriptor)) (dissoc descriptor :end))
                 (integer? (:end descriptor)) (<= 0 (:end descriptor) (:max-bytes descriptor)))
    (fail! :backfill.journal/invalid-descriptor "Unknown or modified journal descriptor." {}))
  (let [path (Path/of (:path descriptor) (make-array String 0))]
    (when (Files/isSymbolicLink path)
      (fail! :backfill.journal/invalid-descriptor "Journal path is a symbolic link." {}))
    path))

(defn- check-scalars!
  "Bound scalar allocations inside the codec before its output cap can act.
   Traversal has a node budget too, avoiding unbounded collection preflight."
  [value limit]
  (let [budget (volatile! (long limit))]
    (letfn [(text-size [^String s]
              (loop [i 0 n 0]
                (if (or (> n limit) (= i (.length s)))
                  n
                  (let [c (.charAt s i)
                        pair? (and (Character/isHighSurrogate c)
                                   (< (inc i) (.length s))
                                   (Character/isLowSurrogate (.charAt s (inc i))))]
                    (recur (+ i (if pair? 2 1))
                           (long (+ n (cond pair? 4 (< (int c) 128) 1 (< (int c) 2048) 2 :else 3))))))))
            (visit [x depth]
              (when (or (> depth 128) (neg? (vswap! budget dec)))
                (fail! :backfill.journal/frame-too-large "Journal value exceeds traversal budget." {}))
              (let [size (cond
                           (string? x) (text-size x)
                           (or (keyword? x) (symbol? x))
                           (+ (text-size (name x)) (if-let [ns (namespace x)] (inc (text-size ns)) 0))
                           (instance? java.math.BigInteger x) (inc (quot (.bitLength ^java.math.BigInteger x) 8))
                           (instance? clojure.lang.BigInt x) (inc (quot (.bitLength (.toBigInteger ^clojure.lang.BigInt x)) 8))
                           (instance? java.math.BigDecimal x) (inc (quot (.bitLength (.unscaledValue ^java.math.BigDecimal x)) 8))
                           (and x (.isArray (class x)))
                           (* (long (java.lang.reflect.Array/getLength x))
                              (let [component (.getComponentType (class x))]
                                (cond
                                  (or (= component Byte/TYPE) (= component Boolean/TYPE)) 1
                                  (or (= component Short/TYPE) (= component Character/TYPE)) 2
                                  (or (= component Float/TYPE) (= component Integer/TYPE)) 4
                                  :else 8)))
                           :else 0)]
                (when (> size limit)
                  (fail! :backfill.journal/frame-too-large "Journal scalar exceeds frame allocation budget." {})))
              (cond
                (ratio? x) (do (visit (numerator x) (inc depth))
                               (visit (denominator x) (inc depth)))
                (map? x) (doseq [[k v] x] (visit k (inc depth)) (visit v (inc depth)))
                (or (sequential? x) (set? x)) (doseq [v x] (visit v (inc depth)))
                (and x (.isArray (class x)) (not (.isPrimitive (.getComponentType (class x)))))
                (doseq [v (seq x)] (visit v (inc depth)))
                (instance? datahike.datom.Datom x) (doseq [v (seq x)] (visit v (inc depth)))))]
      (visit value 0))))

(defn append!
  "Append notifications after this exact prefix and return its new descriptor.
   On failure restore the input prefix. One owner serializes append/dispose;
   concurrent readers may consume previously accepted prefixes."
  [{:keys [end max-frame-bytes max-bytes] :as descriptor} notifications]
  (let [path (checked-path descriptor)]
    (with-open [file (RandomAccessFile. (.toFile path) "rw")]
      (when (< (.length file) end)
        (fail! :backfill.journal/corrupt "Journal is shorter than its accepted prefix." {}))
      (.setLength file end)
      (.seek file end)
      (try
        (doseq [notification notifications]
          (check-scalars! notification max-frame-bytes)
          (let [start (.getFilePointer file)
                payload-start (+ start 8)
                count-bytes (volatile! 0)
                reserve! (fn [n]
                           (when (> (+ @count-bytes n) max-frame-bytes)
                             (fail! :backfill.journal/frame-too-large "Journal frame exceeds byte limit." {}))
                           (when (> (+ payload-start @count-bytes n) max-bytes)
                             (fail! :backfill.journal/quota-exceeded "Journal exceeds disk quota." {}))
                           (vswap! count-bytes + n))
                out (proxy [OutputStream] []
                      (write
                        ([v] (if (number? v)
                               (do (reserve! 1) (.write file (int v)))
                               (let [n (alength ^bytes v)]
                                 (reserve! n) (.write file ^bytes v 0 n))))
                        ([bs offset n] (reserve! n) (.write file ^bytes bs (int offset) (int n)))))]
            (when (> payload-start max-bytes)
              (fail! :backfill.journal/quota-exceeded "Journal exceeds disk quota." {}))
            (.writeLong file 0)
            (boring/write-to! (boring/writer (min 8192 max-frame-bytes) codec) notification out)
            (let [next-end (.getFilePointer file)]
              (.seek file start)
              (.writeLong file @count-bytes)
              (.seek file next-end))))
        (assoc descriptor :end (.getFilePointer file))
        (catch Throwable e
          (try (.setLength file end)
               (catch Throwable rollback (.addSuppressed e rollback)))
          (throw e))))))

(defn reduce-journal
  "Reduce exactly the descriptor's prefix, decoding one bounded frame at a time.
   Honors reduced and closes the file on completion, early return or exception."
  [{:keys [end max-frame-bytes] :as descriptor} f init]
  (let [path (checked-path descriptor)]
    (with-open [file (RandomAccessFile. (.toFile path) "r")]
      (when (< (.length file) end)
        (fail! :backfill.journal/corrupt "Journal is shorter than its accepted prefix." {}))
      (loop [acc init]
        (let [position (.getFilePointer file)]
          (if (= position end)
            acc
            (do
              (when (> (+ position 8) end)
                (fail! :backfill.journal/corrupt "Incomplete journal header." {}))
              (let [n (.readLong file)]
                (when-not (<= 1 n (min max-frame-bytes (- end position 8)))
                  (fail! :backfill.journal/corrupt "Invalid journal frame length." {}))
                (let [payload (byte-array n)
                      _ (.readFully file payload)
                      next-acc (f acc (boring/decode payload codec))]
                  (if (reduced? next-acc) @next-acc (recur next-acc)))))))))))

(defn dispose!
  "Delete only this process's exact owned scratch file. Idempotent."
  [descriptor]
  (when (contains? @owners (:id descriptor))
    (let [path (checked-path descriptor)]
      (Files/deleteIfExists path)
      (swap! owners dissoc (:id descriptor))))
  nil)
