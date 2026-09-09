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

(def ^:private header-bytes 24)

(deftype Cursor [owner ^long start ^long end incarnation]
  Object
  (equals [_ other]
    (and (instance? Cursor other)
         (identical? owner (.-owner ^Cursor other))
         (= start (.-start ^Cursor other))
         (= end (.-end ^Cursor other))
         (= incarnation (.-incarnation ^Cursor other))))
  (hashCode [_]
    (hash [(System/identityHashCode owner) start end incarnation])))

(alter-meta! #'->Cursor assoc :private true)

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
   file bytes respectively. Each frame has a 24-byte length/incarnation header."
  [options]
  (let [{:keys [directory max-frame-bytes max-bytes]} (validate-options! options)
        attrs (make-array FileAttribute 0)
        path (if directory
               (Files/createTempFile (Path/of (str directory) (make-array String 0))
                                     "datahike-build-" ".journal" attrs)
               (Files/createTempFile "datahike-build-" ".journal" attrs))
        owner (Object.)
        descriptor {:id (UUID/randomUUID) :path (str path) :end 0
                    ::cursor (Cursor. owner -1 0 nil)
                    :max-frame-bytes max-frame-bytes :max-bytes max-bytes}]
    (swap! owners assoc (:id descriptor)
           {:metadata (dissoc descriptor :end ::cursor) :capability owner})
    descriptor))

(defn- checked-path ^Path [descriptor]
  (when-not (let [owner (get @owners (:id descriptor))
                  cursor (::cursor descriptor)]
              (and owner (= (:metadata owner) (dissoc descriptor :end ::cursor))
                   (instance? Cursor cursor)
                   (identical? (:capability owner) (.-owner ^Cursor cursor))
                   (integer? (:end descriptor))
                   (= (:end descriptor) (.-end ^Cursor cursor))
                   (<= 0 (:end descriptor) (:max-bytes descriptor))))
    (fail! :backfill.journal/invalid-descriptor "Unknown or modified journal descriptor." {}))
  (let [path (Path/of (:path descriptor) (make-array String 0))]
    (when (Files/isSymbolicLink path)
      (fail! :backfill.journal/invalid-descriptor "Journal path is a symbolic link." {}))
    path))

(defn- check-boundary!
  [^RandomAccessFile file descriptor cursor error-type]
  (when-not (and (instance? Cursor cursor)
                 (identical? (.-owner ^Cursor (::cursor descriptor)) (.-owner ^Cursor cursor))
                 (<= 0 (.-end ^Cursor cursor) (:end descriptor)))
    (fail! error-type "Cursor belongs to another journal or exceeds the accepted prefix." {}))
  (let [^Cursor cursor cursor
        start (.-start cursor)
        end (.-end cursor)]
    (if (zero? end)
      (when-not (and (= -1 start) (nil? (.-incarnation cursor)))
        (fail! error-type "Invalid empty journal cursor." {}))
      (do
        (when-not (and (<= 0 start) (<= header-bytes (- end start))
                       (instance? UUID (.-incarnation cursor)))
          (fail! error-type "Cursor does not identify a complete frame boundary." {}))
        (.seek file start)
        (let [n (.readLong file)
              incarnation (UUID. (.readLong file) (.readLong file))]
          (when-not (= incarnation (.-incarnation cursor))
            (fail! error-type "Journal cursor names an abandoned or modified frame." {}))
          (when-not (and (<= 1 n (:max-frame-bytes descriptor))
                         (= n (- end start header-bytes)))
            (fail! :backfill.journal/corrupt "Invalid journal boundary frame length." {})))))
    cursor))

(defn- check-end! [^RandomAccessFile file descriptor]
  (when (< (.length file) (:end descriptor))
    (fail! :backfill.journal/corrupt "Journal is shorter than its accepted prefix." {}))
  (check-boundary! file descriptor (::cursor descriptor) :backfill.journal/invalid-descriptor))

(defn start-cursor [descriptor]
  (let [path (checked-path descriptor)]
    (with-open [file (RandomAccessFile. (.toFile path) "r")]
      (check-end! file descriptor)
      (Cursor. (.-owner ^Cursor (::cursor descriptor)) -1 0 nil))))

(defn end-cursor [descriptor]
  (let [path (checked-path descriptor)]
    (with-open [file (RandomAccessFile. (.toFile path) "r")]
      (check-end! file descriptor))))

(defn compare-cursors
  "Compare two owned boundaries within this accepted prefix. Stale cursors
   raise invalid-cursor; a stale descriptor raises invalid-descriptor."
  [descriptor left right]
  (let [path (checked-path descriptor)]
    (with-open [file (RandomAccessFile. (.toFile path) "r")]
      (check-end! file descriptor)
      (check-boundary! file descriptor left :backfill.journal/invalid-cursor)
      (check-boundary! file descriptor right :backfill.journal/invalid-cursor)
      (compare (.-end ^Cursor left) (.-end ^Cursor right)))))

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
      (check-end! file descriptor)
      (.setLength file end)
      (.seek file end)
      (let [last-cursor (volatile! (::cursor descriptor))]
        (try
          (doseq [notification notifications]
            (check-scalars! notification max-frame-bytes)
            (let [start (.getFilePointer file)
                  _ (when (> header-bytes (- max-bytes start))
                      (fail! :backfill.journal/quota-exceeded "Journal exceeds disk quota." {}))
                  payload-start (+ start header-bytes)
                  incarnation (UUID/randomUUID)
                  count-bytes (volatile! 0)
                  reserve! (fn [n]
                             (when (> (+ @count-bytes n) max-frame-bytes)
                               (fail! :backfill.journal/frame-too-large "Journal frame exceeds byte limit." {}))
                             (when (> (+ @count-bytes n) (- max-bytes payload-start))
                               (fail! :backfill.journal/quota-exceeded "Journal exceeds disk quota." {}))
                             (vswap! count-bytes + n))
                  out (proxy [OutputStream] []
                        (write
                          ([v] (if (number? v)
                                 (do (reserve! 1) (.write file (int v)))
                                 (let [n (alength ^bytes v)]
                                   (reserve! n) (.write file ^bytes v 0 n))))
                          ([bs offset n] (reserve! n) (.write file ^bytes bs (int offset) (int n)))))]
              (.writeLong file 0)
              (.writeLong file (.getMostSignificantBits incarnation))
              (.writeLong file (.getLeastSignificantBits incarnation))
              (boring/write-to! (boring/writer (min 8192 max-frame-bytes) codec) notification out)
              (let [next-end (.getFilePointer file)]
                (.seek file start)
                (.writeLong file @count-bytes)
                (.seek file next-end)
                (vreset! last-cursor
                         (Cursor. (.-owner ^Cursor (::cursor descriptor)) start next-end incarnation)))))
          (assoc descriptor :end (.getFilePointer file) ::cursor @last-cursor)
          (catch Throwable e
            (try (.setLength file end)
                 (catch Throwable rollback (.addSuppressed e rollback)))
            (throw e)))))))

(defn reduce-range
  "Reduce from an issued cursor through this accepted prefix. Callback receives
   accumulator, value and an opaque next cursor. No earlier payload is decoded.
   Boundary validation reads only the descriptor-end and start frame headers.
   Numeric/foreign/abandoned cursors are rejected; raw file corruption is corrupt.
   Always closes its reader, including reduced, callback and codec exceptions."
  [{:keys [end max-frame-bytes] :as descriptor} start f init]
  (let [path (checked-path descriptor)]
    (with-open [file (RandomAccessFile. (.toFile path) "r")]
      (check-end! file descriptor)
      (check-boundary! file descriptor start :backfill.journal/invalid-cursor)
      (.seek file (.-end ^Cursor start))
      (loop [acc init]
        (let [position (.getFilePointer file)]
          (if (= position end)
            acc
            (do
              (when (> header-bytes (- end position))
                (fail! :backfill.journal/corrupt "Incomplete journal header." {}))
              (let [n (.readLong file)
                    incarnation (UUID. (.readLong file) (.readLong file))]
                (when-not (<= 1 n (min max-frame-bytes (- end position header-bytes)))
                  (fail! :backfill.journal/corrupt "Invalid journal frame length." {}))
                (let [payload (byte-array n)
                      _ (.readFully file payload)
                      cursor (Cursor. (.-owner ^Cursor (::cursor descriptor))
                                      position (.getFilePointer file) incarnation)
                      next-acc (f acc (boring/decode payload codec) cursor)]
                  (if (reduced? next-acc) @next-acc (recur next-acc)))))))))))

(defn reduce-journal
  "Reduce exactly this accepted prefix, decoding one bounded frame at a time."
  [descriptor f init]
  (reduce-range descriptor (start-cursor descriptor) (fn [acc value _] (f acc value)) init))

(defn dispose!
  "Delete only this process's exact owned scratch file. Idempotent."
  [descriptor]
  (when (contains? @owners (:id descriptor))
    (let [path (checked-path descriptor)]
      (Files/deleteIfExists path)
      (swap! owners dissoc (:id descriptor))))
  nil)
