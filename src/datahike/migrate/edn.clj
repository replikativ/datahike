(ns ^:no-doc datahike.migrate.edn
  "Type-exact EDN-lines codec for datahike dumps.

   One datom is one line: an EDN 5-vector `[e a v t added]`. Values are encoded so
   that the *runtime class* round-trips exactly — the core reason this exists is
   #633, where the CBOR codec routes zero/NaN/Inf `double`s through half-precision
   and reads them back as `java.lang.Float`, silently changing a `:db.type/double`
   value's class (datahike's schema is a bare `double?`/`float?` predicate with no
   coercion, so the flipped class fails validation / compares unequal).

   Encoding is by runtime class (so schema-on-read databases round-trip too), with
   a small, **closed** set of reader tags:

     #datahike/float   \"3.14\"     ; java.lang.Float, exact via Float/toString
     #datahike/bytes   \"<b64>\"    ; byte[]
     #datahike/farray  \"<b64>\"    ; float[]  (IEEE-754 big-endian)
     #datahike/darray  \"<b64>\"    ; double[]
     #datahike/sysref  :db/ident   ; ref value pointing at a system entity (#508)

   plus EDN built-ins `#inst`/`#uuid`. `clojure.core/read-string` and `*read-eval*`
   are never used; parsing is `clojure.edn/read` with this closed `:readers` map and
   a throwing `:default`, so a dump can never execute code on import."
  (:require [clojure.edn :as edn])
  (:import [java.util Base64]
           [java.nio ByteBuffer]))

;; ---------------------------------------------------------------------------
;; system-entity references (#508): translate, never re-insert

(defrecord SysRef [ident])

(defmethod print-method SysRef [^SysRef r ^java.io.Writer w]
  (.write w "#datahike/sysref ")
  (print-method (.-ident r) w))

;; ---------------------------------------------------------------------------
;; primitive-array <-> base64 (exact IEEE-754 bytes, order-preserving)

(def ^:private ^Class float-array-class (class (float-array 0)))
(def ^:private ^Class double-array-class (class (double-array 0)))

(defn- b64-encode ^String [^bytes bs]
  (.encodeToString (Base64/getEncoder) bs))

(defn- b64-decode ^bytes [^String s]
  (.decode (Base64/getDecoder) s))

(defn- floats->b64 [^floats fs]
  (let [bb (ByteBuffer/allocate (* 4 (alength fs)))]
    (dotimes [i (alength fs)] (.putFloat bb (aget fs i)))
    (b64-encode (.array bb))))

(defn- b64->floats [^String s]
  (let [bb (ByteBuffer/wrap (b64-decode s))
        n  (quot (.remaining bb) 4)
        fs (float-array n)]
    (dotimes [i n] (aset fs i (.getFloat bb)))
    fs))

(defn- doubles->b64 [^doubles ds]
  (let [bb (ByteBuffer/allocate (* 8 (alength ds)))]
    (dotimes [i (alength ds)] (.putDouble bb (aget ds i)))
    (b64-encode (.array bb))))

(defn- b64->doubles [^String s]
  (let [bb (ByteBuffer/wrap (b64-decode s))
        n  (quot (.remaining bb) 8)
        ds (double-array n)]
    (dotimes [i n] (aset ds i (.getDouble bb)))
    ds))

;; ---------------------------------------------------------------------------
;; value encoding — dispatch on runtime class, tag only what native EDN can't
;; round-trip class-exactly.

(defn encode-value
  "Return an EDN-printable representation of value `v` whose reader round-trips to
   the identical runtime class. `sysref?` is a predicate `(fn [v] -> truthy)` that
   returns the target *ident* when `v` is a ref pointing at a system entity, else
   nil (the caller supplies db context)."
  ([v] (encode-value v (constantly nil)))
  ([v sysref?]
   (if-let [ident (sysref? v)]
     (->SysRef ident)
     (condp instance? v
       Float                (tagged-literal 'datahike/float (Float/toString v))
       ;; BigInteger prints without the `N` suffix and would read back as Long;
       ;; coerce to clojure.lang.BigInt so it prints `123N` and reads as bigint.
       java.math.BigInteger (bigint v)
       (cond
         (instance? float-array-class v)  (tagged-literal 'datahike/farray (floats->b64 v))
         (instance? double-array-class v) (tagged-literal 'datahike/darray (doubles->b64 v))
         (bytes? v)                       (tagged-literal 'datahike/bytes (b64-encode v))
         ;; tuples: encode element-wise, keep the surrounding native vector
         (vector? v)                      (mapv #(encode-value % sysref?) v)
         ;; native EDN handles: keyword string boolean long double bigint bigdec
         ;; symbol, #inst (Date), #uuid
         :else                            v)))))

;; ---------------------------------------------------------------------------
;; closed reader map — the ONLY tags accepted on import

(def readers
  {'datahike/float  (fn [^String s] (Float/parseFloat s))
   'datahike/bytes  (fn [^String s] (b64-decode s))
   'datahike/farray (fn [^String s] (b64->floats s))
   'datahike/darray (fn [^String s] (b64->doubles s))
   'datahike/sysref (fn [ident] (->SysRef ident))})

(defn- unknown-tag [tag value]
  (throw (ex-info (str "Unknown reader tag in dump: #" tag)
                  {:error :import/unknown-tag :tag tag :value value})))

(def ^:private read-opts
  {:readers readers :default unknown-tag :eof ::eof})

(defn read-record
  "Parse one EDN record line into `[e a v t added]`, resolving only the closed tag
   set. Never evaluates code."
  [^String line]
  (edn/read-string read-opts line))

(defn write-record
  "Render a datom tuple `[e a v t added]` as one EDN line string (no newline).
   `v` must already be run through `encode-value`."
  [record]
  (pr-str record))
