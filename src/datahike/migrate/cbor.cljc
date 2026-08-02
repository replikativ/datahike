(ns ^:no-doc datahike.migrate.cbor
  "The dump codec: one datom per top-level CBOR item, concatenated into an
   RFC 8742 CBOR sequence.

   ## Why this replaced the EDN-lines codec

   EDN-lines existed for one reason — `clj-cbor` narrows zero, NaN and ±Infinity
   doubles to float16 and reads them back as `java.lang.Float`, silently changing
   a `:db.type/double` value's class (datahike #633). Every `#datahike/*` tag it
   carried was a workaround for something EDN cannot express, not a design.

   boring's `:archival` profile removes the reason. It preserves float width by
   construction, so the tag table goes away:

     #datahike/float   -> CBOR f32 (major type 7, additional info 26), natively
     #datahike/bytes   -> CBOR major type 2, no base64 wrapper
     #datahike/farray  -> RFC 8746 typed array, natively
     #datahike/darray  -> RFC 8746 typed array, natively
     #datahike/sysref  -> the only one left; see below

   That is not merely tidier. Base64 cost 33% before compression AND destroyed
   the byte-level redundancy zstd would otherwise find, and it made every binary
   value opaque to a foreign reader — which defeats the reason a dump is CBOR at
   all. It also deleted this namespace's JVM coupling: the EDN codec needed
   `java.util.Base64`, `java.nio.ByteBuffer` and `Float/toString`, which is why
   it could not be `.cljc` and this can.

   ## Why :archival and not :canonical

   A dump wants two things that RFC 8949's deterministic profile pins together
   and which are actually separate: reproducible bytes, and host type identity.
   `:canonical` implements §4.2.2 shortest-form floats, so it narrows EVERY
   Double that fits — #633 reintroduced, and strictly worse than clj-cbor, which
   mangles only three values. `:archival` sorts map keys the same way but keeps
   `:float-policy :preserve-width`. See boring's doc/COMPATIBILITY.md.

   ## Framing

   There is none, and that is the point. Each record encodes to its own byte
   array and they are written consecutively, which IS a CBOR sequence — so the
   writer needs no delimiter logic and `boring/decode-seq-from` reads it back in
   memory bounded by the largest single item rather than by the file.

   A newline-delimited format cannot do this without escaping, which is why the
   EDN codec was line-oriented all the way down into the external sort."
  (:require [boring.core :as boring]))

;; ---------------------------------------------------------------------------
;; system-entity references (#508): translate, never re-insert

(defrecord SysRef [ident])

(defn sysref?
  "Is `v` a system-entity reference?

   A predicate here rather than `instance?` at the call site, because the two
   platforms spell the type differently: on the JVM `SysRef` is a class needing
   an `:import`, on ClojureScript it is a var in this namespace. Inside the
   namespace that defines the record, the bare symbol works on both."
  [v]
  (instance? SysRef v))

(def ^:private sysref-name
  "boring's wire name for the record — the class name with / -> . and - -> _.
   Named explicitly rather than derived so a namespace rename is a visible
   format change rather than a silent one."
  "datahike.migrate.cbor.SysRef")

(def registry
  "The dump registry. Only SysRef needs registering: boring emits a defrecord's
   type name natively via CBOR tag 27, so the WRITE side needs nothing, and the
   READ side just needs the constructor keyed by that name.

   Everything else a datom can hold — string, keyword, symbol, long, double,
   float, bigint, bigdec, instant, uuid, bytes, float[]/double[], vectors for
   tuples — boring already handles as standard CBOR, which is exactly the
   property that makes the dump readable from another language."
  (-> (boring/tag-registry)
      (boring/register-record sysref-name map->SysRef)))

(def opts
  "Encode/decode options. `:archival` plus the registry; nothing else is set,
   because every other knob the profile locks is part of what the profile means."
  {:profile :archival :registry registry})

;; ---------------------------------------------------------------------------
;; value encoding

(defn encode-value
  "Prepare a datom value for encoding. `sysref?` returns the target *ident* when
   `v` is a ref pointing at a system entity, else nil (the caller supplies db
   context).

   Compare the EDN codec, which dispatched on runtime class across six branches:
   the whole of that was teaching EDN about types CBOR already has. What remains
   is the one thing no codec can know — that a particular long is a reference to
   a system entity whose id differs between databases."
  ([v] (encode-value v (constantly nil)))
  ([v sysref?]
   (if-let [ident (sysref? v)]
     (->SysRef ident)
     (if (vector? v)
       ;; tuples: element-wise, keeping the surrounding vector
       (mapv #(encode-value % sysref?) v)
       v))))

;; ---------------------------------------------------------------------------
;; records

(defn encode-record
  "Encode one `[e a v t added]` record to its own CBOR bytes.

   Per-record rather than per-file on purpose: the caller writes these
   consecutively (producing the sequence), feeds the same bytes to the chunk
   SHA-256 and to the semantic digest, and never holds more than one record's
   worth. `:archival` makes the bytes a function of the record alone, so the
   digest is a property of the data rather than of when it was written."
  [record]
  (boring/encode record opts))

(defn concat-records
  "Join per-record encodings into one CBOR sequence (RFC 8742) — just the bytes,
   end to end, since that is what a sequence is.

   Portable because the chunk medium is: a konserve store holds a chunk as one
   binary value, and `bassoc` wants the whole thing. The filesystem medium does
   not call this — it writes each record straight to the open stream and never
   holds a chunk."
  [encodings]
  #?(:clj
     (let [total (reduce + (map alength ^objects (into-array encodings)))
           out (byte-array total)]
       (loop [es (seq encodings) off 0]
         (if es
           (let [^bytes e (first es)]
             (System/arraycopy e 0 out off (alength e))
             (recur (next es) (+ off (alength e))))
           out)))
     :cljs
     (let [total (reduce + (map #(.-length %) encodings))
           out (js/Uint8Array. total)]
       (loop [es (seq encodings) off 0]
         (if es
           (let [e (first es)]
             (.set out e off)
             (recur (next es) (+ off (.-length e))))
           out)))))

(defn decode-records
  "Lazily decode a CBOR sequence into records, in memory bounded by the largest
   single item. The caller owns and closes the source.

   `source` is whatever `fs/reader` returned for this runtime — an
   `InputStream` on the JVM, a pull function on ClojureScript. boring's
   `decode-seq-from` takes each on its own platform, so this needs no reader
   conditional of its own.

   It was JVM-only while boring's ClojureScript reader took a whole buffer and
   had no `decode-seq-from`; it has one now, which is what makes the external
   sort portable. The dump path itself does not use this — both media read a
   chunk at a time, bounded by `:chunk-size`, because a lazy seq that performs
   IO cannot be made async (see `migrate/reduce-dump-records`). The callers are
   the external sort and the JVM-only legacy reader, neither of which is async."
  [source]
  (boring/decode-seq-from source opts))

(defn decode-records-from
  "Lazily decode a CBOR sequence held in memory as bytes.

   The portable counterpart to `decode-records`: bounded by the byte array it is
   given rather than by one record, which is the right trade when the caller
   already holds a whole chunk — as every konserve-store read does."
  [bs]
  #?(:clj  (boring/decode-seq-from (java.io.ByteArrayInputStream. bs) opts)
     :cljs (boring/decode-seq bs opts)))

(defn decode-record
  "Decode a single record from its bytes — for the paths that hold one already."
  [^bytes bs]
  (boring/decode bs opts))
