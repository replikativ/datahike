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
  (:require [boring.core :as boring]
            [boring.data :as bdata]
            [clojure.walk :as walk]
            [hasch.core :as hasch]))

;; Resolved once. `Class/forName` is a classloader lookup, and `norm-val` runs
;; once per record on the export path, from `sort.cljc`'s `sort-key` — and only
;; under `:sort? true`. (The manifest's semantic digest hashes `encode-record`
;; bytes and does NOT call this; the other callers are `verify-against`'s
;; id-independent fingerprint, which is not the export path.)
#?(:clj (def ^:private ^Class farray-class (Class/forName "[F")))
#?(:clj (def ^:private ^Class darray-class (Class/forName "[D")))

(defn- array-val?
  "Is `v` one of the three primitive-array value types?

   `:db.type/bytes`, `:db.type/float-array`, `:db.type/double-array` — spelled
   on each platform as the class it actually arrives as."
  [v]
  #?(:clj  (or (bytes? v)
               (instance? farray-class v)
               (instance? darray-class v))
     :cljs (or (instance? js/Uint8Array v)
               (instance? js/Float32Array v)
               (instance? js/Float64Array v))))

(defn- tag-array [v]
  #?(:clj  (cond (bytes? v)                  [:bytes  (hasch/uuid v)]
                 (instance? farray-class v)  [:farray (hasch/uuid v)]
                 :else                       [:darray (hasch/uuid v)])
     :cljs (cond (instance? js/Uint8Array v)   [:bytes  (hasch/uuid v)]
                 (instance? js/Float32Array v) [:farray (hasch/uuid v)]
                 :else                         [:darray (hasch/uuid v)])))

(defn norm-val
  "Stably hashable form of a value: array values compare by CONTENT rather than
   by identity, while keeping their class distinct.

   The content comes from `hasch/uuid`, not from `(vec v)`, and that is the
   whole point. `vec` reads the platform's element type, and the JVM's is
   SIGNED where ClojureScript's is not — the same three bytes gave

     JVM   [:bytes [-1 -128 1]]
     cljs  [:bytes [255 128 1]]

   so the same data normalised differently depending on where it ran.

   The exposure is the SORT KEY, not `verify-against`. `verify-against` is
   JVM-only (`assert-jvm-only!`) and computes both fingerprints in one process,
   so its two sides always agreed with each other. But `sort.cljc` builds its
   key from `norm-val`, so a dump written on Node ordered records differently
   from one written on the JVM — different chunk boundaries, different chunk
   `:sha256`, for the same database. That contradicts the byte-identical-dump
   property `doc/migration.md` says external signing rests on.

   hasch already solves this — it hashes `byte[]`/`Uint8Array`,
   `float[]`/`Float32Array` and `double[]`/`Float64Array` to the same value
   across runtimes, by construction (replikativ/hasch#31), and datahike already
   depends on it for `secondary-only-hash`. Reaching for it here means one
   cross-platform notion of value identity instead of two that disagree.

   The type tag stays, so a byte array and a float array of the same content
   remain distinct — hasch would also keep them apart, but the tag says so
   locally and keeps the shape a reader can see.

   ## Inside collections too, and why that is not optional

   A `:db.type/tuple` whose `:db/tupleTypes` names one of the three arrives as a
   VECTOR holding an array, so a check on the top-level class alone walks past
   it. The first version of this function did exactly that, and both callers
   were wrong for tuples in the way each is wrong for a bare array:

     sort.cljc   `(str [#object[\"[B\" 0x10fc35db …] \"a\"])` — the identity hash
                 again, so chunk boundaries still varied between JVM runs for a
                 database holding tuples.
     migrate.cljc `verify-against` tier 3 puts `(norm-val v)` in a SET, and two
                 `byte[]` are never `=`, so an INTACT dump reported
                 `:field-mismatch` on every sampled entity.

   So the rewrite is a `postwalk`. Only collection values pay for it — a scalar
   takes the first `cond` arm and allocates nothing — and only tuples and
   free-schema nested data are collections at all.

   ## The `::normalized` marker, and what it does NOT do

   It marks the containers this rewrote, so the result says of itself that it is
   a normalised form rather than a value. A collection holding no array is
   returned UNCHANGED and unmarked — `identical?`, not merely `=` — so existing
   dumps move only where they were already nondeterministic.

   It does NOT make the mapping injective, and an earlier version of this
   docstring claimed it did. Measured: `(norm-val [bs \"a\"])` and
   `(norm-val [::normalized [[:bytes (hasch/uuid bs)] \"a\"]])` are the same
   value, because the second holds no array and so passes through untouched.
   The marker moved the collision rather than removing it; escaping a
   pre-existing marker during the walk would be the actual fix. It is left as
   is because the consequence is bounded: two values sharing a sort key TIE, and
   a tie is broken deterministically — `sort` is stable and `merge-runs` breaks
   by run index — so the dump stays byte-identical. Only a value hand-built to
   look like this namespace's output is affected, and only in its ordering
   against the value it imitates."
  [v]
  (cond
    (array-val? v) (tag-array v)

    (coll? v)
    (let [touched? (volatile! false)
          w (walk/postwalk (fn [x]
                             (if (array-val? x)
                               (do (vreset! touched? true) (tag-array x))
                               x))
                           v)]
      (if @touched? [::normalized w] v))

    :else v))

;; ---------------------------------------------------------------------------
;; system-entity references (#508): translate, never re-insert

(defrecord AttrRef [eid])

(defn attr-ref?
  "Is `a` a record's attribute given as an attribute-ENTITY reference?

   The `a` slot of a dump record is normally a keyword ident, and for a
   keyword-attribute source that is the whole truth: there the datom holds the
   keyword, so nothing is resolved at either end.

   Under `:attribute-refs?` the datom holds the attribute's entity id instead,
   and a name is a property of that entity which can CHANGE. Naming such a datom
   in the dump therefore requires choosing a moment, and choosing the export
   moment is what broke: a datom written before a rename came out carrying a name
   that did not exist yet, and replaying the dump met it before the name was
   installed. So the record carries the entity, and the READER picks the name its
   target needs — see `migrate/resolve-attr-refs`."
  [a]
  (instance? AttrRef a))

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
  "boring's wire name for the record, DERIVED rather than written out.

   This was a hand-written `\"datahike.migrate.cbor.SysRef\"` — the CLASS name,
   dots and all. boring writes `namespace/Name`; its own `record-type-name`
   docstring is explicit that a slash separates the two because a dot is
   ambiguous (`a.b.c.D` could split either way). So the name never matched, the
   registry supplied no constructor, and every encoded SysRef decoded to a
   `boring.data/UnknownRecord`. `sysref?` is an `instance?` check, so it answered
   false, `resolve-sysrefs` never fired, and a ref value naming a system entity
   was imported as that opaque record rather than the target's eid. Nothing
   raised — an unregistered record decodes and re-encodes to identical bytes by
   design, which is what made it silent.

   `boring/register-record`'s own docstring gives `\"my.ns.Point\"` as its
   example, which is how the wrong form got written; that is a boring
   documentation bug and is being reported upstream.

   Deriving it removes the class of mistake: `record-type-name` is portable,
   returns exactly what the writer emits on both platforms, and cannot drift from
   it when this namespace is renamed."
  (bdata/record-type-name (map->SysRef {})))

(def ^:private attr-ref-name
  "As `sysref-name`, derived for the same reason."
  (bdata/record-type-name (map->AttrRef {})))

(def registry
  "The dump registry. Only SysRef needs registering: boring emits a defrecord's
   type name natively via CBOR tag 27, so the WRITE side needs nothing, and the
   READ side just needs the constructor keyed by that name.

   Everything else a datom can hold — string, keyword, symbol, long, double,
   float, bigint, bigdec, instant, uuid, bytes, float[]/double[], vectors for
   tuples — boring already handles as standard CBOR, which is exactly the
   property that makes the dump readable from another language."
  (-> (boring/tag-registry)
      (boring/register-record sysref-name map->SysRef)
      (boring/register-record attr-ref-name map->AttrRef)))

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
