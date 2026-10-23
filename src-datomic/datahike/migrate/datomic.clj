(ns datahike.migrate.datomic
  "Datomic Pro as a datahike migration SOURCE and SINK. **Experimental.**

   Nothing in datahike requires this namespace. It requires `datomic.api`, so it
   loads only when `com.datomic/peer` is on the classpath — which is the point:
   datahike takes no Datomic dependency, and a user who has one gets migration
   in both directions by requiring this.

   ## Scope: Pro, via the PEER api

     (require '[datahike.migrate.datomic :as dtm])
     (dtm/import-from-datomic! dh-conn datomic-conn)   ; Datomic -> datahike
     (dtm/export-to-datomic!   @dh-conn datomic-conn)  ; datahike -> Datomic

   The peer API (`datomic.api`) is **Pro-only**. Datomic Cloud and Datomic Local
   speak the CLIENT api (`datomic.client.api`), which has no `d/log` and
   therefore no way to read the transaction history this source is built on.
   Supporting them means a separate namespace and a different history strategy;
   it is not a matter of swapping the require.

   ## The seam

   Both directions go through `datahike.migrate`'s record seam — the same one
   dumps use — so a Datomic migration inherits the batcher, the id mapping, the
   verification and the error attribution rather than reimplementing them. The
   record is `[e a v t op]`.

   ## Identity: what survives and what cannot

   Measured, not assumed (`.internal/datomic-prototype/FINDINGS.md` has the
   probes). Two hard limits shape everything here.

   **Datomic's ids do not fit datahike's.** datahike's `emax`/`txmax` are
   `0x7FFFFFFF` = 2 147 483 647. A Datomic user eid is around 1.76e13 and a tx
   entity id around 1.3e13 — four orders of magnitude above the ceiling. So
   `{:eids :preserve}` is impossible against a Datomic source and is REFUSED
   here rather than silently downgraded. Worse than impossible, it is quiet:
   datahike does not range-check an incoming eid, it simply reallocates, so
   passing Datomic ids through would look like it worked.

   **datahike assigns its own `t`.** Measured through `import-source`: a stream
   stamped `tx0+1000, tx0+1001` lands as `536870913, 536870914` — transaction ids
   are assigned sequentially in source order. The source must still map `t` above
   `tx0`, because a `t` below it is refused as `:import/malformed-record`, but the
   value it chooses is not the value that lands. Hence `datomic-t->dh-t`, which
   exists to satisfy the contract and to order transactions, not to be preserved.

   So: entity and transaction ids are REMAPPED in both directions, transaction
   times and ordering are PRESERVED. A round trip is value-identical and
   history-identical, not id-identical.

   ## Provenance: which datahike transaction was which Datomic one

   Because the ids do not survive, the correspondence is recorded as DATA rather
   than inferred from a numbering convention. Each imported transaction carries
   two datoms ON THE TRANSACTION ENTITY — tx-meta, in the sense that a record
   whose `e` equals its `t` is a datom about the transaction itself:

     :datomic/t        the source `t`      (1004)
     :datomic/tx-eid   the source tx eid   (13194139534316)

   so the question is a query rather than arithmetic:

     (d/q '[:find ?tx :in $ ?t :where [?tx :datomic/t ?t]] @conn 1004)
     ;=> #{[536870917]}

   This needs nothing from datahike that a dump does not already need:
   `transact-entities-directly` builds `(dd/datom new-t a v new-t op)` for a
   record whose `e` is its `t`, so ANY attribute on a transaction entity is
   carried and rewritten to the new id. The source emits the schema for these two
   attributes itself, in the first transaction, exactly as it must for every other
   attribute it uses.

   The alternative — teaching datahike to accept caller-supplied transaction ids
   (a `:tids` option beside `:eids`) — was considered and rejected for this
   purpose. The lever exists (`(get-in migration-state [:tids t] max-tid)`), but
   it would still not reproduce Datomic's ids, since 1.3e13 does not fit in
   `txmax`; it would only choose a different mapping, which is what these two
   attributes already do, queryably and without changing the import contract.

   `{:provenance? false}` turns them off.

   ## Direction-specific limits

   Into Datomic (`export-to-datomic!`):

     * `:db/txInstant` is monotonic-only and must be >= the database basis. A
       FRESH Datomic database has basis instant 1970-01-01, so real historical
       times replay from the first transaction onward provided they ascend.
       Appending into a POPULATED database whose basis is newer than your oldest
       record cannot work, and Datomic says so with `:db.error/past-tx-instant`.
     * Datomic refuses datahike-only schema — `:db/maxLength`, `:db.valid/from`,
       `:db.valid/to`, `:db.secondary/*`, and the value types datahike adds — with
       `:db.error/not-an-entity`. Unknown APPLICATION attributes are fine. This
       namespace strips datahike-only schema by default (`:strip-datahike-schema?`)
       because the alternative is a failed transaction halfway through.
     * Boot attributes cannot be re-asserted: `:db/index` and `:db/txInstant`
       raise `:db.error/datom-cannot-be-altered`. (`:db/doc` is accepted and
       silently redefines, which is why the schema filter is explicit rather
       than trusting Datomic to reject what it should.)
     * Re-asserting an existing `:db.unique/identity` value UPSERTS onto the
       existing entity. Exporting into a non-empty Datomic database therefore
       merges rather than duplicates, which may or may not be what you want.

   From Datomic (`import-from-datomic!`):

     * `:db.type/uri` has no datahike equivalent. Values arrive as `java.net.URI`
       and are carried as their string form, losing the type. See #135.
     * Transaction functions (`:db/fn`, `:db.fn/cas`, …), `:db/fulltext`,
       `:db/lang` and the excision vocabulary have no datahike equivalent and are
       dropped with a warning rather than failing the import.
     * Excised data is, by definition, absent from the log and cannot be
       recovered by any reader."
  (:require [datomic.api :as dt]
            [datahike.migrate :as m]
            [datahike.migrate.cbor :as mcbor]
            [datahike.constants :as const]
            [replikativ.logging :as log])
  (:import [datomic Datom]))

;; ---------------------------------------------------------------------------
;; vocabulary

(def ^:private datomic-only-attrs
  "Datomic vocabulary datahike has no equivalent for. Dropped from a record
   stream flowing INTO datahike, with a warning, rather than failing the import
   on a database that merely happens to use one."
  #{:db/fn :db/code :db/lang :db/fulltext :db/system-tx
    :db.excise/attrs :db.excise/before :db.excise/beforeT
    :db.install/function :db.install/partition :db.install/valueType
    :db.alter/attribute})

(def ^:private install-attrs
  "The `:db.install/*` bookkeeping Datomic writes when schema is transacted.
   datahike derives its schema from `:db/ident` + `:db/valueType` +
   `:db/cardinality` directly, so these carry no information for it and their
   ref values point at attribute entities, which would dangle."
  #{:db.install/attribute :db.install/partition :db.install/valueType
    :db.alter/attribute :db.excise/attrs})

(def ^:private datahike-only-attrs
  "datahike vocabulary Datomic REFUSES with `:db.error/not-an-entity`. Stripped
   from a stream flowing INTO Datomic unless `:strip-datahike-schema? false`."
  #{:db/maxLength :db/noCommit :db/retracted
    :db.valid/from :db.valid/to
    :db.secondary/type :db.secondary/config :db.secondary/attrs
    :db.secondary/only :db.secondary/status :db.secondary/status-type
    :db.secondary/building-since-tx
    :db.meta/attributes})

(def ^:private schema-defining-attrs
  "The attributes whose presence makes a datom part of an attribute's DEFINITION.
   Datomic installs an attribute in the transaction that carries these and refuses
   to use it until the NEXT one, so the sink splits a source transaction on this
   set. datahike has no such rule, which is why a dump can hold both in one."
  #{:db/ident :db/valueType :db/cardinality :db/unique :db/index
    :db/isComponent :db/noHistory :db/doc})

(def ^:private datomic-boot-attrs
  "Attributes whose definition Datomic ships and refuses to have altered
   (`:db.error/datom-cannot-be-altered`). A schema stream must not re-assert
   them. `:db/doc` is deliberately NOT here: Datomic accepts a redefinition of
   it silently, which is exactly why the filter is explicit."
  #{:db/ident :db/valueType :db/cardinality :db/unique :db/index
    :db/txInstant :db/doc :db/isComponent :db/noHistory})

;; ---------------------------------------------------------------------------
;; id mapping

(defn- datomic-t->dh-t
  "Datomic's `t` into datahike's transaction space.

   `t` is small and sequential (the first user transaction is 1000); datahike
   requires a transaction id above `tx0` and refuses anything below it as
   `:import/malformed-record`. Offsetting by `tx0` is order-preserving and
   keeps the gaps, which matters only for readability — datahike reassigns ids
   sequentially anyway (see the ns docstring).

   Refused rather than wrapped on overflow. The headroom is
   `txmax - tx0` = 1 610 612 735 transactions; a database past that cannot be
   imported at all and should learn so here rather than through a silently
   wrong id."
  [t]
  (let [t' (+ const/tx0 t)]
    (when (> t' const/txmax)
      (throw (ex-info (str "Datomic t " t " maps above datahike's txmax: this database has "
                           "more transactions than datahike can represent (" (- const/txmax const/tx0) ").")
                      {:error :datomic/t-overflow :t t :mapped t' :txmax const/txmax})))
    t'))

;; ---------------------------------------------------------------------------
;; partition-mapping: a COMPUTED :eids mapping for a Datomic source
;;
;; Datomic ids are `(partition << 42) | n`, and `n` comes from ONE counter that
;; transactions and every partition share. A partition's ids are therefore not
;; dense: they are spread over every number minted while it was in use. Three
;; partitions written alternately each span three times their own entity count,
;; so a layout of one `[min max]` region per partition needs about three times
;; the ids — and a database with 39 partitions in concurrent use cannot be laid
;; out that way below `tx0` at all.
;;
;; So the mapping is RANK-based: the k-th distinct id of a partition, in id
;; order, becomes `start + k`. The target id space is exactly the number of
;; distinct ids, whatever the source's interleaving. The ids come from the
;; history EAVT index (every entity with a datom) and the history VAET index
;; (every id a ref names, including ids that never had a datom of their own),
;; read partition by partition, so they arrive sorted and nothing is sorted
;; here.
;;
;; Each partition keeps the smallest of three encodings of its id set:
;;
;;   :range   the ids are contiguous: no memory at all
;;   :bitmap  one bit per id in [min, max] plus an int rank per 512 bits:
;;            span/8 × 1.06 bytes. Wins when more than ~1 id in 10 is used.
;;   :table   blocks of 64 ids: the first id as a long, the rest as varint
;;            gaps, and the block's byte offset as an int. About 1.2 B per id
;;            when gaps are below 128, 2.2 below 16 384.
;;
;; A lookup is O(1) for :range/:bitmap and a binary search over blocks plus at
;; most 63 varint decodes for :table.

(def ^:private ^:const part-bits
  "A Datomic entity id is `(partition << 42) | n`."
  42)

(defn- part-of ^long [^long e] (bit-shift-right e part-bits))

(def ^:private tx-part
  "Datomic's transaction partition, 3."
  (part-of (dt/t->tx 1)))

(defn- tx-eid? [x] (and (integer? x) (= tx-part (part-of (long x)))))

(defn- index-id ^long [index ^Datom d]
  (long (if (= :vaet index) (.v d) (.e d))))

(defn- first-id-at-or-after
  "The smallest id >= `from` in `index` (:eavt: an entity with a datom; :vaet: an
   id some ref names), or nil."
  [hdb index ^long from]
  (when-let [^Datom d (first (dt/seek-datoms hdb index from))]
    (index-id index d)))

(defn- max-id-below
  "The largest id in `[lo, hi)` of `index`, given that `lo` is one. Binary search
   on \"is there an id at or after x, below hi\" — about 42 seeks, not a scan."
  [hdb index ^long lo ^long hi]
  (loop [lo lo hi hi]                   ; invariant: P(lo) true, P(hi) false
    (if (<= (- hi lo) 1)
      lo
      (let [mid (+ lo (quot (- hi lo) 2))
            e (first-id-at-or-after hdb index mid)]
        (if (and e (< (long e) hi))
          (recur mid hi)
          (recur lo mid))))))

(defn- index-extents [hdb index]
  (loop [from 0 acc (sorted-map)]
    (if-let [e (first-id-at-or-after hdb index from)]
      (let [p (part-of e)
            hi (bit-shift-left (inc p) part-bits)
            mx (max-id-below hdb index e hi)]
        (recur hi (if (= p tx-part) acc (assoc acc p [e mx]))))
      acc)))

(defn partition-extents
  "`{partition [min-id max-id]}` over the WHOLE history of `db`, for every
   partition but the transaction partition, by index seeks: O(partitions × 42)
   per index, no scan.

   The union of history EAVT (entities with a datom) and history VAET (ids a ref
   names). VAET matters: a ref may name an id that never had a datom — a
   never-used id, an excised entity, or one in a partition that holds nothing
   else — and those ids need a target id too.

   These are BOUNDS, used to check a mapping against a database cheaply. They
   are not a layout: see the section comment on why `[min max]` regions do not
   fit a real Datomic database."
  [db]
  (let [hdb (dt/history db)]
    (merge-with (fn [[a b] [c d]] [(min (long a) (long c)) (max (long b) (long d))])
                (index-extents hdb :eavt)
                (index-extents hdb :vaet))))

(defn- id-cursor
  "A 0-arg fn returning the next DISTINCT id of `index` in partition `p`,
   ascending, or -1 when there are no more."
  [hdb index ^long p]
  (let [it (.iterator ^Iterable (dt/seek-datoms hdb index (bit-shift-left p part-bits)))
        hi (bit-shift-left (inc p) part-bits)
        prev (long-array [-1])
        done (boolean-array 1)]
    (fn ^long []
      (if (aget done 0)
        -1
        (loop []
          (if (.hasNext it)
            (let [x (index-id index (.next it))]
              (cond (>= x hi) (do (aset done 0 true) -1)
                    (== x (aget prev 0)) (recur)
                    :else (do (aset prev 0 x) x)))
            (do (aset done 0 true) -1)))))))

(definterface IRank
  (^long rank [^long x] "The rank of id x in this region's set, or -1."))

(def ^:private ^:const block 64)

(deftype RangeRegion [^long lo ^long cnt]
  IRank
  (rank [_ x] (let [o (- x lo)] (if (and (>= o 0) (< o cnt)) o -1))))

(deftype BitmapRegion [^long lo ^long nbits ^longs words ^ints ranks]
  IRank
  (rank [_ x]
    (let [o (- x lo)]
      (if (or (< o 0) (>= o nbits))
        -1
        (let [w (int (unsigned-bit-shift-right o 6))
              word (aget words w)
              bit (bit-and o 63)
              m (bit-shift-left 1 bit)]
          (if (zero? (bit-and word m))
            -1
            (let [b (bit-shift-right w 3)]
              (loop [i (bit-shift-left b 3) acc (long (aget ranks b))]
                (if (< i w)
                  (recur (inc i) (+ acc (Long/bitCount (aget words i))))
                  (+ acc (Long/bitCount (bit-and word (unchecked-dec m)))))))))))))

(defn- read-varint
  "Decode the varint at `pos` into `out`: slot 0 the value, slot 1 the position
   after it."
  [^bytes data ^long pos ^longs out]
  (loop [p pos shift 0 v 0]
    (let [b (long (aget data p))
          v (bit-or v (bit-shift-left (bit-and b 0x7f) shift))]
      (if (neg? b)                      ; continuation bit (byte is signed)
        (recur (inc p) (+ shift 7) v)
        (do (aset out 0 v) (aset out 1 (inc p)) nil)))))

(deftype TableRegion [^long cnt ^longs firsts ^ints pos ^bytes data]
  IRank
  (rank [_ x]
    (let [n (alength firsts)]
      (if (or (zero? n) (< x (aget firsts 0)))
        -1
        ;; the last block whose first id is <= x
        (let [b (loop [lo 0 hi (dec n)]
                  (if (< lo hi)
                    (let [mid (unsigned-bit-shift-right (+ lo hi 1) 1)]
                      (if (<= (aget firsts mid) x) (recur mid hi) (recur lo (dec mid))))
                    lo))
              f (aget firsts b)
              base (* (long b) block)]
          (if (== f x)
            base
            (let [in-block (min (long block) (- cnt base))]
              ;; walk the block's varint gaps, decoded inline (no allocation
              ;; on this path: it runs once per id of a 393M-datom import)
              (loop [k 1 v f p (long (aget pos b)) shift 0 gap 0]
                (if (>= k in-block)
                  -1
                  (let [byte (long (aget data p))
                        gap (bit-or gap (bit-shift-left (bit-and byte 0x7f) shift))]
                    (if (neg? byte)
                      (recur k v (inc p) (+ shift 7) gap)
                      (let [v (+ v gap)]
                        (cond (== v x) (+ base k)
                              (> v x) -1
                              :else (recur (inc k) v (inc p) 0 0))))))))))))))

(defn- table-ids
  "Reduce `f` over a TableRegion's ids, ascending."
  [^TableRegion t f]
  (let [^longs firsts (.-firsts t) ^ints pos (.-pos t) ^bytes data (.-data t)
        cnt (.-cnt t) out (long-array 2)]
    (dotimes [b (alength firsts)]
      (let [base (* (long b) block)
            in-block (min (long block) (- cnt base))]
        (loop [k 0 v (aget firsts b) p (long (aget pos b))]
          (when (< k in-block)
            (f v)
            (when (< (inc k) in-block)
              (read-varint data p out)
              (recur (inc k) (+ v (aget out 0)) (aget out 1)))))))))

(definterface IIdSink
  (add [^long x])
  (finish []))

(defn- digest-step ^long [^long h ^long x]
  (unchecked-add (unchecked-multiply h 1099511628211) x))

(deftype TableBuilder [^:unsynchronized-mutable ^long cnt
                       ^:unsynchronized-mutable ^long prev
                       ^:unsynchronized-mutable ^long digest
                       ^:unsynchronized-mutable ^longs firsts
                       ^:unsynchronized-mutable ^ints pos
                       ^:unsynchronized-mutable ^bytes data
                       ^:unsynchronized-mutable ^long dlen]
  IIdSink
  (add [_ x]
    (when (and (pos? cnt) (<= x prev))
      (throw (ex-info "partition-mapping ids must arrive ascending and distinct"
                      {:error :datomic/partition-mapping-unsorted :id x :prev prev})))
    (if (zero? (rem cnt block))
      (let [b (quot cnt block)]
        (when (>= b (alength firsts))
          (set! firsts (java.util.Arrays/copyOf firsts (int (* 2 (alength firsts)))))
          (set! pos (java.util.Arrays/copyOf pos (int (* 2 (alength pos))))))
        (aset firsts b x)
        (aset pos b (int dlen)))
      (loop [v (- x prev)]
        (when (>= (+ dlen 1) (alength data))
          (set! data (java.util.Arrays/copyOf data (int (min Integer/MAX_VALUE
                                                             (* 2 (alength data)))))))
        (if (>= v 0x80)
          (do (aset data dlen (unchecked-byte (bit-or (bit-and v 0x7f) 0x80)))
              (set! dlen (inc dlen))
              (recur (unsigned-bit-shift-right v 7)))
          (do (aset data dlen (unchecked-byte v))
              (set! dlen (inc dlen))))))
    (set! prev x)
    (set! digest (digest-step digest x))
    (set! cnt (inc cnt))
    nil)
  (finish [_]
    (let [nb (int (quot (+ cnt (dec block)) block))]
      {:count cnt
       :digest digest
       :table (TableRegion. cnt
                            (java.util.Arrays/copyOf firsts nb)
                            (java.util.Arrays/copyOf pos nb)
                            (java.util.Arrays/copyOf data (int dlen)))})))

(defn- table-builder []
  (TableBuilder. 0 -1 0 (long-array 16) (int-array 16) (byte-array 256) 0))

(defn- table-bytes ^long [^TableRegion t]
  (+ (* 12 (alength ^longs (.-firsts t))) (alength ^bytes (.-data t))))

(defn- bitmap-bytes ^long [^long span]
  (let [nwords (quot (+ span 63) 64)]
    (+ (* 8 nwords) (* 4 (quot (+ nwords 7) 8)))))

(defn- table->bitmap [^TableRegion t ^long lo ^long span]
  (let [nwords (int (quot (+ span 63) 64))
        words (long-array nwords)
        ranks (int-array (quot (+ nwords 7) 8))]
    (table-ids t (fn [^long x]
                   (let [o (- x lo) w (int (unsigned-bit-shift-right o 6))]
                     (aset words w (bit-or (aget words w) (bit-shift-left 1 (bit-and o 63)))))))
    (loop [w 0 acc 0]
      (when (< w nwords)
        (when (zero? (rem w 8)) (aset ranks (quot w 8) (int acc)))
        (recur (inc w) (+ acc (Long/bitCount (aget words w))))))
    (BitmapRegion. lo span words ranks)))

(defn- encode-region
  "The smallest encoding of a finished table: `[kind region bytes]`."
  [{:keys [^TableRegion table count]} ^long lo ^long hi]
  (let [span (inc (- hi lo))
        tb (table-bytes table)
        bb (bitmap-bytes span)]
    (cond
      (== (long count) span) [:range (RangeRegion. lo span) 0]
      (< bb tb)              [:bitmap (table->bitmap table lo span) bb]
      :else                  [:table table tb])))

(defn build-region
  "Encode one partition's ids, given as an ascending, distinct `reducible` of
   longs. `{:kind :min :max :count :digest :bytes :region}`; nil when empty.
   Public so the encoding can be measured without a Datomic database."
  [reducible]
  (let [^IIdSink b (table-builder)
        mm (long-array [Long/MAX_VALUE Long/MIN_VALUE])]
    (reduce (fn [_ x]
              (let [x (long x)]
                (.add b x)
                (when (< x (aget mm 0)) (aset mm 0 x))
                (when (> x (aget mm 1)) (aset mm 1 x))
                nil))
            nil reducible)
    (let [{:keys [count] :as fin} (.finish b)]
      (when (pos? (long count))
        (let [lo (aget mm 0) hi (aget mm 1)
              [kind region bytes] (encode-region fin lo hi)]
          {:kind kind :min lo :max hi :count count :digest (:digest fin)
           :bytes bytes :region region})))))

(defn- partition-ids
  "The DISTINCT ids of partition `p` in history EAVT ∪ history VAET, ascending,
   as a reducible — the two indexes merged on the fly, nothing materialised."
  [hdb ^long p]
  (reify clojure.lang.IReduceInit
    (reduce [_ f init]
      (let [ca (id-cursor hdb :eavt p) cb (id-cursor hdb :vaet p)]
        (loop [a (long (ca)) b (long (cb)) acc init]
          (if (reduced? acc)
            @acc
            (cond
              (and (neg? a) (neg? b)) acc
              (or (neg? b) (and (>= a 0) (< a b))) (recur (long (ca)) b (f acc a))
              (or (neg? a) (< b a)) (recur a (long (cb)) (f acc b))
              :else (recur (long (ca)) (long (cb)) (f acc a)))))))))

(defn- present-partitions
  "The partitions, other than the transaction partition, with any id in history
   EAVT or VAET, ascending. Found by seeking, not scanning."
  [hdb]
  (loop [from 0 acc []]
    (let [a (first-id-at-or-after hdb :eavt from)
          b (first-id-at-or-after hdb :vaet from)
          x (cond (and a b) (min (long a) (long b)) a a b b)]
      (if-not x
        acc
        (let [p (part-of x)]
          (recur (bit-shift-left (inc p) part-bits) (if (= p tx-part) acc (conj acc p))))))))

(defn- source-db
  "The Datomic db value a mapping or an import reads: `db-or-conn` as of
   `basis-t` when given (and older than the db), else as it is."
  [db-or-conn basis-t]
  (let [db (if (instance? datomic.Connection db-or-conn) (dt/db db-or-conn) db-or-conn)
        cur (dt/basis-t db)]
    (cond
      (nil? basis-t) [db cur]
      (> (long basis-t) (long cur))
      (throw (ex-info (str "basis-t " basis-t " is ahead of the Datomic database (basis-t " cur ").")
                      {:error :datomic/basis-t-ahead :basis-t basis-t :source-basis-t cur}))
      (= (long basis-t) (long cur)) [db cur]
      :else [(dt/as-of db basis-t) (long basis-t)])))

(defn- scan-regions
  "`[{partition built-region}]` for every partition of `db`, in partition order."
  [db]
  (let [hdb (dt/history db)]
    (into []
          (keep (fn [p]
                  (when-let [r (build-region (partition-ids hdb p))]
                    (log/info :datomic/partition-mapping-region
                              (assoc (dissoc r :region :digest) :partition p))
                    (assoc r :partition p))))
          (present-partitions hdb))))

(defn- refuse-params! [msg params & [data]]
  (throw (ex-info (str "Invalid partition-mapping parameters: " msg)
                  (merge {:error :datomic/partition-mapping-invalid :params params} data))))

(defn- validate-params!
  "Regions in ascending partition order, each a well-formed id set, laid out
   one after another without overlap, from `:base + :reserved` to `:end`, and
   `:end` at most `tx0`."
  [{:keys [base reserved regions end basis-t] :as params}]
  (when-not (and (nat-int? base) (nat-int? reserved) (integer? end) (integer? basis-t)
                 (sequential? regions))
    (refuse-params! "need integer :base, :reserved, :end, :basis-t and a :regions vector" params))
  (loop [[r & more] regions prev-p -1 next-start (+ (long base) (long reserved))]
    (if-not r
      (when (not= (long end) next-start)
        (refuse-params! (str ":end is " end ", the regions end at " next-start) params))
      (let [{:keys [partition kind min max count start]} r]
        (when-not (and (integer? partition) (> (long partition) prev-p) (not= partition tx-part))
          (refuse-params! "partitions must be distinct, ascending, and not the tx partition"
                          params {:region r}))
        (when-not (#{:range :bitmap :table} kind)
          (refuse-params! (str "unknown region kind " kind) params {:region r}))
        (when-not (and (integer? min) (integer? max) (<= (long min) (long max))
                       (= (long partition) (part-of min)) (= (long partition) (part-of max))
                       (pos-int? count) (<= (long count) (inc (- (long max) (long min))))
                       (or (not= :range kind) (= (long count) (inc (- (long max) (long min))))))
          (refuse-params! "a region's :min/:max/:count do not describe an id set in its partition"
                          params {:region r}))
        (when-not (= (long start) next-start)
          (refuse-params! (str "region of partition " partition " starts at " start
                               ", not right after the previous one (" next-start ")")
                          params {:region r}))
        (recur more (long partition) (+ (long start) (long count))))))
  (when (> (long end) (long const/tx0))
    (throw (ex-info (str "This partition-mapping needs entity ids up to " end
                         ", above tx0 (" const/tx0 "), where datahike's transaction ids begin."
                         " The source has " (- (long end) (long base) (long reserved))
                         " distinct entity ids; with :base " base " only "
                         (- (long const/tx0) (long base) (long reserved))
                         " fit. The ids are already packed exactly (one per entity), so the"
                         " database cannot be imported into one datahike database as is;"
                         " a lower :base helps only if it was raised.")
                    {:error :datomic/partition-mapping-exceeds-tx0
                     :end end :tx0 const/tx0})))
  params)

(deftype PartitionMapping [params regions]
  clojure.lang.ILookup
  (valAt [this k] (.valAt this k nil))
  (valAt [_ k nf]
    (cond
      (not (integer? k)) nf
      ;; datahike transaction ids: a rewritten tx-entity `e` or a ref to one.
      ;; Answering "not mine" hands them to the import's own `:tids`.
      (<= (long const/tx0) (long k) (long const/txmax)) nf
      ;; the source's provenance schema (`provenance-schema`), at -1, -2 …
      (neg? (long k))
      (let [i (dec (- (long k)))]
        (when (>= i (long (:reserved params)))
          (throw (ex-info (str "Negative source id " k " is outside the " (:reserved params)
                               " ids partition-mapping reserves for them.")
                          {:error :datomic/eid-outside-mapping :eid k})))
        (+ (long (:base params)) i))
      :else
      (let [k (long k)
            p (part-of k)
            [start ^IRank region] (get regions p)]
        (when-not region
          (throw (ex-info (str "Datomic entity " k " is in partition " p
                               ", where this partition-mapping (built at basis-t "
                               (:basis-t params) ") knows no ids. The mapping does not"
                               " describe the data being imported: rebuild it from the same"
                               " database at the same basis-t.")
                          {:error :datomic/eid-outside-mapping :eid k :partition p
                           :basis-t (:basis-t params)})))
        (let [r (.rank region k)]
          (when (neg? r)
            (throw (ex-info (str "Datomic entity " k " (partition " p ") is not among the ids"
                                 " this partition-mapping was built from at basis-t "
                                 (:basis-t params) ". Every entity and every ref target in"
                                 " history at that basis has one, so the data being imported"
                                 " is not that database at that basis: rebuild the mapping"
                                 " from it.")
                            {:error :datomic/eid-outside-mapping :eid k :partition p
                             :basis-t (:basis-t params)})))
          (+ (long start) r)))))
  Object
  (toString [_] (str "#datahike.migrate.datomic/partition-mapping " (pr-str params))))

(defn mapping-params
  "The serialisable description of a `partition-mapping` — plain EDN: the
   layout, each region's encoding, size, id bounds and a digest of its ids, and
   the `:basis-t` it was built at. `params->mapping` rebuilds the same mapping
   from it and the same Datomic database."
  [^PartitionMapping m]
  (.-params m))

(defn mapping-bytes
  "Heap the mapping's id sets occupy, in bytes (`:bytes` summed over regions)."
  [^PartitionMapping m]
  (transduce (map :bytes) + 0 (:regions (mapping-params m))))

(defn- assemble [params built]
  (->PartitionMapping params
                      (into {} (map (fn [{:keys [partition start region]}]
                                      [partition [start region]]))
                            built)))

(defn- layout
  "Place built regions one after another from `base + reserved`."
  [built base reserved basis-t]
  (let [[regions end] (reduce (fn [[rs start] r]
                                [(conj rs (assoc r :start start))
                                 (+ (long start) (long (:count r)))])
                              [[] (+ (long base) (long reserved))]
                              built)]
    {:built regions
     :params {:base base :reserved reserved :basis-t basis-t :end end
              :regions (mapv #(select-keys % [:partition :kind :min :max :count :start
                                              :bytes :digest])
                             regions)}}))

(defn params->mapping
  "Rebuild a `partition-mapping` from `mapping-params`.

   With only the params this works when every region is a `:range` (nothing to
   rebuild). Otherwise pass the Datomic db or conn it was built from: the id
   sets are re-read from history at the saved `:basis-t`, and every region's
   encoding, bounds, count and digest must match the saved ones, or this
   throws `:datomic/partition-mapping-mismatch` — the database is not the one
   the parameters describe."
  ([params] (params->mapping params nil))
  ([params db-or-conn]
   (validate-params! params)
   (let [{:keys [regions basis-t]} params]
     (if (every? #(= :range (:kind %)) regions)
       (assemble params (map (fn [{:keys [min count] :as r}]
                               (assoc r :region (RangeRegion. min count)))
                             regions))
       (do
         (when-not db-or-conn
           (throw (ex-info (str "These partition-mapping parameters hold :bitmap/:table regions,"
                                " whose id sets are not in the parameters. Pass the Datomic db"
                                " or conn they were built from: (params->mapping params conn).")
                           {:error :datomic/partition-mapping-needs-source})))
         (let [[db] (source-db db-or-conn basis-t)
               rebuilt (into {} (map (juxt :partition identity)) (scan-regions db))
               saved-keys [:kind :min :max :count :digest :bytes]]
           (when-not (= (set (keys rebuilt)) (set (map :partition regions)))
             (throw (ex-info (str "The Datomic database at basis-t " basis-t " has partitions "
                                  (sort (keys rebuilt)) ", the saved mapping "
                                  (map :partition regions) ".")
                             {:error :datomic/partition-mapping-mismatch})))
           (doseq [r regions
                   :let [b (rebuilt (:partition r))]]
             (when-not (= (select-keys r saved-keys) (select-keys b saved-keys))
               (throw (ex-info (str "Partition " (:partition r) " at basis-t " basis-t
                                    " does not hold the ids the saved mapping describes.")
                               {:error :datomic/partition-mapping-mismatch
                                :saved (select-keys r saved-keys)
                                :found (select-keys b saved-keys)}))))
           (assemble params (map #(assoc % :region (:region (rebuilt (:partition %))))
                                 regions))))))))

(defn partition-mapping
  "A COMPUTED `:eids` mapping for `import-from-datomic!`: every Datomic entity
   id packed into a dense datahike id, partition by partition, by RANK — the
   k-th distinct id of a partition becomes its region's start + k. The target
   id space equals the number of distinct ids, however the source interleaved
   its partitions (see the section comment for why `[min max]` regions do not).

   The ids are every entity with a datom in history EAVT and every id a ref
   names in history VAET, partition by partition (the transaction partition is
   the import's own `:tids`). That is one pass over both history indexes: the
   build costs about as much as reading the database once. Each partition's id
   set keeps the smallest of a range, a rank bitmap or a block-varint table;
   `mapping-bytes` reports what it holds. An id not in the set THROWS — it is
   never passed through, because on the index-build path an unmapped id would
   be written as is.

   Opts:
     :base      first target id (default: just above datahike's system
                entities). `import-from-datomic!` builds its default above the
                target's `:max-eid` too.
     :reserved  ids for the source's negative provenance ids (default 16)
     :basis-t   build the mapping as of this `t` (default: the db's basis)

   Refused when the layout reaches `tx0` (536 870 912), where transaction ids
   begin. `mapping-params` / `params->mapping` round-trip it."
  ([db-or-conn] (partition-mapping db-or-conn {}))
  ([db-or-conn {:keys [base reserved basis-t]
                :or {base (inc (long const/ue0)) reserved 16}}]
   (let [[db basis] (source-db db-or-conn basis-t)
         {:keys [built params]} (layout (scan-regions db) base reserved basis)]
     (validate-params! params)
     (log/info :datomic/partition-mapping
               {:basis-t basis :partitions (count built)
                :entities (- (long (:end params)) (long base) (long reserved))
                :bytes (transduce (map :bytes) + 0 built)})
     (assemble params built))))

(defn- check-mapping-covers!
  "Cheap, BEFORE any scan: the source's per-partition id bounds (index seeks)
   must lie inside the mapping's. A mapping built from another database, or one
   that grew, fails here instead of hours into the import."
  [^PartitionMapping pm db]
  (let [{:keys [regions basis-t]} (mapping-params pm)
        by-p (into {} (map (juxt :partition identity)) regions)]
    (doseq [[p [mn mx]] (partition-extents db)
            :let [r (by-p p)]]
      (when-not (and r (<= (long (:min r)) (long mn)) (<= (long mx) (long (:max r))))
        (throw (ex-info (str "The Datomic database's ids in partition " p " span [" mn " " mx "],"
                             " outside the partition-mapping built at basis-t " basis-t
                             (if r (str " ([" (:min r) " " (:max r) "])") " (no such region)")
                             ". It does not describe this database: rebuild it.")
                        {:error :datomic/eid-outside-mapping :partition p
                         :source [mn mx] :mapping (when r [(:min r) (:max r)])}))))))

;; ---------------------------------------------------------------------------
;; Datomic -> records

(defn- ident-map [db]
  (into {} (dt/q '[:find ?e ?i :where [?e :db/ident ?i]] db)))

(defn ident-timeline
  "attribute-entity -> [[dh-t ident] ...] ascending, for the attributes whose
   ident actually CHANGED. `{}` when none did, which is the usual case.

   `ident-map` is a SNAPSHOT of the current database, so resolving a historical
   datom through it names that datom with the attribute's FINAL ident. For an
   attribute that was never renamed those are the same string and nothing is
   lost. For one that was, a datom written before the rename comes out carrying a
   name that did not exist yet — while the `:db/ident` records still replay the
   rename in causal order, so replaying meets the datom before its name is
   installed.

   Datomic keeps the ident assertions in its own history, so this is one query
   over a handful of datoms rather than a pass over the log. `t` is converted to
   datahike's frame with `datomic-t->dh-t`, the same conversion the records get,
   so the timeline and the records agree about when the rename happened.

   Assertions only: a retraction is implied by the assertion that supersedes it."
  [db]
  (let [rows (dt/q (quote [:find ?e ?i ?tx
                           :where [?e :db/ident ?i ?tx true]])
                   (dt/history db))]
    (into (sorted-map)
          (keep (fn [[e entries]]
                  (let [sorted (vec (sort-by first
                                             (map (fn [[_ i tx]]
                                                    [(datomic-t->dh-t (dt/tx->t tx)) i])
                                                  entries)))]
                    ;; only what MOVED — a single-entry timeline says nothing the
                    ;; snapshot does not already say
                    (when (> (count (distinct (map second sorted))) 1)
                      [e sorted]))))
          (group-by first rows))))

(defn- ref-attr-idents [db]
  (into #{} (map first)
        (dt/q '[:find ?i :where [?a :db/valueType :db.type/ref] [?a :db/ident ?i]] db)))

(defn- tx->records
  "One Datomic log entry -> datahike records, in `(t, txInstant-first, e, a, op)` order.

   `a` becomes a keyword ident.

   A ref value is turned into a keyword ONLY when the attribute is one of
   `schema-defining-attrs` — `:db/valueType` -> `:db.type/string`,
   `:db/cardinality` -> `:db.cardinality/one`. Those name Datomic's own
   vocabulary, datahike does not treat them as refs, and a raw eid there would
   dangle.

   Every OTHER ref value stays numeric and is remapped by datahike's importer
   along with `e`, INCLUDING one that points at an entity carrying a
   `:db/ident`. That is the enum case, and keying on \"the target has an ident\"
   instead of \"the attribute is schema\" broke it: `:p/color` -> `:color/red`
   became the keyword, `ids/apply-mapping` guards ref remapping with
   `(number? v)` so the keyword passed straight through, and
   `transact-entities-directly` then allocated a fresh eid for it. Measured:
   every enum reference pointed at a phantom entity with no datoms, a join
   through it returned `#{}`, and the import reported success. The same dump
   exported back to Datomic then aborted with `:db.error/tempid-not-an-entity`,
   because the sink minted a tempid for an eid that had no datoms to assert.

   An id in the transaction partition is rewritten to that transaction's mapped
   `t`, in `e` and in a ref value alike: the `:db/txInstant` datom names its own
   transaction, which is the shape datahike expects and `export-db` writes; a
   fact written later about a past transaction lands on that transaction; and a
   ref to a transaction names it. The importer then maps those through its
   `:tids` like any other transaction id."
  [{:keys [t data]} idents refs dropped {:keys [provenance? extra renamed-attrs]}]
  (let [dh-t   (datomic-t->dh-t t)
        tx-eid (dt/t->tx t)
        ;; tx-META: `e` equal to `t` makes these datoms ABOUT the transaction.
        ;; They are what survives the id remapping — see the ns docstring.
        prov   (when provenance?
                 [[dh-t :datomic/t t dh-t true]
                  [dh-t :datomic/tx-eid tx-eid dh-t true]])]
    (->> (concat data)
         (keep (fn [^Datom dm]
                 (let [a (idents (.a dm))]
                   (cond
                     (install-attrs a) nil
                     (datomic-only-attrs a) (do (vswap! dropped conj a) nil)
                     (nil? a) (do (vswap! dropped conj (.a dm)) nil)
                     :else
                     (let [v (.v dm)
                           v (cond
                               ;; a SCHEMA ref -> the ident it names. Keyed on
                               ;; the attribute, not on "the target happens to
                               ;; have an ident" — see the docstring.
                               (and (schema-defining-attrs a) (refs a) (idents v))
                               (idents v)
                               ;; a ref to a TRANSACTION. Its target is a tx
                               ;; entity, whose `e` is rewritten to `dh-t` below;
                               ;; the ref has to follow, or it is remapped as an
                               ;; ordinary entity id and names a phantom entity
                               ;; (streaming) or one outside every region
                               ;; (`partition-mapping`).
                               (and (refs a) (tx-eid? v))
                               (datomic-t->dh-t (dt/tx->t v))
                               ;; :db.type/uri has no datahike type; carry the string
                               (instance? java.net.URI v) (str v)
                               :else v)
                           e (.e dm)]
                       ;; A RENAMED attribute goes on the wire by ENTITY, so no
                       ;; naming decision is baked into the record and the READER
                       ;; picks what its target needs — period-correct for an
                       ;; attribute-refs target, flattened for a keyword one. See
                       ;; `ident-timeline` on why the snapshot cannot name these.
                       ;; Every other attribute keeps its ident, so a source with
                       ;; no rename emits exactly what it emitted before.
                       ;; The tx partition is transactions, all of them: the
                       ;; current one's `e` and also a fact written LATER about
                       ;; a past one (`[:db/add old-tx :tx/note "x"]`), which
                       ;; has to land on that transaction's entity, not on a new
                       ;; one (streaming) or outside the mapping (bulk).
                       [(if (tx-eid? e) (datomic-t->dh-t (dt/tx->t e)) e)
                        (if (contains? renamed-attrs (.a dm))
                          (mcbor/->AttrRef (.a dm))
                          a)
                        v dh-t (.added dm)])))))
         ;; `extra` is the provenance SCHEMA, emitted once with the first
         ;; transaction — a source owes its own schema before the data using it.
         (concat prov (map (fn [r] (assoc r 3 dh-t)) extra))
         ;; :db/txInstant first within the transaction, then by (e, a, op) — the
         ;; order `migrate.sort/sort-key` documents and the importer assumes.
         ;;
         ;; `op` LAST but present, retraction before assertion, and it is not
         ;; decoration: for an ident RENAME the retraction and the assertion of
         ;; :db/ident share [1, e, ":db/ident"] exactly, so without `op` in the key
         ;; `sort-by`'s stability hands their order to whatever `d/tx-range`
         ;; happened to return. Datomic returns them ASSERTION-first, which lands
         ;; a second :db/ident on an entity that still holds its old one — the case
         ;; `update-schema` used to throw a ClassCastException on, six hours into a
         ;; 393M-datom import. `migrate.sort/sort-key` has carried `op` for this
         ;; reason all along; this sort claimed to implement that order and did not.
         (sort-by (fn [[e a _ _ op]] [(if (= a :db/txInstant) 0 1) e (str a) (if op 1 0)]))
         vec)))

(defn attribute-schema
  "`{ident {:db/valueType … :db/cardinality … [tuple keys]}}` for every
   attribute of `db`: the ident-keyed schema the index-build path reads to tell
   a ref VALUE from a number. Keyed on the CURRENT ident, which is what a
   renamed attribute's datoms are resolved to before they are read.

   A tuple attribute also carries its `:db/tupleAttrs`, `:db/tupleTypes` or
   `:db/tupleType`, so an importer that remaps ref ELEMENTS of tuples (a ref
   component of a composite, a `:db.type/ref` slot of a heterogeneous tuple)
   can see which slots hold ids."
  [db]
  (into {}
        (map (fn [[a i vt c]]
               (let [tuple (when (= :db.type/tuple vt)
                             (-> (dt/pull db [:db/tupleAttrs :db/tupleTypes :db/tupleType] a)
                                 (select-keys [:db/tupleAttrs :db/tupleTypes :db/tupleType])))]
                 [i (merge {:db/valueType vt :db/cardinality c}
                           (update-vals tuple (fn [x] (if (sequential? x) (vec x) x))))])))
        (dt/q '[:find ?a ?i ?vt ?c
                :where [?a :db/ident ?i] [?a :db/valueType ?v] [?v :db/ident ?vt]
                [?a :db/cardinality ?cc] [?cc :db/ident ?c]]
              db)))

(defn- provenance-schema
  "Schema datoms for the two provenance attributes, at eids that cannot collide.

   NEGATIVE ids, because they only have to be DISTINCT — datahike reallocates
   them like every other source eid — and no Datomic eid is ever negative, so
   this is collision-free by construction rather than by assumption.

   It used to be `(inc (max ident-eid))`, on the reasoning that \"attribute
   entities live in the low hundreds and user entities start around 1.76e13, so
   one past the highest ident eid is free\". That premise is false for any
   database where a USER entity carries a `:db/ident` — a Datomic enum
   (`{:db/ident :color/red}`) is the common case, and the singleton/config
   idiom is another. `ident-map` queries `[?e :db/ident ?i]`, i.e. EVERY ident,
   so one such entity puts `base` inside the occupied user partition.

   Reproduced: with an enum at 17592186045418, `:datomic/tx-eid` was allocated
   17592186045420 — exactly the eid Datomic then handed the next user entity.
   The import reported `:verified? true` over a single entity that was
   simultaneously an attribute definition and a person, and `d/pull` on that
   person returned the attribute's schema. Silent, and it compounds: the merged
   entity now holds both a schema datom and a data datom, which is precisely the
   shape that splits into two entities on the way back out."
  []
  (into []
        (mapcat (fn [[i a vt]]
                  [[i :db/ident a 0 true]
                   [i :db/valueType vt 0 true]
                   [i :db/cardinality :db.cardinality/one 0 true]]))
        [[-1 :datomic/t      :db.type/long]
         [-2 :datomic/tx-eid :db.type/long]]))

(defn- log-t-range
  "The `[from to]` of the transaction log, as Datomic's half-open `tx-range` bounds.

   `to` comes from `dt/basis-t`, which is O(1). The obvious spelling —

       (let [txs (dt/tx-range (dt/log conn) nil nil)]
         [(:t (first txs)) (inc (:t (last txs)))])

   — is a two-fold mistake: it SCANS the whole log to learn its last `t`, and
   because `txs` is bound to a name while `last` walks it, the head is retained
   and the entire log is held in heap. That is the same defect the export path
   carries a `WeakReference` test against; it is easy to write by accident, which
   is why it is spelled out here.

   `first` is still a read, but a lazy seq's first chunk only — nothing is bound,
   so nothing is retained past the call."
  [conn basis-t]
  (let [to (or basis-t (dt/basis-t (dt/db conn)))]
    (when-let [f (:t (first (dt/tx-range (dt/log conn) nil nil)))]
      (when (<= (long f) (long to))
        [f (inc (long to))]))))

(defn source
  "A `chunk-src` over a Datomic connection's transaction log, for `import-source`.

   Descriptors are `t` WINDOWS, not records — `{:from t :to t}` — so the whole
   log is never held. `:read` fetches one window with `dt/tx-range` and converts
   it, which is the IO the seam's `:read` exists to permit.

   Opts:

     :window        transactions per chunk (default 100). A chunk is whole
                    transactions by construction, so tx-alignment is free here.
     :provenance?   (default true) stamp each transaction with `:datomic/t` and
                    `:datomic/tx-eid`, and emit their schema in the first
                    transaction. See the ns docstring.
     :basis-t       read the log up to and including this `t` (default: the
                    database's basis when `source` is called), with idents
                    and schema as of it. `import-from-datomic!` pins it, so
                    the log it imports is the one its mapping and count saw.

   The ident map and ref-attribute set are read ONCE, from the current db, so an
   attribute installed mid-log is still resolvable when its earlier datoms are
   read.

   `:read` is RE-ENTRANT, as the seam requires: the provenance schema is keyed to
   the first `t` of the whole log, not to \"the first chunk read\", so reading the
   same window twice yields the same records — `verify` and the index build both
   read chunks more than once."
  ([conn] (source conn {}))
  ([conn {:keys [window provenance? basis-t] :or {window 100 provenance? true}}]
   (when-not (pos? window)
     (throw (ex-info "Datomic source :window must be positive."
                     {:error :datomic/bad-window :window window})))
   (let [[db basis] (source-db conn basis-t)
         idents  (ident-map db)
         refs    (ref-attr-idents db)
         ;; The attributes the SNAPSHOT above cannot name correctly. Read once,
         ;; like the snapshot, and exposed on the chunk-src so `import-source`
         ;; can be handed it as `:source-meta`.
         timeline (ident-timeline db)
         renamed  (not-empty (set (keys timeline)))
         dropped (volatile! #{})
         [from to] (or (log-t-range conn basis) [0 0])
         schema  (when provenance? (provenance-schema))
         chunks  (vec (for [start (range from to window)]
                        {:from start :to (min to (+ start window))}))]
     {:chunks chunks
      ;; NOT part of the `chunk-src` contract `import-source` reads — that is
      ;; `:chunks` and `:read`. Carried alongside so `import-from-datomic!` can
      ;; pass it as `:source-meta`, which is where a source's format-level facts
      ;; belong; a caller driving `source` directly can do the same.
      :ident-timeline timeline
      :read   (fn [{f :from t :to} _opts]
                (let [rs (into []
                               (mapcat (fn [{:keys [t] :as tx}]
                                         (tx->records tx idents refs dropped
                                                      {:provenance? provenance?
                                                       :renamed-attrs renamed
                                                       ;; only with the log's FIRST
                                                       ;; transaction, whichever chunk
                                                       ;; that turns out to be in
                                                       :extra (when (= t from) schema)})))
                               (dt/tx-range (dt/log conn) f t))]
                  (when (seq @dropped)
                    (log/warn :datomic/dropped-attributes
                              {:msg "Datomic vocabulary with no datahike equivalent was dropped"
                               :attributes @dropped}))
                  rs))})))

(defn record-count
  "How many records `source` will yield. A FULL log scan — the cost is real on a
   large database, and it exists because `import-source` fails closed: without an
   `:expected-count` the import must be told `{:verify? false}` explicitly rather
   than be quietly unverified."
  ([conn] (record-count conn {}))
  ([conn opts]
   (let [{:keys [chunks read]} (source conn opts)]
     (transduce (map #(count (read % {}))) + 0 chunks))))

(defn import-from-datomic!
  "Import a Datomic database into datahike `conn`. **Experimental.**

   Entity and transaction ids are remapped; transaction times and ordering are
   preserved (see the ns docstring). `:eids :preserve` is refused — Datomic ids
   do not fit datahike's id space.

   Opts are `import-source`'s, plus `:window` and `:basis-t` (import the source
   as of that `t`; default: its basis when the call starts — the log, the
   count and the mapping all read that one basis, so a source that keeps
   taking writes is imported as of the start). `:count?` (default true) does
   the extra log scan that makes verification possible; `{:count? false}` skips
   it and then requires `{:verify? false}`, so an unverified import is always
   something the caller asked for.

   `{:build-indexes? true}` builds the database from sorted input instead of
   replaying it (see `datahike.migrate/import-db`). What that path needs from a
   Datomic source is supplied here:

     :schema   derived from the Datomic database (`attribute-schema`), so ref
               values are remapped; pass your own to override. An explicit
               `nil` is refused: it would turn ref remapping off.
     :eids     defaults to `(partition-mapping dtm-conn)` built at the import's
               basis — exact, one target id per source id, with no per-entity
               map. A caller mapping must be TOTAL: on this path an id it does
               not name is written as is.

   Transaction ids: with a `partition-mapping` (the bulk default) or any other
   caller mapping on the bulk path they are datahike's `tx0 + t` (the source's
   `t`, offset); on the streaming path, and on the bulk path with
   `{:eids :allocate}`, they are sequential. `:datomic/t` records the
   correspondence either way.

   A `partition-mapping` passed in is checked BEFORE anything is scanned or
   written: its `:basis-t` must be the import's, its id bounds must cover the
   source's, and its `:base` must lie above the target's `:max-eid`."
  ([dh-conn dtm-conn] (import-from-datomic! dh-conn dtm-conn {}))
  ([dh-conn dtm-conn {:keys [count?] :or {count? true} :as opts}]
   (when (= :preserve (:eids opts))
     (throw (ex-info (str "{:eids :preserve} cannot be honoured for a Datomic source: Datomic entity "
                          "ids (~1.76e13) exceed datahike's emax (" const/emax "). datahike does not "
                          "range-check them, it silently reallocates, so preserving them is not "
                          "merely unsupported but unobservable. Use the default :allocate.")
                     {:error :datomic/eids-preserve-unsupported :emax const/emax})))
   (let [bulk?     (:build-indexes? opts)
         _         (when (and bulk? (contains? opts :schema) (nil? (:schema opts)))
                     (throw (ex-info (str "{:schema nil} with :build-indexes? would import every ref"
                                          " value as a raw Datomic id: the index-build path tells a"
                                          " ref from a number by this schema. Leave :schema out to"
                                          " derive it from the Datomic database.")
                                     {:error :datomic/schema-nil})))
         supplied  (when (instance? PartitionMapping (:eids opts)) (:eids opts))
         saved-t   (some-> supplied mapping-params :basis-t long)
         [db basis] (source-db dtm-conn (:basis-t opts))
         _         (when (and supplied (not= saved-t basis))
                     (throw (ex-info (str "This partition-mapping was built at basis-t " saved-t
                                          ", but the import reads the source at basis-t " basis
                                          ". A mapping knows only the ids that existed when it was"
                                          " built: rebuild it at " basis ", or import as of the"
                                          " mapping with {:basis-t " saved-t "}.")
                                     {:error :datomic/partition-mapping-stale
                                      :mapping-basis-t saved-t :source-basis-t basis})))
         floor     (inc (long (:max-eid @dh-conn)))
         pm        (or supplied
                       (when (and bulk? (nil? (:eids opts)))
                         (partition-mapping db {:basis-t basis
                                                :base (max (inc (long const/ue0)) floor)})))
         _         (when pm
                     (let [base (long (:base (mapping-params pm)))]
                       (when (< base floor)
                         (throw (ex-info (str "This partition-mapping starts at " base
                                              ", inside ids the target already uses (up to "
                                              (dec floor) "). Build it with {:base " floor
                                              "} or more.")
                                         {:error :datomic/partition-mapping-overlaps-target
                                          :base base :target-max-eid (dec floor)}))))
                     (when supplied (check-mapping-covers! pm db)))
         opts      (cond-> (assoc opts :basis-t basis)
                     (and bulk? (not (contains? opts :schema)))
                     (assoc :schema (attribute-schema db))
                     pm (assoc :eids pm))
         src       (source dtm-conn opts)
         n         (when count? (record-count dtm-conn opts))]
     (m/import-source dh-conn src
                      (merge (dissoc opts :count? :window :provenance? :basis-t)
                             ;; NO `:max-tx`. It is a drift CHECK — "the restore
                             ;; should land on the same max-tx the source had" —
                             ;; and that is meaningful only where `t` is
                             ;; preserved, which for a Datomic source it never is
                             ;; (datahike compacts; see the ns docstring).
                             ;; Declaring it made every successful import print
                             ;; `max-tx drifted by -3998`, a warning about
                             ;; working as designed. Measured on 3000
                             ;; transactions. The `:datomic/t` provenance datoms
                             ;; carry the real correspondence.
                             {:source-meta (cond-> {:history? true}
                                             n (assoc :expected-count n)
                                             (seq (:ident-timeline src))
                                             (assoc :ident-timeline
                                                    (:ident-timeline src)))})))))

;; ---------------------------------------------------------------------------
;; records -> Datomic

(defn- sink-tx-data
  "One transaction's records -> Datomic tx-data.

   Source eids become tempids on first assertion and resolved ids thereafter, so
   a RETRACTION — which cannot name a tempid — resolves. The tx entity becomes
   \"datomic.tx\", which is how `:db/txInstant` is carried across.

   A retraction whose `[e a]` is also ASSERTED in the same transaction is
   dropped FOR A CARDINALITY-ONE ATTRIBUTE ONLY: Datomic derives that retraction
   itself, so replaying ours too records it twice. Measured against Datomic:
   `[[:db/add e :p/age 31]]` alone yields tx-data carrying both the assertion and
   `[e :p/age 30 false]`, and sending our own retraction as well records it
   twice.

   For CARDINALITY-MANY the same reasoning is false and the filter was silently
   wrong: a retraction of v1 and an assertion of v2 are independent facts, and
   Datomic derives nothing — measured, `[[:db/add e :p/tag \"c\"]]` yields only
   the assertion. Dropping ours left the retracted value ALIVE in the target.
   Measured end to end: source tags `#{y z}`, target `#{x y z}`.

   `many-idents` is learned from the stream, like `ref-idents`. An attribute of
   UNKNOWN cardinality keeps its retraction — the safe direction, since a
   duplicate retraction is a log-fidelity blemish while a dropped one is wrong
   data."
  [group t eids ref-idents many-idents strip?]
  (let [superseded (into #{} (keep (fn [[e a _ _ op]]
                                     (when (and op (not (many-idents a))) [e a])))
                         group)
        ref->      (fn [e] (if (= e t) "datomic.tx" (or (@eids e) (str "e" e))))]
    (into []
          (comp
           (remove (fn [[e a _ _ op]] (and (not op) (superseded [e a]))))
           ;; datahike-only vocabulary Datomic answers with
           ;; `:db.error/not-an-entity`, which would fail the whole transaction.
           (remove (fn [[_ a _ _ _]] (and strip? (datahike-only-attrs a))))
           ;; A datahike schema entity re-asserting a Datomic BOOT attribute's own
           ;; definition — `[:db/index :db/valueType …]` — raises
           ;; `:db.error/datom-cannot-be-altered`. Only the definition OF a boot
           ;; attribute is a problem; USING one (`[:p/name :db/index true]`) is
           ;; ordinary schema and must pass, so this keys on the entity being a
           ;; boot attribute, not on the attribute of the record.
           (remove (fn [[_ a v _ _]]
                     (and strip? (= a :db/ident) (datomic-boot-attrs v))))
           (map (fn [[e a v _ op]]
                  [(if op :db/add :db/retract)
                   (ref-> e)
                   a
                   (if (and (ref-idents a) (number? v)) (ref-> v) v)])))
          group)))

(defn sink
  "A `sink` over a Datomic connection, for `export-to-sink`.

   One Datomic transaction per source transaction, so `:db/txInstant` and the
   causal order carry across. `export-to-sink` hands whole transactions —
   chunks are transaction-aligned — which is exactly what this needs.

   Opts:

     :strip-datahike-schema?  (default true) drop datahike-only vocabulary that
                              Datomic refuses with `:db.error/not-an-entity`.

   Returns `{:transactions n :eids {source-eid datomic-eid}}` from `:close`."
  ([conn] (sink conn {}))
  ([conn {:keys [strip-datahike-schema?] :or {strip-datahike-schema? true}}]
   {:open  (fn [_opts] {:eids (atom {}) :refs (atom #{}) :idents (atom {})
                        :many (atom #{}) :n 0})
    :write (fn [{:keys [eids refs idents many] :as ctx} records]
             ;; learn the schema from the stream, as the dump's own importer does
             (doseq [[e a v _ op] records]
               (when (and op (= a :db/ident))     (swap! idents assoc e v))
               (when (and op (= a :db/valueType) (= v :db.type/ref)) (swap! refs conj e))
               ;; cardinality, for the superseded filter. Learned from the STREAM
               ;; rather than looked up in the target: `sink-tx-data` runs before
               ;; either half is committed, so a target lookup misses on exactly
               ;; the transaction that installs a card-many attribute and writes
               ;; to it — the one where it matters most.
               (when (and op (= a :db/cardinality) (= v :db.cardinality/many))
                 (swap! many conj e)))
             (let [ref-idents  (into #{} (keep @idents) @refs)
                   many-idents (into #{} (keep @idents) @many)]
               (reduce
                (fn [c group]
                  (let [t  (nth (first group) 3)
                        td (sink-tx-data group t eids ref-idents many-idents
                                         strip-datahike-schema?)
                        ;; DATOMIC REQUIRES SCHEMA IN AN EARLIER TRANSACTION than
                        ;; its first use; datahike accepts an attribute and the
                        ;; datom using it in ONE transaction, and `export-db`
                        ;; therefore emits them together. Measured: replaying such
                        ;; a transaction verbatim raises
                        ;; `:db.error/not-an-entity Unable to resolve entity: :datomic/t`.
                        ;;
                        ;; So a source transaction that installs schema becomes TWO
                        ;; Datomic transactions, schema first. The extra one carries
                        ;; no `:db/txInstant` of its own, so it is stamped by
                        ;; Datomic's clock rather than the source's — the schema's
                        ;; installation time is not preserved, its content is.
                        schema? (fn [[_ _ a]] (contains? schema-defining-attrs a))
                        [sch dat] [(filterv schema? td) (filterv (complement schema?) td)]
                        ;; Split only when this transaction actually USES an
                        ;; attribute it INSTALLS. Splitting on the mere presence
                        ;; of schema is stricter than the rule above, and the
                        ;; difference is not free: a Datomic SOURCE cannot
                        ;; produce a transaction that carries both, so every one
                        ;; of its schema transactions was being split for
                        ;; nothing. Measured on a source with two schema
                        ;; transactions: 4 came back as 6, of which one extra
                        ;; was real and one was this.
                        ;;
                        ;; The real one is datahike's own doing, not the source's
                        ;; — `source` emits the provenance schema with the log's
                        ;; FIRST transaction (`:extra (when (= t from) schema)`)
                        ;; and stamps `:datomic/t` on that same transaction.
                        ;; datahike accepts that; Datomic does not, which is the
                        ;; `:db.error/not-an-entity :datomic/t` the split was
                        ;; added for. That case still splits, because `:datomic/t`
                        ;; is installed and used here.
                        installed (into #{} (keep (fn [[_ _ a v]]
                                                    (when (= a :db/ident) v))
                                                  sch))
                        needs-split? (and (seq sch)
                                          (boolean (some (fn [[_ _ a]]
                                                           (contains? installed a))
                                                         dat)))
                        [sch dat] (if needs-split? [sch dat] [[] td])
                        ;; The split transaction needs the SOURCE's time too.
                        ;; Without it Datomic stamps the schema transaction with
                        ;; the wall clock, which advances the basis past every
                        ;; historical instant that follows — measured:
                        ;; `:db.error/past-tx-instant … 2021-01-01 is older than
                        ;; database basis`, on a FRESH database, caused entirely by
                        ;; the split.
                        ;;
                        ;; The SAME instant, not one millisecond earlier. Datomic
                        ;; accepts an equal `:db/txInstant` and rejects an earlier
                        ;; one (measured both ways), and ordering between the two
                        ;; halves is carried by `t`, not by the instant. The `dec`
                        ;; this replaces made the schema half's time approximate
                        ;; for no benefit, and was reachable as a FAILURE: two
                        ;; source transactions sharing a millisecond put the
                        ;; synthetic instant before the previous transaction, and
                        ;; the export aborted mid-way with `:db.error/past-tx-instant`
                        ;; leaving the target half-migrated. datahike's own
                        ;; allocator is strictly monotonic, but caller-supplied
                        ;; `:db/txInstant` in `:tx-meta` overrides it, and an
                        ;; imported database inherits whatever ties its source had.
                        inst (some (fn [[_ _ a v]] (when (= a :db/txInstant) v)) dat)
                        sch  (cond-> sch
                               (and (seq sch) inst)
                               (conj [:db/add "datomic.tx" :db/txInstant inst]))
                        commit! (fn [c' d]
                                  (if (empty? d)
                                    c'
                                    (let [{:keys [tempids]} @(dt/transact conn d)]
                                      (doseq [[k id] tempids]
                                        (when (string? k)
                                          (if (= k "datomic.tx")
                                            (swap! eids assoc t id)
                                            (swap! eids assoc (parse-long (subs k 1)) id))))
                                      (update c' :n inc))))
                        ;; TEMPIDS DO NOT SPAN TRANSACTIONS. `td` was built once,
                        ;; before the split, so both halves carry the same tempid
                        ;; STRING for a source eid — and Datomic resolves the same
                        ;; string in two transactions to two DIFFERENT entities.
                        ;; An entity with a schema datom and a data datom in one
                        ;; source transaction therefore became two entities, with
                        ;; the ident on one and the data on the other, silently.
                        ;; Measured: `{:db/ident :color/red :p/name "Red"}` landed
                        ;; as two entities and `(:p/name (d/entity db :color/red))`
                        ;; was nil.
                        ;;
                        ;; `commit! sch` fills `eids` for everything the schema
                        ;; half created, so the data half is re-resolved against it
                        ;; here — after that commit, not before.
                        resolve-tempid (fn [x]
                                         (if (and (string? x) (not= x "datomic.tx"))
                                           (or (@eids (parse-long (subs x 1))) x)
                                           x))
                        rebuild (fn [d]
                                  (mapv (fn [[op e a v]]
                                          [op (resolve-tempid e) a (resolve-tempid v)])
                                        d))]
                    (let [c (commit! c sch)]
                      ;; rebuilt only when a schema half actually ran; otherwise
                      ;; `dat` is `td` untouched and there is nothing to resolve.
                      (commit! c (if (seq sch) (rebuild dat) dat)))))
                ctx
                (partition-by #(nth % 3) records))))
    :close (fn [{:keys [n eids]}] {:transactions n :eids @eids})}))

(defn export-to-datomic!
  "Export a datahike database into a Datomic connection. **Experimental.**

   `db-or-conn` is a datahike db or connection; `dtm-conn` a Datomic connection,
   which should be FRESH — see the ns docstring on `:db/txInstant` monotonicity
   and on unique-identity upsert into a populated database.

   Opts are `export-to-sink`'s (`:history?` `:chunk-size` `:xform` …) plus
   `:strip-datahike-schema?`."
  ([db-or-conn dtm-conn] (export-to-datomic! db-or-conn dtm-conn {}))
  ([db-or-conn dtm-conn opts]
   (m/export-to-sink db-or-conn
                     (sink dtm-conn (select-keys opts [:strip-datahike-schema?]))
                     (dissoc opts :strip-datahike-schema?))))
