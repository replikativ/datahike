(ns datahike.schema-cache
  "Two caches over `schema-meta` — the out-of-line `{:schema :rschema
   :system-entities :ident-ref-map :ref-ident-map}` blob every commit record
   names through `:schema-meta-key`.

   READ side (`schema-meta-cache`): a plain memoization. The key is
   `(uuid schema-meta)`, so it is content-addressed and the same key always
   denotes the same bytes — sharing one cache across every store in the process
   is sound, and a stale entry is impossible by construction.

   WRITE side (`schema-meta-durable?` / `mark-schema-meta-durable!`): NOT a
   cache. It records PROOF that a given key is durable in a given store, so a
   commit whose schema has not changed can skip re-writing a blob that is
   already there. The distinction matters because a wrong answer here is not a
   slow read, it is a commit that names a key nothing ever wrote — a database
   that opens fine until the process restarts and then has no schema at all.

   A proof is only ever established by a write this process issued AND awaited.
   It is scoped to the
   STORE OBJECT rather than to a store id, so two stores that happen to share an
   `:id` cannot borrow each other's proof, and it dies with the store. See
   `datahike.store/add-cache-and-handlers` for where the cell is attached and
   `datahike.writing/db->stored*` for the one consumer."
  (:require #?(:clj [clojure.core.cache.wrapped :as cw]
               :cljs [cljs.cache.wrapped :as cw])
            [datahike.config :as dc]
            [konserve.utils :as ku])
  #?(:clj (:import [java.util Date])))

;; Shared schema read cache across all stores
(def schema-meta-cache (cw/lru-cache-factory {} :threshold dc/*schema-meta-cache-size*))

(defn cache-has? [schema-meta-key]
  (cw/has? schema-meta-cache schema-meta-key))

(defn cache-lookup [schema-meta-key]
  (cw/lookup schema-meta-cache schema-meta-key))

(defn cache-miss [schema-meta-key schema-meta]
  (cw/miss schema-meta-cache schema-meta-key schema-meta))

;; =============================================================================
;; Durability proof
;; =============================================================================

(def durable-cell-key
  "Where the proof cell hangs on the konserve store map."
  :datahike/schema-meta-durable)

(defn new-durable-cell
  "The per-store proof cell: an atom of `{:key <schema-meta-key> :at <Date>}`,
   or nil.

   ONE entry, because one is all that is ever asked for. `db->stored` asks only
   about the db it is committing, whose schema is this connection's head — and a
   store object belongs to one connection (sibling branches each build their
   own). A schema change replaces the key rather than adding to it."
  []
  (atom nil))

(defn- get-time
  "Reader-conditional `.getTime`; mirrors `datahike.gc/get-time` — metadata does
   not survive macro expansion, so the hint has to sit at the call site."
  [d]
  #?(:clj (.getTime ^Date d) :cljs (.getTime d)))

(defn- proven-at [store schema-meta-key]
  (when-let [cell (get store durable-cell-key)]
    (let [proven @cell]
      (when (= schema-meta-key (:key proven))
        (:at proven)))))

(def ^:dynamic *shared-re-assert-ms*
  "How long proof of a write survives when another process may be collecting.

   The same number, for the same reason, as
   `datahike.gc/DEFAULT_SHARED_SWEEP_MIN_AGE_MS`: it bounds the window in which
   a sweep running elsewhere can be blind to what this process just wrote. Held
   separately rather than required, because `datahike.gc` depends on THIS
   namespace — and because the two are separate policies that happen to share a
   bound, not one policy read twice. If you change one, look at the other.

   Dynamic so tests can drive the re-assert path without waiting fifteen
   minutes."
  (* 15 60 1000))

(defn re-assert-after-ms
  "How long a durability proof stays good before a commit re-asserts it by
   writing the blob again.

   Under a LOCAL EXCLUSIVE writer the proof never expires. The only thing that
   deletes store keys is collection, `gc-storage!`'s mark unions every branch
   head's `:schema-meta-key` (so our own sweep provably spares the key we hold a
   proof for), and `gc-storage!` drops the proof anyway. Nothing else can remove
   the blob without violating the ownership statement, so re-asserting would be
   pure write amplification.

   Otherwise — `:shared` ownership (the default) or a remote writer backend —
   another process may collect, and a sweep whose mark ran before our head flip
   is blind to what we just wrote. That is the same exposure
   `datahike.gc/DEFAULT_SHARED_SWEEP_MIN_AGE_MS` exists to bound, so the proof
   expires on the same clock: after it, the next commit writes the blob again.
   Because the key is content-addressed, that single write retroactively repairs
   EVERY commit that named it, which is what makes the damage self-healing
   rather than permanent.

   Returns nil for \"never expires\"."
  [config]
  (when-not (dc/local-exclusive-writer? config)
    *shared-re-assert-ms*))

(defn schema-meta-durable?
  "Do we have unexpired PROOF that `schema-meta-key` is durable in `store`?

   False is always safe: it costs one idempotent re-write of a content-addressed
   blob. True must never be a guess — see the namespace docstring."
  [store schema-meta-key re-assert-ms]
  (boolean
   (when-let [at (proven-at store schema-meta-key)]
     (or (nil? re-assert-ms)
         (< (- (get-time (ku/now)) (get-time at)) (long re-assert-ms))))))

(defn mark-schema-meta-durable!
  "Record proof that `schema-meta-key` is durable in `store`.

   A proof means exactly one thing: THIS process wrote that blob and the write
   completed. Callers must only reach this after an awaited write — marking on
   an issued-but-not-awaited write is the bug this whole mechanism replaces.

   A READ that reached the store would be evidence too, and an earlier version
   recorded one. It was dropped: `stored->db` reads historical commits as
   readily as the head, and a proof for a historical schema is never asked
   about, so the only thing it bought was an unbounded cell holding keys nobody
   queries. What it saved was one blob write per CONNECTION (the first commit
   after connect, which is now unproven), not per commit."
  [store schema-meta-key]
  (when-let [cell (get store durable-cell-key)]
    (reset! cell {:key schema-meta-key :at (ku/now)})
    nil))

(defn forget-schema-meta-durable!
  "Drop every proof held for `store`.

   Called where a proof could have been invalidated behind our back: after a
   collection (`datahike.gc/gc-storage!`) and before a head rewind
   (`datahike.versioning/force-branch!`, which can orphan the key the previous
   head named). The next commit then re-establishes it with a real write."
  [store]
  (when-let [cell (get store durable-cell-key)]
    (reset! cell nil)
    nil))
