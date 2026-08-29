(ns datahike.gc-guard
  "The store's SAFE POINT: the instant before which every written object is
   either reachable from a pointer, or garbage.

   WHY THIS EXISTS. Datahike's crash-safety rests on one rule: write every value
   the new state references, and only THEN write the mutable pointer that makes
   it reachable (index nodes before the branch head; see
   `datahike.writing/commit!`). A torn write therefore leaves collectable
   orphans, never a dangling pointer.

   That rule is also a BLIND SPOT for the garbage collector. For the duration of
   such a sequence, freshly written objects sit in the store reachable from
   NOTHING — the pointer still names the previous state. A mark that runs inside
   the window classifies them as garbage, and a sweep deletes them; the pointer
   then lands on deleted objects and the store is corrupt. The objects are not
   `new' by timestamp either — they were written BEFORE the collection started,
   so a cutoff of `now` does not spare them.

   The blind spot belongs to the STORE, not to the writer: `commit!` runs in the
   writer's commit loop, but `datahike.versioning/branch!` and Konserve-backed
   secondary adapters perform the same values-then-pointer sequence elsewhere.
   Konserve therefore owns the canonical per-store registry. This namespace is
   Datahike's compatibility facade over that registry, and every such sequence
   takes the same fence.

   USAGE — wrap the whole values-then-pointer sequence:

     (let [t (writing! store-id)]
       (try (write-values!) (write-pointer!)
            (finally (done! store-id t))))

   or `with-unreferenced-writes`, which does the same.

   A process that dies mid-sequence simply drops its entry: the objects it wrote
   are unreachable, i.e. garbage, and a later cycle collects them. Correct by
   construction.

   SCOPE: Konserve's registry is in-process state. It is exact for every writer
   using the store in THIS process and knows nothing about any other. Head
   fencing (#963) lets writers in several processes share a branch without
   losing each other's commits, but a fence protects the POINTER, not the
   values: a commit in another process is invisible here, its objects are on
   disk reachable from nothing, and this guard cannot spare them. Across
   processes the only protection is `gc-storage!`'s `:min-age-ms` floor (or an
   external collector/writer coordinator) — see its docstring for how that is
   sized. Readers are unconstrained."
  (:require [konserve.gc-guard :as guard])
  ;; Self-require the macro namespace so `with-unreferenced-writes` is available to
  ;; ClojureScript consumers that `:refer` it (the `datahike.test.async` pattern).
  #?(:cljs (:require-macros [datahike.gc-guard])))

;; Konserve owns the one canonical per-store guard registry. Every operation in
;; this namespace delegates to it, so Scriptum and every other Konserve-backed
;; index shares the same safe point as primary Datahike writes and collection. A
;; future compactor must consume this same fence rather than introduce another.

(defn writing!
  "Open an unreferenced-write sequence on `store-id`. Returns a token to close it
   with. Call BEFORE the first value is written."
  [store-id]
  (guard/writing! store-id))

(defn done!
  "Close the sequence — its pointer has landed, so everything it wrote is now
   reachable (or garbage, if the pointer superseded it)."
  [store-id token]
  (guard/done! store-id token))

(defn in-flight?
  "Is an unreferenced-write sequence currently open on `store-id`?

   `safe-point` cannot answer this: it returns `now` both when nothing is in
   flight AND when a sequence opened within the same millisecond, so a test that
   compares timestamps cannot tell a held guard from a missing one. This can."
  [store-id]
  (guard/in-flight? store-id))

(defn safe-point
  "The instant before which every object written to `store-id` is either
   reachable from a pointer or garbage — i.e. the sweep cutoff.

   No sequence in flight => `now`: nothing is mid-write, so the mark's verdict on
   everything written so far is final. Sequences in flight => the START of the
   oldest one: everything it writes lands at or after that instant, so sparing
   from there spares exactly its objects and nothing else.

   Callers must capture their own `now` BEFORE calling this and take the min, so
   that a sequence which opens and closes between the two reads cannot slip
   through: if it completed, its pointer landed and the mark (which runs after)
   sees it."
  [store-id]
  (guard/safe-point store-id))

#?(:clj
   (defmacro with-unreferenced-writes
     "Run `body` as one unreferenced-write sequence against `store-id`: no
      concurrent collection in this process will sweep what it writes, until it
      completes. Use it whenever you write values into the store that only a LATER
      write (a transaction, a branch head) makes reachable."
     [store-id & body]
     `(let [sid# ~store-id
            t#   (writing! sid#)]
        (try ~@body
             (finally (done! sid# t#))))))
