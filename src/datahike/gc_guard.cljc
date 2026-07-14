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
   writer's commit loop, but `datahike.versioning/branch!` performs the very same
   values-then-pointer sequence OUTSIDE the writer entirely. So the guard lives
   here, next to the invariant, and every such sequence takes it.

   USAGE — wrap the whole values-then-pointer sequence:

     (let [t (writing! store-id)]
       (try (write-values!) (write-pointer!)
            (finally (done! store-id t))))

   or `with-unreferenced-writes`, which does the same.

   A process that dies mid-sequence simply drops its entry: the objects it wrote
   are unreachable, i.e. garbage, and a later cycle collects them. Correct by
   construction.

   SCOPE: this is in-process state, which matches datahike's writer model — ALL
   WRITERS FOR A DATABASE RUN IN ONE JVM, coordinating in memory rather than through
   the store, and writer-side maintenance runs with them. A writer in another process
   is outside that model (and outside it for a more basic reason than GC: without head
   fencing, two writers on a branch can lose each other's commits — issue #878).
   Readers are unconstrained."
  #?(:clj (:import [java.util Date])))

(defn- now [] #?(:clj (Date.) :cljs (js/Date.)))
(defn- ms [d] #?(:clj (.getTime ^Date d) :cljs (.getTime d)))

;; store-id -> {token start-instant}. Keyed by the store's :id (from the store
;; config) rather than the store object: separate connections to the same
;; physical store hold DIFFERENT konserve store instances, and a collection on
;; one must see a sequence in flight on another.
(defonce ^:private in-flight (atom {}))

;; Tokens are counter values, not fresh objects: a token is a MAP KEY, and a bare
;; `(js/Object.)` implements neither IHash nor IEquiv in cljs, so it cannot be one.
(defonce ^:private token-seq (atom 0))

(defn writing!
  "Open an unreferenced-write sequence on `store-id`. Returns a token to close it
   with. Call BEFORE the first value is written."
  [store-id]
  (let [token (swap! token-seq inc)]
    (swap! in-flight assoc-in [store-id token] (now))
    token))

(defn done!
  "Close the sequence — its pointer has landed, so everything it wrote is now
   reachable (or garbage, if the pointer superseded it)."
  [store-id token]
  (swap! in-flight (fn [m]
                     (let [m' (update m store-id dissoc token)]
                       (if (empty? (get m' store-id))
                         (dissoc m' store-id)
                         m'))))
  nil)

(defn in-flight?
  "Is an unreferenced-write sequence currently open on `store-id`?

   `safe-point` cannot answer this: it returns `now` both when nothing is in
   flight AND when a sequence opened within the same millisecond, so a test that
   compares timestamps cannot tell a held guard from a missing one. This can."
  [store-id]
  (boolean (seq (get @in-flight store-id))))

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
  (let [starts (vals (get @in-flight store-id))]
    (if (seq starts)
      (reduce (fn [a b] (if (< (ms a) (ms b)) a b)) starts)
      (now))))

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
