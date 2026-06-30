(ns datahike.experimental.graph-util
  "Portable (Clojure + ClojureScript) primitives for graph algorithms:

   - a priority queue with a TOTAL ordering (equal-priority entries never
     collide — this is what the original `sorted-set-by`-on-cost code got
     wrong, silently dropping nodes),
   - a seedable PRNG (deterministic on both JVM and JS, for reproducible
     random walks — no `java.util.Random`),
   - a FIFO queue (no `clojure.lang.PersistentQueue` host reference at call
     sites),
   - small math helpers (`infinity`, `abs`).

   All intermediate arithmetic stays < 2^53 so the PRNG is exact under JS
   doubles.")

;; ---------------------------------------------------------------------------
;; Priority queue (min-first), total order.
;;
;; Backed by a sorted-set of [priority seq item] vectors. A monotonically
;; increasing `seq` makes every entry distinct even when priorities (and
;; items) are equal, so the set never deduplicates distinct insertions — the
;; failure mode of the previous shortest-path/bottleneck implementations,
;; whose comparators compared the cost only.
;;
;; O(log n) push / peek / pop. Duplicate items are allowed, which makes lazy
;; deletion (push an improved key, skip stale pops) straightforward for
;; Dijkstra / Prim / A*.

(defn- pq-compare [a b]
  (let [c (compare (nth a 0) (nth b 0))]
    (if (zero? c) (compare (nth a 1) (nth b 1)) c)))

(defn pq
  "An empty min-priority-queue. Priorities are ordered by `compare`.
   For a max-queue, push negated priorities."
  []
  {:set (sorted-set-by pq-compare) :seq 0})

(defn pq-push
  "Insert `item` with comparable `priority`."
  [q priority item]
  (let [n (:seq q)]
    (-> q
        (update :set conj [priority n item])
        (assoc :seq (inc n)))))

(defn pq-empty? [q]
  (empty? (:set q)))

(defn pq-peek
  "Return [priority item] of the minimum, or nil when empty."
  [q]
  (when-let [e (first (:set q))]
    [(nth e 0) (nth e 2)]))

(defn pq-pop
  "Remove the minimum and return the new queue (no-op when empty)."
  [q]
  (if-let [e (first (:set q))]
    (update q :set disj e)
    q))

;; ---------------------------------------------------------------------------
;; FIFO queue (BFS frontiers). conj / peek / pop behave identically on JVM/JS.

(def empty-queue
  #?(:clj clojure.lang.PersistentQueue/EMPTY
     :cljs cljs.core/PersistentQueue.EMPTY))

;; ---------------------------------------------------------------------------
;; Seedable PRNG — a linear congruential generator (Numerical Recipes
;; constants) over 2^32. Low bits are weak but this is only used for sampling
;; neighbors in random walks, where it is more than adequate, and it is fully
;; deterministic and identical on JVM and JS (every product stays < 2^53).

(def ^:private lcg-mod 4294967296) ;; 2^32

(defn rng
  "Create a PRNG state from an integer `seed`."
  [seed]
  (mod seed lcg-mod))

(defn rng-next
  "Advance the state. Returns [new-state unit] with unit in [0,1)."
  [state]
  (let [s (mod (+ (* 1664525 state) 1013904223) lcg-mod)]
    [s (/ s lcg-mod)]))

(defn rng-int
  "Return [new-state i] with i uniform in [0,n). `n` must be positive."
  [state n]
  (let [[s u] (rng-next state)
        i #?(:clj (long (* u n)) :cljs (int (Math/floor (* u n))))]
    [s (min i (dec n))]))

(defn rng-nth
  "Return [new-state element], picking uniformly from a non-empty indexed coll."
  [state coll]
  (let [v (if (vector? coll) coll (vec coll))
        [s i] (rng-int state (count v))]
    [s (nth v i)]))

(defn rng-shuffle
  "Return [new-state shuffled-vector] — a deterministic Fisher-Yates shuffle of
   `coll` driven by the PRNG (portable across JVM and JS, unlike clojure.core
   `shuffle`, which is unseeded and platform-specific)."
  [state coll]
  (loop [st state i (dec (count coll)) v (vec coll)]
    (if (<= i 0)
      [st v]
      (let [[st' j] (rng-int st (inc i))]
        (recur st' (dec i) (assoc v i (v j) j (v i)))))))

;; ---------------------------------------------------------------------------
;; Math helpers.

(def infinity
  #?(:clj Double/POSITIVE_INFINITY
     :cljs js/Number.POSITIVE_INFINITY))

(defn abs
  "Absolute value, portable (avoids host Math/abs boxing differences)."
  [x]
  (if (neg? x) (- x) x))
