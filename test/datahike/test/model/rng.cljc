(ns datahike.test.model.rng
  "Fork-safe random number generation using SplittableRandom."
  #?(:clj (:import [java.util SplittableRandom])))

(defprotocol PRNG
  (next-double [this])
  (next-long [this])
  (next-int [this bound])
  (next-boolean [this])
  (fork [this]))

#?(:clj
   (defn- wrap-splittable-random
     [rng]
     (reify PRNG
       (next-double [_] (.nextDouble rng))
       (next-long [_] (.nextLong rng))
       (next-int [_ bound] (.nextInt rng (int bound)))
       (next-boolean [_] (.nextBoolean rng))
       (fork [_] (wrap-splittable-random (.split rng))))))

#?(:clj
   (defn create
     [seed]
     (wrap-splittable-random (SplittableRandom. (long seed)))))

#?(:cljs
   (defn- step-state
     "Linear congruential generator step function."
     [s]
     (mod (+ (* s 1103515245) 12345) (js/Math.pow 2 31))))

#?(:cljs
   (defn create
     [seed]
     (let [state (atom (long seed))]
       (reify PRNG
         (next-double [_]
           (/ (double (swap! state step-state)) (js/Math.pow 2 31)))
         (next-long [_]
           (swap! state step-state))
         (next-int [_ bound]
           (mod (swap! state step-state) bound))
         (next-boolean [_]
           (even? (swap! state step-state)))
         (fork [_]
           (create (swap! state step-state)))))))

(defn should-trigger?
  [rng rate]
  (< (next-double rng) rate))

(defn random-in-range
  [rng min-val max-val]
  (+ min-val (next-int rng (inc (- max-val min-val)))))

(defn rand-nth-rng
  [rng coll]
  (nth coll (next-int rng (count coll))))

(defn weighted-sample-rng
  [rng pairs]
  (let [sum (transduce (map second) + pairs)
        at (* sum (next-double rng))]
    (loop [at at
           [[k w] & rest] pairs]
      (if (<= at w)
        k
        (recur (- at w) rest)))))

(defn shuffle-rng
  [rng coll]
  (let [v (vec coll)
        n (count v)]
    (loop [i (dec n)
           v v]
      (if (pos? i)
        (let [j (next-int rng (inc i))
              vi (v i)
              vj (v j)]
          (recur (dec i) (assoc v i vj j vi)))
        v))))

;; ---------------------------------------------------------------------------
;; splitmix64 — cross-platform DETERMINISTIC PRNG: the same seed yields the
;; SAME sequence on the JVM (unchecked longs) and JS (BigInt), so seeded
;; generative tests (the query differential) reproduce identically on both
;; platforms. The platform-diverging `create` above (SplittableRandom / LCG)
;; is kept for the model suite, which never needed cross-platform repro.

#?(:clj
   (defn- sm-mix ^long [^long z0]
     (let [z (unchecked-multiply (bit-xor z0 (unsigned-bit-shift-right z0 30))
                                 -4658895280553007687)          ;; 0xBF58476D1CE4E5B9
           z (unchecked-multiply (bit-xor z (unsigned-bit-shift-right z 27))
                                 -7723592293110705685)]          ;; 0x94D049BB133111EB
       (bit-xor z (unsigned-bit-shift-right z 31)))))

#?(:cljs
   (defn- sm-mix [z0]
     (let [m (js* "0xFFFFFFFFFFFFFFFFn")
           z (js* "((~{} ^ (~{} >> 30n)) * 0xBF58476D1CE4E5B9n) & ~{}" z0 z0 m)
           z (js* "((~{} ^ (~{} >> 27n)) * 0x94D049BB133111EBn) & ~{}" z z m)]
       (js* "~{} ^ (~{} >> 31n)" z z))))

(defn create-splitmix
  "Deterministic cross-platform PRNG (splitmix64). Same seed → same
   next-int/next-boolean/next-double sequence on JVM and JS. next-long
   returns the platform's native 64-bit value (long / BigInt) — use it for
   seeding forks, not for cross-platform comparison."
  [seed]
  (let [state (atom #?(:clj (long seed)
                       :cljs (js* "BigInt(~{}) & 0xFFFFFFFFFFFFFFFFn" seed)))
        step! (fn []
                (sm-mix (swap! state
                               #?(:clj (fn [^long s] (unchecked-add s -7046029254386353131)) ;; 0x9E3779B97F4A7C15
                                  :cljs (fn [s] (js* "(~{} + 0x9E3779B97F4A7C15n) & 0xFFFFFFFFFFFFFFFFn" s))))))]
    (reify PRNG
      (next-long [_] (step!))
      (next-int [_ bound]
        #?(:clj (long (Long/remainderUnsigned (long (step!)) (long bound)))
           :cljs (js/Number (js* "~{} % BigInt(~{})" (step!) bound))))
      (next-boolean [_]
        #?(:clj (zero? (bit-and (long (step!)) 1))
           :cljs (zero? (js/Number (js* "~{} & 1n" (step!))))))
      (next-double [_]
        #?(:clj (/ (double (unsigned-bit-shift-right (long (step!)) 11)) 9007199254740992.0)
           :cljs (/ (js/Number (js* "~{} >> 11n" (step!))) 9007199254740992.0)))
      (fork [this] (create-splitmix (next-long this))))))
