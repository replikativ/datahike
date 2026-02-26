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
   (defn create
     [seed]
     (let [state (atom (long seed))]
       (reify PRNG
         (next-double [_]
           (swap! state (fn [s] (mod (+ (* s 1103515245) 12345) (js/Math.pow 2 31))))
           (/ (double @state) (js/Math.pow 2 31)))
         (next-long [_]
           (swap! state (fn [s] (mod (+ (* s 1103515245) 12345) (js/Math.pow 2 31))))
           @state)
         (next-int [_ bound]
           (mod (next-long _) bound))
         (next-boolean [_]
           (even? (next-long _)))
         (fork [_]
           (create (next-long _)))))))

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