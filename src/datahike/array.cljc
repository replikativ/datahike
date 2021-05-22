(ns datahike.array
  (:require [hitchhiker.tree.node :as n]))

(defn compare-arrays
  "Compare two arrays a and b element-wise in ascending order. If one array is a
  prefix of another then it comes first."
  [a b]
  (if (not (and (bytes? a) (bytes? b)))
    (try
      (compare a b)
      (catch ClassCastException _
        (- (n/-order-on-edn-types a)
           (n/-order-on-edn-types b))))
    (let [bl (alength b)
          al (alength a)]
      (loop [i 1]
        (cond (and (> i bl) (> i al))
              0

              (> i bl)
              1 ;; b is a prefix of a

              (> i al)
              -1 ;; a is a prefix of b

              :else
              (let [ec (compare (aget a (dec i)) (aget b (dec i)))]
                (if (not (zero? ec))
                  ec
                  (recur (inc i)))))))))

(defn a=
  "Extension of Clojure's equality to things we also want to treat like values,
  e.g. certain array types."
  [a b]
  (or (= a b)
      (and (bytes? a)
           (bytes? b)
           (zero? (compare-arrays a b)))))
