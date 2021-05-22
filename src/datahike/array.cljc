(ns datahike.array
  (:require [hitchhiker.tree.node :as n]))

(defn compare-arrays [a b]
  ;; TODO can avoid some of these safety checks?
  (if (not (and (bytes? a) (bytes? b)))
    (try
      (compare a b)
      (catch ClassCastException _
        (- (n/-order-on-edn-types a)
           (n/-order-on-edn-types b))))
    (let [lc (compare (alength a) (alength b))]
      (if (not (zero? lc))
        lc
        (loop [step (alength a)]
          (if-not (zero? step)
            (let [ec (compare (aget a (dec step)) (aget b (dec step)))]
              (if (not (zero? ec))
                ec
                (recur (dec step))))
            0))))))

(defn a=
  "Extension of Clojure's equality to things we also want to treat like values,
  e.g. certain array types."
  [a b]
  (or (= a b)
      (and (bytes? a)
           (bytes? b)
           (zero? (compare-arrays a b)))))
