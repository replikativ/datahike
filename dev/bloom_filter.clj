(ns bloom-filter
  (:require [bloom.core :as bf]))


(let [upper-cardinality 20
      target-false-positive-rate 0.05]
  (def my-bf
    (bf/->bf upper-cardinality target-false-positive-rate)))
(doseq [i (range 10)]
  (bf/add! my-bf (str i)))
(bf/has? my-bf "09")  ;; true
(bf/has? my-bf "45") ;; false



(doseq [i (range 10)]
  (bf/add! my-bf (str [i i])))
(bf/has? my-bf (str [1 1]))  ;; true
(bf/has? my-bf (str [20 20])) ;; false
