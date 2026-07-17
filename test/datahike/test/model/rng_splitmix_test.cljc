(ns datahike.test.model.rng-splitmix-test
  "Pins splitmix64's cross-platform determinism: the literal expected values
   below were generated on the JVM; the same assertions run unchanged on JS.
   If either platform's arithmetic drifts (masking, signedness, shift), the
   pinned sequences diverge and seeded generative tests stop reproducing."
  (:require
   #?(:cljs [cljs.test :refer-macros [is deftest testing]]
      :clj  [clojure.test :refer [deftest is testing]])
   [datahike.test.model.rng :as rng]))

(deftest splitmix-pinned-sequences
  (testing "next-int stream, seed 42, bound 1000"
    (let [r (rng/create-splitmix 42)]
      (is (= [413 291 858 764 250 62 925 908 5 974]
             (vec (repeatedly 10 #(rng/next-int r 1000)))))))
  (testing "next-boolean stream, seed 42"
    (let [r (rng/create-splitmix 42)]
      (is (= [false false true true true true]
             (vec (repeatedly 6 #(rng/next-boolean r)))))))
  (testing "next-int stream, the differential suite's seed, bound 97"
    (let [r (rng/create-splitmix 1721160000042)]
      (is (= [25 38 95 34 13]
             (vec (repeatedly 5 #(rng/next-int r 97)))))))
  (testing "shuffle-rng is deterministic under splitmix"
    (is (= (rng/shuffle-rng (rng/create-splitmix 7) (range 10))
           (rng/shuffle-rng (rng/create-splitmix 7) (range 10))))))
