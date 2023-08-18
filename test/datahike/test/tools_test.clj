(ns datahike.test.tools-test
  (:require [datahike.tools :as dt]
            [clojure.test :refer :all]))

(deftest test-with-elements-of
  (is (= [11 19]
         (dt/with-elements-of [10 20]
           a (inc a)
           b (dec b))))
  (is (= [11]
         (dt/with-elements-of [10]
           a (inc a)
           b (+ a b))))
  (is (= [300 40]
         (dt/with-elements-of [10 30]
           a (* a b)
           b (+ a b))))
  (is (= [100 400]
         (dt/with-elements-of [10 20]
           a (* a a)
           b (* b b)
           c (throw (ex-info "This element should not be evaluated" {}))))))
