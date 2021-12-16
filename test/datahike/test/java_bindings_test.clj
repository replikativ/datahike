(ns datahike.test.java-bindings-test
  (:require
   [clojure.test :refer [is deftest]]))

(deftest test-java-bindings
  (is (datahike.java.DatahikeTest/run)))
