(ns datahike.test.java-bindings
  (:require
   [clojure.test :refer [is deftest]]))

(deftest test-java-bindings
  (is (datahike.java.DatahikeTest/run)))
