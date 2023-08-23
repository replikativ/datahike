(ns build
  (:require [clojure.java.shell :refer [sh]]))

  (defn bb-trigger [_input]
    (sh "bb" "jcompile"))
