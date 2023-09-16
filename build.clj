(ns build
  (:refer-clojure :exclude [compile])
  (:require [clojure.edn :as edn]
            [clojure.tools.build.api :as b]))

(def class-dir "target/classes")
(def basis (b/create-basis {:project "deps.edn"}))

(defn compile-java
  [_]
  (b/javac {:src-dirs ["java"]
            :class-dir class-dir
            :basis basis
            :javac-opts ["--release" "8"
                         "-Xlint:deprecation"]}))
