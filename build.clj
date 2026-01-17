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

(defn javadoc
  "Generate Javadoc for the Java API.
   Output will be in target/javadoc and automatically included in the jar."
  [_]
  (b/javadoc {:src-dirs ["java/src"]
              :output-dir "target/javadoc"
              :javadoc-opts ["-public"
                            "-Xdoclint:none"
                            "-windowtitle" "Datahike Java API"
                            "-doctitle" "Datahike Java API Documentation"
                            "-link" "https://docs.oracle.com/javase/8/docs/api/"
                            "-link" "https://clojure.github.io/clojure/"]})
  (println "Javadoc generated in target/javadoc")
  (println "Javadoc will be automatically published to javadoc.io when released to Clojars"))
