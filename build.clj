(ns build
  (:refer-clojure :exclude [compile])
  (:require [clojure.edn :as edn]
            [clojure.tools.build.api :as b]))

(defn compile [_]
  (let [{:build/keys [deps-file class-dir java-src-dirs]}
        (edn/read-string (slurp "config.edn"))]
    (print (str "Compiling Java classes saving them to '" class-dir "'..."))
    (b/javac {:src-dirs java-src-dirs
              :class-dir class-dir
              :basis (b/create-basis {:project deps-file})
              :javac-opts ["--release" "8"]})
    (println "Done.")))
