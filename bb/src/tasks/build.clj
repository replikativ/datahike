(ns tasks.build
  (:refer-clojure :exclude [compile])
  (:require 
    [babashka.fs :as fs]
    [clojure.tools.build.api :as b]                         ;; via pods
    [tasks.settings :refer [load-settings]]
    [tasks.version :refer [version-str]]))

(def settings (load-settings))
(def project-version (version-str))

(defn clean
  ([] (clean settings))
  ([{:build/keys [target-dir] :as _config}]
  (print (str "Cleaning up target directory '" target-dir "'..."))
  (fs/delete-tree target-dir)
  (println "Done.")))

(defn compile 
  ([] (compile settings))
  ([{:build/keys [deps-file class-dir java-src-dirs] :as _config}]
  (print (str "Compiling Java classes saving them to '" class-dir "'..."))
  (b/javac {:src-dirs java-src-dirs
            :class-dir class-dir
            :basis (b/create-basis {:project deps-file})
            :javac-opts ["-source" "8" "-target" "8"]})
  (println "Done.")))

(defn pom-path [{:build/keys [class-dir lib] :as _config}]
  (b/pom-path {:lib lib
               :class-dir class-dir}))

(defn pom
  ([] (pom settings project-version))
  ([{:build/keys [deps-file class-dir pom-template lib clj-src-dirs] :as _config} version]
  (print (str "Creating pom file from template at '" pom-template "'..."))
  (b/write-pom {:class-dir class-dir
                :src-pom pom-template
                :lib lib
                :version version
                :basis (b/create-basis {:project deps-file})
                :src-dirs clj-src-dirs})
  (println "Done.")))

(defn jar-path [{:build/keys [target-dir lib] :as _config} version]
  (str target-dir "/" (format "%s-%s.jar" (name lib) version)))

(defn jar
  "Returns paths to the jar and the pom file"
  ([] (jar settings project-version))
  ([{:build/keys [class-dir target-dir clj-src-dirs] :as config} version]
  (print (str "Packaging jar at '" target-dir "'..."))
  (b/copy-dir {:src-dirs clj-src-dirs
               :target-dir class-dir})
  (b/jar {:class-dir class-dir
          :jar-file (jar-path config version)})
  (println "Done.")))

(defn package []
  (clean)
  (compile)
  (pom)
  (jar))
