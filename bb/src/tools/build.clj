(ns tools.build
  (:refer-clojure :exclude [compile])
  (:require [babashka.fs :as fs]
            [babashka.process :as p]
            [clojure.tools.build.api :as b]
            [selmer.parser :refer [render]]
            [tools.version :as version :refer [read-edn-file]]))

(defn clean [{:keys [target-dir] :as _project-config}]
  (print (str "Cleaning up target directory '" target-dir "'..."))
  (fs/delete-tree target-dir)
  (println "Done."))

(defn basis [{:keys [deps-file aliases] :as _project-config}]
  (b/create-basis (cond-> {:project deps-file}
                    aliases (assoc :aliases aliases))))

(defn compile-java
  ([] (compile-java (read-edn-file "config.edn")))
  ([{:keys [class-dir java-src-dirs] :as project-config}]
   (print (str "Compiling Java classes saving them to '" class-dir "'..."))
   (b/javac {:src-dirs java-src-dirs
             :class-dir class-dir
             :basis (basis project-config)
             :javac-opts ["--release" "8"
                          "-Xlint:deprecation"]})
   (println "Done.")))

(defn compile-clojure [{:keys [class-dir src-dirs] :as project-config}]
  (print (str "Compiling Clojure namespaces saving them to '" class-dir "'..."))
  (b/compile-clj {:src-dirs src-dirs
                  :class-dir class-dir
                  :basis (basis project-config)})
  (println "Done."))

(defn pom-path [{:keys [class-dir lib] :as _project-config}]
  (b/pom-path {:lib lib
               :class-dir class-dir}))

(defn pom [{:keys [pom-template scm] :as repo-config}
           {:keys [class-dir lib src-dirs] :as project-config}]
  (print (str "Creating pom file from template at '" pom-template "'..."))
  (b/write-pom {:src-pom pom-template
                :src-dirs src-dirs
                :class-dir class-dir
                :lib lib
                :version (version/string repo-config)
                :basis (basis project-config)
                :scm (assoc scm :tag (version/sha))})
  (println "Done." "Saved to" (pom-path project-config)))

(defn jar-path [repo-config {:keys [target-dir jar-pattern] :as project-config}]
  (str target-dir "/" (render jar-pattern {:project project-config
                                           :repo repo-config
                                           :version-str (version/string repo-config)})))

(defn jar
  "Builds jar file"
  [repo-config {:keys [class-dir target-dir src-dirs resource-dir] :as project-config}]
  (print (str "Packaging jar at '" target-dir "'..."))
  (b/copy-dir {:src-dirs (filter identity (conj src-dirs resource-dir))
               :target-dir class-dir})
  (b/jar {:class-dir class-dir
          :jar-file (jar-path repo-config project-config)})
  (println "Done."))

(defn uber
  "Builds uber jar file"
  [repo-config {:keys [class-dir target-dir src-dirs resource-dir main] :as project-config}]
  (print (str "Packaging uber jar at '" target-dir "'..."))
  (b/copy-dir {:src-dirs (filter identity (conj src-dirs resource-dir))
               :target-dir class-dir})
  (b/uber {:class-dir class-dir
           :uber-file (jar-path repo-config project-config)
           :basis (basis project-config)
           :main main})
  (println "Done."))

(defn native-compile [repo-config {:keys [project-target-dir class-path project-name java-interface] :as project-config}]
  (if-let [graalvm-dir (System/getenv "GRAALVM_HOME")]
    (let [native-jar (jar-path repo-config project-config)
          svm-jar (str graalvm-dir "/lib/svm/builder/svm.jar")]
      (println "Compiling native bindings Java class.")
      (p/shell (str graalvm-dir "/bin/javac") "-cp" (str native-jar ":" svm-jar) java-interface)
      (println "Compiling shared library through native image.")
      (p/shell (str graalvm-dir "/bin/native-image")
               "-jar" native-jar
               "-cp" class-path
               (str "-o " project-name)
               "--shared"
               "-H:+ReportExceptionStackTraces"
               "-J-Dclojure.spec.skip-macros=true"
               "-J-Dclojure.compiler.direct-linking=true"
               (str "-H:IncludeResources=" (version/string repo-config))
               "--initialize-at-build-time"
               "-H:Log=registerResource:"
               "--verbose"
               "--no-fallback"
               "--no-server"
               "-J-Xmx5g")
      (fs/delete-tree project-target-dir)
      (fs/create-dir project-target-dir)
      (run! #(fs/move % project-target-dir)
            (concat ["graal_isolate.h" "graal_isolate_dynamic.h"]
                    (map (fn [ext] (str project-name ext))
                         [".so" ".h" "_dynamic.h" ".build_artifacts.txt"]))))
    (do (println "GRAALVM_HOME not set!")
        (println "Please set GRAALVM_HOME to the root of the graalvm directory on your system."))))
