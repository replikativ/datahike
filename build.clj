(ns build
  (:require [clojure.tools.build.api :as b]
            [deps-deploy.deps-deploy :as dd]
            [borkdude.gh-release-artifact :as gh]))

(def lib 'timokramer/datahike)
(def version (format "0.4.%s" (b/git-count-revs nil)))
(def current-commit (gh/current-commit))
(def class-dir "target/classes")
(def basis (b/create-basis {:project "deps.edn"}))
(def jar-file (format "target/%s-%s.jar" (name lib) version))

(defn clean [_]
  (b/delete {:path "target"}))

(defn compile [_]
  (b/javac {:src-dirs ["java"]
            :class-dir class-dir
            :basis basis
            :javac-opts ["-source" "8" "-target" "8"]}))

(defn jar [_]
  (compile nil)
  (b/write-pom {:class-dir class-dir
                :src-pom "./template/pom.xml"
                :lib lib
                :version version
                :basis basis
                :src-dirs ["src"]})
  (b/copy-dir {:src-dirs ["src" "resources"]
               :target-dir class-dir})
  (b/jar {:class-dir class-dir
          :jar-file jar-file}))

(defn deploy [_]
  (println "Don't forget to set CLOJARS_USERNAME and CLOJARS_PASSWORD env vars.")
  (dd/deploy {:installer :remote :artifact jar-file
              :pom-file (b/pom-path {:lib lib :class-dir class-dir})}))

(defn release [_]
  (-> (gh/overwrite-asset {:org (namespace lib)
                           :repo (name lib)
                           :tag version
                           :commit current-commit
                           :file jar-file
                           :content-type "application/java-archive"})
      :url
      println))

(defn install [_]
  (clean nil)
  (jar nil)
  (b/install {:basis (b/create-basis {})
              :lib lib
              :version version
              :jar-file jar-file
              :class-dir class-dir}))

(comment
  (b/pom-path {:lib lib :class-dir class-dir})
  (clean nil)
  (compile nil)
  (jar nil)
  (tag nil)
  (deploy nil)
  (release nil)
  (install nil)

  (name lib)
  (namespace lib)
  (require '[babashka.fs :as fs])
  (fs/file-name (format "target/datahike-%s.jar" version)))
