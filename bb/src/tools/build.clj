(ns tools.build
  (:refer-clojure :exclude [compile])
  (:require [babashka.fs :as fs]
            [clojure.tools.build.api :as b]
            [tools.version :as version :refer [read-edn-file]]))

(defn clean [{:build/keys [target-dir native] :as _config}]
  (print (str "Cleaning up target directory '" target-dir "'..."))
  (fs/delete-tree target-dir)
  (when (fs/exists? (:target-dir native))
    (println (str "Cleaning up native target directory '" (:target-dir native) "'..."))
    (fs/delete-tree (:target-dir native)))
  (println "Done."))

(defn compile
  ([] (compile (read-edn-file "config.edn")))
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

(defn pom [{:build/keys [deps-file class-dir pom-template lib clj-src-dirs scm] :as config}]
  (print (str "Creating pom file from template at '" pom-template "'..."))
  (b/write-pom {:class-dir class-dir
                :src-pom pom-template
                :lib lib
                :version (version/string config)
                :basis (b/create-basis {:project deps-file})
                :src-dirs clj-src-dirs
                :scm (assoc scm :tag (version/sha))})
  (println "Done." "Saved to" (pom-path config)))

(defn jar-path [{:build/keys [target-dir lib] :as config}]
  (str target-dir "/" (format "%s-%s.jar" (name lib) (version/string config))))

(defn jar
  "Returns paths to the jar and the pom file"
  [{:build/keys [class-dir target-dir clj-src-dirs resource-dirs] :as config}]
  (print (str "Packaging jar at '" target-dir "'..."))
  (b/copy-dir {:src-dirs (concat clj-src-dirs resource-dirs)
               :target-dir class-dir})
  (b/jar {:class-dir class-dir
          :jar-file (jar-path config)})
  (println "Done."))

(defn native-jar-path [{:build/keys [target-dir lib] :as config}]
  (str target-dir "/" (format "%s-%s-native-shared-library.jar" (name lib) (version/string config))))

(defn native-shared-library [{:build/keys [deps-file class-dir resource-dirs clj-src-dirs native] :as config}]
  (clean config)
  (compile config)
  (b/copy-dir {:src-dirs (vec (concat resource-dirs clj-src-dirs (:source-dirs native)))
               :target-dir class-dir})
  (let [basis (b/create-basis {:project deps-file
                               :aliases (:aliases native)})]
    (b/compile-clj {:basis     basis
                    :src-dirs  clj-src-dirs
                    :class-dir class-dir})
    (b/compile-clj {:basis     basis
                    :src-dirs  (:source-dirs native)
                    :class-dir class-dir})
    (b/uber {:class-dir class-dir
             :uber-file (native-jar-path config)
             :basis     basis
             :main      (:main native)})))
