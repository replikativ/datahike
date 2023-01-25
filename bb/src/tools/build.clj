(ns tools.build
  (:refer-clojure :exclude [compile])
  (:require
   [babashka.fs :as fs]
   [clojure.tools.build.api :as b]
   [tools.version :refer [version-str]]))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn clean [{:build/keys [target-dir native] :as _config}]
  (print (str "Cleaning up target directory '" target-dir "'..."))
  (fs/delete-tree target-dir)
  (when (fs/exists? (:target-dir native))
    (println (str "Cleaning up native target directory '" (:target-dir native) "'...")) 
    (fs/delete-tree (:target-dir native)))
  (println "Done."))

(defn compile [{:build/keys [deps-file class-dir java-src-dirs] :as _config}]
  (print (str "Compiling Java classes saving them to '" class-dir "'..."))
  (b/javac {:src-dirs java-src-dirs
            :class-dir class-dir
            :basis (b/create-basis {:project deps-file})
            :javac-opts ["-source" "8" "-target" "8"]})
  (println "Done."))

(defn pom-path [{:build/keys [class-dir lib] :as _config}]
  (b/pom-path {:lib lib
               :class-dir class-dir}))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn pom [{:build/keys [deps-file class-dir pom-template lib clj-src-dirs] :as config}]
  (print (str "Creating pom file from template at '" pom-template "'..."))
  (b/write-pom {:class-dir class-dir
                :src-pom pom-template
                :lib lib
                :version (version-str config)
                :basis (b/create-basis {:project deps-file})
                :src-dirs clj-src-dirs})
  (println "Done." "Saved to" (pom-path config)))

(defn jar-path [{:build/keys [target-dir lib] :as config}]
  (str target-dir "/" (format "%s-%s.jar" (name lib) (version-str config))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn jar
  "Returns paths to the jar and the pom file"
  [{:build/keys [class-dir target-dir clj-src-dirs] :as config}]
  (print (str "Packaging jar at '" target-dir "'..."))
  (b/copy-dir {:src-dirs clj-src-dirs
               :target-dir class-dir})
  (b/jar {:class-dir class-dir
          :jar-file (jar-path config)})
  (println "Done."))

(defn native-jar-path [{:build/keys [target-dir lib] :as config}]
  (str target-dir "/" (format "%s-%s-native-shared-library.jar" (name lib) (version-str config))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
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
