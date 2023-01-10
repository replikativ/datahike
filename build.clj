(ns build
  (:require
    [borkdude.gh-release-artifact :as gh]
    [clojure.tools.build.api :as b]
    [deps-deploy.deps-deploy :as dd])
  (:import (clojure.lang ExceptionInfo)))

(def lib 'io.replikativ/datahike)
(def version (format "0.6.%s" (b/git-count-revs nil)))
(def current-commit (gh/current-commit))
(def class-dir "target/classes")
(def basis (b/create-basis {:project "deps.edn"}))
(def jar-file (format "target/%s-%s.jar" (name lib) version))
(def native-shared-library-file (format "target/%s-%s-native-shared-library.jar"
                                        (name lib) version))

(defn clean
  [_]
  (b/delete {:path "target"}))

(defn compile
  [_]
  (b/javac {:src-dirs ["java"]
            :class-dir class-dir
            :basis basis
            :javac-opts ["-source" "8" "-target" "8"]})
  (spit "resources/DATAHIKE_VERSION" version))

(defn jar
  [_]
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

(defn native-shared-library [_]
  (clean nil)
  (b/delete {:path "libdatahike/target"})
  (compile nil)
  (b/copy-dir {:src-dirs   ["src" "resources" "libdatahike/src"]
               :target-dir class-dir})
  (let [basis (b/create-basis {:project "deps.edn"
                               :aliases [:libdatahike]})]
    (b/compile-clj {:basis     basis
                    :src-dirs  ["src"]
                    :class-dir class-dir})
    (b/compile-clj {:basis     basis
                    :src-dirs  ["libdatahike/src"]
                    :class-dir class-dir})
    (b/uber {:class-dir class-dir
             :uber-file native-shared-library-file
             :basis     basis
             :main      'datahike.cli})))

(defn deploy
  "Don't forget to set CLOJARS_USERNAME and CLOJARS_PASSWORD env vars."
  [_]
  (dd/deploy {:installer :remote :artifact jar-file
              :pom-file (b/pom-path {:lib lib :class-dir class-dir})}))

(defn fib [a b]
  (lazy-seq (cons a (fib b (+ a b)))))

(defn retry-with-fib-backoff [retries exec-fn test-fn]
  (loop [idle-times (take retries (fib 1 2))]
    (let [result (exec-fn)]
      (if (test-fn result)
        (when-let [sleep-ms (first idle-times)]
          (println "Returned: " result)
          (println "Retrying with remaining back-off times (in s): " idle-times)
          (Thread/sleep (* 1000 sleep-ms))
          (recur (rest idle-times)))
        result))))

(defn try-release []
  (try (gh/overwrite-asset {:org "replikativ"
                            :repo (name lib)
                            :tag version
                            :commit current-commit
                            :file jar-file
                            :content-type "application/java-archive"
                            :draft false})
       (catch ExceptionInfo e
         (assoc (ex-data e) :failure? true))))

(defn release
  [_]
  (-> (retry-with-fib-backoff 10 try-release :failure?)
      :url
      println))

(defn install
  [_]
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
  (deploy nil)
  (release nil)
  (install nil)

  (name lib)
  (namespace lib)
  (require '[babashka.fs :as fs])
  (fs/file-name (format "target/datahike-%s.jar" version)))
