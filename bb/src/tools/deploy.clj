(ns tools.deploy
  (:require [babashka.process :refer [shell]]
            [tools.build :refer [pom-path jar-path]]))

(defn clj [& args] (apply shell "clojure" args))
(defn quoted [s] (str \" s \"))

(defn remote
  "Deploy library to clojars"
  [repo-config project-config]
  (if-not (System/getenv "CLOJARS_USERNAME")
    (println "Environment variable CLOJARS_USERNAME not set!")
    (if-not (System/getenv "CLOJARS_PASSWORD")
      (println "Environment variable CLOJARS_PASSWORD not set!")
      (clj "-X:deploy" "deps-deploy.deps-deploy/deploy"
           :installer :remote
           :artifact (jar-path repo-config project-config)
           :pom-file (quoted (pom-path project-config))))))

(defn local
  "Install library locally"
  [repo-config project-config]
  (clj "-X:deploy" "deps-deploy.deps-deploy/deploy"
       :installer :local
       :artifact (jar-path repo-config project-config)
       :pom-file (quoted (pom-path project-config))))
