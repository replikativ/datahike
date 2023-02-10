(ns tools.deploy
  (:require [babashka.process :refer [shell]]
            [tools.build :refer [pom-path jar-path]]))

(defn clj [& args] (apply shell "clojure" args))
(defn quoted [s] (str \" s \"))

(defn remote
  "Deploy library to clojars"
  [config]
  (if-not (System/getenv "CLOJARS_USERNAME")
    (println "Environment variable CLOJARS_USERNAME not set!")
    (if-not (System/getenv "CLOJARS_PASSWORD") 
      (println "Environment variable CLOJARS_PASSWORD not set!") 
      (clj "-X:deploy" "deps-deploy.deps-deploy/deploy" 
           :installer :remote 
           :artifact (jar-path config) 
           :pom-file (quoted (pom-path config))))))

(defn local
  "Install library locally"
  [config]
  (clj "-X:deploy" "deps-deploy.deps-deploy/deploy" ;; use clojure.tools.build.api/install instead?
       :installer :local
       :artifact (jar-path config)
       :pom-file (quoted (pom-path config))))
