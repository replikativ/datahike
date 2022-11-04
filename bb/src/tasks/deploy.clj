(ns tasks.deploy
  (:require [tasks.build :refer [pom-path jar-path]]
            [tasks.settings :refer [load-settings]]
            [tasks.version :refer [version-str]]
            [utils.shell :refer [clj]]))

(def settings (load-settings))
(def project-version (version-str))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn remote
  "Don't forget to set CLOJARS_USERNAME and CLOJARS_PASSWORD env vars."
  []
  (clj "-X:deploy" "deps-deploy.deps-deploy/deploy"
       :installer :remote
       :artifact (jar-path settings project-version)
       :pom-file (pom-path settings)))

(defn local  []
  (clj "-X:deploy" "deps-deploy.deps-deploy/deploy" ;; use clojure.tools.build.api/install instead?
       :installer :local
       :artifact (jar-path settings project-version)))
