(ns tools.cljdoc
  (:require [babashka.fs :as fs]
            [babashka.process :refer [shell]]
            [tools.build :as build]
            [tools.deploy :as deploy]
            [tools.version :refer [version-str]]))

(def tmp-dir "/tmp/cljdoc")

(defn docker [& args] (apply shell "docker" args))


#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn preview [config]
  (println "Creating temporary directory at" tmp-dir)
  (fs/delete-tree tmp-dir)
  (fs/create-dirs tmp-dir)

  (build/jar config)
  (deploy/local config)

  (println "---- cljdoc preview: ingesting datahike")
  (docker "run" "--rm"
          "--volume" "$PWD:/repo-to-import"
          "--volume" "$HOME/.m2:/root/.m2"
          "--volume" "/tmp/cljdoc:/app/data"
          "--entrypoint" "clojure" "cljdoc/cljdoc" "-A:cli" "ingest" "-p" (name (:lib config)) "-v" (version-str config)
          "--git" "/repo-to-import")

  (println "---- cljdoc preview: starting server on port 8000")
  (docker "run" "--rm" "-p" "8000:8000" "-v" "/tmp/cljdoc:/app/data" "cljdoc/cljdoc"))
