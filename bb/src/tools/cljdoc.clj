(ns tools.cljdoc
  (:require [babashka.fs :as fs]
            [babashka.process :refer [shell]]
            [clojure.string :as str]
            [tools.version :refer [version-str]]))

(def tmp-dir "/tmp/cljdoc")

(def maven-dir (str (fs/home) "/.m2"))

(defn docker [& args] (apply shell "docker" args))

(defn lib-installed? [{:build/keys [lib] :as config}]
  (let [lib-dir (str maven-dir "/repository/"
                     (str/replace (str lib) "." "/") "/"
                     (version-str config))]
    (println lib-dir)
    (fs/exists? lib-dir)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn preview [{:build/keys [lib] :as config}]
  (println "Creating temporary directory at" tmp-dir)
  (fs/delete-tree tmp-dir)
  (fs/create-dirs tmp-dir)

  (when-not (lib-installed? config)
    (println "Version" (version-str config) "of" (str lib) "is not installed!"
             " Please run 'bb jar' and 'bb install' and try again.")
    (System/exit 1))

  (println "---- cljdoc preview: ingesting datahike")
  (docker "run" "--rm"
          "--volume" (str (fs/cwd) ":/repo-to-import")
          "--volume" (str maven-dir ":/root/.m2")
          "--volume" (str tmp-dir ":/app/data")
          "--entrypoint" "clojure" "cljdoc/cljdoc" "-M:cli" "ingest" "-p" (str lib) "-v" (version-str config)
          "--git" "/repo-to-import")

  (println "---- cljdoc preview: starting server on port 8000")
  (docker "run" "--rm" "-p" "8000:8000" "-v" (str tmp-dir ":/app/data") "cljdoc/cljdoc"))
