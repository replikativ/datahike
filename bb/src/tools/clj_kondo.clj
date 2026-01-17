(ns tools.clj-kondo
  "Generate clj-kondo export configuration for dynamically generated API."
  (:require [babashka.process :as p]))

(defn generate-clj-kondo-config!
  "Generate clj-kondo configuration from api-specification.

  This:
  1. Generates export config at resources/clj-kondo.exports/io.replikativ/datahike/config.edn
     (packaged in jar, auto-imported by consuming projects)
  2. Copies to .clj-kondo/datahike/datahike/config.edn
     (for local development via :config-paths)
  3. Updates main config at .clj-kondo/config.edn with type-mismatch linter
     (for Datahike's own project)

  This allows clj-kondo to understand the dynamically expanded API functions
  from the emit-api macro, providing IDE autocomplete, signature hints, and
  type checking."
  [export-path local-path main-path]
  (println "Generating clj-kondo configuration...")
  (let [clj-code (str "(require '[datahike.codegen.clj-kondo :as clj-kondo]) "
                      "(clj-kondo/write-config! \"" export-path "\") "
                      "(clj-kondo/copy-export-to-local! \"" export-path "\" \"" local-path "\") "
                      "(clj-kondo/update-main-config! \"" main-path "\")")
        result (p/shell {:out :string
                         :err :string}
                        "clojure" "-M" "-e" clj-code)]
    (when-not (zero? (:exit result))
      (println "Error generating clj-kondo config:")
      (println (:err result))
      (throw (ex-info "clj-kondo config generation failed" result)))
    (println (:out result))))
