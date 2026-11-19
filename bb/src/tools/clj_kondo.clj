(ns tools.clj-kondo
  "Generate clj-kondo export configuration for dynamically generated API."
  (:require [babashka.fs :as fs]
            [babashka.process :as p]
            [clojure.edn :as edn]))

(defn generate-clj-kondo-config!
  "Generate clj-kondo configuration from api-specification.

  This:
  1. Generates export config and emit-api hook at resources/clj-kondo.exports/io.replikativ/datahike/
     (packaged in jar, auto-imported by consuming projects)
  2. Updates main config at .clj-kondo/config.edn with type-mismatch linter
     (for Datahike's own project, which loads the export via :config-paths)"
  [export-path main-path]
  (println "Generating clj-kondo configuration...")
  (let [clj-code (str "(require '[datahike.codegen.clj-kondo :as clj-kondo]) "
                      "(clj-kondo/write-config! \"" export-path "\") "
                      "(clj-kondo/update-main-config! \"" main-path "\")")
        result (p/shell {:out :string
                         :err :string}
                        "clojure" "-M" "-e" clj-code)]
    (when-not (zero? (:exit result))
      (println "Error generating clj-kondo config:")
      (println (:err result))
      (throw (ex-info "clj-kondo config generation failed" result)))
    (println (:out result))))

(def ^:private consumer-source
  "(ns consumer (:require [datahike.api :as d]))
(d/q '[:find ?e :in $ ?a :where [?e :name ?a]] 1 2)
(d/datoms 1 :eavt 2 :name \"x\")
(d/connect)
(d/pull 1 2 3 4)
(d/nope)
(d/pull-many 1 {:selector [:name] :eid 2})
(d/database-exists?)
")

(def ^:private expected-findings
  #{[5 :invalid-arity]
    [6 :unresolved-var]
    [7 :invalid-arity]})

(defn- clj-kondo
  [dir & args]
  (apply p/shell {:dir (str dir) :out :string :err :string :continue true}
         "clj-kondo" args))

(defn test-export!
  "Lints a consumer of datahike.api with the clj-kondo export in export-dir.
  Throws if the findings differ from the expected findings."
  [export-dir]
  (let [dir (fs/create-temp-dir {:prefix "clj-kondo-export"})
        api-source (str (fs/absolutize "src/datahike/api.cljc"))]
    (try
      (fs/copy-tree export-dir (fs/path dir ".clj-kondo" "imports" "io.replikativ" "datahike"))
      (fs/create-dirs (fs/path dir "src"))
      (spit (str (fs/path dir "src" "consumer.clj")) consumer-source)
      (let [deps (clj-kondo dir "--lint" api-source "--dependencies")]
        (when-not (zero? (:exit deps))
          (throw (ex-info "clj-kondo failed to analyze datahike.api" deps))))
      (let [{:keys [out]} (clj-kondo dir "--lint" "src" "--config" "{:output {:format :edn}}")
            findings (set (map (juxt :row :type) (:findings (edn/read-string out))))]
        (if (= expected-findings findings)
          (println "clj-kondo export OK")
          (throw (ex-info "Unexpected clj-kondo findings"
                          {:expected expected-findings :actual findings}))))
      (finally
        (fs/delete-tree dir)))))
