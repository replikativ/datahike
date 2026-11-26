(ns tools.clj-kondo
  "Generate clj-kondo export configuration for dynamically generated API."
  (:require [babashka.fs :as fs]
            [babashka.process :as p]
            [pod.borkdude.clj-kondo :as clj-kondo]))

(defn generate-clj-kondo-config!
  "Generate clj-kondo configuration from api-specification.

  Generates export config and emit-api hook at resources/clj-kondo.exports/io.replikativ/datahike/
  (packaged in jar, auto-imported by consuming projects, loaded by Datahike's own .clj-kondo/config.edn)"
  [export-path]
  (println "Generating clj-kondo configuration...")
  (let [clj-code (str "(require '[datahike.codegen.clj-kondo :as clj-kondo]) "
                      "(clj-kondo/write-config! \"" export-path "\")")
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
(d/database-exists?)
")

(def ^:private expected-findings
  #{[5 :invalid-arity]
    [6 :unresolved-var]
    [7 :invalid-arity]})

(defn test-export!
  "Lints a consumer of datahike.api with the clj-kondo export in export-dir.
  Throws if the findings differ from the expected findings.

  Runs clj-kondo through the pod bb.edn already loads, so the check uses the
  same clj-kondo version as `bb lint` and needs no binary on the PATH."
  [export-dir]
  (let [dir (fs/create-temp-dir {:prefix "clj-kondo-export"})
        config-dir (str (fs/path dir ".clj-kondo"))
        cache-dir (str (fs/path dir ".clj-kondo" ".cache"))
        consumer (str (fs/path dir "src" "consumer.clj"))
        api-source (str (fs/absolutize "src/datahike/api.cljc"))]
    (try
      (fs/copy-tree export-dir (fs/path config-dir "imports" "io.replikativ" "datahike"))
      (fs/create-dirs (fs/path dir "src"))
      (spit consumer consumer-source)
      ;; What a consumer's editor does first: analyze the dependency, so its
      ;; vars are in the cache. Reports no findings.
      (clj-kondo/run! {:lint [api-source] :dependencies true
                       :config-dir config-dir :cache-dir cache-dir})
      (let [{:keys [findings]} (clj-kondo/run! {:lint [consumer]
                                                :config-dir config-dir :cache-dir cache-dir})
            actual (set (map (juxt :row :type) findings))]
        (if (= expected-findings actual)
          (println "clj-kondo export OK")
          (throw (ex-info "Unexpected clj-kondo findings"
                          {:expected expected-findings :actual actual :findings findings}))))
      (finally
        (fs/delete-tree dir)))))
