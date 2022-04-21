(ns dependencies-consistency-check
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.pprint :as pp]
            [clojure.set :as set]
            [clojure.string :as string]
            [clojure.tools.cli :as cli]))

(defn read-edn [filepath]
  (edn/read (java.io.PushbackReader. (io/reader filepath))))

(def project-aliases (:aliases (read-edn "deps.edn")))

(def cli-options
  [;; Alias(es) to include in check
   ["-a" "--alias ALIAS" "Alias (without \":\", which will be prefixed onto argument)"
    :parse-fn #(keyword %)
    :multi true
    :update-fn conj
    :validate [#(% project-aliases) #(str "Invalid alias: " % " is undeclared in deps.edn")]]
   ["-A" "--aliases-only" "Exclude direct (release) dependencies"]
   ["-o" "--output FILEPATH" "Filepath to which output should be written"
    :default nil]
   ["-h" "--help"]])

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn usage [summary]
  (string/join "\n" ["Usage: clj -M:deps-consistency [options]\n" "Options:" summary]))

(defn read-trace [trace-file]
  (read-edn trace-file))

(defn create-trace-file [alias filename]
  (sh/sh "clj" (str "-A" (str alias)) "-Strace")
  (when (not= "trace.edn" filename) (sh/sh "mv" "trace.edn" filename)))

(defn get-selected-versions [trace]
  (let [vmap (:vmap trace)]
    (zipmap (keys vmap) (map :select (vals vmap)))))

(defn map-build-pkg-vns [alias-filenames]
  (zipmap (keys alias-filenames)
          (map #(get-selected-versions (read-trace %)) (vals alias-filenames))))

(defn map-pkg-build-vns [build-pkg-vns]
  (let [all-pkgs (apply set/union (map keys (vals build-pkg-vns)))]
    (persistent! (reduce-kv (fn [outer-map build pkg-vns]
                              (reduce-kv (fn [build-map pkg vn]
                                           (assoc! build-map pkg (assoc (pkg build-map) build vn)))
                                         outer-map pkg-vns))
                            (transient (zipmap all-pkgs (repeat {}))) build-pkg-vns))))

(defn get-inconsistent-pkgs [pkg-build-versions]
  (persistent! (reduce-kv (fn [m pkg build-vns]
                            (if (> (count (distinct (vals build-vns))) 1)
                              (assoc! m pkg build-vns) m))
                          (transient {}) pkg-build-versions)))

(defn -main [& args]
  (let [{:keys [options errors summary] :as parsed-opts} (cli/parse-opts args cli-options)
        {:keys [alias aliases-only output]} options
        tmpdir (System/getProperty "java.io.tmpdir")
        trace-filenames #(str tmpdir "trace-" (name %) ".edn")
        alias-filenames (cond-> (zipmap alias (map trace-filenames alias))
                          (nil? aliases-only) (assoc :release (str tmpdir "trace.edn")))]
    (cond
      (:help options)
      (exit 0 (usage summary))

      errors
      (exit 1 (string/join "\n" ["Errors:" (string/join "\n" errors) "" (usage summary)]))

      :else
      (do (doseq [[alias filename] alias-filenames] (create-trace-file alias filename))
          (pp/pprint (-> alias-filenames
                         map-build-pkg-vns
                         map-pkg-build-vns
                         get-inconsistent-pkgs)
                     (if output (io/writer output) *out*))
          (apply sh/sh "rm" (vals alias-filenames))))))
