(ns tools.deps
  (:require [babashka.fs :as fs]
            [babashka.process :as p]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [clojure.set :as set]
            [clojure.string :as string]
            [clojure.tools.cli :as cli]))

(def tmpdir (str (fs/temp-dir) "/clj_deps/"))

(defn read-edn [filepath]
  (edn/read-string (slurp filepath)))

(def project-aliases (:aliases (read-edn "deps.edn")))

(def cli-options
  [;; Alias(es) to include in check
   ["-a" "--alias ALIAS" "Alias (without \":\", which will be prefixed onto argument)"
    :parse-fn #(keyword %)
    :multi true
    :update-fn conj
    :validate [#(% project-aliases) #(str "Invalid alias: " % " is undeclared in deps.edn")]]
   ["-o" "--output FILEPATH" "Filepath to which output should be written"
    :default nil]
   ["-h" "--help"]])

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn usage [summary]
  (string/join "\n" ["Usage: bb deps [options]\n" "Options:" summary]))

(defn create-trace-file [alias filename]
  (p/shell "clojure" "-Strace" (str "-M:" alias))
  (when (not= "trace.edn" filename)
    (fs/move "trace.edn" filename)))

(defn get-selected-versions [trace]
  (update-vals (:vmap trace) :select))

(defn map-build-pkg-vns [alias-filenames]
  (update-vals alias-filenames #(get-selected-versions (read-edn %))))

(defn map-pkg-build-vns [build-pkg-vns]
  (let [all-pkgs (apply set/union (map keys (vals build-pkg-vns)))]
    (persistent! (reduce-kv (fn [outer-map build pkg-vns]
                              (reduce-kv (fn [build-map pkg vn]
                                           (assoc! build-map pkg (assoc (pkg build-map) build vn)))
                                         outer-map pkg-vns))
                            (transient (zipmap all-pkgs (repeat {})))
                            build-pkg-vns))))

(defn get-inconsistent-pkgs [pkg-build-versions]
  (persistent! (reduce-kv (fn [m pkg build-vns]
                            (if (> (count (distinct (vals build-vns))) 1)
                              (assoc! m pkg build-vns) m))
                          (transient {}) pkg-build-versions)))

(defn check [{:keys [output alias] :as _config}]
  (fs/delete-tree tmpdir)
  (fs/create-dirs tmpdir)
  (let [trace-filenames #(str tmpdir "trace-" (name %) ".edn")
        alias-filenames (zipmap alias (map trace-filenames alias))]
    
    (doseq [[alias filename] alias-filenames] (create-trace-file alias filename))
      (pp/pprint (-> alias-filenames
                     map-build-pkg-vns
                     map-pkg-build-vns
                     get-inconsistent-pkgs)
                 (if output (io/writer output) *out*))
      (run! fs/delete (vals alias-filenames))))

(defn -main [& args]
  (let [{:keys [options errors summary] :as _parsed-opts} (cli/parse-opts args cli-options)]
    (cond
      (:help options)
      (exit 0 (usage summary))

      errors
      (exit 1 (string/join "\n" ["Errors:" (string/join "\n" errors) "" (usage summary)]))

      :else 
      (check options))))
