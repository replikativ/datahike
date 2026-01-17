(ns datahike.cli
  (:gen-class)
  (:require [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [clojure.tools.cli :refer [parse-opts]]
            [datahike.api :as d]
            [datahike.pod :refer [run-pod]]
            [datahike.codegen.cli :as cli-gen]
            [clojure.edn :as edn]
            [jsonista.core :as j]
            [datahike.json :as json]
            [clj-cbor.core :as cbor]
            [taoensso.timbre :as log])
  (:import [java.util Date]))

;; This file is following https://github.com/clojure/tools.cli

(when-not (= "true" (System/getenv "BABASHKA_POD"))
  (log/merge-config!
   {:appenders {:println {:enabled? false} ;; leave a "paper trail"
                :stderr {:doc "Always prints to *err*"
                         :enabled? true
                         :fn (fn log-to-stderr [{:keys [output_]}]
                               (binding [*out* *err*]
                                 (println (force output_))))}}}))

(defn get-version []
  (or
   ;; Try to read version from resource file (generated at build time)
   (try
     (some-> (io/resource "DATAHIKE_VERSION")
             slurp
             str/trim)
     (catch Exception _ nil))
   ;; Fallback for development builds
   "development build"))

(defn usage [options-summary]
  (cli-gen/generate-help options-summary))

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (str/join \newline errors)))

(def cli-options
  ;; An option with a required argument
  (let [formats #{:json :edn :pprint :cbor}]
    [["-f" "--format FORMAT" "Output format for the result."
      :default :edn
      :parse-fn keyword
      :validate [formats (str "Must be one of: " (str/join ", " formats))]]
     ["-if" "--input-format FORMAT" "Input format for the transaction."
      :default :edn
      :parse-fn keyword
      :validate [formats (str "Must be one of: " (str/join ", " formats))]]
     [nil "--tx-file PATH" "Use this input file for transactions instead of command line or STDIN."
      :default nil
      :validate [#(.exists (io/file %)) "Transaction file does not exist."]]
     ;; A non-idempotent option (:default is applied first)
     ["-v" nil "Verbosity level"
      :id :verbosity
      :default 0
      :update-fn inc]
     ;; A boolean option defaulting to nil
     ["-h" "--help"]
     [nil "--version" "Show version"]]))

(defn validate-args
  "Validate command line arguments. Either return a map indicating the program
  should exit (with a error message, and optional ok status), or a map
  indicating the command the program should execute and the options provided."
  [args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)
        pod? (= "true" (System/getenv "BABASHKA_POD"))
        command-index (cli-gen/build-command-index)]
    (cond
      pod?
      {:action :pod :options options}

      (:help options) ; help => exit OK with usage summary
      {:exit-message (usage summary) :ok? true :options options}

      (:version options) ; version => exit OK with version string
      {:exit-message (str "Datahike " (get-version)) :ok? true :options options}

      errors          ; errors => exit with description of errors
      {:exit-message (error-msg errors) :options options}

      ;; Try to match command (could be multi-part like "db exists")
      (seq arguments)
      (let [;; Try 2-part command first (e.g., "db exists")
            two-part (str/join " " (take 2 arguments))
            one-part (first arguments)

            [cmd-str remaining-args]
            (cond
              (get command-index two-part)
              [two-part (drop 2 arguments)]

              (get command-index one-part)
              [one-part (rest arguments)]

              :else
              [nil arguments])]

        (if cmd-str
          {:command cmd-str
           :options options
           :arguments (vec remaining-args)}
          {:exit-message (str "Unknown command: " (str/join " " (take 2 arguments))
                              "\n\nUse 'dthk --help' to see available commands.")
           :options options}))

      :else           ; No arguments
      {:exit-message (usage summary)
       :options options})))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn report [format out]
  (case format
    :json        (println (j/write-value-as-string out))
    :edn         (println (pr-str out))
    :pprint      (pprint out)
    :cbor        (.write System/out ^bytes (cbor/encode out))))

(defn -main [& args]
  (let [{:keys [action command options arguments exit-message ok?]}
        (validate-args args)]
    ;; Set logging level
    (case (int (:verbosity options))
      0 (log/set-level! :warn)
      1 (log/set-level! :info)
      2 (log/set-level! :debug)
      3 (log/set-level! :trace)
      (exit 1 (str "Verbosity level not supported: " (:verbosity options))))

    (cond
      ;; Exit with message
      exit-message
      (exit (if ok? 0 1) exit-message)

      ;; Babashka pod mode
      (= action :pod)
      (run-pod args)

      ;; Execute command via generated dispatch
      command
      (try
        (let [result (cli-gen/dispatch-command
                      (str/split command #"\s+")
                      arguments
                      options)]
          (report (:format options) result))
        (catch Exception e
          (println "❌ Error executing command:" (.getMessage e))
          (when (>= (:verbosity options) 2)
            (.printStackTrace e))
          (exit 1 "")))

      ;; No command provided
      :else
      (exit 1 (usage (:summary (parse-opts args cli-options)))))))