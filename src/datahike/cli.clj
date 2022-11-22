(ns datahike.cli
  (:gen-class)
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [clojure.tools.cli :refer [parse-opts]]
            [datahike.api :as api]
            [taoensso.timbre :as log]))

;; This file is following https://github.com/clojure/tools.cli

(defn usage [options-summary]
  (->> ["This is the Datahike command line interface."
        ""
        "Usage: datahike [options] action arguments"
        ""
        "Options:"
        options-summary
        ""
        "Actions:"
        "  transact                Transact transactions in second argument into db configuration provided as path in first argument."
        "  query                   Query the database provided as first argument with provided query and an arbitrary number of arguments pointing to db configuration files or denoting values."
        "  benchmark               Benchmark transacts into db config provided by first argument. The following arguments are starting eid, ending eid and the batch partitioning of the added synthetic Datoms. The Datoms have the form [eid :name ?random-name]"
        ""
        "Please refer to the manual page for more information."]
       (str/join \newline)))

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (str/join \newline errors)))

(def actions #{"transact" "query" "benchmark"})

(def cli-options
  ;; An option with a required argument
  (let [formats #{:json :edn :pretty-json :pprint}]
    [["-f" "--format FORMAT" "Output format for the result."
      :default :edn
      :parse-fn keyword
      :validate [formats (str "Must be one of: " (str/join ", " formats))]]
     [nil "--tx-file PATH" "Use this input file for transactions."
      :default nil
      :validate [#(.exists (io/file %)) "Transaction file does not exist."]]
     ;; A non-idempotent option (:default is applied first)
     ["-v" nil "Verbosity level"
      :id :verbosity
      :default 0
      :update-fn inc]
     ;; A boolean option defaulting to nil
     ["-h" "--help"]]))

(defn validate-args
  "Validate command line arguments. Either return a map indicating the program
  should exit (with a error message, and optional ok status), or a map
  indicating the action the program should take and the options provided."
  [args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)]
    (cond
      (:help options) ; help => exit OK with usage summary
      {:exit-message (usage summary) :ok? true :options options}

      errors          ; errors => exit with description of errors
      {:exit-message (error-msg errors) :options options}

      (and (not= "transact" (first arguments))
           (:tx-file options))
      {:exit-message "The option --tx-file is only applicable to the transact action."
       :options options}

      (actions (first arguments))
      {:action (keyword (first arguments)) :options options
       :arguments (rest arguments)}

      (not (actions (first arguments)))
      {:exit-message (str "Unknown action, must be one of: "
                          (str/join ", " actions))
       :options options}

      :else           ; failed custom validation => exit with usage summary
      {:exit-message (usage summary)
       :options options})))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn read-config [^String config-path]
  (assert (.startsWith config-path "db:") "Database configuration path needs to start with \"db:\".")
  (read-string (slurp (subs config-path 3))))

(defn connect [cfg]
  (when-not (api/database-exists? cfg)
    (api/create-database cfg)
    (log/info "Created database:" (get-in cfg [:store :path])))
  (api/connect cfg))

(defn -main [& args]
  (let [{:keys [action options arguments exit-message ok?]}
        (validate-args args)]
    (case (:verbosity options)
      0 ;; default
      (log/set-level! :warn)
      1
      (log/set-level! :info)
      2
      (log/set-level! :debug)
      3
      (log/set-level! :trace)
      (exit 1 (str "Verbosity level not supported: " (:verbosity options))))

    (if exit-message
      (exit (if ok? 0 1) exit-message)
      (case action
        :transact
        (api/transact (connect (read-config (first arguments)))
                      (vec ;; TODO support set inputs for transact
                       (if-let [tf (:tx-file options)]
                         (read-string (slurp tf))
                         (if-let [s (second arguments)]
                           (read-string s)
                           (read)))))

        :benchmark
        (let [cfg (read-config (first arguments))
              conn (connect cfg)
              args (rest arguments)
              tx-data (vec (for [i (range (read-string (first args))
                                          (read-string (second args)))]
                             [:db/add (inc i)
                              :name (rand-nth ["Chrislain" "Christian"
                                               "Jiayi" "Judith"
                                               "Konrad" "Pablo"
                                               "Timo" "Wade"])]))]
          (doseq [txs (partition (read-string (nth args 2)) tx-data)]
            (time
             (api/transact conn txs))))

        :query
        (let [q-args (mapv
                      #(if (.startsWith ^String % "db:") ;; TODO better db denotation
                         @(connect (read-config %)) (read-string %))
                      (rest arguments))
              _ (when (pos? (:verbosity options))
                  (log/info "Parsed query arguments:" (pr-str q-args)))
              out (apply api/q (read-string (first arguments))
                         q-args)]
          (println
           (case (:format options)
             :json (json/json-str out)
             :pretty-json (with-out-str (json/pprint out))
             :edn (pr-str out)
             :pprint (with-out-str (pprint out)))))))))
