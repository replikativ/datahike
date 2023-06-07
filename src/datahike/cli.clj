(ns datahike.cli
  (:gen-class)
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [clojure.tools.cli :refer [parse-opts]]
            [datahike.api :as d]
            [datahike.pod :refer [run-pod]]
            [clojure.edn :as edn]
            [cheshire.core :as ch]
            [clj-cbor.core :as cbor]
            [taoensso.timbre :as log])
  (:import [java.util Date]))

;; This file is following https://github.com/clojure/tools.cli

(defn usage [options-summary]
  (->> ["This is the Datahike command line interface."
        ""
        "The commands mostly reflect the datahike.api Clojure API. You can find its documentation under api at https://cljdoc.org/d/io.replikativ/datahike/. To instantiate a specific database, you can use db:config_file to access the current database value, conn:config_file to create a mutable connection for manipulation, history:config_file for the historical database over all transactions, since:unix_time_in_ms:config_file to create a database with all facts since the time provided and asof:unix_time_in_ms:config_file to create an snapshot as-of the time provided. To pass in edn data use edn:edn_file and for JSON use json:json_file."
        ""
        "Usage: dhi [options] action arguments"
        ""
        "Options:"
        options-summary
        ""
        "Actions:"
        "  create-database         Create database for a provided configuration file, e.g. create-database config_file"
        "  delete-database         Delete database for a provided configuration file, e.g. delete-database config_file"
        "  database-exists         Check whether database exists for a provided configuration file, e.g. database-exists config_file"
        "  transact                Transact transactions, optionally from a file with --tx-file or from STDIN. Exampe: transact conn:config_file \"[{:name \"Peter\" :age 42}]\""
        "  query                   Query the database, e.g. query '[:find (count ?e) . :where [?e :name _]]' db:mydb.edn. You can pass an arbitrary number of data sources to the query."
        "  benchmark               Benchmarks write performance. The arguments are starting eid, ending eid and the batch partitioning of the added synthetic Datoms. The Datoms have the form [eid :name ?randomly-sampled-name]"
        "  pull                    Pull data in a map syntax for a specific entity: pull db:mydb.edn \"[:db/id, :name]\" \"1\"."
        "  pull-many               Pull data in a map syntax for a list of entities: pull db:mydb.edn \"[:db/id, :name]\" \"[1,2]\""
        "  entity                  Fetch entity: entity db:mydb.edn \"1\""
        "  datoms                  Fetch all datoms from an index: datoms db:mydb.edn  \"{:index :eavt :components [1]}\" "
        "  schema                  Fetch schema for a db."
        "  reverse-schema          Fetch reverse schema for a db."
        "  metrics                 Fetch metrics for a db."
        ""
        "Please refer to the manual page for more information."]
       (str/join \newline)))

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (str/join \newline errors)))

(def actions #{"create-database" "delete-database" "database-exists" "transact" "query" "benchmark"
               "pull" "pull-many" "entity" "datoms" "schema" "reverse-schema" "metrics"})

(def cli-options
  ;; An option with a required argument
  (let [formats #{:json :edn :pretty-json :pprint :cbor}]
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
     ["-h" "--help"]]))

(defn validate-args
  "Validate command line arguments. Either return a map indicating the program
  should exit (with a error message, and optional ok status), or a map
  indicating the action the program should take and the options provided."
  [args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)
        pod? (= "true" (System/getenv "BABASHKA_POD"))]
    (cond
      pod?
      {:action :pod :options options}

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

;; format: optional first argument Unix time in ms for history, last file for db
(def input->db
  {#"conn:(.+)"       #(d/connect (edn/read-string (slurp %)))
   #"db:(.+)"         #(deref (d/connect (edn/read-string (slurp %))))
   #"history:(.+)"    #(d/history @(d/connect (edn/read-string (slurp %))))
   #"since:(.+):(.+)" #(d/since @(d/connect (edn/read-string (slurp %2)))
                                (Date. ^Long (edn/read-string %1)))
   #"asof:(.+):(.+)"  #(d/as-of @(d/connect (edn/read-string (slurp %2)))
                                (Date. ^Long (edn/read-string %1)))
   #"cbor:(.+)"       #(cbor/decode (io/input-stream %))
   #"edn:(.+)"        (comp edn/read-string slurp)
   #"json:(.+)"       (comp #(ch/parse-string % keyword) slurp)})

(defn load-input [s]
  (if-let [res
           (reduce (fn [_ [p f]]
                     (let [m (re-matches p s)]
                       (when (first m)
                         (reduced (apply f (rest m))))))
                   nil
                   input->db)]
    res
    (throw (ex-info "Input format not know." {:type  :input-format-not-known
                                              :input s}))))

(defn report [format out]
  (case format
    :json        (println (json/json-str out))
    :pretty-json (json/pprint out)
    :edn         (println (pr-str out))
    :pprint      (pprint out)
    :cbor        (.write System/out ^bytes (cbor/encode out))))

(defn -main [& args]
  (let [{:keys [action options arguments exit-message ok?]}
        (validate-args args)]
    (case (int (:verbosity options))
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
        :pod
        (run-pod args)

        :create-database
        (report (:format options)
                (d/create-database (read-string (slurp (first arguments)))))

        :delete-database
        (report (:format options)
                (d/delete-database (read-string (slurp (first arguments)))))

        :database-exists
        (report (:format options)
                (d/database-exists? (read-string (slurp (first arguments)))))

        :transact
        (report (:format options)
                (:tx-meta
                 (d/transact (load-input (first arguments))
                             (vec ;; TODO support set inputs for transact
                              (if-let [tf (:tx-file options)]
                                (load-input tf)
                                (if-let [s (second arguments)]
                                  (case (:input-format options)
                                    :edn (edn/read-string s)
                                    :pprint (edn/read-string s)
                                    :json (ch/parse-string s keyword)
                                    :pretty-json (ch/parse-string s keyword)
                                    :cbor (cbor/decode s) ;; does this really make sense?
                                    )
                                  (case (:input-format options)
                                    :edn (edn/read)
                                    :pprint (edn/read)
                                    :json (ch/decode-stream *in* keyword)
                                    :pretty-json :json (ch/decode-stream *in*)
                                    :cbor (cbor/decode *in*))))))))

        :benchmark
        (let [conn (load-input (first arguments))
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
             (d/transact conn txs))))

        :query
        (let [q-args (mapv #(load-input %) (rest arguments))
              out (apply d/q (read-string (first arguments))
                         q-args)]
          (report (:format options) out))

        :pull
        (let [out (into {} (d/pull (load-input (first arguments))
                                   (read-string (second arguments))
                                   (read-string (nth arguments 2))))]
          (report (:format options) out))

        :pull-many
        (let [out (mapv #(into {} %)
                        (d/pull-many (load-input (first arguments))
                                     (read-string (second arguments))
                                     (read-string (nth arguments 2))))]
          (report (:format options) out))

        :entity
        (let [out (into {} (d/entity (load-input (first arguments))
                                     (read-string (second arguments))))]
          (report (:format options) out))

        :datoms
        (let [out (d/datoms (load-input (first arguments))
                            (read-string (second arguments)))]
          (report (:format options) out))

        :schema
        (let [out (d/schema (load-input (first arguments)))]
          (report (:format options) out))

        :reverse-schema
        (let [out (d/reverse-schema (load-input (first arguments)))]
          (report (:format options) out))

        :metrics
        (let [out (d/metrics (load-input (first arguments)))]
          (report (:format options) out))))))

(comment
  (spit "myconfig.edn" {:store {:backend :file,
                                :path "/tmp/dh-shared-db",
                                :config {:in-place? true}},
                        :keep-history? true,
                        :schema-flexibility :read})
  (-main "create-database" "myconfig.edn")
  (-main "transact" "conn:myconfig.edn" "[[:db/add -1 :name \"Linus\"]]")
  (-main "query" "[:find ?n . :where [?e :name ?n]]" "db:myconfig.edn")
  (-main "pull" "db:myconfig.edn" "[*]" "1")
  (-main "delete-database" "myconfig.edn"))
