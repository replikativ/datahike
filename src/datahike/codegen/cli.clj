(ns datahike.codegen.cli
  "Generate CLI commands from API specification.

   This namespace provides CLI-specific configuration and logic for:
   - Deriving command names from operation categories
   - Validating arguments with malli
   - Generating hierarchical help text
   - Handling stdin and file inputs"
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [datahike.api.specification :refer [api-specification malli-schema->argslist]]
            [datahike.api.types :as types]
            [datahike.api :as d]
            [datahike.json :as json]
            [jsonista.core :as j]
            [clj-cbor.core :as cbor]
            [malli.core :as m]
            [malli.error :as me])
  (:import [java.util Date]))

;; =============================================================================
;; CLI-Specific Configuration
;; =============================================================================

(def cli-config
  "CLI-specific configuration that extends the universal API specification.

   Keys:
   - :stdin? - Whether this operation accepts stdin input
   - :stdin-arg - Which argument position (0-indexed) accepts stdin
   - :file-arg - Which argument position supports --file option
   - :batch-size - For streaming operations, how many items per batch"

  {'transact {:stdin? true
              :stdin-arg 1  ; tx-data argument
              :file-arg 1
              :batch-size 1000}

   'load-entities {:stdin? true
                   :stdin-arg 1  ; entities argument
                   :batch-size 5000}})

(defn cli-spec
  "Get merged specification with CLI-specific config."
  [op-name]
  (merge (get api-specification op-name)
         (get cli-config op-name)))

;; =============================================================================
;; Command Name Derivation
;; =============================================================================

(defn ->cli-command
  "Derive CLI command from operation name and categories.
   Returns a vector of command parts: [group? subcommand]

   Examples:
     'database-exists? [:database ...] => [\"db\" \"exists\"]
     'transact [:transaction ...] => [\"tx\" \"transact\"]
     'q [:query ...] => [\"query\"]"
  [op-name categories]
  (let [primary-category (first categories)
        op-str (-> (name op-name)
                   (str/replace #"[?!]$" ""))]  ; Remove ? and !
    (case primary-category
      :database     ["db" (str/replace op-str #"^database-" "")]
      :transaction  ["tx" op-str]
      :query        [op-str]  ; Flat for queries
      :index        ["index" op-str]
      :pull         [op-str]  ; Flat
      :lifecycle    ["db" op-str]
      :schema       ["schema" op-str]
      :advanced     ["advanced" op-str]
      ;; Default: flat command
      [op-str])))

(defn command->string
  "Convert command vector to string: [\"db\" \"exists\"] => \"db exists\""
  [cmd-vec]
  (str/join " " cmd-vec))

(defn build-command-index
  "Build a lookup map from command string to operation name.

   Returns: {\"db exists\" 'database-exists?, \"tx transact\" 'transact, ...}"
  []
  (into {}
        (for [[op-name {:keys [categories]}] api-specification
              :let [cmd (->cli-command op-name categories)]]
          [(command->string cmd) op-name])))

;; =============================================================================
;; Schema to Help Text
;; =============================================================================

(def type-descriptions
  "Human-readable descriptions for common malli types."
  {:datahike/SConfig "database configuration (map or file path)"
   :datahike/SConnection "database connection"
   :datahike/SDB "database value"
   :datahike/SEId "entity identifier (number, keyword, or lookup ref)"
   :datahike/STransactions "transaction data (vector of maps)"
   :datahike/STransactionReport "transaction report map"
   :datahike/SQueryArgs "query arguments map"
   :datahike/SPullOptions "pull pattern options"
   :datahike/SDatoms "sequence of datoms"
   :datahike/SSchema "database schema map"
   :datahike/SMetrics "database metrics map"
   :string "string"
   :int "integer"
   :boolean "boolean"
   :keyword "keyword"
   :map "map"
   :vector "vector"
   :any "any value"})

(defn schema->help-text
  "Convert a malli schema to human-readable help text.

   Examples:
     :datahike/SConfig => \"config\"
     [:tuple :datahike/SConfig] => \"config\"
     [:tuple :datahike/SConnection :datahike/STransactions] => \"connection, transactions\""
  [schema]
  (cond
    ;; Keyword reference - look up description
    (keyword? schema)
    (or (type-descriptions schema)
        (name schema))

    ;; Tuple - list the args
    (and (vector? schema) (= :tuple (first schema)))
    (str/join ", " (map #(or (type-descriptions %) (str %)) (rest schema)))

    ;; :or - show alternatives
    (and (vector? schema) (= :or (first schema)))
    (str/join " or " (map schema->help-text (rest schema)))

    ;; :=> function schema - extract input
    (and (vector? schema) (= :=> (first schema)))
    (let [[_ input-schema _] schema]
      (schema->help-text input-schema))

    ;; :cat - sequential args
    (and (vector? schema) (= :cat (first schema)))
    (str/join ", " (map schema->help-text (rest schema)))

    ;; :function multi-arity - show first arity
    (and (vector? schema) (= :function (first schema)))
    (schema->help-text (second schema))

    ;; [:sequential :any] or similar
    (and (vector? schema) (= :sequential (first schema)))
    "sequence"

    ;; Default
    :else (str schema)))

(defn args-help
  "Generate argument help text for an operation."
  [args-schema]
  (schema->help-text args-schema))

;; =============================================================================
;; Help Text Generation
;; =============================================================================

(defn first-sentence
  "Extract first sentence from docstring."
  [doc]
  (-> doc
      (str/split #"\.\s")
      first
      (str ".")))

(defn group-by-category
  "Group operations by their primary category.
   Returns: {category-keyword [op-names...]}"
  []
  (reduce
   (fn [acc [op-name {:keys [categories]}]]
     (let [primary-cat (first categories)]
       (update acc primary-cat (fnil conj []) op-name)))
   {}
   api-specification))

(def category-titles
  "Human-readable titles for categories."
  {:database "Database Operations"
   :transaction "Transaction Operations"
   :query "Query Operations"
   :pull "Pull Operations"
   :index "Index Operations"
   :lifecycle "Lifecycle Operations"
   :schema "Schema Operations"
   :advanced "Advanced Operations"
   :write "Write Operations"
   :read "Read Operations"
   :diagnostics "Diagnostics"})

(def category-order
  "Order for displaying categories in help."
  [:database :transaction :query :pull :schema :index :advanced])

(defn generate-help
  "Generate hierarchical help text from API specification."
  [options-summary]
  (let [grouped (group-by-category)
        banner "▁▃▅▄▇▅▃▅▃▁  \033[1mdata\033[0mhike cli"
        version (or (try (some-> (clojure.java.io/resource "DATAHIKE_VERSION")
                                 slurp
                                 str/trim)
                         (catch Exception _ nil))
                    "development")]
    (str/join
     "\n"
     (concat
      [banner
       (str "version " version)
       ""
       "This is the Datahike command line interface."
       ""
       "The commands reflect the datahike.api Clojure API."
       "Use db:config_file for database values, conn:config_file for connections."
       ""
       "Usage: dthk [options] <command> [arguments]"
       ""
       "Options:"
       options-summary
       ""
       "Commands:"
       ""]

      ;; Generate hierarchical command listing
      (for [category category-order
            :when (contains? grouped category)]
        (concat
         [(str "  " (get category-titles category category) ":")]
         (for [op-name (get grouped category)
               :let [{:keys [args doc]} (cli-spec op-name)
                     cmd (->cli-command op-name (list category))
                     cmd-str (command->string cmd)
                     args-str (args-help args)]]
           (format "    %-25s %s"
                   (str cmd-str " " args-str)
                   (first-sentence doc)))
         [""]))

      ["Remote Databases:"
       "  Use :writer or :remote-peer in your config file to connect via HTTP."
       "  Example config with :writer for distributed writes:"
       "    {:store {:backend :file :path \"/shared/db\"}"
       "     :writer {:backend :datahike-server :url \"http://localhost:4444\" :token \"xyz\"}}"
       "  See doc/distributed.md for details."
       ""
       "For more information on a specific command, use: dthk help <command>"]))))

;; =============================================================================
;; Argument Parsing and Validation
;; =============================================================================

(def input-parsers
  "Map of regex patterns to parser functions for handling prefixed inputs.

   Patterns:
   - conn:file.edn       => Connect and return connection
   - db:file.edn         => Connect and return database value
   - history:file.edn    => Return history database
   - since:timestamp:file.edn => Return database since timestamp
   - asof:timestamp:file.edn  => Return database as-of timestamp
   - edn:file.edn        => Read EDN from file
   - json:file.edn       => Read JSON from file
   - cbor:file.cbor      => Read CBOR from file"
  {#"conn:(.+)"       #(d/connect (edn/read-string (slurp %)))
   #"db:(.+)"         #(deref (d/connect (edn/read-string (slurp %))))
   #"history:(.+)"    #(d/history @(d/connect (edn/read-string (slurp %))))
   #"since:(.+):(.+)" #(d/since @(d/connect (edn/read-string (slurp %2)))
                                (Date. ^Long (edn/read-string %1)))
   #"asof:(.+):(.+)"  #(d/as-of @(d/connect (edn/read-string (slurp %2)))
                                (Date. ^Long (edn/read-string %1)))
   #"cbor:(.+)"       #(cbor/decode (io/input-stream %))
   #"edn:(.+)"        (comp edn/read-string slurp)
   #"json:(.+)"       (comp #(j/read-value % json/mapper) slurp)})

(defn parse-prefix
  "Parse input prefixes like conn:file.edn, db:file.edn, etc.
   Returns the parsed value or tries to read as EDN if no prefix matches.

   Examples:
     \"conn:config.edn\"  => (d/connect config)
     \"db:config.edn\"    => @(d/connect config)
     \"{:x 1}\"           => {:x 1}"
  [s]
  (if-not (string? s)
    s  ; Already parsed, return as-is
    (if-let [res (reduce (fn [_ [pattern parser-fn]]
                           (let [match (re-matches pattern s)]
                             (when (first match)
                               (reduced (apply parser-fn (rest match))))))
                         nil
                         input-parsers)]
      res
      ;; No prefix matched - try to parse as EDN
      (try
        (edn/read-string s)
        (catch Exception _
          ;; If EDN parsing fails, return as string
          s)))))

(defn validate-args
  "Validate arguments against malli schema.
   Returns {:ok parsed-args} or {:error explanation}"
  [schema args]
  (let [validated (m/validate schema args {:registry types/registry})]
    (if validated
      {:ok args}
      {:error (me/humanize
               (m/explain schema args {:registry types/registry}))})))

(defn try-arities
  "Try multiple arities in order until one validates.
   For multi-arity functions, returns {:ok args arity-index} or {:error last-error}"
  [function-schema args]
  (if (and (vector? function-schema) (= :function (first function-schema)))
    ;; Multi-arity: try each arity schema
    (let [arity-schemas (rest function-schema)]
      (loop [schemas arity-schemas
             idx 0]
        (if (empty? schemas)
          {:error "No arity matched the provided arguments"}
          (let [arity-schema (first schemas)
                [_ input-schema _] arity-schema
                result (validate-args input-schema args)]
            (if (:ok result)
              (assoc result :arity idx)
              (recur (rest schemas) (inc idx)))))))
    ;; Single arity
    (validate-args function-schema args)))

(defn format-validation-error
  "Format malli validation error for CLI users."
  [op-name schema args error]
  (let [{:keys [doc examples]} (cli-spec op-name)
        example (first examples)]
    (str "❌ Invalid arguments for '" op-name "':\n\n"
         "  Expected: " (args-help schema) "\n"
         "  Got:      " (pr-str args) "\n\n"
         "  Error:    " (pr-str error) "\n"
         (when example
           (str "\n  Example:  " (:code example) "\n")))))

;; =============================================================================
;; Command Dispatch
;; =============================================================================

(defn dispatch-command
  "Parse and dispatch a CLI command.

   Args:
     cmd-parts - Command parts from CLI, e.g. [\"db\" \"exists\"]
     raw-args - Raw argument strings from CLI
     cli-opts - Parsed CLI options (format, verbosity, etc.)

   Returns the result of calling the operation, or exits on error."
  [cmd-parts raw-args cli-opts]
  (let [cmd-str (str/join " " cmd-parts)
        command-index (build-command-index)
        op-name (get command-index cmd-str)]

    (if-not op-name
      (do
        (println "❌ Unknown command:" cmd-str)
        (println "\nUse 'dthk --help' to see available commands.")
        (System/exit 1))

      (let [{:keys [args impl]} (cli-spec op-name)
            ;; Parse arguments (handle prefixes, files, etc.)
            parsed-args (mapv parse-prefix raw-args)
            ;; Validate with malli
            validation (try-arities args parsed-args)]

        (if (:ok validation)
          ;; Success - call implementation
          (apply impl parsed-args)

          ;; Validation failed - show error
          (do
            (println (format-validation-error op-name args parsed-args (:error validation)))
            (System/exit 1)))))))
