(ns benchmark.cli
  (:require [clojure.tools.cli :as cli]
            [benchmark.measure :refer [get-measurements time-statistics]]
            [benchmark.compare :refer [compare-benchmarks]]
            [benchmark.config :as c]
            [benchmark.store :refer [save]]
            [clojure.string :refer [join]]
            [datahike.store :as ds]
            [datahike.config :as dc]
            [datahike.index :as di]))

(def output-formats (set (keys (methods save))))
(def config-names (set (map :config-name c/named-db-configs)))
(def implemented-queries #{:simple-query
                           :e-join-query :e-join-query-first-fixed :e-join-query-second-fixed
                           :a-join-query
                           :v-join-query
                           :equals-query :equals-query-1-fixed
                           :less-than-query :less-than-query-1-fixed
                           :scalar-arg-query :scalar-arg-query-with-join
                           :vector-arg-query
                           :limit-query
                           :stddev-query :variance-query :max-query :median-query :avg-query})
(def implemented-functions #{:connection :transaction :query})
(def implemented-data-types #{:int :str})

(def index-names (-> (methods di/default-index-config) keys set (disj :default)))
(def backend-names (-> (methods ds/default-config) keys set (disj :default)))
(def time-stats (-> (time-statistics [0]) keys set))

(def cli-options                                            ;; TODO: use default values from datahike.config when available
  [;; CMD run
   ["-u" "--db-server-url URL" "Base URL for datahike server, e.g. http://localhost:3000"
    :default nil]
   ["-n" "--db-name DBNAME" "Database name for datahike server" :default nil]
   ["-g" "--db-token TOKEN" "Token for datahike server" :default nil]

   ["-t" "--tag TAG" "Add tag to measurements"
    :default #{}
    :assoc-fn (fn [m k v] (assoc m k (conj (get m k) v)))]
   ["-o" "--output-format FORMAT"
    (str "Determines how the results will be processed. "
         "Available are " output-formats " "
         "Currently only edn format is supported for comparisons.")
    :default "edn"
    :validate [output-formats  #(str "Format " % " has not been implemented. "
                                     "Available formats are " output-formats)]]
   ["-e" "--statistics VECTOR"
    (str "Statistic columns shown in the printed comparison summary. "
         "Available are: " time-stats)
    :default [:median :mean :std]
    :parse-fn read-string
    :validate [vector? (str "Must be a vector of cell names. Available are: " time-stats)
               #(every? time-stats %) #(str "Must be a vector of cell names. Available are: " time-stats)]]

   ["-c" "--config-name CONFIGNAME"
    (str "Name of preset database configuration to use. Available are: " config-names)
    :validate [config-names  #(str "A configuration named " % " has not been implemented. "
                                   "Available configurations are: " config-names)]]

   ["-a" "--index INDEXNAME"
    (str "Name of index to use. Available are " index-names)
    :default :all
    :parse-fn read-string
    :validate [index-names #(str "An index named " % " has not been implemented. "
                                 "Available indices are: " index-names)]]
   ["-l" "--history BOOLEAN"
    (str "Boolean indication if measured databases use temporal indices")
    :default false
    :parse-fn read-string
    :validate [#(contains? #{true false :all} %) "Must be a boolean value or keyword :all"]]
   ["-b" "--backend BACKEND"
    (str "Keyword for backend that is going to be used")
    :default :all
    :parse-fn read-string
    :validate [(conj backend-names :all) #(str "A backend named " % " has not been implemented. "
                                               "Available backends are: " backend-names)]]
   ["-k" "--search-caches SIZES"
    (str "Search cache sizes for which measurements should be done")
    :default [dc/*default-search-cache-size*]
    :parse-fn read-string
    :validate [vector? "Must be a vector of non-negative integers."
               #(every? nat-int? %) "Vector must consist of non-negative integers."]]
   ["-m" "--store-caches SIZES"
    (str "Store cache sizes for which measurements should be done")
    :default [dc/*default-store-cache-size*]
    :parse-fn read-string
    :validate [vector? "Must be a vector of positive integers."
               #(every? pos-int? %) "Vector must consist of non-negative integers."]]
   ["-j" "--schema VALUE"
    (str "Schema flexibility configuration. Available are: " #{:read :write})
    :default :write
    :parse-fn read-string
    :validate [#{:read :write :all} #(str "A schema-flexibility named " % " has not been implemented. "
                                          "Available values are: " #{:read :write})]]

   ["-d" "--db-entity-counts VECTOR"
    (str "Numbers of entities in database for which benchmarks should be run. "
         "Must be given as a clojure vector of non-negative integers like '[0 10 100 1000]'.")
    :default [0 1000]
    :parse-fn read-string
    :validate [vector? "Must be a vector of non-negative integers."
               #(every? nat-int? %) "Vector must consist of non-negative integers."]]
   ["-x" "--tx-entity-counts VECTOR"
    (str  "Numbers of entities in transaction for which benchmarks should be run.  "
          "Must be given as a clojure vector of non-negative integers like '[0 10 100 1000]'.")
    :default [0 1000]
    :parse-fn read-string
    :validate [vector? "Must be a vector of non-negative integers."
               #(every? nat-int? %) "Vector must consist of non-negative integers."]]
   ["-y" "--data-types VECTOR"
    (str "Vector of datatypes to test queries on. Available are: " implemented-data-types ".")
    :default [:int :str]
    :parse-fn read-string
    :validate [vector? (str "Must be a vector of data type keywords. Available are " implemented-data-types)
               #(every? implemented-data-types %) (str "Vector must consist of keywords for datatypes. "
                                                       "Available are: " implemented-data-types)]]
   ["-z" "--data-found-opts BOOLEAN"
    (str "Boolean indicating if query is run for existent or nonexistent values in the database.")
    :default true
    :parse-fn read-string
    :validate [#{true false :all} "Must be a boolean value or keyword :all."]]
   ["-i" "--iterations ITERATIONS"
    (str "Number of measurements for each setting taken on the same database.")
    :default 10
    :parse-fn read-string
    :validate [nat-int? "Must be a non-negative integer."]]
   ["-s" "--db-samples DB-SAMPLES"
    (str "Number of database instances on which the full measurements are run.")
    :default 1
    :parse-fn read-string
    :validate [nat-int? "Must be a non-negative integer."]]
   ["-q" "--query QUERYNAME"
    (str "Name of query to test. Available are " implemented-queries)
    :default :all
    :parse-fn read-string
    :validate [(conj implemented-queries :all) (str "Must be a keyword for a implemented database query "
                                                    "Available are: " implemented-queries)]]
   ["-f" "--function FUNCTIONNAME"
    (str "Name of function to test. Available are " implemented-functions)
    :default :all
    :parse-fn read-string
    :validate [(conj implemented-functions :all) (str "Must be a keyword for implemented database function. "
                                                      "Available are: " implemented-functions)]]

   ;; CMD compare

   ["-p" "--[no-]plots" "Output comparison as plots."]

   ["-h" "--help"]])


(defn print-usage-info [summary]
  (println (str "Usage: clj -M:benchmark CMD [OPTIONS] [FILEPATHS] \n\n  Options for command 'run':\n" summary)))

(defmulti run-command (fn [cmd _paths _options] cmd))

(defmethod run-command :default [cmd _paths _options]
  (throw (Exception. (str "Command '" cmd "' does not exist. Valid commands are 'measure' and 'compare'"))))

(defmethod run-command "compare" [_cmd paths {:keys [plots statistics]}]
  (compare-benchmarks paths plots statistics))


(defmethod run-command "measure" [_cmd paths {:keys [tag output-format] :as options}]
  (let [server-description (select-keys options [:db-server-url :db-token :db-name])]
    (if (and (= "remote-db" output-format)
             (some nil? (vals server-description)))
      (do (println (str "Only partial information for remote connection has been given: " server-description))
          (println "Please, define URL, database name, and token to save the data on a remote datahike server."))
      (let [measurements (cond->> (get-measurements options)
                                  (seq tag) (map (fn [entity] (assoc entity :tag (join " " tag))))
                                  true      vec) ]
    (save options paths measurements)))))

(defn -main [& args]
  (let [{:keys [arguments options errors summary]} (cli/parse-opts args cli-options)
        [cmd & paths] arguments]
    (cond
      (some? errors)
      (do (println "Errors:" errors)
          (print-usage-info summary))

      (:help options)
      (print-usage-info summary)

      :else (run-command cmd paths options)))

  (shutdown-agents))

(comment
  (-main "run" "-x" "[0 10000 5000]" "-t" "test-bench" "-o" "edn" "bench.edn")
  )

;TIMBRE_LEVEL=":info" clj -M:benchmark run --backend :file --index :datahike.index/persistent-set -t pss -o edn pss.edn --schema :write --history false
