(ns benchmark.core
  (:require [clojure.tools.cli :as cli]
            [benchmark.measure :refer [get-measurements]]
            [benchmark.store :refer [transact-missing-schema transact-results ->RemoteDB]]
            [benchmark.compare :refer [compare-benchmarks]]
            [benchmark.config :as c]
            [clojure.string :refer [join]]
            [clojure.pprint :refer [pprint]]))

(def output-formats #{"remote-db" "edn" "csv"})
(def config-names #{"mem-set" "mem-hht" "file"})
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

(def cli-options
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
   ["-c" "--config-name CONFIGNAME"
    (str "Name of database configuration to use. Available are " config-names)
    :default :all
    :validate [config-names  #(str "A configuration named " % " has not been implemented. "
                                   "Available configurations are " config-names)]]
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
   ["-y" "--data-types TYPEVECTOR"
    (str "Vector of datatypes to test queries on. Available are " implemented-data-types)
    :default [:int :str]
    :parse-fn read-string
    :validate [vector? "Must be a vector of keywords."
               #(every? implemented-data-types %) (str "Vector must consist of keywords for datatypes. "
                                                       "Available are " implemented-data-types)]]
   ["-z" "--data-found-opts OPTS"
    (str "Vector of booleans indicating if query is run for existent or nonexistent values in the database.")
    :default :all
    :parse-fn read-string
    :validate [#{true false :all} "Must be a boolean value or keyword :all."]]
   ["-i" "--iterations ITERATIONS"
    (str "Number of iterations of each measurement.")
    :default 10
    :parse-fn read-string
    :validate [nat-int? "Must be a non-negative integer."]]
   ["-q" "--query QUERYNAME"
    (str "Name of query to test. Available are " implemented-queries)
    :default :all
    :parse-fn read-string
    :validate [implemented-queries (str "Must be a keyword for a implemented database query "
                                        "Available are: " implemented-queries)]]
   ["-f" "--function FUNCTIONNAME"
    (str "Name of function to test. Available are " implemented-functions)
    :default :all
    :parse-fn read-string
    :validate [implemented-functions (str "Must be a keyword for implemented database function. "
                                          "Available are: " implemented-functions)]]

   ;; CMD compare

   ["-p" "--[no-]plots" "Output comparison as plots."]

   ["-h" "--help"]])


(defn print-usage-info [summary]
  (println (str "Usage: clj -M:benchmark CMD [OPTIONS] [FILEPATHS] \n\n  Options for command 'run':\n" summary)))

(defn full-server-description? [server-description]
  (every? #(not (nil? %)) server-description))

(defn add-ns-to-keys [current-ns hmap]
  (reduce-kv (fn [m k v]
               (let [next-key (if (= current-ns "") k (keyword (str current-ns "/" (name k))))
                     next-ns (name k)
                     next-val (cond
                                (map? v)
                                (add-ns-to-keys next-ns v)

                                (and (vector? v) (map? (first v)))
                                (mapv (partial add-ns-to-keys next-ns) v)

                                :else v)]
                 (assoc m next-key next-val)))
             {}
             hmap))

(defn save-measurements-to-file [paths measurements]
  (let [filename (first paths)]
    (try
      (if (pos? (count paths))
        (do (spit filename measurements)
            (println "Measurements successfully saved to" filename))
        (println measurements))
      (catch Exception e
        (println measurements)
        (println (str "Something went wrong while trying to save measurements to file " filename ":"))
        (.printStackTrace e)))))

(defn -main [& args]
  (let [{:keys [options errors summary] :as parsed-opts} (cli/parse-opts args cli-options)
        [cmd & paths] (:arguments parsed-opts)
        server-info-keys [:db-server-url :db-token :db-name]
        server-description (map options server-info-keys)
        {:keys [tag output-format]} options]
    (cond
      (some? errors)
      (do (println "Errors:" errors)
          (print-usage-info summary))

      (:help options)
      (print-usage-info summary)

      (and (= "remote-db" output-format)
           (not (full-server-description? server-description)))
      (do (println (str "Only partial information for remote connection has been given: "
                        (select-keys options server-info-keys)))
          (println "Please, define URL, database name, and token to save the data on a remote datahike server.")
          (print-usage-info summary))

      :else
      (case cmd
        "compare" (compare-benchmarks paths (:plots options))
        "run" (let [measurements (get-measurements options)
                    tagged (if (empty? tag)
                             (vec measurements)
                             (mapv (fn [entity] (assoc entity :tag (join " " tag))) measurements))]
                (case output-format
                  "remote-db" (let [rdb (apply ->RemoteDB server-description)
                                    db-entry (mapv #(->> (dissoc % :context)
                                                         (merge (:context %))
                                                         (add-ns-to-keys ""))
                                                   tagged)]
                                (println "Database used:" rdb)
                                (try
                                  (transact-missing-schema rdb)
                                  (transact-results rdb db-entry)
                                  (println "Successfully saved measurements to remote database.")
                                  (catch Exception e
                                    (println tagged)
                                    (println (str "Something went wrong while trying to save measurements to remote database:"))
                                    (.printStackTrace e))))
                  "csv" (let [col-paths (map :path c/csv-cols)
                              titles (map :title c/csv-cols)
                              csv (str (join "\t" titles) "\n"
                                       (join "\n" (for [result (sort-by (apply juxt (map (fn [path] (fn [measurement] (get-in measurement path)))
                                                                                         col-paths))
                                                                        tagged)]
                                                    (join "\t" (map (fn [path] (get-in result path ""))
                                                                    col-paths)))))]
                          (save-measurements-to-file paths csv))
                  "edn" (save-measurements-to-file paths tagged)
                  (pprint tagged)))

        (throw (Exception. (str "Command '" cmd "' does not exist. Valid commands are 'run' and 'compare'"))))))

  (shutdown-agents))

(comment
  (-main "run" "-x" "[0 10000 5000]" "-t" "test-bench" "-o" "edn" "bench.edn")
  )

