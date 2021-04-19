(ns benchmark.core
  (:require [clojure.tools.cli :as cli]
            [benchmark.measure :refer [get-measurements]]
            [benchmark.config :as c]
            [benchmark.store :refer [transact-missing-schema transact-results ->RemoteDB]]
            [benchmark.compare :refer [compare-benchmarks]]
            [clojure.string :refer [join]]
            [clojure.pprint :refer [pprint]]))

(def output-formats #{"remote-db" "edn" "csv"})
(def config-names #{"mem-set" "mem-hht" "file"})

(def cli-options
  [["-u" "--db-server-url URL" "Base URL for datahike server, e.g. http://localhost:3000"
    :default nil]
   ["-n" "--db-name DBNAME" "Database name for datahike server" :default nil]
   ["-g" "--db-token TOKEN" "Token for datahike server" :default nil]
   ["-t" "--tag TAG" "Add tag to measurements"
    :default #{}
    :assoc-fn (fn [m k v] (assoc m k (conj (get m k) v)))]
   ["-o" "--output-format FORMAT" "Determines how the results will be processed. Possible are 'remote-db', 'edn' and 'csv'"
    :default "edn"
    :validate [output-formats  #(str "Format " % " has not been implemented. Possible formats are " output-formats)]]
   ["-d" "--config-name CONFIGNAME" "Name of database configuration to use. Available are 'mem-set' 'mem-hht' and 'file'. If not set all configs will be tested"
    :default nil
    :validate [config-names  #(str "A configuration named " % " has not been implemented. Possible configurations are " config-names)]]
   ["-h" "--help"]])

(defn print-usage-info [summary]
  (println (str "Usage: clj -M:benchmark CMD [OPTIONS] [FILEPATHS] \n\n  Options for command 'run':\n" summary)))

(defn full-server-description? [server-description]
  (every? #(not (nil? %)) server-description))

(def csv-cols
  [{:title "DB"                        :path [:context :dh-config :name]}
   {:title "Function"                  :path [:context :function]}
   {:title "DB Size"                   :path [:context :db-size]}
   {:title "TX Size"                   :path [:context :execution :tx-size]}
   {:title "Data Type"                 :path [:context :execution :data-type]}
   {:title "Queried Data in Database?" :path [:context :execution :data-in-db?]}
   {:title "Tags"                      :path [:tag]}
   {:title "Mean Time"                 :path [:time :mean]}
   {:title "Median Time"               :path [:time :median]}
   {:title "Time Std"                  :path [:time :std]}])

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
        {:keys [tag config-name output-format]} options]

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
        "compare" (compare-benchmarks paths)
        "run" (let [measurements (get-measurements (if config-name
                                                     (filter #(= (:config-name %) config-name) c/db-configs)
                                                     c/db-configs))
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
                  "csv" (let [col-paths (map :path csv-cols)
                              titles (map :title csv-cols)
                              csv (str (join "\t" titles) "\n"
                                       (join "\n" (for [result tagged]
                                                    (join "\t" (map (fn [path] (get-in result path))
                                                                    col-paths)))))]
                          (save-measurements-to-file paths csv))
                  "edn" (save-measurements-to-file paths tagged)
                  (pprint tagged)))

        (throw (Exception. (str "Command '" cmd "' does not exist. Valid commands are 'run' and 'compare'"))))))

  (shutdown-agents))

