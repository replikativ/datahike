(ns benchmark.core
  (:require [clojure.tools.cli :as cli]
            [benchmark.measure :as m]
            [benchmark.config :as c]
            [benchmark.store :refer [transact-missing-schema transact-results ->RemoteDB]]
            [clojure.edn :as edn]
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
  (println (str "Usage: clj -M:benchmark [options] \n\n  Options:\n" summary)))

(defn full-server-description? [server-description]
  (every? #(not (nil? %)) server-description))

(defn time-statistics [times]
  (let [n (count times)
        mean (/ (apply + times) n)]
    {:mean mean
     :median (nth (sort times) (int (/ n 2)))
     :std (->> times
               (map #(* (- % mean) (- % mean)))
               (apply +)
               (* (/ 1.0 n))
               Math/sqrt)}))

(def csv-cols
  [{:title "DB" :path [:context :db :name]}
   {:title "Function" :path [:context :function]}
   {:title "DB Size" :path [:context :db-size]}
   {:title "TX Size" :path [:context :tx-size]}
   {:title "Data Type" :path [:context :exec-details :data-type]}
   {:title "Queried Data in Database?" :path [:context :exec-details :data-in-db?]}
   {:title "Tags" :path [:tag]}
   {:title "Mean Time" :path [:time :mean]}
   {:title "Median Time" :path [:time :median]}
   {:title "Time Std" :path [:time :std]}])

(defn -main [& args]
  (let [{:keys [options errors summary]} (cli/parse-opts args cli-options)
        server-info-keys [:db-server-url :db-token :db-name]
        server-description (map options server-info-keys)
        {:keys [tag config-name output-format]} options]

    (cond
      (some? errors)
      (do (println "Errors:" errors)
          (print-usage-info summary))

      (:help options)
      (print-usage-info summary)

      (and (= "remote-db" output-format) (not (full-server-description? server-description)))
      (do (println (str "Only partial information for remote connection has been given: "
                        (select-keys options server-info-keys)))
          (println "Please, define URL, database name, and token to save the data on a remote datahike server.")
          (print-usage-info summary))

      :else
      (let [measurements (vec (for [config (if config-name
                                             (filter #(= (:name %) config-name) c/db-configs)
                                             c/db-configs)
                                    initial-size c/initial-datoms
                                    n c/datom-counts
                                    _ (range c/iterations)]
                                (m/measure-performance-full initial-size n config)))
            processed (->> measurements
                           (apply concat)
                           (group-by :context)
                           (map (fn [[context group]] {:context context
                                                       :time (time-statistics (map :time group))})))
            tagged (if (empty? tag)
                     (vec processed)
                     (mapv (fn [entity] (assoc entity :tag (join " " tag))) processed))]
        (case output-format
          "remote-db" (let [rdb (apply ->RemoteDB server-description)]
                        (println "Database used:" rdb)
                        (transact-missing-schema rdb)
                        (transact-results rdb tagged))
          "csv" (let [col-paths (map :path csv-cols)
                      titles (map :title csv-cols)]
                  (println (join "\t" titles))
                  (run! (fn [result]
                           (println (join "\t" (map (fn [path] (get-in result path))
                                                    col-paths))))
                         tagged))
          "edn" (pprint tagged)
          :default (pprint tagged)))))

  (shutdown-agents))

(defn print-comparison [context group filename1 filename2]
  (let [results (vec group)
        _ (println "results" results)
        times1 (->> results (filter #(= (:source %) filename1)) first :time)
        times2 (->> results (filter #(= (:source %) filename2)) first :time)]
    (when (and times1 times2)                  ;; benchmark results available from both files
      (print " Context: ") (pprint context)
      (println " Times (s): (" filename1 "/" filename2 ")")
      (println " - Mean: (" (:mean times1) "/" (:mean times2) ")")
      (println " - Median: (" (:median times1) "/" (:median times2) ")")
      (println " - Std-Deviation: (" (:std times1) "/" (:std times2) ")"))))

(defn compare-benchmarks [filename1 filename2]
  (when (not (and (.endsWith filename1 ".edn")
                  (.endsWith filename2 ".edn")))
    (println "Both files must be edn files.")
    (System/exit 1))

  (let [bench1 (map #(assoc % :source filename1)
                    (edn/read-string (slurp filename1)))
        bench2 (map #(assoc % :source filename2)
                    (edn/read-string (slurp filename2)))
        grouped-benchmarks (->> (concat bench1 bench2)
                                (group-by :context)
                                (group-by #(-> % first :function)))]

    (println "Connection Statistics:")
    (dorun (for [[context group] (:connection grouped-benchmarks)]
             (print-comparison context group filename1 filename2)))

    (println "Transaction Statistics:")
    (dorun (for [[context group] (:transaction grouped-benchmarks)]
             (print-comparison context group filename1 filename2)))

    (println "Query Statistics:")
    (dorun (for [[context group] (->> (dissoc grouped-benchmarks :connection :transaction)
                                      vals
                                      (apply concat))]
             (print-comparison context group filename1 filename2)))))
