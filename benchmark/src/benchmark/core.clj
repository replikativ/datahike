(ns benchmark.core
  (:require [clojure.tools.cli :as cli]
            [benchmark.measure :as m]
            [benchmark.config :as c]
            [benchmark.store :refer [transact-missing-schema transact-results ->RemoteDB]]
            [clojure.pprint :refer [pprint]]))


(def cli-options
  [["-u" "--db-server-url URL" "Base URL for datahike server, e.g. http://localhost:3000"
    :default nil]
   ["-n" "--db-name DBNAME" "Database name for datahike server" :default nil]
   ["-g" "--db-token TOKEN" "Token for datahike server" :default nil]
   ["-t" "--tag TAG" "Add tag to measurements"
    :default #{}
    :assoc-fn (fn [m k v] (assoc m k (conj (get m k) v)))]
   ["-d" "--config-name CONFIGNAME" "Name of database configuration to use. Available are 'mem-set' 'mem-hht' and 'file'. If not set all configs will be tested" :default nil]

   ["-h" "--help"]])

(defn print-usage-info [summary]
  (println (str "Usage: clj -M:benchmark [options] \n\n  Options:\n" summary)))

(defn full-server-description? [server-description]
  (every? #(not (nil? %)) server-description))

(defn partial-server-description? [server-description]
  (and (some (comp not nil?) server-description)
       (not (full-server-description? server-description))))

(defn time-statistics [times]
  (let [mean (/ (apply + times) (count times))
        median (nth (sort times) (int (/ (count times) 2)))
        std (->> times
                 (map #(* (- % mean) (- % mean)))
                 (apply +)
                 (* (/ 1.0 (count times)))
                 Math/sqrt)]
    {:mean-time mean
     :median-time median
     :std-time std}))

(defn -main [& args]
  (let [{:keys [options errors summary]} (cli/parse-opts args cli-options)
        server-info-keys [:db-server-url :db-token :db-name]
        server-description (map options server-info-keys)
        {:keys [tags config-name]} options]

    (cond
      (some? errors)
      (do (println "Errors:" errors)
          (print-usage-info summary))

      (:help options)
      (print-usage-info summary)

      (partial-server-description? server-description)
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
                           (map (fn [[context group]]
                                  (merge context (time-statistics (map :time group))))))
            tagged (if (empty? tags)
                     (vec processed)
                     (mapv (fn [entity] (assoc entity :tag (vec tags))) processed))]
        (if (full-server-description? server-description)
          (let [rdb (apply ->RemoteDB server-description)]
            (println "Database used:" rdb)
            (transact-missing-schema rdb)
            (transact-results rdb tagged))
          (pprint tagged)))))

  (shutdown-agents))
