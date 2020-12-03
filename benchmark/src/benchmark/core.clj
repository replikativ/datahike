(ns benchmark.core
  (:require [clojure.tools.cli :as cli]
            [benchmark.measure :as m]
            [benchmark.config :as c]
            [benchmark.store :refer [transact-missing-schema transact-data ->RemoteDB]]
            [clojure.pprint :refer [pprint]]))


(def cli-options
  [["-u" "--db-server-url URL" "Base URL for datahike server, e.g. http://localhost:3000"
    :default nil]
   ["-n" "--db-name DBNAME" "Database name for datahike server" :default nil]
   ["-g" "--db-token TOKEN" "Token for datahike server" :default nil]
   ["-t" "--tag TAG" "Add tag to measurements"
    :default #{}
    :assoc-fn (fn [m k v] (assoc m k (conj (get m k) v)))]

   ["-h" "--help"]])

(defn print-usage-info [summary]
  (println (str "Usage: lein run [options] \n\n  Options:\n" summary)))

(defn full-server-description? [server-description]
  (every? #(not (nil? %)) server-description))

(defn partial-server-description? [server-description]
  (and (some (comp not nil?) server-description)
       (not (full-server-description? server-description))))

(defn -main [& args]
  (let [{:keys [options errors summary]} (cli/parse-opts args cli-options)
        server-info-keys [:db-server-url :db-token :db-name]
        server-description (map options server-info-keys)
        tags (:tag options)]

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
      (let [measurements (vec (for [config c/db-configs
                                    initial-size c/initial-datoms
                                    n c/datom-counts
                                    _ (range c/iterations)]
                                (m/measure-performance-full initial-size n config)))
            processed (->> measurements
                           (apply concat)
                           (group-by :context)
                           (map (fn [[context group]]
                                  (assoc context :mean-time (/ (reduce (fn [x y] (+ x (:time y))) 0 group)
                                                               (count group))))))
            tagged (if (empty? tags)
                     (vec processed)
                     (mapv (fn [entity] (assoc entity :tag (vec tags))) processed))]
        (if (full-server-description? server-description)
          (let [rdb (apply ->RemoteDB server-description)]
            (println "db" rdb)
            (transact-missing-schema rdb)
            (transact-data rdb tagged))
          (pprint tagged)))))

  (shutdown-agents))

#_(-main "-t" "test-id" "-u" "http://localhost:3001" "-n" "benchmarks" "-g" "test-token")

;; TIMBRE_LEVEL=':fatal' lein with-profile benchmark run
;; TIMBRE_LEVEL=':fatal' lein with-profile benchmark run -t test-id
;; TIMBRE_LEVEL=':fatal' lein with-profile benchmark run -t test-id -u http://localhost:3001 -n benchmarks -g test-token

