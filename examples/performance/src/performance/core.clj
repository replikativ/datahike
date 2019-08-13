(ns performance.core
  (:gen-class)
  (:require [datahike.api :as d]
            [datomic.api :as da]
            [incanter.stats :as is]
            [incanter.charts :as charts]
            [incanter.datasets :as ds]
            [incanter.core :as ic]))

;; make sure you have a postgres instance!
(def uri {"PostgreSQL" "datahike:pg://datahike:clojure@localhost:5434/datahike"
          "In-Memory" "datahike:mem://perf"
          "LevelDB" "datahike:level:///tmp/lvl-perf"
          "File-based" "datahike:file:///tmp/file-perf"})

;; and start a datomic-free instance
(def datomic-uri "datomic:free://localhost:4334/perf")

(def schema [{:db/ident :name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}])

(defn run-datomic-perf [conn iterations d-count]
  (let [ti (loop [i 0
                  observations []]
             (if (> i iterations)
               observations
               (let [txs (->> (repeatedly #(str (java.util.UUID/randomUUID)))
                              (take d-count)
                              (mapv (fn [id] {:name id})))
                     t0 (.getTime (java.util.Date.))]
                 @(da/transact conn txs)
                 (recur (inc i) (conj observations (- (.getTime (java.util.Date.)) t0))))))]
    ["Datomic" (is/mean ti) (is/sd ti)]))

(defn run-perf [conns iterations d-count]
  (->> conns
       (mapv (fn [[backend conn]]
              (let [ti (loop [i 0
                              observations []]
                         (if (> i iterations)
                           observations
                           (let [txs (->> (repeatedly #(str (java.util.UUID/randomUUID)))
                                          (take d-count)
                                          (mapv (fn [id] {:name id})))
                                 t0 (.getTime (java.util.Date.))]
                             (d/transact! conn txs)
                             (recur (inc i) (conj observations (- (.getTime (java.util.Date.)) t0))))))]
                [backend (is/mean ti) (is/sd ti)])))
       doall))

(defn run-combinations [iterations]
  (-> (for [d-count [1 2 4 8 16 32 64 128 256 512 1024]]
        (do
          (println "datoms" d-count)
          (->> (vals uri) (map d/delete-database) doall)
          (->> (vals uri) (map #(d/create-database % :initial-tx schema)) doall)
          (da/delete-database datomic-uri)
          (da/create-database datomic-uri)
          (let [conns (reduce-kv (fn [m k v] (assoc m k (d/connect v))) {} uri)
                datomic-conn (da/connect datomic-uri)
                _ @(da/transact datomic-conn schema)
               t0 (.getTime (java.util.Date.))
                observations (run-perf conns iterations d-count)
                datomic-observations (run-datomic-perf datomic-conn iterations d-count)]
           (->> conns vals (map d/release) doall)
           [d-count (conj observations datomic-observations)])))
      doall
      vec))

(defn normalize-mem-result [result]
    (->> result
         (mapcat
          (fn [[d-count results]] (mapv (fn [[backend mean sd]] {:datoms d-count :backend backend :mean (/ mean d-count) :sd sd}) results)))
         (group-by :datoms)
         (mapcat (fn [[k v]] (let [mem (->> v (filter #(= (:backend %) "In-Memory")) first :mean)]
                               (mapv (fn [x] (update-in x [:mean] #(/ % mem))) v))))
         vec))

(defn normalized-means-datoms-plot [result]
  (let [data (ic/dataset (normalize-mem-result result))]
    (ic/with-data data
      (doto
        (charts/xy-plot
         :datoms
         :mean
         :group-by :backend
         :points true
         :legend true
         :title "transaction time relative to In-Memory transaction time vs. datoms per transaction of available datahike backends"
         :y-label "transaction time relative to In-Memory transaction time")
        (charts/set-axis :x (charts/log-axis :base 2, :label "Number of datoms per transaction"))))))

(defn means-datoms-plot [result]
  (let [data (ic/dataset (mapcat
                          (fn [[d-count results]] (mapv (fn [[backend mean sd]] {:datoms d-count :backend backend :mean (/ mean d-count) :sd sd}) results)) result))]
    (ic/with-data data
      (doto
        (charts/xy-plot
         :datoms
         :mean
         :group-by :backend
         :points true
         :legend true
         :title "Mean transaction time vs. datoms per transaction of available datahike backends"
         :y-label "Mean transaction time per datom (ms)")
        (charts/set-axis :x (charts/log-axis :base 2, :label "Number of datoms per transaction"))))))

(defn overall-means-datoms-plot [result]
  (let [data (ic/dataset (mapcat
                          (fn [[d-count results]] (mapv (fn [[backend mean sd]] {:datoms d-count :backend backend :mean mean :sd sd}) results)) result))]
    (ic/with-data data
      (doto
        (charts/xy-plot
         :datoms
         :mean
         :group-by :backend
         :points true
         :legend true
         :title "transaction time vs. datoms per transaction of available datahike backends"
         :y-label "transaction time (ms)")
        (charts/set-axis :x (charts/log-axis :base 2, :label "Number of datoms per transaction"))))))

(defn -main [& args]
  (let [result (run-combinations 256)
        formatter (java.text.SimpleDateFormat. "yyyy-MM-dd-HH:mm:ss")
        means-plot (overall-means-datoms-plot result)]
    (ic/save (ic/dataset (mapcat
                        (fn [[d-count results]] (mapv (fn [[backend mean sd]] {:datoms d-count :backend backend :mean mean :sd sd}) results)) result))
             (str "./data/" (.format formatter (java.util.Date.)) "-tx-time-per-datoms.dat"))
    (ic/save means-plot (str "./plots/" (.format formatter (java.util.Date.)) "-tx-time-per-datoms.png") :width 1000 :height 750)))
