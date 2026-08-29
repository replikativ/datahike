(ns benchmark.query-metrics
  "Measure query-observability overhead on warmed point-query paths.

   Run with:

     clojure -M:dev -m benchmark.query-metrics

   The comparison binds `datahike.metrics/*query-metrics?*` around the same
   process, database, query form, caches, and warmed JVM. It reports both the
   cheapest result-cache-hit path and an uncached-result/warm-plan execution.
   The enabled path uses the production sampling rate."
  (:require
   [benchmark.join-strategy :as bench]
   [datahike.api :as d]
   [datahike.metrics :as dhm]
   [datahike.query :as query]
   [replikativ.metrics :as metrics]))

(set! *warn-on-reflection* true)

(def point-query
  '[:find ?name .
    :in $ ?e
    :where
    [?e :name ?name]])

(defn- measure-case [label run]
  ;; Keep the registry shape stable before timing so this measures steady-state
  ;; recording rather than first-use series creation.
  (metrics/reset!)
  (dhm/describe!)
  (binding [dhm/*query-metrics?* true] (run))
  (let [measure-mode (fn [enabled?]
                       (bench/measure #(binding [dhm/*query-metrics?* enabled?] (run))))
        ;; Alternate order so neither mode systematically inherits the hotter
        ;; JIT state. Seven paired rounds also make one GC/noise event harmless.
        pairs (mapv (fn [round]
                      (if (even? round)
                        {:disabled (measure-mode false)
                         :enabled  (measure-mode true)}
                        {:enabled  (measure-mode true)
                         :disabled (measure-mode false)}))
                    (range 7))
        median-result (fn [k]
                        (nth (vec (sort-by :ms (map k pairs)))
                             (quot (count pairs) 2)))
        disabled (median-result :disabled)
        enabled (median-result :enabled)]
    {:case label
     :disabled disabled
     :enabled enabled
     :time-ratio (/ (:ms enabled) (:ms disabled))
     :allocation-delta-kb (* 1024.0
                             (- (:allocated-mb enabled)
                                (:allocated-mb disabled)))}))

(defn- print-result [{:keys [case disabled enabled time-ratio allocation-delta-kb]}]
  (println
   (format "%-28s off=%8.4fms on=%8.4fms ratio=%5.2fx allocation-delta=%7.2fKiB"
           (name case) (:ms disabled) (:ms enabled) time-ratio allocation-delta-kb)))

(defn -main [& _]
  (let [config {:store {:backend :memory :id (random-uuid)}
                :schema-flexibility :read
                :value-caps :default}]
    (d/create-database config)
    (let [conn (d/connect config)]
      (try
        (d/transact conn (mapv (fn [i] {:db/id i :name (str "person-" i)})
                               (range 1 1001)))
        (let [db @conn
              run-hit #(d/q point-query db 500)
              run-exec #(binding [query/*query-result-cache?* false]
                          (d/q point-query db 500))]
          ;; Warm form analysis, plan, compiled program, and the result entry.
          (run-exec)
          (run-hit)
          (println "query metric sample-every:" dhm/*query-metrics-sample-every*)
          (doseq [result [(measure-case :result-cache-hit run-hit)
                          (measure-case :warm-plan-execution run-exec)]]
            (print-result result)))
        (finally
          (d/release conn)
          (d/delete-database config))))))
