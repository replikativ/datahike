(ns benchmark.core
  (:require [benchmark.measure :as m]
            [benchmark.config :as c]))


(defn -main []
  (let [measurements (for [config c/db-configs
                           initial-size c/initial-datoms
                           n c/datom-counts
                           i c/iterations]
                        (m/measure-performance-full initial-size n config))]
    (->> measurements
         (apply concat)
         (group-by :context)
         (map (fn [context vals]
                {:mean-time (/ (transduce :time + vals) (count vals))
                 :context context}))
         clojure.pprint/pprint)))
