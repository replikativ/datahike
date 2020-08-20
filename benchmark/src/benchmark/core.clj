(ns benchmark.core
  (:require [benchmark.measure :as m]
            [benchmark.config :as c]
            [clojure.pprint :as pp]))


(defn -main [& _]
  (let [measurements (vec (for [config       c/db-configs
                                initial-size c/initial-datoms
                                n            c/datom-counts
                                _            (range c/iterations)]
                              (m/measure-performance-full initial-size n config)))]
    (->> measurements
         (apply concat)
         (group-by :context)
         (mapv (fn [[context group]]
                 {:context context
                  :mean-time (/ (reduce (fn [x y] (+ x (:time y))) 0 group)
                                (count group))}))
         pp/pprint)))
