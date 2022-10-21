(ns datahike.middleware.query
  (:require [taoensso.timbre :as log]))

(defn timed [query-handler]
  (fn [query & inputs]
    (let [start (. System (nanoTime))
          result (apply query-handler query inputs)
          time (/ (double (- (. System (nanoTime)) start)) 1000000.0)]
      (log/info "Query time:" {:t time :q query :inputs inputs})
      result)))