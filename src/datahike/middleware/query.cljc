(ns datahike.middleware.query
  (:require [taoensso.timbre :as log]
            [datahike.db.utils :as utils]))

(defn timed-query [query-handler]
  (fn [query & inputs]
    (let [start (. System (nanoTime))
          result (apply query-handler query inputs)
          time (/ (double (- (. System (nanoTime)) start)) 1000000.0)]
      (log/info "Query time:" {:t      time
                               :q      (update query :args str)
                               :inputs (str inputs)})
      result)))

