(ns datahike.middleware.query
  (:require [clojure.pprint :as pprint]))

(defn timed-query [query-handler]
  (fn [query & inputs]
    (let [start (. System (nanoTime))
          result (apply query-handler query inputs)
          t (/ (double (- (. System (nanoTime)) start)) 1000000.0)]
      (println "Query time:")
      (pprint/pprint {:t      t
                      :q      (update query :args str)
                      :inputs (str inputs)})
      result)))

