(ns datahike.middleware.query
  (:require [clojure.pprint :as pprint]))

(defn now []
  #?(:clj (. System (nanoTime))
          :cljs (.getTime (js/Date.))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn timed-query [query-handler]
  (fn [query & inputs]
    (let [start (now)
          result (apply query-handler query inputs)
          t (/ (double (- (now) start)) 1000000.0)]
      (println "Query time:")
      (pprint/pprint {:t      t
                      :q      (update query :args str)
                      :inputs (str inputs)})
      result)))
