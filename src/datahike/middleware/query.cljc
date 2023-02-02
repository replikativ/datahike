(ns datahike.middleware.query
  (:require [clojure.pprint :as pprint]
            [datahike.tools :as dt]))


#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn timed-query [query-handler]
  (fn [query & inputs]
    (let [{:keys [t res]} (dt/timed #(apply query-handler query inputs))]
      (println "Query time:")
      (pprint/pprint {:t      t
                      :q      (update query :args str)
                      :inputs (str inputs)})
      res)))
