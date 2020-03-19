(ns performance.core
  (:gen-class)
  (:require [performance.transactions :refer [get-tx-times]]
            [performance.connect :refer [get-connect-times]]
            [performance.rand-query :refer [get-rand-query-times]]
            [performance.set-query :refer [get-set-query-times]]))

(defn -main [& args]
  (get-connect-times "conn-times")
  (get-tx-times "tx-times")
  ;;(get-rand-query-times "rand-query-times")
  ;; (get-set-query-times "set-query-times")
  )
