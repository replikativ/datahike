(ns performance.core
  (:gen-class)
  (:require [performance.transactions :refer [get-tx-times]]
            [performance.connection :refer [get-connect-times]]
            [performance.rand-query :refer [get-rand-query-times]]
            [performance.set-query :refer [get-set-query-times]]))

(defn -main [& _]
  (get-connect-times "connection-times")
  (get-tx-times "transaction-times")
  (get-rand-query-times "random-query-times")
  (get-set-query-times "set-query-times"))
