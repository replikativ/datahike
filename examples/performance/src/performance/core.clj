(ns performance.core
  (:gen-class)
  (:require [performance.transaction :refer [get-transaction-times]]
            [performance.connection :refer [get-connection-times]]
            [performance.rand-query :refer [get-rand-query-times]]
            [performance.set-query :refer [get-set-query-times]]
            [clojure.java.io]))

(defn -main [& args]
  (let [c (count args)
        connection-iterations (if (< c 1) 50 (first args))
        transaction-iterations (if (< c 2) 1 (first args)) ;; 50 -> over 19 hours
        query-iterations (if (< c 3) 1 (first args))]
    (binding [*out* (clojure.java.io/writer "datahike-performance.log")] ;; conflicts with leveldb?
      (get-connection-times "connection-times" connection-iterations)
      ;;  (get-transaction-times "transaction-times" transaction-iterations)
      ;; (get-rand-query-times "random-query-times" query-iterations)
      ;; (get-set-query-times "set-query-times" query-iterations)
      )))
