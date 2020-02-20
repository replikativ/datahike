(ns performance.core
  (:gen-class)
  (:require [performance.transactions :refer [make-tx-plots]]
            [performance.connect :refer [make-connect-plots]]
            [performance.query :refer [make-query-plots]])
  (:import (java.text SimpleDateFormat)))

(def config {:data-dir       "./data"
             :plot-dir       "./plots"
             :date-formatter (SimpleDateFormat. "yyyy-MM-dd-HH:mm:ss")})

(defn -main [& args]
  (make-tx-plots config "mean-tx-per-datoms")
  (make-connect-plots config "mean-conn-per-datoms")
  (make-query-plots config "mean-query-per-datoms"))
