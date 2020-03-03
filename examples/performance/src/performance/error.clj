(ns performance.error
  (:require [clojure.string :as s]))

(defn short-report [error]
  (let [e (Throwable->map error)]
    (println "  Shortened stacktrace for:  " (:cause e))
    (println "   Start:")
    (doall (for [item (take 5 (:trace e))]
             (println "    " item)))
    (println "   Then:")
    (doall (for [item (drop 5 (:trace e))
                 :when (s/starts-with? (first item) "performance")]
             (println "    " item)))
    nil))