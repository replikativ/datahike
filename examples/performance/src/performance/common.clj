(ns performance.common
  (:require [clojure.string :as s]))

(defn short-error-report [error]
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

(defn make-attr
  ([name type] (make-attr name type :db.cardinality/one))
  ([name type cardinality]
   {:db/ident       name
    :db/valueType   type
    :db/cardinality cardinality}))

(defn linspace [start end point-count]
  (range start (inc end) (/ (- end start) (dec point-count))))


(defn int-linspace [start end point-count]
  (map int (linspace start end point-count)))