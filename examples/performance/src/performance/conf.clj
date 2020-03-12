(ns performance.conf
  (:import (java.text SimpleDateFormat)
           (java.lang Integer)))


;; constant for integer databases
(def max-int Integer/MAX_VALUE)

;; output files

(def data-dir "./data")
(def date-formatter (SimpleDateFormat. "yyyy-MM-dd-HH:mm:ss"))


;; benchmark uris

(def datahike-uris
  (for [sor [false true]
        ti [false true]
        base-uri [{:lib "datahike" :name "In-Memory" :uri "datahike:mem://performance"}
                  {:lib "datahike" :name "LevelDB" :uri "datahike:level:///tmp/lvl-performance"}
                  {:lib "datahike" :name "PostgreSQL" :uri "datahike:pg://datahike:clojure@localhost:5440/performance"}
                  {:lib "datahike" :name "File-based" :uri "datahike:file:///tmp/file-performance"}]]
    (assoc base-uri :schema-on-read sor :temporal-index ti :uri (str (:uri base-uri) "-s" (if sor 1 0) "-t" (if ti 1 0)))))


(def datomic-uris [{:lib "datomic" :name "Datomic Mem" :uri "datomic:mem://performance" :schema-on-read false :temporal-index false} ;; not working on connect
                   {:lib "datomic" :name "Datomic Free" :uri "datomic:free://localhost:4334/performance?password=clojure" :schema-on-read false :temporal-index false}])


(def uris (into [] (concat datahike-uris datomic-uris)))