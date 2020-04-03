(ns performance.config
  (:import (java.text SimpleDateFormat)
           (java.lang Integer)
           (java.util Date)))


;; Maximum value for integer databases
(def max-int Integer/MAX_VALUE)


;; Output files

(def data-dir "./data")

(def date-format "yyyy-MM-dd_HH-mm-ss")

(defn filename [file-suffix]
  (str data-dir "/" (.format (SimpleDateFormat. date-format) (Date.)) "_" file-suffix ".csv"))


;; Benchmark uris

(def datahike-uris
  (for [sor [false true]
        ti [false true]
        base-uri [
                  {:lib :datahike :name "In-Memory" :uri "datahike:mem://performance-ht"}
                  ;;{:lib :datahike :name "In-Memory" :uri "datahike:mem://performance-ps"}
                  {:lib :datahike :name "LevelDB" :uri "datahike:level:///tmp/lvl-performance"}
                  {:lib :datahike :name "PostgreSQL" :uri "datahike:pg://datahike:clojure@localhost:5440/performance"}
                  {:lib :datahike :name "File-based" :uri "datahike:file:///tmp/file-performance"}]]
    (assoc base-uri :schema-on-read sor :temporal-index ti :uri (str (:uri base-uri) "-s" (if sor 1 0) "-t" (if ti 1 0)))))


(def datomic-uris [{:lib :datomic :name "Datomic Mem" :uri "datomic:mem://performance" :schema-on-read false :temporal-index false} ;; not working on connect
                   {:lib :datomic :name "Datomic Free" :uri "datomic:free://localhost:4334/performance?password=clojure" :schema-on-read false :temporal-index false}])

(def hitchhiker-configs [{:lib :hitchhiker :name " Hitchhiker Tree (Datoms)" :uri "datoms" :schema-on-read false :temporal-index false}
                         {:lib :hitchhiker :name " Hitchhiker Tree (Values)" :uri "values" :schema-on-read false :temporal-index false}])

(def uris (into [] (concat datahike-uris datomic-uris)))
