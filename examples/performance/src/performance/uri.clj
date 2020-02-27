(ns performance.uri)

(def datahike
  (for [sor [false true]
        ti [false true]
        base-uri [{:lib "datahike" :name "In-Memory" :uri "datahike:mem://perf"}
                  {:lib "datahike" :name "LevelDB" :uri "datahike:level:///tmp/lvl-perf"}
                  ;; {:lib "datahike" :name "PostgreSQL" :uri "datahike:pg://datahike:clojure@localhost:5434/perf"}
                  {:lib "datahike" :name "File-based" :uri "datahike:file:///tmp/file-perf"}]]
    (assoc base-uri :schema-on-read sor :temporal-index ti :uri (str (:uri base-uri) sor ti))))


(def datomic [;;{:lib "datomic" :name "Datomic" :uri "datomic:free://localhost:4334/perf" :schema-on-read false :temporal-index false}
              ])

(def all (into [] (concat datahike datomic)))