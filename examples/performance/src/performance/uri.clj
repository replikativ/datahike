(ns performance.uri)


(def datahike {"PostgreSQL" "datahike:pg://datahike:clojure@localhost:5434/datahike"
               "In-Memory" "datahike:mem://perf"
               "LevelDB" "datahike:level:///tmp/lvl-perf"
               "File-based" "datahike:file:///tmp/file-perf"})


(def datomic {"Datomic" "datomic:free://localhost:4334/perf"})

(def all (into {} (concat datahike datomic)))