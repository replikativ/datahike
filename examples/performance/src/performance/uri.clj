(ns performance.uri)



(def datahike
  (for [sor [false true]
        ti [false true]
        base-uri [{:lib "datahike" :name "In-Memory" :uri "datahike:mem://performance"}
                  {:lib "datahike" :name "LevelDB" :uri "datahike:level:///tmp/lvl-performance"}
                  {:lib "datahike" :name "PostgreSQL" :uri "datahike:pg://datahike:clojure@localhost:5440/performance"}
                  {:lib "datahike" :name "File-based" :uri "datahike:file:///tmp/file-performance"}]]
    ;;base-uri
      (assoc base-uri :schema-on-read sor :temporal-index ti :uri (str (:uri base-uri) "-s" (if sor 1 0) "-t" (if ti 1 0)))
    ))


(def datomic [{:lib "datomic" :name "Datomic Mem" :uri "datomic:mem://performance" :schema-on-read false :temporal-index false} ;; not working on connect
                 {:lib "datomic" :name "Datomic" :uri "datomic:free://localhost:4334/performance?password=clojure" :schema-on-read false :temporal-index false}
              ])


;;(map println datahike)
(def all (into [] (concat datahike datomic)))