(ns sandbox-transact-bench
  (:require [datahike.api :as d]))


(comment
  (log/set-level! :warn)

    ;; if profiling
    ;;(require '[clj-async-profiler.core :as prof])
    ;;

    ;; ;; The resulting flamegraph will be stored in /tmp/clj-async-profiler/results/
    ;; ;; You can view the SVG directly from there or start a local webserver:
    ;; (prof/serve-files 8080) ; Serve on port 8080


  (defn bench
    [{:keys [avet? update? decreasing? one-transaction?]} counter]
    (let [schema [{:db/ident       :name
                   :db/cardinality :db.cardinality/one
                   :db/index       true
                   :db/unique      :db.unique/identity
                   :db/valueType   :db.type/string}
                  {:db/ident       :sibling
                   :db/cardinality :db.cardinality/many
                   :db/valueType   :db.type/ref}
                  {:db/ident       :age
                   :db/cardinality :db.cardinality/one
                   :db/index       avet?
                   :db/valueType   :db.type/long}]

          cfg {:store  {:backend :mem :id "sandbox" :path "/tmp/benchplay"}
               :keep-history? true
               :schema-flexibility :read
               :initial-tx []}

          _ (d/delete-database cfg)
          _ (d/create-database cfg)
          conn (d/connect cfg)]

      (d/transact conn schema)

      (println "==== avet?" avet? "---- update?" update? "---- decreasing?" decreasing? "--- one-transaction?" one-transaction? "    iter. num.:" counter)
      (if one-transaction?
        (let [txs (vec (for [i (range counter)]
                         [:db/add (if update? 1000 (inc i)) :age (if decreasing? (- counter i) i)]))]
          (time
           (d/transact conn txs)))
    ;; Multiple transactions
        (time
         (doall
           (for [i (range counter)]
             (d/transact conn [[:db/add (if update? 1000 (inc i))  :age (if decreasing? (- counter i) i)]])))))))

  (defn bench-all [counter]
    (bench {:avet? false
            :update? false
            :decreasing? false
            :one-transaction? true}
      counter)

    (bench {:avet? false
            :update? true
            :decreasing? true
            :one-transaction? true}
      counter)

    (bench {:avet? true
            :update? false
            :decreasing? false
            :one-transaction? true}
      counter)

    (bench {:avet? true
            :update? true
            :decreasing? true
            :one-transaction? true}
      counter))


  (for [i (range 3)]
    (do
      (bench-all 100000)
      (prn " ========================== DONE ============")))


  (doall
    (for [avet [false true]
          decreasing [false true]
          update [true false]
          _ (range 3)]
      (bench {:avet? avet
              :update? update
              :decreasing? decreasing
              :one-transaction? true}
        100000)))


  ;; if profiling
  ;; (prof/profile (d/transact conn txs))


  (d/q '[:find ?e ?a
         :in $ ?a
         :where  [?e :age ?a]]
       (d/history @conn) ;; use @conn if no history
       35)

  (d/datoms @conn :eavt 35))
