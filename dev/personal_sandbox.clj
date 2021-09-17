(ns sandbox
  (:require [datahike.api :as d]
            [taoensso.timbre :as log]))

(comment
  (require '[clj-async-profiler.core :as prof])

  ;; For mac mini M1 only
  (reset! clj-async-profiler.core/async-profiler-agent-path
    "/Users/chrislain/Applications/async-profiler/build/libasyncProfiler.so")

  (prof/serve-files 8080))


(comment
  (log/set-level! :warn)

  (require '[clj-async-profiler.core :as prof])

  (def schema [{:db/ident       :name
                :db/cardinality :db.cardinality/one
                :db/index       true
                :db/unique      :db.unique/identity
                :db/valueType   :db.type/string}
               {:db/ident       :sibling
                :db/cardinality :db.cardinality/many
                :db/valueType   :db.type/ref}
               {:db/ident       :age
                :db/cardinality :db.cardinality/one
                :db/valueType   :db.type/long}
               {:db/ident       :tag
                :db/cardinality :db.cardinality/many
                :db/valueType   :db.type/long}])

  (def cfg {:store {:backend :file
                    :path "/tmp/datahike-sandbox"} #_{:backend :mem :id "sandbox"}
            :keep-history? false
            :schema-flexibility :read
            ;:crypto-hash? false
            :attribute-refs? false})

  (def cfg {:store {:backend :mem :id "sandbox"}
            :keep-history? true
            :schema-flexibility :write
            :attribute-refs? true})

  (def conn (do
              (d/delete-database cfg)
              (d/create-database cfg)
              (d/connect cfg)))

  (d/transact conn schema)

  ;; (d/datoms @conn :avet)
  ;; (d/datoms @conn :aevt)
  ;; (d/datoms @conn :eavt)

  ;; (:max-eid @conn)

  ;; (d/transact conn [{:name "Alice"
  ;;                    :age  25}])

  ;; (d/transact conn [{:name    "Charlie"
  ;;                    :age     45
  ;;                    :sibling [{:name "Alice"} {:name "Bob"}]}])

  ;; (d/q '[:find ?e ?a ?v ?t
  ;;        :in $ ?a
  ;;        :where
  ;;        [?e :name ?v ?t]
  ;;        [?e :age ?a]]
  ;;      @conn
  ;;      25)

  ;; (d/q '[:find ?e ?at ?v
  ;;        :where
  ;;        [?e ?a ?v]
  ;;        [?a :db/ident ?at]]
  ;;      @conn)

  ;; (d/q '[:find ?e :where [?e :name "Alice"]] @conn)

  ;; (:schema @conn)

  ;; 336 cli; with crypto; branch size 300
  "Elapsed time: 78838.922105 msecs"
  "Elapsed time: 45119.325215 msecs"
  "Elapsed time: 1010.205228 msecs"
  "Elapsed time: 887.963561 msecs"
  "Elapsed time: 960.62194 msecs"
  "Elapsed time: 1157.105474 msecs"
  "Elapsed time: 7098.153851 msecs"

  ;; 336 cli; with crypto; branch size 64
  "Elapsed time: 3976.268601 msecs"
  "Elapsed time: 4341.530017 msecs"
  "Elapsed time: 1588.27245 msecs"
  "Elapsed time: 1225.420246 msecs"
  "Elapsed time: 1375.592131 msecs"
  "Elapsed time: 1993.08145 msecs"
  "Elapsed time: 13588.679121 msecs"

  ;; 336 cli; without crypto; branch size 64
  "Elapsed time: 2645.62694 msecs"
  "Elapsed time: 3953.076194 msecs"
  "Elapsed time: 1258.180061 msecs"
  "Elapsed time: 1283.123127 msecs"
  "Elapsed time: 1129.341228 msecs"
  "Elapsed time: 1558.835804 msecs"
  "Elapsed time: 13569.332073 msecs"

  ;; development + 374 read handlers; with crypto; branch size 300
  "Elapsed time: 68674.958501 msecs"
  "Elapsed time: 47967.439691 msecs"
  "Elapsed time: 2051.204404 msecs"
  "Elapsed time: 3221.635775 msecs"
  "Elapsed time: 2991.854539 msecs"
  "Elapsed time: 3696.632695 msecs"
  "Elapsed time: 18253.515653 msecs"

  ;; development + 374 read handlers; with crypto; branch size 64
  "Elapsed time: 3883.494083 msecs"
  "Elapsed time: 6333.497708 msecs"
  "Elapsed time: 3161.173292 msecs"
  "Elapsed time: 2385.295082 msecs"
  "Elapsed time: 1646.030033 msecs"
  "Elapsed time: 5482.207728 msecs"
  "Elapsed time: 28630.231233 msecs"


  ;; 7 benchmarks reported above
  (do
    (time
     (d/transact conn (vec (repeatedly 5000 (fn [] {:age (long (rand-int 1000))
                                                    :name (str (rand-int 1000))})))))
    nil)

  (do
    (time
      (prof/profile
        (d/transact conn (vec (shuffle (for [i (range 20000 30001)]
                                         [:db/add i :name (str i)]))))))
    nil)

  (do
    (time
     (d/transact conn (vec (shuffle (for [i (range 20000 30001)]
                                      [:db/add i :age i])))))
    nil)

  (do
    (time
     (d/transact conn (vec (shuffle (for [i (range 30001 40001)]
                                      [:db/add i :age i])))))
    nil)

  (do
    (time
     (d/transact conn (vec (shuffle (for [i (range 10000 20001)]
                                      [:db/add i :tag i])))))
    nil)

  (do
    (time
     (d/transact conn (vec (shuffle (for [i (range 20001 30001)]
                                      [:db/add i :tag i])))))
    nil)

  (do
    (time
     (d/transact conn (vec (for [i (range 300000 400000)]
                             [:db/add i :tag i]))))
    nil)




  ;; we should benchmark both queries and inserts
  (time
   (d/q {:query '[:find (count ?e)
                  :in $
                  :where [?e :name ?v]]
         :args [@conn]}))

  (time
   (do (d/q {:query '[:find ?e2 ?e1
                      :in $
                      :where [?e1 :name ?v1] [?e2 :name ?v2]]
             :args [@conn]})
       nil)))

