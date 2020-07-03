(ns bench
  (:require
   [clj-async-profiler.core :as prof]
   [datahike.core :as d]
   [datahike.db :as db]
   [hitchhiker.tree.messaging :as hmsg]
   [datahike.index.hitchhiker-tree :as hhtree]
   ))

(comment
  (def conn (d/create-conn {:aka { :db/cardinality :db.cardinality/one }}))
  (def all-datoms (vec (map
                         #(vector :db/add (+ 1 %1) :aka %1 %1)
                         (range 2))))


  ;; 100000: 36884 msecs
  (let [conn (d/create-conn {:aka { :db/cardinality :db.cardinality/one }})
        all-datoms (map
                     #(vector :db/add (+ 1 %1) :aka %1 %1)
                     (range 10000))]
    (time
      (doall (d/transact! conn (vec all-datoms))))

    ;; IT only does projections (of 9973 to 9999) when we do the query (and not when inserting)
    (d/q '[:find ?e ?v
                  :where [?e :name ?v]] @conn))

  

  (let [conn (d/create-conn {:aka { :db/cardinality :db.cardinality/one }})
        all-datoms  (reduce into (map
                                     #(for [v (range 2)]
                                        (vector :db/add (+ 1 %1) :aka %1 v))
                                     (range 100000)))]
    (time
      (doall
        (d/transact! conn (vec all-datoms))
        ))
    )

  ;; Don't delete
  ;;
  ;; - For 1000000:
  ;; new version: 18614.990438 msecs
  ;; ;; -apply-op-to-tree: naive map walk: 1M: 13543
  ;; stdrd version: 30206.95185 msecs
  ;;
  ;;For 2M
  ;;new version: 37879
  (let [conn (d/create-conn {:aka { :db/cardinality :db.cardinality/one }})
        all-datoms (vec (map
                          #(vector :db/add 1 :aka %1 %1)
                          (range 100000)))]
    (time
      #_prof/profile
      (doall (d/transact! conn all-datoms))))


  ;; DON'T DELETE
  ;; -apply-op-to-tree: using lookup-fwd-iter: 100k: 29488
  ;; -apply-op-to-tree: naive map walk:         10k: 15634
  ;; -apply-op-to-tree: subseq and updating tree not map: 1M: 41550 40070 42991 37992 40323
  (let [conn (d/create-conn {:aka { :db/cardinality :db.cardinality/one }})
        all-datoms (vec (map
                          #(vector :db/add (+ 1 %1) :aka %1 %1)
                          (range 1000000)))]
    (time
      #_prof/profile
      (doall (d/transact! conn all-datoms))))


  ;; ============= hh-tree =============
  ;; 1M: 13828
  (let [conn (d/create-conn {:aka { :db/cardinality :db.cardinality/one }})
        all-datoms (vec (map
                          #(vector :db/add (+ 1 %1) :aka %1 %1)
                          (range 1000000)))]
    (time
      (doall
        (reduce #(hmsg/insert %1 %2 %2) (hhtree/empty-tree) all-datoms
          ))))

  ;; 1M: 15853
  (let [conn (d/create-conn {:aka { :db/cardinality :db.cardinality/one }})
        all-datoms (vec (map
                          #(vector :db/add 1 :aka %1 %1)
                          (range 1000000)))]
    (time
      #_prof/profile
      (doall
        (reduce #(hmsg/insert %1 %2 %2) (hhtree/empty-tree) all-datoms))))
  ;; ==========================

  ;; ============= Profiling =============
  (do
    (def conn (d/create-conn {:aka { :db/cardinality :db.cardinality/one }}))
    (def all-datoms (map
                     #(vector :db/add 1 #_(+ 1 %1) :aka %1 %1)
                     (range 1000000))))

  (prof/profile (doall (d/transact! conn (vec all-datoms))))

  (prof/serve-files 8080)

  ;; ============ =============




  (let [conn (d/create-conn {:aka { :db/cardinality :db.cardinality/one }})
        all-datoms  (vec (reduce into (map
                                        #(for [v (range 2)]
                                           (vector :db/add (+ 1 %1) :aka %1 v))
                                        (range 10000))))]
    (time
      (doall (d/transact! conn all-datoms))))



  ;; 25975 ms
  (let [conn (d/create-conn {:aka { :db/cardinality :db.cardinality/one }})
        init-datoms  (reduce into (map
                                     #(for [v (range 2)]
                                        (vector :db/add (+ 1 %1) :aka %1 v))
                                     (range 10000)))
        tr-datoms  (reduce into (map
                                     #(for [v (range 2)]
                                        (vector :db/add (+ 1000 %1) :aka %1 v))
                                     (range 100000)))]
    #_(d/transact! conn (vec init-datoms))
    (time
      (doall
        (do
          (d/transact! conn (vec init-datoms))
          (d/transact! conn (vec tr-datoms))))))
)
