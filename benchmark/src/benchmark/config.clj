(ns benchmark.config)


(def datom-counts [1 10 100 1000])                                                    ;; later 100,000
(def iterations 10)
(def max-int 1000000)
(def initial-datoms [0 1000])                                                         ;; later 100,000

(def db-configs
  [{:store          {:backend :mem :id "performance-hht"}
    :schema-flexibility :write
    :keep-history? true
    :index          :datahike.index/hitchhiker-tree}
   {:store          {:backend :file :path "/tmp/performance-hht"}
    :schema-flexibility :write
    :keep-history? true
    :index          :datahike.index/hitchhiker-tree}])

(def schema
   [{:db/ident       :s1
     :db/valueType   :db.type/string
     :db/cardinality :db.cardinality/one}
    {:db/ident       :i1
     :db/valueType   :db.type/bigint
     :db/cardinality :db.cardinality/one}])

(defn rand-entity []
   {:s1 (format "%15d" (rand-int max-int))
    :i1 (rand-int max-int)})


(defn q1 [string-val]
  (conj '[:find ?e :where]
        (conj '[?e :s1] string-val)))

(defn q2 [int-val]
  (conj '[:find ?a :where [?e :s1 ?a]]
        (conj '[?e :i1] int-val)))

