(ns benchmark.config)


(def datom-counts [1 10 100 1000]) ;; later 100,000
(def iterations 10)
(def max-int 1000000)
(def initial-datoms [0 1000]) ;; later 100,000

(def db-configs
  [{:name          "In-memory (HHT)"
    :store          {:backend :mem :path "performance-hht"}
    :schema-on-read false
    :temporal-index true
    :index          :datahike.index/hitchhiker-tree}
   {:name           "File"
    :store          {:backend :file :path "performance-hht"}
    :schema-on-read false
    :temporal-index true
    :index          :datahike.index/hitchhiker-tree}])

(def schema
   [{:db/ident       :s1
     :db/valueType   :db.type/string
     :db/cardinality :db.cardinality/one}
    {:db/ident       :i1
     :db/valueType   :db.type/long
     :db/cardinality :db.cardinality/one}])

(defn rand-entity []
   {:s1 (format "%15d" (rand-int max-int))
    :i1 (rand-int max-int)})


(defn q1 [string-val]
  (conj '[:find ?e :where]
        (conj '[?e :s1] string-val)))

(defn q2 [int-val]
  (conj '[:find ?e :where [?e :s1 ?a]]
        (conj '[?e :i1] int-val)))

