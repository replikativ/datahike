(ns benchmark.config)

(def max-int 1000000)

(def context-cell-order [:function :dh-config :db-datoms :tx-datoms :data-type :data-in-db? :tag])

(def csv-cols
  [{:title "Function"                  :path [:context :function]}
   {:title "DB"                        :path [:context :dh-config :name]}
   {:title "DB Datoms"                 :path [:context :db-datoms]}
   {:title "DB Entities"               :path [:context :db-entities]}
   {:title "TX Datoms"                 :path [:context :execution :tx-datoms]}
   {:title "TX Entities"               :path [:context :execution :tx-entities]}
   {:title "Data Type"                 :path [:context :execution :data-type]}
   {:title "Queried Data in Database?" :path [:context :execution :data-in-db?]}
   {:title "Mean Time"                 :path [:time :mean]}
   {:title "Median Time"               :path [:time :median]}
   {:title "Time Std"                  :path [:time :std]}
   {:title "Time Count"                :path [:time :count]}
   {:title "Tags"                      :path [:tag]}])

(def db-configs
  [{:config-name "mem-set"
    :config {:store {:backend :mem :id "performance-set"}
             :schema-flexibility :write
             :keep-history? true
             :index :datahike.index/persistent-set
             :name "mem-set"}}
   {:config-name "mem-hht"
    :config {:store {:backend :mem :id "performance-hht"}
             :schema-flexibility :write
             :keep-history? true
             :index :datahike.index/hitchhiker-tree}}
   {:config-name "file"
    :config {:store {:backend :file :path "/tmp/performance-hht"}
             :schema-flexibility :write
             :keep-history? true
             :index :datahike.index/hitchhiker-tree}}])

(def schema
   [{:db/ident       :s1
     :db/valueType   :db.type/string
     :db/cardinality :db.cardinality/one}
    {:db/ident       :s2
     :db/valueType   :db.type/string
     :db/cardinality :db.cardinality/one}
    {:db/ident       :i1
     :db/valueType   :db.type/bigint
     :db/cardinality :db.cardinality/one}
    {:db/ident       :i2
     :db/valueType   :db.type/bigint
     :db/cardinality :db.cardinality/one}])

(defn rand-entity []
   {:s1 (format "%15d" (rand-int max-int))
    :s2 (format "%15d" (rand-int max-int))
    :i1 (rand-int max-int)
    :i2 (rand-int max-int)})

(defn rand-int-not-in [int-set]
  (loop [i (rand-int max-int)]
    (if (contains? int-set i)
      (recur (rand-int max-int))
      i)))

(defn rand-str-not-in [str-set]
  (loop [s (format "%15d" (rand-int max-int))]
    (if (contains? str-set s)
      (recur (rand-int max-int))
      s)))

(defn vec-of [n f]
  (vec (repeatedly n f)))

;;; Queries

(defn simple-query [db attr val]
  {:query (conj '[:find ?e :where]
                (conj '[?e] attr val))
   :args [db]})

(defn one-join-query [db attr1 attr2]
  {:query (conj '[:find ?v1 ?v2 :where]
                (conj '[?e] attr1 '?v1)
                (conj '[?e] attr2 '?v2))
   :args [db]})

(defn one-join-query-first-fixed [db attr1 val1 attr2]
  {:query (conj '[:find ?v2 :where]
                (conj '[?e] attr1 val1)
                (conj '[?e] attr2 '?v2))
   :args [db]})

(defn one-join-query-second-fixed [db attr1 attr2 val2]
  {:query (conj '[:find ?v1 :where]
                (conj '[?e] attr1 '?v1)
                (conj '[?e] attr2 val2))
   :args [db]})

(defn scalar-arg-query [db attr val]
  {:query (conj '[:find ?e
                  :in $ ?v
                  :where]
                (conj '[?e] attr '?v))
   :args [db val]})

(defn vector-arg-query [db attr vals]
  {:query (conj '[:find ?e
                  :in $ ?v
                  :where]
                (conj '[?e] attr '?v))
   :args [db vals]})

(defn less-than-query [db attr]
  {:query (conj '[:find ?e1 ?e2 :where]
                (conj '[?e1] attr '?v1)
                (conj '[?e2] attr '?v2)
                '[(< ?v1 ?v2)])
   :args [db]})

(defn equals-query [db attr]
  {:query (conj '[:find ?e1 ?e2 :where]
                (conj '[?e1] attr '?v1)
                (conj '[?e2] attr '?v2)
                '[(= ?v1 ?v2)])
   :args [db]})

(defn less-than-query-1-fixed [db attr comp-val]
  {:query (conj '[:find ?e :where]
                (conj '[?e] attr '?v)
                (conj '[]
                      (sequence (conj '[= ?v] comp-val))))
   :args [db]})

(defn equals-query-1-fixed [db attr comp-val]
  {:query (conj '[:find ?e :where]
                (conj '[?e] attr '?v)
                (conj '[]
                      (sequence (conj '[= ?v] comp-val))))
   :args [db]})


(defn non-var-queries [db]
  [{:function :one-join-query
    :query (one-join-query db :i1 :i2)
    :details {:data-type :int}}
   {:function :one-join-query
    :query (one-join-query db :s1 :s2)
    :details {:data-type :str}}

   {:function :equals-query
    :query (equals-query db :i1)
    :details {:data-type :int}}
   {:function :equals-query
    :query (equals-query db :s1)
    :details {:data-type :str}}

   {:function :less-than-query
    :query (less-than-query db :i1)
    :details {:data-type :int}}
   #_{:function :less-than-query                          ;; class cast error due comparator
      :query (less-than-query db :s1)
      :details {:data-type :str}}

   {:function :equals-query-1-fixed
    :query (equals-query-1-fixed db :i1 (int (/ max-int 2.0)))
    :details {:data-type :int}}
   {:function :equals-query-1-fixed
    :query (equals-query-1-fixed db :s1 (format "%15d" (int (/ max-int 2.0))))
    :details {:data-type :str}}

   {:function :less-than-query-1-fixed
    :query (less-than-query-1-fixed db :i1 (int (/ max-int 2.0)))
    :details {:data-type :int}}
   {:function :less-than-query-1-fixed
    :query (less-than-query-1-fixed db :s1 (format "%15d" (int (/ max-int 2.0))))
    :details {:data-type :str}}])

(defn var-queries [db entities]
  (let [known-s1 (mapv :s1 entities)
        known-s2 (mapv :s2 entities)
        known-i1 (mapv :i1 entities)
        known-i2 (mapv :i2 entities)
        known-i1-set (set known-i1)
        known-i2-set (set known-i2)
        known-s1-set (set known-s1)
        known-s2-set (set known-s2)]
    [{:function :simple-query
      :query (simple-query db :i1 (rand-nth known-i1))
      :details {:data-type :int :data-in-db? true}}
     {:function :simple-query
      :query (simple-query db :i1 (rand-int-not-in known-i1-set))
      :details {:data-type :int :data-in-db? false}}
     {:function :simple-query
      :query (simple-query db :s1 (rand-nth known-s1))
      :details {:data-type :str :data-in-db? true}}
     {:function :simple-query
      :query (simple-query db :s1 (rand-str-not-in known-i1-set))
      :details {:data-type :str :data-in-db? false}}

     {:function :one-join-query-first-fixed
      :query (one-join-query-first-fixed db :i1 (rand-nth known-i1) :i2)
      :details {:data-type :int :data-in-db? true}}
     {:function :one-join-query-first-fixed
      :query (one-join-query-first-fixed db :i1 (rand-int-not-in known-i1-set) :i2)
      :details {:data-type :int :data-in-db? false}}
     {:function :one-join-query-first-fixed
      :query (one-join-query-first-fixed db :s1 (rand-nth known-s1) :s2)
      :details {:data-type :str :data-in-db? true}}
     {:function :one-join-query-first-fixed
      :query (one-join-query-first-fixed db :s1 (rand-str-not-in known-s1-set) :s2)
      :details {:data-type :str :data-in-db? false}}

     {:function :one-join-query-second-fixed
      :query (one-join-query-second-fixed db :i1 :i2 (rand-nth known-i2))
      :details {:data-type :int :data-in-db? true}}
     {:function :one-join-query-second-fixed
      :query (one-join-query-second-fixed db :i1 :i2 (rand-int-not-in known-i2-set))
      :details {:data-type :int :data-in-db? false}}
     {:function :one-join-query-second-fixed
      :query (one-join-query-second-fixed db :s1 :s2 (rand-nth known-s2))
      :details {:data-type :str :data-in-db? true}}
     {:function :one-join-query-second-fixed
      :query (one-join-query-second-fixed db :s1 :s2 (rand-str-not-in known-s2-set))
      :details {:data-type :str :data-in-db? false}}

     {:function :scalar-arg-query
      :query (scalar-arg-query db :i1 (rand-nth known-i1))
      :details {:data-type :int :data-in-db? true}}
     {:function :scalar-arg-query
      :query (scalar-arg-query db :i1 (rand-int-not-in known-i1-set))
      :details {:data-type :int :data-in-db? false}}
     {:function :scalar-arg-query
      :query (scalar-arg-query db :s1 (rand-nth known-s1))
      :details {:data-type :str :data-in-db? true}}
     {:function :scalar-arg-query
      :query (scalar-arg-query db :s1 (rand-str-not-in known-s1-set))
      :details {:data-type :str :data-in-db? false}}

     {:function :vector-arg-query
      :query (vector-arg-query db :i1 (vec-of 1 #(rand-nth known-i1)))
      :details {:data-type :int :data-in-db? true}}
     {:function :vector-arg-query
      :query (vector-arg-query db :i1 (vec-of 1 #(rand-int-not-in known-i1-set)))
      :details {:data-type :int :data-in-db? false}}
     {:function :vector-arg-query
      :query (vector-arg-query db :s1 (vec-of 1 #(rand-nth known-s1)))
      :details {:data-type :str :data-in-db? true}}
     {:function :vector-arg-query
      :query (vector-arg-query db :s1 (vec-of 1 #(rand-str-not-in known-s1-set)))
      :details {:data-type :str :data-in-db? false}}]))

(defn all-queries [db entities]
  (concat (non-var-queries db)
          (var-queries db entities)))
