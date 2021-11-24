(ns benchmark.config)

(def context-cell-order [:function :dh-config :db-datoms :tx-datoms :data-type :data-in-db?])

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
             :keep-history? false
             :index :datahike.index/persistent-set}}
   {:config-name "mem-hht"
    :config {:store {:backend :mem :id "performance-hht"}
             :schema-flexibility :write
             :keep-history? false
             :index :datahike.index/hitchhiker-tree}}
   {:config-name "file"
    :config {:store {:backend :file :path "/tmp/performance-hht"}
             :schema-flexibility :write
             :keep-history? false
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

(defn rand-str [max-int]
  (format "%15d" (rand-int max-int)))

(defn rand-entity [max-int]
  {:s1 (rand-str max-int)
   :s2 (rand-str max-int)
   :i1 (rand-int max-int)
   :i2 (rand-int max-int)})

(defn known [attr entities]
  (mapv attr entities))

(def m-known (memoize known))

(defn known-set [attr entities]
  (set (m-known attr entities)))

(def m-known-set (memoize known-set))

(defn rand-val-not-in [datatype val-set]
  (let [rand-gen (if (= datatype :int) rand-int rand-str)]
    (loop [i (rand-gen Integer/MAX_VALUE)]
      (if (contains? val-set i)
        (recur (rand-gen Integer/MAX_VALUE))
        i))))

(defn rand-attr-val [datatype attr entities in-set?]
  (if in-set?
    #(rand-nth (m-known attr entities))
    #(rand-val-not-in datatype (m-known-set attr entities))))

(defn vec-of [n f]
  (vec (repeatedly n f)))

;;; Queries

(defn simple-query [db attr val]
  {:query (conj '[:find ?e :where]
                (conj '[?e] attr val))
   :args [db]})

(defn limit-query [db attr]
  {:query (conj '[:find ?e ?v :where] ;; pulls entity-count datoms
                (conj '[?e] attr '?v))
   :offset 0
   :limit 100
   :args [db]})

(defn e-join-query [db attr1 attr2] ;; entity-count res lines
  {:query (conj '[:find ?e :where]
                (conj '[?e] attr1 '?v1) ;; pulls entity-count datoms
                (conj '[?e] attr2 '?v2)) ;; pulls entity-count datoms
   :args [db]})

(defn a-join-query [db attr] ;; entity-count res lines
  {:query (conj '[:find ?v1 ?v2 :where]
                (conj '[?e1] attr '?v1) ;; pulls entity-count datoms
                (conj '[?e2] attr '?v2)) ;; pulls entity-count datoms
   :args [db]})

(defn v-join-query [db attr1 attr2] ;; on average entity-count res lines
  {:query (conj '[:find ?e1 ?e2 :where]
                (conj '[?e1] attr1 '?v) ;; pulls entity-count datoms
                (conj '[?e2] attr2 '?v)) ;; pulls entity-count datoms
   :args [db]})

(defn e-join-query-first-fixed [db attr1 val1 attr2]
  {:query (conj '[:find ?v2 :where]
                (conj '[?e] attr1 val1)
                (conj '[?e] attr2 '?v2))
   :args [db]})

(defn e-join-query-second-fixed [db attr1 attr2 val2]
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

(defn scalar-arg-query-with-join [db attr val]
  {:query (conj '[:find ?e1 ?e2 ?v2
                  :in $ ?v1
                  :where]
                (conj '[?e1] attr '?v1)
                (conj '[?e2] attr '?v2))
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

(defn non-var-queries [db datatypes max-int]
  (apply concat
         (for [data-type datatypes]
           (let [[attr1 attr2 middle-elem] (if (= data-type :int)
                                             [:i1 :i2 (int (/ max-int 2.0))]
                                             [:s1 :s2 (format "%15d" (int (/ max-int 2.0)))])]

             [{:function :e-join-query
               :query (e-join-query db attr1 attr2)
               :details {:data-type data-type}}

              {:function :a-join-query
               :query (a-join-query db attr1)
               :details {:data-type data-type}}

              {:function :v-join-query
               :query (v-join-query db attr1 attr2)
               :details {:data-type data-type}}

              {:function :equals-query
               :query (equals-query db attr1)
               :details {:data-type data-type}}

              {:function :less-than-query
               :query (less-than-query db attr1)
               :details {:data-type data-type}}

              {:function :equals-query-1-fixed
               :query (equals-query-1-fixed db attr1 middle-elem)
               :details {:data-type data-type}}

              {:function :less-than-query-1-fixed
               :query (less-than-query-1-fixed db attr1 middle-elem)
               :details {:data-type data-type}}
               
               {:function :limit-query
                :query (limit-query db attr1)
                :details {:data-type data-type}}]))))

(defn var-queries [db entities datatypes data-found-opts]
  (apply concat
         (for [data-type datatypes
               data-in-db? data-found-opts]

           (let [attr1 (if (= data-type :int) :i1 :s1)
                 attr2 (if (= data-type :int) :i2 :s2)
                 rand-val1 (rand-attr-val data-type attr1 entities data-in-db?)
                 rand-val2 (rand-attr-val data-type attr2 entities data-in-db?)]

             [{:function :simple-query
               :query (simple-query db attr1 (rand-val1))
               :details {:data-type data-type :data-in-db? data-in-db?}}

              {:function :e-join-query-first-fixed
               :query (e-join-query-first-fixed db :i1 (rand-val1) :i2)
               :details {:data-type data-type :data-in-db? data-in-db?}}

              {:function :e-join-query-second-fixed
               :query (e-join-query-second-fixed db :i1 :i2 (rand-val2))
               :details {:data-type data-type :data-in-db? data-in-db?}}

              {:function :scalar-arg-query
               :query (scalar-arg-query db :i1 (rand-val1))
               :details {:data-type data-type :data-in-db? data-in-db?}}

              {:function :scalar-arg-query-with-join
               :query (scalar-arg-query-with-join db :i1 (rand-val1))
               :details {:data-type data-type :data-in-db? data-in-db?}}

              {:function :vector-arg-query
               :query (vector-arg-query db :i1 (vec-of 10 rand-val1))
               :details {:data-type data-type :data-in-db? data-in-db?}}]))))

(defn cache-check-queries [db entities datatypes data-found-opts]
  (apply concat
         (for [data-type datatypes
               data-in-db? data-found-opts]
           (let [attr (if (= data-type :int) :i1 :s1)
                 rand-val (rand-attr-val data-type attr entities data-in-db?)
                 val (rand-val)]
             [{:function :simple-query-first-run
               :query (simple-query db attr val)
               :details {:data-type data-type :data-in-db? data-in-db?}}
              {:function :simple-query-second-run
               :query (simple-query db attr val)
               :details {:data-type data-type :data-in-db? data-in-db?}}]))))

(defn aggregate-queries [entities]
  [{:function :sum-query
    :query {:query '[:find (sum ?x)
                     :in [?x ...]]
            :args [(repeatedly (count entities) #(rand-int 100))]}
    :details {:data-type :int}}
   {:function :avg-query
    :query {:query '[:find (avg ?x)
                     :in [?x ...]]
            :args [(repeatedly (count entities) #(rand-int 100))]}
    :details {:data-type :int}}
   {:function :median-query
    :query {:query '[:find (median ?x)
                     :in [?x ...]]
            :args [(repeatedly (count entities) #(rand-int 100))]}
    :details {:data-type :int}}
   {:function :variance-query
    :query {:query '[:find (variance ?x)
                     :in [?x ...]]
            :args [(repeatedly (count entities) #(rand-int 100))]}
    :details {:data-type :int}}
   {:function :stddev-query
    :query {:query '[:find (stddev ?x)
                     :in [?x ...]]
            :args [(repeatedly (count entities) #(rand-int 100))]}
    :details {:data-type :int}}
   {:function :max-query
    :query {:query '[:find (max ?x)
                     :in [?x ...]]
            :args [(repeatedly (count entities) #(rand-int 100))]}
    :details {:data-type :int}}])

(defn all-queries [db entities datatypes data-found-opts]
  (concat (non-var-queries db datatypes (count entities))
          (var-queries db entities datatypes data-found-opts)
          (cache-check-queries db entities datatypes data-found-opts)
          (aggregate-queries entities)))
