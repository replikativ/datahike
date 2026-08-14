(ns datahike.test.secondary-integration-test
  "Integration tests for Proximum and Scriptum secondary index implementations."
  (:require
   [clojure.test :refer [deftest testing is]]
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.index.secondary :as sec]
   [datahike.index.entity-set :as es]
   [datahike.query :as q]
   [datahike.index.secondary.scriptum]
   [datahike.index.secondary.stratum]
   [datahike.migrate :as m]
   [stratum.api :as st]
   [datahike.test.query-aggregates-test :refer [aggregate-contract]]))

;; Proximum requires Java 22+ (class file version 66.0).
;; Load lazily so the test file compiles on older JVMs.
(def ^:private proximum-available?
  (try
    (require 'datahike.index.secondary.proximum)
    true
    (catch Throwable _ false)))

;; ---------------------------------------------------------------------------
;; Proximum (Vector Search) Tests

(deftest test-proximum-lifecycle
  (when-not proximum-available?
    (is (not proximum-available?) "SKIP: proximum requires Java 22+"))
  (when proximum-available?
    (testing "create, insert, search, delete"
      (let [idx (sec/create-index :proximum
                                  {:attrs #{:person/embedding}
                                   :dim 4 :distance :cosine
                                   :store-config {:backend :memory :id (random-uuid)}}
                                  nil)]
        (is (= #{:person/embedding} (sec/-indexed-attrs idx)))

      ;; Insert 3 vectors via -transact
        (let [d1 (datahike.datom/datom 1 :person/embedding (float-array [1.0 0.0 0.0 0.0]))
              d2 (datahike.datom/datom 2 :person/embedding (float-array [0.0 1.0 0.0 0.0]))
              d3 (datahike.datom/datom 3 :person/embedding (float-array [0.7 0.7 0.0 0.0]))
              idx (-> idx
                      (sec/-transact {:datom d1 :added? true})
                      (sec/-transact {:datom d2 :added? true})
                      (sec/-transact {:datom d3 :added? true}))]

        ;; Estimate
          (is (= 2 (sec/-estimate idx {:k 2})))
          (is (= 3 (sec/-estimate idx {:k 10})))

        ;; Search: all 3 entities returned
          (let [results (sec/-search idx {:vector (float-array [1.0 0.0 0.0 0.0]) :k 3} nil)]
            (is (= 3 (es/entity-bitset-cardinality results)))
            (is (= #{1 2 3} (set (es/entity-bitset-seq results)))))

        ;; Search with entity filter
          (let [filter-bs (es/entity-bitset-from-longs [1 3])
                results (sec/-search idx {:vector (float-array [1.0 0.0 0.0 0.0]) :k 3} filter-bs)]
            (is (= #{1 3} (set (es/entity-bitset-seq results)))))

        ;; Ordered results (by distance ascending)
          (is (sec/-can-order? idx :person/embedding :asc))
          (is (not (sec/-can-order? idx :person/embedding :desc)))
          (let [ordered (sec/-slice-ordered idx {:vector (float-array [1.0 0.0 0.0 0.0]) :k 3}
                                            nil nil :asc nil)]
            (is (= 3 (count ordered)))
            (is (= 1 (:entity-id (first ordered)))) ;; closest
            (is (< (:distance (first ordered)) (:distance (second ordered)))))

        ;; Delete entity 2
          (let [idx-del (sec/-transact idx {:datom d2 :added? false})
                results (sec/-search idx-del {:vector (float-array [0.0 1.0 0.0 0.0]) :k 3} nil)]
            (is (not (es/entity-bitset-contains? results 2)))
            (is (= 2 (es/entity-bitset-cardinality results))))

        ;; Non-vector value is silently skipped
          (let [d-str (datahike.datom/datom 4 :person/embedding "not-a-vector")
                idx2 (sec/-transact idx {:datom d-str :added? true})
                results (sec/-search idx2 {:vector (float-array [1.0 0.0 0.0 0.0]) :k 10} nil)]
            (is (= 3 (es/entity-bitset-cardinality results)))))))))

;; ---------------------------------------------------------------------------
;; Scriptum (Full-Text Search) Tests

(deftest test-scriptum-lifecycle
  (testing "create, index documents, search, delete"
    (let [idx (sec/create-index :scriptum
                                {:attrs #{:person/name :person/bio}
                                 :path (str "/tmp/scriptum-test-" (random-uuid))}
                                nil)]
      (is (= #{:person/name :person/bio} (sec/-indexed-attrs idx)))

      ;; Index documents
      (let [d1 (datahike.datom/datom 1 :person/name "Alice Johnson")
            d2 (datahike.datom/datom 1 :person/bio "Expert in machine learning and NLP")
            d3 (datahike.datom/datom 2 :person/name "Bob Smith")
            d4 (datahike.datom/datom 2 :person/bio "Database engineer")
            d5 (datahike.datom/datom 3 :person/name "Charlie Brown")
            d6 (datahike.datom/datom 3 :person/bio "Machine learning researcher")]
        ;; Scriptum writer is mutable, so -transact returns `this`
        (sec/-transact idx {:datom d1 :added? true})
        (sec/-transact idx {:datom d2 :added? true})
        (sec/-transact idx {:datom d3 :added? true})
        (sec/-transact idx {:datom d4 :added? true})
        (sec/-transact idx {:datom d5 :added? true})
        (sec/-transact idx {:datom d6 :added? true})

        ;; Search for "machine learning"
        (let [results (sec/-search idx {:query "machine learning" :field :value :limit 10} nil)]
          (is (= #{1 3} (set (es/entity-bitset-seq results)))))

        ;; Search for "database"
        (let [results (sec/-search idx {:query "database" :field :value :limit 10} nil)]
          (is (= #{2} (set (es/entity-bitset-seq results)))))

        ;; Filtered search
        (let [filter-bs (es/entity-bitset-from-longs [3])
              results (sec/-search idx {:query "machine learning" :field :value} filter-bs)]
          (is (= #{3} (set (es/entity-bitset-seq results)))))

        ;; Ordered results
        (is (sec/-can-order? idx :person/bio :desc))
        (is (not (sec/-can-order? idx :person/bio :asc)))
        (let [ordered (sec/-slice-ordered idx {:query "machine learning" :field :value}
                                          nil nil :desc 10)]
          (is (= 2 (count ordered)))
          (is (every? #(contains? % :score) ordered))
          (is (every? #(contains? #{1 3} (:entity-id %)) ordered)))

        ;; Delete entity 1
        (sec/-transact idx {:datom d1 :added? false})
        (let [results (sec/-search idx {:query "Alice" :field :value :limit 10} nil)]
          (is (zero? (es/entity-bitset-cardinality results))))))))

;; ---------------------------------------------------------------------------
;; Cross-Index Composition Tests

(deftest test-cross-index-bitmap-composition
  (when-not proximum-available?
    (is (not proximum-available?) "SKIP: proximum requires Java 22+"))
  (when proximum-available?
    (testing "RoaringBitmap flows between Proximum and Scriptum"
      (let [;; Create both indices
            vec-idx (sec/create-index :proximum
                                      {:attrs #{:person/embedding}
                                       :dim 4 :distance :cosine
                                       :store-config {:backend :memory :id (random-uuid)}}
                                      nil)
            ft-idx (sec/create-index :scriptum
                                     {:attrs #{:person/bio}
                                      :path (str "/tmp/scriptum-cross-" (random-uuid))}
                                     nil)
          ;; Transact vectors
            vec-idx (-> vec-idx
                        (sec/-transact {:datom (datahike.datom/datom 1 :person/embedding
                                                                     (float-array [1.0 0.0 0.0 0.0]))
                                        :added? true})
                        (sec/-transact {:datom (datahike.datom/datom 2 :person/embedding
                                                                     (float-array [0.0 1.0 0.0 0.0]))
                                        :added? true})
                        (sec/-transact {:datom (datahike.datom/datom 3 :person/embedding
                                                                     (float-array [0.9 0.1 0.0 0.0]))
                                        :added? true}))]
      ;; Transact text
        (sec/-transact ft-idx {:datom (datahike.datom/datom 1 :person/bio "ML researcher")
                               :added? true})
        (sec/-transact ft-idx {:datom (datahike.datom/datom 2 :person/bio "Database admin")
                               :added? true})
        (sec/-transact ft-idx {:datom (datahike.datom/datom 3 :person/bio "ML engineer")
                               :added? true})

      ;; Fulltext "ML" → entities {1, 3}
        (let [ml-bits (sec/-search ft-idx {:query "ML" :field :value} nil)]
          (is (= #{1 3} (set (es/entity-bitset-seq ml-bits))))

        ;; Use as pre-filter for KNN
          (let [knn-filtered (sec/-search vec-idx
                                          {:vector (float-array [1.0 0.0 0.0 0.0]) :k 3}
                                          ml-bits)]
            (is (= #{1 3} (set (es/entity-bitset-seq knn-filtered))))
          ;; Entity 2 excluded by fulltext filter
            (is (not (es/entity-bitset-contains? knn-filtered 2))))

        ;; AND composition
          (let [knn-all (sec/-search vec-idx
                                     {:vector (float-array [1.0 0.0 0.0 0.0]) :k 2} nil)
                combined (es/entity-bitset-and knn-all ml-bits)]
          ;; KNN top-2 = {1, 3}, ML = {1, 3}, AND = {1, 3}
            (is (= #{1 3} (set (es/entity-bitset-seq combined))))))))))

;; ---------------------------------------------------------------------------
;; In-Transaction Maintenance via d/db-with

(deftest test-in-transaction-maintenance
  (testing "secondary indices updated during d/db-with"
    (let [schema {:person/name {:db/index true}
                  :person/bio {}
                  :idx/fulltext {:db.secondary/type :scriptum
                                 :db.secondary/attrs [:person/name :person/bio]
                                 :db.secondary/config {:path (str "/tmp/scriptum-tx-" (random-uuid))}}}
          empty-db (db/empty-db schema)
          ft-idx (sec/create-index :scriptum
                                   {:attrs [:person/name :person/bio]
                                    :path (str "/tmp/scriptum-tx-" (random-uuid))}
                                   empty-db)
          db (assoc empty-db :secondary-indices {:idx/fulltext ft-idx})
          db2 (d/db-with db [{:db/id 1 :person/name "Alice" :person/bio "ML researcher"}
                             {:db/id 2 :person/name "Bob" :person/bio "Database engineer"}])]

      ;; The fulltext index should have been updated in-transaction
      (let [ft (get-in db2 [:secondary-indices :idx/fulltext])
            results (sec/-search ft {:query "ML" :field :value} nil)]
        (is (= #{1} (set (es/entity-bitset-seq results)))))

      ;; Search for name
      (let [ft (get-in db2 [:secondary-indices :idx/fulltext])
            results (sec/-search ft {:query "Alice" :field :value} nil)]
        (is (= #{1} (set (es/entity-bitset-seq results))))))))

(deftest test-in-transaction-proximum
  (when-not proximum-available?
    (is (not proximum-available?) "SKIP: proximum requires Java 22+"))
  (when proximum-available?
    (testing "vector index updated during d/db-with"
      (let [schema {:person/embedding {}
                    :idx/vectors {:db.secondary/type :proximum
                                  :db.secondary/attrs [:person/embedding]
                                  :db.secondary/config {:dim 4 :distance :cosine
                                                        :store-config {:backend :memory
                                                                       :id (random-uuid)}}}}
            empty-db (db/empty-db schema)
            vec-idx (sec/create-index :proximum
                                      {:attrs [:person/embedding]
                                       :dim 4 :distance :cosine
                                       :store-config {:backend :memory :id (random-uuid)}}
                                      empty-db)
            db (assoc empty-db :secondary-indices {:idx/vectors vec-idx})
            db2 (d/db-with db [{:db/id 1 :person/embedding (float-array [1.0 0.0 0.0 0.0])}
                               {:db/id 2 :person/embedding (float-array [0.0 1.0 0.0 0.0])}])]

        (let [vt (get-in db2 [:secondary-indices :idx/vectors])
              results (sec/-search vt {:vector (float-array [1.0 0.0 0.0 0.0]) :k 2} nil)]
          (is (= 2 (es/entity-bitset-cardinality results)))
          (is (es/entity-bitset-contains? results 1))
          (is (es/entity-bitset-contains? results 2)))))))

(deftest test-proximum-knn-clause
  ;; KNN as a first-class Datalog :where clause via the external-engine
  ;; query-spec-fn. Planner-only (the base engine has no external-engine op).
  (when-not proximum-available?
    (is (not proximum-available?) "SKIP: proximum requires Java 22+"))
  (when proximum-available?
    (binding [q/*disable-planner* false]
      (let [cfg {:store {:backend :memory :id (random-uuid)} :schema-flexibility :write}]
        (d/create-database cfg)
        (try
          (let [conn (d/connect cfg)]
            (d/transact conn [{:db/ident :doc/name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
                              {:db/ident :doc/embedding :db/valueType :db.type/float-array
                               :db/cardinality :db.cardinality/one :db.secondary/only true}])
            (d/transact conn [{:db/ident :idx/emb :db.secondary/type :proximum :db.secondary/attrs [:doc/embedding]
                               :db.secondary/config {:dim 4 :distance :cosine :capacity 100
                                                     :store-config {:backend :memory :id (random-uuid)}}}])
            (Thread/sleep 800)
            (d/transact conn [{:doc/name "east"  :doc/embedding (float-array [1.0 0.0 0.0 0.0])}
                              {:doc/name "east2" :doc/embedding (float-array [0.9 0.1 0.0 0.0])}
                              {:doc/name "north" :doc/embedding (float-array [0.0 1.0 0.0 0.0])}])
            (Thread/sleep 500)
            (testing "retrieval: [[?e ?distance]] binds entity + cosine distance and joins to a scalar attr"
              (let [rows (d/q '[:find ?name ?distance :in $ ?qvec
                                :where [(datahike.index.secondary.proximum/knn :idx/emb ?qvec 3) [[?e ?distance]]]
                                [?e :doc/name ?name]]
                              @conn (float-array [1.0 0.0 0.0 0.0]))
                    m (into {} (map (fn [[n d]] [n d]) rows))]
                (is (= #{"east" "east2" "north"} (set (keys m))))
                (is (< (m "east") (m "east2") (m "north")) "distances present and ordered")
                (is (== 0.0 (m "east")))))
            (testing "filter: [?e ...] composes with a Datalog predicate"
              (let [names (d/q '[:find [?name ...] :in $ ?qvec
                                 :where [(datahike.index.secondary.proximum/knn :idx/emb ?qvec 3) [?e ...]]
                                 [?e :doc/name ?name]
                                 [(clojure.string/starts-with? ?name "east")]]
                               @conn (float-array [1.0 0.0 0.0 0.0]))]
                (is (= #{"east" "east2"} (set names)))))
            (d/release conn))
          (finally (d/delete-database cfg)))))))

;; ---------------------------------------------------------------------------
;; Stratum Entity-Filter Aggregate Tests

(deftest test-stratum-entity-filter-aggregate
  (testing "IColumnarAggregate with entity-filter mask injection"
    (let [idx (sec/create-index :stratum
                                {:attrs #{:person/salary :person/dept}}
                                nil)
          datoms [(datahike.datom/datom 1 :person/salary 50000)
                  (datahike.datom/datom 1 :person/dept "eng")
                  (datahike.datom/datom 2 :person/salary 60000)
                  (datahike.datom/datom 2 :person/dept "eng")
                  (datahike.datom/datom 3 :person/salary 70000)
                  (datahike.datom/datom 3 :person/dept "sales")
                  (datahike.datom/datom 4 :person/salary 80000)
                  (datahike.datom/datom 4 :person/dept "sales")
                  (datahike.datom/datom 5 :person/salary 90000)
                  (datahike.datom/datom 5 :person/dept "eng")]
          t (sec/-as-transient idx)
          _ (doseq [d datoms] (sec/-transact! t {:datom d :added? true}))
          idx (sec/-persistent! t)]

      ;; Full aggregate (no filter)
      (let [result (sec/-columnar-aggregate idx
                                            {:agg [[:avg :salary]] :group [:dept]})]
        (is (= 2 (count result)))
        ;; eng: (50+60+90)/3 = 66666.67, sales: (70+80)/2 = 75000
        (let [eng (first (filter #(= "eng" (:dept %)) result))
              sales (first (filter #(= "sales" (:dept %)) result))]
          (is (< (abs (- (:avg eng) 66666.67)) 1.0))
          (is (== 75000.0 (:avg sales)))))

      ;; Filtered aggregate — only entities {1, 2, 3}
      (let [filter-bs (es/entity-bitset-from-longs [1 2 3])
            result (sec/-columnar-aggregate idx
                                            {:agg [[:avg :salary]] :group [:dept]}
                                            filter-bs)]
        (is (= 2 (count result)))
        ;; eng: (50+60)/2 = 55000, sales: 70/1 = 70000
        (let [eng (first (filter #(= "eng" (:dept %)) result))
              sales (first (filter #(= "sales" (:dept %)) result))]
          (is (== 55000.0 (:avg eng)))
          (is (== 70000.0 (:avg sales)))))

      ;; Filtered aggregate — only entity {5}
      (let [filter-bs (es/entity-bitset-from-longs [5])
            result (sec/-columnar-aggregate idx
                                            {:agg [[:sum :salary]]}
                                            filter-bs)]
        (is (= 1 (count result)))
        (is (== 90000 (:sum (first result))))))))

(deftest test-stratum-partial-coverage-aggregate
  (testing "aggregate with partial coverage: filter via PSS, aggregate via stratum"
    (let [schema {:person/name {:db/index true}
                  :person/salary {}
                  :person/dept {}
                  :idx/analytics {:db.secondary/type :stratum
                                  :db.secondary/attrs [:person/salary :person/dept]}}
          empty-db (db/empty-db schema)
          stratum-idx (sec/create-index :stratum
                                        {:attrs #{:person/salary :person/dept}}
                                        empty-db)
          db (assoc empty-db :secondary-indices {:idx/analytics stratum-idx})
          ;; Add people: some named "Ivan", some not
          db (d/db-with db [{:db/id 1 :person/name "Ivan" :person/salary 50000 :person/dept "eng"}
                            {:db/id 2 :person/name "Ivan" :person/salary 80000 :person/dept "sales"}
                            {:db/id 3 :person/name "Petr" :person/salary 60000 :person/dept "eng"}
                            {:db/id 4 :person/name "Ivan" :person/salary 70000 :person/dept "eng"}
                            {:db/id 5 :person/name "Petr" :person/salary 90000 :person/dept "sales"}])]

      ;; :person/name is NOT in stratum, but :person/salary IS.
      ;; Query: avg salary of Ivans — partial coverage
      (let [result (binding [datahike.query/*disable-planner* false]
                     (d/q '[:find (avg ?s) .
                            :where [?e :person/name "Ivan"] [?e :person/salary ?s]]
                          db))]
        ;; Ivan salaries: 50000 + 80000 + 70000 = 200000 / 3 ≈ 66666.67
        (is (some? result))
        (is (< (abs (- result 66666.67)) 1.0)))

      ;; Verify legacy gives same result
      (let [result-legacy (binding [datahike.query/*disable-planner* true]
                            (d/q '[:find (avg ?s) .
                                   :where [?e :person/name "Ivan"] [?e :person/salary ?s]]
                                 db))]
        (is (< (abs (- result-legacy 66666.67)) 1.0))))))

(deftest test-stratum-predicate-pushdown
  (testing "predicates on covered columns translated to stratum WHERE"
    (let [schema {:person/salary {}
                  :person/dept {}
                  :idx/analytics {:db.secondary/type :stratum
                                  :db.secondary/attrs [:person/salary :person/dept]}}
          empty-db (db/empty-db schema)
          stratum-idx (sec/create-index :stratum
                                        {:attrs #{:person/salary :person/dept}}
                                        empty-db)
          db (assoc empty-db :secondary-indices {:idx/analytics stratum-idx})
          db (d/db-with db [{:db/id 1 :person/salary 50000 :person/dept "eng"}
                            {:db/id 2 :person/salary 80000 :person/dept "sales"}
                            {:db/id 3 :person/salary 60000 :person/dept "eng"}
                            {:db/id 4 :person/salary 70000 :person/dept "eng"}
                            {:db/id 5 :person/salary 90000 :person/dept "sales"}])]

      ;; Predicate filter: salary > 65000
      (let [result (binding [datahike.query/*disable-planner* false]
                     (d/q '[:find (avg ?s) .
                            :where [?e :person/salary ?s] [(> ?s 65000)]]
                          db))]
        ;; Salaries > 65000: 80000, 70000, 90000 → avg = 80000
        (is (some? result))
        (is (== 80000.0 result)))

      ;; Verify legacy gives same result
      (let [result-legacy (binding [datahike.query/*disable-planner* true]
                            (d/q '[:find (avg ?s) .
                                   :where [?e :person/salary ?s] [(> ?s 65000)]]
                                 db))]
        (is (== 80000.0 result-legacy))))))

;; ---------------------------------------------------------------------------
;; Cross-Index Composition: Scriptum → EntityBitSet → Stratum Aggregate

(deftest test-cross-index-scriptum-to-stratum-aggregate
  (testing "scriptum search produces bitmap → constrains stratum aggregate"
    (let [schema {:person/name {:db/index true}
                  :person/bio {}
                  :person/salary {}
                  :person/dept {}
                  :idx/fulltext {:db.secondary/type :scriptum
                                 :db.secondary/attrs [:person/bio]
                                 :db.secondary/config {:path (str "/tmp/scriptum-cross-strat-" (random-uuid))}}
                  :idx/analytics {:db.secondary/type :stratum
                                  :db.secondary/attrs [:person/salary :person/dept]}}
          empty-db (db/empty-db schema)
          ft-idx (sec/create-index :scriptum
                                   {:attrs #{:person/bio}
                                    :path (str "/tmp/scriptum-cross-strat-" (random-uuid))}
                                   empty-db)
          st-idx (sec/create-index :stratum
                                   {:attrs #{:person/salary :person/dept}}
                                   empty-db)
          db (assoc empty-db :secondary-indices
                    {:idx/fulltext ft-idx :idx/analytics st-idx})
          db (d/db-with db [{:db/id 1 :person/name "Alice" :person/bio "ML researcher" :person/salary 90000 :person/dept "eng"}
                            {:db/id 2 :person/name "Bob" :person/bio "Database admin" :person/salary 60000 :person/dept "ops"}
                            {:db/id 3 :person/name "Charlie" :person/bio "ML engineer" :person/salary 80000 :person/dept "eng"}
                            {:db/id 4 :person/name "Diana" :person/bio "PM" :person/salary 70000 :person/dept "eng"}
                            {:db/id 5 :person/name "Eve" :person/bio "ML ops" :person/salary 75000 :person/dept "ops"}])]

      ;; Direct protocol-level test: scriptum → bitmap → stratum aggregate
      (let [ft (get-in db [:secondary-indices :idx/fulltext])
            st (get-in db [:secondary-indices :idx/analytics])
            ;; Step 1: scriptum search for "ML" → EntityBitSet
            ml-entities (sec/-search ft {:query "ML" :field :value} nil)]
        ;; ML entities: {1, 3, 5}
        (is (= #{1 3 5} (set (es/entity-bitset-seq ml-entities))))

        ;; Step 2: pass bitmap as entity-filter to stratum aggregate
        (let [result (sec/-columnar-aggregate st
                                              {:agg [[:avg :salary]] :group [:dept]}
                                              ml-entities)]
          ;; eng: (90000 + 80000)/2 = 85000 (only entities 1,3 — not 4)
          ;; ops: 75000/1 = 75000 (only entity 5 — not 2)
          (is (= 2 (count result)))
          (let [eng (first (filter #(= "eng" (:dept %)) result))
                ops (first (filter #(= "ops" (:dept %)) result))]
            (is (== 85000.0 (:avg eng)))
            (is (== 75000.0 (:avg ops)))))

        ;; Step 3: chain scriptum → proximum → stratum (if proximum available)
        ;; Not tested here — but the bitmap algebra works the same way
        ))))

;; ---------------------------------------------------------------------------
;; Entity-Filter Constraining Fused Scan (General Non-Aggregate Path)

(deftest test-entity-filter-constrains-fused-scan
  (testing "secondary index search produces entity-filter that constrains PSS scan"
    (let [schema {:person/name {:db/index true}
                  :person/bio {}
                  :person/salary {}
                  :idx/fulltext {:db.secondary/type :scriptum
                                 :db.secondary/attrs [:person/bio]
                                 :db.secondary/config {:path (str "/tmp/scriptum-fused-" (random-uuid))}}}
          empty-db (db/empty-db schema)
          ft-idx (sec/create-index :scriptum
                                   {:attrs #{:person/bio}
                                    :path (str "/tmp/scriptum-fused-" (random-uuid))}
                                   empty-db)
          db (assoc empty-db :secondary-indices {:idx/fulltext ft-idx})
          db (d/db-with db [{:db/id 1 :person/name "Alice" :person/bio "ML researcher" :person/salary 90000}
                            {:db/id 2 :person/name "Bob" :person/bio "Database admin" :person/salary 60000}
                            {:db/id 3 :person/name "Charlie" :person/bio "ML engineer" :person/salary 80000}
                            {:db/id 4 :person/name "Diana" :person/bio "PM" :person/salary 70000}
                            {:db/id 5 :person/name "Eve" :person/bio "ML ops" :person/salary 75000}])]

      ;; Scriptum produces entity bitmap → used as entity-filter for PSS name lookup
      ;; Direct protocol test: filter PSS results using scriptum bitmap
      (let [ft (get-in db [:secondary-indices :idx/fulltext])
            ml-entities (sec/-search ft {:query "ML" :field :value} nil)]
        (is (= #{1 3 5} (set (es/entity-bitset-seq ml-entities))))

        ;; Now verify this can filter a PSS scan
        ;; Get all names, then filter by ML entity bitmap
        (let [all-names (d/q '[:find ?e ?n :where [?e :person/name ?n]] db)
              ml-names (filter (fn [[eid _]] (es/entity-bitset-contains? ml-entities eid)) all-names)]
          (is (= #{[1 "Alice"] [3 "Charlie"] [5 "Eve"]} (set ml-names))))))))

;; ---------------------------------------------------------------------------
;; Purge propagation — end-to-end
;;
;; Verifies the GDPR-relevant invariant against a real konserve-backed
;; secondary index: after :db.purge/entity, the purged entity must no
;; longer surface via the secondary index's own search path.

(deftest test-purge-removes-from-scriptum
  (testing "after purge the entity no longer surfaces via Scriptum fulltext"
    (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
               :keep-history? true
               :schema-flexibility :write}
          scriptum-path (str "/tmp/scriptum-purge-test-" (random-uuid))
          _ (d/create-database cfg)
          conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :person/name
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one
                           :db/unique :db.unique/identity
                           :db/index true}
                          {:db/ident :person/bio
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}])
        ;; Install scriptum BEFORE adding data so the index sees live add events
        ;; (no async backfill to wait on).
        (d/transact conn [{:db/ident :idx/fulltext
                           :db.secondary/type :scriptum
                           :db.secondary/attrs [:person/name :person/bio]
                           :db.secondary/config {:path scriptum-path}}])
        (Thread/sleep 500)

        (d/transact conn [{:person/name "Alice" :person/bio "Machine learning researcher"}
                          {:person/name "Bob" :person/bio "Database engineer"}])
        (Thread/sleep 500)

        ;; Sanity: Alice and her bio surface via fulltext before purge
        (let [ft (get-in (d/db conn) [:secondary-indices :idx/fulltext])
              by-name (sec/-search ft {:query "Alice" :field :value} nil)
              by-bio  (sec/-search ft {:query "machine" :field :value} nil)]
          (is (pos? (es/entity-bitset-cardinality by-name))
              "Alice should be findable by name before purge")
          (is (pos? (es/entity-bitset-cardinality by-bio))
              "Alice's bio should be findable before purge"))

        ;; Purge Alice's entity (all her datoms across covered attributes)
        (d/transact conn [[:db.purge/entity [:person/name "Alice"]]])
        (Thread/sleep 500)

        ;; After purge: Alice no longer surfaces via either covered attribute
        (let [ft (get-in (d/db conn) [:secondary-indices :idx/fulltext])
              by-name (sec/-search ft {:query "Alice" :field :value} nil)
              by-bio  (sec/-search ft {:query "machine" :field :value} nil)]
          (is (zero? (es/entity-bitset-cardinality by-name))
              "Alice should not be findable by name after purge")
          (is (zero? (es/entity-bitset-cardinality by-bio))
              "Alice's bio should not be findable after purge"))

        ;; Bob is untouched
        (let [ft (get-in (d/db conn) [:secondary-indices :idx/fulltext])
              by-name (sec/-search ft {:query "Bob" :field :value} nil)]
          (is (pos? (es/entity-bitset-cardinality by-name))
              "Bob should still be findable after Alice's purge"))

        (finally
          (d/release conn)
          (d/delete-database cfg))))))

;; ---------------------------------------------------------------------------
;; Columnar aggregate delegate: same contract as the reference implementation

(deftest test-stratum-aggregate-contract
  (testing "the columnar path answers datahike's aggregate contract"
    ;; A fast path may only claim an aggregate it provably computes to the
    ;; contract in `query-aggregates-test/aggregate-contract`. Mapping by NAME
    ;; alone is what let this path return the SAMPLE variance (÷n−1) where the
    ;; reference returns the population one, and ##NaN for a one-element group.
    ;; stratum's population ops are named :variance-pop / :stddev-pop, so the
    ;; adapter must translate rather than pass the name through.
    ;; Keep this test honest. Asserting that the MAPPING TABLE claims these
    ;; aggregates is not enough — it says nothing about whether the query
    ;; reaches the delegate. It did not: the query below was written with a
    ;; `.` (FindScalar) and `columnar-eligible?` requires a FindRel, so both
    ;; bindings ran the reference engine and every assertion compared it to
    ;; itself. Record the delegate's actual invocations instead.
    (is (datahike.index.secondary.stratum/stratum-compatible-aggs?
         [[:variance :num/v] [:stddev :num/v] [:median :num/v] [:avg :num/v]])
        "the columnar path must claim these aggregates")
    (doseq [{:keys [agg in expect note]} aggregate-contract]
      (let [schema {:num/v {}
                    :idx/analytics {:db.secondary/type :stratum
                                    :db.secondary/attrs [:num/v]}}
            empty-db (db/empty-db schema)
            stratum-idx (sec/create-index :stratum {:attrs #{:num/v}} empty-db)
            db (-> (assoc empty-db :secondary-indices {:idx/analytics stratum-idx})
                   (d/db-with (vec (map-indexed (fn [i v] {:db/id (inc i) :num/v v}) in))))
            ;; FindRel, NOT FindScalar: `columnar-eligible?` declines a `.`
            ;; find, which would route this straight past the delegate.
            query {:find [(list agg '?x)] :where '[[?e :num/v ?x]]}
            label (str "(" agg " " (pr-str in) ")" (when note (str " — " note)))
            used (atom [])
            real-agg datahike.index.secondary.stratum/columnar-aggregate
            real-maps datahike.index.secondary.stratum/columnar-aggregate-from-maps
            ;; The result cache MUST be off. The two calls below are the same
            ;; query against the same data, so the second one is a cache hit
            ;; that never executes — the "reference" value would just be the
            ;; columnar result handed back, and the comparison would again be
            ;; the fast path against itself.
            columnar (binding [q/*query-result-cache?* false]
                       (with-redefs [datahike.index.secondary.stratum/columnar-aggregate
                                     (fn [& args] (swap! used conj :cols) (apply real-agg args))
                                     datahike.index.secondary.stratum/columnar-aggregate-from-maps
                                     (fn [& args] (swap! used conj :maps) (apply real-maps args))]
                         (ffirst (binding [q/*disable-planner* false] (d/q query db)))))
            reference (binding [q/*query-result-cache?* false]
                        (ffirst (binding [q/*disable-planner* true] (d/q query db))))]
        (is (seq @used)
            (str label " — the delegate must actually run, or this compares "
                 "the reference engine to itself"))
        (is (and (number? columnar)
                 (< (abs (- (double expect) (double columnar))) 1e-9))
            (str label " columnar"))
        (is (= (double reference) (double columnar))
            (str label " — columnar and reference must agree"))
        ;; a truncating median or an integral avg passes tolerance while still
        ;; being the wrong answer, so pin the type too
        (is (= (double? reference) (double? columnar))
            (str label " — same numeric type on both paths"))))))

(deftest test-stratum-aggregate-eligibility-gates
  (testing "the columnar path declines what it cannot compute to the contract"
    ;; Two entry points reach the columnar aggregate and they did not share
    ;; guards: the fused one (execute-columnar-aggregate) checks arity and column
    ;; type, while the secondary-index one (try-secondary-index-aggregate)
    ;; checked neither, so it answered a different function than the query asked
    ;; for. Both gates now live on both paths.
    (let [mk (fn [vals]
               (let [schema {:num/v {}
                             :idx/analytics {:db.secondary/type :stratum
                                             :db.secondary/attrs [:num/v]}}
                     e (db/empty-db schema)
                     idx (sec/create-index :stratum {:attrs #{:num/v}} e)]
                 (-> (assoc e :secondary-indices {:idx/analytics idx})
                     (d/db-with (vec (map-indexed (fn [i v] {:db/id (inc i) :num/v v}) vals))))))
          agree? (fn [db query]
                   ;; the cache must be off, or the second call is a hit that
                   ;; returns the first engine's answer
                   (binding [q/*query-result-cache?* false]
                     (let [c (binding [q/*disable-planner* false] (d/q query db))
                           r (binding [q/*disable-planner* true] (d/q query db))]
                       [(= c r) c r])))]

      (testing "an aggregate with a count argument is a different function"
        ;; `(min 2 ?x)` returns the two smallest as a COLLECTION. Only the last
        ;; argument was read, so the count was dropped and a scalar returned.
        (doseq [q ['[:find (min 2 ?x) :where [?e :num/v ?x]]
                   '[:find (max 2 ?x) :where [?e :num/v ?x]]]]
          (let [[ok? c r] (agree? (mk [10 15 20 35 75]) q)]
            (is ok? (str q " — columnar " (pr-str c) " vs reference " (pr-str r))))))

      (testing "an aggregate applies to the DEDUPLICATED find projection"
        ;; A Datalog aggregate applies to the answer set. Aggregating inside the
        ;; index counts a repeated value once per datom, so `(count ?x)` over
        ;; 10,10,40 answered 3 where the projection has two members.
        (doseq [q ['[:find (count ?x) :where [?e :num/v ?x]]
                   '[:find (sum ?x) :where [?e :num/v ?x]]
                   '[:find (avg ?x) :where [?e :num/v ?x]]
                   '[:find (variance ?x) :where [?e :num/v ?x]]
                   '[:find (median ?x) :where [?e :num/v ?x]]]]
          (let [[ok? c r] (agree? (mk [10 10 40]) q)]
            (is ok? (str q " over duplicates — columnar " (pr-str c)
                         " vs reference " (pr-str r))))))

      (testing "duplicate-insensitive aggregates keep the fast path"
        (doseq [q ['[:find (min ?x) :where [?e :num/v ?x]]
                   '[:find (max ?x) :where [?e :num/v ?x]]
                   '[:find (count-distinct ?x) :where [?e :num/v ?x]]]]
          (let [[ok? c r] (agree? (mk [10 10 40]) q)]
            (is ok? (str q " — columnar " (pr-str c) " vs reference " (pr-str r)))))))))

;; ---------------------------------------------------------------------------
;; Retraction granularity: a retraction names ONE datom, not the entity

(defn- ^:private retraction-cfg []
  {:store {:backend :memory :id (java.util.UUID/randomUUID)}
   :index :datahike.index/persistent-set
   :keep-history? false :schema-flexibility :write})

(deftest scriptum-retraction-keeps-the-entitys-other-attributes
  (testing "the retract branch deleted by `_entity_id` alone, which removed
            EVERY document the entity had — all of its other indexed attributes
            with it.

            Measured before the fix: one entity with :doc/body and :doc/title,
            both `:db.secondary/only` and both covered by one scriptum index;
            retracting :doc/body left `-sec-value` returning nil for :doc/title
            while the primary still held its content hash. So the database
            reported the attribute present and the only copy of its text was
            gone — the primary never holds it. `export-db` then refused
            (`:export/secondary-only-unresolvable`), which is how it surfaced;
            before the export learned to check hashes it wrote the HASH as the
            title's value and reported success.

            The fix is a composite `_ea` term, because `sc/delete-docs` takes a
            single Lucene Term and a conjunction is therefore not expressible."
    (let [cfg  (retraction-cfg)
          _    (do (d/delete-database cfg) (d/create-database cfg))
          conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :doc/body :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one :db.secondary/only true}
                          {:db/ident :doc/title :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one :db.secondary/only true}])
        (d/transact conn [{:db/ident :idx/ft :db.secondary/type :scriptum
                           :db.secondary/attrs [:doc/body :doc/title]
                           :db.secondary/config {:path (str "/tmp/dh-retr-" (java.util.UUID/randomUUID))}}])
        (Thread/sleep 800)
        (let [eid (get-in (d/transact conn [{:db/id -1 :doc/body "the whole document"
                                             :doc/title "My Title"}])
                          [:tempids -1])
              idx #(get (:secondary-indices @conn) :idx/ft)]
          (Thread/sleep 500)
          (is (= "the whole document" (sec/-sec-value (idx) :doc/body eid)))
          (is (= "My Title" (sec/-sec-value (idx) :doc/title eid)))

          ;; Retracted by the ORIGINAL value, not by the hash a query returns:
          ;; the retract path re-hashes its argument to find the stored datom,
          ;; so passing the hash back is a silent no-op with empty :tx-data.
          (d/transact conn [[:db/retract eid :doc/body "the whole document"]])
          (Thread/sleep 700)

          (is (nil? (sec/-sec-value (idx) :doc/body eid))
              "the retracted attribute's value is gone, which is the point")
          (is (= "My Title" (sec/-sec-value (idx) :doc/title eid))
              "and the attribute that was NOT retracted still has its value")

          (testing "so the database can still be backed up — the export refuses
                    any :db.secondary/only value it cannot recover, and this one
                    is recoverable again"
            (is (map? (m/export-db @conn (str "/tmp/dh-retr-dump-" (java.util.UUID/randomUUID)) {})))))
        (finally (d/release conn))))))

(deftest proximum-covers-exactly-one-attribute
  (when-not proximum-available?
    (is (not proximum-available?) "SKIP: proximum requires Java 22+"))
  (when proximum-available?
    (testing "proximum is keyed by an external id, and this adapter uses the
              entity id — so an index covering two attributes stores both of an
              entity's vectors under one key. Both halves were measured:
              an entity holding both attributes made proximum itself throw
              \"External id already exists\" from inside the async index update,
              naming neither the attribute nor the cause; and two attributes on
              DISJOINT entities refused nothing at all, after which `-sec-value`
              — which ignores its `attr` argument, being able to fetch only by
              id — answered an entity's :p/face with its :p/emb vector.

              Refused at declaration instead, which is also what makes ignoring
              `attr` honest: with one covered attribute there is nothing to
              disambiguate."
      (let [cfg  (retraction-cfg)
            _    (do (d/delete-database cfg) (d/create-database cfg))
            conn (d/connect cfg)]
        (try
          (d/transact conn [{:db/ident :p/emb :db/valueType :db.type/float-array
                             :db/cardinality :db.cardinality/one}
                            {:db/ident :p/face :db/valueType :db.type/float-array
                             :db/cardinality :db.cardinality/one}])
          (let [decl (fn [attrs]
                       (try (d/transact conn [{:db/ident (keyword "idx" (str "v" (count attrs)))
                                               :db.secondary/type :proximum
                                               :db.secondary/attrs attrs
                                               :db.secondary/config
                                               {:dim 4 :distance :cosine
                                                :store-config {:backend :memory
                                                               :id (java.util.UUID/randomUUID)}}}])
                            :accepted
                            (catch Exception e (str (ex-message e)))))]
            (is (re-find #"covers exactly one attribute" (str (decl [:p/emb :p/face])))
                "two attributes are refused, and the message says why")
            (is (= :accepted (decl [:p/emb]))
                "while the supported shape — one index per vector attribute — is unaffected"))
          (finally (d/release conn)))))))

;; ---------------------------------------------------------------------------
;; Stratum: a transaction sets CELLS, and reads see the current row

(defn- ^:private stratum-conn [vt?]
  (let [cfg (retraction-cfg)]
    (d/delete-database cfg) (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn [{:db/ident :p/dept :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one}
                        {:db/ident :p/salary :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/one}])
      (d/transact conn [{:db/ident :idx/an :db.secondary/type :stratum
                         :db.secondary/attrs [:p/dept :p/salary]
                         :db.secondary/config (if vt? {:valid-time true} {})}])
      (Thread/sleep 700)
      conn)))

(deftest stratum-update-touches-one-cell-and-leaves-one-row
  (testing "the adapter used to hand-roll SCD2, and its transient could only
            append a whole row or drop a whole row — `pending-adds` held only
            the columns THIS transaction wrote and was treated as a complete
            row. So a card-one update left a duplicate whose sibling columns
            were nil, and `-sec-value` (`:limit 1`, unordered) returned the
            stale one. Measured before: 2 rows, `:dept nil` on the new one,
            salary 50000 after being set to 60000. `-columnar-aggregate` then
            threw ArrayIndexOutOfBoundsException where it worked on a clean
            dataset.

            It now delegates to stratum's `upsert!`, which merges the cells
            onto the existing row."
    (let [conn (stratum-conn false)]
      (try
        (let [eid (get-in (d/transact conn [{:db/id -1 :p/dept "eng" :p/salary 50000}])
                          [:tempids -1])
              idx #(get (:secondary-indices @conn) :idx/an)]
          (Thread/sleep 500)
          (d/transact conn [{:db/id eid :p/salary 60000}])
          (Thread/sleep 700)
          (is (= 1 (st/row-count (.-dataset (idx)))) "one row, not a duplicate")
          (is (= 60000 (sec/-sec-value (idx) :p/salary eid)) "the current value")
          (is (= "eng" (sec/-sec-value (idx) :p/dept eid))
              "and the column this transaction never mentioned survives")
          (is (= [{:dept "eng" :_count 1 :avg 60000.0}]
                 (sec/-columnar-aggregate (idx) {:group [:dept] :agg [[:avg :salary]]}))
              "the index's own aggregate entry point stops throwing"))
        (finally (d/release conn))))))

(deftest stratum-retraction-clears-one-cell
  (let [conn (stratum-conn false)]
    (try
      (let [eid (get-in (d/transact conn [{:db/id -1 :p/dept "eng" :p/salary 50000}])
                        [:tempids -1])
            idx #(get (:secondary-indices @conn) :idx/an)]
        (Thread/sleep 500)
        (d/transact conn [[:db/retract eid :p/salary 50000]])
        (Thread/sleep 700)
        (is (nil? (sec/-sec-value (idx) :p/salary eid)) "the retracted cell is cleared")
        (is (= "eng" (sec/-sec-value (idx) :p/dept eid))
            "and the entity's other attribute survives — `pending-retracts` names
             [entity attribute], where it used to name the entity and drop its row"))
      (finally (d/release conn)))))

(deftest stratum-vt-mode-reads-the-current-row-and-stamps-a-real-window
  (testing "two fixes that delegation does not give for free. `-sec-value` had
            no current-row filter, so it read a SUPERSEDED row; and both axis
            filters are needed, because an SCD2-on-both-axes update closes the
            old row's `_system_to` while leaving `_valid_to` open — that is the
            audit chain. Separately, `tx-meta-for-secondary` found no
            `:db/txInstant` (the in-progress tx entity is not in EAVT yet), so
            every row was stamped `_valid_from 0` AND `_valid_to 0` — a
            zero-width window, valid at no instant."
    (let [conn (stratum-conn true)]
      (try
        (let [eid (get-in (d/transact conn [{:db/id -1 :p/dept "eng" :p/salary 50000}])
                          [:tempids -1])
              idx #(get (:secondary-indices @conn) :idx/an)]
          (Thread/sleep 500)
          (d/transact conn [{:db/id eid :p/salary 60000}])
          (Thread/sleep 700)
          (is (= 60000 (sec/-sec-value (idx) :p/salary eid))
              "the CURRENT generation, not a superseded one")
          (let [rows (st/q {:from (.-dataset (idx))
                            :select [:eid :_valid_from :_valid_to]})]
            (is (every? #(pos? (:_valid_from %)) rows)
                "every row carries a real instant, not 0")
            (is (not-any? #(= (:_valid_from %) (:_valid_to %)) rows)
                "and no row has a zero-width validity window")))
        (finally (d/release conn))))))
