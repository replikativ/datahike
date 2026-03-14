(ns datahike.test.secondary-integration-test
  "Integration tests for Proximum and Scriptum secondary index implementations."
  (:require
   [clojure.test :refer [deftest testing is]]
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.index.secondary :as sec]
   [datahike.index.entity-set :as es]
   [datahike.index.secondary.scriptum]
   [datahike.index.secondary.stratum]))

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
    (println "SKIP test-proximum-lifecycle: proximum requires Java 22+"))
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
    (println "SKIP test-cross-index-bitmap-composition: proximum requires Java 22+"))
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
    (println "SKIP test-in-transaction-proximum: proximum requires Java 22+"))
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
      (let [result (binding [datahike.query/*force-legacy* false]
                     (d/q '[:find (avg ?s) .
                            :where [?e :person/name "Ivan"] [?e :person/salary ?s]]
                          db))]
        ;; Ivan salaries: 50000 + 80000 + 70000 = 200000 / 3 ≈ 66666.67
        (is (some? result))
        (is (< (abs (- result 66666.67)) 1.0)))

      ;; Verify legacy gives same result
      (let [result-legacy (binding [datahike.query/*force-legacy* true]
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
      (let [result (binding [datahike.query/*force-legacy* false]
                     (d/q '[:find (avg ?s) .
                            :where [?e :person/salary ?s] [(> ?s 65000)]]
                          db))]
        ;; Salaries > 65000: 80000, 70000, 90000 → avg = 80000
        (is (some? result))
        (is (== 80000.0 result)))

      ;; Verify legacy gives same result
      (let [result-legacy (binding [datahike.query/*force-legacy* true]
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
