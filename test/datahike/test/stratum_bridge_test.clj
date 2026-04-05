(ns datahike.test.stratum-bridge-test
  "Tests for the Stratum columnar analytics bridge."
  (:require
   [clojure.test :refer [deftest testing is]]
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.test.core-test :as core-test]
   [datahike.index.secondary.stratum :as dst]))

;; ---------------------------------------------------------------------------
;; Conversion tests

(deftest test-relation-column-conversion
  (testing "tuples to columns"
    (let [tuples [["Alice" 30 80000.0]
                  ["Bob" 25 90000.0]]
          cols (dst/relation->columns tuples [:name :age :salary])]
      (is (some? cols))
      (is (= 3 (count (keys cols))))
      (is (instance? (Class/forName "[Ljava.lang.String;") (:name cols)))
      (is (instance? (Class/forName "[J") (:age cols)))
      (is (instance? (Class/forName "[D") (:salary cols)))))

  (testing "columns to relation"
    (let [result-maps [{:dept "eng" :avg 85000.0}
                       {:dept "sales" :avg 65000.0}]
          tuples (dst/columns->relation result-maps [:dept :avg])]
      (is (= [["eng" 85000.0] ["sales" 65000.0]] tuples))))

  (testing "empty input"
    (is (nil? (dst/relation->columns [] [:a :b])))))

;; ---------------------------------------------------------------------------
;; Aggregate tests

(deftest test-aggregate
  (testing "group-by with avg"
    (let [tuples [["eng" 80000.0]
                  ["eng" 90000.0]
                  ["eng" 85000.0]
                  ["sales" 60000.0]
                  ["sales" 70000.0]]
          result (dst/aggregate
                  {:group [:dept] :agg [[:avg :salary]]}
                  tuples [:dept :salary])]
      (is (= 2 (count result)))
      (let [eng (first (filter #(= "eng" (:dept %)) result))
            sales (first (filter #(= "sales" (:dept %)) result))]
        (is (== 85000.0 (:avg eng)))
        (is (== 65000.0 (:avg sales))))))

  (testing "multiple aggregations"
    (let [tuples [["a" 10.0] ["a" 20.0] ["b" 30.0]]
          result (dst/aggregate
                  {:group [:g] :agg [[:sum :v] [:count] [:min :v] [:max :v]]}
                  tuples [:g :v])]
      (is (= 2 (count result)))
      (let [a (first (filter #(= "a" (:g %)) result))]
        (is (== 30.0 (:sum a)))
        (is (== 2 (:count a)))
        (is (== 10.0 (:min a)))
        (is (== 20.0 (:max a))))))

  (testing "with ordering"
    (let [tuples [["eng" 80000.0] ["eng" 90000.0] ["sales" 60000.0]]
          result (dst/aggregate
                  {:group [:dept] :agg [[:avg :salary]]
                   :order [[:avg :desc]]}
                  tuples [:dept :salary])]
      (is (= "eng" (:dept (first result))))))

  (testing "no group-by (global aggregate)"
    (let [tuples [["a" 10.0] ["b" 20.0] ["c" 30.0]]
          result (dst/aggregate
                  {:agg [[:sum :v] [:avg :v]]}
                  tuples [:name :v])]
      (is (= 1 (count result)))
      (is (== 60.0 (:sum (first result))))
      (is (== 20.0 (:avg (first result)))))))

;; ---------------------------------------------------------------------------
;; Window function tests

(deftest test-window
  (testing "row-number with partition"
    (let [tuples [["eng" "Alice" 80000.0]
                  ["eng" "Bob" 90000.0]
                  ["eng" "Charlie" 85000.0]
                  ["sales" "Dave" 60000.0]
                  ["sales" "Eve" 70000.0]]
          result (dst/window
                  {:window [{:op :row-number :as :rank
                             :partition-by [:dept]
                             :order [[:salary :desc]]}]}
                  tuples [:dept :name :salary])]
      (is (= 5 (count result)))
      ;; Check that ranking resets per partition
      (let [eng-rows (filter #(= "eng" (:dept %)) result)
            sales-rows (filter #(= "sales" (:dept %)) result)]
        (is (= #{1.0 2.0 3.0} (set (map :rank eng-rows))))
        (is (= #{1.0 2.0} (set (map :rank sales-rows))))))))

;; ---------------------------------------------------------------------------
;; End-to-end with Datahike

(deftest test-datahike-to-stratum-flow
  (testing "Datahike query → Stratum aggregate"
    (let [schema {:person/name {:db/index true}
                  :person/dept {:db/index true}
                  :person/salary {:db/index true}}
          empty-db (db/empty-db schema)
          db (d/db-with empty-db
                        [{:db/id 1 :person/name "Alice" :person/dept "eng" :person/salary 80000}
                         {:db/id 2 :person/name "Bob" :person/dept "eng" :person/salary 90000}
                         {:db/id 3 :person/name "Charlie" :person/dept "sales" :person/salary 60000}
                         {:db/id 4 :person/name "Dave" :person/dept "eng" :person/salary 85000}
                         {:db/id 5 :person/name "Eve" :person/dept "sales" :person/salary 70000}])
          ;; Step 1: Datahike query for dept + salary tuples
          tuples (vec (d/q '[:find ?dept ?salary
                             :where
                             [?e :person/dept ?dept]
                             [?e :person/salary ?salary]]
                           db))
          ;; Step 2: Stratum aggregate
          agg-result (dst/aggregate
                      {:group [:dept]
                       :agg [[:avg :salary] [:count]]
                       :order [[:avg :desc]]}
                      tuples [:dept :salary])]
      (is (= 2 (count agg-result)))
      (is (= "eng" (:dept (first agg-result))))
      (is (== 85000.0 (:avg (first agg-result))))
      (is (== 3 (:count (first agg-result))))
      (is (= "sales" (:dept (second agg-result))))
      (is (== 65000.0 (:avg (second agg-result)))))))

;; ---------------------------------------------------------------------------
;; Metadata tests

(deftest test-external-engine-metadata
  (testing "aggregate has correct metadata"
    (let [meta (-> #'dst/aggregate meta :datahike/external-engine)]
      (is (some? meta))
      (is (= :dynamic (:binding-columns meta)))
      (is (false? (:accepts-entity-filter? meta)))
      (is (= :all-bound (:input-vars meta)))
      (is (fn? (:cost-model meta)))))

  (testing "window has correct metadata"
    (let [meta (-> #'dst/window meta :datahike/external-engine)]
      (is (some? meta))
      (is (fn? (:cost-model meta)))))

  (testing "cost model returns expected shape"
    (let [cost-fn (-> #'dst/aggregate meta :datahike/external-engine :cost-model)
          cost (cost-fn nil nil [{:group [:dept] :agg [[:avg :salary]]}] 2)]
      (is (contains? cost :estimated-card))
      (is (contains? cost :cost-per-result)))))

;; ---------------------------------------------------------------------------
;; End-to-end: Stratum aggregate/window as WHERE clause through compiled engine

(def ^:private e2e-db
  (delay
    (let [schema {:person/name   {:db/index true}
                  :person/dept   {:db/index true}
                  :person/salary {:db/index true}}]
      (d/db-with (db/empty-db schema)
                 [{:db/id 1 :person/name "Alice"   :person/dept "eng"   :person/salary 80000}
                  {:db/id 2 :person/name "Bob"     :person/dept "eng"   :person/salary 90000}
                  {:db/id 3 :person/name "Charlie" :person/dept "sales" :person/salary 60000}
                  {:db/id 4 :person/name "Dave"    :person/dept "eng"   :person/salary 85000}
                  {:db/id 5 :person/name "Eve"     :person/dept "sales" :person/salary 70000}]))))

(deftest test-aggregate-in-where-clause
  (if-not core-test/compiled-engine?
    (is (= :skipped :skipped) "Requires compiled engine (DATAHIKE_COMPILED_QUERY=true)")
    (do
      (testing "group-by aggregate via function binding in WHERE"
        (let [result (d/q '[:find ?dept ?avg
                            :where
                            [?e :person/dept ?dept]
                            [?e :person/salary ?salary]
                            [(datahike.index.secondary.stratum/aggregate
                              {:group [:dept] :agg [[:avg :salary]]})
                             [[?dept ?avg]]]]
                          @e2e-db)]
          (is (= 2 (count result)))
          (let [result-map (into {} result)]
            (is (== 85000.0 (get result-map "eng")))
            (is (== 65000.0 (get result-map "sales"))))))

      (testing "multiple aggregates in one call"
        (let [result (d/q '[:find ?dept ?total ?cnt
                            :where
                            [?e :person/dept ?dept]
                            [?e :person/salary ?salary]
                            [(datahike.index.secondary.stratum/aggregate
                              {:group [:dept] :agg [[:sum :salary] [:count]]})
                             [[?dept ?total ?cnt]]]]
                          @e2e-db)]
          (is (= 2 (count result)))
          (let [eng (first (filter #(= "eng" (first %)) result))]
            (is (== 255000.0 (second eng)))
            (is (== 3 (nth eng 2))))))

      (testing "HAVING via predicate after aggregate"
        (let [result (d/q '[:find ?dept ?avg
                            :where
                            [?e :person/dept ?dept]
                            [?e :person/salary ?salary]
                            [(datahike.index.secondary.stratum/aggregate
                              {:group [:dept] :agg [[:avg :salary]]})
                             [[?dept ?avg]]]
                            [(> ?avg 70000)]]
                          @e2e-db)]
          (is (= 1 (count result)))
          (is (= "eng" (ffirst result))))))))

(deftest test-window-in-where-clause
  (if-not core-test/compiled-engine?
    (is (= :skipped :skipped) "Requires compiled engine (DATAHIKE_COMPILED_QUERY=true)")
    (testing "row-number window function in WHERE"
      (let [result (d/q '[:find ?name ?salary ?rn
                          :where
                          [?e :person/name ?name]
                          [?e :person/salary ?salary]
                          [?e :person/dept ?dept]
                          [(datahike.index.secondary.stratum/window
                            {:window [{:op :row-number :as :rn
                                       :partition-by [:dept]
                                       :order [[:salary :desc]]}]})
                           [[?dept ?name ?salary ?rn]]]]
                        @e2e-db)]
        (is (= 5 (count result)))
        ;; Each eng person gets a distinct rank 1-3
        (let [eng-ranks (->> result
                             (filter #(contains? #{"Alice" "Bob" "Dave"} (first %)))
                             (map #(nth % 2))
                             set)]
          (is (= #{1.0 2.0 3.0} eng-ranks)))
        ;; Each sales person gets a distinct rank 1-2
        (let [sales-ranks (->> result
                               (filter #(contains? #{"Charlie" "Eve"} (first %)))
                               (map #(nth % 2))
                               set)]
          (is (= #{1.0 2.0} sales-ranks)))))))
