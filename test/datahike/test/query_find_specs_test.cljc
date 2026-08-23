(ns datahike.test.query-find-specs-test
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.test.core-test :as tdc]))

(def test-db (d/db-with
              (db/empty-db)
              [[:db/add 1 :name "Petr"]
               [:db/add 1 :age 44]
               [:db/add 2 :name "Ivan"]
               [:db/add 2 :age 25]
               [:db/add 3 :name "Sergey"]
               [:db/add 3 :age 11]]))

(deftest test-find-specs
  (is (= (set (d/q '[:find [?name ...]
                     :where [_ :name ?name]] test-db))
         #{"Ivan" "Petr" "Sergey"}))
  (is (= (d/q '[:find [?name ?age]
                :where [1 :name ?name]
                [1 :age  ?age]] test-db)
         ["Petr" 44]))
  (is (= (d/q '[:find ?name .
                :where [1 :name ?name]] test-db)
         "Petr"))

  (testing "Multiple results get cut"
    (is (contains?
         #{["Petr" 44] ["Ivan" 25] ["Sergey" 11]}
         (d/q '[:find [?name ?age]
                :where [?e :name ?name]
                [?e :age  ?age]] test-db)))
    (is (contains?
         #{"Ivan" "Petr" "Sergey"}
         (d/q '[:find ?name .
                :where [_ :name ?name]] test-db))))

  (testing "Aggregates work with find specs"
    (is (= (d/q '[:find [(count ?name) ...]
                  :where [_ :name ?name]] test-db)
           [3]))
    (is (= (d/q '[:find [(count ?name)]
                  :where [_ :name ?name]] test-db)
           [3]))
    (is (= (d/q '[:find (count ?name) .
                  :where [_ :name ?name]] test-db)
           3))))

(deftest test-all-vars-in-bound
  ;; Every var in the query supplied through :in, so after the planner folds
  ;; the constants in, the pattern is fully ground and its group produces no
  ;; column. The fused scan emits one tuple PER EMITTED VAR — with none it
  ;; emits nothing whether or not the datom exists, so a satisfied existence
  ;; test read as "no solution" and the answer came back empty.
  (testing "the entity exists"
    (are [q res] (= res (d/q q test-db 1))
      '[:find ?e . :in $ ?e :where [?e :name _]]              1
      '[:find ?e :in $ ?e :where [?e :name _]]                #{[1]}
      '[:find [?e ...] :in $ ?e :where [?e :name _]]          [1]
      '[:find (count ?e) :in $ ?e :where [?e :name _]]        [[1]]
      '[:find (pull ?e [:name]) :in $ ?e :where [?e :name _]] [[{:name "Petr"}]]))

  (testing "the entity does not exist — still empty"
    (is (nil? (d/q '[:find ?e . :in $ ?e :where [?e :name _]] test-db 999)))
    (is (= #{} (d/q '[:find ?e :in $ ?e :where [?e :name _]] test-db 999))))

  (testing "two ground groups"
    (is (= #{[1 2]}
           (d/q '[:find ?e ?f :in $ ?e ?f :where [?e :name _] [?f :name _]]
                test-db 1 2)))
    (is (= #{}
           (d/q '[:find ?e ?f :in $ ?e ?f :where [?e :name _] [?f :name _]]
                test-db 1 999)))))

#_(t/test-ns 'datahike.test.query-find-specs)
