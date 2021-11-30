(ns datahike.test.attribute-refs.query-find-specs-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [deftest testing is]]
      :clj [clojure.test :as t :refer [deftest testing is]])
   [datahike.test.attribute-refs.utils :refer [ref-db ref-e0
                                               wrap-direct-datoms]]
   [datahike.api :as d]))

(def test-db (d/db-with
              ref-db
              (wrap-direct-datoms ref-db ref-e0 :db/add
                                  [[1 :name "Petr"]
                                   [1 :age 44]
                                   [2 :name "Ivan"]
                                   [2 :age 25]
                                   [3 :name "Sergey"]
                                   [3 :age 11]])))

(deftest test-find-specs
  (is (= #{"Ivan" "Petr" "Sergey"}
         (set (d/q '[:find [?name ...]
                     :where [_ :name ?name]]
                   test-db))))
  (is (= ["Petr" 44]
         (d/q (concat '[:find [?name ?age] :where]
                      [[(+ ref-e0 1) :name '?name]
                       [(+ ref-e0 1) :age  '?age]])
              test-db)))
  (is (= "Petr"
         (d/q (concat '[:find ?name . :where]
                      [[(+ ref-e0 1) :name '?name]])
              test-db)))

  (testing "Multiple results get cut"
    (is (contains?
         #{["Petr" 44] ["Ivan" 25] ["Sergey" 11]}
         (d/q '[:find [?name ?age]
                :where [?e :name ?name]
                [?e :age  ?age]]
              test-db)))
    (is (contains?
         #{"Ivan" "Petr" "Sergey"}
         (d/q '[:find ?name .
                :where [_ :name ?name]]
              test-db))))

  (testing "Aggregates work with find specs"
    (is (= [3]
           (d/q '[:find [(count ?name) ...]
                  :where [_ :name ?name]]
                test-db)))
    (is (= [3]
           (d/q '[:find [(count ?name)]
                  :where [_ :name ?name]]
                test-db)))
    (is (= 3
           (d/q '[:find (count ?name) .
                  :where [_ :name ?name]]
                test-db)))))
