(ns datahike.test.index
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [datahike.core :as d]
   [datahike.db :as db]
   [datahike.test.core :as tdc]))

(deftest test-datoms
  (let [dvec #(vector (:e %) (:a %) (:v %))
        db (-> (d/empty-db {:age {:db/index true}})
               (d/db-with [ [:db/add tdc/e1 :name "Petr"]
                            [:db/add tdc/e1 :age 44]
                            [:db/add tdc/e2 :name "Ivan"]
                            [:db/add tdc/e2 :age 25]
                            [:db/add tdc/e3 :name "Sergey"]
                            [:db/add tdc/e3 :age 11] ]))]
    (testing "Main indexes, sort order"
      (is (= [ [tdc/e1 :age 44]
               [tdc/e2 :age 25]
               [tdc/e3 :age 11]
               [tdc/e1 :name "Petr"]
               [tdc/e2 :name "Ivan"]
               [tdc/e3 :name "Sergey"] ]
             (map dvec (d/datoms db :aevt))))

      (is (= [ [tdc/e1 :age 44]
               [tdc/e1 :name "Petr"]
               [tdc/e2 :age 25]
               [tdc/e2 :name "Ivan"]
               [tdc/e3 :age 11]
               [tdc/e3 :name "Sergey"] ]
             (map dvec (d/datoms db :eavt))))

      (is (= [ [tdc/e3 :age 11]
               [tdc/e2 :age 25]
               [tdc/e1 :age 44] ]
             (map dvec (d/datoms db :avet))))) ;; name non-indexed, excluded from avet

    (testing "Components filtration"
      (is (= [ [tdc/e1 :age 44]
               [tdc/e1 :name "Petr"] ]
             (map dvec (d/datoms db :eavt tdc/e1))))

      (is (= [ [tdc/e1 :age 44] ]
             (map dvec (d/datoms db :eavt tdc/e1 :age))))

      (is (= [ [tdc/e3 :age 11]
               [tdc/e2 :age 25]
               [tdc/e1 :age 44] ]
             (map dvec (d/datoms db :avet :age)))))))

(deftest test-seek-datoms
  (let [dvec #(vector (:e %) (:a %) (:v %))
        db (-> (d/empty-db { :name { :db/index true }
                             :age  { :db/index true } })
               (d/db-with [[:db/add tdc/e1 :name "Petr"]
                           [:db/add tdc/e1 :age 44]
                           [:db/add tdc/e2 :name "Ivan"]
                           [:db/add tdc/e2 :age 25]
                           [:db/add tdc/e3 :name "Sergey"]
                           [:db/add tdc/e3 :age 11]]))]

    (testing "Non-termination"
      (is (= (map dvec (d/seek-datoms db :avet :age 10))
             [ [tdc/e3 :age 11]
               [tdc/e2 :age 25]
               [tdc/e1 :age 44]
               [tdc/e2 :name "Ivan"]
               [tdc/e1 :name "Petr"]
               [tdc/e3 :name "Sergey"] ])))

    (testing "Closest value lookup"
      (is (= (map dvec (d/seek-datoms db :avet :name "P"))
             [ [tdc/e1 :name "Petr"]
               [tdc/e3 :name "Sergey"] ])))

    (testing "Exact value lookup"
      (is (= (map dvec (d/seek-datoms db :avet :name "Petr"))
             [ [tdc/e1 :name "Petr"]
               [tdc/e3 :name "Sergey"] ])))))

#_(deftest test-rseek-datoms ;; TODO: implement rseek within hitchhiker tree
    (let [dvec #(vector (:e %) (:a %) (:v %))
          db (-> (d/empty-db {:name {:db/index true}
                              :age  {:db/index true}})
                 (d/db-with [[:db/add 1 :name "Petr"]
                             [:db/add 1 :age 44]
                             [:db/add 2 :name "Ivan"]
                             [:db/add 2 :age 25]
                             [:db/add 3 :name "Sergey"]
                             [:db/add 3 :age 11]]))]

      (testing "Non-termination"
        (is (= (map dvec (d/rseek-datoms db :avet :name "Petr"))
               [[1 :name "Petr"]
                [2 :name "Ivan"]
                [1 :age 44]
                [2 :age 25]
                [3 :age 11]])))

      (testing "Closest value lookup"
        (is (= (map dvec (d/rseek-datoms db :avet :age 26))
               [[2 :age 25]
                [3 :age 11]])))

      (testing "Exact value lookup"
        (is (= (map dvec (d/rseek-datoms db :avet :age 25))
               [[2 :age 25]
                [3 :age 11]])))))

(deftest test-index-range
  (let [dvec #(vector (:e %) (:a %) (:v %))
        db    (d/db-with
                (d/empty-db { :name { :db/index true}
                              :age  { :db/index true} })
                [ { :db/id tdc/e1 :name "Ivan"   :age 15 }
                  { :db/id tdc/e2 :name "Oleg"   :age 20 }
                  { :db/id tdc/e3 :name "Sergey" :age 7 }
                  { :db/id tdc/e4 :name "Pavel"  :age 45 }
                  { :db/id tdc/e5 :name "Petr"   :age 20 } ])]
    (is (= (map dvec (d/index-range db :name "Pe" "S"))
           [ [tdc/e5 :name "Petr"] ]))
    (is (= (map dvec (d/index-range db :name "O" "Sergey"))
           [ [tdc/e2 :name "Oleg"]
             [tdc/e4 :name "Pavel"]
             [tdc/e5 :name "Petr"]
             [tdc/e3 :name "Sergey"] ]))

    (is (= (map dvec (d/index-range db :name nil "P"))
           [ [tdc/e1 :name "Ivan"]
             [tdc/e2 :name "Oleg"] ]))
    (is (= (map dvec (d/index-range db :name "R" nil))
           [ [tdc/e3 :name "Sergey"] ]))
    (is (= (map dvec (d/index-range db :name nil nil))
           [ [tdc/e1 :name "Ivan"]
             [tdc/e2 :name "Oleg"]
             [tdc/e4 :name "Pavel"]
             [tdc/e5 :name "Petr"]
             [tdc/e3 :name "Sergey"] ]))

    (is (= (map dvec (d/index-range db :age 15 20))
           [ [tdc/e1 :age 15]
             [tdc/e2 :age 20]
             [tdc/e5 :age 20]]))
    (is (= (map dvec (d/index-range db :age 7 45))
           [ [tdc/e3 :age 7]
             [tdc/e1 :age 15]
             [tdc/e2 :age 20]
             [tdc/e5 :age 20]
             [tdc/e4 :age 45] ]))
    (is (= (map dvec (d/index-range db :age 0 100))
           [ [tdc/e3 :age 7]
             [tdc/e1 :age 15]
             [tdc/e2 :age 20]
             [tdc/e5 :age 20]
             [tdc/e4 :age 45] ]))))
