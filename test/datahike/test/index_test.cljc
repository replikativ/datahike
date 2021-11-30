(ns datahike.test.index-test
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer        [is deftest testing]])
   [datahike.api :as d]
   [datahike.db :as db]))

(deftest test-datoms
  (let [dvec #(vector (:e %) (:a %) (:v %))
        db (-> (db/empty-db {:age {:db/index true}})
               (d/db-with [[:db/add 1 :name "Petr"]
                           [:db/add 1 :age 44]
                           [:db/add 2 :name "Ivan"]
                           [:db/add 2 :age 25]
                           [:db/add 3 :name "Sergey"]
                           [:db/add 3 :age 11]]))]
    (testing "Main indexes, sort order"
      (is (= [[1 :age 44]
              [2 :age 25]
              [3 :age 11]
              [1 :name "Petr"]
              [2 :name "Ivan"]
              [3 :name "Sergey"]]
             (map dvec (d/datoms db :aevt))))

      (is (= [[1 :age 44]
              [1 :name "Petr"]
              [2 :age 25]
              [2 :name "Ivan"]
              [3 :age 11]
              [3 :name "Sergey"]]
             (map dvec (d/datoms db :eavt))))

      (is (= [[3 :age 11]
              [2 :age 25]
              [1 :age 44]]
             (map dvec (d/datoms db :avet))))) ;; name non-indexed, excluded from avet

    (testing "Components filtration"
      (is (= [[1 :age 44]
              [1 :name "Petr"]]
             (map dvec (d/datoms db :eavt 1))))

      (is (= [[1 :age 44]]
             (map dvec (d/datoms db :eavt 1 :age))))

      (is (= [[3 :age 11]
              [2 :age 25]
              [1 :age 44]]
             (map dvec (d/datoms db :avet :age)))))))

(deftest test-seek-datoms
  (let [dvec #(vector (:e %) (:a %) (:v %))
        db (-> (db/empty-db {:name {:db/index true}
                             :age  {:db/index true}})
               (d/db-with [[:db/add 1 :name "Petr"]
                           [:db/add 1 :age 44]
                           [:db/add 2 :name "Ivan"]
                           [:db/add 2 :age 25]
                           [:db/add 3 :name "Sergey"]
                           [:db/add 3 :age 11]]))]

    (testing "Non-termination"
      (is (= (map dvec (d/seek-datoms db :avet :age 10))
             [[3 :age 11]
              [2 :age 25]
              [1 :age 44]
              [2 :name "Ivan"]
              [1 :name "Petr"]
              [3 :name "Sergey"]])))

    (testing "Closest value lookup"
      (is (= (map dvec (d/seek-datoms db :avet :name "P"))
             [[1 :name "Petr"]
              [3 :name "Sergey"]])))

    (testing "Exact value lookup"
      (is (= (map dvec (d/seek-datoms db :avet :name "Petr"))
             [[1 :name "Petr"]
              [3 :name "Sergey"]])))))

#_(deftest test-rseek-datoms ;; TODO: implement rseek within hitchhiker tree
    (let [dvec #(vector (:e %) (:a %) (:v %))
          db (-> (db/empty-db {:name {:db/index true}
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
               (db/empty-db {:name {:db/index true}
                             :age  {:db/index true}})
               [{:db/id 1 :name "Ivan"   :age 15}
                {:db/id 2 :name "Oleg"   :age 20}
                {:db/id 3 :name "Sergey" :age 7}
                {:db/id 4 :name "Pavel"  :age 45}
                {:db/id 5 :name "Petr"   :age 20}])]
    (is (= (map dvec (d/index-range db {:attrid :name :start "Pe" :end "S"}))
           [[5 :name "Petr"]]))
    (is (= (map dvec (d/index-range db {:attrid :name :start "O" :end "Sergey"}))
           [[2 :name "Oleg"]
            [4 :name "Pavel"]
            [5 :name "Petr"]
            [3 :name "Sergey"]]))

    (is (= (map dvec (d/index-range db {:attrid :name :start nil :end "P"}))
           [[1 :name "Ivan"]
            [2 :name "Oleg"]]))
    (is (= (map dvec (d/index-range db {:attrid :name :start "R" :end nil}))
           [[3 :name "Sergey"]]))
    (is (= (map dvec (d/index-range db {:attrid :name :start nil :end nil}))
           [[1 :name "Ivan"]
            [2 :name "Oleg"]
            [4 :name "Pavel"]
            [5 :name "Petr"]
            [3 :name "Sergey"]]))

    (is (= (map dvec (d/index-range db {:attrid :age :start 15 :end 20}))
           [[1 :age 15]
            [2 :age 20]
            [5 :age 20]]))
    (is (= (map dvec (d/index-range db {:attrid :age :start 7 :end 45}))
           [[3 :age 7]
            [1 :age 15]
            [2 :age 20]
            [5 :age 20]
            [4 :age 45]]))
    (is (= (map dvec (d/index-range db {:attrid :age :start 0 :end 100}))
           [[3 :age 7]
            [1 :age 15]
            [2 :age 20]
            [5 :age 20]
            [4 :age 45]]))))
