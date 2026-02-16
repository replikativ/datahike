(ns datahike.test.index-test
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer        [is deftest testing]])
   [datahike.api :as d]
   [datahike.constants :refer [e0 tx0 emax txmax]]
   [datahike.datom :as dd]
   [datahike.db :as db]
   [datahike.index :as di]
   [datahike.index.persistent-set :as pset]
   [org.replikativ.persistent-sorted-set :as psset]))

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

(deftest test-slice []
  (testing "Test index -slice"
    (let [dvec #(vector (:e %) (:a %) (:v %))
          db (d/db-with
              (db/empty-db {:name {:db/index true}
                            :age  {:db/index true}})
              [{:db/id 1 :name "Ivan"   :age 15}
               {:db/id 2 :name "Oleg"   :age 20}
               {:db/id 3 :name "Sergey" :age 7}
               {:db/id 4 :name "Pavel"  :age 45}
               {:db/id 5 :name "Petr"   :age 20}])
          eavt (:eavt db)
          aevt (:aevt db)
          avet (:avet db)]

      (is (= (di/-slice eavt (dd/datom e0 nil nil tx0) (dd/datom emax nil nil txmax) :eavt)
             (d/datoms db :eavt)))
      (is (= (map dvec (di/-slice eavt (dd/datom e0 nil nil tx0) (dd/datom 2 nil nil txmax) :eavt))
             [[1 :age 15]
              [1 :name "Ivan"]
              [2 :age 20]
              [2 :name "Oleg"]]))
      (is (= (map dvec (di/-slice eavt (dd/datom e0 nil nil tx0) (dd/datom 3 :age 7 txmax) :eavt))
             [[1 :age 15]
              [1 :name "Ivan"]
              [2 :age 20]
              [2 :name "Oleg"]
              [3 :age 7]]))
      (is (= (map dvec (di/-slice eavt (dd/datom e0 :age nil tx0) (dd/datom 3 :name "Timofey" txmax) :eavt))
             [[1 :age 15]
              [1 :name "Ivan"]
              [2 :age 20]
              [2 :name "Oleg"]
              [3 :age 7]
              [3 :name "Sergey"]]))
      (is (= (map dvec (di/-slice eavt (dd/datom e0 :age nil tx0) (dd/datom 3 :name "Timofey" tx0) :eavt))
             [[1 :age 15]
              [1 :name "Ivan"]
              [2 :age 20]
              [2 :name "Oleg"]
              [3 :age 7]
              [3 :name "Sergey"]]))
      (is (= (map dvec (di/-slice eavt (dd/datom e0 :age nil tx0) (dd/datom 5 :age nil txmax) :eavt))
             [[1 :age 15]
              [1 :name "Ivan"]
              [2 :age 20]
              [2 :name "Oleg"]
              [3 :age 7]
              [3 :name "Sergey"]
              [4 :age 45]
              [4 :name "Pavel"]
              [5 :age 20]]))

      (is (= (map dvec (di/-slice aevt (dd/datom e0 nil nil tx0) (dd/datom 3 :name "Pavel" txmax) :aevt))
             [[1 :age 15]
              [2 :age 20]
              [3 :age 7]
              [4 :age 45]
              [5 :age 20]
              [1 :name "Ivan"]
              [2 :name "Oleg"]]))
      (is (= (map dvec (di/-slice aevt (dd/datom e0 nil nil tx0) (dd/datom 5 :age 18 txmax) :aevt))
             [[1 :age 15]
              [2 :age 20]
              [3 :age 7]
              [4 :age 45]]))
      (is (= (map dvec (di/-slice aevt (dd/datom e0 nil nil tx0) (dd/datom 3 :name nil txmax) :aevt))
             [[1 :age 15]
              [2 :age 20]
              [3 :age 7]
              [4 :age 45]
              [5 :age 20]
              [1 :name "Ivan"]
              [2 :name "Oleg"]
              [3 :name "Sergey"]]))

      (is (= (map dvec (di/-slice avet (dd/datom e0 nil nil tx0) (dd/datom 3 :age 50 txmax) :avet))
             [[3 :age 7]
              [1 :age 15]
              [2 :age 20]
              [5 :age 20]
              [4 :age 45]])))))

(deftest test-upsert-replace-comparators
  (testing "Replace comparators return 0 for old/new datom pairs"
    (let [old-datom (dd/datom 1 :name "Ivan" 100 true)
          new-datom (dd/datom 1 :name "Petr" 101 true)]

      (testing "EAVT replace comparator"
        (let [cmp (dd/index-type->cmp-replace :eavt)]
          (is (= 0 (cmp old-datom new-datom))
              "EAVT replace should compare only (e,a), ignoring v and tx")))

      (testing "AEVT replace comparator"
        (let [cmp (dd/index-type->cmp-replace :aevt)]
          (is (= 0 (cmp old-datom new-datom))
              "AEVT replace should compare only (a,e), ignoring v and tx")))

      (testing "AVET replace comparator"
        (let [cmp (dd/index-type->cmp-replace :avet)]
          (is (= 0 (cmp old-datom new-datom))
              "AVET replace should compare only (a,e), ignoring v and tx - not (a,v)!")))))

  (testing "Upsert operations work correctly when values change"
    (let [old-datom (dd/datom 1 :age 25 100 true)
          new-datom (dd/datom 1 :age 26 101 true)]

      (testing "EAVT index upsert"
        (let [index (psset/sorted-set* {:cmp (dd/index-type->cmp-quick :eavt)})
              index-with-old (pset/insert index old-datom :eavt)
              updated (pset/upsert index-with-old new-datom :eavt old-datom)
              datoms (seq updated)]
          (is (= 1 (count datoms))
              "Should have exactly 1 datom after upsert")
          (is (= new-datom (first datoms))
              "Should contain the new datom, not the old one")))

      (testing "AEVT index upsert"
        (let [index (psset/sorted-set* {:cmp (dd/index-type->cmp-quick :aevt)})
              index-with-old (pset/insert index old-datom :aevt)
              updated (pset/upsert index-with-old new-datom :aevt old-datom)
              datoms (seq updated)]
          (is (= 1 (count datoms))
              "Should have exactly 1 datom after upsert")
          (is (= new-datom (first datoms))
              "Should contain the new datom, not the old one")))

      (testing "AVET index upsert"
        (let [index (psset/sorted-set* {:cmp (dd/index-type->cmp-quick :avet)})
              index-with-old (pset/insert index old-datom :avet)
              updated (pset/upsert index-with-old new-datom :avet old-datom)
              datoms (seq updated)]
          (is (= 1 (count datoms))
              "Should have exactly 1 datom after upsert")
          (is (= new-datom (first datoms))
              "Should contain the new datom, not the old one")))))

  (testing "Upsert with different entity IDs does not use replace"
    (let [datom1 (dd/datom 1 :age 25 100 true)
          datom2 (dd/datom 2 :age 26 101 true)]

      (testing "AVET index - different entities, same attribute"
        (let [index (psset/sorted-set* {:cmp (dd/index-type->cmp-quick :avet)})
              index-with-d1 (pset/insert index datom1 :avet)
              updated (pset/upsert index-with-d1 datom2 :avet nil)  ; nil old-datom means insert
              datoms (seq updated)]
          (is (= 2 (count datoms))
              "Should have 2 datoms when upserting different entities")
          (is (some #(= datom1 %) datoms)
              "Should still contain the first datom")
          (is (some #(= datom2 %) datoms)
              "Should also contain the second datom"))))))

(deftest test-avet-upsert-replace-ordering-inconsistency
  (testing "AVET upsert must handle case where (a,v,e) and (a,e) orderings differ"
    (let [index (psset/sorted-set* {:cmp (dd/index-type->cmp-quick :avet)})
          datom-e2-v50 (dd/datom 2 42 50 100 true)
          datom-e3-v75 (dd/datom 3 42 75 100 true)
          datom-e1-v100 (dd/datom 1 42 100 100 true)
          index-with-three (-> index
                               (pset/insert datom-e2-v50 :avet)
                               (pset/insert datom-e3-v75 :avet)
                               (pset/insert datom-e1-v100 :avet))]
      (is (= 3 (count (seq index-with-three)))
          "Should have 3 datoms")
      (is (some #(= datom-e1-v100 %) (seq index-with-three))
          "Datom [e=1, a=42, v=100] should exist")
      (is (some #(= datom-e2-v50 %) (seq index-with-three))
          "Datom [e=2, a=42, v=50] should exist")
      (is (some #(= datom-e3-v75 %) (seq index-with-three))
          "Datom [e=3, a=42, v=75] should exist")
      (let [new-datom-e1-v25 (dd/datom 1 42 25 101 true)
            updated (pset/upsert index-with-three new-datom-e1-v25 :avet datom-e1-v100)]
        (is (= 3 (count (seq updated)))
            "Should still have 3 datoms after upsert")
        (is (some #(= datom-e2-v50 %) (seq updated))
            "Datom [e=2, a=42, v=50] should STILL exist after replacing [e=1, a=42, v=100]")
        (is (some #(= datom-e3-v75 %) (seq updated))
            "Datom [e=3, a=42, v=75] should STILL exist after replacing [e=1, a=42, v=100]")
        (is (some #(= new-datom-e1-v25 %) (seq updated))
            "Datom [e=1, a=42, v=25] should exist after replacement")
        (is (not (some #(= datom-e1-v100 %) (seq updated)))
            "Old datom [e=1, a=42, v=100] should NOT exist after replacement")))))
