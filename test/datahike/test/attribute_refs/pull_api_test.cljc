(ns datahike.test.attribute-refs.pull-api-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing]])
   [datahike.test.attribute-refs.utils :refer [ref-db ref-e0 ref-config
                                               wrap-direct-datoms wrap-ref-datoms]]
   [datahike.api :as d]))

(def test-direct-datoms
  [[1 :name  "Petr"]
   [1 :aka   "Devil"]
   [1 :aka   "Tupen"]
   [2 :name  "David"]
   [3 :name  "Thomas"]
   [4 :name  "Lucy"]
   [5 :name  "Elizabeth"]
   [6 :name  "Matthew"]
   [7 :name  "Eunan"]
   [8 :name  "Kerri"]
   [9 :name  "Rebecca"]
   [10 :name "Part A"]
   [11 :name "Part A.A"]
   [12 :name "Part A.A.A"]
   [13 :name "Part A.A.A.A"]
   [14 :name "Part A.A.A.B"]
   [15 :name "Part A.B"]
   [16 :name "Part A.B.A"]
   [17 :name "Part A.B.A.A"]
   [18 :name "Part A.B.A.B"]])

(def test-ref-datoms
  [[1 :child 2]
   [1 :child 3]
   [2 :father 1]
   [3 :father 1]
   [6 :father 3]
   [10 :part 11]
   [11 :part 12]
   [12 :part 13]
   [12 :part 14]
   [10 :part 15]
   [15 :part 16]
   [16 :part 17]
   [16 :part 18]])

(defn test-datoms [db offset]
  (vec (concat (wrap-direct-datoms db offset :db/add  test-direct-datoms)
               (wrap-ref-datoms db offset :db/add test-ref-datoms))))

(def test-db (d/db-with ref-db (test-datoms ref-db ref-e0)))

(deftest test-pull-attr-spec
  (is (= {:name "Petr" :aka ["Devil" "Tupen"]}
         (d/pull test-db '[:name :aka] (+ ref-e0 1))))

  (is (= {:name "Matthew" :father {:db/id (+ ref-e0 3)} :db/id (+ ref-e0 6)}
         (d/pull test-db '[:name :father :db/id] (+ ref-e0 6))))

  (is (= [{:name "Petr"} {:name "Elizabeth"}
          {:name "Eunan"} {:name "Rebecca"}]
         (d/pull-many test-db '[:name]
                      (mapv (partial + ref-e0) [1 5 7 9])))))

(deftest test-pull-reverse-attr-spec
  (is (= {:name "David" :_child [{:db/id (+ ref-e0 1)}]}
         (d/pull test-db '[:name :_child] (+ ref-e0 2))))

  (is (= {:name "David" :_child [{:name "Petr"}]}
         (d/pull test-db '[:name {:_child [:name]}] (+ ref-e0 2))))

  (testing "Reverse non-component references yield collections"
    (is (= {:name "Thomas" :_father [{:db/id (+ ref-e0 6)}]}
           (d/pull test-db '[:name :_father] (+ ref-e0 3))))

    (is (= {:name "Petr" :_father [{:db/id (+ ref-e0 2)} {:db/id (+ ref-e0 3)}]}
           (d/pull test-db '[:name :_father] (+ ref-e0 1))))

    (is (= {:name "Thomas" :_father [{:name "Matthew"}]}
           (d/pull test-db '[:name {:_father [:name]}] (+ ref-e0 3))))

    (is (= {:name "Petr" :_father [{:name "David"} {:name "Thomas"}]}
           (d/pull test-db '[:name {:_father [:name]}] (+ ref-e0 1))))))

(deftest test-pull-component-attr
  (let [parts {:name "Part A",
               :part
               [{:db/id (+ ref-e0 11)
                 :name "Part A.A",
                 :part
                 [{:db/id (+ ref-e0 12)
                   :name "Part A.A.A",
                   :part
                   [{:db/id (+ ref-e0 13) :name "Part A.A.A.A"}
                    {:db/id (+ ref-e0 14) :name "Part A.A.A.B"}]}]}
                {:db/id (+ ref-e0 15)
                 :name "Part A.B",
                 :part
                 [{:db/id (+ ref-e0 16)
                   :name "Part A.B.A",
                   :part
                   [{:db/id (+ ref-e0 17) :name "Part A.B.A.A"}
                    {:db/id (+ ref-e0 18) :name "Part A.B.A.B"}]}]}]}
        rpart (update-in parts [:part 0 :part 0 :part]
                         (partial into [{:db/id (+ ref-e0 10)}]))
        recdb (d/db-with test-db (wrap-ref-datoms test-db ref-e0 :db/add
                                                  [[12 :part 10]]))]

    (testing "Component entities are expanded recursively"
      (is (= parts (d/pull test-db '[:name :part] (+ ref-e0 10)))))

    (testing "Reverse component references yield a single result"
      (is (= {:name "Part A.A" :_part {:db/id (+ ref-e0 10)}}
             (d/pull test-db [:name :_part] (+ ref-e0 11))))

      (is (= {:name "Part A.A" :_part {:name "Part A"}}
             (d/pull test-db [:name {:_part [:name]}] (+ ref-e0 11)))))

    (testing "Like explicit recursion, expansion will not allow loops"
      (is (= rpart (d/pull recdb '[:name :part] (+ ref-e0 10)))))))

(deftest test-pull-wildcard
  (is (= {:db/id (+ ref-e0 1)
          :name "Petr"
          :aka ["Devil" "Tupen"]
          :child [{:db/id (+ ref-e0 2)} {:db/id (+ ref-e0 3)}]}
         (d/pull test-db '[*] (+ ref-e0 1))))

  (is (= {:db/id (+ ref-e0 2)
          :name "David"
          :_child [{:db/id (+ ref-e0 1)}]
          :father {:db/id (+ ref-e0 1)}}
         (d/pull test-db '[* :_child] (+ ref-e0 2)))))

(deftest test-pull-limit
  (let [db (d/db-with test-db
                      (concat
                       (wrap-ref-datoms test-db ref-e0 :db/add
                                        [[4 :friend 5]
                                         [4 :friend 6]
                                         [4 :friend 7]
                                         [4 :friend 8]])
                       (wrap-direct-datoms test-db ref-e0 :db/add
                                           (for [idx (range 2000)]
                                             [8 :aka (str "aka-" idx)]))))]

    (testing "Without an explicit limit, the default is 1000"
      (is (= 1000 (->> (d/pull db '[:aka] (+ ref-e0 8)) :aka count))))

    (testing "Explicit limit can reduce the default"
      (is (= 500 (->> (d/pull db '[(limit :aka 500)] (+ ref-e0 8)) :aka count)))
      (is (= 500 (->> (d/pull db '[[:aka :limit 500]] (+ ref-e0 8)) :aka count))))

    (testing "Explicit limit can increase the default"
      (is (= 1500 (->> (d/pull db '[(limit :aka 1500)] (+ ref-e0 8)) :aka count))))

    (testing "A nil limit produces unlimited results"
      (is (= 2000 (->> (d/pull db '[(limit :aka nil)] (+ ref-e0 8)) :aka count))))

    (testing "Limits can be used as map specification keys"
      (is (= {:name "Lucy"
              :friend [{:name "Elizabeth"} {:name "Matthew"}]}
             (d/pull db '[:name {(limit :friend 2) [:name]}] (+ ref-e0 4)))))))

(deftest test-pull-default
  (testing "Empty results return nil"
    (is (nil? (d/pull test-db '[:foo] (+ ref-e0 1)))))

  (testing "A default can be used to replace nil results"
    (is (= {:foo "bar"}
           (d/pull test-db '[(default :foo "bar")] (+ ref-e0 1))))
    (is (= {:foo "bar"}
           (d/pull test-db '[[:foo :default "bar"]] (+ ref-e0 1))))))

(deftest test-pull-as
  (is (= {"Name" "Petr", :alias ["Devil" "Tupen"]}
         (d/pull test-db '[[:name :as "Name"] [:aka :as :alias]] (+ ref-e0 1)))))

(deftest test-pull-attr-with-opts
  (is (= {"Name" "Nothing"}
         (d/pull test-db '[[:x :as "Name" :default "Nothing"]] (+ ref-e0 1)))))

(deftest test-pull-map
  (testing "Single attrs yield a map"
    (is (= {:name "Matthew" :father {:name "Thomas"}}
           (d/pull test-db '[:name {:father [:name]}] (+ ref-e0 6)))))

  (testing "Multi attrs yield a collection of maps"
    (is (= {:name "Petr" :child [{:name "David"}
                                 {:name "Thomas"}]}
           (d/pull test-db '[:name {:child [:name]}] (+ ref-e0 1)))))

  (testing "Missing attrs are dropped"
    (is (= {:name "Petr"}
           (d/pull test-db '[:name {:father [:name]}] (+ ref-e0 1)))))

  (testing "Non matching results are removed from collections"
    (is (= {:name "Petr" :child []}
           (d/pull test-db '[:name {:child [:foo]}] (+ ref-e0 1)))))

  (testing "Map specs can override component expansion"
    (let [parts {:name "Part A" :part [{:name "Part A.A"} {:name "Part A.B"}]}]
      (is (= parts
             (d/pull test-db '[:name {:part [:name]}] (+ ref-e0 10))))

      (is (= parts
             (d/pull test-db '[:name {:part 1}] (+ ref-e0 10)))))))

(deftest test-pull-recursion
  (let [irmap (get test-db :ident-ref-map)
        datoms [[4 :friend 5]
                [5 :friend 6]
                [6 :friend 7]
                [7 :friend 8]
                [4 :enemy 6]
                [5 :enemy 7]
                [6 :enemy 8]
                [7 :enemy 4]]
        db (d/db-with test-db (wrap-ref-datoms test-db ref-e0 :db/add datoms))
        friends {:db/id (+ ref-e0 4)
                 :name "Lucy"
                 :friend
                 [{:db/id (+ ref-e0 5)
                   :name "Elizabeth"
                   :friend
                   [{:db/id (+ ref-e0 6)
                     :name "Matthew"
                     :friend
                     [{:db/id (+ ref-e0 7)
                       :name "Eunan"
                       :friend
                       [{:db/id (+ ref-e0 8)
                         :name "Kerri"}]}]}]}]}
        enemies {:db/id (+ ref-e0 4) :name "Lucy"
                 :friend
                 [{:db/id (+ ref-e0 5) :name "Elizabeth"
                   :friend
                   [{:db/id (+ ref-e0 6) :name "Matthew"
                     :enemy [{:db/id (+ ref-e0 8) :name "Kerri"}]}]
                   :enemy
                   [{:db/id (+ ref-e0 7) :name "Eunan"
                     :friend
                     [{:db/id (+ ref-e0 8) :name "Kerri"}]
                     :enemy
                     [{:db/id (+ ref-e0 4) :name "Lucy"
                       :friend [{:db/id (+ ref-e0 5)}]}]}]}]
                 :enemy
                 [{:db/id (+ ref-e0 6) :name "Matthew"
                   :friend
                   [{:db/id (+ ref-e0 7) :name "Eunan"
                     :friend
                     [{:db/id (+ ref-e0 8) :name "Kerri"}]
                     :enemy [{:db/id (+ ref-e0 4) :name "Lucy"
                              :friend [{:db/id (+ ref-e0 5) :name "Elizabeth"}]}]}]
                   :enemy
                   [{:db/id (+ ref-e0 8) :name "Kerri"}]}]}]

    (testing "Infinite recursion"
      (is (= friends (d/pull db '[:db/id :name {:friend ...}] (+ ref-e0 4)))))

    (testing "Multiple recursion specs in one pattern"
      (is (= enemies (d/pull db '[:db/id :name {:friend 2 :enemy 2}] (+ ref-e0 4)))))

    (let [db (d/db-with db [[:db/add (+ ref-e0 8) (:friend irmap) (+ ref-e0 4)]])]
      (testing "Cycles are handled by returning only the :db/id of entities which have been seen before"
        (is (= (update-in friends (take 8 (cycle [:friend 0]))
                          assoc :friend [{:db/id (+ ref-e0 4) :name "Lucy" :friend [{:db/id (+ ref-e0 5)}]}])
               (d/pull db '[:db/id :name {:friend ...}] (+ ref-e0 4))))))))

(deftest test-dual-recursion
  (let [_ (d/delete-database ref-config)
        _ (d/create-database ref-config)
        conn (d/connect ref-config)
        schema [{:db/ident :part
                 :db/cardinality :db.cardinality/one
                 :db/valueType :db.type/ref}
                {:db/ident :spec
                 :db/cardinality :db.cardinality/one
                 :db/valueType :db.type/ref}]
        _ (d/transact conn schema)
        test-e0 (:max-eid @conn)
        db (d/db-with @conn (wrap-ref-datoms @conn test-e0 :db/add
                                             [[1 :part 2]
                                              [2 :part 3]
                                              [3 :part 1]
                                              [1 :spec 2]
                                              [2 :spec 1]]))]
    (is (= (d/pull db '[:db/id {:part ...} {:spec ...}] (+ test-e0 1))
           {:db/id (+ test-e0 1),
            :spec {:db/id (+ test-e0 2)
                   :spec {:db/id (+ test-e0 1),
                          :spec {:db/id (+ test-e0 2)}, :part {:db/id (+ test-e0 2)}}
                   :part {:db/id (+ test-e0 3),
                          :part {:db/id (+ test-e0 1),
                                 :spec {:db/id (+ test-e0 2)},
                                 :part {:db/id (+ test-e0 2)}}}}
            :part {:db/id (+ test-e0 2)
                   :spec {:db/id (+ test-e0 1), :spec {:db/id (+ test-e0 2)}, :part {:db/id (+ test-e0 2)}}
                   :part {:db/id (+ test-e0 3),
                          :part {:db/id (+ test-e0 1),
                                 :spec {:db/id (+ test-e0 2)},
                                 :part {:db/id (+ test-e0 2)}}}}}))))

(deftest test-deep-recursion
  (let [start 100
        depth 1500
        irmap (get test-db :ident-ref-map)
        txd   (mapcat
               (fn [idx]
                 [[:db/add idx (:name irmap) (str "Person-" idx)]
                  [:db/add (dec idx) (:friend irmap) idx]])
               (range (inc start) depth))
        db    (d/db-with test-db
                         (concat txd [[:db/add start (:name irmap) (str "Person-" start)]]))
        pulled (d/pull db '[:name {:friend ...}] start)
        path   (->> [:friend 0]
                    (repeat (dec (- depth start)))
                    (into [] cat))]
    (is (= (str "Person-" (dec depth))
           (:name (get-in pulled path))))))
