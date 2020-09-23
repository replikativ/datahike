(ns datahike.test.pull-api
  (:require
    #?(:cljs [cljs.test    :as t :refer-macros [is deftest testing]]
       :clj  [clojure.test :as t :refer        [is deftest testing]])
    [datahike.core :as d]
    [datahike.constants :refer [tx0]]
    [datahike.test.core :as tdc]))

;; TODO: fix tests, 1 fails, 3 errors
(def test-schema
  {:aka    {:db/cardinality :db.cardinality/many}
   :child  {:db/cardinality :db.cardinality/many
            :db/valueType :db.type/ref}
   :friend {:db/cardinality :db.cardinality/many
            :db/valueType :db.type/ref}
   :enemy  {:db/cardinality :db.cardinality/many
            :db/valueType :db.type/ref}
   :father {:db/valueType :db.type/ref}

   :part   {:db/valueType :db.type/ref
            :db/isComponent true
            :db/cardinality :db.cardinality/many}
   :spec   {:db/valueType :db.type/ref
            :db/isComponent true
            :db/cardinality :db.cardinality/one}})

(def test-datoms
  (->>
    [[tdc/e1 :name  "Petr"]
     [tdc/e1 :aka   "Devil"]
     [tdc/e1 :aka   "Tupen"]
     [tdc/e2 :name  "David"]
     [tdc/e3 :name  "Thomas"]
     [tdc/e4 :name  "Lucy"]
     [tdc/e5 :name  "Elizabeth"]
     [tdc/e6 :name  "Matthew"]
     [tdc/e7 :name  "Eunan"]
     [tdc/e8 :name  "Kerri"]
     [tdc/e9 :name  "Rebecca"]
     [tdc/e1 :child tdc/e2]
     [tdc/e1 :child tdc/e3]
     [tdc/e2 :father tdc/e1]
     [tdc/e3 :father tdc/e1]
     [tdc/e6 :father tdc/e3]
     [tdc/e10 :name  "Part A"]
     [tdc/e11 :name  "Part A.A"]
     [tdc/e10 :part tdc/e11]
     [tdc/e12 :name  "Part A.A.A"]
     [tdc/e11 :part tdc/e12]
     [tdc/e13 :name  "Part A.A.A.A"]
     [tdc/e12 :part tdc/e13]
     [tdc/e14 :name  "Part A.A.A.B"]
     [tdc/e12 :part tdc/e14]
     [tdc/e15 :name  "Part A.B"]
     [tdc/e10 :part tdc/e15]
     [tdc/e16 :name  "Part A.B.A"]
     [tdc/e15 :part tdc/e16]
     [tdc/e17 :name  "Part A.B.A.A"]
     [tdc/e16 :part tdc/e17]
     [tdc/e18 :name  "Part A.B.A.B"]
     [tdc/e16 :part tdc/e18]]
    (map (fn [[e a v]] (d/datom e a v tx0)))))

(def test-db (d/init-db test-datoms test-schema))

(deftest test-pull-attr-spec
  (is (= {:name "Petr" :aka ["Devil" "Tupen"]}
         (d/pull test-db '[:name :aka] tdc/e1)))

  (is (= {:name "Matthew" :father {:db/id tdc/e3} :db/id tdc/e6}
         (d/pull test-db '[:name :father :db/id] tdc/e6)))

  (is (= [{:name "Petr"} {:name "Elizabeth"}
          {:name "Eunan"} {:name "Rebecca"}]
         (d/pull-many test-db '[:name] [tdc/e1 tdc/e5 tdc/e7 tdc/e9]))))

(deftest test-pull-reverse-attr-spec
  (is (= {:name "David" :_child [{:db/id tdc/e1}]}
         (d/pull test-db '[:name :_child] tdc/e2)))

  (is (= {:name "David" :_child [{:name "Petr"}]}
         (d/pull test-db '[:name {:_child [:name]}] tdc/e2)))

  (testing "Reverse non-component references yield collections"
    (is (= {:name "Thomas" :_father [{:db/id tdc/e6}]}
           (d/pull test-db '[:name :_father] tdc/e3)))

    (is (= {:name "Petr" :_father [{:db/id tdc/e2} {:db/id tdc/e3}]}
           (d/pull test-db '[:name :_father] tdc/e1)))

    (is (= {:name "Thomas" :_father [{:name "Matthew"}]}
           (d/pull test-db '[:name {:_father [:name]}] tdc/e3)))

    (is (= {:name "Petr" :_father [{:name "David"} {:name "Thomas"}]}
           (d/pull test-db '[:name {:_father [:name]}] tdc/e1)))))

(deftest test-pull-component-attr
  (let [parts {:name "Part A",
               :part
               [{:db/id tdc/e11
                 :name "Part A.A",
                 :part
                 [{:db/id tdc/e12
                   :name "Part A.A.A",
                   :part
                   [{:db/id tdc/e13 :name "Part A.A.A.A"}
                    {:db/id tdc/e14 :name "Part A.A.A.B"}]}]}
                {:db/id tdc/e15
                 :name "Part A.B",
                 :part
                 [{:db/id tdc/e16
                   :name "Part A.B.A",
                   :part
                   [{:db/id tdc/e17 :name "Part A.B.A.A"}
                    {:db/id tdc/e18 :name "Part A.B.A.B"}]}]}]}
        rpart (update-in parts [:part 0 :part 0 :part]
                         (partial into [{:db/id tdc/e10}]))
        recdb (d/init-db
               (concat test-datoms [(d/datom tdc/e12 :part tdc/e10)])
               test-schema)]

    (testing "Component entities are expanded recursively"
      (is (= parts (d/pull test-db '[:name :part] tdc/e10))))

    (testing "Reverse component references yield a single result"
      (is (= {:name "Part A.A" :_part {:db/id tdc/e10}}
             (d/pull test-db [:name :_part] tdc/e11)))

      (is (= {:name "Part A.A" :_part {:name "Part A"}}
             (d/pull test-db [:name {:_part [:name]}] tdc/e11))))

    (testing "Like explicit recursion, expansion will not allow loops"
      (is (= rpart (d/pull recdb '[:name :part] tdc/e10))))))

(deftest test-pull-wildcard
  (is (= {:db/id tdc/e1 :name "Petr" :aka ["Devil" "Tupen"]
          :child [{:db/id tdc/e2} {:db/id tdc/e3}]}
         (d/pull test-db '[*] tdc/e1)))

  (is (= {:db/id tdc/e2 :name "David" :_child [{:db/id tdc/e1}] :father {:db/id tdc/e1}}
         (d/pull test-db '[* :_child] tdc/e2))))

(deftest test-pull-limit
  (let [db (d/init-db
             (concat
               test-datoms
               [(d/datom tdc/e4 :friend tdc/e5)
                (d/datom tdc/e4 :friend tdc/e6)
                (d/datom tdc/e4 :friend tdc/e7)
                (d/datom tdc/e4 :friend tdc/e8)]
               (for [idx (range 2000)]
                 (d/datom tdc/e8 :aka (str "aka-" idx))))
              test-schema)]

    (testing "Without an explicit limit, the default is 1000"
      (is (= 1000 (->> (d/pull db '[:aka] tdc/e8) :aka count))))

    (testing "Explicit limit can reduce the default"
      (is (= 500 (->> (d/pull db '[(limit :aka 500)] tdc/e8) :aka count)))
      (is (= 500 (->> (d/pull db '[[:aka :limit 500]] tdc/e8) :aka count))))

    (testing "Explicit limit can increase the default"
      (is (= 1500 (->> (d/pull db '[(limit :aka 1500)] tdc/e8) :aka count))))

    (testing "A nil limit produces unlimited results"
      (is (= 2000 (->> (d/pull db '[(limit :aka nil)] tdc/e8) :aka count))))

    (testing "Limits can be used as map specification keys"
      (is (= {:name "Lucy"
              :friend [{:name "Elizabeth"} {:name "Matthew"}]}
             (d/pull db '[:name {(limit :friend 2) [:name]}] tdc/e4))))))

(deftest test-pull-default
  (testing "Empty results return nil"
    (is (nil? (d/pull test-db '[:foo] tdc/e1))))

  (testing "A default can be used to replace nil results"
    (is (= {:foo "bar"}
           (d/pull test-db '[(default :foo "bar")] tdc/e1)))
    (is (= {:foo "bar"}
           (d/pull test-db '[[:foo :default "bar"]] tdc/e1)))))

(deftest test-pull-as
  (is (= {"Name" "Petr", :alias ["Devil" "Tupen"]}
         (d/pull test-db '[[:name :as "Name"] [:aka :as :alias]] tdc/e1))))

(deftest test-pull-attr-with-opts
  (is (= {"Name" "Nothing"}
         (d/pull test-db '[[:x :as "Name" :default "Nothing"]] tdc/e1))))

(deftest test-pull-map
  (testing "Single attrs yield a map"
    (is (= {:name "Matthew" :father {:name "Thomas"}}
           (d/pull test-db '[:name {:father [:name]}] tdc/e6))))

  (testing "Multi attrs yield a collection of maps"
    (is (= {:name "Petr" :child [{:name "David"}
                                 {:name "Thomas"}]}
           (d/pull test-db '[:name {:child [:name]}] tdc/e1))))

  (testing "Missing attrs are dropped"
    (is (= {:name "Petr"}
           (d/pull test-db '[:name {:father [:name]}] tdc/e1))))

  (testing "Non matching results are removed from collections"
    (is (= {:name "Petr" :child []}
           (d/pull test-db '[:name {:child [:foo]}] tdc/e1))))

  (testing "Map specs can override component expansion"
    (let [parts {:name "Part A" :part [{:name "Part A.A"} {:name "Part A.B"}]}]
      (is (= parts
             (d/pull test-db '[:name {:part [:name]}] tdc/e10)))

      (is (= parts
             (d/pull test-db '[:name {:part 1}] tdc/e10))))))

(deftest test-pull-recursion
  (let [db      (-> test-db
                    (d/db-with [[:db/add tdc/e4 :friend tdc/e5]
                                [:db/add tdc/e5 :friend tdc/e6]
                                [:db/add tdc/e6 :friend tdc/e7]
                                [:db/add tdc/e7 :friend tdc/e8]
                                [:db/add tdc/e4 :enemy tdc/e6]
                                [:db/add tdc/e5 :enemy tdc/e7]
                                [:db/add tdc/e6 :enemy tdc/e8]
                                [:db/add tdc/e7 :enemy tdc/e4]]))
        friends {:db/id tdc/e4
                 :name "Lucy"
                 :friend
                 [{:db/id tdc/e5
                   :name "Elizabeth"
                   :friend
                   [{:db/id tdc/e6
                     :name "Matthew"
                     :friend
                     [{:db/id tdc/e7
                       :name "Eunan"
                       :friend
                       [{:db/id tdc/e8
                         :name "Kerri"}]}]}]}]}
        enemies {:db/id tdc/e4 :name "Lucy"
                 :friend
                 [{:db/id tdc/e5 :name "Elizabeth"
                   :friend
                   [{:db/id tdc/e6 :name "Matthew"
                     :enemy [{:db/id tdc/e8 :name "Kerri"}]}]
                   :enemy
                   [{:db/id tdc/e7 :name "Eunan"
                     :friend
                     [{:db/id tdc/e8 :name "Kerri"}]
                     :enemy
                     [{:db/id tdc/e4 :name "Lucy"
                       :friend [{:db/id tdc/e5}]}]}]}]
                 :enemy
                 [{:db/id tdc/e6 :name "Matthew"
                   :friend
                   [{:db/id tdc/e7 :name "Eunan"
                     :friend
                     [{:db/id tdc/e8 :name "Kerri"}]
                     :enemy [{:db/id tdc/e4 :name "Lucy"
                              :friend [{:db/id tdc/e5 :name "Elizabeth"}]}]}]
                   :enemy
                   [{:db/id tdc/e8 :name "Kerri"}]}]}]

    (testing "Infinite recursion"
      (is (= friends (d/pull db '[:db/id :name {:friend ...}] tdc/e4))))

    (testing "Multiple recursion specs in one pattern"
      (is (= enemies (d/pull db '[:db/id :name {:friend 2 :enemy 2}] tdc/e4))))

    (let [db (d/db-with db [[:db/add tdc/e8 :friend tdc/e4]])]
      (testing "Cycles are handled by returning only the :db/id of entities which have been seen before"
        (is (= (update-in friends (take 8 (cycle [:friend 0]))
                          assoc :friend [{:db/id tdc/e4 :name "Lucy" :friend [{:db/id tdc/e5}]}])
               (d/pull db '[:db/id :name {:friend ...}] tdc/e4)))))))

(deftest test-dual-recursion

  (let [empty (d/empty-db {:part { :db/valueType :db.type/ref }
                           :spec { :db/valueType :db.type/ref }})]
    (let [db (d/db-with empty [[:db/add tdc/e1 :part tdc/e2]
                               [:db/add tdc/e2 :part tdc/e3]
                               [:db/add tdc/e3 :part tdc/e1]
                               [:db/add tdc/e1 :spec tdc/e2]
                               [:db/add tdc/e2 :spec tdc/e1]])]
      (is (= (d/pull db '[:db/id {:part ...} {:spec ...}] tdc/e1)
             {:db/id tdc/e1,
              :spec {:db/id tdc/e2
                     :spec {:db/id tdc/e1,
                            :spec {:db/id tdc/e2}, :part {:db/id tdc/e2}}
                     :part {:db/id tdc/e3,
                            :part {:db/id tdc/e1,
                                   :spec {:db/id tdc/e2},
                                   :part {:db/id tdc/e2}}}}
              :part {:db/id tdc/e2
                     :spec {:db/id tdc/e1, :spec {:db/id tdc/e2}, :part {:db/id tdc/e2}}
                     :part {:db/id tdc/e3,
                            :part {:db/id tdc/e1,
                                   :spec {:db/id tdc/e2},
                                   :part {:db/id tdc/e2}}}}})))))

(deftest test-deep-recursion
  (let [start 100
        depth 1500
        txd   (mapcat
               (fn [idx]
                 [(d/datom idx :name (str "Person-" idx))
                  (d/datom (dec idx) :friend idx)])
               (range (inc start) depth))
        db    (d/init-db (concat
                          test-datoms
                          [(d/datom start :name (str "Person-" start))]
                          txd)
                         test-schema)
        pulled (d/pull db '[:name {:friend ...}] start)
        path   (->> [:friend 0]
                    (repeat (dec (- depth start)))
                    (into [] cat))]
    (is (= (str "Person-" (dec depth))
           (:name (get-in pulled path))))))

#_(t/test-ns 'datahike.test.pull-api)
