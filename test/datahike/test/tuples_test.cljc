(ns datahike.test.tuples-test
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [datahike.api :as d]
   [datahike.db :as db])
  #?(:clj
     (:import [clojure.lang ExceptionInfo])))

(deftest test-schema-declaration
  (testing "composite tuple"
    (is (db/empty-db {:reg/semester+course+student {:db/valueType   :db.type/tuple
                                                    :db/tupleAttrs  [:reg/course :reg/semester :reg/student]}})))

  (testing "heterogeneous tuples"
    (is (db/empty-db  {:player/location {:db/valueType :db.type/tuple
                                         :db/tupleTypes [:db.type/long :db.type/long]}})))

  (testing "homogeneous tuples"
    (is (db/empty-db  {:db/tupleAttrs {:db/valueType :db.type/tuple
                                       :db/tupleType :db.type/keyword}}))))

(defn connect
  []
  (d/delete-database) ;; deletes the 'default' db
  (d/create-database {:schema-flexibility :write})
  (d/connect))

(deftest test-transaction
  (testing "homogeneous tuple"
    (let [conn (connect)]
      (d/transact conn [{:db/ident       :prices
                         :db/valueType   :db.type/tuple
                         :db/tupleType   :db.type/number
                         :db/cardinality :db.cardinality/one}])
      (testing "of less than 9 values"
        (is (d/transact conn [{:prices [1 2 3 4 5 6 7 8]}]))
        (testing "are of different types"
          (is (thrown-with-msg? ExceptionInfo #".*Cannot store homogeneous tuple with values of different type.*"
                                (d/transact conn [{:prices [1 2 3 4 5 6 "fdsfdsf"]}]))))
        (testing "are of wrong type"
          (is (thrown-with-msg? ExceptionInfo #".*Cannot store homogeneous tuple. Values are of wrong type.*"
                                (d/transact conn [{:prices ["a" "b" "fdsfdsf"]}])))))
      (testing "of more than 8 values"
        (is (thrown-with-msg? ExceptionInfo #".*Cannot store more than 8 values .*"
                              (d/transact conn [{:prices [1 2 3 4 5 6 7 8 9]}]))))))

  (testing "heterogeneous tuple"
    (let [conn (connect)]
      (d/transact conn [{:db/ident       :coord
                         :db/valueType   :db.type/tuple
                         :db/tupleTypes  [:db.type/long :db.type/keyword]
                         :db/cardinality :db.cardinality/one}])
      (is (d/transact conn [{:coord [100 :coord/west]}]))
      (testing "with wrong number of values"
        (is (thrown-with-msg? ExceptionInfo #".*Cannot store heterogeneous tuple: expecting 2 values, got 3.*"
                              (d/transact conn [{:coord [100 :coord/west 9]}]))))
      (testing "with type mismatch"
        (is (thrown-with-msg? ExceptionInfo #".*Cannot store heterogeneous tuple: there is a mismatch between values.* and their types.*"
                              (d/transact conn [{:coord [100 9]}]))))))

  (testing "composite tuple"
    (let [conn (connect)
          reg-schema [{:db/ident       :reg/course
                       :db/valueType   :db.type/string
                       :db/cardinality :db.cardinality/one}
                      {:db/ident       :reg/semester
                       :db/valueType   :db.type/string
                       :db/cardinality :db.cardinality/one}
                      {:db/ident       :reg/student
                       :db/valueType   :db.type/string
                       :db/cardinality :db.cardinality/one}]]
      (d/transact conn reg-schema)
      (is (d/transact conn [{:db/ident       :reg/semester+course+student
                             :db/valueType   :db.type/tuple
                             :db/tupleAttrs  [:reg/course :reg/semester :reg/student]
                             :db/cardinality :db.cardinality/one}]))
      (is (d/transact conn [{:reg/course   "BIO-101"
                             :reg/semester "2018-fall"
                             :reg/student  "johndoe@university.edu"}])))))

(deftest test-transact-and-query-non-composite
  (testing "heterogeneous"
    (let [conn (connect)]
      (d/transact conn [{:db/ident       :coord
                         :db/valueType   :db.type/tuple
                         :db/tupleTypes  [:db.type/long :db.type/keyword]
                         :db/cardinality :db.cardinality/one}])
      (d/transact conn [[:db/add 100 :coord [100 :coord/west]]])
      (is (= #{[[100 :coord/west]]}
             (d/q '[:find ?v
                    :where [_ :coord ?v]]
                  @conn)))))
  (testing "homogeneous"
    (let [conn (connect)]
      (d/transact conn [{:db/ident       :coord
                         :db/valueType   :db.type/tuple
                         :db/tupleType   :db.type/long
                         :db/cardinality :db.cardinality/one}])
      (d/transact conn [[:db/add 100 :coord [100 200 300]]])
      (is (= #{[[100 200 300]]}
             (d/q '[:find ?v
                    :where [_ :coord ?v]]
                  @conn))))))

(deftest test-transact-and-query-composite
  (let [conn (connect)]
    (d/transact conn [{:db/ident       :a
                       :db/valueType   :db.type/long
                       :db/cardinality :db.cardinality/one}
                      {:db/ident       :a+b+c
                       :db/valueType   :db.type/tuple
                       :db/tupleAttrs  [:a :b :c]
                       :db/cardinality :db.cardinality/one}])
    (is (d/transact conn [[:db/add 100 :a 123]]))
    (is (= #{[123]}
           (d/q '[:find ?v
                  :where [100 :a ?v]]
                @conn)))
    (is (= #{[100 [123 nil nil]]}
           (d/q '[:find ?e ?v
                  :where [?e :a+b+c ?v]]
                @conn)))
    (is (= #{[[123 nil nil]]}
           (d/q '[:find ?v
                  :where [100 :a+b+c ?v]]
                @conn)))))

(defn some-datoms
  [db es]
  (into #{} (map (juxt :e :a :v)) (mapcat #(d/datoms db {:index :eavt :components [%]}) es)))

(deftest test-more-composite-transaction
  (let [conn (connect)
        e    100]
    (d/transact conn [{:db/ident :a
                       :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one}
                      {:db/ident :b
                       :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one}
                      {:db/ident :c
                       :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one}
                      {:db/ident :d
                       :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one}
                      {:db/ident :a+b
                       :db/valueType :db.type/tuple
                       :db/tupleAttrs [:a :b]
                       :db/cardinality :db.cardinality/one}
                      {:db/ident :a+c+d
                       :db/valueType :db.type/tuple
                       :db/tupleAttrs [:a :c :d]
                       :db/cardinality :db.cardinality/one}])
    (are [tx datoms] (= datoms (some-datoms (:db-after (d/transact conn tx)) [e]))
      [[:db/add e :a "a"]]
      #{[e :a     "a"]
        [e :a+b   ["a" nil]]
        [e :a+c+d ["a" nil nil]]}

      [[:db/add e :b "b"]]
      #{[e :a     "a"]
        [e :b     "b"]
        [e :a+b   ["a" "b"]]
        [e :a+c+d ["a" nil nil]]}

      [[:db/add e :a "A"]]
      #{[e :a     "A"]
        [e :b     "b"]
        [e :a+b   ["A" "b"]]
        [e :a+c+d ["A" nil nil]]}

      [[:db/add e :c "c"]
       [:db/add e :d "d"]]
      #{[e :a     "A"]
        [e :b     "b"]
        [e :a+b   ["A" "b"]]
        [e :c     "c"]
        [e :d     "d"]
        [e :a+c+d ["A" "c" "d"]]}

      [[:db/add e :a "a"]]
      #{[e :a     "a"]
        [e :b     "b"]
        [e :a+b   ["a" "b"]]
        [e :c     "c"]
        [e :d     "d"]
        [e :a+c+d ["a" "c" "d"]]}

      [[:db/add e :a "A"]
       [:db/add e :b "B"]
       [:db/add e :c "C"]
       [:db/add e :d "D"]]
      #{[e :a     "A"]
        [e :b     "B"]
        [e :a+b   ["A" "B"]]
        [e :c     "C"]
        [e :d     "D"]
        [e :a+c+d ["A" "C" "D"]]}

      [[:db/retract e :a "A"]]
      #{[e :b     "B"]
        [e :a+b   [nil "B"]]
        [e :c     "C"]
        [e :d     "D"]
        [e :a+c+d [nil "C" "D"]]}

      [[:db/retract e :b "B"]]
      #{[e :c     "C"]
        [e :d     "D"]
        [e :a+c+d [nil "C" "D"]]})

    (is (thrown-with-msg? ExceptionInfo #"Can’t modify tuple attrs directly:.*"
                          (d/transact conn [{:db/id 100 :a+b ["A" "B"]}])))))

(deftest test-queries
  (let [conn (connect)]
    (d/transact conn [{:db/ident :a
                       :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one}
                      {:db/ident :b
                       :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one}
                      {:db/ident :a+b
                       :db/valueType :db.type/tuple
                       :db/tupleAttrs [:a :b]
                       :db/cardinality :db.cardinality/one
                       :db/unique :db.unique/value}])

    (d/transact conn [{:db/id 1 :a "A" :b "B"}
                      {:db/id 2 :a "A" :b "b"}
                      {:db/id 3 :a "a" :b "B"}
                      {:db/id 4 :a "a" :b "b"}])

    (is (= #{[3]}
           (d/q '[:find ?e
                  :where [?e :a+b ["a" "B"]]] @conn)))

    (is (= #{[["a" "B"]]}
           (d/q '[:find ?a+b
                  :where [[:a+b ["a" "B"]] :a+b ?a+b]] @conn)))

    (is (= #{[["A" "B"]] [["A" "b"]] [["a" "B"]] [["a" "b"]]}
           (d/q '[:find ?a+b
                  :where [?e :a ?a]
                  [?e :b ?b]
                  [(tuple ?a ?b) ?a+b]] @conn)))

    (is (= #{["A" "B"] ["A" "b"] ["a" "B"] ["a" "b"]}
           (d/q '[:find ?a ?b
                  :where [?e :a+b ?a+b]
                  [(untuple ?a+b) [?a ?b]]] @conn)))))

(deftest test-lookup-refs
  (let [conn (connect)]
    (d/transact conn [{:db/ident :a
                       :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one}
                      {:db/ident :b
                       :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one}
                      {:db/ident :c
                       :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one
                       :db/unique :db.unique/identity}
                      {:db/ident :d
                       :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one
                       :db/unique :db.unique/identity}
                      {:db/ident :a+b
                       :db/valueType :db.type/tuple
                       :db/tupleAttrs [:a :b]
                       :db/cardinality :db.cardinality/one
                       :db/unique :db.unique/identity}])

    (d/transact conn
                [{:db/id 100 :a "A" :b "B"}
                 {:db/id 200 :a "a" :b "b"}])

    (d/transact conn [[:db/add [:a+b ["A" "B"]] :c "C"]
                      {:db/id [:a+b ["a" "b"]] :c "c"}])

    (is (= #{[100 :a "A"]
             [100 :b "B"]
             [100 :a+b ["A" "B"]]
             [100 :c "C"]
             [200 :a "a"]
             [200 :b "b"]
             [200 :a+b ["a" "b"]]
             [200 :c "c"]}
           (some-datoms (d/db conn) [100 200])))

    (is (thrown-with-msg? ExceptionInfo #"Cannot add .* because of unique constraint: .*"
                          (d/transact conn [[:db/add [:a+b ["A" "B"]] :c "c"]])))

    (is (thrown-with-msg? ExceptionInfo #".*Conflicting upsert: \[\:c \"c\"] .*"
                          (d/transact conn [{:db/id [:a+b ["A" "B"]] :c "c"}])))

    ;; change tuple + upsert
    (d/transact conn
                [{:db/id [:a+b ["A" "B"]]
                  :b "b"
                  :d "D"}])

    (is (= #{[100 :a "A"]
             [100 :b "b"]
             [100 :a+b ["A" "b"]]
             [100 :c "C"]
             [100 :d "D"]
             [200 :a "a"]
             [200 :b "b"]
             [200 :a+b ["a" "b"]]
             [200 :c "c"]}
           (some-datoms (d/db conn) [100 200])))

    (is (= {:db/id 200
            :a     "a"
            :b     "b"
            :a+b   ["a" "b"]
            :c     "c"}
           (d/pull (d/db conn) '[*] [:a+b ["a" "b"]])))))

(deftest test-unique
  (let [conn (connect)]
    (d/transact conn [{:db/ident :a
                       :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one}
                      {:db/ident :b
                       :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one}
                      {:db/ident :a+b
                       :db/valueType :db.type/tuple
                       :db/tupleAttrs [:a :b]
                       :db/cardinality :db.cardinality/one
                       :db/unique :db.unique/identity}])

    (d/transact conn [[:db/add 100 :a "a"]])
    (d/transact conn [[:db/add 200 :a "A"]])
    (is (thrown-with-msg? ExceptionInfo #"Cannot add .* because of unique constraint: .*"
                          (d/transact conn [[:db/add 100 :a "A"]])))

    (d/transact conn [[:db/add 100 :b "b"]
                      [:db/add 200 :b "b"]
                      {:db/id 300 :a "a" :b "B"}])

    (is (= #{[100 :a "a"]
             [100 :b "b"]
             [100 :a+b ["a" "b"]]
             [200 :a "A"]
             [200 :b "b"]
             [200 :a+b ["A" "b"]]
             [300 :a "a"]
             [300 :b "B"]
             [300 :a+b ["a" "B"]]}
           (some-datoms (d/db conn) [100 200 300])))

    (is (thrown-with-msg? ExceptionInfo #"Cannot add .* because of unique constraint: .*"
                          (d/transact conn [[:db/add 100 :a "A"]])))
    (is (thrown-with-msg? ExceptionInfo #"Cannot add .* because of unique constraint: .*"
                          (d/transact conn [[:db/add 100 :b "B"]])))
    (is (thrown-with-msg? ExceptionInfo #"Cannot add .* because of unique constraint: .*"
                          (d/transact conn [[:db/add 100 :a "A"]
                                            [:db/add 100 :b "B"]])))

    (testing "multiple tuple updates"
      ;; changing both tuple components in a single operation
      (d/transact conn [{:db/id 100 :a "A" :b "B"}])
      (is (= {:db/id 100 :a "A" :b "B" :a+b ["A" "B"]}
             (d/pull (d/db conn) '[*] 100)))

      ;; adding entity with two tuple components in a single operation
      (d/transact conn [{:db/id 4 :a "a" :b "c"}])
      (is (= {:db/id 4 :a "a" :b "c" :a+b ["a" "c"]}
             (d/pull (d/db conn) '[*] 4))))))

(deftest test-validation
  (let [db (db/empty-db {:a+b {:db/valueType :db.type/tuple
                               :db/tupleAttrs [:a :b]}})
        db1 (d/db-with db [[:db/add 100 :a "a"]])
        err-msg #"Can’t modify tuple attrs directly:.*"]
    (is (thrown-with-msg? ExceptionInfo err-msg
                          (d/db-with db [[:db/add 100 :a+b [nil nil]]])))
    (is (thrown-with-msg? ExceptionInfo err-msg
                          (d/db-with db1 [[:db/add 100 :a+b ["a" nil]]])))
    (is (thrown-with-msg? ExceptionInfo err-msg
                          (d/db-with db [[:db/add 100 :a "a"]
                                         [:db/add 100 :a+b ["a" nil]]])))
    (is (thrown-with-msg? ExceptionInfo err-msg
                          (d/db-with db1 [[:db/retract 100 :a+b ["a" nil]]])))))

(deftest test-indexes
  (let [db (-> (db/empty-db {:a+b+c {:db/tupleAttrs [:a :b :c]
                                     :db/valueType :db.type/tuple
                                     :db/index true}})
               (d/db-with
                [{:db/id 1 :a "a" :b "b" :c "c"}
                 {:db/id 2 :a "A" :b "b" :c "c"}
                 {:db/id 3 :a "a" :b "B" :c "c"}
                 {:db/id 4 :a "A" :b "B" :c "c"}
                 {:db/id 5 :a "a" :b "b" :c "C"}
                 {:db/id 6 :a "A" :b "b" :c "C"}
                 {:db/id 7 :a "a" :b "B" :c "C"}
                 {:db/id 8 :a "A" :b "B" :c "C"}]))]
    (is (= [6]
           (mapv :e (d/datoms db :avet :a+b+c ["A" "b" "C"]))))
    (is (= []
           (mapv :e (d/datoms db :avet :a+b+c ["A" "b" nil]))))
    (is (= [8 4 6 2]
           (mapv :e (d/index-range db {:attrid :a+b+c :start ["A" "B" "C"] :end ["A" "b" "c"]}))))
    (is (= [8 4]
           (mapv :e (d/index-range db {:attrid :a+b+c :start ["A" "B" nil] :end ["A" "b" nil]}))))))
