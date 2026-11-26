(ns datahike.test.query-collection-binding-test
  "Pins the result of collection and relation bindings, for `:in` inputs and
   for the output of a function clause. The values are fixed here, so a change
   to how these bindings are built must keep them."
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer        [is deftest testing]])
   [datahike.api :as d]
   [datahike.db :as db]))

(defn- thrown-msg [f]
  (try (f) nil
       (catch #?(:clj Exception :cljs :default) e
         (ex-message e))))

(deftest test-in-collection-binding
  (testing "scalar collection"
    (is (= #{[1] [2] [3]} (d/q '[:find ?x :in [?x ...]] [1 2 3])))
    (is (= #{[1] [2]} (d/q '[:find ?x :in [?x ...]] [1 2 2 1]))
        "duplicate values give one row in a set result")
    (is (= #{[nil] [1]} (d/q '[:find ?x :in [?x ...]] [nil 1]))
        "a nil value is a value")
    (is (= #{} (d/q '[:find ?x :in [?x ...]] []))))
  (testing "tuple collection"
    (is (= #{[1 :a] [2 :b]} (d/q '[:find ?a ?b :in [[?a ?b] ...]] [[1 :a] [2 :b]])))
    (is (= #{[1 :a]} (d/q '[:find ?a ?b :in [[?a ?b] ...]] [[1 :a] [1 :a]])))
    (is (= #{[1 :a] [2 nil]} (d/q '[:find ?a ?b :in [[?a ?b] ...]] [[1 :a] [2 nil]])))
    (is (= #{[1 :a]} (d/q '[:find ?a ?b :in [[?a ?b] ...]] [[1 :a :extra]]))
        "extra items of a tuple are ignored")
    (is (= #{[1 :a] [2 :b]} (d/q '[:find ?a ?b :in [[?a ?b] ...]] (list [1 :a] (list 2 :b))))
        "a tuple can be any sequence")
    (is (= #{[1] [2]} (d/q '[:find ?a :in [[?a _] ...]] [[1 :a] [2 :b]]))
        "an ignored column")
    (is (= #{[1 2 3]} (d/q '[:find ?a ?b ?c :in [[?a ?b ?c] ...]] [[1 2 3]]))))
  (testing "nested and mixed shapes"
    (is (= #{[1 :a] [1 :b] [2 :c]}
           (d/q '[:find ?a ?b :in [[?a [?b ...]] ...]] [[1 [:a :b]] [2 [:c]]])))
    (is (= #{[1]} (d/q '[:find ?x :in [[?x ...] ...]] [[1] [1]]))))
  (testing "tuple of the wrong length"
    (is (some? (thrown-msg #(d/q '[:find ?a ?b :in [[?a ?b] ...]] [[1 :a] [2]])))
        "a short tuple is an error")
    (is (re-find #"Not enough elements"
                 (thrown-msg #(d/q '[:find ?a ?b :in [[?a ?b] ...]] [[1 :a] [2]])))))
  (testing "a value that is not a collection"
    (is (re-find #"Cannot bind value"
                 (thrown-msg #(d/q '[:find ?x :in [?x ...]] 1))))
    (is (re-find #"Cannot bind value"
                 (thrown-msg #(d/q '[:find ?a ?b :in [[?a ?b] ...]] [1 2])))))
  (testing "a bound variable is unified"
    (is (= #{[2 2]} (d/q '[:find ?x ?y :in [?x ...] [?y ...] :where [(= ?x ?y)]] [1 2] [2 3])))))

(deftest test-in-tuple-and-relation-binding
  (is (= #{[1 :a]} (d/q '[:find ?a ?b :in [?a ?b]] [1 :a])))
  (is (= #{[1 :a] [2 :b]} (d/q '[:find ?a ?b :in [[?a ?b]]] [[1 :a] [2 :b]])))
  (is (= #{[1 :a] [2 :b]} (d/q '[:find ?a ?b :in [[?a ?b] ...]] [[1 :a] [2 :b]]))))

(deftest test-function-output-binding
  (testing "collection of scalars"
    (is (= #{[1] [2] [3]} (d/q '[:find ?x :in ?c :where [(identity ?c) [?x ...]]] [1 2 3])))
    (is (= #{[nil] [1]} (d/q '[:find ?x :in ?c :where [(identity ?c) [?x ...]]] [nil 1]))))
  (testing "collection of tuples, one function call per row"
    (is (= #{[1 :a] [2 :b]}
           (d/q '[:find ?a ?b :in [?c ...] :where [(identity ?c) [[?a ?b] ...]]]
                [[[1 :a]] [[2 :b]]])))
    (is (= #{[1 :a] [2 :b] [2 :c]}
           (d/q '[:find ?a ?b :in [?k ...] :where [(identity ?k) [[?a ?b] ...]]]
                [[[1 :a] [2 :b]] [[2 :c] [1 :a]]])))
    (is (= #{[0 1 :a] [0 2 :b] [1 3 :c]}
           (d/q '[:find ?i ?a ?b :in [[?i ?c] ...]
                  :where [(identity ?c) [[?a ?b] ...]]]
                [[0 [[1 :a] [2 :b]]] [1 [[3 :c]]]]))
        "the output joins each input row with its own results"))
  (testing "a nil result drops the row; an empty result drops the row"
    (is (= #{[1 :a]}
           (d/q '[:find ?i ?a :in [[?i ?c] ...] :where [(identity ?c) [?a ...]]]
                [[1 [:a]] [2 nil] [3 []]])))
    (is (= #{} (d/q '[:find ?x :in ?c :where [(identity ?c) [?x ...]]] []))))
  (testing "a tuple of the wrong length"
    (is (re-find #"Not enough elements"
                 (thrown-msg #(d/q '[:find ?a ?b :in ?c :where [(identity ?c) [[?a ?b] ...]]]
                                   [[1 :a] [2]])))))
  (testing "the output names a variable that the row binds: unification"
    (is (= #{[2]}
           (d/q '[:find ?x :in [?x ...] ?c :where [(identity ?c) [?x ...]]] [1 2] [2 3])))
    (is (= #{[1 :a]}
           (d/q '[:find ?a ?b :in [[?a ?b] ...] ?c
                  :where [(identity ?c) [[?a ?b] ...]]]
                [[1 :a] [3 :z]] [[1 :a] [2 :b]]))))
  (testing "a binding that is a scalar, a tuple, a relation"
    (is (= #{[1 :a]} (d/q '[:find ?a ?b :in ?c :where [(identity ?c) [?a ?b]]] [1 :a])))
    (is (= #{[1 :a] [2 :b]} (d/q '[:find ?a ?b :in ?c :where [(identity ?c) [[?a ?b]]]] [[1 :a] [2 :b]])))
    (is (= #{[3]} (d/q '[:find ?x :in ?c :where [(identity ?c) ?x]] 3)))))

(deftest test-function-output-binding-over-data
  (let [db (-> (db/empty-db {})
               (d/db-with [{:db/id 1 :name "a" :age 10}
                           {:db/id 2 :name "b" :age 20}
                           {:db/id 3 :name "c" :age 20}]))]
    (is (= #{[1 10 10] [1 10 1] [2 20 20] [2 20 2] [3 20 20] [3 20 3]}
           (d/q '[:find ?e ?age ?x :in $ :where
                  [?e :age ?age]
                  [(vector ?age ?e) [?x ...]]]
                db))
        "a collection output per data row")
    (is (= #{[1 10 :a] [2 20 :b] [3 20 :b]}
           (d/q '[:find ?e ?age ?k :in $ :where
                  [?e :age ?age]
                  [(identity [[10 :a] [20 :b]]) [[?age ?k] ...]]]
                db))
        "a tuple output that joins on a variable the data row binds")))
