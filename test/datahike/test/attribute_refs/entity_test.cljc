(ns datahike.test.attribute-refs.entity-test
  (:require
   [#?(:cljs cljs.reader :clj clojure.edn) :as edn]
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj [clojure.test :as t :refer [is deftest testing]])
   [datahike.api :as d]
   [datahike.impl.entity :as de]
   [datahike.test.attribute-refs.utils :refer [ref-db ref-e0 shift-entities]]
   [datahike.test.core-test :as tdc]))

(t/use-fixtures :once tdc/no-namespace-maps)

(deftest test-entity
  (let [entities [{:db/id 1, :name "Ivan", :age 19, :aka ["X" "Y"]}
                  {:db/id 2, :name "Petr", :sex "male", :aka ["Z"]}]
        db (d/db-with ref-db (shift-entities ref-e0 entities))
        e (d/entity db (+ ref-e0 1))]
    (is (= (:db/id e) (+ ref-e0 1)))
    (is (identical? (d/entity-db e) db))
    (is (= (:name e) "Ivan"))
    (is (= (e :name) "Ivan"))                               ; IFn form
    (is (= (:age e) 19))
    (is (= (:aka e) #{"X" "Y"}))
    (is (= true (contains? e :age)))
    (is (= false (contains? e :not-found)))
    (is (= (into {} e)
           {:name "Ivan", :age 19, :aka #{"X" "Y"}}))
    (is (= (into {} (d/entity db (+ ref-e0 1)))
           {:name "Ivan", :age 19, :aka #{"X" "Y"}}))
    (is (= (into {} (d/entity db (+ ref-e0 2)))
           {:name "Petr", :sex "male", :aka #{"Z"}}))

    (is (= (pr-str (d/entity db 1)) "{:db/id 1}"))
    (is (= (pr-str (let [e (d/entity db 1)] (:unknown e) e))
           (str "{:db/id 1}")))
    ;; read back in to account for unordered-ness
    (is (= (edn/read-string (pr-str (let [e (d/entity db (+ ref-e0 1))] (:name e) e)))
           (edn/read-string (str "{:name \"Ivan\", :db/id " (+ ref-e0 1) "}"))))))

(deftest test-entity-refs
  (let [db (d/db-with ref-db
                      [{:db/id (+ ref-e0 1) :children [(+ ref-e0 10)]}
                       {:db/id (+ ref-e0 10) :father (+ ref-e0 1) :children [(+ ref-e0 100) (+ ref-e0 101)]}
                       {:db/id (+ ref-e0 100) :father (+ ref-e0 10)}
                       {:db/id (+ ref-e0 101) :father (+ ref-e0 10)}])
        e #(d/entity db (+ ref-e0 %))]

    (is (= (:children (e 1)) #{(e 10)}))
    (is (= (:children (e 10)) #{(e 100) (e 101)}))

    (testing "empty attribute"
      (is (= (:children (e 100)) nil)))

    (testing "nested navigation"
      (is (= (-> (e 1) :children first :children) #{(e 100) (e 101)}))
      (is (= (-> (e 10) :children first :father) (e 10)))
      (is (= (-> (e 10) :father :children) #{(e 10)}))

      (testing "after touch"
        (let [e1 (e 1)
              e10 (e 10)]
          (de/touch e1)
          (de/touch e10)
          (is (= (-> e1 :children first :children) #{(e 100) (e 101)}))
          (is (= (-> e10 :children first :father) (e 10)))
          (is (= (-> e10 :father :children) #{(e 10)})))))

    (testing "backward navigation"
      (is (= (:_children (e 1)) nil))
      (is (= (:_father (e 1)) #{(e 10)}))
      (is (= (:_children (e 10)) #{(e 1)}))
      (is (= (:_father (e 10)) #{(e 100) (e 101)}))
      (is (= (-> (e 100) :_children first :_children) #{(e 1)})))))

(deftest test-entity-misses
  (let [entities (shift-entities ref-e0
                                 [{:db/id 1, :name "Ivan"}
                                  {:db/id 2, :name "Oleg"}])
        db (d/db-with ref-db entities)]
    (is (nil? (d/entity db [:name "Petr"])))
    (is (= 777 (:db/id (d/entity db 777))))
    (is (thrown-msg? "Lookup ref attribute should be marked as :db/unique: [:not-an-attr 777]"
                     (d/entity db [:not-an-attr 777])))))
