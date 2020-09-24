(ns datahike.test.entity
  (:require
   [#?(:cljs cljs.reader :clj clojure.edn) :as edn]
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [datahike.core :as d]
   [datahike.test.core :as tdc])
  #?(:clj
     (:import [clojure.lang ExceptionInfo])))

(t/use-fixtures :once tdc/no-namespace-maps)

(deftest test-entity
  (let [db (-> (d/empty-db {:aka {:db/cardinality :db.cardinality/many}})
               (d/db-with [{:db/id tdc/e1, :name "Ivan", :age 19, :aka ["X" "Y"]}
                           {:db/id tdc/e2, :name "Ivan", :sex "male", :aka ["Z"]}
                           [:db/add tdc/e3 :huh? false]]))
        e  (d/entity db tdc/e1)]
    (is (= (:db/id e) tdc/e1))
    (is (identical? (d/entity-db e) db))
    (is (= (:name e) "Ivan"))
    (is (= (e :name) "Ivan")) ; IFn form
    (is (= (:age  e) 19))
    (is (= (:aka  e) #{"X" "Y"}))
    (is (= true (contains? e :age)))
    (is (= false (contains? e :not-found)))
    (is (= (into {} e)
           {:name "Ivan", :age 19, :aka #{"X" "Y"}}))
    (is (= (into {} (d/entity db tdc/e1))
           {:name "Ivan", :age 19, :aka #{"X" "Y"}}))
    (is (= (into {} (d/entity db tdc/e2))
           {:name "Ivan", :sex "male", :aka #{"Z"}}))
    (let [e3 (d/entity db tdc/e3)]
      (is (= (into {} e3) {:huh? false})) ; Force caching.
      (is (false? (:huh? e3))))

    (is (= (pr-str (d/entity db 1)) "{:db/id 1}"))
    (is (= (pr-str (let [e (d/entity db tdc/e1)] (:unknown e) e)) (str "{:db/id " tdc/e1 "}")))
    ;; read back in to account for unordered-ness
    (is (= (edn/read-string (pr-str (let [e (d/entity db tdc/e1)] (:name e) e)))
           (edn/read-string (str "{:name \"Ivan\", :db/id " tdc/e1 "}"))))))

(deftest test-entity-refs
  (let [db (-> (d/empty-db {:father   {:db/valueType   :db.type/ref}
                            :children {:db/valueType   :db.type/ref
                                       :db/cardinality :db.cardinality/many}})
               (d/db-with
                [{:db/id tdc/e1, :children [tdc/e2]}
                 {:db/id tdc/e2, :father tdc/e1, :children [tdc/e3 tdc/e4]}
                 {:db/id tdc/e3, :father tdc/e2}
                 {:db/id tdc/e4, :father tdc/e2}]))
        e  #(d/entity db %)]

    (is (= (:children (e tdc/e1))   #{(e tdc/e2)}))
    (is (= (:children (e tdc/e2))  #{(e tdc/e3) (e tdc/e4)}))

    (testing "empty attribute"
      (is (= (:children (e tdc/e3)) nil)))

    (testing "nested navigation"
      (is (= (-> (e tdc/e1) :children first :children) #{(e tdc/e3) (e tdc/e4)}))
      (is (= (-> (e tdc/e2) :children first :father) (e tdc/e2)))
      (is (= (-> (e tdc/e2) :father :children) #{(e tdc/e2)}))

      (testing "after touch"
        (let [e1  (e tdc/e1)
              e10 (e tdc/e2)]
          (d/touch e1)
          (d/touch e10)
          (is (= (-> e1 :children first :children) #{(e tdc/e3) (e tdc/e4)}))
          (is (= (-> e10 :children first :father) (e tdc/e2)))
          (is (= (-> e10 :father :children) #{(e tdc/e2)})))))

    (testing "backward navigation"
      (is (= (:_children (e tdc/e1))  nil))
      (is (= (:_father   (e tdc/e1))  #{(e tdc/e2)}))
      (is (= (:_children (e tdc/e2)) #{(e tdc/e1)}))
      (is (= (:_father   (e tdc/e2)) #{(e tdc/e3) (e tdc/e4)}))
      (is (= (-> (e tdc/e3) :_children first :_children) #{(e tdc/e1)})))))

(deftest test-entity-misses
  (let [db (-> (d/empty-db {:name {:db/unique :db.unique/identity}})
               (d/db-with [{:db/id tdc/e1, :name "Ivan"}
                           {:db/id tdc/e2, :name "Oleg"}]))]
    (is (nil? (d/entity db nil)))
    (is (nil? (d/entity db "abc")))
    (is (nil? (d/entity db :keyword)))
    (is (nil? (d/entity db [:name "Petr"])))
    (is (= 777 (:db/id (d/entity db 777))))
    (is (thrown-msg? "Lookup ref attribute should be marked as :db/unique: [:not-an-attr 777]"
                     (d/entity db [:not-an-attr 777])))))
