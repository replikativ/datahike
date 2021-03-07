(ns datahike.test.entity-test
  (:require
   [#?(:cljs cljs.reader :clj clojure.edn) :as edn]
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [datahike.core :as d]
   [datahike.impl.entity :as de]
   [datahike.db :as db]
   [datahike.test.cljs]
   [clojure.core.async :refer [go <!]]
   [datahike.test.core-test :as tdc]
   [hitchhiker.tree.utils.cljs.async :as ha]
   )
  #?(:clj
     (:import [clojure.lang ExceptionInfo])))

#?(:clj (t/use-fixtures :once tdc/no-namespace-maps))


(deftest test-entity
  #?(:cljs 
     (t/async done 
              (go (let [db (-> (<! (d/empty-db {:aka {:db/cardinality :db.cardinality/many}}))
                               (d/db-with [{:db/id 1, :name "Ivan", :age 19, :aka ["X" "Y"]}
                                           {:db/id 2, :name "Ivan", :sex "male", :aka ["Z"]}
                                           [:db/add 3 :huh? false]])
                               (<!))
                        e  (<! (de/touch (<! (d/entity db 1))))]

                    (is (= (:db/id e) 1))
                    (is (identical? (d/entity-db e) db))
                    (is (= (:name e) "Ivan"))
                    (is (= (<! (e :name)) "Ivan")) ; IFn form
                    (is (= (:age  e) 19))
                    (is (= (:aka  e) #{"X" "Y"}))
                    (is (= true (contains? (<! (de/touch e)) :age)))
                    
                    (is (= false (contains? (<! (de/touch e)) :not-found)))
                    (is (= (into {}  e)
                           {:name "Ivan", :age 19, :aka #{"X" "Y"}}))
                    (is (= (into {} (<! (d/touch (<! (d/entity db 1)))))
                           {:name "Ivan", :age 19, :aka #{"X" "Y"}}))
                    (is (= (into {} (<! (d/touch (<! (d/entity db 2)))))
                           {:name "Ivan", :sex "male", :aka #{"Z"}}))
                    (let [e3 (<! (de/touch (<! (d/entity db 3))))]
                      (is (= (into {} e3) {:huh? false})) ; Force caching.
                      (is (false? (:huh? e3))))


                    ;;
                    )
                  
                  (done)))
     :clj (let [db (-> (d/empty-db {:aka {:db/cardinality :db.cardinality/many}})
                       (d/db-with [{:db/id 1, :name "Ivan", :age 19, :aka ["X" "Y"]}
                                   {:db/id 2, :name "Ivan", :sex "male", :aka ["Z"]}
                                   [:db/add 3 :huh? false]]))
                e  (d/entity db 1)]
            (is (= (:db/id e) 1))
            (is (identical? (d/entity-db e) db))
            (is (= (:name e) "Ivan"))
            (is (= (e :name) "Ivan")) ; IFn form
            (is (= (:age  e) 19))
            (is (= (:aka  e) #{"X" "Y"}))
            (is (= true (contains? e :age)))
            (is (= false (contains? e :not-found)))
            (is (= (into {} e)
                   {:name "Ivan", :age 19, :aka #{"X" "Y"}}))
            (is (= (into {} (d/entity db 1))
                   {:name "Ivan", :age 19, :aka #{"X" "Y"}}))
            (is (= (into {} (d/entity db 2))
                   {:name "Ivan", :sex "male", :aka #{"Z"}}))
            (let [e3 (d/entity db 3)]
              (is (= (into {} e3) {:huh? false})) ; Force caching.
              (is (false? (:huh? e3))))

            (is (= (pr-str (d/entity db 1)) "{:db/id 1}"))
            (is (= (pr-str (let [e (d/entity db 1)] (:unknown e) e)) "{:db/id 1}"))
    ;; read back in to account for unordered-ness
            (is (= (edn/read-string (pr-str (let [e (d/entity db 1)] (:name e) e)))
                   (edn/read-string "{:name \"Ivan\", :db/id 1}"))))))

(deftest test-entity-refs
  #?(:cljs 
     (t/async done
              (go (let [db (-> (<! (d/empty-db {:father   {:db/valueType   :db.type/ref}
                                                :children {:db/valueType   :db.type/ref
                                                           :db/cardinality :db.cardinality/many}}))
                               (d/db-with
                                [{:db/id 1, :children [10]}
                                 {:db/id 10, :father 1, :children [100 101]}
                                 {:db/id 100, :father 10}
                                 {:db/id 101, :father 10}])
                               (<!))
                        e  #(go (<! (de/touch (<! (d/entity db %)))))]

                    (is (= (:children (<! (e 1)))   #{(<! (e 10))}))
                    (is (= (:children (<! (e 10)))  #{(<! (e 100)) (<! (e 101))}))

                    (testing "empty attribute"
                      (is (= (:children (<! (e 100))) nil)))

                    (testing "nested navigation" ;; TODO: consider different interface / refactor of tests. Like get-in.
                      (is (= (-> (<! (e 1)) (de/lookup-entity :children) (<!) first (de/lookup-entity :children) (<!)) #{(<! (e 100)) (<! (e 101))}))
                      (is (= (-> (<! (e 10)) (de/lookup-entity :children) (<!) first (de/lookup-entity :father) (<!)) (<! (e 10))))
                      (is (= (-> (<! (e 10)) (de/lookup-entity :father) (<!) (de/lookup-entity :children) (<!)) #{(<! (e 10))}))

                      (testing "after touch" ;; TODO: consider different interface / refactor of tests. Like get-in.
                        (let [e1  (<! (e 1))
                              e10 (<! (e 10))]
                          (<! (de/touch e1))
                          (<! (de/touch e10))
                          (is (= (->  e1  (de/lookup-entity :children) (<!) first (de/lookup-entity :children) (<!)) #{(<! (e 100)) (<! (e 101))}))
                          (is (= (-> e10  (de/lookup-entity :children) (<!) first (de/lookup-entity :father) (<!)) (<! (e 10))))
                          (is (= (-> e10 (de/lookup-entity :father) (<!) (de/lookup-entity :children) (<!)) #{(<! (e 10))})))))

                    (testing "backward navigation" ;; TODO: consider different interface / refactor of tests. Like get-in.
                      (is (= (<! (de/lookup-entity (<! (e 1)) :_children))  nil))
                      (is (= (<! (de/lookup-entity (<! (e 1)) :_father))  #{(<! (e 10))}))
                      (is (= (<! (de/lookup-entity (<! (e 10)) :_children)) #{(<! (e 1))}))
                      (is (= (<! (de/lookup-entity (<! (e 10)) :_father)) #{(<! (e 100)) (<! (e 101))}))
                      (is (= (-> (de/lookup-entity (<! (e 100)) :_children) (<!) first (de/lookup-entity  :_children) (<!)) #{(<! (e 1))})))
                    (done))))
     :clj (let [db (-> (d/empty-db {:father   {:db/valueType   :db.type/ref}
                                    :children {:db/valueType   :db.type/ref
                                               :db/cardinality :db.cardinality/many}})
                       (d/db-with
                        [{:db/id 1, :children [10]}
                         {:db/id 10, :father 1, :children [100 101]}
                         {:db/id 100, :father 10}
                         {:db/id 101, :father 10}]))
                e  #(d/entity db %)]

            (is (= (:children (e 1))   #{(e 10)}))
            (is (= (:children (e 10))  #{(e 100) (e 101)}))

            (testing "empty attribute"
              (is (= (:children (e 100)) nil)))

            (testing "nested navigation"
              (is (= (-> (e 1) :children first :children) #{(e 100) (e 101)}))
              (is (= (-> (e 10) :children first :father) (e 10)))
              (is (= (-> (e 10) :father :children) #{(e 10)}))

              (testing "after touch"
                (let [e1  (e 1)
                      e10 (e 10)]
                  (d/touch e1)
                  (d/touch e10)
                  (is (= (-> e1 :children first :children) #{(e 100) (e 101)}))
                  (is (= (-> e10 :children first :father) (e 10)))
                  (is (= (-> e10 :father :children) #{(e 10)})))))

            (testing "backward navigation"
              (is (= (:_children (e 1))  nil))
              (is (= (:_father   (e 1))  #{(e 10)}))
              (is (= (:_children (e 10)) #{(e 1)}))
              (is (= (:_father   (e 10)) #{(e 100) (e 101)}))
              (is (= (-> (e 100) :_children first :_children) #{(e 1)}))))))

(deftest test-entity-misses
  #?(:cljs 
     (t/async done 
             (ha/go-try (let [db (-> (ha/<? (d/empty-db {:name {:db/unique :db.unique/identity}}))
                              (d/db-with [{:db/id 1, :name "Ivan"}
                                          {:db/id 2, :name "Oleg"}])
                              (ha/<?))]
                   (is (nil? (<! (d/entity db nil))))
                   (is (nil? (<! (d/entity db "abc"))))
                   (is (nil? (<! (d/entity db :keyword))))
                   (is (nil? (<! (d/entity db [:name "Petr"]))))
                   (is (= 777 (<! (de/lookup-entity (<! (d/entity db 777)) :db/id))))
                   (is (thrown-msg? "Lookup ref attribute should be marked as :db/unique: [:not-an-attr 777]"
                                    (ha/<? (d/entity db [:not-an-attr 777])))))
                 (done)))
     :clj (let [db (-> (d/empty-db {:name {:db/unique :db.unique/identity}})
                       (d/db-with [{:db/id 1, :name "Ivan"}
                                   {:db/id 2, :name "Oleg"}]))]
            (is (nil? (d/entity db nil)))
            (is (nil? (d/entity db "abc")))
            (is (nil? (d/entity db :keyword)))
            (is (nil? (d/entity db [:name "Petr"])))
            (is (= 777 (:db/id (d/entity db 777))))
            (is (thrown-msg? "Lookup ref attribute should be marked as :db/unique: [:not-an-attr 777]"
                             (d/entity db [:not-an-attr 777]))))))



