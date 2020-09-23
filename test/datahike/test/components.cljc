(ns datahike.test.components
  (:require
    [#?(:cljs cljs.reader :clj clojure.edn) :as edn]
    #?(:cljs [cljs.test    :as t :refer-macros [is deftest testing]]
       :clj  [clojure.test :as t :refer        [is deftest testing]])
    [datahike.core :as d]
    [datahike.test.core :as tdc]))

(t/use-fixtures :once tdc/no-namespace-maps)

#?(:cljs
   (def Throwable js/Error))

(deftest test-components
  (is (thrown-msg? "Bad attribute specification for :profile: {:db/isComponent true} should also have {:db/valueType :db.type/ref}"
        (d/empty-db {:profile {:db/isComponent true}})))
  (is (thrown-msg? "Bad attribute specification for {:profile {:db/isComponent \"aaa\"}}, expected one of #{true false}"
        (d/empty-db {:profile {:db/isComponent "aaa" :db/valueType :db.type/ref}})))
  
  (let [empty-db (d/empty-db {:profile {:db/valueType   :db.type/ref
                                        :db/isComponent true}})
        db (d/db-with
             empty-db
             [{:db/id tdc/e1 :name "Ivan" :profile tdc/e3}
              {:db/id tdc/e3 :email "@3"}
              {:db/id tdc/e4 :email "@4"}])
        visible #(edn/read-string (pr-str %))
        touched #(visible (d/touch %))]
    
    (testing "touch"
      (is (= (touched (d/entity db tdc/e1))
             {:db/id tdc/e1
              :name "Ivan"
              :profile {:db/id tdc/e3
                        :email "@3"}}))
      (is (= (touched (d/entity (d/db-with db [[:db/add tdc/e3 :profile tdc/e4]]) tdc/e1))
             {:db/id tdc/e1
              :name "Ivan"
              :profile {:db/id tdc/e3
                        :email "@3"
                        :profile {:db/id tdc/e4
                                  :email "@4"}}})))
    (testing "retractEntity"
      (let [db (d/db-with db [[:db.fn/retractEntity tdc/e1]])]
        (is (= (d/q '[:find ?a ?v :in $ ?e1 :where [?e1 ?a ?v]]
                    db tdc/e1)
               #{}))
        (is (= (d/q '[:find ?a ?v :in $ ?e3 :where [?e3 ?a ?v]]
                    db tdc/e3)
               #{}))))
    
    (testing "retractAttribute"
      (let [db (d/db-with db [[:db.fn/retractAttribute tdc/e1 :profile]])]
        (is (= (d/q '[:find ?a ?v :in $ ?e3 :where [?e3 ?a ?v]]
                    db tdc/e3)
               #{}))))
    
    (testing "reverse navigation"
      (is (= (visible (:_profile (d/entity db tdc/e3)))
             {:db/id tdc/e1})))))

(deftest test-components-multival
  (let [empty-db (d/empty-db {:profile {:db/valueType   :db.type/ref
                                        :db/cardinality :db.cardinality/many
                                        :db/isComponent true}})
        db (d/db-with empty-db
             [{:db/id tdc/e1 :name "Ivan" :profile [tdc/e3 tdc/e4]}
              {:db/id tdc/e3 :email "@3"}
              {:db/id tdc/e4 :email "@4"}])
        visible #(edn/read-string (pr-str %))
        touched #(visible (d/touch %))]
    
    (testing "touch"
      (is (= (touched (d/entity db tdc/e1))
             {:db/id tdc/e1
              :name "Ivan"
              :profile #{{:db/id tdc/e3 :email "@3"}
                         {:db/id tdc/e4 :email "@4"}}})))
    
    (testing "retractEntity"
      (let [db (d/db-with db [[:db.fn/retractEntity tdc/e1]])]
        (is (= (d/q '[:find ?a ?v :in $ [?e ...] :where [?e ?a ?v]] db [tdc/e1 tdc/e3 tdc/e4])
               #{}))))
    
    (testing "retractAttribute"
      (let [db (d/db-with db [[:db.fn/retractAttribute tdc/e1 :profile]])]
        (is (= (d/q '[:find ?a ?v :in $ [?e ...] :where [?e ?a ?v]] db [tdc/e3 tdc/e4])
               #{}))))
    
    (testing "reverse navigation"
      (is (= (visible (:_profile (d/entity db tdc/e3)))
             {:db/id tdc/e1})))))
